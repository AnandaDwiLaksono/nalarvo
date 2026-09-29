use crate::{ApplicationContext, ApplicationError, CredentialRef, SecretValue};
use nalarvo_domain::{
    AuditRecord, CompanyId, DomainEvent, PrincipalRef, ScopeRef, UserId, WorkspaceId,
};
use nalarvo_persistence::{
    self as persistence, AgentRecord, CredentialRefRecord, DepartmentRecord,
    ProviderConnectionRecord, RoleRecord, WorkspaceRecord,
};
use uuid::Uuid;

impl ApplicationContext {
    pub async fn get_workspace(
        &self,
        principal: &UserId,
        workspace: &WorkspaceId,
    ) -> Result<WorkspaceRecord, ApplicationError> {
        let item = persistence::get_workspace(&self.pool, workspace)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(workspace.0.clone()))?;
        if persistence::list_workspaces_for_user(&self.pool, principal)
            .await?
            .into_iter()
            .any(|candidate| candidate.id == item.id)
        {
            Ok(item)
        } else {
            Err(ApplicationError::NotFound(workspace.0.clone()))
        }
    }

    pub async fn list_credentials(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<CredentialRefRecord>, ApplicationError> {
        Ok(persistence::list_credential_refs_full(&self.pool, workspace_id).await?)
    }

    pub async fn submit_credential(
        &self,
        workspace_id: &WorkspaceId,
        name: &str,
        secret: &str,
    ) -> Result<CredentialRefRecord, ApplicationError> {
        let id = Uuid::now_v7().to_string();
        let locator = format!("secretstore://workspace/{}/{}", workspace_id.0, id);
        let credential_ref = CredentialRef::new(&id);
        let secret_value = SecretValue::new(secret);

        self.secret_store.put(&credential_ref, &secret_value)?;
        if let Err(err) =
            persistence::create_credential_ref(&self.pool, workspace_id, &id, name, &locator).await
        {
            let _ = self.secret_store.delete(&credential_ref);
            return Err(err.into());
        }

        persistence::get_credential_ref(&self.pool, workspace_id, &id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id))
    }

    pub async fn disable_credential(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
        expected_version: i64,
    ) -> Result<(), ApplicationError> {
        Ok(persistence::update_credential_ref_status(
            &self.pool,
            workspace_id,
            id,
            "DISABLED",
            expected_version,
        )
        .await?)
    }

    pub async fn revoke_credential(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
        expected_version: i64,
    ) -> Result<(), ApplicationError> {
        Ok(persistence::update_credential_ref_status(
            &self.pool,
            workspace_id,
            id,
            "REVOKED",
            expected_version,
        )
        .await?)
    }

    pub async fn list_providers(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ProviderConnectionRecord>, ApplicationError> {
        Ok(persistence::list_provider_connections(&self.pool, workspace_id).await?)
    }

    pub async fn create_provider(
        &self,
        workspace_id: &WorkspaceId,
        name: &str,
        provider_kind: &str,
        credential_ref_id: Option<&str>,
    ) -> Result<ProviderConnectionRecord, ApplicationError> {
        let id = Uuid::now_v7().to_string();
        persistence::create_provider_connection(
            &self.pool,
            workspace_id,
            &id,
            name,
            provider_kind,
            credential_ref_id,
        )
        .await?;
        persistence::get_provider_connection_full(&self.pool, workspace_id, &id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id))
    }

    pub async fn enable_provider(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
        expected_version: i64,
    ) -> Result<ProviderConnectionRecord, ApplicationError> {
        persistence::update_provider_status(
            &self.pool,
            workspace_id,
            id,
            "ENABLED",
            expected_version,
        )
        .await?;
        persistence::get_provider_connection_full(&self.pool, workspace_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn disable_provider(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
        expected_version: i64,
    ) -> Result<ProviderConnectionRecord, ApplicationError> {
        persistence::update_provider_status(
            &self.pool,
            workspace_id,
            id,
            "DISABLED",
            expected_version,
        )
        .await?;
        persistence::get_provider_connection_full(&self.pool, workspace_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn test_provider_health(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
    ) -> Result<ProviderConnectionRecord, ApplicationError> {
        let provider = persistence::get_provider_connection_full(&self.pool, workspace_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;

        let endpoint = provider
            .endpoint
            .as_deref()
            .unwrap_or("https://api.openai.com/v1");
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .map_err(|e| ApplicationError::Validation(e.to_string()))?;

        let health_status = match client.get(endpoint).send().await {
            Ok(res) if res.status().is_success() => "HEALTHY",
            _ => "UNAVAILABLE",
        };

        persistence::update_provider_health(&self.pool, workspace_id, id, health_status).await?;
        persistence::get_provider_connection_full(&self.pool, workspace_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn activate_company(
        &self,
        workspace_id: &WorkspaceId,
        company_id: &CompanyId,
        expected_version: i64,
        principal: PrincipalRef,
    ) -> Result<nalarvo_domain::Company, ApplicationError> {
        let mut company = persistence::get_company(&self.pool, workspace_id, company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(company_id.0.clone()))?;
        let correlation_id = Uuid::now_v7().to_string();
        if company.director_user_id.is_none() {
            company.director_user_id = Some(UserId(principal.principal_id.clone()));
        }
        company.activate()?;
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyActivated".into(),
            schema_version: 1,
            company_id: company.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: company.id.0.clone(),
            aggregate_version: company.row_version,
            occurred_at: company.updated_at,
            correlation_id: correlation_id.clone(),
            causation_id: correlation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(company.id.0.clone()),
            payload: serde_json::json!({"company_id": company.id.0, "status": company.status.to_string()}),
        };
        let mut tx = self.pool.begin().await?;
        persistence::update_company_tx(&mut tx, &company, expected_version).await?;
        persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "CompanyActivated".into(),
            principal: principal.clone(),
            scope: ScopeRef::company(company_id.0.clone()),
            occurred_at: chrono::Utc::now(),
            details: serde_json::json!({"company_id": company_id.0}),
        });
        let _ = self.event_broadcaster.send(event);
        Ok(company)
    }

    pub async fn pause_company(
        &self,
        workspace_id: &WorkspaceId,
        company_id: &CompanyId,
        expected_version: i64,
        principal: PrincipalRef,
    ) -> Result<nalarvo_domain::Company, ApplicationError> {
        let mut company = persistence::get_company(&self.pool, workspace_id, company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(company_id.0.clone()))?;
        let correlation_id = Uuid::now_v7().to_string();
        company.pause()?;
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyPaused".into(),
            schema_version: 1,
            company_id: company.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: company.id.0.clone(),
            aggregate_version: company.row_version,
            occurred_at: company.updated_at,
            correlation_id: correlation_id.clone(),
            causation_id: correlation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(company.id.0.clone()),
            payload: serde_json::json!({"company_id": company.id.0, "status": company.status.to_string()}),
        };
        let mut tx = self.pool.begin().await?;
        persistence::update_company_tx(&mut tx, &company, expected_version).await?;
        persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        let _ = self.event_broadcaster.send(event);
        Ok(company)
    }

    pub async fn resume_company(
        &self,
        workspace_id: &WorkspaceId,
        company_id: &CompanyId,
        expected_version: i64,
        principal: PrincipalRef,
    ) -> Result<nalarvo_domain::Company, ApplicationError> {
        let mut company = persistence::get_company(&self.pool, workspace_id, company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(company_id.0.clone()))?;
        let correlation_id = Uuid::now_v7().to_string();
        company.resume()?;
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyResumed".into(),
            schema_version: 1,
            company_id: company.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: company.id.0.clone(),
            aggregate_version: company.row_version,
            occurred_at: company.updated_at,
            correlation_id: correlation_id.clone(),
            causation_id: correlation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(company.id.0.clone()),
            payload: serde_json::json!({"company_id": company.id.0, "status": company.status.to_string()}),
        };
        let mut tx = self.pool.begin().await?;
        persistence::update_company_tx(&mut tx, &company, expected_version).await?;
        persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        let _ = self.event_broadcaster.send(event);
        Ok(company)
    }

    pub async fn archive_company(
        &self,
        workspace_id: &WorkspaceId,
        company_id: &CompanyId,
        expected_version: i64,
        principal: PrincipalRef,
    ) -> Result<nalarvo_domain::Company, ApplicationError> {
        let mut company = persistence::get_company(&self.pool, workspace_id, company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(company_id.0.clone()))?;
        let correlation_id = Uuid::now_v7().to_string();
        company.archive()?;
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyArchived".into(),
            schema_version: 1,
            company_id: company.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: company.id.0.clone(),
            aggregate_version: company.row_version,
            occurred_at: company.updated_at,
            correlation_id: correlation_id.clone(),
            causation_id: correlation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(company.id.0.clone()),
            payload: serde_json::json!({"company_id": company.id.0, "status": company.status.to_string()}),
        };
        let mut tx = self.pool.begin().await?;
        persistence::update_company_tx(&mut tx, &company, expected_version).await?;
        persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        let _ = self.event_broadcaster.send(event);
        Ok(company)
    }

    pub async fn list_departments(
        &self,
        company_id: &CompanyId,
    ) -> Result<Vec<DepartmentRecord>, ApplicationError> {
        Ok(persistence::list_departments_full(&self.pool, company_id).await?)
    }

    pub async fn create_department(
        &self,
        company_id: &CompanyId,
        name: &str,
    ) -> Result<DepartmentRecord, ApplicationError> {
        let id = Uuid::now_v7().to_string();
        persistence::create_department(&self.pool, company_id, &id, name).await?;
        persistence::get_department(&self.pool, company_id, &id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id))
    }

    pub async fn get_department(
        &self,
        company_id: &CompanyId,
        id: &str,
    ) -> Result<DepartmentRecord, ApplicationError> {
        persistence::get_department(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn update_department(
        &self,
        company_id: &CompanyId,
        id: &str,
        name: &str,
        expected_version: i64,
    ) -> Result<DepartmentRecord, ApplicationError> {
        persistence::update_department(&self.pool, company_id, id, name, expected_version).await?;
        persistence::get_department(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn retire_department(
        &self,
        company_id: &CompanyId,
        id: &str,
    ) -> Result<(), ApplicationError> {
        Ok(persistence::retire_department(&self.pool, company_id, id).await?)
    }

    pub async fn list_roles(
        &self,
        company_id: &CompanyId,
    ) -> Result<Vec<RoleRecord>, ApplicationError> {
        Ok(persistence::list_roles_full(&self.pool, company_id).await?)
    }

    pub async fn create_role(
        &self,
        company_id: &CompanyId,
        name: &str,
    ) -> Result<RoleRecord, ApplicationError> {
        let id = Uuid::now_v7().to_string();
        persistence::create_role(&self.pool, company_id, &id, name).await?;
        persistence::get_role(&self.pool, company_id, &id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id))
    }

    pub async fn get_role(
        &self,
        company_id: &CompanyId,
        id: &str,
    ) -> Result<RoleRecord, ApplicationError> {
        persistence::get_role(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn update_role(
        &self,
        company_id: &CompanyId,
        id: &str,
        name: &str,
        expected_version: i64,
    ) -> Result<RoleRecord, ApplicationError> {
        persistence::update_role(&self.pool, company_id, id, name, expected_version).await?;
        persistence::get_role(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn list_agents(
        &self,
        company_id: &CompanyId,
    ) -> Result<Vec<AgentRecord>, ApplicationError> {
        Ok(persistence::list_agents_full(&self.pool, company_id).await?)
    }

    pub async fn create_agent(
        &self,
        company_id: &CompanyId,
        name: &str,
        primary_department_id: &str,
        role_id: &str,
        model_profile_id: Option<&str>,
        capacity: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        if persistence::get_department(&self.pool, company_id, primary_department_id)
            .await?
            .is_none()
        {
            return Err(ApplicationError::NotFound(
                primary_department_id.to_string(),
            ));
        }
        if persistence::get_role(&self.pool, company_id, role_id)
            .await?
            .is_none()
        {
            return Err(ApplicationError::NotFound(role_id.to_string()));
        }

        let id = Uuid::now_v7().to_string();
        // ensure department_role link exists before insert trigger fires
        let _ = persistence::assign_department_role(
            &self.pool,
            company_id,
            primary_department_id,
            role_id,
        )
        .await;
        persistence::create_agent(
            &self.pool,
            company_id,
            &id,
            name,
            primary_department_id,
            role_id,
            model_profile_id,
            capacity,
        )
        .await?;
        persistence::get_agent_full(&self.pool, company_id, &id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id))
    }

    pub async fn get_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
    ) -> Result<AgentRecord, ApplicationError> {
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
        name: &str,
        primary_department_id: &str,
        role_id: &str,
        model_profile_id: Option<&str>,
        capacity: i64,
        expected_version: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        if persistence::get_department(&self.pool, company_id, primary_department_id)
            .await?
            .is_none()
        {
            return Err(ApplicationError::NotFound(
                primary_department_id.to_string(),
            ));
        }
        if persistence::get_role(&self.pool, company_id, role_id)
            .await?
            .is_none()
        {
            return Err(ApplicationError::NotFound(role_id.to_string()));
        }

        let _ = persistence::assign_department_role(
            &self.pool,
            company_id,
            primary_department_id,
            role_id,
        )
        .await;
        persistence::update_agent(
            &self.pool,
            company_id,
            id,
            name,
            primary_department_id,
            role_id,
            model_profile_id,
            capacity,
            expected_version,
        )
        .await?;
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn activate_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
        expected_version: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        persistence::update_agent_status(&self.pool, company_id, id, "ACTIVE", expected_version)
            .await?;
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn pause_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
        expected_version: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        persistence::update_agent_status(&self.pool, company_id, id, "PAUSED", expected_version)
            .await?;
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn resume_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
        expected_version: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        persistence::update_agent_status(&self.pool, company_id, id, "ACTIVE", expected_version)
            .await?;
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn retire_agent(
        &self,
        company_id: &CompanyId,
        id: &str,
        expected_version: i64,
    ) -> Result<AgentRecord, ApplicationError> {
        persistence::update_agent_status(&self.pool, company_id, id, "RETIRED", expected_version)
            .await?;
        persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn get_agent_availability(
        &self,
        company_id: &CompanyId,
        id: &str,
    ) -> Result<String, ApplicationError> {
        let agent = persistence::get_agent_full(&self.pool, company_id, id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;
        // ponytail: current_allocations hardcoded to 0; real count comes from M3 allocations table
        let availability = match agent.status.as_str() {
            "ACTIVE" => "AVAILABLE",
            "PAUSED" => "UNAVAILABLE",
            _ => "UNAVAILABLE",
        };
        Ok(availability.into())
    }
}

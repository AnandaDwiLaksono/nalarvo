use crate::secret_store::{CredentialRef, SecretStore, SecretValue};
use crate::{ApplicationContext, ApplicationError, CommandMeta, ProposeActionCommand};
use nalarvo_domain::{AuthorityDecision, PrincipalRef};
use nalarvo_persistence::get_credential_ref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CredentialBrokerRequest {
    pub workspace_id: String,
    pub company_id: String,
    pub principal: PrincipalRef,
    pub credential_ref_id: String,
    pub model_profile_id: String,
    pub action: String,
    pub tool: String,
}

#[derive(Clone)]
pub struct CredentialBroker {
    app: ApplicationContext,
    secret_store: Arc<dyn SecretStore>,
}

impl CredentialBroker {
    pub(crate) fn new(app: ApplicationContext, secret_store: Arc<dyn SecretStore>) -> Self {
        Self { app, secret_store }
    }

    pub async fn resolve_for_core(
        &self,
        req: CredentialBrokerRequest,
    ) -> Result<SecretValue, ApplicationError> {
        if req.action != "credential.use"
            || !matches!(req.tool.as_str(), "provider.call" | "model.invoke")
        {
            self.app
                .record_audit(nalarvo_domain::AuditRecord {
                    audit_id: uuid::Uuid::now_v7().to_string(),
                    action: "CredentialUseDenied".into(),
                    principal: req.principal.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&req.company_id),
                    occurred_at: chrono::Utc::now(),
                    details: serde_json::json!({
                        "reason": "invalid_action_or_tool",
                        "credential_ref_id": req.credential_ref_id,
                        "model_profile_id": req.model_profile_id,
                    }),
                })
                .await?;
            return Err(ApplicationError::Unauthorized);
        }
        let ws = nalarvo_domain::WorkspaceId(req.workspace_id.clone());
        let cred = get_credential_ref(&self.app.pool, &ws, &req.credential_ref_id)
            .await
            .map_err(ApplicationError::Persistence)?
            .ok_or_else(|| ApplicationError::NotFound(req.credential_ref_id.clone()))?;

        if cred.status != "ACTIVE" {
            self.app
                .record_audit(nalarvo_domain::AuditRecord {
                    audit_id: uuid::Uuid::now_v7().to_string(),
                    action: "CredentialUseDenied".into(),
                    principal: req.principal.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&req.company_id),
                    occurred_at: chrono::Utc::now(),
                    details: serde_json::json!({
                        "reason": "inactive_credential_status",
                        "credential_ref_id": req.credential_ref_id,
                        "status": cred.status,
                    }),
                })
                .await?;
            return Err(ApplicationError::Unauthorized);
        }

        // A profile grant is valid only when its persisted profile resolves to this credential.
        let grant_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM company_workspace_resource_grants g
             JOIN companies c ON c.id = g.company_id AND c.workspace_id = g.workspace_id
             JOIN workspaces w ON w.id=c.workspace_id AND w.status='ACTIVE'
             JOIN model_profiles mp ON mp.id = g.model_profile_id AND mp.workspace_id = g.workspace_id
             JOIN model_profile_versions pv ON pv.profile_id = mp.id AND pv.workspace_id = mp.workspace_id AND pv.version = mp.current_version
             JOIN models m ON m.id = pv.model_id AND m.workspace_id = pv.workspace_id
             JOIN provider_connections pc ON pc.id = m.provider_connection_id AND pc.workspace_id = m.workspace_id
             WHERE g.company_id = ? AND g.workspace_id = ? AND g.model_profile_id = ?
               AND pc.credential_ref_id = ? AND c.status='ACTIVE' AND mp.status='ACTIVE' AND m.status='ACTIVE' AND pc.status='ENABLED'"
        )
        .bind(&req.company_id)
        .bind(&req.workspace_id)
        .bind(&req.model_profile_id)
        .bind(&req.credential_ref_id)
        .fetch_one(&self.app.pool)
        .await?;

        if grant_count == 0 {
            self.app
                .record_audit(nalarvo_domain::AuditRecord {
                    audit_id: uuid::Uuid::now_v7().to_string(),
                    action: "CredentialUseDenied".into(),
                    principal: req.principal.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&req.company_id),
                    occurred_at: chrono::Utc::now(),
                    details: serde_json::json!({
                        "reason": "missing_resource_grant",
                        "credential_ref_id": req.credential_ref_id,
                        "model_profile_id": req.model_profile_id,
                        "company_id": req.company_id,
                    }),
                })
                .await?;
            return Err(ApplicationError::Unauthorized);
        }

        let action_cmd = ProposeActionCommand {
            company_id: req.company_id.clone(),
            project_id: None,
            run_id: None,
            step_id: None,
            actor: req.principal.clone(),
            action_type: req.action.clone(),
            target: req.credential_ref_id.clone(),
            parameters: Some(serde_json::json!({
                "model_profile_id": req.model_profile_id,
                "tool": req.tool,
            })),
            idempotency_key: None,
            meta: CommandMeta {
                principal: Some(req.principal.clone()),
                idempotency_key: None,
                correlation_id: None,
                causation_id: None,
            },
        };

        let decision = self.app.evaluate_action_authority(&action_cmd).await?;
        if decision != AuthorityDecision::Allow {
            self.app
                .record_audit(nalarvo_domain::AuditRecord {
                    audit_id: uuid::Uuid::now_v7().to_string(),
                    action: "CredentialUseDenied".into(),
                    principal: req.principal.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&req.company_id),
                    occurred_at: chrono::Utc::now(),
                    details: serde_json::json!({
                        "reason": "authority_denied",
                        "decision": format!("{:?}", decision),
                        "credential_ref_id": req.credential_ref_id,
                    }),
                })
                .await?;
            return Err(ApplicationError::Unauthorized);
        }

        // ponytail: native tool execution remains in runtime; this seam provides trusted resolution
        let secret = self
            .secret_store
            .get(&CredentialRef::new(&req.credential_ref_id))?;

        self.app
            .record_audit(nalarvo_domain::AuditRecord {
                audit_id: uuid::Uuid::now_v7().to_string(),
                action: "CredentialUsed".into(),
                principal: req.principal.clone(),
                scope: nalarvo_domain::ScopeRef::company(&req.company_id),
                occurred_at: chrono::Utc::now(),
                details: serde_json::json!({
                    "credential_ref_id": req.credential_ref_id,
                    "model_profile_id": req.model_profile_id,
                    "action": req.action,
                    "tool": req.tool,
                }),
            })
            .await?;

        Ok(secret)
    }

    pub async fn dispatch_core_tool_test_seam(
        &self,
        req: CredentialBrokerRequest,
    ) -> Result<bool, ApplicationError> {
        let secret = self.resolve_for_core(req).await?;
        let present = !secret.expose().is_empty();
        Ok(present)
    }
}

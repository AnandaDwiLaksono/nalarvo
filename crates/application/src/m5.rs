//! M5 application governance service implementation.

use crate::{ApplicationContext, ApplicationError, CommandMeta};
use chrono::Utc;
use nalarvo_domain::{
    AuthorityDecision, DomainError, GovernanceContext, GrantEffect, PermissionGrant, Policy,
    PolicyEffect, PolicyVersion, PrincipalRef, evaluate_authority,
};
use nalarvo_persistence::m5::{ActionRecord, ApprovalDecisionRecord, ApprovalRequestRecord};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyRules {
    permission: String,
    effect: PolicyEffect,
}

// ponytail: no per-grant accounting; reject every restriction until atomic enforcement exists.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantConstraints {}

#[derive(Debug, Clone)]
pub struct ProposeActionCommand {
    pub company_id: String,
    pub project_id: Option<String>,
    pub run_id: Option<String>,
    pub step_id: Option<String>,
    pub actor: PrincipalRef,
    pub action_type: String,
    pub target: String,
    pub parameters: Option<serde_json::Value>,
    pub idempotency_key: Option<String>,
    pub meta: CommandMeta,
}

#[derive(Debug, Clone)]
pub struct ProposedActionOutcome {
    pub action_id: String,
    pub fingerprint: String,
    pub decision: AuthorityDecision,
    pub approval_request_id: Option<String>,
}

impl ApplicationContext {
    pub fn compute_action_fingerprint(
        company_id: &str,
        project_id: Option<&str>,
        action_type: &str,
        target: &str,
        parameters: Option<&serde_json::Value>,
    ) -> String {
        let mut hasher = Sha256::new();
        let mut material = parameters.cloned();
        if let Some(serde_json::Value::Object(ref mut p)) = material {
            p.remove("worker_principal_id");
        }
        hasher.update(
            serde_json::json!([company_id, project_id, action_type, target, material])
                .to_string()
                .as_bytes(),
        );
        hex::encode(hasher.finalize())
    }

    pub async fn evaluate_action_authority(
        &self,
        cmd: &ProposeActionCommand,
    ) -> Result<AuthorityDecision, ApplicationError> {
        let mut connection = self.pool.acquire().await?;
        Self::evaluate_action_authority_connection(&mut connection, cmd).await
    }

    async fn evaluate_action_authority_connection(
        connection: &mut sqlx::SqliteConnection,
        cmd: &ProposeActionCommand,
    ) -> Result<AuthorityDecision, ApplicationError> {
        Self::evaluate_action_authority_at(connection, cmd, Utc::now()).await
    }

    async fn evaluate_action_authority_at(
        connection: &mut sqlx::SqliteConnection,
        cmd: &ProposeActionCommand,
        now: chrono::DateTime<Utc>,
    ) -> Result<AuthorityDecision, ApplicationError> {
        let workspace: String = sqlx::query_scalar("SELECT c.workspace_id FROM companies c JOIN workspaces w ON w.id=c.workspace_id WHERE c.id=? AND c.status='ACTIVE' AND w.status='ACTIVE'")
            .bind(&cmd.company_id).fetch_optional(&mut *connection).await?
            .ok_or(ApplicationError::Unauthorized)?;
        if let Some(project) = &cmd.project_id {
            let valid: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM projects WHERE id=? AND company_id=? AND status='ACTIVE'",
            )
            .bind(project)
            .bind(&cmd.company_id)
            .fetch_one(&mut *connection)
            .await?;
            if valid != 1 {
                return Err(ApplicationError::Unauthorized);
            }
        }
        if let Some(run) = &cmd.run_id {
            let staffed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs r JOIN assignments a ON a.company_id=r.company_id AND a.project_id=r.project_id AND a.work_item_id=r.work_item_id AND a.agent_id=r.executing_agent_id AND a.status='ACTIVE' JOIN agent_allocations al ON al.id=a.agent_allocation_id AND al.company_id=a.company_id AND al.project_id=a.project_id AND al.agent_id=a.agent_id AND al.status='ACTIVE' JOIN teams t ON t.id=al.team_id AND t.company_id=al.company_id AND t.project_id=al.project_id AND t.status='ACTIVE' WHERE r.id=? AND r.company_id=?")
                .bind(run).bind(&cmd.company_id).fetch_one(&mut *connection).await?;
            if staffed == 0 {
                return Err(ApplicationError::Unauthorized);
            }
        }
        let principal_type_str = cmd.actor.principal_type.to_string();
        let grants_rows = sqlx::query(
            "SELECT pg.*, pd.workspace_id AS definition_workspace, pd.name as permission_name, pd.operation FROM permission_grants pg LEFT JOIN permission_definitions pd ON pg.permission_definition_id = pd.id WHERE pg.company_id = ? AND pg.principal_type = ? AND pg.principal_id = ? AND pg.revoked_at IS NULL"
        )
        .bind(&cmd.company_id)
        .bind(&principal_type_str)
        .bind(&cmd.actor.principal_id)
        .fetch_all(&mut *connection)
        .await?;

        let mut grants = Vec::new();
        for row in grants_rows {
            let invalid_scope = || {
                ApplicationError::Validation("Invalid permission grant Workspace or scope".into())
            };
            if row.get::<String, _>("workspace_id") != workspace
                || row
                    .try_get::<Option<String>, _>("definition_workspace")?
                    .as_deref()
                    != Some(workspace.as_str())
            {
                return Err(invalid_scope());
            }
            let scope_type: String = row.get("scope_type");
            let scope_id: String = row.get("scope_id");
            let applies = match scope_type.to_ascii_uppercase().as_str() {
                "COMPANY" if scope_id == cmd.company_id => true,
                "PROJECT" => {
                    let owned: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM projects WHERE id=? AND company_id=?",
                    )
                    .bind(&scope_id)
                    .bind(&cmd.company_id)
                    .fetch_one(&mut *connection)
                    .await?;
                    if owned != 1 {
                        return Err(invalid_scope());
                    }
                    cmd.project_id.as_deref() == Some(scope_id.as_str())
                }
                _ => return Err(invalid_scope()),
            };
            if let Some(constraints) = row.try_get::<Option<String>, _>("constraints")? {
                let invalid = || {
                    ApplicationError::Validation(format!(
                        "Invalid or unsupported permission grant constraints: {}",
                        row.get::<String, _>("id")
                    ))
                };
                if !constraints.trim_start().starts_with('{') {
                    return Err(invalid());
                }
                let _: GrantConstraints =
                    serde_json::from_str(&constraints).map_err(|_| invalid())?;
            }
            let invalid = || {
                ApplicationError::Validation(format!(
                    "Invalid permission grant validity or definition: {}",
                    row.get::<String, _>("id")
                ))
            };
            let parse = |field| -> Result<Option<chrono::DateTime<Utc>>, ApplicationError> {
                row.try_get::<Option<String>, _>(field)?
                    .map(|value| {
                        chrono::DateTime::parse_from_rfc3339(&value)
                            .map(|time| time.with_timezone(&Utc))
                            .map_err(|_| invalid())
                    })
                    .transpose()
            };
            let start = parse("starts_at")?;
            let expiry = parse("expires_at")?;
            if start.zip(expiry).is_some_and(|(start, end)| start >= end) {
                return Err(invalid());
            }
            let effect_str: String = row.get("effect");
            let effect = match effect_str.as_str() {
                "ALLOW" => GrantEffect::Allow,
                "DENY" => GrantEffect::Deny,
                _ => return Err(invalid()),
            };
            let perm_name = row
                .try_get::<Option<String>, _>("permission_name")?
                .ok_or_else(invalid)?;
            let op = row
                .try_get::<Option<String>, _>("operation")?
                .ok_or_else(invalid)?;
            // Validate every applicable record before filtering; invalid DENY must not disappear.
            if !applies
                || start.is_some_and(|start| now < start)
                || expiry.is_some_and(|end| now >= end)
            {
                continue;
            }
            let permission = if perm_name.contains('.') {
                perm_name
            } else {
                op
            };
            grants.push(PermissionGrant {
                id: row.get("id"),
                workspace_id: row.get("workspace_id"),
                company_id: row.get("company_id"),
                permission_definition_id: row.get("permission_definition_id"),
                permission,
                principal_type: row.get("principal_type"),
                principal_id: row.get("principal_id"),
                scope_type: row.get("scope_type"),
                scope_id: row.get("scope_id"),
                effect,
                created_at: row.get("created_at"),
            });
        }

        let policy_rows = sqlx::query(
            "SELECT p.id, p.workspace_id, p.status, pv.version, pv.rules FROM policies p LEFT JOIN policy_versions pv ON p.id = pv.policy_id AND p.current_version = pv.version WHERE p.company_id = ? OR (p.company_id IS NULL AND p.workspace_id=?)"
        )
        .bind(&cmd.company_id)
        .bind(&workspace)
        .fetch_all(&mut *connection)
        .await?;

        let mut policies = Vec::new();
        for row in policy_rows {
            let id: String = row.get("id");
            let invalid =
                || ApplicationError::Validation(format!("Invalid policy configuration: {id}"));
            if row.get::<String, _>("workspace_id") != workspace {
                return Err(invalid());
            }
            match row.get::<String, _>("status").as_str() {
                "ACTIVE" => {}
                "DRAFT" | "INACTIVE" | "DISABLED" | "ARCHIVED" => continue,
                _ => return Err(invalid()),
            }
            let version = row
                .try_get::<Option<i64>, _>("version")?
                .and_then(|v| u64::try_from(v).ok())
                .filter(|v| *v > 0)
                .ok_or_else(invalid)?;
            let rules: String = row
                .try_get::<Option<String>, _>("rules")?
                .ok_or_else(invalid)?;
            // Validate before applicability filtering: an invalid selector cannot prove irrelevance.
            // Serde structs also accept positional arrays; policy configuration must be an object.
            if !rules.trim_start().starts_with('{') {
                return Err(invalid());
            }
            let parsed: PolicyRules = serde_json::from_str(&rules).map_err(|_| invalid())?;
            if !nalarvo_domain::CANONICAL_PERMISSION_DEFINITIONS
                .iter()
                .any(|(permission, ..)| *permission == parsed.permission)
            {
                return Err(invalid());
            }
            policies.push(Policy {
                id,
                version: PolicyVersion { version },
                permission: parsed.permission,
                effect: parsed.effect,
                company_rule: false,
            });
        }

        let context = GovernanceContext {
            company_id: cmd.company_id.clone(),
            actor: cmd.actor.clone(),
            action: cmd.action_type.clone(),
            resource: cmd.target.clone(),
        };

        Ok(evaluate_authority(&context, &grants, &policies))
    }

    pub async fn propose_action(
        &self,
        cmd: ProposeActionCommand,
    ) -> Result<ProposedActionOutcome, ApplicationError> {
        match self.propose_action_once(cmd.clone()).await {
            Err(ApplicationError::Database(error))
                if error
                    .as_database_error()
                    .is_some_and(|database| database.is_unique_violation()) =>
            {
                self.propose_action_once(cmd).await
            }
            result => result,
        }
    }

    async fn propose_action_once(
        &self,
        cmd: ProposeActionCommand,
    ) -> Result<ProposedActionOutcome, ApplicationError> {
        if let Some(run_id) = &cmd.run_id {
            let step_id = cmd
                .step_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    ApplicationError::Validation(
                        "Runtime Action requires a canonical ExecutionStep".into(),
                    )
                })?;
            if cmd.idempotency_key.as_deref().is_none_or(str::is_empty) {
                return Err(ApplicationError::Validation(
                    "Runtime Action requires stable idempotency".into(),
                ));
            }
            let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs r JOIN execution_steps s ON s.run_id=r.id AND s.company_id=r.company_id WHERE r.id=? AND r.company_id=? AND r.project_id=? AND s.id=?")
                .bind(run_id).bind(&cmd.company_id).bind(&cmd.project_id).bind(step_id).fetch_one(&self.pool).await?;
            if valid != 1 {
                return Err(ApplicationError::Validation(
                    "Runtime Action Run/Project/Step binding is invalid".into(),
                ));
            }
        }
        if let Some(ref ikey) = cmd.idempotency_key {
            let existing = sqlx::query(
                "SELECT a.id, a.action_type, a.target, a.input, a.authorization, a.status,
                        (SELECT r.project_id FROM runs r WHERE r.id = a.run_id) AS run_project_id,
                        (SELECT e.payload FROM domain_events e WHERE e.aggregate_id = a.id AND e.event_type = 'ActionProposed' ORDER BY e.rowid LIMIT 1) AS proposed_payload
                 FROM actions a WHERE a.company_id = ? AND a.idempotency_key = ?"
            )
            .bind(&cmd.company_id)
            .bind(ikey)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = existing {
                let existing_id: String = row.get("id");
                let existing_auth: String = row.get("authorization");
                let existing_input: Option<String> = row.get("input");
                let existing_params = existing_input.and_then(|i| serde_json::from_str(&i).ok());
                let run_project_id: Option<String> = row.get("run_project_id");
                let proposed_payload_raw: Option<String> = row.get("proposed_payload");
                let proposed_payload: Option<serde_json::Value> = proposed_payload_raw
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok());

                let existing_project_id = proposed_payload
                    .as_ref()
                    .and_then(|p| p.get("project_id").and_then(|v| v.as_str()))
                    .or(run_project_id.as_deref());

                let existing_fingerprint = proposed_payload
                    .as_ref()
                    .and_then(|p| {
                        p.get("fingerprint")
                            .and_then(|v| v.as_str().map(String::from))
                    })
                    .unwrap_or_else(|| {
                        Self::compute_action_fingerprint(
                            &cmd.company_id,
                            existing_project_id,
                            &row.get::<String, _>("action_type"),
                            &row.get::<String, _>("target"),
                            existing_params.as_ref(),
                        )
                    });

                let current_fingerprint = Self::compute_action_fingerprint(
                    &cmd.company_id,
                    cmd.project_id.as_deref(),
                    &cmd.action_type,
                    &cmd.target,
                    cmd.parameters.as_ref(),
                );

                if existing_fingerprint == current_fingerprint {
                    let existing_approval_id: Option<String> =
                        sqlx::query_scalar("SELECT id FROM approval_requests WHERE action_id = ?")
                            .bind(&existing_id)
                            .fetch_optional(&self.pool)
                            .await?;

                    let decision = match existing_auth.as_str() {
                        "APPROVAL_REQUIRED" => AuthorityDecision::RequireApproval,
                        "ALLOWED" | "APPROVED" => AuthorityDecision::Allow,
                        _ => AuthorityDecision::Deny,
                    };

                    return Ok(ProposedActionOutcome {
                        action_id: existing_id,
                        fingerprint: existing_fingerprint,
                        decision,
                        approval_request_id: existing_approval_id,
                    });
                } else {
                    return Err(ApplicationError::IdempotencyKeyReuseMismatch(format!(
                        "Idempotency key reuse mismatch for key {ikey}"
                    )));
                }
            }
        }

        let decision = self.evaluate_action_authority(&cmd).await?;
        let workspace_id: String =
            sqlx::query_scalar("SELECT workspace_id FROM companies WHERE id=?")
                .bind(&cmd.company_id)
                .fetch_one(&self.pool)
                .await?;
        let action_id = format!("act_{}", Uuid::now_v7());
        let fingerprint = Self::compute_action_fingerprint(
            &cmd.company_id,
            cmd.project_id.as_deref(),
            &cmd.action_type,
            &cmd.target,
            cmd.parameters.as_ref(),
        );
        let now = Utc::now().to_rfc3339();

        match decision {
            AuthorityDecision::RequireApproval => {
                let approval_id = format!("appr_{}", Uuid::now_v7());
                let action_rec = ActionRecord {
                    id: action_id.clone(),
                    workspace_id: workspace_id.clone(),
                    company_id: cmd.company_id.clone(),
                    run_id: cmd.run_id.clone(),
                    step_id: cmd.step_id.clone(),
                    requested_by_type: cmd.actor.principal_type.to_string(),
                    requested_by_id: cmd.actor.principal_id.clone(),
                    action_type: cmd.action_type.clone(),
                    target: cmd.target.clone(),
                    input: cmd.parameters.as_ref().map(|p| p.to_string()),
                    authorization: "APPROVAL_REQUIRED".into(),
                    status: "PENDING".into(),
                    idempotency_key: cmd.idempotency_key.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                };
                let approval_rec = ApprovalRequestRecord {
                    id: approval_id.clone(),
                    action_id: action_id.clone(),
                    workspace_id: workspace_id.clone(),
                    company_id: cmd.company_id.clone(),
                    requested_by_id: cmd.actor.principal_id.clone(),
                    status: "PENDING".into(),
                    reason: Some("Governance policy requires approval".into()),
                    requested_at: now,
                };

                let correlation_id = cmd
                    .meta
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| Uuid::now_v7().to_string());
                let causation_id = cmd
                    .meta
                    .causation_id
                    .clone()
                    .unwrap_or_else(|| correlation_id.clone());

                let mut tx = self.pool.begin().await?;
                sqlx::query(
                    "INSERT INTO actions (
                        id, workspace_id, company_id, run_id, step_id,
                        requested_by_type, requested_by_id, action_type, target,
                        input, authorization, status, idempotency_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&action_rec.id)
                .bind(&action_rec.workspace_id)
                .bind(&action_rec.company_id)
                .bind(&action_rec.run_id)
                .bind(&action_rec.step_id)
                .bind(&action_rec.requested_by_type)
                .bind(&action_rec.requested_by_id)
                .bind(&action_rec.action_type)
                .bind(&action_rec.target)
                .bind(&action_rec.input)
                .bind(&action_rec.authorization)
                .bind(&action_rec.status)
                .bind(&action_rec.idempotency_key)
                .bind(&action_rec.created_at)
                .bind(&action_rec.updated_at)
                .execute(&mut *tx)
                .await?;

                sqlx::query(
                    "INSERT INTO approval_requests (
                        id, action_id, workspace_id, company_id, requested_by_id, status, reason, requested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&approval_rec.id)
                .bind(&approval_rec.action_id)
                .bind(&approval_rec.workspace_id)
                .bind(&approval_rec.company_id)
                .bind(&approval_rec.requested_by_id)
                .bind(&approval_rec.status)
                .bind(&approval_rec.reason)
                .bind(&approval_rec.requested_at)
                .execute(&mut *tx)
                .await?;

                let proposed_event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionProposed".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id: causation_id.clone(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &proposed_event)
                    .await?;
                Self::record_authority_tx(
                    &mut tx,
                    &cmd,
                    &action_id,
                    &workspace_id,
                    "REQUIRE_APPROVAL",
                )
                .await?;
                let event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionApprovalRequired".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id,
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "approval_id": approval_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
                let requested = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ApprovalRequested".into(),
                    aggregate_type: "Approval".into(),
                    aggregate_id: approval_id.clone(),
                    causation_id: event.event_id.clone(),
                    payload: serde_json::json!({"approval_request_id":approval_id,"action_id":action_id,"company_id":cmd.company_id,"project_id":cmd.project_id,"status":"PENDING"}),
                    ..event.clone()
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &requested).await?;
                let audit_record = nalarvo_domain::AuditRecord {
                    audit_id: Uuid::now_v7().to_string(),
                    action: "ActionApprovalRequired".into(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    occurred_at: Utc::now(),
                    details: serde_json::json!({
                        "action_id": action_id,
                        "approval_id": approval_id,
                        "fingerprint": fingerprint,
                        "decision": "REQUIRE_APPROVAL",
                    }),
                };
                self.durable_audit_sink
                    .record_tx(&mut tx, &audit_record)
                    .await?;
                tx.commit().await?;
                self.audit_sink.record(audit_record);

                Ok(ProposedActionOutcome {
                    action_id,
                    fingerprint,
                    decision,
                    approval_request_id: Some(approval_id),
                })
            }
            AuthorityDecision::Allow => {
                let action_rec = ActionRecord {
                    id: action_id.clone(),
                    workspace_id: workspace_id.clone(),
                    company_id: cmd.company_id.clone(),
                    run_id: cmd.run_id.clone(),
                    step_id: cmd.step_id.clone(),
                    requested_by_type: cmd.actor.principal_type.to_string(),
                    requested_by_id: cmd.actor.principal_id.clone(),
                    action_type: cmd.action_type.clone(),
                    target: cmd.target.clone(),
                    input: cmd.parameters.as_ref().map(|p| p.to_string()),
                    authorization: "ALLOWED".into(),
                    status: "PENDING".into(),
                    idempotency_key: cmd.idempotency_key.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                };

                let correlation_id = cmd
                    .meta
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| Uuid::now_v7().to_string());
                let causation_id = cmd
                    .meta
                    .causation_id
                    .clone()
                    .unwrap_or_else(|| correlation_id.clone());

                let mut tx = self.pool.begin().await?;
                sqlx::query(
                    "INSERT INTO actions (
                        id, workspace_id, company_id, run_id, step_id,
                        requested_by_type, requested_by_id, action_type, target,
                        input, authorization, status, idempotency_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&action_rec.id)
                .bind(&action_rec.workspace_id)
                .bind(&action_rec.company_id)
                .bind(&action_rec.run_id)
                .bind(&action_rec.step_id)
                .bind(&action_rec.requested_by_type)
                .bind(&action_rec.requested_by_id)
                .bind(&action_rec.action_type)
                .bind(&action_rec.target)
                .bind(&action_rec.input)
                .bind(&action_rec.authorization)
                .bind(&action_rec.status)
                .bind(&action_rec.idempotency_key)
                .bind(&action_rec.created_at)
                .bind(&action_rec.updated_at)
                .execute(&mut *tx)
                .await?;

                let proposed_event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionProposed".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id: causation_id.clone(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "project_id": cmd.project_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &proposed_event)
                    .await?;
                let event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionAllowed".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id,
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
                Self::record_authority_tx(&mut tx, &cmd, &action_id, &workspace_id, "ALLOW")
                    .await?;
                let audit_record = nalarvo_domain::AuditRecord {
                    audit_id: Uuid::now_v7().to_string(),
                    action: "ActionAllowed".into(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    occurred_at: Utc::now(),
                    details: serde_json::json!({
                        "action_id": action_id,
                        "fingerprint": fingerprint,
                        "decision": "ALLOW",
                    }),
                };
                self.durable_audit_sink
                    .record_tx(&mut tx, &audit_record)
                    .await?;
                tx.commit().await?;
                self.audit_sink.record(audit_record);

                Ok(ProposedActionOutcome {
                    action_id,
                    fingerprint,
                    decision,
                    approval_request_id: None,
                })
            }
            AuthorityDecision::Deny => {
                let action_rec = ActionRecord {
                    id: action_id.clone(),
                    workspace_id: workspace_id.clone(),
                    company_id: cmd.company_id.clone(),
                    run_id: cmd.run_id.clone(),
                    step_id: cmd.step_id.clone(),
                    requested_by_type: cmd.actor.principal_type.to_string(),
                    requested_by_id: cmd.actor.principal_id.clone(),
                    action_type: cmd.action_type.clone(),
                    target: cmd.target.clone(),
                    input: cmd.parameters.as_ref().map(|p| p.to_string()),
                    authorization: "DENIED".into(),
                    status: "PENDING".into(),
                    idempotency_key: cmd.idempotency_key.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                };

                let correlation_id = cmd
                    .meta
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| Uuid::now_v7().to_string());
                let causation_id = cmd
                    .meta
                    .causation_id
                    .clone()
                    .unwrap_or_else(|| correlation_id.clone());

                let mut tx = self.pool.begin().await?;
                sqlx::query(
                    "INSERT INTO actions (
                        id, workspace_id, company_id, run_id, step_id,
                        requested_by_type, requested_by_id, action_type, target,
                        input, authorization, status, idempotency_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&action_rec.id)
                .bind(&action_rec.workspace_id)
                .bind(&action_rec.company_id)
                .bind(&action_rec.run_id)
                .bind(&action_rec.step_id)
                .bind(&action_rec.requested_by_type)
                .bind(&action_rec.requested_by_id)
                .bind(&action_rec.action_type)
                .bind(&action_rec.target)
                .bind(&action_rec.input)
                .bind(&action_rec.authorization)
                .bind(&action_rec.status)
                .bind(&action_rec.idempotency_key)
                .bind(&action_rec.created_at)
                .bind(&action_rec.updated_at)
                .execute(&mut *tx)
                .await?;

                let proposed_event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionProposed".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id: causation_id.clone(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "project_id": cmd.project_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &proposed_event)
                    .await?;
                let event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "ActionDenied".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Action".into(),
                    aggregate_id: action_id.clone(),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: correlation_id.clone(),
                    causation_id,
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({
                        "action_id": action_id,
                        "action_type": cmd.action_type,
                        "target": cmd.target,
                        "fingerprint": fingerprint,
                    }),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
                Self::record_authority_tx(&mut tx, &cmd, &action_id, &workspace_id, "DENY").await?;
                let audit_record = nalarvo_domain::AuditRecord {
                    audit_id: Uuid::now_v7().to_string(),
                    action: "ActionDenied".into(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    occurred_at: Utc::now(),
                    details: serde_json::json!({
                        "action_id": action_id,
                        "fingerprint": fingerprint,
                        "decision": "DENY",
                    }),
                };
                self.durable_audit_sink
                    .record_tx(&mut tx, &audit_record)
                    .await?;
                tx.commit().await?;
                self.audit_sink.record(audit_record);

                Ok(ProposedActionOutcome {
                    action_id,
                    fingerprint,
                    decision,
                    approval_request_id: None,
                })
            }
        }
    }

    async fn record_authority_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        cmd: &ProposeActionCommand,
        action_id: &str,
        workspace_id: &str,
        decision: &str,
    ) -> Result<(), ApplicationError> {
        sqlx::query("INSERT INTO authority_decisions(id,workspace_id,company_id,action_id,principal_type,principal_id,decision,evaluated_at) VALUES(?,?,?,?,?,?,?,?)")
            .bind(Uuid::now_v7().to_string()).bind(workspace_id).bind(&cmd.company_id).bind(action_id)
            .bind(cmd.actor.principal_type.to_string()).bind(&cmd.actor.principal_id).bind(decision)
            .bind(Utc::now().to_rfc3339()).execute(&mut **tx).await?;
        sqlx::query("UPDATE actions SET project_id=?, work_item_id=(SELECT work_item_id FROM runs WHERE id=actions.run_id), agent_id=(SELECT executing_agent_id FROM runs WHERE id=actions.run_id), worker_principal_id=?, tool_id=?, tool_version=?, fingerprint=?, correlation_id=?, causation_id=? WHERE id=?")
            .bind(if cmd.run_id.is_some() { cmd.project_id.as_deref() } else { None })
            .bind(cmd.parameters.as_ref().and_then(|p| p["worker_principal_id"].as_str()))
            .bind(cmd.parameters.as_ref().and_then(|p| p["tool_id"].as_str()))
            .bind(cmd.parameters.as_ref().and_then(|p| p["tool_version"].as_str()))
            .bind(Self::compute_action_fingerprint(&cmd.company_id, cmd.project_id.as_deref(), &cmd.action_type, &cmd.target, cmd.parameters.as_ref()))
            .bind(cmd.meta.correlation_id.as_deref().unwrap_or(action_id))
            .bind(cmd.meta.causation_id.as_deref().unwrap_or(action_id)).bind(action_id).execute(&mut **tx).await?;
        Ok(())
    }

    /// Shared live authority and canonical material validation inside the caller's transaction.
    async fn validate_runtime_action_connection(
        &self,
        connection: &mut sqlx::SqliteConnection,
        cmd: &ProposeActionCommand,
        action_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
        run_state: &str,
        check_filesystem_material: bool,
    ) -> Result<sqlx::sqlite::SqliteRow, ApplicationError> {
        let row = sqlx::query("SELECT a.*,r.project_id AS canonical_project,r.work_item_id AS canonical_work_item,r.executing_agent_id AS canonical_agent,r.lifecycle_state AS run_state,p.status AS project_state,p.working_root_path,s.lifecycle_state AS step_state,c.status AS company_state,ag.status AS agent_state,wi.status AS work_state, EXISTS(SELECT 1 FROM assignments ass JOIN agent_allocations alloc ON alloc.id=ass.agent_allocation_id AND alloc.company_id=ass.company_id AND alloc.project_id=ass.project_id AND alloc.agent_id=ass.agent_id JOIN teams team ON team.id=alloc.team_id AND team.company_id=alloc.company_id AND team.project_id=alloc.project_id WHERE ass.company_id=r.company_id AND ass.project_id=r.project_id AND ass.work_item_id=r.work_item_id AND ass.agent_id=r.executing_agent_id AND ass.status='ACTIVE' AND alloc.status='ACTIVE' AND team.status='ACTIVE') AS staffed FROM actions a JOIN runs r ON a.run_id=r.id AND a.company_id=r.company_id JOIN companies c ON c.id=r.company_id AND c.workspace_id=a.workspace_id JOIN projects p ON p.id=r.project_id AND p.company_id=r.company_id JOIN work_items wi ON wi.id=r.work_item_id AND wi.company_id=r.company_id AND wi.project_id=r.project_id JOIN agents ag ON ag.id=r.executing_agent_id AND ag.company_id=r.company_id JOIN execution_steps s ON s.id=a.step_id AND s.run_id=r.id AND s.company_id=r.company_id WHERE a.id=? AND a.company_id=?")
            .bind(action_id).bind(&cmd.company_id).fetch_optional(&mut *connection).await?
            .ok_or_else(|| ApplicationError::Validation("Invalid canonical Action binding".into()))?;
        if !matches!(
            row.get::<String, _>("status").as_str(),
            "PENDING" | "STARTED"
        ) {
            return Ok(row);
        }
        let params = cmd.parameters.as_ref().ok_or_else(|| {
            ApplicationError::Validation("Missing normalized Tool operation".into())
        })?;
        let root: Option<String> = row.get("working_root_path");
        #[cfg(windows)]
        let filesystem = crate::project_filesystem::ProjectFilesystem::open(std::path::Path::new(
            root.as_deref().unwrap_or(""),
        ))
        .map_err(|_| ApplicationError::Validation("Invalid current Project root".into()))?;
        #[cfg(windows)]
        let canonical = filesystem.root.clone();
        #[cfg(not(windows))]
        let canonical = root
            .and_then(|r| std::fs::canonicalize(r).ok())
            .filter(|p| p.is_dir())
            .ok_or_else(|| ApplicationError::Validation("Invalid current Project root".into()))?;
        #[cfg(not(windows))]
        let _ = check_filesystem_material;
        self.validate_project_content_boundary(&canonical)?;
        #[cfg(windows)]
        if check_filesystem_material {
            let binding = if cmd.action_type == "filesystem.write"
                && crate::project_filesystem::components(params["path"].as_str().unwrap_or(""))
                    .is_ok()
            {
                filesystem.write_binding(params["path"].as_str().unwrap_or(""))
            } else {
                filesystem.root_binding()
            }
            .map_err(|_| {
                ApplicationError::Validation("Project filesystem binding denied".into())
            })?;
            if params["filesystem_binding"] != binding {
                return Err(ApplicationError::Validation(
                    "Project filesystem authorization changed".into(),
                ));
            }
        }
        if row.get::<String, _>("run_state") != run_state
            || row.get::<String, _>("company_state") != "ACTIVE"
            || row.get::<String, _>("project_state") != "ACTIVE"
            || row.get::<String, _>("agent_state") != "ACTIVE"
            || row.get::<String, _>("work_state") != "IN_PROGRESS"
            || row.get::<i64, _>("staffed") != 1
            || row
                .get::<Option<String>, _>("canonical_work_item")
                .is_none()
            || row.get::<Option<String>, _>("canonical_agent").is_none()
            || row.get::<String, _>("step_state") != "RUNNING"
            || row.get::<String, _>("canonical_project")
                != cmd.project_id.clone().unwrap_or_default()
            || row.get::<Option<String>, _>("run_id").as_deref() != Some(lease.run_id.as_str())
            || params["root_binding"].as_str() != canonical.to_str()
            || row.get::<Option<String>, _>("fingerprint").as_deref()
                != Some(
                    Self::compute_action_fingerprint(
                        &cmd.company_id,
                        cmd.project_id.as_deref(),
                        &cmd.action_type,
                        &cmd.target,
                        cmd.parameters.as_ref(),
                    )
                    .as_str(),
                )
        {
            return Err(ApplicationError::Validation(
                "Runtime Action material or context changed".into(),
            ));
        }
        let auth: String = row.get("authorization");
        let now = Utc::now();
        let decision = Self::evaluate_action_authority_at(connection, cmd, now).await?;
        if auth == "APPROVED" {
            let expiry: Option<String> = sqlx::query_scalar("SELECT expires_at FROM approval_requests WHERE action_id=? AND company_id=? AND status='APPROVED'")
                .bind(action_id).bind(&cmd.company_id).fetch_optional(&mut *connection).await?
                .ok_or(ApplicationError::Unauthorized)?;
            if let Some(expiry) = expiry {
                let expiry = chrono::DateTime::parse_from_rfc3339(&expiry)
                    .map_err(|_| ApplicationError::Validation("Invalid approval expiry".into()))?;
                if expiry <= now {
                    return Err(ApplicationError::Validation("Approval expired".into()));
                }
            }
        }
        if decision == AuthorityDecision::Deny
            || !matches!(auth.as_str(), "ALLOWED" | "APPROVED")
            || (decision == AuthorityDecision::RequireApproval && auth != "APPROVED")
        {
            return Err(ApplicationError::Unauthorized);
        }
        let provider = params["provider_connection_id"].as_str().ok_or_else(|| {
            ApplicationError::Validation("Tool usage requires canonical provider lineage".into())
        })?;
        let valid_provider: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_connections WHERE id=? AND workspace_id=?",
        )
        .bind(provider)
        .bind(row.get::<String, _>("workspace_id"))
        .fetch_one(&mut *connection)
        .await?;
        if valid_provider != 1 {
            return Err(ApplicationError::Validation(
                "Invalid canonical provider lineage".into(),
            ));
        }
        Ok(row)
    }

    pub async fn resume_approved_runtime_action(
        &self,
        cmd: &ProposeActionCommand,
        action_id: &str,
        approval_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
        expected_run_version: i64,
    ) -> Result<nalarvo_domain::Run, ApplicationError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease).await?;
        let company_id = nalarvo_domain::CompanyId(cmd.company_id.clone());
        let run_state: String =
            sqlx::query_scalar("SELECT lifecycle_state FROM runs WHERE id=? AND company_id=?")
                .bind(&lease.run_id)
                .bind(&company_id.0)
                .fetch_one(&mut *tx)
                .await?;
        if run_state != "WAITING_APPROVAL" && run_state != "QUEUED" {
            return Err(ApplicationError::Validation(
                "Run is not awaiting approved resume".into(),
            ));
        }
        let row = self
            .validate_runtime_action_connection(&mut tx, cmd, action_id, lease, &run_state, true)
            .await?;
        if row.get::<String, _>("status") != "PENDING"
            || row.get::<String, _>("authorization") != "APPROVED"
        {
            return Err(ApplicationError::Unauthorized);
        }
        let bound: i64 = sqlx::query_scalar("SELECT count(*) FROM approval_requests p JOIN runtime_checkpoints c ON c.run_id=? AND c.company_id=p.company_id WHERE p.id=? AND p.action_id=? AND p.status='APPROVED' AND c.run_state='WAITING_APPROVAL' AND c.execution_phase='APPROVAL_PENDING' AND c.safe_to_resume=1 AND json_extract(c.continuation_metadata,'$.action_id')=p.action_id AND json_extract(c.continuation_metadata,'$.approval_id')=p.id AND json_extract(c.continuation_metadata,'$.fingerprint')=? AND json_extract(c.continuation_metadata,'$.idempotency_key')=?")
            .bind(&lease.run_id).bind(approval_id).bind(action_id).bind(row.get::<Option<String>,_>("fingerprint")).bind(&cmd.idempotency_key).fetch_one(&mut *tx).await?;
        if bound != 1 {
            return Err(ApplicationError::Unauthorized);
        }
        let budgets = nalarvo_persistence::m5::applicable_budgets(
            &mut tx,
            &row.get::<String, _>("workspace_id"),
            &cmd.company_id,
            cmd.project_id.as_deref(),
            "TOOL_CALL",
            1.0,
        )
        .await?;
        for budget in budgets {
            if !nalarvo_persistence::m5::budget_allows(&budget, 1.0)? {
                return Err(ApplicationError::Validation("BUDGET_EXHAUSTED".into()));
            }
        }
        let run = nalarvo_persistence::m4::transition_run_tx(
            &mut tx,
            &company_id,
            &lease.run_id,
            expected_run_version,
            nalarvo_domain::RunStatus::Running,
            None,
            None,
        )
        .await?;
        let emitted: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_events WHERE aggregate_id=? AND event_type='RunResumed' AND json_extract(payload,'$.approval_request_id')=?")
            .bind(&run.id).bind(approval_id).fetch_one(&mut *tx).await?;
        if emitted == 0 {
            let event = nalarvo_domain::DomainEvent {
                event_id: Uuid::now_v7().to_string(),
                event_type: "RunResumed".into(),
                schema_version: 1,
                company_id,
                aggregate_type: "Run".into(),
                aggregate_id: run.id.clone(),
                aggregate_version: run.row_version,
                occurred_at: Utc::now(),
                correlation_id: run.correlation_id.clone(),
                causation_id: approval_id.into(),
                principal: cmd.actor.clone(),
                scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                payload: serde_json::json!({"run_id":run.id,"status":"RUNNING","approval_request_id":approval_id,"action_id":action_id,"project_id":run.project_id}),
            };
            nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        }
        tx.commit().await?;
        Ok(run)
    }

    /// Atomic lease/authority/budget gate before invocation and external effect.
    pub async fn start_runtime_action(
        &self,
        cmd: &ProposeActionCommand,
        action_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
    ) -> Result<bool, ApplicationError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease).await?;
        let row = self
            .validate_runtime_action_connection(&mut tx, cmd, action_id, lease, "RUNNING", false)
            .await?;
        if row.get::<String, _>("status") != "PENDING" {
            tx.commit().await?;
            return Ok(false);
        }
        let params = cmd.parameters.as_ref().ok_or_else(|| {
            ApplicationError::Validation("Missing normalized Tool operation".into())
        })?;
        let now = Utc::now().to_rfc3339();
        let budgets = nalarvo_persistence::m5::applicable_budgets(
            &mut tx,
            &row.get::<String, _>("workspace_id"),
            &cmd.company_id,
            cmd.project_id.as_deref(),
            "TOOL_CALL",
            1.0,
        )
        .await?;
        for b in &budgets {
            if !nalarvo_persistence::m5::budget_allows(b, 1.0)? {
                let version: i64 = row.get("row_version");
                sqlx::query("UPDATE actions SET status='REJECTED',result='BUDGET_EXHAUSTED',updated_at=?,row_version=row_version+1 WHERE id=? AND status='PENDING'")
                    .bind(&now).bind(action_id).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO authority_decisions(id,workspace_id,company_id,action_id,principal_type,principal_id,decision,reason,evaluated_at) VALUES(?,?,?,?,?,?,'BUDGET_EXHAUSTED','TOOL_CALL limit',?)")
                    .bind(Uuid::now_v7().to_string()).bind(row.get::<String,_>("workspace_id")).bind(&cmd.company_id).bind(action_id)
                    .bind(cmd.actor.principal_type.to_string()).bind(&cmd.actor.principal_id).bind(&now).execute(&mut *tx).await?;
                self.runtime_action_event_tx(
                    &mut tx,
                    cmd,
                    action_id,
                    "ActionBudgetRejected",
                    version + 1,
                    Some(&serde_json::json!({"failure_class":"BUDGET_EXHAUSTED"})),
                )
                .await?;
                let event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "BudgetExhausted".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
                    aggregate_type: "Budget".into(),
                    aggregate_id: b.get("id"),
                    aggregate_version: 1,
                    occurred_at: Utc::now(),
                    correlation_id: cmd
                        .meta
                        .correlation_id
                        .clone()
                        .unwrap_or_else(|| action_id.into()),
                    causation_id: action_id.into(),
                    principal: cmd.actor.clone(),
                    scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
                    payload: serde_json::json!({"budget_id":b.get::<String,_>("id"),"action_id":action_id,"resource_type":"TOOL_CALL","amount":1,"total":b.get::<f64,_>("total_limit"),"reserved":b.get::<f64,_>("reserved_amount"),"spent":b.get::<f64,_>("spent_amount"),"limit_total":b.get::<Option<f64>,_>("limit_amount")}),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
                self.durable_audit_sink
                    .record_tx(
                        &mut tx,
                        &nalarvo_domain::AuditRecord {
                            audit_id: Uuid::now_v7().to_string(),
                            action: "BudgetExhausted".into(),
                            principal: cmd.actor.clone(),
                            scope: event.scope,
                            occurred_at: event.occurred_at,
                            details: event.payload,
                        },
                    )
                    .await?;
                tx.commit().await?;
                return Err(ApplicationError::Validation("BUDGET_EXHAUSTED".into()));
            }
        }
        // A competing Action can consume the last budget slot and publish while this
        // Action waits. Preserve durable budget rejection before target-material check.
        self.validate_runtime_action_connection(&mut tx, cmd, action_id, lease, "RUNNING", true)
            .await?;
        for b in budgets {
            let budget: String = b.get("id");
            nalarvo_persistence::m5::hold_budget_tx(
                &mut tx,
                &nalarvo_persistence::m5::BudgetReservation {
                    id: format!("tool:{budget}:{action_id}"),
                    budget_id: budget,
                    action_id: Some(action_id.into()),
                    workspace_id: row.get("workspace_id"),
                    company_id: cmd.company_id.clone(),
                    resource_type: "TOOL_CALL".into(),
                    amount: 1.0,
                    expires_at: None,
                    created_at: now.clone(),
                },
            )
            .await?;
        }
        let version: i64 = row.get("row_version");
        let changed = sqlx::query("UPDATE actions SET status='STARTED',started_at=?,updated_at=?,worker_principal_id=?,row_version=row_version+1 WHERE id=? AND status='PENDING' AND row_version=?")
            .bind(&now).bind(&now).bind(&lease.worker_principal_id).bind(action_id).bind(version).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(ApplicationError::StaleVersion {
                current: version + 1,
                expected: version,
            });
        }
        sqlx::query("INSERT INTO tool_invocations(id,action_id,workspace_id,company_id,project_id,run_id,step_id,tool_name,tool_version,idempotency_key,status,request,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,'STARTED',?,?)")
            .bind(format!("tool:{action_id}")).bind(action_id).bind(row.get::<String,_>("workspace_id")).bind(&cmd.company_id).bind(&cmd.project_id).bind(&cmd.run_id).bind(&cmd.step_id)
            .bind(params["tool_id"].as_str()).bind(params["tool_version"].as_str()).bind(&cmd.idempotency_key)
            .bind(serde_json::json!({"fingerprint":row.get::<Option<String>,_>("fingerprint"),"target":cmd.target}).to_string()).bind(&now).execute(&mut *tx).await?;
        self.runtime_action_event_tx(&mut tx, cmd, action_id, "ActionStarted", version + 1, None)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Hold the Core write lock through the synchronous filesystem publication. A
    /// cancellation, lease takeover or Project edit must commit before or after it.
    #[cfg(windows)]
    pub async fn execute_runtime_write(
        &self,
        cmd: &ProposeActionCommand,
        action_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
        write: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| e.to_string())?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease)
            .await
            .map_err(|e| e.to_string())?;
        let row = self
            .validate_runtime_action_connection(&mut tx, cmd, action_id, lease, "RUNNING", true)
            .await
            .map_err(|e| e.to_string())?;
        if row.get::<String, _>("status") != "STARTED" || cmd.action_type != "filesystem.write" {
            return Err("Runtime write no longer authorized".into());
        }
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease)
            .await
            .map_err(|e| e.to_string())?;
        // No await between current authorization and mutation. No provider defaults.
        write()?;
        tx.commit()
            .await
            .map_err(|_| "filesystem operation failed".to_owned())
    }

    /// Serialize final shell authorization and process creation with cancellation/takeover.
    /// The returned process guard must kill its Job on drop, including commit failure.
    #[cfg(windows)]
    pub async fn spawn_runtime_shell<T>(
        &self,
        cmd: &ProposeActionCommand,
        action_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
        spawn: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| e.to_string())?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease)
            .await
            .map_err(|e| e.to_string())?;
        let row = self
            .validate_runtime_action_connection(&mut tx, cmd, action_id, lease, "RUNNING", true)
            .await
            .map_err(|e| e.to_string())?;
        if cmd.action_type != "shell.execute" || row.get::<String, _>("status") != "STARTED" {
            return Err("Runtime shell no longer authorized".into());
        }
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease)
            .await
            .map_err(|e| e.to_string())?;
        let process = spawn()?;
        tx.commit()
            .await
            .map_err(|_| "Shell spawn acknowledgement failed".to_owned())?;
        Ok(process)
    }

    async fn runtime_action_event_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        cmd: &ProposeActionCommand,
        action_id: &str,
        event_type: &str,
        version: i64,
        result: Option<&serde_json::Value>,
    ) -> Result<(), ApplicationError> {
        let event = nalarvo_domain::DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: event_type.into(),
            schema_version: 1,
            company_id: nalarvo_domain::CompanyId(cmd.company_id.clone()),
            aggregate_type: "Action".into(),
            aggregate_id: action_id.into(),
            aggregate_version: version,
            occurred_at: Utc::now(),
            correlation_id: cmd
                .meta
                .correlation_id
                .clone()
                .unwrap_or_else(|| action_id.into()),
            causation_id: cmd
                .meta
                .causation_id
                .clone()
                .unwrap_or_else(|| action_id.into()),
            principal: cmd.actor.clone(),
            scope: nalarvo_domain::ScopeRef::company(&cmd.company_id),
            payload: serde_json::json!({"action_id":action_id,"run_id":cmd.run_id,"step_id":cmd.step_id,"project_id":cmd.project_id,"result":result}),
        };
        nalarvo_persistence::insert_domain_event_and_outbox_tx(tx, &event).await?;
        self.durable_audit_sink
            .record_tx(
                tx,
                &nalarvo_domain::AuditRecord {
                    audit_id: Uuid::now_v7().to_string(),
                    action: event_type.into(),
                    principal: cmd.actor.clone(),
                    scope: event.scope,
                    occurred_at: event.occurred_at,
                    details: event.payload,
                },
            )
            .await?;
        Ok(())
    }

    /// One canonical terminal fact, invocation and resource realization transaction.
    /// Reprocessing the same result has no additional accounting or events.
    pub async fn finish_runtime_action(
        &self,
        cmd: &ProposeActionCommand,
        action_id: &str,
        lease: &nalarvo_persistence::m4::ExecutionLease,
        status: &str,
        result: &serde_json::Value,
    ) -> Result<(), ApplicationError> {
        let event_type = match status {
            "SUCCEEDED" => "ActionSucceeded",
            "FAILED" => "ActionFailed",
            "CANCELLED" => "ActionCancelled",
            "OUTCOME_UNKNOWN" => "ActionOutcomeUnknown",
            _ => {
                return Err(ApplicationError::Validation(
                    "Invalid terminal Action state".into(),
                ));
            }
        };
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, lease).await?;
        let row=sqlx::query("SELECT a.*,r.work_item_id AS canonical_work_item,r.executing_agent_id AS canonical_agent FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=? AND a.company_id=? AND a.run_id=?")
            .bind(action_id).bind(&cmd.company_id).bind(&lease.run_id).fetch_one(&mut *tx).await?;
        let current: String = row.get("status");
        if current == status {
            tx.commit().await?;
            return Ok(());
        }
        if current != "STARTED" {
            return Err(ApplicationError::Validation(
                "Action is not executing".into(),
            ));
        }
        let version: i64 = row.get("row_version");
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE actions SET status=?,result=?,completed_at=?,updated_at=?,row_version=row_version+1 WHERE id=? AND row_version=?")
            .bind(status).bind(result.to_string()).bind(&now).bind(&now).bind(action_id).bind(version).execute(&mut *tx).await?;
        sqlx::query("UPDATE tool_invocations SET status=?,response=?,failure_class=?,completed_at=? WHERE action_id=? AND status='STARTED'")
            .bind(status).bind(result.to_string()).bind(if status=="SUCCEEDED"{None}else{Some(status)}).bind(&now).bind(action_id).execute(&mut *tx).await?;
        let held=sqlx::query("SELECT id,budget_id,amount FROM resource_reservations WHERE action_id=? AND resource_type='TOOL_CALL' AND status='HELD' AND ? != 'OUTCOME_UNKNOWN'").bind(action_id).bind(status).fetch_all(&mut *tx).await?;
        for reservation in held {
            let budget: String = reservation.get("budget_id");
            let amount: f64 = reservation.get("amount");
            sqlx::query("UPDATE budgets SET reserved_amount=reserved_amount-?,spent_amount=spent_amount+?,updated_at=? WHERE id=?").bind(amount).bind(amount).bind(&now).bind(&budget).execute(&mut *tx).await?;
            sqlx::query("UPDATE budget_limits SET reserved_amount=reserved_amount-?,spent_amount=spent_amount+? WHERE budget_id=? AND resource_type='TOOL_CALL'").bind(amount).bind(amount).bind(&budget).execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE resource_reservations SET status='REALIZED',released_at=? WHERE id=?",
            )
            .bind(&now)
            .bind(reservation.get::<String, _>("id"))
            .execute(&mut *tx)
            .await?;
        }
        let provider:String=sqlx::query_scalar("SELECT provider_connection_id FROM model_invocations WHERE run_id=? ORDER BY invocation_index DESC LIMIT 1")
            .bind(&lease.run_id).fetch_optional(&mut *tx).await?.or_else(||cmd.parameters.as_ref().and_then(|p|p["provider_connection_id"].as_str().map(str::to_owned)))
            .ok_or_else(||ApplicationError::Validation("Tool usage requires canonical provider lineage".into()))?;
        sqlx::query("INSERT INTO usage_records(id,workspace_id,company_id,project_id,work_item_id,agent_id,run_id,step_id,provider_connection_id,model_id,usage_type,quantity,unit,occurred_at,metadata,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,'TOOL_CALL',1,'call',?,?,?)")
            .bind(format!("tool:{action_id}")).bind(row.get::<String,_>("workspace_id")).bind(&cmd.company_id).bind(&cmd.project_id).bind(row.get::<String,_>("canonical_work_item")).bind(row.get::<String,_>("canonical_agent")).bind(&cmd.run_id).bind(&cmd.step_id).bind(provider).bind(cmd.parameters.as_ref().and_then(|p|p["tool_id"].as_str()).unwrap_or(&cmd.action_type))
            .bind(&now).bind(serde_json::json!({"action_id":action_id,"tool_invocation_id":format!("tool:{action_id}")}).to_string()).bind(&now).execute(&mut *tx).await?;
        self.runtime_action_event_tx(
            &mut tx,
            cmd,
            action_id,
            event_type,
            version + 1,
            Some(result),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn transition_action_execution(
        &self,
        company_id: &str,
        action_id: &str,
        status: &str,
        event_type: &str,
        principal: PrincipalRef,
        failure: Option<&str>,
    ) -> Result<(), ApplicationError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT status, authorization, action_type, target FROM actions WHERE id = ? AND company_id = ?",
        )
        .bind(action_id)
        .bind(company_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApplicationError::NotFound(action_id.into()))?;
        let current: String = row.get("status");
        let authorization: String = row.get("authorization");
        if status == "STARTED" && !matches!(authorization.as_str(), "ALLOWED" | "APPROVED") {
            return Err(ApplicationError::Unauthorized);
        }
        if current == status {
            tx.commit().await?;
            return Ok(());
        }
        let valid = match status {
            "STARTED" => matches!(current.as_str(), "PENDING" | "APPROVED"),
            "SUCCEEDED" | "FAILED" | "CANCELLED" | "OUTCOME_UNKNOWN" => current == "STARTED",
            _ => false,
        };
        if !valid {
            return Err(ApplicationError::Validation(format!(
                "Invalid action execution transition {current} -> {status}"
            )));
        }
        let now = Utc::now();
        sqlx::query("UPDATE actions SET status = ?, updated_at = ? WHERE id = ? AND company_id = ? AND status = ?")
            .bind(status)
            .bind(now.to_rfc3339())
            .bind(action_id)
            .bind(company_id)
            .bind(&current)
            .execute(&mut *tx)
            .await?;
        let event = nalarvo_domain::DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: event_type.into(),
            schema_version: 1,
            company_id: nalarvo_domain::CompanyId(company_id.into()),
            aggregate_type: "Action".into(),
            aggregate_id: action_id.into(),
            aggregate_version: 1,
            occurred_at: now,
            correlation_id: Uuid::now_v7().to_string(),
            causation_id: action_id.into(),
            principal: principal.clone(),
            scope: nalarvo_domain::ScopeRef::company(company_id),
            payload: serde_json::json!({
                "action_id": action_id,
                "status": status,
                "failure": failure,
            }),
        };
        nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        let audit = nalarvo_domain::AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: event_type.into(),
            principal,
            scope: nalarvo_domain::ScopeRef::company(company_id),
            occurred_at: now,
            details: serde_json::json!({
                "action_id": action_id,
                "status": status,
                "failure": failure,
            }),
        };
        self.durable_audit_sink.record_tx(&mut tx, &audit).await?;
        tx.commit().await?;
        self.audit_sink.record(audit);
        Ok(())
    }

    pub async fn record_action_started(
        &self,
        company_id: &str,
        action_id: &str,
        principal: PrincipalRef,
    ) -> Result<(), ApplicationError> {
        self.transition_action_execution(
            company_id,
            action_id,
            "STARTED",
            "ActionStarted",
            principal,
            None,
        )
        .await
    }

    pub async fn record_action_succeeded(
        &self,
        company_id: &str,
        action_id: &str,
        principal: PrincipalRef,
    ) -> Result<(), ApplicationError> {
        self.transition_action_execution(
            company_id,
            action_id,
            "SUCCEEDED",
            "ActionSucceeded",
            principal,
            None,
        )
        .await
    }

    pub async fn record_action_failed(
        &self,
        company_id: &str,
        action_id: &str,
        principal: PrincipalRef,
        failure: impl Into<String>,
    ) -> Result<(), ApplicationError> {
        let failure = failure.into();
        self.transition_action_execution(
            company_id,
            action_id,
            "FAILED",
            "ActionFailed",
            principal,
            Some(&failure),
        )
        .await
    }

    pub async fn record_action_cancelled(
        &self,
        company_id: &str,
        action_id: &str,
        principal: PrincipalRef,
    ) -> Result<(), ApplicationError> {
        self.transition_action_execution(
            company_id,
            action_id,
            "CANCELLED",
            "ActionCancelled",
            principal,
            None,
        )
        .await
    }

    pub async fn record_action_outcome_unknown(
        &self,
        company_id: &str,
        action_id: &str,
        principal: PrincipalRef,
        reason: impl Into<String>,
    ) -> Result<(), ApplicationError> {
        let reason = reason.into();
        self.transition_action_execution(
            company_id,
            action_id,
            "OUTCOME_UNKNOWN",
            "ActionOutcomeUnknown",
            principal,
            Some(&reason),
        )
        .await
    }

    pub async fn list_actions(
        &self,
        company_id: &str,
    ) -> Result<Vec<ActionRecord>, ApplicationError> {
        let rows = sqlx::query_as::<_, ActionRecord>(
            "SELECT * FROM actions WHERE company_id = ? ORDER BY created_at DESC",
        )
        .bind(company_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_action(
        &self,
        company_id: &str,
        action_id: &str,
    ) -> Result<Option<ActionRecord>, ApplicationError> {
        let row = sqlx::query_as::<_, ActionRecord>(
            "SELECT * FROM actions WHERE company_id = ? AND id = ?",
        )
        .bind(company_id)
        .bind(action_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn ensure_canonical_permissions(
        &self,
        workspace_id: &str,
    ) -> Result<(), ApplicationError> {
        let now = chrono::Utc::now().to_rfc3339();
        for (name, resource, operation, desc) in
            nalarvo_domain::CANONICAL_PERMISSION_DEFINITIONS.iter()
        {
            let id = format!("pd_{name}");
            sqlx::query(
                "INSERT INTO permission_definitions (id, workspace_id, name, resource, operation, description, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(workspace_id, name) DO NOTHING",
            )
            .bind(&id)
            .bind(workspace_id)
            .bind(name)
            .bind(resource)
            .bind(operation)
            .bind(desc)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn list_permission_definitions(
        &self,
    ) -> Result<Vec<nalarvo_persistence::m5::PermissionDefinitionRecord>, ApplicationError> {
        let rows = sqlx::query_as::<_, nalarvo_persistence::m5::PermissionDefinitionRecord>(
            "SELECT * FROM permission_definitions ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_permission_grants(
        &self,
        company_id: &str,
    ) -> Result<Vec<nalarvo_persistence::m5::PermissionGrantRecord>, ApplicationError> {
        let rows = sqlx::query_as::<_, nalarvo_persistence::m5::PermissionGrantRecord>(
            "SELECT * FROM permission_grants WHERE company_id = ? ORDER BY created_at DESC",
        )
        .bind(company_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_policies(
        &self,
        company_id: &str,
    ) -> Result<Vec<nalarvo_persistence::m5::PolicyRecord>, ApplicationError> {
        let rows = sqlx::query_as::<_, nalarvo_persistence::m5::PolicyRecord>(
            "SELECT p.* FROM policies p JOIN companies c ON c.id=? WHERE p.workspace_id=c.workspace_id AND (p.company_id=c.id OR p.company_id IS NULL) ORDER BY p.name ASC",
        )
        .bind(company_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_budgets(
        &self,
        company_id: &str,
    ) -> Result<Vec<nalarvo_persistence::m5::BudgetRecord>, ApplicationError> {
        let rows = sqlx::query_as::<_, nalarvo_persistence::m5::BudgetRecord>(
            "SELECT * FROM budgets WHERE company_id = ? ORDER BY name ASC",
        )
        .bind(company_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_approvals(
        &self,
        company_id: &str,
    ) -> Result<Vec<ApprovalRequestRecord>, ApplicationError> {
        let rows = sqlx::query(
            "SELECT id, action_id, workspace_id, company_id, requested_by_id, status, reason, requested_at FROM approval_requests WHERE company_id = ? ORDER BY requested_at DESC",
        )
        .bind(company_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| ApprovalRequestRecord {
                id: r.get("id"),
                action_id: r.get("action_id"),
                workspace_id: r.get("workspace_id"),
                company_id: r.get("company_id"),
                requested_by_id: r.get("requested_by_id"),
                status: r.get("status"),
                reason: r.get("reason"),
                requested_at: r.get("requested_at"),
            })
            .collect())
    }

    pub async fn get_approval(
        &self,
        company_id: &str,
        approval_id: &str,
    ) -> Result<ApprovalRequestRecord, ApplicationError> {
        let row = sqlx::query(
            "SELECT id, action_id, workspace_id, company_id, requested_by_id, status, reason, requested_at FROM approval_requests WHERE company_id = ? AND id = ?",
        )
        .bind(company_id)
        .bind(approval_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            ApplicationError::Domain(DomainError::Validation(format!(
                "Approval {} not found",
                approval_id
            )))
        })?;

        Ok(ApprovalRequestRecord {
            id: row.get("id"),
            action_id: row.get("action_id"),
            workspace_id: row.get("workspace_id"),
            company_id: row.get("company_id"),
            requested_by_id: row.get("requested_by_id"),
            status: row.get("status"),
            reason: row.get("reason"),
            requested_at: row.get("requested_at"),
        })
    }

    pub async fn approval_fingerprint(
        &self,
        company_id: &str,
        action_id: &str,
    ) -> Result<String, ApplicationError> {
        let fingerprint: Option<String> = sqlx::query_scalar("SELECT json_extract(e.payload,'$.fingerprint') FROM actions a JOIN approval_requests p ON p.action_id=a.id AND p.company_id=a.company_id JOIN domain_events e ON e.aggregate_id=a.id AND e.company_id=a.company_id AND e.event_type='ActionProposed' WHERE a.company_id=? AND a.id=? ORDER BY e.rowid LIMIT 1")
            .bind(company_id).bind(action_id).fetch_optional(&self.pool).await?.flatten();
        fingerprint
            .filter(|fp| fp.len() == 64 && fp.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or_else(|| {
                ApplicationError::Validation("Approval fingerprint missing or invalid".into())
            })
    }

    pub async fn decide_action_approval(
        &self,
        company_id: &str,
        approval_id: &str,
        decided_by: PrincipalRef,
        fingerprint: &str,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ApprovalDecisionRecord, ApplicationError> {
        let approval = self.get_approval(company_id, approval_id).await?;
        if approval.status != "PENDING" {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Approval is not pending".into(),
            )));
        }

        // SEC-02 Self-approval prevention
        if approval.requested_by_id == decided_by.principal_id {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Self-approval is strictly forbidden".into(),
            )));
        }

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let owner: Option<String> = sqlx::query_scalar("SELECT w.owner_user_id FROM companies c JOIN workspaces w ON w.id=c.workspace_id WHERE c.id=? AND c.workspace_id=? AND c.status='ACTIVE' AND w.status='ACTIVE'")
            .bind(company_id).bind(&approval.workspace_id).fetch_optional(&mut *tx).await?;
        if decided_by.principal_type != nalarvo_domain::PrincipalType::User
            || owner.as_deref() != Some(&decided_by.principal_id)
        {
            return Err(ApplicationError::Validation(
                "Approval requires the active workspace owner".into(),
            ));
        }
        let current: Option<(String, Option<String>, String)> = sqlx::query_as("SELECT status,expires_at,requested_by_id FROM approval_requests WHERE id=? AND action_id=? AND company_id=? AND workspace_id=?")
            .bind(approval_id).bind(&approval.action_id).bind(company_id).bind(&approval.workspace_id).fetch_optional(&mut *tx).await?;
        let Some((status, expires_at, requester)) = current else {
            return Err(ApplicationError::Validation(
                "Approval scope mismatch".into(),
            ));
        };
        if status != "PENDING" || requester == decided_by.principal_id {
            return Err(ApplicationError::Validation(
                "Approval is not pending or is self-approved".into(),
            ));
        }
        if let Some(expiry) = expires_at {
            let expiry = chrono::DateTime::parse_from_rfc3339(&expiry)
                .map_err(|_| ApplicationError::Validation("Invalid approval expiry".into()))?;
            if expiry <= Utc::now() {
                return Err(ApplicationError::Validation("Approval expired".into()));
            }
        }
        let action_row = sqlx::query("SELECT company_id, action_type, target, input, authorization, project_id, requested_by_type, requested_by_id, (SELECT payload FROM domain_events WHERE aggregate_id = actions.id AND company_id=actions.company_id AND event_type = 'ActionProposed' ORDER BY rowid LIMIT 1) AS proposed_payload FROM actions WHERE id = ? AND company_id=? AND workspace_id=? AND status='PENDING'")
            .bind(&approval.action_id).bind(company_id).bind(&approval.workspace_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ApplicationError::Domain(DomainError::Validation("Action not found".into())))?;

        let act_auth: String = action_row.get("authorization");
        if act_auth != "APPROVAL_REQUIRED" {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Action does not require approval".into(),
            )));
        }

        let act_target: String = action_row.get("target");
        let act_type: String = action_row.get("action_type");
        let act_input_raw: Option<String> = action_row.get("input");
        let act_input_json: Option<serde_json::Value> = act_input_raw
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| ApplicationError::Validation("Invalid Action input".into()))?;
        let action_project_id: Option<String> = action_row.get("project_id");

        let proposed_payload_raw: Option<String> = action_row.get("proposed_payload");
        let proposed_payload: Option<serde_json::Value> = proposed_payload_raw
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let project_id = action_project_id.as_deref().or_else(|| {
            proposed_payload
                .as_ref()
                .and_then(|p| p.get("project_id").and_then(|v| v.as_str()))
        });
        let calculated_fp = Self::compute_action_fingerprint(
            company_id,
            project_id,
            &act_type,
            &act_target,
            act_input_json.as_ref(),
        );
        let original_fp = proposed_payload
            .as_ref()
            .and_then(|p| p.get("fingerprint").and_then(|v| v.as_str()))
            .ok_or_else(|| ApplicationError::Validation("Approval fingerprint missing".into()))?;

        if calculated_fp != fingerprint || original_fp != fingerprint {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Approval request fingerprint mismatch".into(),
            )));
        }

        let requester = PrincipalRef {
            principal_type: match action_row.get::<String, _>("requested_by_type").as_str() {
                "AGENT" => nalarvo_domain::PrincipalType::Agent,
                "USER" => nalarvo_domain::PrincipalType::User,
                "SYSTEM" => nalarvo_domain::PrincipalType::System,
                _ => {
                    return Err(ApplicationError::Validation(
                        "Invalid Action requester".into(),
                    ));
                }
            },
            principal_id: action_row.get("requested_by_id"),
        };
        let command = ProposeActionCommand {
            company_id: company_id.into(),
            project_id: project_id.map(str::to_owned),
            run_id: None,
            step_id: None,
            actor: requester,
            action_type: act_type,
            target: act_target,
            parameters: act_input_json,
            idempotency_key: None,
            meta: crate::CommandMeta::default(),
        };
        if Self::evaluate_action_authority_connection(&mut tx, &command).await?
            == AuthorityDecision::Deny
        {
            return Err(ApplicationError::Validation(
                "Action authority revoked".into(),
            ));
        }
        if let Some(project) = project_id {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM projects WHERE id=? AND company_id=? AND status='ACTIVE'",
            )
            .bind(project)
            .bind(company_id)
            .fetch_one(&mut *tx)
            .await?;
            if active != 1 {
                return Err(ApplicationError::Validation(
                    "Approval Project is not active".into(),
                ));
            }
        }

        let outcome = if approved { "APPROVED" } else { "REJECTED" };
        let decision_id = format!("dec_{}", Uuid::now_v7());
        let decision_rec = ApprovalDecisionRecord {
            id: decision_id,
            approval_request_id: approval.id.clone(),
            decided_by_type: decided_by.principal_type.to_string(),
            decided_by_id: decided_by.principal_id.clone(),
            decision: outcome.into(),
            reason,
            decided_at: Utc::now().to_rfc3339(),
        };

        let run_link: Option<(String, String, i64)> = sqlx::query_as("SELECT r.id,r.lifecycle_state,r.row_version FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=? AND r.company_id=?")
            .bind(&approval.action_id).bind(company_id).fetch_optional(&mut *tx).await?;
        if let Some((_, ref state, _)) = run_link
            && state != "WAITING_APPROVAL"
        {
            return Err(ApplicationError::Validation(
                "Run is not waiting for approval".into(),
            ));
        }
        let checkpoint_bound: i64 = if let Some((ref run_id, _, _)) = run_link {
            sqlx::query_scalar("SELECT COUNT(*) FROM runtime_checkpoints WHERE run_id=? AND company_id=? AND run_state='WAITING_APPROVAL' AND safe_to_resume=1 AND json_extract(continuation_metadata,'$.action_id')=? AND json_extract(continuation_metadata,'$.approval_id')=? AND json_extract(continuation_metadata,'$.fingerprint')=?")
                .bind(run_id).bind(company_id).bind(&approval.action_id).bind(approval_id).bind(fingerprint)
                .fetch_one(&mut *tx).await?
        } else {
            0
        };
        if run_link.is_some() && checkpoint_bound != 1 {
            return Err(ApplicationError::Validation(
                "Approval checkpoint missing or mismatched".into(),
            ));
        }
        if let Some((ref run_id, _, _)) = run_link {
            let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM durable_jobs WHERE run_id=? AND company_id=? AND status='WAITING_APPROVAL'")
                .bind(run_id).bind(company_id).fetch_one(&mut *tx).await?;
            if jobs != 1 {
                return Err(ApplicationError::Validation("Approval job missing".into()));
            }
        }

        let updated = sqlx::query(
            "UPDATE approval_requests SET status = ?, decided_at = ? WHERE id = ? AND status = 'PENDING'",
        )
        .bind(&decision_rec.decision)
        .bind(&decision_rec.decided_at)
        .bind(&decision_rec.approval_request_id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Approval is not pending".into(),
            )));
        }

        sqlx::query(
            "INSERT INTO approval_decisions (id, approval_request_id, decided_by_type, decided_by_id, decision, reason, decided_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&decision_rec.id)
        .bind(&decision_rec.approval_request_id)
        .bind(&decision_rec.decided_by_type)
        .bind(&decision_rec.decided_by_id)
        .bind(&decision_rec.decision)
        .bind(&decision_rec.reason)
        .bind(&decision_rec.decided_at)
        .execute(&mut *tx)
        .await?;

        let action_status = if approved { "PENDING" } else { "FAILED" };
        sqlx::query(
            "UPDATE actions SET authorization = ?, status = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&decision_rec.decision)
        .bind(action_status)
        .bind(&decision_rec.decided_at)
        .bind(&approval.action_id)
        .execute(&mut *tx)
        .await?;

        let event_type = if approved {
            "ApprovalGranted"
        } else {
            "ApprovalRejected"
        };
        let event = nalarvo_domain::DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: event_type.into(),
            schema_version: 1,
            company_id: nalarvo_domain::CompanyId(company_id.to_string()),
            aggregate_type: "Approval".into(),
            aggregate_id: approval.id.clone(),
            aggregate_version: 1,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7().to_string(),
            causation_id: approval.id.clone(),
            principal: decided_by.clone(),
            scope: nalarvo_domain::ScopeRef::company(company_id),
            payload: serde_json::json!({
                "approval_id": approval.id,
                "action_id": approval.action_id,
                "decision": outcome,
                "fingerprint": fingerprint,
                "reason": decision_rec.reason,
            }),
        };
        nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        let action_event = nalarvo_domain::DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: if approved {
                "ActionApproved"
            } else {
                "ActionRejected"
            }
            .into(),
            schema_version: 1,
            company_id: nalarvo_domain::CompanyId(company_id.to_string()),
            aggregate_type: "Action".into(),
            aggregate_id: approval.action_id.clone(),
            aggregate_version: 2,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7().to_string(),
            causation_id: approval.id.clone(),
            principal: decided_by.clone(),
            scope: nalarvo_domain::ScopeRef::company(company_id),
            payload: serde_json::json!({
                "approval_id": approval.id,
                "action_id": approval.action_id,
                "decision": outcome,
                "fingerprint": fingerprint,
            }),
        };
        nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &action_event).await?;
        self.durable_audit_sink
            .record_tx(
                &mut tx,
                &nalarvo_domain::AuditRecord {
                    audit_id: Uuid::now_v7().to_string(),
                    action: action_event.event_type.clone(),
                    principal: decided_by.clone(),
                    scope: action_event.scope,
                    occurred_at: Utc::now(),
                    details: action_event.payload,
                },
            )
            .await?;
        if let Some((ref run_id, _, version)) = run_link {
            let at = Utc::now().to_rfc3339();
            if approved {
                // Schedule durably; the new lease holder revalidates before leaving WAITING_APPROVAL.
                let job = sqlx::query("UPDATE durable_jobs SET status='PENDING',available_at=?,lease_owner=NULL,lease_until=NULL,updated_at=? WHERE run_id=? AND company_id=? AND status='WAITING_APPROVAL'")
                    .bind(&at).bind(&at).bind(run_id).bind(company_id).execute(&mut *tx).await?;
                if job.rows_affected() != 1 {
                    return Err(ApplicationError::Validation("Approval job missing".into()));
                }
                let run_event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "RunQueued".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(company_id.to_string()),
                    aggregate_type: "Run".into(),
                    aggregate_id: run_id.clone(),
                    aggregate_version: version,
                    occurred_at: Utc::now(),
                    correlation_id: sqlx::query_scalar(
                        "SELECT correlation_id FROM runs WHERE id=?",
                    )
                    .bind(run_id)
                    .fetch_one(&mut *tx)
                    .await?,
                    causation_id: approval.id.clone(),
                    principal: decided_by.clone(),
                    scope: nalarvo_domain::ScopeRef::company(company_id),
                    payload: serde_json::json!({"run_id":run_id,"status":"WAITING_APPROVAL","approval_id":approval.id}),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &run_event).await?;
            } else {
                sqlx::query("UPDATE runtime_checkpoints SET safe_to_resume=0 WHERE run_id=? AND company_id=?")
                    .bind(run_id).bind(company_id).execute(&mut *tx).await?;
                sqlx::query("UPDATE durable_jobs SET status='CANCELLED',updated_at=? WHERE run_id=? AND company_id=? AND status='WAITING_APPROVAL'")
                    .bind(&at).bind(run_id).bind(company_id).execute(&mut *tx).await?;
                sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED',completed_at=?,updated_at=?,row_version=row_version+1 WHERE id=? AND company_id=? AND lifecycle_state='WAITING_APPROVAL' AND row_version=?")
                    .bind(&at).bind(&at).bind(run_id).bind(company_id).bind(version).execute(&mut *tx).await?;
                sqlx::query("UPDATE execution_steps SET lifecycle_state='CANCELLED',completed_at=? WHERE run_id=? AND company_id=? AND lifecycle_state IN ('PENDING','RUNNING')")
                    .bind(&at).bind(run_id).bind(company_id).execute(&mut *tx).await?;
                let run_event = nalarvo_domain::DomainEvent {
                    event_id: Uuid::now_v7().to_string(),
                    event_type: "RunCancelled".into(),
                    schema_version: 1,
                    company_id: nalarvo_domain::CompanyId(company_id.into()),
                    aggregate_type: "Run".into(),
                    aggregate_id: run_id.clone(),
                    aggregate_version: version + 1,
                    occurred_at: Utc::now(),
                    correlation_id: sqlx::query_scalar(
                        "SELECT correlation_id FROM runs WHERE id=?",
                    )
                    .bind(run_id)
                    .fetch_one(&mut *tx)
                    .await?,
                    causation_id: approval.id.clone(),
                    principal: decided_by.clone(),
                    scope: nalarvo_domain::ScopeRef::company(company_id),
                    payload: serde_json::json!({"run_id":run_id,"status":"CANCELLED","approval_id":approval.id,"reason":"Approval rejected"}),
                };
                nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &run_event).await?;
                sqlx::query("INSERT INTO runtime_results(id,company_id,run_id,run_status,result_summary,failure_class,failure_detail,correlation_id,causation_id,created_at) VALUES(?,?,?,'CANCELLED','Approval rejected','CANCELLED','Approval rejected',?,?,?)")
                    .bind(Uuid::now_v7().to_string()).bind(company_id).bind(run_id)
                    .bind(&run_event.correlation_id).bind(&approval.id).bind(&at).execute(&mut *tx).await?;
                self.durable_audit_sink
                    .record_tx(
                        &mut tx,
                        &nalarvo_domain::AuditRecord {
                            audit_id: Uuid::now_v7().to_string(),
                            action: "RunCancelled".into(),
                            principal: decided_by.clone(),
                            scope: run_event.scope,
                            occurred_at: Utc::now(),
                            details: run_event.payload,
                        },
                    )
                    .await?;
            }
        }
        let audit_record = nalarvo_domain::AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: event_type.into(),
            principal: decided_by,
            scope: nalarvo_domain::ScopeRef::company(company_id),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "approval_id": approval.id,
                "action_id": approval.action_id,
                "decision": outcome,
                "fingerprint": fingerprint,
            }),
        };
        self.durable_audit_sink
            .record_tx(&mut tx, &audit_record)
            .await?;
        tx.commit().await?;
        self.audit_sink.record(audit_record);

        Ok(decision_rec)
    }

    pub async fn expire_approval(
        &self,
        company_id: &str,
        approval_id: &str,
        reason: Option<String>,
    ) -> Result<(), ApplicationError> {
        self.close_pending_approval(
            company_id,
            approval_id,
            PrincipalRef::system("system"),
            reason,
            true,
        )
        .await
    }

    pub async fn cancel_approval(
        &self,
        company_id: &str,
        approval_id: &str,
        cancelled_by: PrincipalRef,
        reason: Option<String>,
    ) -> Result<(), ApplicationError> {
        self.close_pending_approval(company_id, approval_id, cancelled_by, reason, false)
            .await
    }

    async fn close_pending_approval(
        &self,
        company_id: &str,
        approval_id: &str,
        principal: PrincipalRef,
        reason: Option<String>,
        expired: bool,
    ) -> Result<(), ApplicationError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT p.action_id,p.status,a.run_id,a.step_id,a.status AS action_status,a.authorization,a.row_version,a.correlation_id FROM approval_requests p JOIN actions a ON a.id=p.action_id AND a.company_id=p.company_id WHERE p.id=? AND p.company_id=?")
            .bind(approval_id).bind(company_id).fetch_optional(&mut *tx).await?
            .ok_or_else(|| ApplicationError::NotFound(approval_id.into()))?;
        if row.get::<String, _>("status") != "PENDING" {
            return Err(ApplicationError::Domain(DomainError::Validation(
                "Approval is not pending".into(),
            )));
        }
        if row.get::<String, _>("action_status") != "PENDING"
            || row.get::<String, _>("authorization") != "APPROVAL_REQUIRED"
        {
            return Err(ApplicationError::Validation(
                "Action is not waiting for approval".into(),
            ));
        }
        let action_id: String = row.get("action_id");
        let run_id: Option<String> = row.get("run_id");
        let now = Utc::now();
        let at = now.to_rfc3339();
        let (approval_status, action_status, approval_event, action_event) = if expired {
            ("EXPIRED", "FAILED", "ApprovalExpired", "ActionExpired")
        } else {
            (
                "CANCELLED",
                "CANCELLED",
                "ApprovalCancelled",
                "ActionCancelled",
            )
        };
        let run = if let Some(ref run_id) = run_id {
            let run: (String, i64, String) = sqlx::query_as("SELECT lifecycle_state,row_version,correlation_id FROM runs WHERE id=? AND company_id=?")
                .bind(run_id).bind(company_id).fetch_one(&mut *tx).await?;
            if run.0 != "WAITING_APPROVAL" {
                return Err(ApplicationError::Validation(
                    "Run is not waiting for approval".into(),
                ));
            }
            let checkpoint: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_checkpoints WHERE run_id=? AND company_id=? AND run_state='WAITING_APPROVAL' AND safe_to_resume=1 AND json_extract(continuation_metadata,'$.action_id')=? AND json_extract(continuation_metadata,'$.approval_id')=?")
                .bind(run_id).bind(company_id).bind(&action_id).bind(approval_id).fetch_one(&mut *tx).await?;
            let job: i64 = sqlx::query_scalar("SELECT count(*) FROM durable_jobs WHERE run_id=? AND company_id=? AND status='WAITING_APPROVAL'")
                .bind(run_id).bind(company_id).fetch_one(&mut *tx).await?;
            if checkpoint != 1 || job != 1 {
                return Err(ApplicationError::Validation(
                    "Approval checkpoint or job missing".into(),
                ));
            }
            Some(run)
        } else {
            None
        };
        let changed = sqlx::query("UPDATE approval_requests SET status=?,reason=?,decided_at=? WHERE id=? AND company_id=? AND status='PENDING'")
            .bind(approval_status).bind(&reason).bind(&at).bind(approval_id).bind(company_id).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(ApplicationError::Validation(
                "Approval no longer pending".into(),
            ));
        }
        let version: i64 = row.get("row_version");
        let changed = sqlx::query("UPDATE actions SET authorization=?,status=?,updated_at=?,completed_at=?,row_version=row_version+1 WHERE id=? AND company_id=? AND status='PENDING' AND row_version=?")
            .bind(approval_status).bind(action_status).bind(&at).bind(&at).bind(&action_id).bind(company_id).bind(version).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(ApplicationError::Validation(
                "Action changed while awaiting approval".into(),
            ));
        }
        if let (Some(run_id), Some((_, run_version, correlation_id))) = (&run_id, &run) {
            nalarvo_persistence::m4::transition_run_tx(
                &mut tx,
                &nalarvo_domain::CompanyId(company_id.into()),
                run_id,
                *run_version,
                nalarvo_domain::RunStatus::Cancelled,
                None,
                None,
            )
            .await?;
            sqlx::query("INSERT INTO runtime_results(id,company_id,run_id,run_status,result_summary,failure_class,failure_detail,correlation_id,causation_id,created_at) VALUES(?,?,?,'CANCELLED',?,'CANCELLED',?,?,?,?)")
                .bind(Uuid::now_v7().to_string()).bind(company_id).bind(run_id)
                .bind(if expired { "Approval expired" } else { "Approval cancelled" })
                .bind(&reason).bind(correlation_id).bind(approval_id).bind(&at)
                .execute(&mut *tx).await?;
            sqlx::query("UPDATE runtime_checkpoints SET safe_to_resume=0 WHERE run_id=? AND company_id=? AND safe_to_resume=1")
                .bind(run_id).bind(company_id).execute(&mut *tx).await?;
            sqlx::query("UPDATE durable_jobs SET status='CANCELLED',updated_at=? WHERE run_id=? AND company_id=? AND status='WAITING_APPROVAL'")
                .bind(&at).bind(run_id).bind(company_id).execute(&mut *tx).await?;
            if let Some(step_id) = row.get::<Option<String>, _>("step_id") {
                sqlx::query("UPDATE execution_steps SET lifecycle_state='CANCELLED',completed_at=? WHERE id=? AND run_id=? AND company_id=? AND lifecycle_state='RUNNING'")
                    .bind(&at).bind(step_id).bind(run_id).bind(company_id).execute(&mut *tx).await?;
            }
        }
        let correlation = row
            .get::<Option<String>, _>("correlation_id")
            .or_else(|| run.as_ref().map(|r| r.2.clone()))
            .unwrap_or_else(|| approval_id.into());
        let payload = serde_json::json!({"approval_id":approval_id,"action_id":action_id,"reason":reason,"run_id":run_id});
        for (kind, id, event_type, aggregate_version, details) in [
            ("Approval", approval_id, approval_event, 1, payload.clone()),
            ("Action", action_id.as_str(), action_event, version+1, payload.clone()),
        ].into_iter().chain(run_id.as_ref().zip(run.as_ref()).map(|(id, run)| ("Run", id.as_str(), "RunCancelled", run.1+1, serde_json::json!({"run_id":id,"status":"CANCELLED","approval_id":approval_id,"reason":reason})))) {
            let event = nalarvo_domain::DomainEvent {
                event_id: Uuid::now_v7().to_string(), event_type: event_type.into(), schema_version: 1,
                company_id: nalarvo_domain::CompanyId(company_id.into()), aggregate_type: kind.into(),
                aggregate_id: id.into(), aggregate_version, occurred_at: now,
                correlation_id: correlation.clone(), causation_id: approval_id.into(),
                principal: principal.clone(), scope: nalarvo_domain::ScopeRef::company(company_id),
                payload: details.clone(),
            };
            nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
            let audit = nalarvo_domain::AuditRecord {
                audit_id: Uuid::now_v7().to_string(), action: event_type.into(), principal: principal.clone(),
                scope: event.scope, occurred_at: now, details,
            };
            self.durable_audit_sink.record_tx(&mut tx, &audit).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn wait_run_for_approval(
        &self,
        company_id: &nalarvo_domain::CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
    ) -> Result<nalarvo_domain::Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if run.status != nalarvo_domain::RunStatus::Running {
            return Err(ApplicationError::Validation(
                "Only running runs can wait for approval".into(),
            ));
        }

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let updated = nalarvo_persistence::m4::transition_run_tx(
            &mut tx,
            company_id,
            run_id,
            expected_version,
            nalarvo_domain::RunStatus::WaitingApproval,
            None,
            None,
        )
        .await?;

        let event = nalarvo_domain::DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunWaitingApproval".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: nalarvo_domain::ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "WAITING_APPROVAL",
            }),
        };
        nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        let audit = nalarvo_domain::AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunWaitingApproval".into(),
            principal: run.requested_by,
            scope: nalarvo_domain::ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "run_id": run_id,
                "status": "WAITING_APPROVAL",
            }),
        };
        self.durable_audit_sink.record_tx(&mut tx, &audit).await?;
        tx.commit().await?;
        self.audit_sink.record(audit);

        Ok(updated)
    }
}

#[cfg(test)]
mod temporal_tests {
    use super::*;

    #[tokio::test]
    async fn grant_instants_boundaries_and_invalid_records_fail_closed() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        nalarvo_persistence::run_migrations(&pool).await.unwrap();
        nalarvo_persistence::bootstrap_personal_workspace(
            &pool,
            &nalarvo_domain::UserId("u".into()),
            &nalarvo_domain::WorkspaceId("w".into()),
            "test@example.com",
            "Owner",
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO companies(id,workspace_id,name,status,row_version,created_at,updated_at) VALUES('c','w','Company','ACTIVE',1,'t','t')").execute(&pool).await.unwrap();
        ApplicationContext::new(pool.clone())
            .ensure_canonical_permissions("w")
            .await
            .unwrap();
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('g','w','c','pd_shell.execute','SYSTEM','worker','COMPANY','c','ALLOW','t')").execute(&pool).await.unwrap();
        let cmd = ProposeActionCommand {
            company_id: "c".into(),
            project_id: None,
            run_id: None,
            step_id: None,
            actor: PrincipalRef::system("worker"),
            action_type: "shell.execute".into(),
            target: "probe.exe".into(),
            parameters: None,
            idempotency_key: None,
            meta: CommandMeta::default(),
        };
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-18T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut connection = pool.acquire().await.unwrap();
        for (start, expiry, expected) in [
            (None, None, AuthorityDecision::Allow),
            (Some("2026-10-18T12:00:00Z"), None, AuthorityDecision::Allow),
            (
                Some("2026-10-18T00:00:00-12:00"),
                None,
                AuthorityDecision::Allow,
            ),
            (
                Some("2026-10-18T12:00:00.000000001Z"),
                None,
                AuthorityDecision::Deny,
            ),
            (Some("2026-10-18T13:00:00Z"), None, AuthorityDecision::Deny),
            (
                Some("2026-10-18T01:00:00-12:00"),
                None,
                AuthorityDecision::Deny,
            ),
            (None, Some("2026-10-18T12:00:00Z"), AuthorityDecision::Deny),
            (
                None,
                Some("2026-10-19T02:00:00+14:00"),
                AuthorityDecision::Deny,
            ),
            (None, Some("2026-10-18T11:00:00Z"), AuthorityDecision::Deny),
            (
                None,
                Some("2026-10-19T01:00:00+14:00"),
                AuthorityDecision::Deny,
            ),
            (
                None,
                Some("2026-10-18T12:00:00.000000001Z"),
                AuthorityDecision::Allow,
            ),
            (
                Some("2026-10-18T11:00:00Z"),
                Some("2026-10-18T13:00:00Z"),
                AuthorityDecision::Allow,
            ),
        ] {
            sqlx::query("UPDATE permission_grants SET starts_at=?,expires_at=? WHERE id='g'")
                .bind(start)
                .bind(expiry)
                .execute(&mut *connection)
                .await
                .unwrap();
            assert_eq!(
                ApplicationContext::evaluate_action_authority_at(&mut connection, &cmd, now)
                    .await
                    .unwrap(),
                expected,
                "{start:?} {expiry:?}"
            );
        }
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('valid','w','c','pd_shell.execute','SYSTEM','worker','COMPANY','c','ALLOW','t')").execute(&mut *connection).await.unwrap();
        for effect in ["ALLOW", "DENY"] {
            sqlx::query("UPDATE permission_grants SET effect=? WHERE id='g'")
                .bind(effect)
                .execute(&mut *connection)
                .await
                .unwrap();
            for (start, expiry) in [
                (Some("not-a-time"), None),
                (None, Some("not-a-time")),
                (Some(""), None),
                (None, Some("")),
                (Some("2026-02-30T00:00:00Z"), None),
                (Some("2026-10-18T12:00:00"), None),
                (Some("2999-01-01T00:00:00Z"), Some("bad")),
                (Some("bad"), Some("2000-01-01T00:00:00Z")),
                (Some("2026-10-18T13:00:00Z"), Some("2026-10-18T11:00:00Z")),
                (
                    Some("2026-10-18T12:00:00Z"),
                    Some("2026-10-19T02:00:00+14:00"),
                ),
            ] {
                sqlx::query("UPDATE permission_grants SET starts_at=?,expires_at=? WHERE id='g'")
                    .bind(start)
                    .bind(expiry)
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                let error =
                    ApplicationContext::evaluate_action_authority_at(&mut connection, &cmd, now)
                        .await
                        .unwrap_err();
                assert!(
                    matches!(error, ApplicationError::Validation(ref message) if message.contains("Invalid permission grant validity") && message.contains("g")),
                    "{error}"
                );
            }
        }
        sqlx::query("PRAGMA foreign_keys=OFF")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("UPDATE permission_grants SET starts_at=NULL,expires_at=NULL,permission_definition_id='missing' WHERE id='g'").execute(&mut *connection).await.unwrap();
        assert!(
            ApplicationContext::evaluate_action_authority_at(&mut connection, &cmd, now)
                .await
                .is_err()
        );
    }
}

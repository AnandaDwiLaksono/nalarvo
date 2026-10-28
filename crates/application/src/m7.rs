//! M7 staffing coordination. Allocation is participation, not an authority grant.
use crate::{ApplicationContext, ApplicationError, CommandMeta};
use chrono::Utc;
use nalarvo_domain::{
    AgentAllocation, AgentAllocationStatus, CompanyId, DomainEvent, PrincipalRef, PrincipalType,
    ScopeRef,
};
use nalarvo_persistence::{
    IdempotencyCheck, check_idempotency_tx, hash_request, insert_agent_allocation_tx,
    insert_domain_event_and_outbox_tx, save_idempotency_record_tx,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaffTeamCommand {
    pub company_id: CompanyId,
    pub project_id: String,
    pub team_id: String,
    pub meta: CommandMeta,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaffingShortage {
    pub requirement_id: String,
    pub role_id: String,
    pub desired: u32,
    pub filled: u32,
    pub missing: u32,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaffingOutcome {
    pub allocations: Vec<AgentAllocation>,
    pub shortages: Vec<StaffingShortage>,
    pub team_active: bool,
}
fn invalid(message: &str) -> ApplicationError {
    ApplicationError::Validation(message.into())
}

pub(crate) async fn owner_scope(
    tx: &mut Transaction<'_, Sqlite>,
    company: &str,
    project: &str,
    actor: &PrincipalRef,
) -> Result<(), ApplicationError> {
    if actor.principal_type != PrincipalType::User {
        return Err(ApplicationError::Unauthorized);
    }
    let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM companies c JOIN workspaces ws ON ws.id=c.workspace_id JOIN projects p ON p.company_id=c.id WHERE c.id=? AND p.id=? AND c.status='ACTIVE' AND ws.status='ACTIVE' AND EXISTS(SELECT 1 FROM workspace_memberships m WHERE m.workspace_id=ws.id AND m.user_id=?) AND p.status IN ('DRAFT','STAFFING','ACTIVE','PAUSED')")
        .bind(company).bind(project).bind(&actor.principal_id).fetch_one(&mut **tx).await?;
    if valid != 1 {
        return Err(ApplicationError::Unauthorized);
    }
    Ok(())
}

pub(crate) async fn event(
    tx: &mut Transaction<'_, Sqlite>,
    cmd: &StaffTeamCommand,
    kind: &str,
    aggregate: &str,
    id: &str,
    version: i64,
    payload: serde_json::Value,
) -> Result<(), ApplicationError> {
    let correlation = cmd
        .meta
        .correlation_id
        .clone()
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let audit_payload = payload.clone();
    insert_domain_event_and_outbox_tx(
        tx,
        &DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: kind.into(),
            schema_version: 1,
            company_id: cmd.company_id.clone(),
            aggregate_type: aggregate.into(),
            aggregate_id: id.into(),
            aggregate_version: version,
            occurred_at: Utc::now(),
            correlation_id: correlation.clone(),
            causation_id: cmd.meta.causation_id.clone().unwrap_or(correlation),
            principal: cmd
                .meta
                .principal
                .clone()
                .ok_or(ApplicationError::Unauthorized)?,
            scope: ScopeRef::company(&cmd.company_id.0),
            payload,
        },
    )
    .await?;
    nalarvo_persistence::m5::insert_audit_record_tx(
        tx,
        &nalarvo_domain::AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: kind.into(),
            principal: cmd
                .meta
                .principal
                .clone()
                .ok_or(ApplicationError::Unauthorized)?,
            scope: ScopeRef::company(&cmd.company_id.0),
            occurred_at: Utc::now(),
            details: audit_payload,
        },
    )
    .await?;
    Ok(())
}

impl ApplicationContext {
    pub async fn staff_team(
        &self,
        cmd: StaffTeamCommand,
    ) -> Result<StaffingOutcome, ApplicationError> {
        let actor = cmd
            .meta
            .principal
            .as_ref()
            .ok_or(ApplicationError::Unauthorized)?;
        let key = cmd
            .meta
            .idempotency_key
            .as_deref()
            .filter(|k| !k.trim().is_empty() && k.len() <= 128)
            .ok_or_else(|| invalid("Staffing requires bounded idempotency key"))?;
        let hash = hash_request(
            &serde_json::to_vec(&cmd).map_err(|_| invalid("Invalid staffing command"))?,
        );
        let scope = format!(
            "company:{}:{}:{}:StaffTeam",
            cmd.company_id.0, cmd.project_id, actor.principal_id
        );
        // ponytail: SQLite single-writer transaction; partition storage only if write throughput requires it.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        if let IdempotencyCheck::Cached(body) =
            check_idempotency_tx(&mut tx, &scope, key, &hash).await?
        {
            return serde_json::from_str(&body).map_err(|_| invalid("Invalid staffing replay"));
        }
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.allocate_agent",
            serde_json::json!({"team_id":cmd.team_id}),
        )
        .await?;
        let team: Option<String> = sqlx::query_scalar(
            "SELECT status FROM teams WHERE id=? AND company_id=? AND project_id=?",
        )
        .bind(&cmd.team_id)
        .bind(&cmd.company_id.0)
        .bind(&cmd.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if !matches!(team.as_deref(), Some("FORMING" | "ACTIVE")) {
            return Err(invalid("Team cannot be staffed"));
        }
        let requirements = sqlx::query("SELECT * FROM staffing_requirements WHERE company_id=? AND project_id=? AND (team_id=? OR team_id IS NULL) AND status!='CANCELLED' ORDER BY created_at,id")
            .bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&cmd.team_id).fetch_all(&mut *tx).await?;
        if requirements.is_empty()
            || requirements
                .iter()
                .any(|r| r.get::<String, _>("status") == "DRAFT")
        {
            return Err(invalid("Open staffing requirements required"));
        }
        let mut outcome = StaffingOutcome {
            allocations: vec![],
            shortages: vec![],
            team_active: false,
        };
        let now = Utc::now();
        for req in requirements {
            let id: String = req.get("id");
            let role: String = req.get("role_id");
            let department: Option<String> = req.get("department_id");
            let desired: i64 = req.get("desired_count");
            let raw: String = req.get("required_capability_ids");
            let capabilities: Vec<String> =
                serde_json::from_str(&raw).map_err(|_| invalid("Invalid staffing capabilities"))?;
            if desired <= 0
                || desired > 100
                || capabilities.len() > 64
                || capabilities
                    .iter()
                    .any(|s| s.trim().is_empty() || s.len() > 128)
            {
                return Err(invalid("Invalid staffing constraints"));
            }
            let mut filled: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_allocations WHERE company_id=? AND project_id=? AND staffing_requirement_id=? AND status='ACTIVE'")
                .bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&id).fetch_one(&mut *tx).await?;
            let candidates = sqlx::query("SELECT a.id,(SELECT COUNT(*) FROM agent_allocations al WHERE al.company_id=a.company_id AND al.agent_id=a.id AND al.status='ACTIVE') AS load FROM agents a JOIN departments d ON d.id=a.primary_department_id AND d.company_id=a.company_id AND d.status='ACTIVE' JOIN roles r ON r.id=a.role_id AND r.company_id=a.company_id AND r.status='ACTIVE' WHERE a.company_id=? AND a.role_id=? AND a.status='ACTIVE' AND (? IS NULL OR a.primary_department_id=?) AND NOT EXISTS(SELECT 1 FROM agent_allocations al WHERE al.team_id=? AND al.agent_id=a.id AND al.status IN ('PLANNED','ACTIVE','PAUSED')) AND (SELECT COUNT(*) FROM agent_allocations al WHERE al.company_id=a.company_id AND al.agent_id=a.id AND al.status='ACTIVE')<a.capacity ORDER BY load,a.created_at,a.id")
                .bind(&cmd.company_id.0).bind(&role).bind(&department).bind(&department).bind(&cmd.team_id).fetch_all(&mut *tx).await?;
            for candidate in candidates {
                if filled >= desired {
                    break;
                }
                let agent: String = candidate.get("id");
                let held: Vec<String> = sqlx::query_scalar("SELECT capability_id FROM agent_capabilities WHERE company_id=? AND agent_id=?")
                    .bind(&cmd.company_id.0).bind(&agent).fetch_all(&mut *tx).await?;
                if !capabilities.iter().all(|cap| held.contains(cap)) {
                    continue;
                }
                let allocation = AgentAllocation {
                    id: Uuid::now_v7().to_string(),
                    company_id: cmd.company_id.clone(),
                    project_id: cmd.project_id.clone(),
                    team_id: cmd.team_id.clone(),
                    agent_id: agent,
                    staffing_requirement_id: Some(id.clone()),
                    status: AgentAllocationStatus::Active,
                    row_version: 1,
                    created_at: now,
                    updated_at: now,
                    released_at: None,
                };
                insert_agent_allocation_tx(&mut tx, &allocation).await?;
                event(&mut tx,&cmd,"AgentAllocated","AgentAllocation",&allocation.id,1,serde_json::json!({"id":allocation.id,"agent_id":allocation.agent_id,"team_id":cmd.team_id,"requirement_id":id})).await?;
                outcome.allocations.push(allocation);
                filled += 1;
            }
            let status = if filled >= desired {
                "FILLED"
            } else if filled > 0 {
                "PARTIALLY_FILLED"
            } else {
                "BLOCKED"
            };
            let version: i64 = req.get("row_version");
            sqlx::query("UPDATE staffing_requirements SET status=?,row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND project_id=?")
                .bind(status).bind(now.to_rfc3339()).bind(&id).bind(&cmd.company_id.0).bind(&cmd.project_id).execute(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "StaffingRequirementReconciled",
                "StaffingRequirement",
                &id,
                version + 1,
                serde_json::json!({"status":status,"filled":filled,"desired":desired}),
            )
            .await?;
            if filled < desired {
                outcome.shortages.push(StaffingShortage {
                    requirement_id: id,
                    role_id: role,
                    desired: desired as u32,
                    filled: filled as u32,
                    missing: (desired - filled) as u32,
                    reason: "NO_ELIGIBLE_CAPACITY".into(),
                });
            }
        }
        outcome.team_active = outcome.shortages.is_empty();
        if outcome.team_active && team.as_deref() == Some("FORMING") {
            let version: i64 = sqlx::query_scalar("UPDATE teams SET status='ACTIVE',row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND project_id=? RETURNING row_version")
                .bind(now.to_rfc3339()).bind(&cmd.team_id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "TeamActivated",
                "Team",
                &cmd.team_id,
                version,
                serde_json::json!({"status":"ACTIVE"}),
            )
            .await?;
        }
        let body =
            serde_json::to_string(&outcome).map_err(|_| invalid("Invalid staffing result"))?;
        save_idempotency_record_tx(&mut tx, &scope, key, &hash, &body).await?;
        tx.commit().await?;
        self.audit_sink.record(nalarvo_domain::AuditRecord { audit_id: Uuid::now_v7().to_string(),action:"StaffTeam".into(),principal:actor.clone(),scope:ScopeRef::company(&cmd.company_id.0),occurred_at:now,details:serde_json::json!({"project_id":cmd.project_id,"team_id":cmd.team_id,"allocated":outcome.allocations.len(),"shortages":outcome.shortages.len()}) });
        Ok(outcome)
    }
}

use crate::m7::{StaffTeamCommand, event, owner_scope};
use crate::{ApplicationContext, ApplicationError};
use chrono::Utc;
use nalarvo_domain::{Run, RunStatus};
use nalarvo_persistence::{
    IdempotencyCheck, check_idempotency_tx, hash_request, save_idempotency_record_tx,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkRequirements {
    pub role_id: String,
    pub priority: i64,
    pub required: bool,
    pub acceptance_contains: String,
    pub input_version_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkReadiness {
    pub work_item_id: String,
    pub ready: bool,
    pub reason: String,
}
fn invalid(s: &str) -> ApplicationError {
    ApplicationError::Validation(s.into())
}

pub(crate) async fn readiness(
    tx: &mut Transaction<'_, Sqlite>,
    c: &str,
    p: &str,
    w: &str,
) -> Result<WorkReadiness, ApplicationError> {
    let state: Option<String> = sqlx::query_scalar("SELECT w.status FROM work_items w JOIN projects p ON p.id=w.project_id AND p.company_id=w.company_id JOIN companies c ON c.id=p.company_id JOIN workspaces ws ON ws.id=c.workspace_id WHERE w.id=? AND w.company_id=? AND w.project_id=? AND p.status='ACTIVE' AND c.status='ACTIVE' AND ws.status='ACTIVE'")
        .bind(w).bind(c).bind(p).fetch_optional(&mut **tx).await?;
    let reason = if !matches!(state.as_deref(), Some("BACKLOG" | "READY" | "IN_PROGRESS")) {
        "LIFECYCLE"
    } else {
        let blockers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blockers WHERE company_id=? AND project_id=? AND work_item_id=? AND resolved_at IS NULL").bind(c).bind(p).bind(w).fetch_one(&mut **tx).await?;
        let dependencies: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM work_dependencies d JOIN work_items parent ON parent.id=d.depends_on_work_item_id AND parent.company_id=d.company_id AND parent.project_id=d.project_id WHERE d.company_id=? AND d.project_id=? AND d.work_item_id=? AND d.dependency_kind='HARD' AND parent.status!='COMPLETED'").bind(c).bind(p).bind(w).fetch_one(&mut **tx).await?;
        let inputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM work_required_inputs i LEFT JOIN artifact_versions v ON v.id=i.artifact_version_id AND v.company_id=i.company_id LEFT JOIN artifacts a ON a.id=v.artifact_id AND a.company_id=i.company_id WHERE i.company_id=? AND i.project_id=? AND i.work_item_id=? AND (a.id IS NULL OR a.lifecycle_state!='ACTIVE' OR a.project_id IS NOT i.project_id)").bind(c).bind(p).bind(w).fetch_one(&mut **tx).await?;
        if blockers > 0 {
            "BLOCKER"
        } else if dependencies > 0 {
            "HARD_DEPENDENCY"
        } else if inputs > 0 {
            "INPUT"
        } else {
            "READY"
        }
    };
    Ok(WorkReadiness {
        work_item_id: w.into(),
        ready: reason == "READY",
        reason: reason.into(),
    })
}
impl ApplicationContext {
    pub async fn begin_scheduled_work(
        &self,
        company: &str,
        project: &str,
        work: &str,
    ) -> Result<(), ApplicationError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let configured:i64=sqlx::query_scalar("SELECT COUNT(*) FROM work_requirements WHERE company_id=? AND project_id=? AND work_item_id=?").bind(company).bind(project).bind(work).fetch_one(&mut *tx).await?;
        if configured != 0 {
            if !readiness(&mut tx, company, project, work).await?.ready {
                return Err(invalid("Work readiness revoked"));
            }
            let row:Option<i64>=sqlx::query_scalar("UPDATE work_items SET status='IN_PROGRESS',row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND project_id=? AND status='READY' RETURNING row_version").bind(Utc::now().to_rfc3339()).bind(work).bind(company).bind(project).fetch_optional(&mut *tx).await?;
            if let Some(version) = row {
                let cmd = StaffTeamCommand {
                    company_id: nalarvo_domain::CompanyId(company.into()),
                    project_id: project.into(),
                    team_id: String::new(),
                    meta: crate::CommandMeta {
                        principal: Some(nalarvo_domain::PrincipalRef::system("core-scheduler")),
                        ..Default::default()
                    },
                };
                event(
                    &mut tx,
                    &cmd,
                    "WorkItemStarted",
                    "WorkItem",
                    work,
                    version,
                    serde_json::json!({"status":"IN_PROGRESS"}),
                )
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn configure_work(
        &self,
        cmd: StaffTeamCommand,
        work: &str,
        input: WorkRequirements,
    ) -> Result<(), ApplicationError> {
        if input.role_id.trim().is_empty()
            || !(-1000..=1000).contains(&input.priority)
            || input.acceptance_contains.trim().is_empty()
            || input.acceptance_contains.len() > 4096
            || input.input_version_ids.len() > 64
        {
            return Err(invalid("Invalid work requirements"));
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(
            &mut tx,
            &cmd.company_id.0,
            &cmd.project_id,
            cmd.meta
                .principal
                .as_ref()
                .ok_or(ApplicationError::Unauthorized)?,
        )
        .await?;
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"configure_work":work,"requirements":input}),
        )
        .await?;
        let state: Option<String> = sqlx::query_scalar(
            "SELECT status FROM work_items WHERE id=? AND company_id=? AND project_id=?",
        )
        .bind(work)
        .bind(&cmd.company_id.0)
        .bind(&cmd.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        let attempts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs WHERE company_id=? AND project_id=? AND work_item_id=?",
        )
        .bind(&cmd.company_id.0)
        .bind(&cmd.project_id)
        .bind(work)
        .fetch_one(&mut *tx)
        .await?;
        if !matches!(state.as_deref(), Some("BACKLOG" | "READY")) || attempts != 0 {
            return Err(invalid("Executed work requires governed replan"));
        }
        sqlx::query("INSERT INTO work_requirements(company_id,project_id,work_item_id,role_id,priority,required,acceptance_contains) VALUES (?,?,?,?,?,?,?) ON CONFLICT(company_id,project_id,work_item_id) DO UPDATE SET role_id=excluded.role_id,priority=excluded.priority,required=excluded.required,acceptance_contains=excluded.acceptance_contains")
            .bind(&cmd.company_id.0).bind(&cmd.project_id).bind(work).bind(&input.role_id).bind(input.priority).bind(input.required).bind(&input.acceptance_contains).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM work_required_inputs WHERE company_id=? AND project_id=? AND work_item_id=?").bind(&cmd.company_id.0).bind(&cmd.project_id).bind(work).execute(&mut *tx).await?;
        for id in &input.input_version_ids {
            let scoped: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifact_versions v JOIN artifacts a ON a.id=v.artifact_id AND a.company_id=v.company_id WHERE v.id=? AND v.company_id=? AND a.project_id=? AND a.lifecycle_state='ACTIVE'").bind(id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut *tx).await?;
            if scoped != 1 {
                return Err(invalid("Input version scope invalid"));
            }
            sqlx::query("INSERT INTO work_required_inputs VALUES (?,?,?,?)")
                .bind(&cmd.company_id.0)
                .bind(&cmd.project_id)
                .bind(work)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        event(
            &mut tx,
            &cmd,
            "WorkRequirementsConfigured",
            "WorkItem",
            work,
            1,
            serde_json::json!({"priority":input.priority,"role_id":input.role_id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn work_readiness(
        &self,
        company: &str,
        project: &str,
        work: &str,
    ) -> Result<WorkReadiness, ApplicationError> {
        let mut tx = self.pool.begin().await?;
        readiness(&mut tx, company, project, work).await
    }
    pub async fn schedule_work(&self, cmd: StaffTeamCommand) -> Result<Vec<Run>, ApplicationError> {
        let actor = cmd
            .meta
            .principal
            .as_ref()
            .ok_or(ApplicationError::Unauthorized)?;
        let key = cmd
            .meta
            .idempotency_key
            .as_deref()
            .filter(|v| !v.trim().is_empty() && v.len() <= 128)
            .ok_or_else(|| invalid("Scheduler idempotency key required"))?;
        let hash = hash_request(&serde_json::to_vec(&cmd).map_err(|_| invalid("Invalid command"))?);
        let scope = format!(
            "company:{}:{}:{}:ScheduleWork",
            cmd.company_id.0, cmd.project_id, actor.principal_id
        );
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        if let IdempotencyCheck::Cached(body) =
            check_idempotency_tx(&mut tx, &scope, key, &hash).await?
        {
            return serde_json::from_str(&body).map_err(|_| invalid("Invalid scheduler replay"));
        }
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"schedule_team":cmd.team_id}),
        )
        .await?;
        let team: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM teams WHERE id=? AND company_id=? AND project_id=? AND status='ACTIVE'").bind(&cmd.team_id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut *tx).await?;
        if team != 1 {
            return Err(invalid("Active team required"));
        }
        let works = sqlx::query("SELECT w.id,r.role_id,r.priority FROM work_items w JOIN work_requirements r ON r.work_item_id=w.id AND r.company_id=w.company_id AND r.project_id=w.project_id WHERE w.company_id=? AND w.project_id=? AND w.status IN ('BACKLOG','READY') AND NOT EXISTS(SELECT 1 FROM runs run WHERE run.work_item_id=w.id) ORDER BY r.priority DESC,w.created_at,w.id LIMIT 100")
            .bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_all(&mut *tx).await?;
        let mut runs = Vec::new();
        for work in works {
            let id: String = work.get("id");
            if !readiness(&mut tx, &cmd.company_id.0, &cmd.project_id, &id)
                .await?
                .ready
            {
                continue;
            }
            let role: String = work.get("role_id");
            let candidate = sqlx::query("SELECT al.id AS allocation_id,a.id AS agent_id,a.model_profile_id FROM agent_allocations al JOIN agents a ON a.id=al.agent_id AND a.company_id=al.company_id JOIN departments d ON d.id=a.primary_department_id AND d.company_id=a.company_id AND d.status='ACTIVE' JOIN roles r ON r.id=a.role_id AND r.company_id=a.company_id AND r.status='ACTIVE' WHERE al.company_id=? AND al.project_id=? AND al.team_id=? AND al.status='ACTIVE' AND a.status='ACTIVE' AND a.role_id=? AND NOT EXISTS(SELECT 1 FROM runs run WHERE run.executing_agent_id=a.id AND run.lifecycle_state NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED')) ORDER BY a.created_at,a.id,al.id LIMIT 1")
                .bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&cmd.team_id).bind(&role).fetch_optional(&mut *tx).await?;
            let Some(candidate) = candidate else {
                continue;
            };
            let agent: String = candidate.get("agent_id");
            let allocation: String = candidate.get("allocation_id");
            let existing: Option<(String,String,String)> = sqlx::query_as("SELECT id,agent_id,agent_allocation_id FROM assignments WHERE company_id=? AND project_id=? AND work_item_id=? AND is_primary=1 AND status='ACTIVE'").bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&id).fetch_optional(&mut *tx).await?;
            let assignment = if let Some((assignment, assigned, assigned_allocation)) = existing {
                if assigned != agent || assigned_allocation != allocation {
                    continue;
                }
                assignment
            } else {
                let assignment = Uuid::now_v7().to_string();
                sqlx::query("INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,is_primary,status,assigned_at) VALUES (?,?,?,?,?,?,1,'ACTIVE',?)").bind(&assignment).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&id).bind(&agent).bind(&allocation).bind(Utc::now().to_rfc3339()).execute(&mut *tx).await?;
                event(&mut tx,&cmd,"WorkAssigned","Assignment",&assignment,1,serde_json::json!({"work_item_id":id,"agent_id":agent,"allocation_id":allocation})).await?;
                assignment
            };
            let at = Utc::now();
            let correlation = cmd
                .meta
                .correlation_id
                .clone()
                .unwrap_or_else(|| Uuid::now_v7().to_string());
            let run = Run {
                id: Uuid::now_v7().to_string(),
                company_id: cmd.company_id.clone(),
                project_id: cmd.project_id.clone(),
                work_item_id: id.clone(),
                assignment_id: Some(assignment),
                executing_agent_id: agent,
                status: RunStatus::Queued,
                trigger_type: "SCHEDULER".into(),
                attempt_number: 1,
                retry_of_run_id: None,
                model_profile_version_id: candidate.get("model_profile_id"),
                requested_by: actor.clone(),
                queued_at: at,
                started_at: None,
                completed_at: None,
                failure_class: None,
                failure_detail: None,
                correlation_id: correlation.clone(),
                causation_id: cmd.meta.causation_id.clone(),
                row_version: 1,
                created_at: at,
                updated_at: at,
            };
            sqlx::query("UPDATE work_items SET status='READY',row_version=row_version+1,updated_at=? WHERE id=? AND status='BACKLOG'").bind(at.to_rfc3339()).bind(&id).execute(&mut *tx).await?;
            nalarvo_persistence::m4::insert_run_tx(&mut tx, &run).await?;
            event(
                &mut tx,
                &cmd,
                "RunCreated",
                "Run",
                &run.id,
                1,
                serde_json::json!({"work_item_id":id,"trigger_type":"SCHEDULER"}),
            )
            .await?;
            sqlx::query("INSERT INTO durable_jobs(id,job_type,company_id,run_id,payload,available_at,priority,attempt,max_attempts,status,correlation_id,created_at,updated_at) VALUES (?,'EXECUTE_RUN',?,?,?, ?,?,0,3,'PENDING',?,?,?)")
                .bind(format!("job-{}",run.id)).bind(&cmd.company_id.0).bind(&run.id).bind(serde_json::json!({"run_id":run.id,"project_id":cmd.project_id,"work_item_id":id,"executing_agent_id":run.executing_agent_id}).to_string()).bind(at.to_rfc3339()).bind(work.get::<i64,_>("priority")).bind(&correlation).bind(at.to_rfc3339()).bind(at.to_rfc3339()).execute(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "RunQueued",
                "Run",
                &run.id,
                1,
                serde_json::json!({"status":"QUEUED"}),
            )
            .await?;
            runs.push(run);
        }
        save_idempotency_record_tx(
            &mut tx,
            &scope,
            key,
            &hash,
            &serde_json::to_string(&runs).map_err(|_| invalid("Invalid schedule"))?,
        )
        .await?;
        tx.commit().await?;
        Ok(runs)
    }
}

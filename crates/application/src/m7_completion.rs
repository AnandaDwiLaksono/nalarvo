use crate::m7::{StaffTeamCommand, event, owner_scope};
use crate::{ApplicationContext, ApplicationError};
use chrono::Utc;
use nalarvo_domain::Project;
use sqlx::{Row, Sqlite, Transaction};
fn invalid(s: &str) -> ApplicationError {
    ApplicationError::Validation(s.into())
}
pub(crate) async fn completion_guard(
    tx: &mut Transaction<'_, Sqlite>,
    company: &str,
    project: &str,
) -> Result<(), ApplicationError> {
    let bad:i64=sqlx::query_scalar("WITH scoped_actions AS (SELECT a.id FROM actions a WHERE a.company_id=?1 AND (a.project_id=?2 OR EXISTS(SELECT 1 FROM runs r WHERE r.id=a.run_id AND r.company_id=a.company_id AND r.project_id=?2) OR EXISTS(SELECT 1 FROM domain_events e WHERE e.aggregate_id=a.id AND e.company_id=a.company_id AND e.event_type='ActionProposed' AND json_extract(e.payload,'$.project_id')=?2))) SELECT (SELECT COUNT(*) FROM work_items w LEFT JOIN work_requirements q ON q.work_item_id=w.id WHERE w.company_id=?1 AND w.project_id=?2 AND ((COALESCE(q.required,1)=1 AND w.status!='COMPLETED') OR (w.status='COMPLETED' AND q.work_item_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM work_evaluations e WHERE e.work_item_id=w.id AND e.accepted=1 AND e.outcome='PASS'))))+(SELECT COUNT(*) FROM objectives WHERE company_id=?1 AND project_id=?2 AND is_required=1 AND status!='ACHIEVED')+(SELECT COUNT(*) FROM runs WHERE company_id=?1 AND project_id=?2 AND lifecycle_state NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED'))+(SELECT COUNT(*) FROM actions a JOIN scoped_actions s ON s.id=a.id WHERE a.status IN ('PENDING','EXECUTING','STARTED','OUTCOME_UNKNOWN'))+(SELECT COUNT(*) FROM resource_reservations rr JOIN budgets b ON b.id=rr.budget_id AND b.company_id=rr.company_id AND b.workspace_id=rr.workspace_id WHERE rr.company_id=?1 AND rr.status='HELD' AND (b.project_id=?2 OR rr.action_id IN (SELECT id FROM scoped_actions) OR EXISTS(SELECT 1 FROM model_invocations m JOIN runs r ON r.id=m.run_id AND r.company_id=m.company_id WHERE m.company_id=rr.company_id AND r.project_id=?2 AND rr.id='model:'||m.id||':'||rr.budget_id)))+(SELECT COUNT(*) FROM blockers WHERE company_id=?1 AND project_id=?2 AND resolved_at IS NULL)")
        .bind(company).bind(project).fetch_one(&mut **tx).await?;
    let approvals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM approval_requests p JOIN actions a ON a.id=p.action_id AND a.company_id=p.company_id WHERE p.company_id=?1 AND p.status='PENDING' AND (a.project_id=?2 OR EXISTS(SELECT 1 FROM runs r WHERE r.id=a.run_id AND r.company_id=a.company_id AND r.project_id=?2) OR EXISTS(SELECT 1 FROM domain_events e WHERE e.company_id=a.company_id AND e.aggregate_id=p.id AND e.event_type='ApprovalRequested' AND json_extract(e.payload,'$.project_id')=?2))")
        .bind(company).bind(project).fetch_one(&mut **tx).await?;
    let model: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM model_dispatches d WHERE d.company_id=? AND d.project_id=? AND (d.status IN ('PREPARED','DISPATCHING','OUTCOME_UNKNOWN') OR EXISTS(SELECT 1 FROM resource_reservations r WHERE r.model_dispatch_id=d.id AND r.status='HELD'))")
        .bind(company).bind(project).fetch_one(&mut **tx).await?;
    if bad != 0 || approvals != 0 || model != 0 {
        return Err(invalid(
            "Project acceptance, approval, active Run, reservation or blocker incomplete",
        ));
    }
    Ok(())
}
impl ApplicationContext {
    pub async fn complete_project_governed(
        &self,
        cmd: StaffTeamCommand,
        expected: i64,
    ) -> Result<Project, ApplicationError> {
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
            .ok_or_else(|| invalid("Completion idempotency key required"))?;
        let hash = nalarvo_persistence::hash_request(
            &serde_json::to_vec(&(&cmd, expected)).map_err(|_| invalid("Invalid completion"))?,
        );
        let scope = format!(
            "company:{}:{}:{}:CompleteProject",
            cmd.company_id.0, cmd.project_id, actor.principal_id
        );
        let mut result = self.get_project(&cmd.company_id, &cmd.project_id).await?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let membership:i64=sqlx::query_scalar("SELECT COUNT(*) FROM companies c JOIN workspaces w ON w.id=c.workspace_id JOIN workspace_memberships m ON m.workspace_id=w.id WHERE c.id=? AND c.status='ACTIVE' AND w.status='ACTIVE' AND m.user_id=?").bind(&cmd.company_id.0).bind(&actor.principal_id).fetch_one(&mut *tx).await?;
        if actor.principal_type != nalarvo_domain::PrincipalType::User || membership != 1 {
            return Err(ApplicationError::Unauthorized);
        }
        if let nalarvo_persistence::IdempotencyCheck::Cached(body) =
            nalarvo_persistence::check_idempotency_tx(&mut tx, &scope, key, &hash).await?
        {
            return serde_json::from_str(&body).map_err(|_| invalid("Invalid completion replay"));
        }
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"complete_project":true,"expected_version":expected}),
        )
        .await?;
        completion_guard(&mut tx, &cmd.company_id.0, &cmd.project_id).await?;
        let at = Utc::now().to_rfc3339();
        let allocations=sqlx::query("SELECT id,row_version FROM agent_allocations WHERE company_id=? AND project_id=? AND status IN ('ACTIVE','PLANNED','PAUSED')").bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_all(&mut *tx).await?;
        for a in allocations {
            let id: String = a.get("id");
            let v: i64 = a.get("row_version");
            sqlx::query("UPDATE agent_allocations SET status='RELEASED',released_at=?,updated_at=?,row_version=row_version+1 WHERE id=?").bind(&at).bind(&at).bind(&id).execute(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "AgentAllocationReleased",
                "AgentAllocation",
                &id,
                v + 1,
                serde_json::json!({"project_completed":true}),
            )
            .await?;
        }
        sqlx::query("UPDATE assignments SET status='RELEASED',ended_at=?,row_version=row_version+1 WHERE company_id=? AND project_id=? AND status='ACTIVE'").bind(&at).bind(&cmd.company_id.0).bind(&cmd.project_id).execute(&mut *tx).await?;
        let teams=sqlx::query("SELECT id,row_version FROM teams WHERE company_id=? AND project_id=? AND status IN ('ACTIVE','FORMING','PAUSED')").bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_all(&mut *tx).await?;
        for t in teams {
            let id: String = t.get("id");
            let v: i64 = t.get("row_version");
            sqlx::query("UPDATE teams SET status='DISBANDED',updated_at=?,row_version=row_version+1 WHERE id=?").bind(&at).bind(&id).execute(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "TeamDisbanded",
                "Team",
                &id,
                v + 1,
                serde_json::json!({"project_completed":true}),
            )
            .await?;
        }
        let n=sqlx::query("UPDATE projects SET status='COMPLETED',row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND status='ACTIVE' AND row_version=?").bind(&at).bind(&cmd.project_id).bind(&cmd.company_id.0).bind(expected).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(invalid("Stale version or invalid completion lifecycle"));
        }
        event(
            &mut tx,
            &cmd,
            "ProjectCompleted",
            "Project",
            &cmd.project_id,
            expected + 1,
            serde_json::json!({"status":"COMPLETED"}),
        )
        .await?;
        result.status = nalarvo_domain::ProjectStatus::Completed;
        result.row_version = expected + 1;
        result.updated_at = Utc::now();
        nalarvo_persistence::save_idempotency_record_tx(
            &mut tx,
            &scope,
            key,
            &hash,
            &serde_json::to_string(&result).map_err(|_| invalid("Invalid project"))?,
        )
        .await?;
        tx.commit().await?;
        Ok(result)
    }
}

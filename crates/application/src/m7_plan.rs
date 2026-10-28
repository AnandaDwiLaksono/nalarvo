use crate::m7::{StaffTeamCommand, event, owner_scope};
use crate::{ApplicationContext, ApplicationError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedWork {
    pub key: String,
    pub title: String,
    pub work_type: nalarvo_domain::WorkItemType,
    pub role_id: String,
    pub priority: i64,
    pub acceptance_contains: String,
    pub depends_on: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedStaff {
    pub role_id: String,
    pub department_id: Option<String>,
    pub desired_count: u32,
    pub required_capability_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectPlanProposal {
    pub company_id: String,
    pub project_id: String,
    pub work: Vec<ProposedWork>,
    pub staffing: Vec<ProposedStaff>,
    pub cancel_work_item_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanProposalRecord {
    pub id: String,
    pub project_version: i64,
    pub proposal: ProjectPlanProposal,
}
fn invalid(s: &str) -> ApplicationError {
    ApplicationError::Validation(s.into())
}
fn object<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, ApplicationError> {
    if raw.len() > 65536 {
        return Err(invalid("Proposal too large"));
    }
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| invalid("Invalid structured proposal"))?;
    if !value.is_object() {
        return Err(invalid("Proposal must be object"));
    }
    serde_json::from_value(value).map_err(|_| invalid("Invalid proposal schema"))
}
async fn validate(
    tx: &mut Transaction<'_, Sqlite>,
    cmd: &StaffTeamCommand,
    plan: &ProjectPlanProposal,
) -> Result<(), ApplicationError> {
    if plan.company_id != cmd.company_id.0
        || plan.project_id != cmd.project_id
        || plan.work.is_empty()
        || plan.work.len() > 32
        || plan.staffing.len() > 16
        || plan.cancel_work_item_ids.len() > 32
    {
        return Err(invalid("Invalid proposal scope or bounds"));
    }
    let mut edges = BTreeMap::new();
    for w in &plan.work {
        if w.key.is_empty()
            || w.key.len() > 64
            || !w
                .key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || w.title.trim().is_empty()
            || w.title.len() > 256
            || !(-1000..=1000).contains(&w.priority)
            || w.acceptance_contains.trim().is_empty()
            || w.acceptance_contains.len() > 4096
            || w.depends_on.len() > 32
            || edges.insert(w.key.clone(), w.depends_on.clone()).is_some()
        {
            return Err(invalid("Invalid proposed work"));
        }
        let role: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM roles WHERE id=? AND company_id=? AND status='ACTIVE'",
        )
        .bind(&w.role_id)
        .bind(&cmd.company_id.0)
        .fetch_one(&mut **tx)
        .await?;
        if role != 1 {
            return Err(invalid("Proposed role invalid"));
        }
    }
    let mut resolved = BTreeSet::new();
    while resolved.len() < edges.len() {
        let before = resolved.len();
        for (key, deps) in &edges {
            let mut unique = BTreeSet::new();
            if deps
                .iter()
                .any(|d| !edges.contains_key(d) || d == key || !unique.insert(d))
            {
                return Err(invalid("Invalid proposed dependency"));
            }
            if deps.iter().all(|d| resolved.contains(d)) {
                resolved.insert(key.clone());
            }
        }
        if resolved.len() == before {
            return Err(invalid("Proposed dependency cycle"));
        }
    }
    for s in &plan.staffing {
        if s.desired_count == 0
            || s.desired_count > 100
            || s.required_capability_ids.len() > 64
            || s.required_capability_ids
                .iter()
                .any(|v| v.trim().is_empty() || v.len() > 128)
        {
            return Err(invalid("Invalid proposed staffing"));
        }
        let role:i64=sqlx::query_scalar("SELECT COUNT(*) FROM roles r WHERE r.id=? AND r.company_id=? AND r.status='ACTIVE' AND (? IS NULL OR EXISTS(SELECT 1 FROM department_roles dr JOIN departments d ON d.id=dr.department_id AND d.company_id=dr.company_id WHERE dr.company_id=r.company_id AND dr.role_id=r.id AND d.id=? AND d.status='ACTIVE'))")
            .bind(&s.role_id).bind(&cmd.company_id.0).bind(&s.department_id).bind(&s.department_id).fetch_one(&mut **tx).await?;
        if role != 1 {
            return Err(invalid("Invalid staffing scope"));
        }
    }
    let mut cancelled = BTreeSet::new();
    for id in &plan.cancel_work_item_ids {
        if !cancelled.insert(id) {
            return Err(invalid("Duplicate future work cancellation"));
        }
        let valid:i64=sqlx::query_scalar("SELECT COUNT(*) FROM work_items w WHERE w.id=? AND w.company_id=? AND w.project_id=? AND w.status IN ('BACKLOG','READY') AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.work_item_id=w.id) AND NOT EXISTS(SELECT 1 FROM work_dependencies d JOIN work_items child ON child.id=d.work_item_id WHERE d.depends_on_work_item_id=w.id AND child.status NOT IN ('CANCELLED','COMPLETED'))")
            .bind(id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut **tx).await?;
        if valid != 1 {
            return Err(invalid(
                "Replan cannot alter executed, completed or required history",
            ));
        }
    }
    Ok(())
}
impl ApplicationContext {
    /// Model generation is an existing durable planning WorkItem, never direct provider access.
    pub async fn generate_project_plan(
        &self,
        cmd: StaffTeamCommand,
        planner_role: &str,
        reason: &str,
    ) -> Result<String, ApplicationError> {
        if reason.trim().is_empty() || reason.len() > 1024 {
            return Err(invalid("Planning trigger required"));
        }
        let actor = cmd
            .meta
            .principal
            .as_ref()
            .ok_or(ApplicationError::Unauthorized)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"generate_plan":reason,"role_id":planner_role}),
        )
        .await?;
        let role: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM roles WHERE id=? AND company_id=? AND status='ACTIVE'",
        )
        .bind(planner_role)
        .bind(&cmd.company_id.0)
        .fetch_one(&mut *tx)
        .await?;
        if role != 1 {
            return Err(invalid("Planning role scope invalid"));
        }
        let id = Uuid::now_v7().to_string();
        let at = Utc::now().to_rfc3339();
        let description = format!(
            "GenerateProjectPlan. Trigger: {reason}. Return JSON object only: {{\"company_id\":\"{}\",\"project_id\":\"{}\",\"work\":[{{\"key\":\"task\",\"title\":\"Task\",\"work_type\":\"TASK\",\"role_id\":\"existing scoped role\",\"priority\":0,\"acceptance_contains\":\"done\",\"depends_on\":[]}}],\"staffing\":[],\"cancel_work_item_ids\":[]}}. Proposal only. Never mutate state. Use only existing scoped roles.",
            cmd.company_id.0, cmd.project_id
        );
        sqlx::query("INSERT INTO work_items(id,company_id,project_id,title,description,logical_type,status,created_at,updated_at) VALUES (?,?,?,'GenerateProjectPlan',?,'TASK','BACKLOG',?,?)").bind(&id).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&description).bind(&at).bind(&at).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO work_requirements VALUES (?,?,?,?,1000,0,'project_id')")
            .bind(&cmd.company_id.0)
            .bind(&cmd.project_id)
            .bind(&id)
            .bind(planner_role)
            .execute(&mut *tx)
            .await?;
        event(
            &mut tx,
            &cmd,
            "ProjectPlanningRequested",
            "WorkItem",
            &id,
            1,
            serde_json::json!({"reason":reason,"role_id":planner_role}),
        )
        .await?;
        tx.commit().await?;
        Ok(id)
    }
    /// The originating user collects asynchronous output under current authority;
    /// worker identity and correlation metadata are not delegation credentials.
    pub async fn collect_plan_proposal(
        &self,
        cmd: StaffTeamCommand,
        run: &str,
        version: &str,
    ) -> Result<PlanProposalRecord, ApplicationError> {
        let actor = cmd
            .meta
            .principal
            .as_ref()
            .ok_or(ApplicationError::Unauthorized)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"collect_plan":run,"source_version_id":version}),
        )
        .await?;
        // BEGIN IMMEDIATE serializes current authority, provenance and the unique
        // source_run_id insert with revocation and concurrent collectors.
        let origins = sqlx::query("SELECT e.principal_type,e.principal_id,e.correlation_id,e.causation_id FROM runs r JOIN work_items w ON w.id=r.work_item_id AND w.company_id=r.company_id AND w.project_id=r.project_id JOIN work_requirements q ON q.work_item_id=w.id AND q.company_id=w.company_id AND q.project_id=w.project_id JOIN domain_events e ON e.aggregate_id=w.id AND e.company_id=w.company_id AND e.aggregate_type='WorkItem' AND e.event_type='ProjectPlanningRequested' WHERE r.id=? AND r.company_id=? AND r.project_id=? AND w.title='GenerateProjectPlan'")
            .bind(run).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_all(&mut *tx).await?;
        if origins.len() != 1
            || origins[0].get::<String, _>("principal_type") != "USER"
            || origins[0].get::<String, _>("principal_id") != actor.principal_id
            || origins[0]
                .get::<String, _>("correlation_id")
                .trim()
                .is_empty()
            || origins[0]
                .get::<String, _>("causation_id")
                .trim()
                .is_empty()
        {
            return Err(ApplicationError::Unauthorized);
        }
        let row=sqlx::query("SELECT a.id,p.row_version FROM artifact_versions v JOIN artifacts a ON a.id=v.artifact_id AND a.company_id=v.company_id JOIN runs r ON r.id=a.run_id AND r.company_id=a.company_id AND r.project_id=a.project_id AND r.work_item_id=a.work_item_id JOIN projects p ON p.id=r.project_id WHERE v.id=? AND v.company_id=? AND r.project_id=? AND r.id=? AND r.lifecycle_state='SUCCEEDED' AND EXISTS(SELECT 1 FROM model_invocations m JOIN context_snapshots s ON s.id=json_extract(m.request_metadata,'$.context_snapshot_id') AND s.company_id=m.company_id AND s.run_id=m.run_id WHERE m.run_id=r.id AND m.status='SUCCEEDED')")
            .bind(version).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(run).fetch_optional(&mut *tx).await?.ok_or_else(||invalid("Proposal requires trusted model output"))?;
        let artifact: String = row.get("id");
        let bytes = self
            .read_artifact_content(&cmd.company_id.0, &artifact, version)
            .await?;
        let raw = std::str::from_utf8(&bytes).map_err(|_| invalid("Invalid proposal encoding"))?;
        let canonical:Option<String>=sqlx::query_scalar("SELECT json_extract(output_payload,'$.text') FROM runtime_results WHERE run_id=? AND company_id=?").bind(run).bind(&cmd.company_id.0).fetch_optional(&mut *tx).await?.flatten();
        if canonical.as_deref() != Some(raw) {
            return Err(invalid("Proposal must match immutable runtime output"));
        }
        let plan: ProjectPlanProposal = object(raw)?;
        validate(&mut tx, &cmd, &plan).await?;
        if let Some((id,pv,raw))=sqlx::query_as::<_,(String,i64,String)>("SELECT id,project_version,proposal FROM plan_proposals WHERE source_run_id=? AND company_id=? AND project_id=?").bind(run).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_optional(&mut *tx).await? {return Ok(PlanProposalRecord{id,project_version:pv,proposal:object(&raw)?});}
        let record = PlanProposalRecord {
            id: Uuid::now_v7().to_string(),
            project_version: row.get("row_version"),
            proposal: plan,
        };
        sqlx::query("INSERT INTO plan_proposals VALUES (?,?,?,?,?,?,?,?)")
            .bind(&record.id)
            .bind(&cmd.company_id.0)
            .bind(&cmd.project_id)
            .bind(run)
            .bind(version)
            .bind(record.project_version)
            .bind(raw)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *tx)
            .await?;
        event(&mut tx,&cmd,"ProjectPlanProposed","Project",&cmd.project_id,record.project_version,serde_json::json!({"proposal_id":record.id,"source_run_id":run,"source_version_id":version})).await?;
        tx.commit().await?;
        Ok(record)
    }
    pub async fn decide_project_plan(
        &self,
        cmd: StaffTeamCommand,
        proposal_id: &str,
        apply: bool,
        confirmed: bool,
        reason: &str,
    ) -> Result<Vec<String>, ApplicationError> {
        if reason.trim().is_empty() || reason.len() > 1024 || (apply && !confirmed) {
            return Err(invalid(
                "Explicit material plan confirmation and reason required",
            ));
        }
        let actor = cmd
            .meta
            .principal
            .as_ref()
            .ok_or(ApplicationError::Unauthorized)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        let row=sqlx::query("SELECT proposal,project_version FROM plan_proposals WHERE id=? AND company_id=? AND project_id=?").bind(proposal_id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_optional(&mut *tx).await?.ok_or_else(||invalid("Proposal not found"))?;
        let plan: ProjectPlanProposal = object(&row.get::<String, _>("proposal"))?;
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"proposal_id":proposal_id,"apply":apply,"reason":reason}),
        )
        .await?;
        if sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM plan_decisions WHERE proposal_id=?")
            .bind(proposal_id)
            .fetch_one(&mut *tx)
            .await?
            != 0
        {
            return Err(invalid("Proposal already decided"));
        }
        let version: i64 =
            sqlx::query_scalar("SELECT row_version FROM projects WHERE id=? AND company_id=?")
                .bind(&cmd.project_id)
                .bind(&cmd.company_id.0)
                .fetch_one(&mut *tx)
                .await?;
        if version != row.get::<i64, _>("project_version") {
            return Err(invalid("Stale plan proposal"));
        }
        let mut ids = BTreeMap::new();
        if apply {
            validate(&mut tx, &cmd, &plan).await?;
            let team:i64=sqlx::query_scalar("SELECT COUNT(*) FROM teams WHERE id=? AND company_id=? AND project_id=? AND status IN ('FORMING','ACTIVE')").bind(&cmd.team_id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut *tx).await?;
            if team != 1 {
                return Err(invalid("Plan team scope invalid"));
            }
            let at = Utc::now().to_rfc3339();
            for w in &plan.work {
                let id = Uuid::now_v7().to_string();
                ids.insert(w.key.clone(), id.clone());
                sqlx::query("INSERT INTO work_items(id,company_id,project_id,title,logical_type,status,created_at,updated_at) VALUES (?,?,?,?,?,'BACKLOG',?,?)").bind(&id).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&w.title).bind(w.work_type.to_string()).bind(&at).bind(&at).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO work_requirements VALUES (?,?,?,?,?,1,?)")
                    .bind(&cmd.company_id.0)
                    .bind(&cmd.project_id)
                    .bind(&id)
                    .bind(&w.role_id)
                    .bind(w.priority)
                    .bind(&w.acceptance_contains)
                    .execute(&mut *tx)
                    .await?;
                event(
                    &mut tx,
                    &cmd,
                    "WorkItemCreated",
                    "WorkItem",
                    &id,
                    1,
                    serde_json::json!({"proposal_id":proposal_id}),
                )
                .await?;
            }
            for w in &plan.work {
                for dep in &w.depends_on {
                    sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD',?)")
                        .bind(&cmd.company_id.0)
                        .bind(&cmd.project_id)
                        .bind(&ids[&w.key])
                        .bind(&ids[dep])
                        .bind(&at)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            for s in &plan.staffing {
                let id = Uuid::now_v7().to_string();
                sqlx::query("INSERT INTO staffing_requirements(id,company_id,project_id,team_id,role_id,department_id,desired_count,required_capability_ids,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,'OPEN',?,?)").bind(&id).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&cmd.team_id).bind(&s.role_id).bind(&s.department_id).bind(i64::from(s.desired_count)).bind(serde_json::to_string(&s.required_capability_ids).map_err(|_|invalid("Invalid staffing"))?).bind(&at).bind(&at).execute(&mut *tx).await?;
                event(
                    &mut tx,
                    &cmd,
                    "StaffingRequirementOpened",
                    "StaffingRequirement",
                    &id,
                    1,
                    serde_json::json!({"proposal_id":proposal_id}),
                )
                .await?;
            }
            for id in &plan.cancel_work_item_ids {
                let v:i64=sqlx::query_scalar("UPDATE work_items SET status='CANCELLED',row_version=row_version+1,updated_at=? WHERE id=? RETURNING row_version").bind(&at).bind(id).fetch_one(&mut *tx).await?;
                event(
                    &mut tx,
                    &cmd,
                    "WorkItemCancelled",
                    "WorkItem",
                    id,
                    v,
                    serde_json::json!({"proposal_id":proposal_id}),
                )
                .await?;
            }
            sqlx::query("UPDATE projects SET row_version=row_version+1,updated_at=? WHERE id=?")
                .bind(&at)
                .bind(&cmd.project_id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("INSERT INTO plan_decisions VALUES (?,?,?,?,?,?,?)")
            .bind(proposal_id)
            .bind(&cmd.company_id.0)
            .bind(&cmd.project_id)
            .bind(if apply { "APPLY" } else { "REJECT" })
            .bind(&actor.principal_id)
            .bind(reason)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *tx)
            .await?;
        event(
            &mut tx,
            &cmd,
            if apply {
                "ProjectReplanned"
            } else {
                "ProjectPlanRejected"
            },
            "Project",
            &cmd.project_id,
            version + i64::from(apply),
            serde_json::json!({"proposal_id":proposal_id,"reason":reason}),
        )
        .await?;
        tx.commit().await?;
        Ok(ids.into_values().collect())
    }
}

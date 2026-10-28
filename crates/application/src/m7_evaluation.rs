use crate::m7::{StaffTeamCommand, event, owner_scope};
use crate::{ApplicationContext, ApplicationError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvaluationOutcome {
    Pass,
    Fail,
    NeedsRevision,
    Unsure,
}
impl EvaluationOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::NeedsRevision => "NEEDS_REVISION",
            Self::Unsure => "UNSURE",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluateWork {
    pub run_id: String,
    pub artifact_version_id: Option<String>,
    pub human_outcome: Option<EvaluationOutcome>,
    pub model_run_id: Option<String>,
    pub reason: String,
    pub accept: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkEvaluation {
    pub id: String,
    pub outcome: EvaluationOutcome,
    pub accepted: bool,
}
fn invalid(s: &str) -> ApplicationError {
    ApplicationError::Validation(s.into())
}
impl ApplicationContext {
    pub async fn revise_work_attempt(
        &self,
        cmd: StaffTeamCommand,
        previous_run: &str,
        instructions: &str,
        expected_version: i64,
    ) -> Result<nalarvo_domain::Run, ApplicationError> {
        if instructions.trim().is_empty() || instructions.len() > 4096 {
            return Err(invalid("Bounded revision instructions required"));
        }
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
            .ok_or_else(|| invalid("Revision idempotency key required"))?;
        let hash = nalarvo_persistence::hash_request(
            &serde_json::to_vec(&(&cmd, previous_run, instructions, expected_version))
                .map_err(|_| invalid("Invalid revision"))?,
        );
        let scope = format!(
            "company:{}:{}:{}:ReviseWork",
            cmd.company_id.0, cmd.project_id, actor.principal_id
        );
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        if let nalarvo_persistence::IdempotencyCheck::Cached(body) =
            nalarvo_persistence::check_idempotency_tx(&mut tx, &scope, key, &hash).await?
        {
            return serde_json::from_str(&body).map_err(|_| invalid("Invalid revision replay"));
        }
        crate::m7_governance::governed(&mut tx,&cmd,"project.modify",serde_json::json!({"revise_run":previous_run,"instructions":instructions,"expected_version":expected_version})).await?;
        let mut run = self
            .get_run(&cmd.company_id, &cmd.project_id, previous_run)
            .await?;
        let valid:i64=sqlx::query_scalar("SELECT COUNT(*) FROM runs r JOIN work_items w ON w.id=r.work_item_id JOIN assignments a ON a.id=r.assignment_id JOIN agent_allocations al ON al.id=a.agent_allocation_id JOIN teams t ON t.id=al.team_id WHERE r.id=? AND r.company_id=? AND r.project_id=? AND r.lifecycle_state='SUCCEEDED' AND r.row_version=? AND w.status='IN_PROGRESS' AND a.status='ACTIVE' AND al.status='ACTIVE' AND t.id=? AND t.status='ACTIVE' AND EXISTS(SELECT 1 FROM work_evaluations e WHERE e.run_id=r.id AND e.accepted=0 AND e.outcome IN ('FAIL','NEEDS_REVISION')) AND NOT EXISTS(SELECT 1 FROM runs later WHERE later.work_item_id=r.work_item_id AND later.attempt_number>r.attempt_number) AND NOT EXISTS(SELECT 1 FROM actions act JOIN runs ar ON ar.id=act.run_id WHERE ar.work_item_id=w.id AND act.status IN ('PENDING','EXECUTING','STARTED','OUTCOME_UNKNOWN'))")
            .bind(previous_run).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(expected_version).bind(&cmd.team_id).fetch_one(&mut *tx).await?;
        if valid != 1
            || !crate::m7_work::readiness(
                &mut tx,
                &cmd.company_id.0,
                &cmd.project_id,
                &run.work_item_id,
            )
            .await?
            .ready
        {
            return Err(invalid(
                "Revision requires latest evaluated successful attempt, current assignment and readiness",
            ));
        }
        let at = Utc::now();
        sqlx::query("UPDATE work_items SET description=COALESCE(description,'')||?,row_version=row_version+1,updated_at=? WHERE id=?")
            .bind(format!("\nRevision: {instructions}")).bind(at.to_rfc3339()).bind(&run.work_item_id).execute(&mut *tx).await?;
        run.id = Uuid::now_v7().to_string();
        run.retry_of_run_id = Some(previous_run.into());
        run.attempt_number += 1;
        run.trigger_type = "REVISION".into();
        run.status = nalarvo_domain::RunStatus::Queued;
        run.requested_by = actor.clone();
        run.started_at = None;
        run.completed_at = None;
        run.failure_class = None;
        run.failure_detail = None;
        run.queued_at = at;
        run.created_at = at;
        run.updated_at = at;
        run.row_version = 1;
        nalarvo_persistence::m4::insert_run_tx(&mut tx, &run).await?;
        sqlx::query("INSERT INTO durable_jobs(id,job_type,company_id,run_id,payload,available_at,priority,attempt,max_attempts,status,correlation_id,created_at,updated_at) VALUES (?,'EXECUTE_RUN',?,?,?, ?,0,0,3,'PENDING',?,?,?)")
            .bind(format!("job-{}",run.id)).bind(&cmd.company_id.0).bind(&run.id).bind(serde_json::json!({"run_id":run.id}).to_string()).bind(at.to_rfc3339()).bind(&run.correlation_id).bind(at.to_rfc3339()).bind(at.to_rfc3339()).execute(&mut *tx).await?;
        event(&mut tx,&cmd,"WorkRevisionRequested","WorkItem",&run.work_item_id,run.attempt_number as i64,serde_json::json!({"previous_run_id":previous_run,"run_id":run.id,"instructions":instructions})).await?;
        event(
            &mut tx,
            &cmd,
            "RunCreated",
            "Run",
            &run.id,
            1,
            serde_json::json!({"trigger_type":"REVISION","previous_run_id":previous_run}),
        )
        .await?;
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
        nalarvo_persistence::save_idempotency_record_tx(
            &mut tx,
            &scope,
            key,
            &hash,
            &serde_json::to_string(&run).map_err(|_| invalid("Invalid revision"))?,
        )
        .await?;
        tx.commit().await?;
        Ok(run)
    }
    pub async fn evaluate_work(
        &self,
        cmd: StaffTeamCommand,
        input: EvaluateWork,
    ) -> Result<WorkEvaluation, ApplicationError> {
        if input.reason.trim().is_empty() || input.reason.len() > 4096 {
            return Err(invalid("Evaluation reason required"));
        }
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
            .ok_or_else(|| invalid("Evaluation idempotency key required"))?;
        let hash = nalarvo_persistence::hash_request(
            &serde_json::to_vec(&(&cmd, &input)).map_err(|_| invalid("Invalid evaluation"))?,
        );
        let scope = format!(
            "company:{}:{}:{}:EvaluateWork",
            cmd.company_id.0, cmd.project_id, actor.principal_id
        );
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        owner_scope(&mut tx, &cmd.company_id.0, &cmd.project_id, actor).await?;
        if let nalarvo_persistence::IdempotencyCheck::Cached(body) =
            nalarvo_persistence::check_idempotency_tx(&mut tx, &scope, key, &hash).await?
        {
            return serde_json::from_str(&body).map_err(|_| invalid("Invalid evaluation replay"));
        }
        crate::m7_governance::governed(
            &mut tx,
            &cmd,
            "project.modify",
            serde_json::json!({"evaluate":input}),
        )
        .await?;
        let row=sqlx::query("SELECT r.work_item_id,r.lifecycle_state,q.acceptance_contains,w.status FROM runs r JOIN work_items w ON w.id=r.work_item_id AND w.company_id=r.company_id AND w.project_id=r.project_id JOIN work_requirements q ON q.work_item_id=w.id WHERE r.id=? AND r.company_id=? AND r.project_id=?")
            .bind(&input.run_id).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_optional(&mut *tx).await?.ok_or_else(||invalid("Evaluation scope invalid"))?;
        let work: String = row.get("work_item_id");
        let state: String = row.get("lifecycle_state");
        if !matches!(
            state.as_str(),
            "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "CANCELLED"
        ) || row.get::<String, _>("status") == "COMPLETED"
        {
            return Err(invalid(
                "Evaluation requires terminal attempt and unaccepted work",
            ));
        }
        let latest:i64=sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE work_item_id=? AND created_at>(SELECT created_at FROM runs WHERE id=?)").bind(&work).bind(&input.run_id).fetch_one(&mut *tx).await?;
        if latest != 0 {
            return Err(invalid("Evaluation must reference latest attempt"));
        }
        let criteria: String = row.get("acceptance_contains");
        let text = if let Some(version) = &input.artifact_version_id {
            let artifact:Option<String>=sqlx::query_scalar("SELECT a.id FROM artifact_versions v JOIN artifacts a ON a.id=v.artifact_id AND a.company_id=v.company_id WHERE v.id=? AND v.company_id=? AND a.project_id=? AND a.work_item_id=? AND a.run_id=? AND a.lifecycle_state='ACTIVE'")
                .bind(version).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&work).bind(&input.run_id).fetch_optional(&mut *tx).await?;
            let artifact = artifact.ok_or_else(|| invalid("Evaluation Artifact scope invalid"))?;
            Some(
                String::from_utf8(
                    self.read_artifact_content(&cmd.company_id.0, &artifact, version)
                        .await?,
                )
                .map_err(|_| invalid("Evaluation requires text Artifact"))?,
            )
        } else {
            let payload: Option<String> = sqlx::query_scalar(
                "SELECT output_payload FROM runtime_results WHERE run_id=? AND company_id=?",
            )
            .bind(&input.run_id)
            .bind(&cmd.company_id.0)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
            payload
                .and_then(|p| serde_json::from_str::<serde_json::Value>(&p).ok())
                .and_then(|v| v.get("text").and_then(|t| t.as_str()).map(String::from))
        };
        // ponytail: exact substring acceptance is the deterministic MVP check; richer validators when criteria demand them.
        let deterministic = if state != "SUCCEEDED" {
            EvaluationOutcome::Fail
        } else if text.as_ref().is_some_and(|t| t.contains(&criteria)) {
            EvaluationOutcome::Pass
        } else if text.is_some() {
            EvaluationOutcome::NeedsRevision
        } else {
            EvaluationOutcome::Unsure
        };
        let model_outcome = if let Some(model_run) = &input.model_run_id {
            if input.human_outcome.is_some() {
                return Err(invalid("Choose model or human evaluation"));
            }
            let raw:Option<String>=sqlx::query_scalar("SELECT json_extract(rr.output_payload,'$.text') FROM runs r JOIN runtime_results rr ON rr.run_id=r.id AND rr.company_id=r.company_id JOIN runs target ON target.id=? WHERE r.id=? AND r.company_id=? AND r.project_id=? AND r.lifecycle_state='SUCCEEDED' AND r.executing_agent_id<>target.executing_agent_id AND r.id<>target.id AND EXISTS(SELECT 1 FROM model_invocations m JOIN context_snapshots s ON s.id=json_extract(m.request_metadata,'$.context_snapshot_id') AND s.run_id=m.run_id AND s.company_id=m.company_id WHERE m.run_id=r.id AND m.status='SUCCEEDED')")
                .bind(&input.run_id).bind(model_run).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_optional(&mut *tx).await?.flatten();
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Review {
                run_id: String,
                outcome: EvaluationOutcome,
                reason: String,
            }
            let raw = raw
                .filter(|s| s.len() <= 8192)
                .ok_or_else(|| invalid("Trusted independent model evaluation required"))?;
            let review: Review =
                serde_json::from_str(&raw).map_err(|_| invalid("Invalid structured evaluation"))?;
            if review.run_id != input.run_id
                || review.reason.trim().is_empty()
                || review.reason.len() > 4096
            {
                return Err(invalid("Evaluation target or reason invalid"));
            }
            Some(review.outcome)
        } else {
            None
        };
        let recommendation = input.human_outcome.or_else(|| match model_outcome {
            Some(EvaluationOutcome::Pass) if deterministic == EvaluationOutcome::Unsure => None,
            other => other,
        });
        let outcome = match recommendation {
            Some(EvaluationOutcome::Pass)
                if deterministic == EvaluationOutcome::Fail
                    || deterministic == EvaluationOutcome::NeedsRevision
                    || state != "SUCCEEDED" =>
            {
                return Err(invalid(
                    "Human acceptance cannot waive deterministic failure",
                ));
            }
            Some(v) => v,
            None => deterministic,
        };
        if input.accept && outcome != EvaluationOutcome::Pass {
            return Err(invalid("Only PASS may be accepted"));
        }
        if input.accept {
            let status: String = row.get("status");
            if status != "IN_PROGRESS" {
                return Err(invalid("Only IN_PROGRESS work may complete"));
            }
            if !crate::m7_work::readiness(&mut tx, &cmd.company_id.0, &cmd.project_id, &work)
                .await?
                .ready
            {
                return Err(invalid("Work blocked"));
            }
            let pending:i64=sqlx::query_scalar("SELECT COUNT(*) FROM actions a JOIN runs r ON r.id=a.run_id WHERE r.work_item_id=? AND (a.status IN ('PENDING','EXECUTING','OUTCOME_UNKNOWN') OR a.authorization IN ('APPROVAL_REQUIRED','REJECTED','EXPIRED'))").bind(&work).fetch_one(&mut *tx).await?;
            if pending != 0 {
                return Err(invalid("Work has unresolved Action or approval"));
            }
        }
        let evaluation = WorkEvaluation {
            id: Uuid::now_v7().to_string(),
            outcome,
            accepted: input.accept,
        };
        sqlx::query("INSERT INTO work_evaluations(id,company_id,project_id,work_item_id,run_id,artifact_version_id,outcome,method,accepted,criteria,reason,reviewer_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&evaluation.id).bind(&cmd.company_id.0).bind(&cmd.project_id).bind(&work).bind(&input.run_id).bind(&input.artifact_version_id).bind(outcome.as_str()).bind(if input.human_outcome.is_some(){"HUMAN"}else if input.model_run_id.is_some(){"MODEL"}else{"DETERMINISTIC"}).bind(input.accept).bind(&criteria).bind(&input.reason).bind(&actor.principal_id).bind(Utc::now().to_rfc3339()).execute(&mut *tx).await?;
        event(&mut tx,&cmd,"WorkEvaluated","WorkItem",&work,1,serde_json::json!({"evaluation_id":evaluation.id,"run_id":input.run_id,"outcome":outcome,"accepted":input.accept})).await?;
        if input.accept {
            let version:i64=sqlx::query_scalar("UPDATE work_items SET status='COMPLETED',row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND project_id=? RETURNING row_version").bind(Utc::now().to_rfc3339()).bind(&work).bind(&cmd.company_id.0).bind(&cmd.project_id).fetch_one(&mut *tx).await?;
            sqlx::query("UPDATE assignments SET status='RELEASED',ended_at=?,row_version=row_version+1 WHERE work_item_id=? AND status='ACTIVE'").bind(Utc::now().to_rfc3339()).bind(&work).execute(&mut *tx).await?;
            event(
                &mut tx,
                &cmd,
                "WorkItemCompleted",
                "WorkItem",
                &work,
                version,
                serde_json::json!({"evaluation_id":evaluation.id}),
            )
            .await?;
        }
        nalarvo_persistence::save_idempotency_record_tx(
            &mut tx,
            &scope,
            key,
            &hash,
            &serde_json::to_string(&evaluation).map_err(|_| invalid("Invalid evaluation"))?,
        )
        .await?;
        tx.commit().await?;
        Ok(evaluation)
    }
}

use crate::{ApplicationContext, ApplicationError, CommandMeta};
use chrono::Utc;
use nalarvo_domain::{
    AuditRecord, CompanyId, DomainEvent, ExecutionStep, PrincipalRef, Run, RunStatus, ScopeRef,
    WorkItemStatus,
};
use nalarvo_persistence::{
    IdempotencyCheck, check_idempotency_tx, hash_request, insert_domain_event_and_outbox_tx,
    m4::{
        DurableJob, RuntimeResult, append_step, cancel_run_with_result, get_run, list_runs,
        timeout_run_with_result, transition_run,
    },
    save_idempotency_record_tx,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRunCommand {
    pub company_id: CompanyId,
    pub project_id: String,
    pub work_item_id: String,
    pub executing_agent_id: String,
    pub trigger_type: String,
    pub assignment_id: Option<String>,
    pub retry_of_run_id: Option<String>,
    pub model_profile_version_id: Option<String>,
    #[serde(default)]
    pub meta: CommandMeta,
}

impl CreateRunCommand {
    pub fn new(
        company_id: CompanyId,
        project_id: impl Into<String>,
        work_item_id: impl Into<String>,
        executing_agent_id: impl Into<String>,
        trigger_type: impl Into<String>,
    ) -> Self {
        Self {
            company_id,
            project_id: project_id.into(),
            work_item_id: work_item_id.into(),
            executing_agent_id: executing_agent_id.into(),
            trigger_type: trigger_type.into(),
            assignment_id: None,
            retry_of_run_id: None,
            model_profile_version_id: None,
            meta: CommandMeta::default(),
        }
    }

    pub fn with_assignment(mut self, assignment_id: impl Into<String>) -> Self {
        self.assignment_id = Some(assignment_id.into());
        self
    }

    pub fn with_model_profile_version(mut self, version_id: impl Into<String>) -> Self {
        self.model_profile_version_id = Some(version_id.into());
        self
    }

    pub fn idempotency(mut self, key: impl Into<String>) -> Self {
        self.meta.idempotency_key = Some(key.into());
        self
    }
}

impl ApplicationContext {
    pub async fn create_run(&self, cmd: CreateRunCommand) -> Result<Run, ApplicationError> {
        let principal = cmd
            .meta
            .principal
            .clone()
            .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));
        let correlation_id = cmd
            .meta
            .correlation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let causation_id = cmd
            .meta
            .causation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| correlation_id.clone());

        // Scope validation against M3 tables
        self.validate_run_scope(
            &cmd.company_id,
            &cmd.project_id,
            &cmd.work_item_id,
            &cmd.executing_agent_id,
            cmd.assignment_id.as_deref(),
        )
        .await?;

        // Handle attempt and parent verification for retries
        let (attempt_number, retry_of) = if let Some(ref retry_id) = cmd.retry_of_run_id {
            let old = self
                .get_run(&cmd.company_id, &cmd.project_id, retry_id)
                .await?;
            if !matches!(old.status, RunStatus::Failed | RunStatus::TimedOut) {
                return Err(ApplicationError::Validation(
                    "Only failed or timed out runs can be retried".into(),
                ));
            }
            if old.work_item_id != cmd.work_item_id
                || old.executing_agent_id != cmd.executing_agent_id
                || old.assignment_id != cmd.assignment_id
            {
                return Err(ApplicationError::Validation(
                    "Retry must preserve work item, assignment, and executing agent".into(),
                ));
            }
            (old.attempt_number + 1, Some(retry_id.clone()))
        } else {
            (1, None)
        };

        let req_bytes =
            serde_json::to_vec(&cmd).map_err(|e| ApplicationError::Validation(e.to_string()))?;
        let req_hash = hash_request(&req_bytes);
        let idempotency_scope = format!(
            "company:{}:{}:{}:CreateRun",
            cmd.company_id.0, cmd.project_id, principal.principal_id
        );

        let mut tx = self.pool.begin().await?;

        if let Some(ref key) = cmd.meta.idempotency_key {
            let check = check_idempotency_tx(&mut tx, &idempotency_scope, key, &req_hash).await?;
            if let IdempotencyCheck::Cached(body) = check {
                let cached = serde_json::from_str(&body)
                    .map_err(|e| ApplicationError::Validation(e.to_string()))?;
                return Ok(cached);
            }
        }

        let now = Utc::now();
        let run = Run {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            work_item_id: cmd.work_item_id.clone(),
            assignment_id: cmd.assignment_id.clone(),
            executing_agent_id: cmd.executing_agent_id.clone(),
            status: RunStatus::Queued,
            trigger_type: cmd.trigger_type.clone(),
            attempt_number,
            retry_of_run_id: retry_of,
            model_profile_version_id: cmd.model_profile_version_id.clone(),
            requested_by: principal.clone(),
            queued_at: now,
            started_at: None,
            completed_at: None,
            failure_class: None,
            failure_detail: None,
            correlation_id: correlation_id.clone(),
            causation_id: Some(causation_id.clone()),
            row_version: 1,
            created_at: now,
            updated_at: now,
        };

        sqlx::query(
            "INSERT INTO runs(id, company_id, project_id, work_item_id, assignment_id, executing_agent_id, lifecycle_state, trigger_type, attempt_number, retry_of_run_id, model_profile_version_id, requested_by_type, requested_by_id, queued_at, started_at, completed_at, failure_class, failure_detail, correlation_id, causation_id, row_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&run.id)
        .bind(&run.company_id.0)
        .bind(&run.project_id)
        .bind(&run.work_item_id)
        .bind(&run.assignment_id)
        .bind(&run.executing_agent_id)
        .bind(run.status.to_string())
        .bind(&run.trigger_type)
        .bind(run.attempt_number as i64)
        .bind(&run.retry_of_run_id)
        .bind(&run.model_profile_version_id)
        .bind(run.requested_by.principal_type.to_string())
        .bind(&run.requested_by.principal_id)
        .bind(run.queued_at.to_rfc3339())
        .bind(run.started_at.map(|t| t.to_rfc3339()))
        .bind(run.completed_at.map(|t| t.to_rfc3339()))
        .bind(&run.failure_class)
        .bind(&run.failure_detail)
        .bind(&run.correlation_id)
        .bind(&run.causation_id)
        .bind(run.row_version)
        .bind(run.created_at.to_rfc3339())
        .bind(run.updated_at.to_rfc3339())
        .execute(&mut *tx)
        .await?;

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunCreated".into(),
            schema_version: 1,
            company_id: cmd.company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run.id.clone(),
            aggregate_version: 1,
            occurred_at: now,
            correlation_id: correlation_id.clone(),
            causation_id: causation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(&cmd.company_id.0),
            payload: serde_json::to_value(&run)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?,
        };
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;

        if let Some(ref key) = cmd.meta.idempotency_key {
            let body = serde_json::to_string(&run)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?;
            save_idempotency_record_tx(&mut tx, &idempotency_scope, key, &req_hash, &body).await?;
        }

        tx.commit().await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunCreated".into(),
            principal,
            scope: ScopeRef::company(&cmd.company_id.0),
            occurred_at: now,
            details: serde_json::json!({
                "company_id": cmd.company_id.0,
                "project_id": cmd.project_id,
                "id": run.id,
                "work_item_id": run.work_item_id,
                "correlation_id": correlation_id,
            }),
        });

        Ok(run)
    }

    pub async fn get_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
    ) -> Result<Run, ApplicationError> {
        self.get_project(company_id, project_id).await?;
        let run = get_run(&self.pool, company_id, run_id)
            .await?
            .filter(|r| r.project_id == project_id)
            .ok_or_else(|| ApplicationError::NotFound(run_id.into()))?;
        Ok(run)
    }

    pub async fn list_runs(
        &self,
        company_id: &CompanyId,
        project_id: &str,
    ) -> Result<Vec<Run>, ApplicationError> {
        self.get_project(company_id, project_id).await?;
        Ok(list_runs(&self.pool, company_id, project_id).await?)
    }

    pub async fn queue_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        meta: Option<CommandMeta>,
    ) -> Result<Run, ApplicationError> {
        let meta = meta.unwrap_or_default();
        let principal = meta
            .principal
            .clone()
            .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));
        let correlation_id = meta
            .correlation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());

        let run = self.get_run(company_id, project_id, run_id).await?;
        let idempotency_scope = format!(
            "company:{}:{}:{}:QueueRun",
            company_id.0, project_id, principal.principal_id
        );
        let req_bytes = serde_json::to_vec(&serde_json::json!({
            "run_id": run_id,
            "expected_version": expected_version
        }))
        .map_err(|e| ApplicationError::Validation(e.to_string()))?;
        let req_hash = hash_request(&req_bytes);

        if let Some(ref key) = meta.idempotency_key {
            let mut tx = self.pool.begin().await?;
            let check = check_idempotency_tx(&mut tx, &idempotency_scope, key, &req_hash).await?;
            if let IdempotencyCheck::Cached(body) = check {
                let cached = serde_json::from_str(&body)
                    .map_err(|e| ApplicationError::Validation(e.to_string()))?;
                return Ok(cached);
            }
        }

        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }

        // Admission validation check
        if let Err((_failure_class, failure_detail)) = self.check_admission(&run).await {
            self.reject_run_admission(
                company_id,
                project_id,
                run_id,
                expected_version,
                &failure_detail,
            )
            .await?;
            return Err(ApplicationError::Validation(format!(
                "Admission rejected: {failure_detail}"
            )));
        }

        // If run is already QUEUED, and durable job already exists, duplicate-safe return
        let existing_job: Option<String> = sqlx::query_scalar(
            "SELECT id FROM durable_jobs WHERE job_type = 'EXECUTE_RUN' AND run_id = ?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;

        if run.status == RunStatus::Queued && existing_job.is_some() {
            return Ok(run);
        }

        let job = DurableJob {
            id: format!("job-{}", run.id),
            job_type: "EXECUTE_RUN".into(),
            company_id: company_id.clone(),
            run_id: run.id.clone(),
            payload: serde_json::json!({
                "run_id": run.id,
                "project_id": project_id,
                "work_item_id": run.work_item_id,
                "executing_agent_id": run.executing_agent_id,
            }),
            available_at: Utc::now(),
            priority: 0,
            attempt: 0,
            max_attempts: 3,
            correlation_id: correlation_id.clone(),
        };

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunQueued".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run.id.clone(),
            aggregate_version: run.row_version + 1,
            occurred_at: Utc::now(),
            correlation_id: correlation_id.clone(),
            causation_id: run.id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run.id,
                "status": "QUEUED",
            }),
        };

        nalarvo_persistence::m4::queue_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            &job,
            &event,
        )
        .await?;

        let updated = self.get_run(company_id, project_id, run_id).await?;

        if let Some(ref key) = meta.idempotency_key {
            let body = serde_json::to_string(&updated)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?;
            let mut tx = self.pool.begin().await?;
            save_idempotency_record_tx(&mut tx, &idempotency_scope, key, &req_hash, &body).await?;
            tx.commit().await?;
        }

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunQueued".into(),
            principal,
            scope: ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": company_id.0,
                "project_id": project_id,
                "run_id": run.id,
            }),
        });

        Ok(updated)
    }

    pub async fn start_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }

        // Validate admission
        if let Err((_failure_class, failure_detail)) = self.check_admission(&run).await {
            self.reject_run_admission(
                company_id,
                project_id,
                run_id,
                expected_version,
                &failure_detail,
            )
            .await?;
            return Err(ApplicationError::Validation(format!(
                "Admission rejected: {failure_detail}"
            )));
        }

        // Transition run to RUNNING
        let updated_run = transition_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            RunStatus::Running,
            None,
            None,
        )
        .await?;

        // Coordinate WorkItem: If READY, move to IN_PROGRESS (once)
        let work_item = self.get_work_item(company_id, &run.work_item_id).await?;
        if work_item.status == WorkItemStatus::Ready {
            self.transition_work_item(
                company_id,
                project_id,
                &run.work_item_id,
                WorkItemStatus::InProgress,
                work_item.row_version,
            )
            .await?;
        }

        let principal = run.requested_by.clone();
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunStarted".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated_run.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: principal.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "RUNNING",
            }),
        };

        let mut tx = self.pool.begin().await?;
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunStarted".into(),
            principal,
            scope: ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": company_id.0,
                "project_id": project_id,
                "run_id": run_id,
            }),
        });

        Ok(updated_run)
    }

    pub async fn pause_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if run.status != RunStatus::Running {
            return Err(ApplicationError::Validation(
                "Only running runs can be paused".into(),
            ));
        }

        let updated = transition_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            RunStatus::Paused,
            None,
            None,
        )
        .await?;

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunPaused".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "PAUSED",
            }),
        };
        let mut tx = self.pool.begin().await?;
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;

        Ok(updated)
    }

    pub async fn resume_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if !matches!(
            run.status,
            RunStatus::Paused | RunStatus::WaitingApproval | RunStatus::WaitingDependency
        ) {
            return Err(ApplicationError::Validation(
                "Only paused or waiting runs can resume".into(),
            ));
        }

        let updated = transition_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            RunStatus::Running,
            None,
            None,
        )
        .await?;

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunResumed".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "RUNNING",
            }),
        };
        let mut tx = self.pool.begin().await?;
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;

        Ok(updated)
    }

    pub async fn cancel_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        reason: Option<String>,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;

        // Duplicate-safe cancellation
        if run.status == RunStatus::Cancelled {
            return Ok(run);
        }

        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }

        if !matches!(
            run.status,
            RunStatus::Queued
                | RunStatus::Running
                | RunStatus::Paused
                | RunStatus::WaitingApproval
                | RunStatus::WaitingDependency
        ) {
            return Err(ApplicationError::Validation(
                "Cannot cancel terminal run".into(),
            ));
        }

        let principal = run.requested_by.clone();
        let correlation_id = run.correlation_id.clone();

        let result = RuntimeResult {
            id: Uuid::now_v7().to_string(),
            company_id: company_id.clone(),
            run_id: run_id.into(),
            run_status: RunStatus::Cancelled,
            result_summary: reason.as_deref().unwrap_or("Cancelled").to_string(),
            output_payload: None,
            output_metadata: None,
            resource_usage_summary: None,
            failure_class: Some("CANCELLED".into()),
            failure_detail: reason.clone(),
            warnings: None,
            correlation_id: correlation_id.clone(),
            causation_id: Some(run_id.into()),
        };

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunCancelled".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: expected_version + 1,
            occurred_at: Utc::now(),
            correlation_id: correlation_id.clone(),
            causation_id: run_id.into(),
            principal: principal.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "CANCELLED",
                "reason": reason,
            }),
        };

        let updated = cancel_run_with_result(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            &result,
            &event,
        )
        .await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunCancelled".into(),
            principal,
            scope: ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": company_id.0,
                "project_id": project_id,
                "run_id": run_id,
                "reason": reason,
            }),
        });

        Ok(updated)
    }

    pub async fn succeed_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        result_summary: &str,
        output_payload: Option<serde_json::Value>,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if run.status != RunStatus::Running {
            return Err(ApplicationError::Validation(
                "Only running runs can succeed".into(),
            ));
        }

        let updated = transition_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            RunStatus::Succeeded,
            None,
            None,
        )
        .await?;

        let result = RuntimeResult {
            id: Uuid::now_v7().to_string(),
            company_id: company_id.clone(),
            run_id: run_id.into(),
            run_status: RunStatus::Succeeded,
            result_summary: result_summary.into(),
            output_payload,
            output_metadata: None,
            resource_usage_summary: None,
            failure_class: None,
            failure_detail: None,
            warnings: None,
            correlation_id: run.correlation_id.clone(),
            causation_id: Some(run_id.into()),
        };

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunSucceeded".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "SUCCEEDED",
                "summary": result_summary,
            }),
        };

        nalarvo_persistence::m4::store_terminal_result(&self.pool, &result, &event).await?;

        // Notice: WorkItem is NEVER completed by Run completion!
        Ok(updated)
    }

    pub async fn fail_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        failure_class: &str,
        failure_detail: &str,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if run.status != RunStatus::Running {
            return Err(ApplicationError::Validation(
                "Only running runs can fail".into(),
            ));
        }

        let updated = transition_run(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            RunStatus::Failed,
            Some(failure_class),
            Some(failure_detail),
        )
        .await?;

        let result = RuntimeResult {
            id: Uuid::now_v7().to_string(),
            company_id: company_id.clone(),
            run_id: run_id.into(),
            run_status: RunStatus::Failed,
            result_summary: failure_detail.into(),
            output_payload: None,
            output_metadata: None,
            resource_usage_summary: None,
            failure_class: Some(failure_class.into()),
            failure_detail: Some(failure_detail.into()),
            warnings: None,
            correlation_id: run.correlation_id.clone(),
            causation_id: Some(run_id.into()),
        };

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunFailed".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: updated.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "FAILED",
                "failure_class": failure_class,
                "failure_detail": failure_detail,
            }),
        };

        nalarvo_persistence::m4::store_terminal_result(&self.pool, &result, &event).await?;

        // Notice: WorkItem is NEVER failed by Run failure!
        Ok(updated)
    }

    pub async fn timed_out_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        timeout_class: Option<&str>,
        timeout_detail: Option<&str>,
    ) -> Result<Run, ApplicationError> {
        let run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        if run.status != RunStatus::Running {
            return Err(ApplicationError::Validation(
                "Only running runs can time out".into(),
            ));
        }

        let result = RuntimeResult {
            id: Uuid::now_v7().to_string(),
            company_id: company_id.clone(),
            run_id: run_id.into(),
            run_status: RunStatus::TimedOut,
            result_summary: timeout_detail.unwrap_or("run timed out").to_string(),
            output_payload: None,
            output_metadata: None,
            resource_usage_summary: None,
            failure_class: timeout_class.map(|s| s.to_string()),
            failure_detail: timeout_detail.map(|s| s.to_string()),
            warnings: None,
            correlation_id: run.correlation_id.clone(),
            causation_id: Some(run_id.into()),
        };

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunTimedOut".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: expected_version + 1,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: run.requested_by.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "status": "TIMED_OUT",
                "timeout_class": timeout_class,
                "timeout_detail": timeout_detail,
            }),
        };

        let updated = timeout_run_with_result(
            &self.pool,
            company_id,
            run_id,
            expected_version,
            &result,
            &event,
        )
        .await?;

        Ok(updated)
    }

    pub async fn retry_run(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        failed_run_id: &str,
        expected_version: i64,
        meta: Option<CommandMeta>,
    ) -> Result<Run, ApplicationError> {
        let failed = self.get_run(company_id, project_id, failed_run_id).await?;
        if failed.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: failed.row_version,
                expected: expected_version,
            });
        }
        if !matches!(failed.status, RunStatus::Failed | RunStatus::TimedOut) {
            return Err(ApplicationError::Validation(
                "Only failed or timed out runs can be retried".into(),
            ));
        }

        let meta = meta.unwrap_or_default();
        let principal = meta
            .principal
            .clone()
            .unwrap_or_else(|| failed.requested_by.clone());

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunRetried".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: failed_run_id.into(),
            aggregate_version: expected_version,
            occurred_at: Utc::now(),
            correlation_id: failed.correlation_id.clone(),
            causation_id: failed_run_id.into(),
            principal: principal.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "failed_run_id": failed_run_id,
                "attempt_number": failed.attempt_number + 1,
            }),
        };

        let retry = nalarvo_persistence::m4::retry_run(
            &self.pool,
            company_id,
            failed_run_id,
            expected_version,
            &event,
        )
        .await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunRetried".into(),
            principal,
            scope: ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": company_id.0,
                "project_id": project_id,
                "failed_run_id": failed_run_id,
                "retry_run_id": retry.id,
            }),
        });

        Ok(retry)
    }

    pub async fn append_execution_step(
        &self,
        step: &ExecutionStep,
    ) -> Result<(), ApplicationError> {
        Ok(append_step(&self.pool, step).await?)
    }

    pub async fn reject_run_admission(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        run_id: &str,
        expected_version: i64,
        detail: &str,
    ) -> Result<Run, ApplicationError> {
        let mut run = self.get_run(company_id, project_id, run_id).await?;
        if run.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }
        run.reject_admission("ADMISSION_REJECTED".into(), detail.into())?;

        let now_str = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE runs SET lifecycle_state = 'FAILED', failure_class = 'ADMISSION_REJECTED', failure_detail = ?, completed_at = ?, updated_at = ?, row_version = row_version + 1 WHERE id = ? AND company_id = ? AND row_version = ?"
        )
        .bind(detail)
        .bind(&now_str)
        .bind(&now_str)
        .bind(run_id)
        .bind(&company_id.0)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() != 1 {
            return Err(ApplicationError::StaleVersion {
                current: run.row_version,
                expected: expected_version,
            });
        }

        let principal = run.requested_by.clone();
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "RunAdmissionRejected".into(),
            schema_version: 1,
            company_id: company_id.clone(),
            aggregate_type: "Run".into(),
            aggregate_id: run_id.into(),
            aggregate_version: run.row_version,
            occurred_at: Utc::now(),
            correlation_id: run.correlation_id.clone(),
            causation_id: run_id.into(),
            principal: principal.clone(),
            scope: ScopeRef::company(&company_id.0),
            payload: serde_json::json!({
                "run_id": run_id,
                "failure_class": "ADMISSION_REJECTED",
                "failure_detail": detail,
            }),
        };
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "RunAdmissionRejected".into(),
            principal,
            scope: ScopeRef::company(&company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": company_id.0,
                "project_id": project_id,
                "run_id": run_id,
                "failure_detail": detail,
            }),
        });

        self.get_run(company_id, project_id, run_id).await
    }

    async fn validate_run_scope(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        work_item_id: &str,
        agent_id: &str,
        assignment_id: Option<&str>,
    ) -> Result<(), ApplicationError> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM projects p JOIN work_items w ON w.id = ? AND w.company_id = p.company_id AND w.project_id = p.id JOIN agents a ON a.id = ? AND a.company_id = p.company_id WHERE p.id = ? AND p.company_id = ?"
        )
        .bind(work_item_id)
        .bind(agent_id)
        .bind(project_id)
        .bind(&company_id.0)
        .fetch_one(&self.pool)
        .await?;

        if exists != 1 {
            return Err(ApplicationError::Validation(
                "Invalid company, project, work item, or executing agent scope".into(),
            ));
        }

        if let Some(aid) = assignment_id {
            let assign_exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM assignments WHERE id = ? AND company_id = ? AND project_id = ? AND work_item_id = ? AND agent_id = ?"
            )
            .bind(aid)
            .bind(&company_id.0)
            .bind(project_id)
            .bind(work_item_id)
            .bind(agent_id)
            .fetch_one(&self.pool)
            .await?;

            if assign_exists != 1 {
                return Err(ApplicationError::Validation(
                    "Invalid assignment scope for run".into(),
                ));
            }
        }

        Ok(())
    }

    async fn check_admission(&self, run: &Run) -> Result<(), (String, String)> {
        // 1. Project check
        let proj = sqlx::query_scalar::<_, String>(
            "SELECT status FROM projects WHERE id = ? AND company_id = ?",
        )
        .bind(&run.project_id)
        .bind(&run.company_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

        let Some(proj_status) = proj else {
            return Err(("ADMISSION_REJECTED".into(), "Project not found".into()));
        };
        if matches!(proj_status.as_str(), "CANCELLED" | "ARCHIVED") {
            return Err((
                "ADMISSION_REJECTED".into(),
                format!("Project is not active: status is {proj_status}"),
            ));
        }

        // 2. WorkItem check
        let work = sqlx::query_scalar::<_, String>(
            "SELECT status FROM work_items WHERE id = ? AND company_id = ? AND project_id = ?",
        )
        .bind(&run.work_item_id)
        .bind(&run.company_id.0)
        .bind(&run.project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

        let Some(work_status) = work else {
            return Err((
                "ADMISSION_REJECTED".into(),
                "Work item not found in project".into(),
            ));
        };
        if matches!(work_status.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") {
            return Err((
                "ADMISSION_REJECTED".into(),
                format!("Work item cannot be run: status is {work_status}"),
            ));
        }

        // 3. Agent check
        let agent_row = sqlx::query(
            "SELECT status, model_profile_id FROM agents WHERE id = ? AND company_id = ?",
        )
        .bind(&run.executing_agent_id)
        .bind(&run.company_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

        let Some(agent) = agent_row else {
            return Err(("ADMISSION_REJECTED".into(), "Agent not found".into()));
        };
        let agent_status: String = agent.get(0);
        let agent_model_profile_id: Option<String> = agent.get(1);

        if agent_status != "ACTIVE" {
            return Err((
                "ADMISSION_REJECTED".into(),
                format!("Agent is not active: status is {agent_status}"),
            ));
        }

        // 4. Allocation check: Agent must have an ACTIVE allocation in this project
        let active_alloc: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_allocations WHERE company_id = ? AND project_id = ? AND agent_id = ? AND status = 'ACTIVE'",
        )
        .bind(&run.company_id.0)
        .bind(&run.project_id)
        .bind(&run.executing_agent_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

        if active_alloc == 0 {
            return Err((
                "ADMISSION_REJECTED".into(),
                "Agent has no active allocation in project".into(),
            ));
        }

        // 5. Assignment check: If assignment_id is present, must be ACTIVE and match
        if let Some(assignment_id) = &run.assignment_id {
            let active_assign: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM assignments WHERE id = ? AND company_id = ? AND project_id = ? AND work_item_id = ? AND agent_id = ? AND status = 'ACTIVE'",
            )
            .bind(assignment_id)
            .bind(&run.company_id.0)
            .bind(&run.project_id)
            .bind(&run.work_item_id)
            .bind(&run.executing_agent_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

            if active_assign == 0 {
                return Err((
                    "ADMISSION_REJECTED".into(),
                    "Work assignment is not active or mismatched".into(),
                ));
            }
        }

        // 6. Model profile and Resource grant check
        let profile_to_check = if let Some(mpv) = &run.model_profile_version_id {
            let pid = sqlx::query_scalar::<_, String>(
                "SELECT profile_id FROM model_profile_versions WHERE profile_id = ? OR (profile_id || ':' || version) = ? LIMIT 1",
            )
            .bind(mpv)
            .bind(mpv)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

            if let Some(pid) = pid {
                Some(pid)
            } else {
                let pid =
                    sqlx::query_scalar::<_, String>("SELECT id FROM model_profiles WHERE id = ?")
                        .bind(mpv)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

                if pid.is_none() {
                    return Err((
                        "ADMISSION_REJECTED".into(),
                        format!("Model profile version not found: {mpv}"),
                    ));
                }
                pid
            }
        } else {
            agent_model_profile_id
        };

        if let Some(profile_id) = profile_to_check {
            let grant_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM company_workspace_resource_grants WHERE company_id = ? AND model_profile_id = ?",
            )
            .bind(&run.company_id.0)
            .bind(&profile_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| ("ADMISSION_REJECTED".into(), e.to_string()))?;

            if grant_count == 0 {
                return Err((
                    "ADMISSION_REJECTED".into(),
                    format!("Company has no resource grant for model profile {profile_id}"),
                ));
            }
        }

        Ok(())
    }
}

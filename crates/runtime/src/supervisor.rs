// crates/runtime/src/supervisor.rs
use crate::worker::{RunExecutionRequest, RuntimeStreamEvent, execute_run_worker_process};
use nalarvo_domain::{
    DomainEvent, ExecutionStep, PrincipalRef, PrincipalType, RunStatus, ScopeRef,
};
use nalarvo_model_gateway::MockMode;
use nalarvo_persistence::m4::{
    DurableJob, ExecutionLease, ModelInvocation, RuntimeResult, UsageRecord,
    append_invocation_with_lease, append_step_with_lease, append_usage_with_lease, claim_job,
    expire_leases, finalize_run_with_result_with_lease, get_run, heartbeat_lease,
    release_current_lease, transition_run_with_lease, verify_current_lease,
};
use sqlx::SqlitePool;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub max_concurrency: usize,
    pub worker_principal_id: String,
    pub lease_duration_secs: i64,
    pub heartbeat_interval_secs: i64,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 2,
            worker_principal_id: "system:runtime-supervisor-1".into(),
            lease_duration_secs: 30,
            heartbeat_interval_secs: 5,
        }
    }
}

pub struct RuntimeSupervisor {
    pub pool: SqlitePool,
    pub config: SupervisorConfig,
    pub active_workers: Arc<AtomicUsize>,
    workers: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl RuntimeSupervisor {
    pub fn new(pool: SqlitePool, config: SupervisorConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            pool,
            config,
            active_workers: Arc::new(AtomicUsize::new(0)),
            workers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn shutdown(&self) {
        self.stop();
        let handles = std::mem::take(&mut *self.workers.lock().await);
        for handle in handles {
            let _ = handle.await;
        }
    }

    pub fn current_active_count(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    pub fn is_capacity_available(&self) -> bool {
        self.current_active_count() < self.config.max_concurrency
    }

    pub async fn poll_and_dispatch_once(
        &self,
        event_tx: Option<mpsc::Sender<RuntimeStreamEvent>>,
    ) -> Result<bool, String> {
        if *self.shutdown_rx.borrow() {
            return Ok(false);
        }
        if !self.is_capacity_available() {
            info!("Runtime supervisor capacity full, deferring job claim");
            return Ok(false);
        }

        let lease_dur = chrono::Duration::seconds(self.config.lease_duration_secs);
        let claimed = claim_job(&self.pool, &self.config.worker_principal_id, lease_dur)
            .await
            .map_err(|e| e.to_string())?;

        let Some((job, lease)) = claimed else {
            return Ok(false);
        };

        let active_counter = self.active_workers.clone();
        active_counter.fetch_add(1, Ordering::SeqCst);

        let pool = self.pool.clone();
        let config = self.config.clone();
        let mut stop_rx = self.shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            let res = run_job_execution(pool, config, job, lease, event_tx, &mut stop_rx).await;
            active_counter.fetch_sub(1, Ordering::SeqCst);
            if let Err(e) = res {
                error!("Job execution task failed: {e}");
            }
        });

        {
            let mut guard = self.workers.lock().await;
            guard.retain(|h| !h.is_finished());
            guard.push(handle);
        }

        Ok(true)
    }

    pub async fn clean_expired_leases(&self) -> Result<u64, String> {
        expire_leases(&self.pool, chrono::Utc::now())
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn reconcile_orphaned_runs(&self) -> Result<usize, String> {
        self.clean_expired_leases().await?;

        // Find non-terminal runs that are RUNNING but have no active valid execution lease
        let rows = sqlx::query(
            "SELECT r.id, r.company_id, r.row_version, r.correlation_id, r.causation_id
             FROM runs r
             WHERE r.lifecycle_state = 'RUNNING'
               AND NOT EXISTS (
                   SELECT 1 FROM run_execution_leases l
                   WHERE l.run_id = r.id
                     AND l.released_at IS NULL
                     AND l.expires_at > ?
               )",
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        let mut reconciled = 0;
        for row in rows {
            use sqlx::Row;
            let run_id: String = row.get(0);
            let company_id_str: String = row.get(1);
            let row_version: i64 = row.get(2);
            let correlation_id: String = row.get(3);
            let causation_id: Option<String> = row.get(4);
            let company_id = nalarvo_domain::CompanyId(company_id_str);

            let result = RuntimeResult {
                id: Uuid::now_v7().to_string(),
                company_id: company_id.clone(),
                run_id: run_id.clone(),
                run_status: RunStatus::Failed,
                result_summary: "Worker crashed and execution lease expired".into(),
                output_payload: None,
                output_metadata: None,
                resource_usage_summary: None,
                failure_class: Some("WORKER_LOST".into()),
                failure_detail: Some(
                    "Run execution interrupted: worker crashed or terminated".into(),
                ),
                warnings: None,
                correlation_id: correlation_id.clone(),
                causation_id: causation_id.clone().or_else(|| Some(run_id.clone())),
            };

            let event = DomainEvent {
                event_id: Uuid::now_v7().to_string(),
                event_type: "RunFailed".into(),
                schema_version: 1,
                company_id: company_id.clone(),
                aggregate_type: "Run".into(),
                aggregate_id: run_id.clone(),
                aggregate_version: row_version + 1,
                occurred_at: chrono::Utc::now(),
                correlation_id,
                causation_id: causation_id.unwrap_or_else(|| run_id.clone()),
                principal: PrincipalRef {
                    principal_type: PrincipalType::System,
                    principal_id: self.config.worker_principal_id.clone(),
                },
                scope: ScopeRef::company(company_id.0.clone()),
                payload: serde_json::json!({
                    "run_id": run_id,
                    "company_id": company_id.0,
                    "status": "FAILED",
                    "reason": "WORKER_LOST",
                }),
            };

            let res = nalarvo_persistence::m4::finalize_orphaned_run_with_result(
                &self.pool,
                &company_id,
                &run_id,
                row_version,
                &result,
                &event,
            )
            .await;

            if res.is_ok() {
                reconciled += 1;
            }
        }

        Ok(reconciled)
    }
}

impl Drop for RuntimeSupervisor {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

pub struct SupervisorHandle {
    handle: tokio::task::JoinHandle<()>,
    stop_tx: watch::Sender<bool>,
}

impl SupervisorHandle {
    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.handle.await;
    }

    pub fn abort(&self) {
        let _ = self.stop_tx.send(true);
        self.handle.abort();
    }
}

pub fn start_runtime_supervisor(pool: SqlitePool, config: SupervisorConfig) -> SupervisorHandle {
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let supervisor = RuntimeSupervisor::new(pool, config);
        let _ = supervisor.reconcile_orphaned_runs().await;
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        supervisor.shutdown().await;
                        break;
                    }
                }
                _ = interval.tick() => {
                    let _ = supervisor.reconcile_orphaned_runs().await;
                    let _ = supervisor.poll_and_dispatch_once(None).await;
                }
            }
        }
    });
    SupervisorHandle { handle, stop_tx }
}

async fn run_job_execution(
    pool: SqlitePool,
    config: SupervisorConfig,
    job: DurableJob,
    lease: ExecutionLease,
    event_tx: Option<mpsc::Sender<RuntimeStreamEvent>>,
    stop_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    if *stop_rx.borrow() {
        return Ok(());
    }
    let company_id = &job.company_id;
    ensure_current_lease(&pool, &lease).await?;
    let run = get_run(&pool, company_id, &job.run_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Run {} not found", job.run_id))?;

    // Start Run: QUEUED -> RUNNING
    let run_running = transition_run_with_lease(
        &pool,
        &lease,
        company_id,
        &run.id,
        run.row_version,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .map_err(|e| e.to_string())?;

    let context_input = crate::context::ContextInput {
        company_id: company_id.0.clone(),
        project_id: run_running.project_id.clone(),
        project_name: "Nalarvo Execution Project".into(),
        project_instructions: "Perform execution task".into(),
        objective_summary: Some("Execute work item task".into()),
        team_name: "Default Team".into(),
        agent_name: run_running.executing_agent_id.clone(),
        role_name: "Agent Role".into(),
        department_name: "Engineering".into(),
        work_item_title: format!("Work Item {}", run_running.work_item_id),
        work_item_description: "Execute assigned work item".into(),
        acceptance_criteria: "Produce valid result".into(),
        assignment_summary: run_running.assignment_id.clone().unwrap_or_default(),
        requested_output: "Structured response".into(),
        safe_execution_metadata: format!("run_id={}", run_running.id),
        excluded_values: vec![],
    };

    let mock_mode = match job
        .payload
        .get("mock_scenario")
        .and_then(|v| v.as_str())
        .unwrap_or("SUCCESS_TEXT")
    {
        "SUCCESS_STRUCTURED" => MockMode::SUCCESS_STRUCTURED,
        "STREAMING_SUCCESS" => MockMode::STREAMING_SUCCESS,
        "DELAYED_SUCCESS" => MockMode::DELAYED_SUCCESS,
        "TIMEOUT" => MockMode::TIMEOUT,
        "RATE_LIMIT" => MockMode::RATE_LIMIT,
        "AUTH_FAILURE" => MockMode::AUTH_FAILURE,
        "MALFORMED_RESPONSE" => MockMode::MALFORMED_RESPONSE,
        "PROVIDER_UNAVAILABLE" => MockMode::PROVIDER_UNAVAILABLE,
        "CANCELLED" => MockMode::CANCELLED,
        _ => MockMode::SUCCESS_TEXT,
    };

    let provider_connection_id = job
        .payload
        .get("provider_connection_id")
        .and_then(|v| v.as_str())
        .unwrap_or("provider-conn-default")
        .to_string();

    let model_key = job
        .payload
        .get("model_key")
        .and_then(|v| v.as_str())
        .unwrap_or("gpt-4o-mini")
        .to_string();

    let base_url = job
        .payload
        .get("base_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let auth_token = job
        .payload
        .get("auth_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let exec_req = RunExecutionRequest {
        run_id: run_running.id.clone(),
        company_id: company_id.0.clone(),
        project_id: run_running.project_id.clone(),
        work_item_id: run_running.work_item_id.clone(),
        agent_id: run_running.executing_agent_id.clone(),
        model_profile_version_id: run_running.model_profile_version_id.clone(),
        provider_connection_id: provider_connection_id.clone(),
        base_url,
        auth_token,
        model_key: model_key.clone(),
        context_input,
        worker_principal_id: config.worker_principal_id.clone(),
        lease_id: lease.id.clone(),
        lease_version: lease.lease_version,
        pool: Some(pool.clone()),
        expected_run_version: run_running.row_version,
        mock_mode,
        cancel_rx: Some(cancel_rx),
    };

    let worker_bin = resolve_worker_executable()?;
    let mut lease = lease;
    let heartbeat_interval = u64::try_from(config.heartbeat_interval_secs)
        .ok()
        .filter(|secs| *secs > 0 && *secs < config.lease_duration_secs as u64)
        .ok_or("heartbeat interval must be positive and shorter than lease duration")?;
    let mut ticks = tokio::time::interval(tokio::time::Duration::from_secs(heartbeat_interval));
    ticks.tick().await;
    let mut controls = tokio::time::interval(tokio::time::Duration::from_millis(25));
    controls.tick().await;
    let deadline_secs = job
        .payload
        .get("deadline_secs")
        .and_then(|value| value.as_u64());
    let deadline = async {
        match deadline_secs {
            Some(secs) => tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(deadline);
    let execution = execute_run_worker_process(&worker_bin, exec_req, event_tx);
    tokio::pin!(execution);
    let outcome = loop {
        tokio::select! {
            result = &mut execution => break result.map_err(|e| format!("Worker process execution failed: {e}"))?,
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    let _ = cancel_tx.send(true);
                    let _ = (&mut execution).await;
                    let _ = release_current_lease(&pool, &lease, "SHUTDOWN").await;
                    return Ok(());
                }
            }
            _ = ticks.tick() => {
                lease = heartbeat_lease(
                    &pool, &lease.id, &lease.worker_principal_id, lease.lease_version,
                    chrono::Duration::seconds(config.lease_duration_secs),
                ).await.map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("Execution lease {} expired or was preempted", lease.id))?;
            }
            _ = controls.tick() => {
                let current = get_run(&pool, company_id, &run_running.id)
                    .await.map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("Run {} not found", run_running.id))?;
                if current.status == RunStatus::Cancelled {
                    let _ = cancel_tx.send(true);
                    let _ = (&mut execution).await;
                    // The canonical application cancellation transaction already persisted
                    // result/event/job/lease state. Runtime only stops and reaps the child.
                    return Ok(());
                }
            }
            _ = &mut deadline => {
                let _ = cancel_tx.send(true);
                let _ = (&mut execution).await;
                finalize_deadline(&pool, &lease, &run_running, &config.worker_principal_id).await?;
                let _ = release_current_lease(&pool, &lease, "TIMED_OUT").await;
                return Ok(());
            }
        }
    };
    ensure_current_lease(&pool, &lease).await?;

    // Append Context Step Record
    let context_step = ExecutionStep {
        id: Uuid::now_v7().to_string(),
        company_id: company_id.clone(),
        run_id: run_running.id.clone(),
        sequence_no: 1,
        step_type: "CONTEXT_PREP".into(),
        status: outcome.context_step_status,
        parent_step_id: None,
        input_metadata: None,
        output_metadata: Some(serde_json::json!({ "context_bytes": 512 })),
        failure_class: None,
        failure_detail: None,
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        created_at: chrono::Utc::now(),
    };
    let _ = append_step_with_lease(&pool, &lease, &context_step).await;

    // Append Model Step & Invocation Record
    let model_step = ExecutionStep {
        id: Uuid::now_v7().to_string(),
        company_id: company_id.clone(),
        run_id: run_running.id.clone(),
        sequence_no: 2,
        step_type: "MODEL_CALL".into(),
        status: outcome.model_step_status,
        parent_step_id: Some(context_step.id.clone()),
        input_metadata: None,
        output_metadata: outcome
            .output_text
            .as_ref()
            .map(|t| serde_json::json!({ "length": t.len() })),
        failure_class: outcome.failure_class.clone(),
        failure_detail: outcome.failure_detail.clone(),
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        created_at: chrono::Utc::now(),
    };
    let _ = append_step_with_lease(&pool, &lease, &model_step).await;

    let inv_id = Uuid::now_v7().to_string();
    let invocation = ModelInvocation {
        id: inv_id.clone(),
        company_id: company_id.clone(),
        run_id: run_running.id.clone(),
        step_id: model_step.id.clone(),
        agent_id: run_running.executing_agent_id.clone(),
        provider_connection_id: provider_connection_id.clone(),
        model_id: model_key.clone(),
        model_profile_version_id: run_running.model_profile_version_id.clone(),
        invocation_index: 1,
        status: if outcome.status == RunStatus::Succeeded {
            "SUCCEEDED".into()
        } else {
            "FAILED".into()
        },
        request_metadata: Some(serde_json::json!({ "model": model_key })),
        response_metadata: outcome
            .output_text
            .as_ref()
            .map(|t| serde_json::json!({ "summary": t.chars().take(50).collect::<String>() })),
        input_tokens: outcome.input_tokens,
        output_tokens: outcome.output_tokens,
        estimated_cost: Some(0.0001),
        latency_ms: outcome.latency_ms,
        provider_request_id: Some(Uuid::now_v7().to_string()),
        started_at: chrono::Utc::now(),
        completed_at: Some(chrono::Utc::now()),
        failure_class: outcome.failure_class.clone(),
        failure_detail: outcome.failure_detail.clone(),
    };
    let _ = append_invocation_with_lease(&pool, &lease, &invocation).await;

    // Append Usage Record
    if let (Some(in_t), Some(out_t)) = (outcome.input_tokens, outcome.output_tokens) {
        let usage = UsageRecord {
            id: Uuid::now_v7().to_string(),
            workspace_id: "workspace-1".into(),
            company_id: company_id.clone(),
            project_id: run_running.project_id.clone(),
            work_item_id: run_running.work_item_id.clone(),
            agent_id: run_running.executing_agent_id.clone(),
            run_id: run_running.id.clone(),
            step_id: Some(model_step.id.clone()),
            provider_connection_id: provider_connection_id.clone(),
            model_id: model_key.clone(),
            usage_type: "TOKENS".into(),
            quantity: in_t + out_t,
            unit: "TOKENS".into(),
            estimated_cost: Some(0.0001),
            occurred_at: chrono::Utc::now(),
            metadata: Some(serde_json::json!({ "input": in_t, "output": out_t })),
        };
        let _ = append_usage_with_lease(&pool, &lease, &usage).await;
    }

    let result_rec = RuntimeResult {
        id: Uuid::now_v7().to_string(),
        company_id: company_id.clone(),
        run_id: run_running.id.clone(),
        run_status: outcome.status,
        result_summary: outcome.result_summary.clone(),
        output_payload: outcome
            .output_text
            .map(|t| serde_json::json!({ "text": t })),
        output_metadata: Some(serde_json::json!({ "latency_ms": outcome.latency_ms })),
        resource_usage_summary: Some(
            serde_json::json!({ "total_tokens": outcome.input_tokens.unwrap_or(0) + outcome.output_tokens.unwrap_or(0) }),
        ),
        failure_class: outcome.failure_class,
        failure_detail: outcome.failure_detail,
        warnings: None,
        correlation_id: run_running.correlation_id.clone(),
        causation_id: run_running.causation_id.clone(),
    };

    let event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: match outcome.status {
            RunStatus::Succeeded => "RunSucceeded",
            RunStatus::TimedOut => "RunTimedOut",
            RunStatus::Cancelled => "RunCancelled",
            _ => "RunFailed",
        }
        .into(),
        schema_version: 1,
        company_id: company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run_running.id.clone(),
        aggregate_version: run_running.row_version + 1,
        occurred_at: chrono::Utc::now(),
        correlation_id: run_running.correlation_id.clone(),
        causation_id: run_running
            .causation_id
            .clone()
            .unwrap_or_else(|| run_running.id.clone()),
        principal: PrincipalRef {
            principal_type: PrincipalType::System,
            principal_id: config.worker_principal_id.clone(),
        },
        scope: ScopeRef::company(company_id.0.clone()),
        payload: serde_json::json!({
            "run_id": run_running.id,
            "company_id": company_id.0,
            "status": outcome.status.to_string(),
        }),
    };

    finalize_run_with_result_with_lease(
        &pool,
        &lease,
        company_id,
        &run_running.id,
        run_running.row_version,
        &result_rec,
        &event,
    )
    .await
    .map_err(|e| e.to_string())?;
    let _ = release_current_lease(&pool, &lease, "COMPLETED").await;

    Ok(())
}

async fn finalize_deadline(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    run: &nalarvo_domain::Run,
    worker_principal_id: &str,
) -> Result<(), String> {
    let result = RuntimeResult {
        id: Uuid::now_v7().to_string(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        run_status: RunStatus::TimedOut,
        result_summary: "Run deadline exceeded".into(),
        output_payload: None,
        output_metadata: None,
        resource_usage_summary: None,
        failure_class: Some("RUN_DEADLINE_EXCEEDED".into()),
        failure_detail: Some("Run execution deadline exceeded".into()),
        warnings: None,
        correlation_id: run.correlation_id.clone(),
        causation_id: Some(run.id.clone()),
    };
    let event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "RunTimedOut".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: run.row_version + 1,
        occurred_at: chrono::Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.causation_id.clone().unwrap_or_else(|| run.id.clone()),
        principal: PrincipalRef {
            principal_type: PrincipalType::System,
            principal_id: worker_principal_id.to_string(),
        },
        scope: ScopeRef::company(run.company_id.0.clone()),
        payload: serde_json::json!({
            "run_id": run.id,
            "company_id": run.company_id.0,
            "status": "TIMED_OUT",
        }),
    };
    finalize_run_with_result_with_lease(
        pool,
        lease,
        &run.company_id,
        &run.id,
        run.row_version,
        &result,
        &event,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn ensure_current_lease(pool: &SqlitePool, lease: &ExecutionLease) -> Result<(), String> {
    let valid = verify_current_lease(
        pool,
        &lease.run_id,
        &lease.worker_principal_id,
        &lease.id,
        lease.lease_version,
    )
    .await
    .map_err(|e| e.to_string())?;

    if !valid {
        return Err(format!(
            "Execution lease {} for run {} expired or was preempted",
            lease.id, lease.run_id
        ));
    }
    Ok(())
}

pub fn resolve_worker_executable() -> Result<std::path::PathBuf, String> {
    let bin_name = if cfg!(windows) {
        "nalarvo-worker.exe"
    } else {
        "nalarvo-worker"
    };

    if let Some(cargo_bin) = option_env!("CARGO_BIN_EXE_nalarvo-worker") {
        let candidate = std::path::PathBuf::from(cargo_bin);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(bin_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
        if let Some(grandparent) = parent.parent() {
            let candidate = grandparent.join(bin_name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    if let Ok(env_path) = std::env::var("NALARVO_WORKER_BIN") {
        let candidate = std::path::PathBuf::from(env_path);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    let dev_candidate = std::path::PathBuf::from("target/debug").join(bin_name);
    if dev_candidate.is_file() {
        return Ok(dev_candidate);
    }

    Err(format!(
        "Worker executable '{bin_name}' not found adjacent to current executable or via NALARVO_WORKER_BIN"
    ))
}

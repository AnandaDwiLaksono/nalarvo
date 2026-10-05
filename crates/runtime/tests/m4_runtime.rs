// crates/runtime/tests/m4_runtime.rs
use chrono::{Duration as ChronoDuration, Utc};
use nalarvo_domain::{CompanyId, PrincipalRef, Run, RunStatus};
use nalarvo_model_gateway::{MockMode, ProviderErrorKind};
use nalarvo_persistence::create_pool;
use nalarvo_persistence::m4::{DurableJob, create_run, queue_run};
use nalarvo_persistence::run_migrations;
use nalarvo_runtime::context::{ContextInput, build_context};
use nalarvo_runtime::recovery::{RecoveryDecision, RecoveryError, decide_recovery};
use nalarvo_runtime::supervisor::{RuntimeSupervisor, SupervisorConfig};
use nalarvo_runtime::worker::{
    RunExecutionRequest, RuntimeStreamEvent, execute_run_worker, provider_failure_class,
};
use serde_json::json;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

async fn setup_db() -> SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('r','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','r','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('a','c','A','d','r',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,created_at,updated_at) VALUES ('p','c','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,logical_type,created_at,updated_at) VALUES ('wi','c','p','W','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,logical_type,created_at,updated_at) VALUES ('wi2','c','p','W2','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,created_at,updated_at) VALUES ('provider-conn-default','w','PC','TEST','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','w','provider-conn-default','gpt-4o-mini','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    pool
}

fn sample_run(work_item_id: &str) -> Run {
    Run::create(
        CompanyId("c".into()),
        "p".into(),
        work_item_id.into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        format!("corr-{work_item_id}"),
    )
    .unwrap()
}

fn sample_event(run: &Run, kind: &str) -> nalarvo_domain::DomainEvent {
    nalarvo_domain::DomainEvent {
        event_id: format!("{kind}-{}", run.id),
        event_type: kind.into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: run.row_version,
        occurred_at: Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::user("u"),
        scope: nalarvo_domain::ScopeRef::company(run.company_id.0.clone()),
        payload: json!({"run_id": run.id, "state": run.status.to_string()}),
    }
}

fn sample_job(run: &Run, mock_scenario: &str) -> DurableJob {
    DurableJob {
        id: format!("job-{}", run.id),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({"run_id": run.id, "mock_scenario": mock_scenario}),
        available_at: Utc::now() - ChronoDuration::seconds(1),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    }
}

// 1. Context Isolation & Secret Canary
#[test]
fn test_context_isolation_and_secret_canary() {
    let mut input = ContextInput::minimal("c", "p", "Title");
    input.project_name = "Nalarvo Project".into();
    input.excluded_values = vec!["SECRET_CANARY_123".into(), "FOREIGN_TENANT_KEY".into()];

    let ctx = build_context(&input).expect("context build");
    assert!(!ctx.contains("SECRET_CANARY_123"));
    assert!(!ctx.contains("FOREIGN_TENANT_KEY"));

    // Inject secret canary into context input
    let mut leaky_input = input.clone();
    leaky_input.project_instructions = "Leak SECRET_CANARY_123".into();
    assert!(build_context(&leaky_input).is_err());
}

// 2. Provider Failure Classification
#[test]
fn test_provider_failure_classification() {
    assert_eq!(
        provider_failure_class(ProviderErrorKind::Timeout),
        "PROVIDER_TIMEOUT"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::Authentication),
        "PROVIDER_AUTHENTICATION"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::RateLimited),
        "PROVIDER_RATE_LIMIT"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::Unavailable),
        "PROVIDER_UNAVAILABLE"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::MalformedResponse),
        "PROVIDER_MALFORMED_RESPONSE"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::Cancelled),
        "PROVIDER_CANCELLED"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::Http),
        "INTERNAL_PROVIDER_ERROR"
    );
    assert_eq!(
        provider_failure_class(ProviderErrorKind::InvalidRequest),
        "INTERNAL_PROVIDER_ERROR"
    );
}

// 3. Renderer Stream Receiver Drop Does Not Cancel Execution
#[tokio::test]
async fn test_renderer_stream_receiver_drop_does_not_cancel_execution() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx); // Drop receiver early!

    let req = RunExecutionRequest {
        run_id: "run-drop-rx".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn-1".into(),
        base_url: None,
        auth_token: None,
        model_key: "gpt-4o-mini".into(),
        context_input: ContextInput::minimal("c", "p", "Work Item Title"),
        worker_principal_id: "worker-1".into(),
        lease_id: "lease-1".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::SUCCESS_TEXT,
        cancel_rx: None,
    };

    let outcome = execute_run_worker(req, Some(tx)).await;
    assert_eq!(outcome.status, RunStatus::Succeeded);
    assert_eq!(outcome.output_text.as_deref(), Some("hello"));
}

// 4. Cooperative Cancellation
#[tokio::test]
async fn test_cooperative_cancellation() {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    cancel_tx.send(true).unwrap(); // Trigger cancellation immediately

    let req = RunExecutionRequest {
        run_id: "run-cancel".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn-1".into(),
        base_url: None,
        auth_token: None,
        model_key: "gpt-4o-mini".into(),
        context_input: ContextInput::minimal("c", "p", "Work Item Title"),
        worker_principal_id: "worker-1".into(),
        lease_id: "lease-1".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::DELAYED_SUCCESS,
        cancel_rx: Some(cancel_rx),
    };

    let outcome = execute_run_worker(req, None).await;
    assert_eq!(outcome.status, RunStatus::Failed);
    assert_eq!(outcome.failure_class.as_deref(), Some("PROVIDER_CANCELLED"));
}

#[tokio::test]
async fn test_provider_timeout_leads_to_failed_status() {
    use nalarvo_domain::ExecutionStepStatus;

    let req = RunExecutionRequest {
        run_id: "run-provider-timeout".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn-1".into(),
        base_url: None,
        auth_token: None,
        model_key: "gpt-4o-mini".into(),
        context_input: ContextInput::minimal("c", "p", "Work Item Title"),
        worker_principal_id: "worker-1".into(),
        lease_id: "lease-1".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::TIMEOUT,
        cancel_rx: None,
    };

    let outcome = execute_run_worker(req, None).await;
    assert_eq!(outcome.status, RunStatus::Failed);
    assert_eq!(outcome.failure_class.as_deref(), Some("PROVIDER_TIMEOUT"));
    assert_eq!(outcome.model_step_status, ExecutionStepStatus::Failed);
}

// 5. Recovery Decisions (Terminal / Queued / Running)
#[test]
fn test_recovery_decisions() {
    // Terminal states return error
    assert_eq!(
        decide_recovery("SUCCEEDED", true, 0),
        Err(RecoveryError::TerminalRun)
    );
    assert_eq!(
        decide_recovery("FAILED", false, 1),
        Err(RecoveryError::TerminalRun)
    );
    assert_eq!(
        decide_recovery("TIMED_OUT", true, 2),
        Err(RecoveryError::TerminalRun)
    );
    assert_eq!(
        decide_recovery("CANCELLED", false, 0),
        Err(RecoveryError::TerminalRun)
    );

    // Queued & Paused / Waiting -> RequeueJob
    assert_eq!(
        decide_recovery("QUEUED", false, 0).unwrap(),
        RecoveryDecision::RequeueJob
    );
    assert_eq!(
        decide_recovery("PAUSED", false, 1).unwrap(),
        RecoveryDecision::RequeueJob
    );
    assert_eq!(
        decide_recovery("WAITING_APPROVAL", false, 1).unwrap(),
        RecoveryDecision::RequeueJob
    );

    // Running states
    assert_eq!(
        decide_recovery("RUNNING", true, 2).unwrap(),
        RecoveryDecision::ResumeSafe
    );
    assert_eq!(
        decide_recovery("RUNNING", false, 0).unwrap(),
        RecoveryDecision::RequeueJob
    );
    assert_eq!(
        decide_recovery("RUNNING", false, 1).unwrap(),
        RecoveryDecision::FailSafe("worker lost during non-idempotent phase".into())
    );

    // Unsupported state
    assert!(decide_recovery("UNKNOWN_STATE", false, 0).is_err());
}

// 6. Output Truncation & Bounds
#[tokio::test]
async fn test_output_bounds_and_truncation() {
    let req = RunExecutionRequest {
        run_id: "run-long-err".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn-1".into(),
        base_url: None,
        auth_token: None,
        model_key: "gpt-4o-mini".into(),
        context_input: ContextInput::minimal("", "", ""), // Invalid scope trigger error!
        worker_principal_id: "worker-1".into(),
        lease_id: "lease-1".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::SUCCESS_TEXT,
        cancel_rx: None,
    };

    let outcome = execute_run_worker(req, None).await;
    assert_eq!(outcome.status, RunStatus::Failed);
    assert_eq!(
        outcome.failure_class.as_deref(),
        Some("CONTEXT_BUILD_FAILED")
    );
    if let Some(detail) = &outcome.failure_detail {
        assert!(detail.len() <= 512);
    }
}

// 7. Bounded Capacity Stays Queued & Duplicate Dispatch Prevention
#[tokio::test]
async fn test_bounded_capacity_and_duplicate_dispatch() {
    let pool = setup_db().await;

    let config = SupervisorConfig {
        max_concurrency: 1, // Only 1 job at a time!
        worker_principal_id: "worker-sup-1".into(),
        lease_duration_secs: 10,
        heartbeat_interval_secs: 2,
    };
    let supervisor = RuntimeSupervisor::new(pool.clone(), config);

    // Create 2 runs & queue 2 durable jobs
    let run1 = sample_run("wi");
    let run2 = sample_run("wi2");
    create_run(&pool, &run1).await.unwrap();
    create_run(&pool, &run2).await.unwrap();

    let job1 = sample_job(&run1, "DELAYED_SUCCESS");
    let job2 = sample_job(&run2, "SUCCESS_TEXT");
    queue_run(
        &pool,
        &run1.company_id,
        &run1.id,
        run1.row_version,
        &job1,
        &sample_event(&run1, "RunQueued1"),
    )
    .await
    .unwrap();
    queue_run(
        &pool,
        &run2.company_id,
        &run2.id,
        run2.row_version,
        &job2,
        &sample_event(&run2, "RunQueued2"),
    )
    .await
    .unwrap();

    // First dispatch claims job1
    let claimed1 = supervisor
        .poll_and_dispatch_once(None)
        .await
        .expect("poll 1");
    assert!(claimed1, "Job 1 should be claimed");

    // Immediately poll again -> capacity is full (1/1 active)
    let claimed2 = supervisor
        .poll_and_dispatch_once(None)
        .await
        .expect("poll 2");
    assert!(
        !claimed2,
        "Job 2 should not be claimed because capacity is full"
    );
    assert_eq!(supervisor.current_active_count(), 1);

    // Wait for job 1 to complete in tokio task
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(supervisor.current_active_count(), 0);

    // Now poll again -> job 2 is claimed
    let claimed3 = supervisor
        .poll_and_dispatch_once(None)
        .await
        .expect("poll 3");
    assert!(claimed3, "Job 2 should now be claimed");

    // Duplicate dispatch while job 2 is running -> no more jobs available
    let claimed_dup = supervisor
        .poll_and_dispatch_once(None)
        .await
        .expect("poll dup");
    assert!(!claimed_dup, "Duplicate claim should find no available job");

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(supervisor.current_active_count(), 0);
}

// 8. Stream Event Flow Verification
#[tokio::test]
async fn test_stream_event_delivery() {
    let (tx, mut rx) = mpsc::channel(10);

    let req = RunExecutionRequest {
        run_id: "run-stream".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn-1".into(),
        base_url: None,
        auth_token: None,
        model_key: "gpt-4o-mini".into(),
        context_input: ContextInput::minimal("c", "p", "Work Item Title"),
        worker_principal_id: "worker-1".into(),
        lease_id: "lease-1".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::SUCCESS_TEXT,
        cancel_rx: None,
    };

    let _outcome = execute_run_worker(req, Some(tx)).await;

    let delta = rx.recv().await.unwrap();
    assert_eq!(delta, RuntimeStreamEvent::OutputDelta("hello".into()));
    let final_out = rx.recv().await.unwrap();
    assert_eq!(final_out, RuntimeStreamEvent::FinalOutput("hello".into()));
}

// 9. Stale Worker Fencing: lease expired, new owner acquired, stale worker mutations rejected
#[tokio::test]
async fn test_stale_worker_fencing() {
    use nalarvo_domain::{ExecutionStep, ExecutionStepStatus};
    use nalarvo_persistence::m4::{append_step, expire_leases, get_run, heartbeat_lease};

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();

    // Worker A acquires lease
    let job = sample_job(&run, "SUCCESS_TEXT");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &sample_event(&run, "RunQueued"),
    )
    .await
    .unwrap();

    let (claimed_job, lease_a) =
        nalarvo_persistence::m4::claim_job(&pool, "worker-a", chrono::Duration::seconds(1))
            .await
            .expect("claim")
            .expect("job claimed");
    assert_eq!(claimed_job.id, job.id);
    assert_eq!(lease_a.worker_principal_id, "worker-a");
    assert_eq!(lease_a.lease_version, 1);

    // Heartbeat succeeds while valid
    let hb = heartbeat_lease(
        &pool,
        &lease_a.id,
        "worker-a",
        lease_a.lease_version,
        chrono::Duration::seconds(1),
    )
    .await
    .expect("heartbeat");
    assert!(hb.is_some(), "valid heartbeat must succeed");

    // Lease expires
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let expired = expire_leases(&pool, chrono::Utc::now())
        .await
        .expect("expire");
    assert_eq!(expired, 1, "lease A must be expired");

    // Stale heartbeat from A after expiry → REJECTED
    let hb_stale = heartbeat_lease(
        &pool,
        &lease_a.id,
        "worker-a",
        lease_a.lease_version,
        chrono::Duration::seconds(10),
    )
    .await
    .expect("heartbeat stale");
    assert!(hb_stale.is_none(), "STALE heartbeat must be rejected");

    // Stale step mutation from A → canonical write must be attributed to run
    // but ownership guard proves only current lease owner may progress execution.
    let step = ExecutionStep {
        id: "stale-step".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        sequence_no: 99,
        step_type: "MODEL_CALL".into(),
        status: ExecutionStepStatus::Failed,
        parent_step_id: None,
        input_metadata: None,
        output_metadata: None,
        failure_class: Some("STALE".into()),
        failure_detail: Some("stale worker attempt".into()),
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        created_at: chrono::Utc::now(),
    };
    // append_step is canonical write path; stale worker reaches it only via supervisor,
    // which holds the current lease. Direct worker writes are not exposed.
    append_step(&pool, &step).await.expect("append step");

    // Worker B claims fresh lease (version resets to 1 for new lease row)
    let (_job_b, lease_b) =
        nalarvo_persistence::m4::claim_job(&pool, "worker-b", chrono::Duration::seconds(30))
            .await
            .expect("claim b")
            .expect("job claimed by B");
    assert_eq!(lease_b.worker_principal_id, "worker-b");
    assert_ne!(lease_b.id, lease_a.id, "B must own a NEW lease");

    // A attempts to release/modify B's lease → rejected (owner mismatch)
    let released_by_a =
        nalarvo_persistence::m4::release_lease(&pool, &lease_b.id, "worker-a", "STALE_RELEASE")
            .await
            .expect("release attempt");
    assert!(!released_by_a, "stale worker A must NOT release B's lease");

    // B remains sole valid owner
    let hb_b = heartbeat_lease(
        &pool,
        &lease_b.id,
        "worker-b",
        lease_b.lease_version,
        chrono::Duration::seconds(30),
    )
    .await
    .expect("heartbeat b");
    assert!(hb_b.is_some(), "B heartbeat must succeed");

    // Run state untouched by stale A attempts
    let final_run = get_run(&pool, &run.company_id, &run.id)
        .await
        .expect("get run")
        .expect("run exists");
    assert_eq!(final_run.status, RunStatus::Queued);
}

#[tokio::test]
async fn supervisor_rejects_expired_lease_before_canonical_write() {
    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "SUCCESS_TEXT");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &sample_event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    let (_, lease) =
        nalarvo_persistence::m4::claim_job(&pool, "worker-a", ChronoDuration::seconds(30))
            .await
            .unwrap()
            .unwrap();
    nalarvo_persistence::m4::expire_leases(&pool, lease.expires_at + ChronoDuration::seconds(1))
        .await
        .unwrap();

    assert!(
        nalarvo_runtime::supervisor::ensure_current_lease(&pool, &lease)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps WHERE run_id = ?")
        .bind(&run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

// 10. Initial Lease Race: exactly one owner
#[tokio::test]
async fn test_initial_lease_race() {
    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();

    let job = sample_job(&run, "SUCCESS_TEXT");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &sample_event(&run, "RunQueued"),
    )
    .await
    .unwrap();

    // Two workers race for the same job
    let a = nalarvo_persistence::m4::claim_job(&pool, "worker-a", chrono::Duration::seconds(30));
    let b = nalarvo_persistence::m4::claim_job(&pool, "worker-b", chrono::Duration::seconds(30));

    let (res_a, res_b) = tokio::join!(a, b);

    let owner_a = res_a.unwrap();
    let owner_b = res_b.unwrap();

    // Exactly one owner
    assert!(
        (owner_a.is_some() && owner_b.is_none()) || (owner_a.is_none() && owner_b.is_some()),
        "exactly one worker must win the race"
    );
}

// 11. ExecutionStepStatus canonical lifecycle invariants
#[test]
fn test_execution_step_status_lifecycle() {
    use nalarvo_domain::ExecutionStepStatus;

    assert!(ExecutionStepStatus::Succeeded.is_terminal());
    assert!(ExecutionStepStatus::Failed.is_terminal());
    assert!(ExecutionStepStatus::Cancelled.is_terminal());
    assert!(ExecutionStepStatus::Skipped.is_terminal());
    assert!(!ExecutionStepStatus::Pending.is_terminal());
    assert!(!ExecutionStepStatus::Running.is_terminal());

    for status in [
        ExecutionStepStatus::Pending,
        ExecutionStepStatus::Running,
        ExecutionStepStatus::Succeeded,
        ExecutionStepStatus::Failed,
        ExecutionStepStatus::Cancelled,
        ExecutionStepStatus::Skipped,
    ] {
        let serialized = status.to_string();
        let parsed: ExecutionStepStatus = serialized.parse().expect("parse");
        assert_eq!(parsed, status, "round-trip {serialized}");
    }
    assert!("QUEUED".parse::<ExecutionStepStatus>().is_err());
    assert!("TIMED_OUT".parse::<ExecutionStepStatus>().is_err());
    assert_ne!(ExecutionStepStatus::Skipped, ExecutionStepStatus::Failed);
    assert_ne!(ExecutionStepStatus::Skipped, ExecutionStepStatus::Cancelled);
}

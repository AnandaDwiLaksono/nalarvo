use chrono::{Duration as ChronoDuration, Utc};
use nalarvo_domain::{CompanyId, PrincipalRef, Run, RunStatus};
use nalarvo_model_gateway::MockMode;
use nalarvo_persistence::create_pool;
use nalarvo_persistence::m4::{DurableJob, claim_job, create_run, get_run, queue_run};
use nalarvo_persistence::run_migrations;
use nalarvo_runtime::context::ContextInput;
use nalarvo_runtime::supervisor::{RuntimeSupervisor, SupervisorConfig};
use nalarvo_runtime::worker::{
    RunExecutionRequest, execute_run_worker_process, provider_invocation_count,
    worker_process_spawn_count,
};
use serde_json::json;
use sqlx::SqlitePool;
use std::path::Path;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

static PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ProcessCounters {
    spawns: usize,
    provider_calls: usize,
}

impl ProcessCounters {
    fn snapshot() -> Self {
        Self {
            spawns: worker_process_spawn_count(),
            provider_calls: provider_invocation_count(),
        }
    }

    fn assert_delta(&self, spawns: usize, provider_calls: usize) {
        assert_eq!(
            worker_process_spawn_count() - self.spawns,
            spawns,
            "worker spawns during this test"
        );
        assert_eq!(
            provider_invocation_count() - self.provider_calls,
            provider_calls,
            "provider calls during this test"
        );
    }
}

async fn setup_db() -> SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('workspace-1','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','workspace-1','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('r','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','r','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('a','c','A','d','r',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,created_at,updated_at) VALUES ('p','c','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('wi','c','p','W','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('wi2','c','p','W2','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,created_at,updated_at) VALUES ('provider-conn-default','workspace-1','PC','TEST','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','workspace-1','provider-conn-default','gpt-4o-mini','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    pool
}

async fn wait_terminal(pool: &SqlitePool, company_id: &CompanyId, run_id: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(run) = get_run(pool, company_id, run_id).await.unwrap()
                && run.status.is_terminal()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("run reached terminal status");
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

fn request(mode: MockMode) -> RunExecutionRequest {
    RunExecutionRequest {
        run_id: "process-run".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "conn".into(),
        base_url: None,
        auth_token: None,
        model_key: "mock-model".into(),
        context_input: ContextInput::minimal("c", "p", "Process test"),
        worker_principal_id: "worker".into(),
        lease_id: "lease".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: mode,
        cancel_rx: None,
    }
}

#[tokio::test]
async fn executes_in_real_worker_process_over_framed_stdio() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();
    let outcome = execute_run_worker_process(
        Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")),
        request(MockMode::SUCCESS_TEXT),
        None,
    )
    .await
    .expect("worker process");

    assert_eq!(outcome.status, RunStatus::Succeeded);
    assert_eq!(outcome.output_text.as_deref(), Some("hello"));
    counters.assert_delta(1, 1);
}

#[tokio::test]
async fn cancellation_reaches_worker_process_and_exits() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut req = request(MockMode::DELAYED_SUCCESS);
    req.cancel_rx = Some(cancel_rx);
    cancel_tx.send(true).unwrap();

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        execute_run_worker_process(Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")), req, None),
    )
    .await
    .expect("worker exits")
    .expect("worker process");

    assert_eq!(outcome.status, RunStatus::Failed);
    assert_eq!(outcome.failure_class.as_deref(), Some("PROVIDER_CANCELLED"));
    counters.assert_delta(1, 1);
}

// 1. Real Process Happy-Path Vertical
#[tokio::test]
async fn test_process_happy_path_vertical() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "SUCCESS_TEXT");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 5,
            heartbeat_interval_secs: 1,
            worker_principal_id: "system:supervisor-1".into(),
        },
    );

    let claimed = supervisor.poll_and_dispatch_once(None).await.unwrap();
    assert!(claimed, "Job should be claimed by supervisor");

    wait_terminal(&pool, &run.company_id, &run.id).await;
    while supervisor.current_active_count() > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify Run SUCCEEDED in persistence
    let fetched = get_run(&pool, &CompanyId("c".into()), &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, RunStatus::Succeeded);

    // Verify Steps persisted
    let steps: Vec<(String, String)> = sqlx::query_as("SELECT step_type, lifecycle_state FROM execution_steps WHERE run_id = ? ORDER BY sequence_no")
        .bind(&run.id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].1, "SUCCEEDED");
    assert_eq!(steps[1].1, "SUCCEEDED");

    // Verify Timeline, Result, and Usage persisted
    let timeline_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_events WHERE aggregate_type = 'Run' AND aggregate_id = ?",
    )
    .bind(&run.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(timeline_count > 0);

    let res_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(res_count, 1);

    let usage_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM usage_records WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(usage_count, 1);

    // Verify WorkItem remains IN_PROGRESS
    let wi_status: String = sqlx::query_scalar("SELECT status FROM work_items WHERE id = 'wi'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(wi_status, "IN_PROGRESS");

    // Verify zero M5 actions/tools tables
    let actions_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('actions', 'tool_invocations')")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(actions_count, 0);

    counters.assert_delta(1, 1);
}

// 2. Duplicate StartRun — Real Process Path
#[tokio::test]
async fn test_duplicate_start_run_real_process() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "DELAYED_SUCCESS");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 2,
            lease_duration_secs: 5,
            heartbeat_interval_secs: 1,
            worker_principal_id: "system:supervisor-1".into(),
        },
    );

    // First claim succeeds
    let claimed1 = supervisor.poll_and_dispatch_once(None).await.unwrap();
    assert!(claimed1);

    // Second claim for same run fails (already claimed/running)
    let claimed2 = supervisor.poll_and_dispatch_once(None).await.unwrap();
    assert!(!claimed2, "Duplicate dispatch should be rejected");

    wait_terminal(&pool, &run.company_id, &run.id).await;
    while supervisor.current_active_count() > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    counters.assert_delta(1, 1);
}

// 3. Stale Owner Provider Fencing
#[tokio::test]
async fn test_stale_owner_provider_fencing_proof() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "SUCCESS_TEXT");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    // Worker A claims job -> lease A (version 1)
    let lease_dur = ChronoDuration::seconds(5);
    let (_job_a, lease_a) = claim_job(&pool, "worker-a", lease_dur)
        .await
        .unwrap()
        .unwrap();

    // Lease A expires or Worker B preempts -> Worker B claims lease B (version 2)
    sqlx::query("UPDATE run_execution_leases SET worker_principal_id = 'worker-b', lease_version = lease_version + 1 WHERE id = ?")
        .bind(&lease_a.id)
        .execute(&pool)
        .await
        .unwrap();

    // Stale Worker A attempts to execute using lease A (version 1)
    let mut req_a = request(MockMode::SUCCESS_TEXT);
    req_a.run_id = run.id.clone();
    req_a.company_id = run.company_id.0.clone();
    req_a.project_id = run.project_id.clone();
    req_a.work_item_id = run.work_item_id.clone();
    req_a.agent_id = run.executing_agent_id.clone();
    req_a.worker_principal_id = "worker-a".into();
    req_a.lease_id = lease_a.id.clone();
    req_a.lease_version = lease_a.lease_version; // version 1 (stale!)
    req_a.pool = Some(pool.clone());

    let worker_bin = nalarvo_runtime::supervisor::resolve_worker_executable().unwrap();
    let res = execute_run_worker_process(&worker_bin, req_a, None).await;

    // Must be rejected by Core BEFORE model/provider invocation!
    assert!(res.is_err(), "Stale worker request must be rejected");
    let err_msg = res.unwrap_err();
    assert!(
        err_msg.contains("stale worker fencing"),
        "Error message should mention stale worker fencing: {err_msg}"
    );

    // Assert provider invocation count caused by stale A == 0!
    counters.assert_delta(1, 0);
}

// 4. Queued Cancellation Process Proof
#[tokio::test]
async fn test_queued_cancellation_process_proof() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "SUCCESS_TEXT");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    // Cancel run before supervisor poll
    sqlx::query("UPDATE runs SET lifecycle_state = 'CANCELLED', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();

    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 5,
            heartbeat_interval_secs: 1,
            worker_principal_id: "system:supervisor-1".into(),
        },
    );

    let _claimed = supervisor.poll_and_dispatch_once(None).await.unwrap();
    let mut attempts = 0;
    while supervisor.current_active_count() > 0 && attempts < 50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        attempts += 1;
    }

    counters.assert_delta(0, 0);
}

// 5. Renderer Disconnect / Reconnect Process Proof
#[tokio::test]
async fn test_renderer_disconnect_reconnect_process_proof() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "DELAYED_SUCCESS");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::channel(1);
    drop(rx); // Drop receiver (renderer disconnect!)

    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 5,
            heartbeat_interval_secs: 1,
            worker_principal_id: "system:supervisor-1".into(),
        },
    );

    let claimed = supervisor.poll_and_dispatch_once(Some(tx)).await.unwrap();
    assert!(claimed);

    wait_terminal(&pool, &run.company_id, &run.id).await;
    while supervisor.current_active_count() > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Query DB afterward (reconnect & assert state)
    let fetched = get_run(&pool, &CompanyId("c".into()), &run.id)
        .await
        .unwrap()
        .unwrap();
    if fetched.status != RunStatus::Succeeded {
        panic!(
            "Run failed with class: {:?}, detail: {:?}",
            fetched.failure_class, fetched.failure_detail
        );
    }
    assert_eq!(fetched.status, RunStatus::Succeeded);

    let steps: Vec<(String, String)> = sqlx::query_as("SELECT step_type, lifecycle_state FROM execution_steps WHERE run_id = ? ORDER BY sequence_no")
        .bind(&run.id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(steps.len(), 2);
    let res_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(res_count, 1);
    let usage_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM usage_records WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(usage_count, 1);

    counters.assert_delta(1, 1);
}

// 6. Process-Based Provider Timeout Proof
#[tokio::test]
async fn test_process_provider_timeout_proof() {
    let _guard = PROCESS_TEST_LOCK.lock().await;
    let counters = ProcessCounters::snapshot();

    let pool = setup_db().await;
    let run = sample_run("wi");
    create_run(&pool, &run).await.unwrap();
    let job = sample_job(&run, "TIMEOUT");
    let event = sample_event(&run, "RunQueued");
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();

    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 5,
            heartbeat_interval_secs: 1,
            worker_principal_id: "system:supervisor-1".into(),
        },
    );

    let claimed = supervisor.poll_and_dispatch_once(None).await.unwrap();
    assert!(claimed);

    wait_terminal(&pool, &run.company_id, &run.id).await;
    while supervisor.current_active_count() > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let fetched = get_run(&pool, &CompanyId("c".into()), &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, RunStatus::Failed);
    assert_eq!(fetched.failure_class.as_deref(), Some("PROVIDER_TIMEOUT"));

    // Verify ExecutionStep is FAILED (never TIMED_OUT)
    let step_statuses: Vec<String> =
        sqlx::query_scalar("SELECT lifecycle_state FROM execution_steps WHERE run_id = ?")
            .bind(&run.id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(step_statuses.contains(&"FAILED".to_string()));
    assert!(!step_statuses.contains(&"TIMED_OUT".to_string()));

    // Verify WorkItem remains IN_PROGRESS
    let wi_status: String = sqlx::query_scalar("SELECT status FROM work_items WHERE id = 'wi'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(wi_status, "IN_PROGRESS");

    counters.assert_delta(1, 1);
}

use chrono::{Duration as ChronoDuration, Utc};
use nalarvo_domain::{CompanyId, PrincipalRef, Run, RunStatus};
use nalarvo_model_gateway::MockMode;
use nalarvo_persistence::m4::{
    DurableJob, claim_job, create_run, expire_leases, get_run, queue_run,
};
use nalarvo_persistence::{create_pool, run_migrations};
use nalarvo_runtime::context::ContextInput;
use nalarvo_runtime::recovery::{RecoveryDecision, decide_recovery};
use nalarvo_runtime::supervisor::{RuntimeSupervisor, SupervisorConfig, start_runtime_supervisor};
use nalarvo_runtime::worker::{
    RunExecutionRequest, execute_run_worker_process, reset_worker_process_spawn_count,
    worker_process_spawn_count,
};
use serde_json::json;
use sqlx::SqlitePool;
use std::{path::Path, time::Duration};

static RECOVERY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn file_db(filename: &str) -> (tempfile::TempDir, SqlitePool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(filename);
    let pool = create_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    for query in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('r','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','r','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('a','c','A','d','r',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,created_at,updated_at) VALUES ('p','c','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('wi','c','p','W','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('wi2','c','p','W2','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,created_at,updated_at) VALUES ('pc','w','PC','TEST','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','w','pc','gpt-4o-mini','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    (dir, pool)
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

async fn enqueue(pool: &SqlitePool, run: &Run, scenario: &str) {
    create_run(pool, run).await.unwrap();
    let job = DurableJob {
        id: format!("job-{}", run.id),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({"run_id": run.id, "mock_scenario": scenario}),
        available_at: Utc::now() - ChronoDuration::seconds(1),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    };
    let event = nalarvo_domain::DomainEvent {
        event_id: format!("queued-{}", run.id),
        event_type: "RunQueued".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: run.row_version,
        occurred_at: Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::user("u"),
        scope: nalarvo_domain::ScopeRef::company("c"),
        payload: json!({"run_id": run.id}),
    };
    queue_run(
        pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();
}

fn config(worker: &str) -> SupervisorConfig {
    SupervisorConfig {
        max_concurrency: 1,
        lease_duration_secs: 10,
        heartbeat_interval_secs: 2,
        worker_principal_id: worker.into(),
    }
}

async fn wait_idle(supervisor: &RuntimeSupervisor) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while supervisor.current_active_count() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("execution task completes");
}

#[tokio::test]
async fn expired_claim_recovers_after_supervisor_restart_and_persists_to_disk() {
    let (_dir, pool) = file_db("recovery.sqlite").await;
    let run = sample_run("wi");
    enqueue(&pool, &run, "SUCCESS_TEXT").await;

    // Simulate the old daemon dying after claiming a durable job with a short 1s lease.
    let (claimed, old_lease) = claim_job(&pool, "old-daemon", ChronoDuration::seconds(1))
        .await
        .unwrap()
        .expect("first claim");
    assert_eq!(claimed.run_id, run.id);

    // Natural time elapses without manual SQL updates; lease expires honestly
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let expired_count = expire_leases(&pool, Utc::now()).await.unwrap();
    assert_eq!(expired_count, 1);
    drop(pool);

    let path = _dir.path().join("recovery.sqlite");
    let reopened = create_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let before = get_run(&reopened, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, RunStatus::Queued);
    assert!(
        !nalarvo_persistence::m4::verify_current_lease(
            &reopened,
            &run.id,
            &old_lease.worker_principal_id,
            &old_lease.id,
            old_lease.lease_version,
        )
        .await
        .unwrap()
    );

    let restarted = RuntimeSupervisor::new(reopened.clone(), config("new-daemon"));
    assert!(restarted.poll_and_dispatch_once(None).await.unwrap());
    wait_idle(&restarted).await;

    // Reopen the database again: assertions cover SQLite durability, not pooled memory state.
    drop(restarted);
    drop(reopened);
    let durable = create_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let finished = get_run(&durable, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.status, RunStatus::Succeeded);
    let results: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_results WHERE run_id = ?")
        .bind(&run.id)
        .fetch_one(&durable)
        .await
        .unwrap();
    assert_eq!(results, 1);
    let attempts: i64 = sqlx::query_scalar("SELECT attempt FROM durable_jobs WHERE run_id = ?")
        .bind(&run.id)
        .fetch_one(&durable)
        .await
        .unwrap();
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn real_worker_process_can_be_cancelled_and_next_child_still_completes() {
    let _guard = RECOVERY_TEST_LOCK.lock().await;
    let executable = Path::new(env!("CARGO_BIN_EXE_nalarvo-worker"));
    let request = RunExecutionRequest {
        run_id: "cancelled-real-child".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        model_profile_version_id: None,
        provider_connection_id: "pc".into(),
        base_url: None,
        auth_token: None,
        model_key: "test-model".into(),
        context_input: ContextInput::minimal("c", "p", "Cancel child"),
        worker_principal_id: "test-supervisor".into(),
        lease_id: "test-lease".into(),
        lease_version: 1,
        pool: None,
        expected_run_version: 1,
        mock_mode: MockMode::DELAYED_SUCCESS,
        cancel_rx: None,
    };
    let task = tokio::spawn(execute_run_worker_process(
        executable,
        request.clone(),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(15)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    // kill_on_drop and process reap: prove the next real child process executes cleanly
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        execute_run_worker_process(executable, request, None),
    )
    .await
    .expect("second child is reaped and protocol can run")
    .unwrap();
    assert_eq!(outcome.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn real_child_crash_recovery_flow_leaves_db_consistent() {
    let _guard = RECOVERY_TEST_LOCK.lock().await;
    reset_worker_process_spawn_count();
    let (_dir, pool) = file_db("crash_recovery.sqlite").await;
    let run = sample_run("wi");
    enqueue(&pool, &run, "DELAYED_SUCCESS").await;

    // Configure 2-second lease (heartbeat 1s) to expire naturally without manual SQL
    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 2,
            heartbeat_interval_secs: 1,
            worker_principal_id: "supervisor-crash-test".into(),
        },
    );
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());

    // Allow worker execution task to start and transition run to RUNNING
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Some(r)) = get_run(&pool, &run.company_id, &run.id).await
                && r.status == RunStatus::Running
                && worker_process_spawn_count() >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run transitioned to RUNNING and worker spawned");

    // Verify recovery logic decision for interrupted run with 0 completed steps
    let current_run = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_run.status, RunStatus::Running);

    let decision = decide_recovery(&current_run.status.to_string(), false, 0).unwrap();
    assert_eq!(decision, RecoveryDecision::RequeueJob);

    // Simulate crash: stop supervisor and await worker tasks
    supervisor.shutdown().await;
    drop(supervisor);
    drop(pool);

    // Wait for the 2-second lease to naturally expire without manual SQL rewriting
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // Spawn new supervisor on reopened DB and prove PRODUCTION reconciliation marks the crashed run FAILED safely
    let path = _dir.path().join("crash_recovery.sqlite");
    let recovered_pool = create_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let new_supervisor =
        RuntimeSupervisor::new(recovered_pool.clone(), config("supervisor-recovered"));
    let reconciled = new_supervisor.reconcile_orphaned_runs().await.unwrap();
    assert_eq!(reconciled, 1);

    // Reconcile again to prove IDEMPOTENCY: second pass must reconcile 0
    let reconciled_again = new_supervisor.reconcile_orphaned_runs().await.unwrap();
    assert_eq!(reconciled_again, 0);

    let final_run = get_run(&recovered_pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_run.status, RunStatus::Failed);
    assert_eq!(final_run.failure_class.as_deref(), Some("WORKER_LOST"));

    // Verify consistent result and event creation
    let result_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&recovered_pool)
            .await
            .unwrap();
    assert_eq!(result_count, 1);
}

#[tokio::test]
async fn daemon_graceful_shutdown_reaps_background_supervisor_cleanly() {
    let (_dir, pool) = file_db("daemon_reap.sqlite").await;
    let run = sample_run("wi");
    enqueue(&pool, &run, "DELAYED_SUCCESS").await;

    let cfg = SupervisorConfig {
        max_concurrency: 2,
        lease_duration_secs: 2,
        heartbeat_interval_secs: 1,
        worker_principal_id: "daemon-supervisor".into(),
    };

    let handle = start_runtime_supervisor(pool.clone(), cfg);

    // Wait until supervisor starts execution and run becomes RUNNING
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Some(r)) = get_run(&pool, &run.company_id, &run.id).await
                && r.status == RunStatus::Running
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("run transitioned to RUNNING");

    // Structured daemon shutdown gracefully shuts down supervisor and terminates workers
    handle.shutdown().await;

    // Restart while old lease is still valid; shutdown must release it promptly.
    let leases: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM run_execution_leases WHERE run_id = ? AND released_at IS NULL",
    )
    .bind(&run.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(leases, 0);

    // Start a new supervisor, proving background periodic reconciliation terminalizes the orphan
    let restarted = start_runtime_supervisor(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 2,
            lease_duration_secs: 2,
            heartbeat_interval_secs: 1,
            worker_principal_id: "daemon-supervisor-restarted".into(),
        },
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Some(r)) = get_run(&pool, &run.company_id, &run.id).await
                && r.status == RunStatus::Failed
                && r.failure_class.as_deref() == Some("WORKER_LOST")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("interrupted run reconciled to FAILED with WORKER_LOST");

    restarted.shutdown().await;

    // Verify DB remains consistent
    let fetched = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, RunStatus::Failed);
    assert_eq!(fetched.failure_class.as_deref(), Some("WORKER_LOST"));
}

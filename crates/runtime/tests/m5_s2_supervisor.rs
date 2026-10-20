use chrono::Utc;
use nalarvo_application::ApplicationContext;
use nalarvo_domain::{CompanyId, PrincipalRef, Run, RunStatus};
use nalarvo_model_gateway::ModelResponse;
use nalarvo_persistence::{
    create_pool,
    m4::{DurableJob, create_run, get_run, queue_run},
    run_migrations,
};
use nalarvo_runtime::{
    core_dispatch,
    supervisor::{RuntimeSupervisor, SupervisorConfig},
    worker::{
        reset_worker_process_spawn_count, worker_process_reap_count, worker_process_spawn_count,
    },
};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup() -> (tempfile::TempDir, SqlitePool, Run) {
    let root =
        tempfile::tempdir_in(std::env::var("TMPDIR").expect("Hermes scratch TMPDIR")).unwrap();
    let pool = create_pool(&format!(
        "sqlite://{}",
        root.path().join("approval.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('d','c','D','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','d','role','2026-01-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('a','c','A','d','role',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES('wi','c','p','W','IN_PROGRESS','TASK','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES('team','c','p','T','ACTIVE','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES('alloc','c','p','team','a','ACTIVE','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,status,assigned_at) VALUES('assignment','c','p','wi','a','alloc','ACTIVE','2026-01-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,provider_kind,name,status,row_version,created_at,updated_at) VALUES('pc','w','mock','Mock','ENABLED',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd','w','filesystem.write','filesystem','write','2026-01-01T00:00:00Z')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg','w','c','pd','SYSTEM','worker-a','PROJECT','p','ALLOW','2026-01-01T00:00:00Z')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg2','w','c','pd','SYSTEM','worker-b','PROJECT','p','ALLOW','2026-01-01T00:00:00Z')",
        "INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol','w','c','p1','ACTIVE',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv1','pol',1,'{\"permission\":\"filesystem.write\",\"effect\":\"REQUIRE_APPROVAL\"}','2026-01-01T00:00:00Z')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    sqlx::query("UPDATE projects SET working_root_path=? WHERE id='p'")
        .bind(root.path().to_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let run = Run::create(
        CompanyId("c".into()),
        "p".into(),
        "wi".into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "corr".into(),
    )
    .unwrap();
    create_run(&pool, &run).await.unwrap();
    let job = DurableJob {
        id: "job".into(),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({"mock_scenario":"TOOL_PROPOSAL","provider_connection_id":"pc"}),
        available_at: Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    };
    let event = nalarvo_domain::DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
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
        payload: json!({"run_id":run.id}),
    };
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
    (root, pool, run)
}
fn supervisor(pool: SqlitePool, worker: &str) -> RuntimeSupervisor {
    RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            worker_principal_id: worker.into(),
            lease_duration_secs: 10,
            heartbeat_interval_secs: 2,
        },
    )
    .with_action_handler(core_dispatch::handler(ApplicationContext::new(pool)))
}
async fn idle(supervisor: &RuntimeSupervisor) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if supervisor.current_active_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn heartbeat_during_model_request_preserves_core_action_fence() {
    let _guard = TEST_LOCK.lock().await;
    let (_root, pool, run) = setup().await;
    let seen_version = Arc::new(AtomicI64::new(0));
    let seen = seen_version.clone();
    let observer_pool = pool.clone();
    let model: nalarvo_runtime::worker::ModelRequestHandler = Arc::new(move |ctx, _, _| {
        let pool = observer_pool.clone();
        let seen = seen.clone();
        Box::pin(async move {
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let version: i64 = sqlx::query_scalar(
                        "SELECT lease_version FROM run_execution_leases WHERE id=?",
                    )
                    .bind(&ctx.lease_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    if version > ctx.lease_version {
                        seen.store(version, Ordering::SeqCst);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            Ok(ModelResponse {
                text: None,
                structured: Some(
                    json!({"call_id":"write-1","tool_name":"filesystem.write","target":"note.txt","arguments":{"path":"note.txt","content":"approved content"}}),
                ),
                usage: None,
                request_id: Some("model-request".into()),
            })
        })
    });
    let s = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            worker_principal_id: "worker-a".into(),
            lease_duration_secs: 10,
            heartbeat_interval_secs: 1,
        },
    )
    .with_handlers(
        core_dispatch::handler(ApplicationContext::new(pool.clone())),
        model,
    );
    assert!(s.poll_and_dispatch_once(None).await.unwrap());
    idle(&s).await;
    assert!(
        seen_version.load(Ordering::SeqCst) > 1,
        "model request overlapped DB heartbeat"
    );
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::WaitingApproval
    );
    let row = sqlx::query("SELECT a.id,p.id,json_extract(e.payload,'$.fingerprint') FROM actions a JOIN approval_requests p ON p.action_id=a.id JOIN domain_events e ON e.aggregate_id=a.id AND e.event_type='ActionProposed' WHERE a.run_id=?")
        .bind(&run.id).fetch_one(&pool).await.unwrap();
    let (action, approval, fingerprint): (String, String, String) =
        (row.get(0), row.get(1), row.get(2));
    let state: String = sqlx::query_scalar("SELECT status FROM actions WHERE id=?")
        .bind(&action)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "PENDING");
    assert!(!fingerprint.is_empty());
    ApplicationContext::new(pool.clone())
        .decide_action_approval(
            "c",
            &approval,
            PrincipalRef::user("u"),
            &fingerprint,
            true,
            None,
        )
        .await
        .unwrap();
    assert!(s.poll_and_dispatch_once(None).await.unwrap());
    idle(&s).await;
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id=?")
            .bind(&action)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "SUCCEEDED"
    );
    s.shutdown().await;
}

#[tokio::test]
async fn shell_approval_real_worker_survives_supervisor_heartbeat() {
    shell_approval_real_worker_with_heartbeat(false).await;
}

#[tokio::test]
async fn shell_failure_real_worker_survives_supervisor_heartbeat() {
    shell_approval_real_worker_with_heartbeat(true).await;
}

async fn shell_approval_real_worker_with_heartbeat(failed: bool) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let _guard = TEST_LOCK.lock().await;
    let (root, pool, run) = setup().await;
    sqlx::query("UPDATE permission_definitions SET name='shell.execute',resource='shell',operation='execute' WHERE id='pd'").execute(&pool).await.unwrap();
    sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"shell.execute\",\"effect\":\"REQUIRE_APPROVAL\"}'").execute(&pool).await.unwrap();
    let binary = tempfile::tempdir().unwrap();
    let exe = binary.path().join("trusted.exe");
    assert!(
        std::process::Command::new("rustc")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/shell_fixture.rs"
            ))
            .args(["--edition=2024", "-o"])
            .arg(&exe)
            .status()
            .unwrap()
            .success()
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let args = vec![
        if failed { "tree-failure" } else { "tree" }.to_string(),
        listener.local_addr().unwrap().to_string(),
    ];
    let policy = nalarvo_runtime::shell::ShellPolicy::from_json(
        &json!([{"executable":exe,"args":args,"timeout_ms":30000}]).to_string(),
    )
    .unwrap();
    let proposal = json!({"call_id":"shell-heartbeat","tool_name":"shell.execute","target":exe,"arguments":{"executable":exe,"args":args}});
    let model: nalarvo_runtime::worker::ModelRequestHandler = Arc::new(move |_, _, _| {
        let proposal = proposal.clone();
        Box::pin(async move {
            Ok(ModelResponse {
                text: None,
                structured: Some(proposal),
                usage: None,
                request_id: None,
            })
        })
    });
    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            worker_principal_id: "worker-a".into(),
            lease_duration_secs: 10,
            heartbeat_interval_secs: 1,
        },
    )
    .with_handlers(
        core_dispatch::handler_with_shell(ApplicationContext::new(pool.clone()), policy),
        model,
    );
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());
    idle(&supervisor).await;
    let (approval,fp):(String,String)=sqlx::query_as("SELECT p.id,a.fingerprint FROM actions a JOIN approval_requests p ON p.action_id=a.id WHERE a.run_id=?").bind(&run.id).fetch_one(&pool).await.unwrap();
    ApplicationContext::new(pool.clone())
        .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
        .await
        .unwrap();
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());
    let mut streams = Vec::new();
    for _ in 0..2 {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(8), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut stream = BufReader::new(stream);
        let mut ready = String::new();
        stream.read_line(&mut ready).await.unwrap();
        streams.push((ready.starts_with("parent "), stream));
    }
    tokio::time::timeout(Duration::from_secs(8),async {
        loop {
            let version:i64=sqlx::query_scalar("SELECT lease_version FROM run_execution_leases WHERE run_id=? AND released_at IS NULL").bind(&run.id).fetch_one(&pool).await.unwrap();
            if version>1 { break; } tokio::task::yield_now().await;
        }
    }).await.unwrap();
    streams
        .iter_mut()
        .find(|(parent, _)| *parent)
        .unwrap()
        .1
        .get_mut()
        .write_all(b"q")
        .await
        .unwrap();
    idle(&supervisor).await;
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        if failed {
            RunStatus::Failed
        } else {
            RunStatus::Succeeded
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM actions WHERE status=? AND authorization='APPROVED'"
        )
        .bind(if failed { "FAILED" } else { "SUCCEEDED" })
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert!(!root.path().join("descendant-effect.txt").exists());
    supervisor.shutdown().await;
}

#[tokio::test]
async fn orphaned_pending_approval_recovers_before_generic_failure() {
    let _guard = TEST_LOCK.lock().await;
    let (_root, pool, run) = setup().await;
    let (_action, _approval, _fingerprint) = suspended(&pool, &run).await;
    // Recreate the crash seam: proposal committed; suspension transaction not committed.
    sqlx::query("DELETE FROM runtime_checkpoints WHERE run_id=?")
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET lifecycle_state='RUNNING' WHERE id=?")
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE durable_jobs SET status='CLAIMED',lease_owner='worker-a',lease_until='2000-01-01T00:00:00Z' WHERE run_id=?")
        .bind(&run.id).execute(&pool).await.unwrap();
    sqlx::query("UPDATE run_execution_leases SET released_at=NULL,release_reason=NULL,expires_at='2000-01-01T00:00:00Z' WHERE run_id=?")
        .bind(&run.id).execute(&pool).await.unwrap();
    let s = supervisor(pool.clone(), "recovery");
    assert_eq!(s.reconcile_orphaned_runs().await.unwrap(), 1);
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::WaitingApproval
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "WAITING_APPROVAL"
    );
    assert_eq!(s.reconcile_orphaned_runs().await.unwrap(), 0);
    s.shutdown().await;
}

#[tokio::test]
async fn orphaned_approved_claim_requeues_without_restarting_effect() {
    let _guard = TEST_LOCK.lock().await;
    let (_root, pool, run) = setup().await;
    let (_action, approval, fingerprint) = suspended(&pool, &run).await;
    ApplicationContext::new(pool.clone())
        .decide_action_approval(
            "c",
            &approval,
            PrincipalRef::user("u"),
            &fingerprint,
            true,
            None,
        )
        .await
        .unwrap();
    let (_job, _lease) =
        nalarvo_persistence::m4::claim_job(&pool, "worker-b", chrono::Duration::seconds(10))
            .await
            .unwrap()
            .unwrap();
    let queued = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    nalarvo_persistence::m4::transition_run_with_lease(
        &pool,
        &_lease,
        &run.company_id,
        &run.id,
        queued.row_version,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .unwrap();
    sqlx::query("UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&_lease.id)
        .execute(&pool)
        .await
        .unwrap();
    let s = supervisor(pool.clone(), "recovery");
    assert_eq!(s.reconcile_orphaned_runs().await.unwrap(), 1);
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Queued
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool_invocations WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    s.shutdown().await;
}

async fn suspended(pool: &SqlitePool, run: &Run) -> (String, String, String) {
    let a = supervisor(pool.clone(), "worker-a");
    assert!(a.poll_and_dispatch_once(None).await.unwrap());
    idle(&a).await;
    assert_eq!(
        get_run(pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::WaitingApproval
    );
    assert!(!a.poll_and_dispatch_once(None).await.unwrap());
    let row=sqlx::query("SELECT a.id,p.id,json_extract(e.payload,'$.fingerprint') FROM actions a JOIN approval_requests p ON p.action_id=a.id JOIN domain_events e ON e.aggregate_id=a.id AND e.event_type='ActionProposed' WHERE a.run_id=?")
        .bind(&run.id).fetch_one(pool).await.unwrap();
    let state: String = sqlx::query_scalar("SELECT status FROM durable_jobs WHERE run_id=?")
        .bind(&run.id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(state, "WAITING_APPROVAL");
    let version = get_run(pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap()
        .row_version;
    assert!(
        ApplicationContext::new(pool.clone())
            .resume_run(&run.company_id, "p", &run.id, version)
            .await
            .is_err()
    );
    let job = DurableJob {
        id: "bypass".into(),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({}),
        available_at: Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 1,
        correlation_id: run.correlation_id.clone(),
    };
    let event = nalarvo_domain::DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        event_type: "RunQueued".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: version + 1,
        occurred_at: Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::user("u"),
        scope: nalarvo_domain::ScopeRef::company("c"),
        payload: json!({"run_id":run.id}),
    };
    assert!(
        queue_run(pool, &run.company_id, &run.id, version, &job, &event)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM run_execution_leases WHERE run_id=? AND released_at IS NOT NULL"
        )
        .bind(&run.id)
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_results WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    a.shutdown().await;
    (row.get(0), row.get(1), row.get(2))
}
#[tokio::test]
async fn approval_restart_resumes_same_action_step_once() {
    let _guard = TEST_LOCK.lock().await;
    reset_worker_process_spawn_count();
    let (root, pool, run) = setup().await;
    let (action_id, approval_id, fp) = suspended(&pool, &run).await;
    assert!(!root.path().join("note.txt").exists());
    assert_eq!(worker_process_spawn_count(), 1);
    assert_eq!(
        worker_process_reap_count(),
        1,
        "worker A wait() completed before suspension"
    );
    let step_id: String = sqlx::query_scalar("SELECT step_id FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    drop(pool);
    let pool = create_pool(&format!(
        "sqlite://{}",
        root.path().join("approval.db").display()
    ))
    .await
    .unwrap();
    let app = ApplicationContext::new(pool.clone());
    app.decide_action_approval("c", &approval_id, PrincipalRef::user("u"), &fp, true, None)
        .await
        .unwrap();
    let b = supervisor(pool.clone(), "worker-b");
    assert!(b.poll_and_dispatch_once(None).await.unwrap());
    idle(&b).await;
    let status = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap()
        .status;
    if status != RunStatus::Succeeded {
        let states: Vec<(String, String)> =
            sqlx::query_as("SELECT authorization,status FROM actions WHERE run_id=?")
                .bind(&run.id)
                .fetch_all(&pool)
                .await
                .unwrap();
        panic!(
            "Run {status:?}, Actions {states:?}, file {}",
            root.path().join("note.txt").exists()
        );
    }
    assert!(Path::new(&root.path().join("note.txt")).exists());
    for table in [
        "actions",
        "execution_steps",
        "tool_invocations",
        "runtime_results",
    ] {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE run_id=?"))
                .bind(&run.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "{table}");
    }
    let approvals: i64 =
        sqlx::query_scalar("SELECT count(*) FROM approval_requests WHERE action_id=?")
            .bind(&action_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(approvals, 1);
    let resumed_step: String = sqlx::query_scalar("SELECT step_id FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(resumed_step, step_id);
    let step_state: String =
        sqlx::query_scalar("SELECT lifecycle_state FROM execution_steps WHERE id=?")
            .bind(&step_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(step_state, "SUCCEEDED");
    let model_invocations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM model_invocations WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        model_invocations, 0,
        "continuation performs no model request"
    );
    let attempts: i64 = sqlx::query_scalar("SELECT attempt FROM durable_jobs WHERE id='job'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(worker_process_spawn_count(), 2);
    assert_eq!(
        worker_process_reap_count(),
        2,
        "both child processes reaped"
    );
    let leases: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM run_execution_leases WHERE run_id=? AND released_at IS NOT NULL",
    )
    .bind(&run.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(leases, 2);
    let _ = b.poll_and_dispatch_once(None).await.unwrap();
    idle(&b).await;
    let duplicate = supervisor(pool.clone(), "worker-b");
    let _ = duplicate.poll_and_dispatch_once(None).await.unwrap();
    idle(&duplicate).await;
    duplicate.shutdown().await;
    let events: Vec<String> = sqlx::query_scalar("SELECT e.event_type FROM domain_events e JOIN outbox_messages o ON o.domain_event_id=e.id ORDER BY e.rowid")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(
        events,
        [
            "RunQueued",
            "ActionProposed",
            "ActionApprovalRequired",
            "ApprovalRequested",
            "RunWaitingApproval",
            "ApprovalGranted",
            "ActionApproved",
            "RunQueued",
            "RunResumed",
            "ActionStarted",
            "ActionSucceeded",
            "RunSucceeded"
        ]
    );
    println!("approval vertical: {events:?}");
    b.shutdown().await;
}
#[tokio::test]
async fn approved_resume_outbox_failure_rolls_back_state_and_event() {
    let _guard = TEST_LOCK.lock().await;
    let (_root, pool, run) = setup().await;
    let (action, approval, fingerprint) = suspended(&pool, &run).await;
    let app = ApplicationContext::new(pool.clone());
    app.decide_action_approval(
        "c",
        &approval,
        PrincipalRef::user("u"),
        &fingerprint,
        true,
        None,
    )
    .await
    .unwrap();
    let (_job, lease) =
        nalarvo_persistence::m4::claim_job(&pool, "worker-b", chrono::Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
    let row = sqlx::query(
        "SELECT step_id,input,idempotency_key,action_type,target FROM actions WHERE id=?",
    )
    .bind(&action)
    .fetch_one(&pool)
    .await
    .unwrap();
    let cmd = nalarvo_application::ProposeActionCommand {
        company_id: "c".into(),
        project_id: Some("p".into()),
        run_id: Some(run.id.clone()),
        step_id: row.get("step_id"),
        actor: PrincipalRef::system("worker-b"),
        action_type: row.get("action_type"),
        target: row.get("target"),
        parameters: Some(serde_json::from_str(&row.get::<String, _>("input")).unwrap()),
        idempotency_key: row.get("idempotency_key"),
        meta: Default::default(),
    };
    let waiting = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, RunStatus::WaitingApproval);
    sqlx::query("CREATE TRIGGER fail_resumed BEFORE INSERT ON outbox_messages WHEN (SELECT event_type FROM domain_events WHERE id=NEW.domain_event_id)='RunResumed' BEGIN SELECT RAISE(ABORT,'forced resumed outbox fault'); END").execute(&pool).await.unwrap();
    assert!(
        app.resume_approved_runtime_action(&cmd, &action, &approval, &lease, waiting.row_version)
            .await
            .is_err()
    );
    let rolled_back = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (rolled_back.status, rolled_back.row_version),
        (waiting.status, waiting.row_version)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool_invocations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_resumed")
        .execute(&pool)
        .await
        .unwrap();
    let resumed = app
        .resume_approved_runtime_action(&cmd, &action, &approval, &lease, waiting.row_version)
        .await
        .unwrap();
    assert_eq!(resumed.status, RunStatus::Running);
    assert!(
        app.resume_approved_runtime_action(&cmd, &action, &approval, &lease, resumed.row_version)
            .await
            .is_err()
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM domain_events e JOIN outbox_messages o ON o.domain_event_id=e.id WHERE e.event_type='RunResumed'").fetch_one(&pool).await.unwrap(),1);
}

#[tokio::test]
async fn resumed_worker_revalidates_authority() {
    let _guard = TEST_LOCK.lock().await;
    let (root, pool, run) = setup().await;
    let (_action, approval, fp) = suspended(&pool, &run).await;
    ApplicationContext::new(pool.clone())
        .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
        .await
        .unwrap();
    sqlx::query("DELETE FROM permission_grants WHERE id='pg2'")
        .execute(&pool)
        .await
        .unwrap();
    let b = supervisor(pool.clone(), "worker-b");
    assert!(b.poll_and_dispatch_once(None).await.unwrap());
    idle(&b).await;
    assert!(!root.path().join("note.txt").exists());
    assert_ne!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actions WHERE run_id=?")
        .bind(&run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(actions, 1);
    let forbidden: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(forbidden, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool_invocations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    b.shutdown().await;
}
#[tokio::test]
async fn worker_dispatch_rejects_grant_constraints_before_tool_execution() {
    let _guard = TEST_LOCK.lock().await;
    for constraints in [
        "{}",
        "{",
        r#"{"maximum_tool_calls":0}"#,
        r#"{"maximum_tool_calls":1}"#,
    ] {
        let (root, pool, run) = setup().await;
        sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"ALLOW\"}'").execute(&pool).await.unwrap();
        sqlx::query("UPDATE permission_grants SET constraints=?")
            .bind(constraints)
            .execute(&pool)
            .await
            .unwrap();
        let a = supervisor(pool.clone(), "worker-a");
        assert!(a.poll_and_dispatch_once(None).await.unwrap());
        idle(&a).await;
        let success = constraints == "{}";
        assert_eq!(
            get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status
                == RunStatus::Succeeded,
            success
        );
        assert_eq!(root.path().join("note.txt").exists(), success);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool_invocations")
                .fetch_one(&pool)
                .await
                .unwrap(),
            i64::from(success)
        );
        if !success {
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM resource_reservations")
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM domain_events WHERE event_type='ActionStarted'"
                )
                .fetch_one(&pool)
                .await
                .unwrap(),
                0
            );
        }
        a.shutdown().await;
    }
}

#[tokio::test]
async fn approved_resume_revalidates_changed_grant_constraints() {
    let _guard = TEST_LOCK.lock().await;
    for constraints in [
        "{",
        r#"{"maximum_tool_calls":0}"#,
        r#"{"maximum_tool_calls":1}"#,
    ] {
        let (root, pool, run) = setup().await;
        let (_action, approval, fp) = suspended(&pool, &run).await;
        ApplicationContext::new(pool.clone())
            .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
            .await
            .unwrap();
        sqlx::query("UPDATE permission_grants SET constraints=?")
            .bind(constraints)
            .execute(&pool)
            .await
            .unwrap();
        let b = supervisor(pool.clone(), "worker-b");
        assert!(b.poll_and_dispatch_once(None).await.unwrap());
        idle(&b).await;
        assert!(!root.path().join("note.txt").exists());
        assert_ne!(
            get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Succeeded
        );
        for table in ["tool_invocations", "resource_reservations"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0,
                "{table}"
            );
        }
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')").fetch_one(&pool).await.unwrap(), 0);
        b.shutdown().await;
    }
}

#[tokio::test]
async fn approved_resume_rechecks_approval_expiry_without_execution() {
    let _guard = TEST_LOCK.lock().await;
    for expiry in ["not-a-time", "2000-01-01T14:00:00+14:00"] {
        let (root, pool, run) = setup().await;
        let (_action, approval, fp) = suspended(&pool, &run).await;
        ApplicationContext::new(pool.clone())
            .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
            .await
            .unwrap();
        sqlx::query("UPDATE approval_requests SET expires_at=? WHERE id=?")
            .bind(expiry)
            .bind(&approval)
            .execute(&pool)
            .await
            .unwrap();
        let b = supervisor(pool.clone(), "worker-b");
        assert!(b.poll_and_dispatch_once(None).await.unwrap());
        idle(&b).await;
        assert!(!root.path().join("note.txt").exists());
        assert_ne!(
            get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Succeeded
        );
        for table in ["tool_invocations", "resource_reservations"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0,
                "{table}"
            );
        }
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')").fetch_one(&pool).await.unwrap(), 0);
        b.shutdown().await;
    }
}

#[tokio::test]
async fn approved_resume_with_invalid_or_expired_grant_never_executes() {
    let _guard = TEST_LOCK.lock().await;
    for expiry in ["not-a-time", "2000-01-01T14:00:00+14:00"] {
        let (root, pool, run) = setup().await;
        let (_action, approval, fp) = suspended(&pool, &run).await;
        ApplicationContext::new(pool.clone())
            .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
            .await
            .unwrap();
        sqlx::query("UPDATE permission_grants SET expires_at=?")
            .bind(expiry)
            .execute(&pool)
            .await
            .unwrap();
        let b = supervisor(pool.clone(), "worker-b");
        assert!(b.poll_and_dispatch_once(None).await.unwrap());
        idle(&b).await;
        assert!(!root.path().join("note.txt").exists());
        assert_ne!(
            get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Succeeded
        );
        for table in ["tool_invocations", "resource_reservations"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0,
                "{table}"
            );
        }
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')").fetch_one(&pool).await.unwrap(), 0);
        b.shutdown().await;
    }
}

#[tokio::test]
async fn approved_resume_with_invalid_policy_never_executes() {
    let _guard = TEST_LOCK.lock().await;
    let (root, pool, run) = setup().await;
    let (_action, approval, fp) = suspended(&pool, &run).await;
    ApplicationContext::new(pool.clone())
        .decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
        .await
        .unwrap();
    sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"REQUIRE_APROVAL\"}'")
        .execute(&pool).await.unwrap();
    let b = supervisor(pool.clone(), "worker-b");
    assert!(b.poll_and_dispatch_once(None).await.unwrap());
    idle(&b).await;
    assert!(!root.path().join("note.txt").exists());
    assert_ne!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    for table in ["tool_invocations", "resource_reservations"] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap(),
            0,
            "{table}"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE event_type IN ('RunResumed','ActionStarted')"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    b.shutdown().await;
}

#[tokio::test]
async fn cancelled_waiting_run_cannot_resurrect() {
    let _guard = TEST_LOCK.lock().await;
    let (_root, pool, run) = setup().await;
    let (_action, approval, fp) = suspended(&pool, &run).await;
    let app = ApplicationContext::new(pool.clone());
    let current = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    app.cancel_run(
        &run.company_id,
        "p",
        &run.id,
        current.row_version,
        Some("cancel".into()),
    )
    .await
    .unwrap();
    assert!(
        app.decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, true, None)
            .await
            .is_err()
    );
    let b = supervisor(pool.clone(), "worker-b");
    assert!(!b.poll_and_dispatch_once(None).await.unwrap());
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Cancelled
    );
    let job: String = sqlx::query_scalar("SELECT status FROM durable_jobs WHERE id='job'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job, "CANCELLED");
    let checkpoint: i64 =
        sqlx::query_scalar("SELECT safe_to_resume FROM runtime_checkpoints WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(checkpoint, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE event_type='RunResumed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    b.shutdown().await;
}
#[tokio::test]
async fn rejection_expiry_never_queue_suspended_job() {
    let _guard = TEST_LOCK.lock().await;
    for expire in [false, true] {
        let (_root, pool, run) = setup().await;
        let (_action, approval, fp) = suspended(&pool, &run).await;
        let app = ApplicationContext::new(pool.clone());
        if expire {
            app.expire_approval("c", &approval, Some("timeout".into()))
                .await
                .unwrap();
        } else {
            app.decide_action_approval("c", &approval, PrincipalRef::user("u"), &fp, false, None)
                .await
                .unwrap();
        }
        let b = supervisor(pool.clone(), "worker-b");
        assert!(!b.poll_and_dispatch_once(None).await.unwrap());
        let state: String = sqlx::query_scalar("SELECT status FROM durable_jobs WHERE id='job'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "CANCELLED");
        assert_eq!(
            get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Cancelled
        );
        let resumable: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_checkpoints WHERE run_id=? AND safe_to_resume=1",
        )
        .bind(&run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(resumable, 0);
        b.shutdown().await;
    }
}

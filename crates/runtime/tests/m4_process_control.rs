use axum::{Router, routing::post};
use chrono::{Duration as ChronoDuration, Utc};
use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
};
use nalarvo_domain::{
    CompanyId, DomainEvent, PrincipalRef, PrincipalType, Run, RunStatus, ScopeRef,
};
use nalarvo_persistence::create_pool;
use nalarvo_persistence::m4::{DurableJob, create_run, get_run, queue_run};
use nalarvo_persistence::run_migrations;
use nalarvo_runtime::worker::{reset_worker_process_spawn_count, worker_process_spawn_count};
use nalarvo_runtime::{
    core_model,
    supervisor::{RuntimeSupervisor, SupervisorConfig},
};
use serde_json::json;
use sqlx::SqlitePool;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::net::TcpListener;

static CONTROL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ControlSecrets;

impl SecretStore for ControlSecrets {
    fn get(&self, reference: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        assert_eq!(reference.id, "control-credential");
        Ok(SecretValue::new("mock-token"))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}

async fn setup() -> SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    for sql in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('workspace-1','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','workspace-1','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,status,created_at,updated_at) VALUES ('control-credential','workspace-1','Key','secretstore://control-credential','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('r','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','r','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('a','c','A','d','r',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,status,created_at,updated_at) VALUES ('p','c','P','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES ('team','c','p','T','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES ('alloc','c','p','team','a','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('wi','c','p','W','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,assigned_at) VALUES ('assignment','c','p','wi','a','alloc','2026-10-01T00:00:00Z')",
        "INSERT INTO model_profiles(id,workspace_id,name,created_at,updated_at) VALUES ('profile','workspace-1','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,credential_ref_id,name,provider_kind,status,created_at,updated_at) VALUES ('provider-conn-default','workspace-1','control-credential','PC','openai_compatible','ENABLED','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','workspace-1','provider-conn-default','gpt-4o-mini','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('profile','workspace-1',1,'m','2026-10-01T00:00:00Z')",
        "UPDATE model_profiles SET current_version=1 WHERE id='profile'",
        "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('c','workspace-1','profile','2026-10-01T00:00:00Z')",
        "UPDATE agents SET model_profile_id='profile' WHERE id='a'",
    ] {
        sqlx::query(sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let app = ApplicationContext::new(pool.clone());
    app.ensure_canonical_permissions("workspace-1")
        .await
        .unwrap();
    for (id, definition) in [
        ("control-provider-grant", "pd_provider.use"),
        ("control-credential-grant", "pd_credential.use"),
    ] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?,'workspace-1','c',?,'SYSTEM','system:control-test','COMPANY','c','ALLOW','2026-10-01T00:00:00Z')")
            .bind(id).bind(definition).execute(&pool).await.unwrap();
    }
    pool
}

async fn spawn_slow_provider() -> (
    String,
    tokio::sync::mpsc::Receiver<()>,
    tokio::task::JoinHandle<()>,
    Arc<AtomicUsize>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let tx = tx.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_secs(10)).await;
                axum::Json(json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "created": 1234567,
                    "model": "gpt-4o-mini",
                    "choices": [{
                        "index": 0,
                        "message": { "role": "assistant", "content": "slow response" },
                        "finish_reason": "stop"
                    }]
                }))
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (
        format!("http://127.0.0.1:{}/v1/chat/completions", addr.port()),
        rx,
        handle,
        requests,
    )
}

async fn queue_with_provider(
    pool: &SqlitePool,
    base_url: Option<&str>,
    deadline_secs: Option<u64>,
) -> Run {
    let mut run = Run::create(
        CompanyId("c".into()),
        "p".into(),
        "wi".into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "control-test".into(),
    )
    .unwrap();
    run.assignment_id = Some("assignment".into());
    create_run(pool, &run).await.unwrap();
    let mut payload = json!({
        "run_id": run.id,
        "mock_scenario": "SUCCESS_TEXT",
    });
    if let Some(url) = base_url {
        sqlx::query("UPDATE provider_connections SET endpoint=? WHERE id='provider-conn-default'")
            .bind(url)
            .execute(pool)
            .await
            .unwrap();
    }
    if let Some(secs) = deadline_secs {
        payload["deadline_secs"] = json!(secs);
    }
    let job = DurableJob {
        id: format!("job-{}", run.id),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload,
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
        scope: ScopeRef::company("c"),
        payload: json!({"run_id": run.id, "status": "QUEUED"}),
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
    run
}

async fn wait_idle(supervisor: &RuntimeSupervisor) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while supervisor.current_active_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("supervisor task finished");
}

async fn wait_terminal(pool: &SqlitePool, company_id: &CompanyId, run_id: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(r) = get_run(pool, company_id, run_id).await.unwrap()
                && matches!(
                    r.status,
                    RunStatus::Succeeded
                        | RunStatus::Failed
                        | RunStatus::Cancelled
                        | RunStatus::TimedOut
                )
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("run reached terminal status");
}

fn supervisor(pool: SqlitePool) -> RuntimeSupervisor {
    let app = ApplicationContext::new(pool.clone()).with_secret_store(Arc::new(ControlSecrets));
    RuntimeSupervisor::new(
        pool,
        SupervisorConfig {
            max_concurrency: 1,
            worker_principal_id: "system:control-test".into(),
            lease_duration_secs: 10,
            heartbeat_interval_secs: 1,
        },
    )
    .with_model_handler(core_model::handler(app))
}

#[tokio::test]
async fn running_cancel_api_transition_kills_worker_child() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    reset_worker_process_spawn_count();
    let pool = setup().await;
    let (slow_url, mut req_rx, server_handle, requests) = spawn_slow_provider().await;
    let run = queue_with_provider(&pool, Some(&slow_url), None).await;
    let supervisor = supervisor(pool.clone());
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());

    // Wait until child is spawned, running, and has invoked the model provider (in-flight)
    tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
        .await
        .expect("worker process spawned and called model provider")
        .expect("request channel open");

    let running = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.status, RunStatus::Running);

    // Exercise canonical cancel_run transition API
    let result = nalarvo_persistence::m4::RuntimeResult {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        run_status: RunStatus::Cancelled,
        result_summary: "Cancelled by test".into(),
        output_payload: None,
        output_metadata: None,
        resource_usage_summary: None,
        failure_class: Some("CANCELLED".into()),
        failure_detail: Some("test cancellation".into()),
        warnings: None,
        correlation_id: run.correlation_id.clone(),
        causation_id: Some(run.id.clone()),
    };
    let event = DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        event_type: "RunCancelled".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: running.row_version + 1,
        occurred_at: Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.causation_id.clone().unwrap_or_else(|| run.id.clone()),
        principal: PrincipalRef {
            principal_type: PrincipalType::User,
            principal_id: "u".into(),
        },
        scope: ScopeRef::company(run.company_id.0.clone()),
        payload: serde_json::json!({ "run_id": run.id, "status": "CANCELLED" }),
    };
    nalarvo_persistence::m4::cancel_run_with_result(
        &pool,
        &run.company_id,
        &run.id,
        running.row_version,
        &result,
        &event,
    )
    .await
    .unwrap();

    wait_terminal(&pool, &run.company_id, &run.id).await;
    wait_idle(&supervisor).await;

    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Cancelled
    );
    assert_eq!(worker_process_spawn_count(), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    server_handle.abort();
}

#[tokio::test]
async fn run_deadline_times_out_and_kills_worker_child() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    reset_worker_process_spawn_count();
    let pool = setup().await;
    let (slow_url, _req_rx, server_handle, requests) = spawn_slow_provider().await;
    let run = queue_with_provider(&pool, Some(&slow_url), Some(1)).await;
    let supervisor = supervisor(pool.clone());
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());

    wait_terminal(&pool, &run.company_id, &run.id).await;
    wait_idle(&supervisor).await;

    let timed_out = get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(timed_out.status, RunStatus::TimedOut);
    assert_eq!(
        timed_out.failure_class.as_deref(),
        Some("RUN_DEADLINE_EXCEEDED")
    );
    assert_eq!(worker_process_spawn_count(), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    server_handle.abort();
}

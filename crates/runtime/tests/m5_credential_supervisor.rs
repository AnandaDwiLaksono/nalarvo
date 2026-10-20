use chrono::Utc;
use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
};
use nalarvo_domain::{CompanyId, DomainEvent, PrincipalRef, Run, RunStatus, ScopeRef};
use nalarvo_persistence::{
    create_pool,
    m4::{DurableJob, create_run, get_run, queue_run},
    run_migrations,
};
use nalarvo_runtime::{
    core_model,
    supervisor::{RuntimeSupervisor, SupervisorConfig},
};
use serde_json::json;
use sqlx::SqlitePool;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const CANARY: &str = "supervisor-secret-canary-8dc70b";

#[derive(Clone)]
struct CountingStore(Arc<AtomicUsize>);
impl SecretStore for CountingStore {
    fn get(&self, reference: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        assert_eq!(reference.id, "cred");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(SecretValue::new(CANARY))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}

async fn fixture(pool: &SqlitePool, endpoint: &str, authorized: bool) -> Run {
    let now = "2026-10-01T00:00:00Z";
    for query in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@local','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,status,created_at,updated_at) VALUES ('cred','w','Key','secretstore://cred','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('role','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','role','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('agent','c','A','d','role',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES ('project','c','P','MEDIUM','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES ('team','c','project','T','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES ('alloc','c','project','team','agent','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('item','c','project','W','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,assigned_at) VALUES ('assignment','c','project','item','agent','alloc','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(query).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO provider_connections(id,workspace_id,credential_ref_id,name,provider_kind,endpoint,status,created_at,updated_at) VALUES ('provider','w','cred','P','openai_compatible',?,'ENABLED',?,?)")
        .bind(endpoint).bind(now).bind(now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('model','w','provider','synthetic-model',?,?)")
        .bind(now).bind(now).execute(pool).await.unwrap();
    for query in [
        "INSERT INTO model_profiles(id,workspace_id,name,created_at,updated_at) VALUES ('profile','w','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('profile','w',1,'model','2026-10-01T00:00:00Z')",
        "UPDATE model_profiles SET current_version=1 WHERE id='profile'",
        "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('c','w','profile','2026-10-01T00:00:00Z')",
        "UPDATE agents SET model_profile_id='profile' WHERE id='agent'",
    ] {
        sqlx::query(query).execute(pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone());
    app.ensure_canonical_permissions("w").await.unwrap();
    if authorized {
        for (id, definition) in [
            ("provider_grant", "pd_provider.use"),
            ("credential_grant", "pd_credential.use"),
        ] {
            sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?,'w','c',?,'SYSTEM','worker','COMPANY','c','ALLOW',?)")
                .bind(id).bind(definition).bind(now).execute(pool).await.unwrap();
        }
    }
    let mut run = Run::create(
        CompanyId("c".into()),
        "project".into(),
        "item".into(),
        "agent".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "corr".into(),
    )
    .unwrap();
    run.assignment_id = Some("assignment".into());
    create_run(pool, &run).await.unwrap();
    let job = DurableJob {
        id: "job".into(),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({}), // canonical application queue: no worker model/provider hints.
        available_at: Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 1,
        correlation_id: run.correlation_id.clone(),
    };
    let event = DomainEvent {
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
        scope: ScopeRef::company("c"),
        payload: json!({"run_id":run.id}),
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

#[tokio::test]
async fn child_supervisor_core_model_broker_local_provider() {
    assert!(std::path::Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")).is_file());
    for authorized in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("runtime.db");
        let pool = create_pool(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0u8; 4096];
                let size = stream.read(&mut chunk).await.unwrap();
                assert!(size > 0 && bytes.len() + size <= 16384);
                bytes.extend_from_slice(&chunk[..size]);
            }
            let request = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
            assert!(request.contains(&format!("authorization: bearer {CANARY}")));
            assert!(request.starts_with("post /v1/chat/completions "));
            count.fetch_add(1, Ordering::SeqCst);
            let body = r#"{"choices":[{"message":{"content":"local-ok"}}]}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let run = fixture(&pool, &endpoint, authorized).await;
        let gets = Arc::new(AtomicUsize::new(0));
        let app = ApplicationContext::new(pool.clone())
            .with_secret_store(Arc::new(CountingStore(gets.clone())));
        let supervisor = RuntimeSupervisor::new(
            pool.clone(),
            SupervisorConfig {
                max_concurrency: 1,
                worker_principal_id: "worker".into(),
                lease_duration_secs: 10,
                heartbeat_interval_secs: 2,
            },
        )
        .with_model_handler(core_model::handler(app));
        assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if supervisor.current_active_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await
        .unwrap();
        supervisor.shutdown().await;
        if authorized {
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
        } else {
            server.abort();
        }
        assert_eq!(
            (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
            if authorized { (1, 1) } else { (0, 0) }
        );
        let result = get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.status,
            if authorized {
                RunStatus::Succeeded
            } else {
                RunStatus::Failed
            }
        );
        let used: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_records WHERE action='CredentialUsed' AND scope_id='c' AND details LIKE '%model.invoke%'").fetch_one(&pool).await.unwrap();
        assert_eq!(used, i64::from(authorized));
        if authorized {
            let output: String = sqlx::query_scalar(
                "SELECT json_extract(output_payload,'$.text') FROM runtime_results WHERE run_id=?",
            )
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(output, "local-ok");
        }
        drop(pool);
        // The canary belongs only to SecretStore and the local HTTP request, never SQLite.
        for name in ["runtime.db", "runtime.db-wal", "runtime.db-shm"] {
            let path = root.path().join(name);
            if path.exists() {
                assert!(
                    !std::fs::read(path)
                        .unwrap()
                        .windows(CANARY.len())
                        .any(|b| b == CANARY.as_bytes()),
                    "{name} contains credential"
                );
            }
        }
    }
}

use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
};
use nalarvo_domain::{CompanyId, PrincipalRef, WorkspaceId};
use nalarvo_model_gateway::{ModelMessage, ModelRequest};
use nalarvo_runtime::{core_dispatch::TrustedRunContext, core_model};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, watch},
};
#[derive(Default)]
struct GateStore {
    entered: Notify,
    released: Mutex<bool>,
    gate: Condvar,
}
struct ResolutionGate {
    entered: Notify,
    released: Mutex<bool>,
    gate: Condvar,
}
impl nalarvo_domain::AuditSink for ResolutionGate {
    fn record(&self, record: nalarvo_domain::AuditRecord) {
        if record.action != "CredentialUsed" {
            return;
        }
        self.entered.notify_one();
        let mut open = self.released.lock().unwrap();
        while !*open {
            open = self.gate.wait(open).unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r1_disable_commits_after_secret_resolved_before_transport_and_cancellation_fences() {
    for cancelled in [false, true] {
        let (endpoint, calls, _, _, server) = provider(false).await;
        let store = Arc::new(GateStore::default());
        store.open();
        let (db, original, ctx) = fixture(&endpoint, store.clone()).await;
        let gate = Arc::new(ResolutionGate {
            entered: Notify::new(),
            released: Mutex::new(false),
            gate: Condvar::new(),
        });
        let app = ApplicationContext::with_audit_sink(original.pool.clone(), gate.clone())
            .with_secret_store(store)
            .with_content_store(Arc::new(
                nalarvo_application::content_store::LocalContentStore::open(
                    &db.path().join("content"),
                )
                .unwrap(),
            ));
        let handler = core_model::handler(app.clone());
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move { handler(ctx, request(), rx).await });
        barrier(&gate.entered).await;
        if cancelled {
            tx.send(true).unwrap();
        } else {
            app.disable_provider(&WorkspaceId("w".into()), "provider", 1)
                .await
                .unwrap();
        }
        *gate.released.lock().unwrap() = true;
        gate.gate.notify_all();
        assert!(task.await.unwrap().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            scalar(
                &app,
                "SELECT COUNT(*) FROM resource_reservations WHERE status='HELD'"
            )
            .await,
            0
        );
        assert_eq!(scalar(&app, "SELECT COUNT(*) FROM usage_records").await, 0);
        server.abort();
        app.pool.close().await;
    }
}
impl SecretStore for GateStore {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        self.entered.notify_one();
        let mut open = self.released.lock().unwrap();
        while !*open {
            open = self.gate.wait(open).unwrap();
        }
        Ok(SecretValue::new("synthetic-residual-token"))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}
impl GateStore {
    fn open(&self) {
        *self.released.lock().unwrap() = true;
        self.gate.notify_all();
    }
}
async fn fixture(
    endpoint: &str,
    store: Arc<GateStore>,
) -> (tempfile::TempDir, ApplicationContext, TrustedRunContext) {
    let db = tempfile::tempdir().unwrap();
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        db.path().join("core.db").display()
    ))
    .await
    .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    let now = "2026-10-01T00:00:00Z";
    for query in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@local','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,is_personal,created_at,updated_at) VALUES ('w','u','W','w',0,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('other','w','O','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,status,created_at,updated_at) VALUES ('cred','w','Key','secretstore://cred','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('role','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','role','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,model_profile_id,created_at,updated_at) VALUES ('agent','c','A','d','role',1,NULL,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES ('project','c','P','MEDIUM','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES ('team','c','project','T','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES ('alloc','c','project','team','agent','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES ('item','c','project','W','IN_PROGRESS','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,assigned_at) VALUES ('assignment','c','project','item','agent','alloc','2026-10-01T00:00:00Z')",
        "INSERT INTO runs(id,company_id,project_id,work_item_id,assignment_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at) VALUES ('run','c','project','item','assignment','agent','RUNNING','MANUAL',1,'SYSTEM','worker','2026-10-01T00:00:00Z','corr','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO run_execution_leases(id,run_id,worker_principal_id,lease_version,acquired_at,heartbeat_at,expires_at,created_at) VALUES ('lease','run','worker',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z','2099-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    // Provider is inserted separately with the ephemeral endpoint.
    sqlx::query("INSERT INTO provider_connections(id,workspace_id,credential_ref_id,name,provider_kind,endpoint,status,created_at,updated_at) VALUES ('provider','w','cred','P','openai_compatible',?,'ENABLED',?,?)")
        .bind(endpoint).bind(now).bind(now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('model','w','provider','synthetic-model',?,?)")
        .bind(now).bind(now).execute(&pool).await.unwrap();
    for query in [
        "INSERT INTO model_profiles(id,workspace_id,name,created_at,updated_at) VALUES ('profile','w','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('profile','w',1,'model','2026-10-01T00:00:00Z')",
        "UPDATE model_profiles SET current_version=1 WHERE id='profile'",
        "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('c','w','profile','2026-10-01T00:00:00Z')",
        "UPDATE agents SET model_profile_id='profile' WHERE id='agent'",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone())
        .with_secret_store(store)
        .with_content_store(Arc::new(
            nalarvo_application::content_store::LocalContentStore::open(&db.path().join("content"))
                .unwrap(),
        ));
    app.ensure_canonical_permissions("w").await.unwrap();
    for q in [
        "INSERT INTO workspace_memberships(workspace_id,user_id,role,created_at) VALUES('w','u','OWNER','t')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('pg','w','c','pd_provider.use','SYSTEM','worker','COMPANY','c','ALLOW','t'),('cg','w','c','pd_credential.use','SYSTEM','worker','COMPANY','c','ALLOW','t'),('ug','w','c','pd_project.modify','USER','u','COMPANY','c','ALLOW','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,created_at,updated_at) VALUES('budget','w','c','B',10,'t','t')",
        "INSERT INTO work_requirements VALUES('c','project','item','role',0,0,'authorized')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let ctx = TrustedRunContext {
        run_id: "run".into(),
        company_id: "c".into(),
        project_id: "project".into(),
        work_item_id: "item".into(),
        agent_id: "agent".into(),
        lease_id: "lease".into(),
        lease_version: 1,
        worker_principal_id: "worker".into(),
        live_lease_version: None,
    };
    (db, app, ctx)
}
fn request() -> ModelRequest {
    ModelRequest {
        model: "synthetic-model".into(),
        messages: vec![ModelMessage::user("test")],
        temperature: None,
        max_tokens: None,
        stream: false,
    }
}

async fn barrier(signal: &Notify) {
    tokio::time::timeout(std::time::Duration::from_secs(10), signal.notified())
        .await
        .unwrap();
}
async fn provider(
    hang: bool,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<Notify>,
    Arc<Notify>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let signal = Arc::new(Notify::new());
    let count = calls.clone();
    let received = signal.clone();
    let resume = Arc::new(Notify::new());
    let proceed = resume.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 32768];
            assert!(stream.read(&mut buf).await.unwrap() > 0);
            count.fetch_add(1, Ordering::SeqCst);
            received.notify_one();
            if hang {
                proceed.notified().await;
            }
            let body = r#"{"choices":[{"message":{"content":"authorized"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        }
    });
    (endpoint, calls, signal, resume, server)
}
fn completion(key: &str) -> nalarvo_application::m7::StaffTeamCommand {
    nalarvo_application::m7::StaffTeamCommand {
        company_id: CompanyId("c".into()),
        project_id: "project".into(),
        team_id: "team".into(),
        meta: nalarvo_application::CommandMeta {
            principal: Some(PrincipalRef::user("u")),
            idempotency_key: Some(key.into()),
            ..Default::default()
        },
    }
}
async fn scalar(app: &ApplicationContext, q: &str) -> i64 {
    sqlx::query_scalar(q).fetch_one(&app.pool).await.unwrap()
}
async fn recover(app: &ApplicationContext) {
    sqlx::query(
        "UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00Z' WHERE id='lease'",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let supervisor = nalarvo_runtime::supervisor::RuntimeSupervisor::new(
        app.pool.clone(),
        nalarvo_runtime::supervisor::SupervisorConfig::default(),
    );
    assert_eq!(supervisor.reconcile_orphaned_runs().await.unwrap(), 1);
    assert_eq!(supervisor.reconcile_orphaned_runs().await.unwrap(), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r1_revocation_during_credential_resolution_fences_dispatch() {
    for mode in [
        "disable",
        "disable_reenable",
        "credential_revoke",
        "permission_revoke",
        "resource_revoke",
        "workspace_suspend",
        "agent_pause",
        "run_cancel",
    ] {
        let (endpoint, calls, _, _, server) = provider(false).await;
        let store = Arc::new(GateStore::default());
        let (_db, app, ctx) = fixture(&endpoint, store.clone()).await;
        let handler = core_model::handler(app.clone());
        let task =
            tokio::spawn(async move { handler(ctx, request(), watch::channel(false).1).await });
        barrier(&store.entered).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        match mode {
            "disable" | "disable_reenable" => {
                assert_eq!(
                    app.disable_provider(&WorkspaceId("w".into()), "provider", 1)
                        .await
                        .unwrap()
                        .status,
                    "DISABLED"
                );
                if mode == "disable_reenable" {
                    app.enable_provider(&WorkspaceId("w".into()), "provider", 2)
                        .await
                        .unwrap();
                }
            }
            "credential_revoke" => app
                .revoke_credential(&WorkspaceId("w".into()), "cred", 1)
                .await
                .unwrap(),
            "resource_revoke" => {
                sqlx::query("UPDATE agents SET model_profile_id=NULL WHERE id='agent'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM company_workspace_resource_grants WHERE company_id='c'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "workspace_suspend" => {
                sqlx::query("UPDATE workspaces SET status='SUSPENDED' WHERE id='w'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "agent_pause" => {
                sqlx::query("UPDATE agents SET status='PAUSED' WHERE id='agent'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "run_cancel" => {
                sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED' WHERE id='run'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            _ => {
                sqlx::query(
                    "UPDATE permission_grants SET revoked_at='2026-10-01T00:00:00Z' WHERE id='pg'",
                )
                .execute(&app.pool)
                .await
                .unwrap();
            }
        }
        store.open();
        let outcome = task.await.unwrap();
        assert!(outcome.is_err(), "stale authority dispatched: {mode}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "outbound request after revocation: {mode}"
        );
        assert_eq!(scalar(&app, "SELECT COUNT(*) FROM usage_records").await, 0);
        assert_eq!(
            scalar(
                &app,
                "SELECT COUNT(*) FROM resource_reservations WHERE status='HELD'"
            )
            .await,
            0
        );
        server.abort();
        app.pool.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2_interrupted_dispatch_stays_discoverable_after_restart_and_blocks_completion() {
    let (endpoint, calls, received, _, server) = provider(true).await;
    let store = Arc::new(GateStore::default());
    store.open();
    let (db, app, ctx) = fixture(&endpoint, store).await;
    let handler = core_model::handler(app.clone());
    let task = tokio::spawn(async move { handler(ctx, request(), watch::channel(false).1).await });
    barrier(&received).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM resource_reservations WHERE status='HELD'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM model_invocations").await,
        0
    );
    app.pool.close().await;
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        db.path().join("core.db").display()
    ))
    .await
    .unwrap();
    let app = ApplicationContext::new(pool);
    recover(&app).await;
    assert!(
        app.complete_project_governed(completion("unknown"), 1)
            .await
            .is_err(),
        "unresolved dispatch completed Project"
    );
    assert_eq!(scalar(&app,"SELECT COUNT(*) FROM model_dispatches WHERE run_id='run' AND project_id='project' AND status='OUTCOME_UNKNOWN'").await,1);
    assert_eq!(scalar(&app,"SELECT COUNT(*) FROM resource_reservations r JOIN model_dispatches d ON d.id=r.model_dispatch_id WHERE d.run_id='run' AND r.status='HELD'").await,1);
    let dispatch: String = sqlx::query_scalar("SELECT id FROM model_dispatches")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert!(
        nalarvo_persistence::m5::release_model_reservation(&app.pool, &dispatch)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO runs(id,company_id,project_id,work_item_id,assignment_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,retry_of_run_id,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at) SELECT 'retry',company_id,project_id,work_item_id,assignment_id,executing_agent_id,'QUEUED','RETRY',2,id,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at FROM runs WHERE id='run'").execute(&app.pool).await.is_err());
    for q in [
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('unrelated','c','Unrelated','MEDIUM','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES('other-team','c','unrelated','T','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(q).execute(&app.pool).await.unwrap();
    }
    let mut other = completion("other-project");
    other.project_id = "unrelated".into();
    other.team_id = "other-team".into();
    app.complete_project_governed(other, 1).await.unwrap();
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='budget'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (1.0, 0.0)
    );
    assert_eq!(scalar(&app, "SELECT COUNT(*) FROM usage_records").await, 0);
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM runtime_results WHERE run_id='run'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM model_invocations").await,
        0
    );
    server.abort();
    app.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r1_dispatch_wins_disable_is_nonblocking_and_new_epoch_requires_new_call() {
    let (endpoint, calls, received, resume, server) = provider(true).await;
    let store = Arc::new(GateStore::default());
    store.open();
    let (_db, app, ctx) = fixture(&endpoint, store).await;
    let handler = core_model::handler(app.clone());
    let next_ctx = ctx.clone();
    let task = tokio::spawn(async move { handler(ctx, request(), watch::channel(false).1).await });
    barrier(&received).await;
    // Provider response is held. Disable must commit without waiting for HTTP.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app.disable_provider(&WorkspaceId("w".into()), "provider", 1),
    )
    .await
    .unwrap()
    .unwrap();
    resume.notify_one();
    assert_eq!(
        task.await.unwrap().unwrap().text.as_deref(),
        Some("authorized")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let handler = core_model::handler(app.clone());
    assert!(
        handler(next_ctx.clone(), request(), watch::channel(false).1)
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    app.enable_provider(&WorkspaceId("w".into()), "provider", 2)
        .await
        .unwrap();
    let task =
        tokio::spawn(async move { handler(next_ctx, request(), watch::channel(false).1).await });
    barrier(&received).await;
    resume.notify_one();
    assert!(task.await.unwrap().is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM model_dispatches WHERE status='RESOLVED'"
        )
        .await,
        2
    );
    assert_eq!(scalar(&app, "SELECT COUNT(*) FROM usage_records").await, 2);
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='budget'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (0.0, 2.0)
    );
    assert!(
        sqlx::query("UPDATE model_invocations SET status='FAILED'")
            .execute(&app.pool)
            .await
            .is_err()
    );
    let serialized: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM domain_events UNION ALL SELECT details FROM audit_records",
    )
    .fetch_all(&app.pool)
    .await
    .unwrap();
    assert!(
        serialized
            .iter()
            .all(|s| !s.contains("synthetic-residual-token") && !s.contains("secretstore://"))
    );
    server.abort();
    app.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2_held_before_invocation_recovery_releases_only_proven_unsent_and_allows_completion() {
    let (endpoint, calls, _, _, server) = provider(false).await;
    let store = Arc::new(GateStore::default());
    let (db, app, ctx) = fixture(&endpoint, store.clone()).await;
    let handler = core_model::handler(app.clone());
    let task = tokio::spawn(async move { handler(ctx, request(), watch::channel(false).1).await });
    barrier(&store.entered).await;
    assert_eq!(scalar(&app,"SELECT COUNT(*) FROM resource_reservations r JOIN model_dispatches d ON d.id=r.model_dispatch_id WHERE r.status='HELD' AND d.status='PREPARED' AND d.project_id='project' AND d.run_id='run'").await,1);
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM model_invocations").await,
        0
    );
    // The live Run alone must not mask the orphan guard.
    sqlx::query("UPDATE runs SET lifecycle_state='FAILED' WHERE id='run'")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(
        app.complete_project_governed(completion("prepared"), 1)
            .await
            .is_err()
    );
    sqlx::query("UPDATE runs SET lifecycle_state='RUNNING' WHERE id='run'")
        .execute(&app.pool)
        .await
        .unwrap();
    task.abort();
    store.open();
    assert!(task.await.unwrap_err().is_cancelled());
    app.pool.close().await;
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        db.path().join("core.db").display()
    ))
    .await
    .unwrap();
    let app = ApplicationContext::new(pool);
    recover(&app).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM model_dispatches WHERE status='ABORTED'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM resource_reservations WHERE status='RELEASED'"
        )
        .await,
        1
    );
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='budget'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (0.0, 0.0)
    );
    assert_eq!(scalar(&app, "SELECT COUNT(*) FROM usage_records").await, 0);
    app.complete_project_governed(completion("reconciled"), 1)
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &app,
            "SELECT COUNT(*) FROM projects WHERE id='project' AND status='COMPLETED'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM model_invocations").await,
        0
    );
    assert!(
        sqlx::query("UPDATE runtime_results SET run_status='SUCCEEDED'")
            .execute(&app.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE model_dispatches SET status='DISPATCHING'")
            .execute(&app.pool)
            .await
            .is_err()
    );
    server.abort();
    app.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r2_completed_project_cannot_create_obligations() {
    let (endpoint, calls, _, _, server) = provider(false).await;
    let store = Arc::new(GateStore::default());
    store.open();
    let (_db, app, ctx) = fixture(&endpoint, store).await;
    sqlx::query("UPDATE runs SET lifecycle_state='FAILED' WHERE id='run'")
        .execute(&app.pool)
        .await
        .unwrap();
    app.complete_project_governed(completion("done"), 1)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET lifecycle_state='RUNNING' WHERE id='run'")
        .execute(&app.pool)
        .await
        .unwrap();
    let handler = core_model::handler(app.clone());
    assert!(
        handler(ctx, request(), watch::channel(false).1)
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM resource_reservations").await,
        0
    );
    assert_eq!(
        scalar(&app, "SELECT COUNT(*) FROM model_dispatches").await,
        0
    );
    server.abort();
    app.pool.close().await;
}

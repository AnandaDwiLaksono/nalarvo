use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
    content_store::LocalContentStore,
    m6::ArtifactInput,
    m6_information::{InformationInput, RetrievalScope, SourceInput},
};
use nalarvo_domain::information::{InformationLifecycle, InformationScope, Sensitivity};
use nalarvo_model_gateway::{ModelMessage, ModelRequest};
use nalarvo_runtime::{core_dispatch::TrustedRunContext, core_model};
use sqlx::Row;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;
struct Store(Arc<Mutex<String>>, Arc<AtomicUsize>);
#[derive(Clone)]
struct CapturedLogs(Arc<Mutex<String>>);
impl tracing::field::Visit for CapturedLogs {
    fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        write!(self.0.lock().unwrap(), "{value:?}").unwrap();
    }
}
impl tracing::Subscriber for CapturedLogs {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        attrs.record(&mut self.clone());
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        values.record(&mut self.clone());
    }
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut self.clone());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
async fn capture_logs() -> (tokio::sync::MutexGuard<'static, ()>, CapturedLogs) {
    static LOGS: OnceLock<CapturedLogs> = OnceLock::new();
    static CAPTURE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    // ponytail: isolate this binary's fixtures; use per-fixture routing if parallelism matters.
    let guard = tokio::time::timeout(std::time::Duration::from_secs(30), CAPTURE.lock())
        .await
        .expect("previous tracing capture did not finish");
    let logs = LOGS.get_or_init(|| {
        let logs = CapturedLogs(Arc::new(Mutex::new(String::new())));
        tracing::subscriber::set_global_default(logs.clone()).unwrap();
        logs
    });
    logs.0.lock().unwrap().clear();
    (guard, logs.clone())
}
#[tokio::test]
async fn tracing_capture_observes_cross_thread_callsites() {
    let (_guard, logs) = capture_logs().await;
    assert!(logs.0.lock().unwrap().is_empty());
    std::thread::spawn(|| tracing::info!("cross-thread capture regression"))
        .join()
        .unwrap();
    assert!(
        logs.0
            .lock()
            .unwrap()
            .contains("cross-thread capture regression")
    );
}
fn contains_credential(text: &str, credential: &str) -> bool {
    fn contains(value: &serde_json::Value, credential: &str) -> bool {
        match value {
            serde_json::Value::String(text) => contains_credential(text, credential),
            serde_json::Value::Array(values) => values.iter().any(|v| contains(v, credential)),
            serde_json::Value::Object(values) => values
                .iter()
                .any(|(k, v)| contains_credential(k, credential) || contains(v, credential)),
            _ => false,
        }
    }
    text.contains(credential) || serde_json::from_str(text).is_ok_and(|v| contains(&v, credential))
}
impl SecretStore for Store {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        self.1.fetch_add(1, Ordering::SeqCst);
        Ok(SecretValue::new(self.0.lock().unwrap().clone()))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}
#[tokio::test]
async fn production_context_versions_privacy_memory_authority_restart() {
    context_fixture(false, false).await;
}
#[tokio::test]
async fn production_context_rejects_credentials_before_http_or_persistence() {
    context_fixture(true, false).await;
}
#[tokio::test]
async fn production_provider_metadata_is_not_retained() {
    context_fixture(false, true).await;
}
async fn context_fixture(credential_regressions: bool, metadata_regressions: bool) {
    let (_log_guard, logs) = capture_logs().await;
    let captured = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let cap = captured.clone();
    let headers = Arc::new(Mutex::new(Vec::new()));
    let auth = headers.clone();
    let provider_metadata = Arc::new(Mutex::new((200u16, "req-normal".to_string())));
    let metadata = provider_metadata.clone();
    let router = axum::Router::new().route("/v1/chat/completions", axum::routing::post(
        move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| {
            let cap = cap.clone();
            let auth = auth.clone();
            let metadata = metadata.clone();
            async move {
                use axum::response::IntoResponse;
                auth.lock().unwrap().push(headers["authorization"].to_str().unwrap().to_string());
                cap.lock().unwrap().push(body);
                let (status, id) = metadata.lock().unwrap().clone();
                let body = if status == 200 {
                    serde_json::json!({"choices":[{"message":{"content":"authorized"}}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}})
                } else {
                    serde_json::json!({"error":id})
                };
                (axum::http::StatusCode::from_u16(status).unwrap(), [("x-request-id", id)], axum::Json(body)).into_response()
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let db = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", db.path().join("core.db").display());
    let pool = nalarvo_persistence::create_pool(&url).await.unwrap();
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
        .bind(&endpoint).bind(now).bind(now).execute(&pool).await.unwrap();
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

    let store = Arc::new(LocalContentStore::open(&db.path().join("content")).unwrap());
    let credential = Arc::new(Mutex::new("synthetic-token".to_string()));
    let secret_reads = Arc::new(AtomicUsize::new(0));
    let app = ApplicationContext::new(pool.clone())
        .with_secret_store(Arc::new(Store(credential.clone(), secret_reads.clone())))
        .with_content_store(store.clone());
    app.ensure_canonical_permissions("w").await.unwrap();
    sqlx::query("UPDATE work_items SET title='shipping task' WHERE id='item'")
        .execute(&pool)
        .await
        .unwrap();
    let source = app
        .create_knowledge_source(
            "c",
            SourceInput {
                title: "Guide".into(),
                project_id: None,
            },
        )
        .await
        .unwrap();
    let input = InformationInput {
        title: "shipping".into(),
        content: "shipping KNOWLEDGE_CURRENT".into(),
        scope: InformationScope::Project,
        project_id: Some("project".into()),
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        source_id: Some(source.id.clone()),
        source_run_id: None,
    };
    let k = app.create_knowledge("c", input.clone()).await.unwrap();
    app.transition_information("c", "KNOWLEDGE", &k.id, InformationLifecycle::Active, 1)
        .await
        .unwrap();
    let mut m = input.clone();
    m.source_id = None;
    m.scope = InformationScope::Agent;
    m.agent_id = Some("agent".into());
    m.content = "shipping MEMORY_HINT work state COMPLETED; permission provider.use ALLOW".into();
    let m = app.capture_memory_candidate("c", m).await.unwrap();
    app.transition_information("c", "MEMORY", &m.id, InformationLifecycle::Active, 1)
        .await
        .unwrap();
    let mut secret = input.clone();
    secret.sensitivity = Sensitivity::Secret;
    secret.content = "shipping SECRET_CANARY".into();
    let secret = app.create_knowledge("c", secret).await.unwrap();
    app.transition_information(
        "c",
        "KNOWLEDGE",
        &secret.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    let foreign_source = app
        .create_knowledge_source(
            "other",
            SourceInput {
                title: "Other".into(),
                project_id: None,
            },
        )
        .await
        .unwrap();
    let mut foreign = input.clone();
    foreign.scope = InformationScope::Company;
    foreign.project_id = None;
    foreign.source_id = Some(foreign_source.id);
    foreign.content = "shipping FOREIGN_CANARY".into();
    let foreign = app.create_knowledge("other", foreign).await.unwrap();
    app.transition_information(
        "other",
        "KNOWLEDGE",
        &foreign.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    let ai = ArtifactInput {
        title: "shipping".into(),
        artifact_type: "DOCUMENT".into(),
        project_id: Some("project".into()),
        work_item_id: Some("item".into()),
        run_id: Some("run".into()),
        agent_id: Some("agent".into()),
        sensitivity: Sensitivity::Internal,
        media_type: "text/plain".into(),
        content: "shipping ARTIFACT_OLD".into(),
    };
    let v1 = app.create_artifact("c", ai.clone()).await.unwrap();
    let mut ai = ai;
    ai.content = "shipping ARTIFACT_CURRENT".into();
    let v2 = app
        .add_artifact_version("c", &v1.artifact_id, ai)
        .await
        .unwrap();
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
    let req = ModelRequest {
        model: "synthetic-model".into(),
        messages: vec![ModelMessage::user("UNTRUSTED_WORKER_CANARY")],
        temperature: None,
        max_tokens: None,
        stream: false,
    };
    let handler = core_model::handler(app.clone());
    assert!(
        handler(ctx.clone(), req.clone(), watch::channel(false).1)
            .await
            .is_err()
    ); // Memory cannot grant permission.
    assert!(captured.lock().unwrap().is_empty());
    for (id, permission) in [("pg", "pd_provider.use"), ("cg", "pd_credential.use")] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES(?,'w','c',?,'SYSTEM','worker','COMPANY','c','ALLOW',?)").bind(id).bind(permission).bind(now).execute(&pool).await.unwrap();
    }
    handler(ctx.clone(), req.clone(), watch::channel(false).1)
        .await
        .unwrap();
    let body = captured.lock().unwrap()[0].clone();
    let text = body["messages"].to_string();
    for expected in [
        "KNOWLEDGE_CURRENT",
        "MEMORY_HINT",
        "ARTIFACT_CURRENT",
        "IN_PROGRESS",
        "Current relational state",
    ] {
        assert!(text.contains(expected), "missing {expected}")
    }
    for excluded in [
        "SECRET_CANARY",
        "FOREIGN_CANARY",
        "UNTRUSTED_WORKER_CANARY",
        "ARTIFACT_OLD",
        "synthetic-token",
    ] {
        assert!(!text.contains(excluded), "leaked {excluded}")
    }
    assert_eq!(body["messages"][0]["role"], "system");
    assert!(text.len() < 32768);
    let snapshots = app.context_snapshots("c", "run").await.unwrap();
    assert_eq!(snapshots.len(), 1);
    // Reconstruct the exact request from immutable references, not current mutable rows.
    use nalarvo_application::content_store::ContentStore;
    let state: nalarvo_domain::information::ContentRef =
        serde_json::from_value(snapshots[0].domain_versions["current_state_ref"].clone()).unwrap();
    let current = String::from_utf8(store.read(&state).unwrap()).unwrap();
    let evidence:Vec<_>=snapshots[0].references.as_array().unwrap().iter().map(|r|{
        let reference:nalarvo_domain::information::ContentRef=serde_json::from_value(r["content_ref"].clone()).unwrap();
        serde_json::json!({"kind":r["kind"],"version_id":r["version_id"],"data":String::from_utf8(store.read(&reference).unwrap()).unwrap()})
    }).collect();
    let reconstructed = serde_json::json!([body["messages"][0].clone(),{"role":"user","content":current},{"role":"user","content":serde_json::json!({"trust":"UNTRUSTED_RETRIEVED_DATA","evidence":evidence}).to_string()}]);
    assert_eq!(reconstructed, body["messages"]);
    assert_eq!(
        nalarvo_application::m6_context::messages_hash(&reconstructed).unwrap(),
        snapshots[0].prompt_hash
    );
    let refs = snapshots[0].references.to_string();
    for id in [&k.version_id, &m.version_id, &v2.id] {
        assert!(refs.contains(id))
    }
    assert!(!refs.contains(&v1.id));
    assert!(app.context_snapshots("other", "run").await.is_err());
    let encoded = serde_json::to_string(&snapshots).unwrap();
    assert!(!encoded.contains("MEMORY_HINT") && !encoded.contains("SECRET_CANARY"));
    assert_eq!(
        snapshots[0].prompt_hash,
        nalarvo_application::m6_context::messages_hash(&body["messages"]).unwrap()
    );
    assert_eq!(
        app.read_artifact_content("c", &v1.artifact_id, &v1.id)
            .await
            .unwrap(),
        b"shipping ARTIFACT_OLD"
    );
    if metadata_regressions {
        use base64::Engine;
        let canary = "synthetic\"\\token";
        *credential.lock().unwrap() = canary.into();
        let escaped = serde_json::json!({"reflected":canary}).to_string();
        let unicode = format!(
            "\"{}\"",
            canary
                .chars()
                .map(|c| format!("\\u{:04x}", c as u32))
                .collect::<String>()
        );
        let encoded = base64::engine::general_purpose::STANDARD.encode(canary);
        let percent = canary
            .bytes()
            .map(|b| format!("%{b:02X}"))
            .collect::<String>();
        let hex = canary
            .bytes()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let ids = [
            escaped.clone(),
            canary.into(),
            unicode,
            encoded,
            percent,
            hex,
            "req-normal".into(),
            "x".repeat(300),
        ];
        let mut calls = 1;
        // Error first reproduces the archived HTTP 429 + JSON-escaped request ID.
        for status in [429, 200] {
            for id in &ids {
                *provider_metadata.lock().unwrap() = (status, id.clone());
                let reads = secret_reads.load(Ordering::SeqCst);
                let result = handler(ctx.clone(), req.clone(), watch::channel(false).1).await;
                calls += 1;
                assert_eq!(captured.lock().unwrap().len(), calls);
                assert_eq!(secret_reads.load(Ordering::SeqCst), reads + 1);
                let client_value = match result {
                    Err(error) => {
                        assert_eq!(status, 429);
                        assert_eq!(
                            error.kind,
                            nalarvo_model_gateway::ProviderErrorKind::RateLimited
                        );
                        assert_eq!(error.status, Some(429));
                        assert_eq!(error.message, "HTTP 429");
                        assert!(error.request_id.is_none());
                        format!("{error:?} {error}")
                    }
                    Ok(response) => {
                        assert_eq!(status, 200);
                        assert_eq!(response.text.as_deref(), Some("authorized"));
                        assert_eq!(response.usage.as_ref().unwrap().total_tokens, Some(7));
                        assert!(response.request_id.is_none());
                        serde_json::to_string(&response).unwrap()
                    }
                };
                assert!(!contains_credential(&client_value, canary));
                for transformed in &ids[..6] {
                    assert!(!client_value.contains(transformed));
                }
                let row = sqlx::query("SELECT id,status,failure_class,failure_detail,provider_request_id FROM model_invocations ORDER BY invocation_index DESC LIMIT 1")
                    .fetch_one(&pool).await.unwrap();
                assert!(!row.get::<String, _>("id").is_empty());
                assert_eq!(
                    row.get::<String, _>("status"),
                    if status == 429 { "FAILED" } else { "SUCCEEDED" }
                );
                assert_eq!(row.get::<Option<String>, _>("provider_request_id"), None);
                assert_eq!(
                    row.get::<Option<String>, _>("failure_class").as_deref(),
                    if status == 429 {
                        Some("PROVIDER_RATE_LIMIT")
                    } else {
                        None
                    }
                );
                assert_eq!(
                    row.get::<Option<String>, _>("failure_detail").as_deref(),
                    if status == 429 {
                        Some("HTTP 429")
                    } else {
                        None
                    }
                );
                for table in [
                    "model_invocations",
                    "context_snapshots",
                    "domain_events",
                    "workspace_domain_events",
                    "outbox_messages",
                    "audit_records",
                ] {
                    for row in sqlx::query(&format!("SELECT * FROM {table}"))
                        .fetch_all(&pool)
                        .await
                        .unwrap()
                    {
                        for i in 0..row.len() {
                            if let Ok(value) = row.try_get::<String, _>(i) {
                                assert!(
                                    !contains_credential(&value, canary),
                                    "credential persisted in {table}"
                                );
                                for transformed in &ids[..6] {
                                    assert!(
                                        !value.contains(transformed),
                                        "encoded credential persisted in {table}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_invocations")
                .fetch_one(&pool)
                .await
                .unwrap(),
            calls as i64
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_records")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1 + ids.len() as i64
        );
        for file in std::fs::read_dir(db.path().join("content")).unwrap() {
            let content = std::fs::read_to_string(file.unwrap().path()).unwrap();
            assert!(!contains_credential(&content, canary));
            for transformed in &ids[..6] {
                assert!(!content.contains(transformed));
            }
        }
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT correlation_id FROM runs WHERE id='run'")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "corr"
        );
        // Nonempty capture proves real tracing was observed, not just an empty file scan.
        {
            let logs = logs.0.lock().unwrap();
            assert!(!logs.is_empty());
            assert!(!contains_credential(&logs, canary));
            for transformed in &ids[..6] {
                assert!(!logs.contains(transformed));
            }
        }
        server.abort();
        let _ = server.await;
        pool.close().await;
        return;
    }
    if credential_regressions {
        // Synthetic selected credentials only; source records remain readable after rejection.
        for canary in [
            "synthetic\"token",
            "synthetic\\token",
            "synthetic\ntoken",
            "synthetic雪😀token",
            "synthetic\"\\token",
        ] {
            *credential.lock().unwrap() = canary.into();
            for escaped in [false, true] {
                let text = if escaped {
                    serde_json::json!({"shipping": canary})
                        .to_string()
                        .replace('雪', "\\u96ea")
                        .replace('😀', "\\ud83d\\ude00")
                } else {
                    format!("shipping {canary}")
                };
                for kind in ["WORK_ITEM", "KNOWLEDGE", "MEMORY", "ARTIFACT"] {
                    let mut information = input.clone();
                    information.content = text.clone();
                    let record = match kind {
                        "WORK_ITEM" => {
                            sqlx::query("UPDATE work_items SET description=? WHERE id='item'")
                                .bind(&text)
                                .execute(&pool)
                                .await
                                .unwrap();
                            None
                        }
                        "KNOWLEDGE" | "MEMORY" => {
                            let record = if kind == "KNOWLEDGE" {
                                app.create_knowledge("c", information).await.unwrap()
                            } else {
                                information.source_id = None;
                                app.capture_memory_candidate("c", information)
                                    .await
                                    .unwrap()
                            };
                            app.transition_information(
                                "c",
                                kind,
                                &record.id,
                                InformationLifecycle::Active,
                                1,
                            )
                            .await
                            .unwrap();
                            Some(record)
                        }
                        _ => {
                            app.add_artifact_version(
                                "c",
                                &v1.artifact_id,
                                ArtifactInput {
                                    title: "shipping".into(),
                                    artifact_type: "DOCUMENT".into(),
                                    project_id: Some("project".into()),
                                    work_item_id: Some("item".into()),
                                    run_id: Some("run".into()),
                                    agent_id: Some("agent".into()),
                                    sensitivity: Sensitivity::Internal,
                                    media_type: "text/plain".into(),
                                    content: text.clone(),
                                },
                            )
                            .await
                            .unwrap();
                            None
                        }
                    };
                    let files_before: std::collections::BTreeSet<_> =
                        std::fs::read_dir(db.path().join("content"))
                            .unwrap()
                            .map(|p| p.unwrap().file_name())
                            .collect();
                    let objects_before: i64 =
                        sqlx::query_scalar("SELECT COUNT(*) FROM content_objects")
                            .fetch_one(&pool)
                            .await
                            .unwrap();
                    assert!(
                        handler(ctx.clone(), req.clone(), watch::channel(false).1)
                            .await
                            .is_err(),
                        "credential accepted from {kind}, escaped={escaped}"
                    );
                    assert_eq!(
                        captured.lock().unwrap().len(),
                        1,
                        "additional provider request for {kind}"
                    );
                    assert_eq!(app.context_snapshots("c", "run").await.unwrap().len(), 1);
                    for snapshot in app.context_snapshots("c", "run").await.unwrap() {
                        let reference = serde_json::from_value(
                            snapshot.domain_versions["current_state_ref"].clone(),
                        )
                        .unwrap();
                        assert!(!contains_credential(
                            &String::from_utf8(store.read(&reference).unwrap()).unwrap(),
                            canary
                        ));
                    }
                    assert_eq!(
                        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM content_objects")
                            .fetch_one(&pool)
                            .await
                            .unwrap(),
                        objects_before
                    );
                    let files_after: std::collections::BTreeSet<_> =
                        std::fs::read_dir(db.path().join("content"))
                            .unwrap()
                            .map(|p| p.unwrap().file_name())
                            .collect();
                    assert_eq!(
                        files_after, files_before,
                        "context blob written before rejection"
                    );
                    for table in [
                        "context_snapshots",
                        "model_invocations",
                        "domain_events",
                        "workspace_domain_events",
                        "outbox_messages",
                        "audit_records",
                    ] {
                        for row in sqlx::query(&format!("SELECT * FROM {table}"))
                            .fetch_all(&pool)
                            .await
                            .unwrap()
                        {
                            for i in 0..row.len() {
                                if let Ok(value) = row.try_get::<String, _>(i) {
                                    assert!(
                                        !contains_credential(&value, canary),
                                        "credential persisted in {table}"
                                    );
                                }
                            }
                        }
                    }
                    if let Some(record) = record {
                        assert_eq!(
                            String::from_utf8(store.read(&record.content_ref).unwrap()).unwrap(),
                            text
                        );
                        app.transition_information(
                            "c",
                            kind,
                            &record.id,
                            InformationLifecycle::Retired,
                            2,
                        )
                        .await
                        .unwrap();
                    } else if kind == "WORK_ITEM" {
                        assert_eq!(
                            sqlx::query_scalar::<_, String>(
                                "SELECT description FROM work_items WHERE id='item'"
                            )
                            .fetch_one(&pool)
                            .await
                            .unwrap(),
                            text
                        );
                        sqlx::query("UPDATE work_items SET description=NULL WHERE id='item'")
                            .execute(&pool)
                            .await
                            .unwrap();
                    } else {
                        let version = app
                            .artifact_versions("c", &v1.artifact_id)
                            .await
                            .unwrap()
                            .pop()
                            .unwrap();
                        assert_eq!(
                            app.read_artifact_content("c", &v1.artifact_id, &version.id)
                                .await
                                .unwrap(),
                            text.as_bytes()
                        );
                        app.add_artifact_version(
                            "c",
                            &v1.artifact_id,
                            ArtifactInput {
                                title: "shipping".into(),
                                artifact_type: "DOCUMENT".into(),
                                project_id: Some("project".into()),
                                work_item_id: Some("item".into()),
                                run_id: Some("run".into()),
                                agent_id: Some("agent".into()),
                                sensitivity: Sensitivity::Internal,
                                media_type: "text/plain".into(),
                                content: "shipping authorized".into(),
                            },
                        )
                        .await
                        .unwrap();
                    }
                }
            }
        }
        *credential.lock().unwrap() = "synthetic\"\\token".into();
        handler(ctx.clone(), req.clone(), watch::channel(false).1)
            .await
            .unwrap();
        assert_eq!(captured.lock().unwrap().len(), 2);
        assert_eq!(headers.lock().unwrap()[1], "Bearer synthetic\"\\token");
        assert_eq!(app.context_snapshots("c", "run").await.unwrap().len(), 2);
        server.abort();
        let _ = server.await;
        pool.close().await;
        return;
    }
    let mut reflected = input;
    reflected.content = "shipping synthetic-token".into();
    let reflected = app.create_knowledge("c", reflected).await.unwrap();
    app.transition_information(
        "c",
        "KNOWLEDGE",
        &reflected.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    assert!(handler(ctx, req, watch::channel(false).1).await.is_err());
    assert_eq!(captured.lock().unwrap().len(), 1);
    assert_eq!(headers.lock().unwrap()[0], "Bearer synthetic-token");
    assert_eq!(app.context_snapshots("c", "run").await.unwrap().len(), 1);
    pool.close().await;
    let restarted = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store);
    assert_eq!(
        restarted.context_snapshots("c", "run").await.unwrap().len(),
        1
    );
    assert_eq!(
        restarted
            .artifact_versions("c", &v1.artifact_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        restarted
            .get_information("c", "MEMORY", &m.id)
            .await
            .unwrap()
            .version_id,
        m.version_id
    );
    assert!(
        !restarted
            .search_information(
                "c",
                &RetrievalScope {
                    project_id: Some("project".into()),
                    agent_id: Some("agent".into())
                },
                "shipping"
            )
            .await
            .unwrap()
            .is_empty()
    );
    server.abort();
    let _ = server.await;
    restarted.pool.close().await;
}

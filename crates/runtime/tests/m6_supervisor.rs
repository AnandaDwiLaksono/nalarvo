use chrono::Utc;
use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
    content_store::LocalContentStore,
    m6::ArtifactInput,
    m6_information::{InformationInput, SourceInput},
};
use nalarvo_domain::{
    CompanyId, DomainEvent, PrincipalRef, Run, RunStatus, ScopeRef,
    information::{InformationLifecycle, InformationScope, Sensitivity},
};
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
    sync::{Arc, Mutex},
    time::Duration,
};
struct Store;
impl SecretStore for Store {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        Ok(SecretValue::new("m6-supervisor-token"))
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
async fn queued_run_child_core_gateway_receives_scoped_versions() {
    let root = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", root.path().join("core.db").display());
    let pool = create_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let captured = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let cap = captured.clone();
    let router = axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
            let cap = cap.clone();
            async move {
                cap.lock().unwrap().push(body);
                axum::Json(json!({"choices":[{"message":{"content":"M6 supervisor verified"}}]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let run = fixture(&pool, &endpoint, true).await;
    sqlx::query("UPDATE work_items SET title='shipping task' WHERE id='item'")
        .execute(&pool)
        .await
        .unwrap();
    let store = Arc::new(LocalContentStore::open(&root.path().join("content")).unwrap());
    let app = ApplicationContext::new(pool.clone())
        .with_secret_store(Arc::new(Store))
        .with_content_store(store.clone());
    let source = app
        .create_knowledge_source(
            "c",
            SourceInput {
                title: "Guide".into(),
                project_id: Some("project".into()),
            },
        )
        .await
        .unwrap();
    let input = InformationInput {
        title: "shipping".into(),
        content: "shipping SUPERVISOR_KNOWLEDGE".into(),
        scope: InformationScope::Project,
        project_id: Some("project".into()),
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        source_id: Some(source.id),
        source_run_id: None,
    };
    let k = app.create_knowledge("c", input.clone()).await.unwrap();
    app.transition_information("c", "KNOWLEDGE", &k.id, InformationLifecycle::Active, 1)
        .await
        .unwrap();
    let mut m = input.clone();
    m.source_id = None;
    m.content = "shipping SUPERVISOR_MEMORY".into();
    let m = app.capture_memory_candidate("c", m).await.unwrap();
    app.transition_information("c", "MEMORY", &m.id, InformationLifecycle::Active, 1)
        .await
        .unwrap();
    let mut restricted = input;
    restricted.sensitivity = Sensitivity::Restricted;
    restricted.content = "shipping SENSITIVE_SUPERVISOR_CANARY".into();
    let restricted = app.create_knowledge("c", restricted).await.unwrap();
    app.transition_information(
        "c",
        "KNOWLEDGE",
        &restricted.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    let artifact = app
        .create_artifact(
            "c",
            ArtifactInput {
                title: "shipping".into(),
                artifact_type: "DOCUMENT".into(),
                project_id: Some("project".into()),
                work_item_id: Some("item".into()),
                run_id: Some(run.id.clone()),
                agent_id: Some("agent".into()),
                sensitivity: Sensitivity::Internal,
                media_type: "text/plain".into(),
                content: "shipping SUPERVISOR_ARTIFACT".into(),
            },
        )
        .await
        .unwrap();
    let supervisor = RuntimeSupervisor::new(
        pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            worker_principal_id: "worker".into(),
            lease_duration_secs: 20,
            heartbeat_interval_secs: 2,
        },
    )
    .with_model_handler(core_model::handler(app.clone()));
    assert!(supervisor.poll_and_dispatch_once(None).await.unwrap());
    tokio::time::timeout(Duration::from_secs(20), async {
        while supervisor.current_active_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    supervisor.shutdown().await;
    assert_eq!(
        get_run(&pool, &CompanyId("c".into()), &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    let requests = captured.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let text = requests[0]["messages"].to_string();
    for expected in [
        "SUPERVISOR_KNOWLEDGE",
        "SUPERVISOR_MEMORY",
        "SUPERVISOR_ARTIFACT",
    ] {
        assert!(text.contains(expected))
    }
    assert!(!text.contains("SENSITIVE_SUPERVISOR_CANARY") && !text.contains("m6-supervisor-token"));
    let hash = nalarvo_application::m6_context::messages_hash(&requests[0]["messages"]).unwrap();

    let snapshots = app.context_snapshots("c", &run.id).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].prompt_hash, hash);
    let refs = snapshots[0].references.to_string();
    for id in [&k.version_id, &m.version_id, &artifact.id] {
        assert!(refs.contains(id))
    }
    let linked:i64=sqlx::query_scalar("SELECT COUNT(*) FROM model_invocations WHERE run_id=? AND json_extract(request_metadata,'$.context_snapshot_id')=?").bind(&run.id).bind(&snapshots[0].id).fetch_one(&pool).await.unwrap();
    assert_eq!(linked, 1);
    pool.close().await;
    let restarted = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store);
    assert_eq!(
        restarted.context_snapshots("c", &run.id).await.unwrap()[0].prompt_hash,
        hash
    );
    assert_eq!(
        restarted
            .read_artifact_content("c", &artifact.artifact_id, &artifact.id)
            .await
            .unwrap(),
        b"shipping SUPERVISOR_ARTIFACT"
    );
    server.abort();
}

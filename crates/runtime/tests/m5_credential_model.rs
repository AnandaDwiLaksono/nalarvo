use nalarvo_application::{
    ApplicationContext, CredentialRef, SecretStore, SecretStoreError, SecretValue,
};
use nalarvo_model_gateway::ProviderErrorKind;
use nalarvo_model_gateway::{ModelMessage, ModelRequest};
use nalarvo_runtime::{core_dispatch::TrustedRunContext, core_model};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::watch,
};

#[derive(Clone)]
struct CountingStore(Arc<AtomicUsize>);
impl SecretStore for CountingStore {
    fn get(&self, reference: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        assert_eq!(reference.id, "cred");
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(SecretValue::new("synthetic-token"))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn real_provider_requires_current_lease_grant_permission_and_eligible_model() {
    let requests = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let count = requests.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0u8; 8192];
            let size = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..size]);
            assert!(
                request.contains("Authorization: Bearer synthetic-token")
                    || request.contains("authorization: Bearer synthetic-token")
            );
            let ordinal = count.fetch_add(1, Ordering::SeqCst);
            if ordinal == 3 {
                tokio::time::sleep(std::time::Duration::from_secs(31)).await;
                continue;
            }
            let body = if ordinal == 0 || ordinal >= 4 {
                r#"{"choices":[{"message":{"content":"authorized"}}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}"#
            } else if ordinal == 1 {
                r#"{"choices":[{"message":{"content":"synthetic-token"}}]}"#
            } else {
                r#"{"error":"synthetic-token must never persist"}"#
            };
            let (status, id) = if ordinal == 2 {
                ("429 Too Many Requests", "req-429")
            } else {
                ("200 OK", "req-1")
            };
            stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nx-request-id: {id}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        }
    });
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
    let gets = Arc::new(AtomicUsize::new(0));
    let app = ApplicationContext::new(pool.clone())
        .with_secret_store(Arc::new(CountingStore(gets.clone())));
    app.ensure_canonical_permissions("w").await.unwrap();
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
    let request = ModelRequest {
        model: "synthetic-model".into(),
        messages: vec![ModelMessage::user("test")],
        temperature: None,
        max_tokens: None,
        stream: false,
    };
    let call = |context: TrustedRunContext, req: ModelRequest| {
        let handler = core_model::handler(app.clone());
        async move { handler(context, req, watch::channel(false).1).await }
    };
    // Every denied case must stop before both secret resolution and HTTP.
    macro_rules! deny_without_side_effects {
        ($context:expr, $request:expr) => {{
            assert!(call($context, $request).await.is_err());
            assert_eq!(
                (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
                (0, 0)
            );
            let invocations: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM model_invocations WHERE run_id='run'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let usage: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM usage_records WHERE run_id='run'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!((invocations, usage), (0, 0));
        }};
    }
    deny_without_side_effects!(ctx.clone(), request.clone()); // missing provider.use
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('provider_grant','w','c','pd_provider.use','SYSTEM','worker','COMPANY','c','ALLOW',?)")
        .bind(now).execute(&pool).await.unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone()); // missing credential.use
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('grant','w','c','pd_credential.use','SYSTEM','worker','COMPANY','c','ALLOW',?)")
        .bind(now).execute(&pool).await.unwrap();
    sqlx::query("UPDATE runs SET assignment_id=NULL WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE runs SET assignment_id='assignment' WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE assignments SET status='RELEASED' WHERE id='assignment'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE assignments SET status='ACTIVE' WHERE id='assignment'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET model_profile_version_id='profile:2' WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone()); // unknown pinned revision
    sqlx::query("UPDATE runs SET model_profile_version_id=NULL WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    // Pin the authorized current profile while removing the grant below.
    sqlx::query("UPDATE runs SET model_profile_version_id='profile' WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agents SET model_profile_id=NULL WHERE id='agent'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM company_workspace_resource_grants")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone()); // missing resource grant
    sqlx::query("INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('c','w','profile',?)")
        .bind(now).execute(&pool).await.unwrap();
    sqlx::query("UPDATE agents SET model_profile_id='profile' WHERE id='agent'")
        .execute(&pool)
        .await
        .unwrap();
    let mut wrong = ctx.clone();
    wrong.company_id = "other".into();
    deny_without_side_effects!(wrong, request.clone());
    sqlx::query("UPDATE credential_refs SET status='REVOKED' WHERE id='cred'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE credential_refs SET status='ACTIVE' WHERE id='cred'")
        .execute(&pool)
        .await
        .unwrap();
    let mut wrong_model = request.clone();
    wrong_model.model = "unauthorized-model".into();
    deny_without_side_effects!(ctx.clone(), wrong_model);
    sqlx::query("UPDATE models SET status='RETIRED' WHERE id='model'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE models SET status='ACTIVE' WHERE id='model'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE provider_connections SET status='DISABLED' WHERE id='provider'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE provider_connections SET status='ENABLED' WHERE id='provider'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE model_profiles SET status='RETIRED' WHERE id='profile'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE model_profiles SET status='ACTIVE' WHERE id='profile'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agents SET status='PAUSED' WHERE id='agent'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE agents SET status='ACTIVE' WHERE id='agent'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_allocations SET status='RELEASED' WHERE id='alloc'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE agent_allocations SET status='ACTIVE' WHERE id='alloc'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status='CANCELLED' WHERE id='item'")
        .execute(&pool)
        .await
        .unwrap();
    deny_without_side_effects!(ctx.clone(), request.clone());
    sqlx::query("UPDATE work_items SET status='IN_PROGRESS' WHERE id='item'")
        .execute(&pool)
        .await
        .unwrap();
    let mut stale = ctx.clone();
    stale.lease_version = 2;
    deny_without_side_effects!(stale, request.clone());
    for (table, field, value, restore) in [
        ("workspaces", "status", "SUSPENDED", "ACTIVE"),
        ("teams", "status", "PAUSED", "ACTIVE"),
        (
            "run_execution_leases",
            "expires_at",
            "zzz",
            "2099-10-01T00:00:00Z",
        ),
        (
            "run_execution_leases",
            "expires_at",
            "2000-01-01T00:00:00-12:00",
            "2099-10-01T00:00:00Z",
        ),
        ("permission_grants", "workspace_id", "foreign", "w"),
        ("permission_definitions", "workspace_id", "foreign", "w"),
    ] {
        sqlx::query(&format!("UPDATE {table} SET {field}=?"))
            .bind(value)
            .execute(&pool)
            .await
            .unwrap();
        deny_without_side_effects!(ctx.clone(), request.clone());
        sqlx::query(&format!("UPDATE {table} SET {field}=?"))
            .bind(restore)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,created_at,updated_at) VALUES('budget','w','c','B',1,'t','t')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('other-project','c','Other','MEDIUM','ACTIVE','t','t')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,created_at,updated_at) VALUES('project-budget','w','c','project','Project',1,'t','t'),('other-budget','w','c','other-project','Other',0,'t','t')").execute(&pool).await.unwrap();
    for budget in ["budget", "project-budget"] {
        sqlx::query("UPDATE budgets SET total_limit=0 WHERE id=?")
            .bind(budget)
            .execute(&pool)
            .await
            .unwrap();
        deny_without_side_effects!(ctx.clone(), request.clone());
        sqlx::query("UPDATE budgets SET total_limit=1 WHERE id=?")
            .bind(budget)
            .execute(&pool)
            .await
            .unwrap();
    }
    for dimension in ["TOKENS", "INPUT_TOKENS", "OUTPUT_TOKENS", "COST"] {
        sqlx::query("INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount) VALUES('dimension','project-budget',?,0)").bind(dimension).execute(&pool).await.unwrap();
        deny_without_side_effects!(ctx.clone(), request.clone());
        sqlx::query("DELETE FROM budget_limits WHERE id='dimension'")
            .execute(&pool)
            .await
            .unwrap();
    }
    for mutation in [
        "spent_amount=-1",
        "reserved_amount=-1",
        "total_limit='malformed'",
        "status='BROKEN'",
        "ends_at='zzz'",
        "spent_amount=1",
    ] {
        sqlx::query(&format!("UPDATE budgets SET {mutation}"))
            .execute(&pool)
            .await
            .unwrap();
        deny_without_side_effects!(ctx.clone(), request.clone());
        sqlx::query("UPDATE budgets SET total_limit=1,reserved_amount=0,spent_amount=0,status='ACTIVE',ends_at=NULL").execute(&pool).await.unwrap();
    }
    let response = call(ctx.clone(), request.clone()).await.unwrap();
    assert_eq!(response.text.as_deref(), Some("authorized"));
    assert_eq!(
        (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
        (1, 1)
    );
    let budget: (f64, f64) =
        sqlx::query_as("SELECT reserved_amount,spent_amount FROM budgets WHERE id='budget'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(budget, (0.0, 1.0));
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='project-budget'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        budget
    );
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='other-budget'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        (0.0, 0.0)
    );
    assert!(call(ctx.clone(), request.clone()).await.is_err());
    assert_eq!(
        (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
        (1, 1)
    );
    sqlx::query("UPDATE budgets SET total_limit=5 WHERE id!='other-budget'")
        .execute(&pool)
        .await
        .unwrap();
    let used: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_records WHERE action='CredentialUsed' AND scope_id='c' AND details LIKE '%model.invoke%'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(used, 1); // durable audit, not just tracing
    type InvocationProof = (
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<f64>,
        String,
    );
    let (model_id, provider_id, provider_request_id, input, output, cost, step): InvocationProof = sqlx::query_as("SELECT model_id,provider_connection_id,provider_request_id,input_tokens,output_tokens,estimated_cost,step_id FROM model_invocations WHERE run_id='run'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(
        (
            model_id.as_str(),
            provider_id.as_str(),
            provider_request_id.as_deref(),
            input,
            output,
            cost
        ),
        ("model", "provider", Some("req-1"), Some(3), Some(4), None)
    );
    let (usage_step, quantity, usage_cost): (String, i64, Option<f64>) = sqlx::query_as(
        "SELECT step_id,quantity,estimated_cost FROM usage_records WHERE run_id='run'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (usage_step.as_str(), quantity, usage_cost),
        (step.as_str(), 7, None)
    );
    sqlx::query("UPDATE run_execution_leases SET lease_version=lease_version+1 WHERE id='lease'")
        .execute(&pool)
        .await
        .unwrap(); // heartbeat during worker turn
    let mut ctx = ctx;
    ctx.lease_version = 2; // supervisor supplies refreshed version at handler entry
    let reflected = call(ctx.clone(), request.clone()).await.unwrap_err();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM model_invocations WHERE run_id='run'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    assert!(!reflected.message.contains("synthetic-token"));
    assert_eq!(
        (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
        (2, 2)
    );
    let limited = call(ctx.clone(), request.clone()).await.unwrap_err();
    assert_eq!(limited.kind, ProviderErrorKind::RateLimited);
    assert_eq!(
        (
            limited.message.as_str(),
            limited.request_id.as_deref(),
            limited.status
        ),
        ("HTTP 429", Some("req-429"), Some(429))
    );
    let timed_out = call(ctx, request).await.unwrap_err();
    assert_eq!(timed_out.kind, ProviderErrorKind::Timeout);
    type FailureProof = (
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<f64>,
        Option<String>,
    );
    let failures: Vec<FailureProof> = sqlx::query_as("SELECT status,failure_class,provider_request_id,input_tokens,output_tokens,estimated_cost,failure_detail FROM model_invocations WHERE run_id='run' AND status='FAILED' ORDER BY invocation_index")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(failures.len(), 2);
    assert_eq!(
        (
            failures[0].0.as_str(),
            failures[0].1.as_str(),
            failures[0].2.as_deref(),
            failures[0].3,
            failures[0].4,
            failures[0].5,
            failures[0].6.as_deref()
        ),
        (
            "FAILED",
            "PROVIDER_RATE_LIMIT",
            Some("req-429"),
            None,
            None,
            None,
            Some("HTTP 429")
        )
    );
    assert_eq!(
        (
            failures[1].1.as_str(),
            failures[1].2.as_deref(),
            failures[1].6.as_deref()
        ),
        ("PROVIDER_TIMEOUT", None, Some("timed out"))
    );
    let usage_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_records WHERE run_id='run'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(usage_count, 1);
    assert_eq!(
        (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
        (4, 4)
    );
    let audit_details: Vec<String> = sqlx::query_scalar("SELECT details FROM audit_records")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(
        audit_details
            .iter()
            .all(|row| !row.contains("synthetic-token"))
    );
    let leaked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM model_invocations WHERE failure_detail LIKE '%synthetic-token%' OR provider_request_id LIKE '%synthetic-token%'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(leaked, 0);
    let (reserved, spent): (f64, f64) =
        sqlx::query_as("SELECT reserved_amount,spent_amount FROM budgets WHERE id='budget'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(reserved >= 1.0); // timeout/unknown outcomes never refund authority
    sqlx::query("UPDATE budgets SET total_limit=? WHERE id!='other-budget'")
        .bind(reserved + spent + 1.0)
        .execute(&pool)
        .await
        .unwrap();
    let concurrent_ctx = TrustedRunContext {
        run_id: "run".into(),
        company_id: "c".into(),
        project_id: "project".into(),
        work_item_id: "item".into(),
        agent_id: "agent".into(),
        lease_id: "lease".into(),
        lease_version: 2,
        worker_principal_id: "worker".into(),
        live_lease_version: None,
    };
    let request = ModelRequest {
        model: "synthetic-model".into(),
        messages: vec![ModelMessage::user("budget race")],
        temperature: None,
        max_tokens: Some(8),
        stream: false,
    };
    let (a, b) = tokio::join!(
        call(concurrent_ctx.clone(), request.clone()),
        call(concurrent_ctx.clone(), request.clone())
    );
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "{a:?} {b:?}"
    );
    assert_eq!(
        (gets.load(Ordering::SeqCst), requests.load(Ordering::SeqCst)),
        (5, 5)
    );
    let total: (f64, f64) = sqlx::query_as(
        "SELECT reserved_amount+spent_amount,total_limit FROM budgets WHERE id='budget'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total.0, total.1);
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount+spent_amount,total_limit FROM budgets WHERE id='project-budget'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        total
    );
    assert!(call(concurrent_ctx, request).await.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 5);
    server.abort();
}

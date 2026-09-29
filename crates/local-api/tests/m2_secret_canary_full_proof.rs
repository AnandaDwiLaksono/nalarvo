use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use nalarvo_application::{
    ApplicationContext, CredentialRef, FakeSecretStore, InMemoryAuditSink, SecretStore,
};
use nalarvo_contracts::{CredentialRefDto, ProviderConnectionDto};
use nalarvo_domain::WorkspaceId;
use nalarvo_local_api::router_with_app;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default)]
struct LogCapture {
    logs: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = StringVisitor(String::new());
        event.record(&mut visitor);
        self.logs.lock().unwrap().push(visitor.0);
    }
}

struct StringVisitor(String);
impl tracing::field::Visit for StringVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={:?}", field.name(), value);
    }
}

#[tokio::test]
async fn test_secret_canary_exhaustive_zero_leak_proof() {
    // Setup in-memory log capture
    let log_capture = LogCapture::default();
    let subscriber = tracing_subscriber::registry().with(log_capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("canary_full.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let audit_sink = Arc::new(InMemoryAuditSink::new());
    let base_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let secret_store = Arc::new(FakeSecretStore::default());
    let app_ctx = ApplicationContext::with_ports(
        base_ctx.pool.clone(),
        audit_sink.clone(),
        secret_store.clone(),
    );

    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    let synthetic_canary = "CANARY_SYNTHETIC_SECRET_987654321_DO_NOT_LOG";
    let token = "test-bearer-token";
    let app = router_with_app(token, Some(app_ctx.clone()));

    let mut leak_count = 0;

    // Surface 1: Submit credential via HTTP API
    let submit_req = Request::builder()
        .method("POST")
        .uri("/api/v1/workspace/credentials")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"name":"ProdKey","secret":"{synthetic_canary}"}}"#
        )))
        .unwrap();

    let res = app.clone().oneshot(submit_req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body_bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    if body_str.contains(synthetic_canary) {
        leak_count += 1;
    }

    // Verify stored in authorized SecretStore path
    let cred_dto: CredentialRefDto = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        secret_store
            .get(&CredentialRef::new(&cred_dto.id))
            .unwrap()
            .expose(),
        synthetic_canary.as_bytes()
    );

    // Surface 2: Create Provider referencing credential
    let create_prov_req = Request::builder()
        .method("POST")
        .uri("/api/v1/workspace/providers")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"name":"OpenAI Main","provider_kind":"openai","credential_ref_id":"{}"}}"#,
            cred_dto.id
        )))
        .unwrap();

    let res = app.clone().oneshot(create_prov_req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let prov_bytes = res.into_body().collect().await.unwrap().to_bytes();
    let prov_str = String::from_utf8_lossy(&prov_bytes);
    if prov_str.contains(synthetic_canary) {
        leak_count += 1;
    }
    let prov_dto: ProviderConnectionDto = serde_json::from_slice(&prov_bytes).unwrap();

    // Surface 3: GET /api/v1/workspace/credentials
    let list_cred_req = Request::builder()
        .method("GET")
        .uri("/api/v1/workspace/credentials")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(list_cred_req).await.unwrap();
    let list_bytes = res.into_body().collect().await.unwrap().to_bytes();
    if String::from_utf8_lossy(&list_bytes).contains(synthetic_canary) {
        leak_count += 1;
    }

    // Surface 4: GET /api/v1/workspace/providers
    let list_prov_req = Request::builder()
        .method("GET")
        .uri("/api/v1/workspace/providers")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(list_prov_req).await.unwrap();
    let list_prov_bytes = res.into_body().collect().await.unwrap().to_bytes();
    if String::from_utf8_lossy(&list_prov_bytes).contains(synthetic_canary) {
        leak_count += 1;
    }

    // Surface 5: Trigger safe error response
    let bad_req = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/workspace/credentials/{}:disable",
            cred_dto.id
        ))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"expected_version": 9999}"#)) // stale version error
        .unwrap();
    let res = app.clone().oneshot(bad_req).await.unwrap();
    let err_bytes = res.into_body().collect().await.unwrap().to_bytes();
    if String::from_utf8_lossy(&err_bytes).contains(synthetic_canary) {
        leak_count += 1;
    }

    // Surface 6: Record & DTO serialization
    let cred_rec = app_ctx
        .list_credentials(&workspace_id)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let prov_rec = app_ctx
        .list_providers(&workspace_id)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let rec_str = format!("{cred_rec:?} {prov_rec:?}");
    if rec_str.contains(synthetic_canary) {
        leak_count += 1;
    }
    let json_rec_str = format!(
        "{} {}",
        serde_json::to_string(&cred_dto).unwrap(),
        serde_json::to_string(&prov_dto).unwrap()
    );
    if json_rec_str.contains(synthetic_canary) {
        leak_count += 1;
    }

    // Surface 7: Raw SQLite database content on disk
    let raw_db_bytes = std::fs::read(&db_path).unwrap();
    if String::from_utf8_lossy(&raw_db_bytes).contains(synthetic_canary) {
        leak_count += 1;
    }

    // Surface 8: Database tables (domain_events, workspace_domain_events, outbox_messages, etc.)
    let domain_events: Vec<(String,)> = sqlx::query_as("SELECT payload FROM domain_events")
        .fetch_all(&app_ctx.pool)
        .await
        .unwrap();
    for (payload,) in domain_events {
        if payload.contains(synthetic_canary) {
            leak_count += 1;
        }
    }
    let ws_events: Vec<(String,)> = sqlx::query_as("SELECT payload FROM workspace_domain_events")
        .fetch_all(&app_ctx.pool)
        .await
        .unwrap();
    for (payload,) in ws_events {
        if payload.contains(synthetic_canary) {
            leak_count += 1;
        }
    }

    // Surface 9: AuditSink records
    for record in audit_sink.records() {
        let record_str = format!("{record:?}");
        if record_str.contains(synthetic_canary) {
            leak_count += 1;
        }
    }

    // Surface 10: Structured logs
    let logs = log_capture.logs.lock().unwrap().join("\n");
    if logs.contains(synthetic_canary) {
        leak_count += 1;
    }

    assert_eq!(
        leak_count, 0,
        "SECRET CANARY LEAK COUNT MUST BE ZERO OUTSIDE SECRETSTORE! Found {} leaks",
        leak_count
    );
}

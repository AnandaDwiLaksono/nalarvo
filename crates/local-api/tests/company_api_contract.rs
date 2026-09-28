use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use nalarvo_application::ApplicationContext;
use nalarvo_contracts::{
    CompanyDto, CompanyListResponse, CreateCompanyRequest, ErrorEnvelope, UpdateCompanyRequest,
    error_codes,
};
use nalarvo_local_api::router_with_app;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_TOKEN: &str = "test-token-123456";
const WORKSPACE_ID: &str = "0191e4b8-0002-7000-8000-000000000001";

async fn setup_app() -> axum::Router {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_api.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx))
}

#[tokio::test]
async fn test_unauthorized_request_rejected() {
    let app = setup_app().await;

    let req = Request::builder()
        .uri(format!("/api/v1/workspaces/{WORKSPACE_ID}/companies"))
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let body = res.into_body().collect().await.unwrap().to_bytes();
    let err: ErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(err.error.code, error_codes::UNAUTHORIZED);
}

#[tokio::test]
async fn test_company_crud_lifecycle_and_idempotency() {
    let app = setup_app().await;

    // 1. Create company
    let create_payload = CreateCompanyRequest {
        name: "Acme Corporation".into(),
        description: Some("Making anvils".into()),
    };
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/workspaces/{WORKSPACE_ID}/companies"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("idempotency-key", "idem-co-1")
        .body(Body::from(serde_json::to_vec(&create_payload).unwrap()))
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let created: CompanyDto = serde_json::from_slice(&body).unwrap();
    assert_eq!(created.name, "Acme Corporation");
    assert_eq!(created.row_version, 1);
    assert_eq!(created.status, "DRAFT");

    // 2. Idempotent retry with same key returns cached result
    let req_idem = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/workspaces/{WORKSPACE_ID}/companies"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("idempotency-key", "idem-co-1")
        .body(Body::from(serde_json::to_vec(&create_payload).unwrap()))
        .unwrap();

    let res_idem = app.clone().oneshot(req_idem).await.unwrap();
    assert_eq!(res_idem.status(), StatusCode::CREATED);
    let body_idem = res_idem.into_body().collect().await.unwrap().to_bytes();
    let idem_dto: CompanyDto = serde_json::from_slice(&body_idem).unwrap();
    assert_eq!(idem_dto.id, created.id);

    // 3. List companies in workspace
    let req_list = Request::builder()
        .uri(format!("/api/v1/workspaces/{WORKSPACE_ID}/companies"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();

    let res_list = app.clone().oneshot(req_list).await.unwrap();
    assert_eq!(res_list.status(), StatusCode::OK);
    let body_list = res_list.into_body().collect().await.unwrap().to_bytes();
    let list: CompanyListResponse = serde_json::from_slice(&body_list).unwrap();
    assert_eq!(list.companies.len(), 1);
    assert_eq!(list.companies[0].id, created.id);

    // 4. Get company
    let req_get = Request::builder()
        .uri(format!(
            "/api/v1/workspaces/{WORKSPACE_ID}/companies/{}",
            created.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();

    let res_get = app.clone().oneshot(req_get).await.unwrap();
    assert_eq!(res_get.status(), StatusCode::OK);
    let body_get = res_get.into_body().collect().await.unwrap().to_bytes();
    let fetched: CompanyDto = serde_json::from_slice(&body_get).unwrap();
    assert_eq!(fetched.id, created.id);

    // 5. Update company with optimistic concurrency check
    let update_payload_stale = UpdateCompanyRequest {
        name: "Acme Corp Renewed".into(),
        description: None,
        expected_version: 0, // Stale!
    };
    let req_update_stale = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/v1/workspaces/{WORKSPACE_ID}/companies/{}",
            created.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&update_payload_stale).unwrap(),
        ))
        .unwrap();

    let res_update_stale = app.clone().oneshot(req_update_stale).await.unwrap();
    assert_eq!(res_update_stale.status(), StatusCode::CONFLICT);
    let body_err = res_update_stale
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let err: ErrorEnvelope = serde_json::from_slice(&body_err).unwrap();
    assert_eq!(err.error.code, error_codes::STALE_VERSION);

    // Valid update with expected_version = 1
    let update_payload_valid = UpdateCompanyRequest {
        name: "Acme Corp Renewed".into(),
        description: Some("New desc".into()),
        expected_version: 1,
    };
    let req_update_valid = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/v1/workspaces/{WORKSPACE_ID}/companies/{}",
            created.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&update_payload_valid).unwrap(),
        ))
        .unwrap();

    let res_update_valid = app.clone().oneshot(req_update_valid).await.unwrap();
    assert_eq!(res_update_valid.status(), StatusCode::OK);
    let body_valid = res_update_valid
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let updated: CompanyDto = serde_json::from_slice(&body_valid).unwrap();
    assert_eq!(updated.name, "Acme Corp Renewed");
    assert_eq!(updated.row_version, 2);
}

#[tokio::test]
async fn test_cross_workspace_scope_isolation() {
    let app = setup_app().await;

    // Create company in default workspace
    let create_payload = CreateCompanyRequest {
        name: "Scoped Co".into(),
        description: None,
    };
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/workspaces/{WORKSPACE_ID}/companies"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&create_payload).unwrap()))
        .unwrap();

    let res = app.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let created: CompanyDto = serde_json::from_slice(&body).unwrap();

    // Query using a different workspace ID must return NOT_FOUND (404)
    let fake_ws = "0191e4b8-9999-7000-8000-000000000099";
    let req_diff_ws = Request::builder()
        .uri(format!(
            "/api/v1/workspaces/{fake_ws}/companies/{}",
            created.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();

    let res_diff_ws = app.oneshot(req_diff_ws).await.unwrap();
    assert_eq!(res_diff_ws.status(), StatusCode::NOT_FOUND);
}

//! M5 Axum router and contract tests.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use nalarvo_application::ApplicationContext;
use nalarvo_local_api::router_with_app;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_TOKEN: &str = "test-token";

#[tokio::test]
async fn policy_and_budget_scope_survive_http_contract() {
    let ctx = ApplicationContext::init("sqlite::memory:").await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,is_personal,created_at,updated_at) VALUES('w','u','W','w',0,'t','t'),('foreign','u','F','f',0,'t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t'),('other','foreign','F','ACTIVE','t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
        "INSERT INTO policies(id,workspace_id,company_id,name,current_version,created_at,updated_at) VALUES('workspace','w',NULL,'A',1,'t','t'),('company','w','c','B',1,'t','t'),('foreign','foreign',NULL,'C',1,'t','t'),('other','foreign','other','D',1,'t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,created_at,updated_at) VALUES('company','w','c',NULL,'A',1,'t','t'),('project','w','c','p','B',1,'t','t')",
    ] {
        sqlx::query(q).execute(&ctx.pool).await.unwrap();
    }
    let app = router_with_app(Arc::<str>::from(TEST_TOKEN), Some(ctx));
    for resource in ["policies", "budgets"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/companies/c/{resource}"))
                    .header("authorization", format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|item| item["workspace_id"] == "w"));
        if resource == "policies" {
            assert!(items[0]["company_id"].is_null());
            assert_eq!(items[1]["company_id"], "c");
            let dto: Vec<nalarvo_contracts::PolicyDto> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_value(dto).unwrap(), serde_json::json!(items));
        } else {
            assert!(items[0]["project_id"].is_null());
            assert_eq!(items[1]["project_id"], "p");
            let dto: Vec<nalarvo_contracts::BudgetDto> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_value(dto).unwrap(), serde_json::json!(items));
        }
    }
}

#[tokio::test]
async fn test_m5_approvals_api_routes() {
    let app_ctx = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let app = router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx.clone()));

    // 1. Unauthorized request without bearer token
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/approvals")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 2. Authorized GET list approvals
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/approvals")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let items: Vec<nalarvo_contracts::ApprovalInboxItem> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(items.len(), 0);

    // 3. Actions list
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/actions")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let actions: Vec<nalarvo_contracts::ActionSummaryDto> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(actions.len(), 0);

    // 4. Permission definitions list
    let req = Request::builder()
        .uri("/api/v1/permission-definitions")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let pds: Vec<nalarvo_contracts::PermissionDefinitionDto> =
        serde_json::from_slice(&bytes).unwrap();
    assert!(pds.iter().any(|p| p.name == "credential.use"));
    assert!(pds.iter().any(|p| p.name == "filesystem.write"));

    // 5. Permission grants list
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/permission-grants")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 6. Policies list
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/policies")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 7. Budgets list
    let req = Request::builder()
        .uri("/api/v1/companies/comp_1/budgets")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

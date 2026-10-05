use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use nalarvo_application::ApplicationContext;
use nalarvo_contracts::*;
use nalarvo_local_api::router_with_app;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_TOKEN: &str = "test-token-m4";

struct TestSetup {
    app: axum::Router,
    company_id: String,
    project_id: String,
    work_item_id: String,
    agent_id: String,
}

async fn setup_app() -> TestSetup {
    let app_ctx = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let app = router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx.clone()));

    // 1. Create company via API
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/workspaces/0191e4b8-0002-7000-8000-000000000001/companies")
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"name": "Company M4"})).unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let co_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let company_id = co_json["id"].as_str().unwrap().to_string();

    // 2. Create department, role, agent
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_id}/departments"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"name": "Engineering"})).unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let dept_json: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let dept_id = dept_json["id"].as_str().unwrap();

    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_id}/roles"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"name": "Engineer", "department_id": dept_id})).unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let role_json: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let role_id = role_json["id"].as_str().unwrap();

    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_id}/agents"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "name": "Bot 1",
                "primary_department_id": dept_id,
                "role_id": role_id,
                "capacity": 5
            }))
            .unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let agent_json: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let agent_id = agent_json["id"].as_str().unwrap().to_string();

    // 3. Create project
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_id}/projects"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"name": "Project M4"})).unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let p_json: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let project_id = p_json["id"].as_str().unwrap().to_string();

    // 4. Create work item
    let req = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_id}/projects/{project_id}/work"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "title": "Task 1",
                "description": null,
                "objective_id": null,
                "parent_work_item_id": null,
                "work_type": "TASK"
            }))
            .unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let w_json: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let work_item_id = w_json["id"].as_str().unwrap().to_string();

    TestSetup {
        app,
        company_id,
        project_id,
        work_item_id,
        agent_id,
    }
}

#[tokio::test]
async fn run_routes_exist_and_enforce_bearer_auth() {
    let setup = setup_app().await;

    let path = format!(
        "/api/v1/companies/{}/projects/{}/runs",
        setup.company_id, setup.project_id
    );
    let unauth_req = Request::builder().uri(&path).body(Body::empty()).unwrap();
    let res = setup.app.clone().oneshot(unauth_req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let auth_req = Request::builder()
        .uri(&path)
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = setup.app.clone().oneshot(auth_req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let list: RunListResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(list.runs.len(), 0);
}

#[tokio::test]
async fn run_lifecycle_and_details_endpoints_work_end_to_end() {
    let setup = setup_app().await;
    let app = setup.app;

    // 1. Create Run
    let create_req = CreateRunRequest {
        work_item_id: setup.work_item_id.clone(),
        assignment_id: None,
        executing_agent_id: setup.agent_id.clone(),
        trigger_type: "MANUAL".into(),
        retry_of_run_id: None,
    };
    let req = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&create_req).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let run: RunDto = serde_json::from_slice(&body).unwrap();
    assert_eq!(run.lifecycle_state, "QUEUED");
    let run_id = run.id;

    // 2. Get Run
    let req = Request::builder()
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let run_show: RunShowResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(run_show.run.id, run_id);

    // 3. Queue Run (idempotent/retry transition to queue)
    let req = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}/queue",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&QueueRunRequest {
                expected_version: 1,
            })
            .unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let accepted: CommandAcceptedResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(accepted.command, "QUEUE");

    // 4. Cancel Run
    let req = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}/cancel",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&CancelRunRequest {
                expected_version: 2,
                reason: Some("operator_stop".into()),
            })
            .unwrap(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    // 5. Steps / Timeline / Result / Usage
    let req = Request::builder()
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}/steps",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}/timeline",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri(format!(
            "/api/v1/companies/{}/projects/{}/runs/{run_id}/usage",
            setup.company_id, setup.project_id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

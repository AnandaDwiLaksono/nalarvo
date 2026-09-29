use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use nalarvo_application::ApplicationContext;
use nalarvo_local_api::router_with_app;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "m2-test-token";

async fn app() -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    // Keep the directory alive for the pool lifetime.
    let path = dir.keep().join("api.db");
    let ctx = ApplicationContext::init(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    router_with_app(Arc::<str>::from(TOKEN), Some(ctx))
}

#[tokio::test]
async fn workspace_endpoint_returns_server_scoped_identity_without_client_identity_input() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .uri("/api/v1/workspace")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let workspace: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(workspace["id"], "0191e4b8-0002-7000-8000-000000000001");
    assert_eq!(
        workspace["owner_user_id"],
        "0191e4b8-0001-7000-8000-000000000001"
    );
    assert_eq!(workspace["status"], "ACTIVE");
    assert_eq!(workspace["row_version"], 2);
    assert!(
        workspace["created_at"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        workspace["updated_at"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
}

#[tokio::test]
async fn company_can_be_activated_through_semantic_route_with_expected_version() {
    let app = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/workspaces/0191e4b8-0002-7000-8000-000000000001/companies")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Company A","description":null}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let company: Value = serde_json::from_slice(&body).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/companies/{}:activate",
                    company["id"].as_str().unwrap()
                ))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let activated: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(activated["status"], "ACTIVE");
    assert_eq!(activated["row_version"], 2);
}

#[tokio::test]
async fn company_lifecycle_full_cycle_pause_resume_archive_and_occ() {
    let app = app().await;

    // 1. Create company
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/workspaces/0191e4b8-0002-7000-8000-000000000001/companies")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Company B","description":null}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let company: Value = serde_json::from_slice(&body).unwrap();
    let company_id = company["id"].as_str().unwrap();

    // 2. Activate with wrong expected_version -> 409 CONFLICT
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:activate"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":99}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // 3. Activate -> 200 OK (version becomes 2)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:activate"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let val: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(val["status"], "ACTIVE");
    assert_eq!(val["row_version"], 2);

    // 4. Pause -> 200 OK (version becomes 3)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:pause"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let val: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(val["status"], "PAUSED");
    assert_eq!(val["row_version"], 3);

    // 5. Resume -> 200 OK (version becomes 4)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:resume"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":3}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let val: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(val["status"], "ACTIVE");
    assert_eq!(val["row_version"], 4);

    // 6. Pause before archive -> 200 OK (version becomes 5)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:pause"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":4}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 7. Archive -> 200 OK (version becomes 6)
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/companies/{company_id}:archive"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"expected_version":5}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let val: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(val["status"], "ARCHIVED");
    assert_eq!(val["row_version"], 6);
}

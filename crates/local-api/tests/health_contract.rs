use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn health_requires_the_daemon_bearer_token() {
    let app = nalarvo_local_api::router("m0-test-token");

    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let accepted = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .header("authorization", "Bearer m0-test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);

    let bytes = accepted.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        String::from_utf8(bytes.to_vec()).unwrap(),
        r#"{"status":"ok","service":"nalarvo-core"}"#
    );
}

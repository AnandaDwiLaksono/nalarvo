use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn daemon_serves_health_via_local_client() {
    let token = "test-token-123456";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    let router = nalarvo_local_api::router(Arc::<str>::from(token));
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let health = nalarvo_client::health(&base_url, token).await.unwrap();
    assert_eq!(health.status, "ok");
    assert_eq!(health.service, "nalarvo-core");

    let bad_auth = nalarvo_client::health(&base_url, "wrong_token").await;
    assert!(bad_auth.is_err());
}

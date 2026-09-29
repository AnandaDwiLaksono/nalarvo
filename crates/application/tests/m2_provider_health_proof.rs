use axum::{Router, http::StatusCode, routing::get};
use nalarvo_application::ApplicationContext;
use nalarvo_domain::WorkspaceId;
use tempfile::tempdir;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_fake_http_provider_health_probe() {
    // 1. Spawn a local fake HTTP server for 200 OK
    let app_ok = Router::new().route("/health", get(|| async { (StatusCode::OK, "OK") }));
    let listener_ok = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_ok = listener_ok.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener_ok, app_ok).await.unwrap();
    });

    // 2. Spawn a local fake HTTP server for 401 Unauthorized
    let app_401 = Router::new().route(
        "/health",
        get(|| async { (StatusCode::UNAUTHORIZED, "Unauthorized") }),
    );
    let listener_401 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_401 = listener_401.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener_401, app_401).await.unwrap();
    });

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("health_test.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    // Provider A: Healthy endpoint (200 OK -> HEALTHY)
    let p_ok = app_ctx
        .create_provider(&workspace_id, "Healthy Provider", "openai", None)
        .await
        .unwrap();
    assert_eq!(p_ok.status, "CONFIGURED"); // Lifecycle state = CONFIGURED
    sqlx::query("UPDATE provider_connections SET endpoint = ? WHERE id = ?")
        .bind(format!("http://{}/health", addr_ok))
        .bind(&p_ok.id)
        .execute(&app_ctx.pool)
        .await
        .unwrap();

    let res_ok = app_ctx
        .test_provider_health(&workspace_id, &p_ok.id)
        .await
        .unwrap();
    assert_eq!(res_ok.health, "HEALTHY");
    assert_eq!(res_ok.status, "CONFIGURED"); // Distinct from health state

    // Provider B: Auth failure endpoint (401 Unauthorized -> UNAVAILABLE)
    let p_401 = app_ctx
        .create_provider(&workspace_id, "Auth Failing Provider", "openai", None)
        .await
        .unwrap();
    sqlx::query("UPDATE provider_connections SET endpoint = ? WHERE id = ?")
        .bind(format!("http://{}/health", addr_401))
        .bind(&p_401.id)
        .execute(&app_ctx.pool)
        .await
        .unwrap();

    let res_401 = app_ctx
        .test_provider_health(&workspace_id, &p_401.id)
        .await
        .unwrap();
    assert_eq!(res_401.health, "UNAVAILABLE");
    assert_eq!(res_401.status, "CONFIGURED"); // Lifecycle state remains CONFIGURED

    // Provider C: Connection refused endpoint (Port not bound -> UNAVAILABLE)
    let p_unavail = app_ctx
        .create_provider(&workspace_id, "Unavailable Provider", "openai", None)
        .await
        .unwrap();
    sqlx::query("UPDATE provider_connections SET endpoint = ? WHERE id = ?")
        .bind("http://127.0.0.1:59999/nonexistent")
        .bind(&p_unavail.id)
        .execute(&app_ctx.pool)
        .await
        .unwrap();

    let res_unavail = app_ctx
        .test_provider_health(&workspace_id, &p_unavail.id)
        .await
        .unwrap();
    assert_eq!(res_unavail.health, "UNAVAILABLE");
    assert_eq!(res_unavail.status, "CONFIGURED");

    // Provider D: Disable lifecycle and test health
    let disabled = app_ctx
        .disable_provider(&workspace_id, &p_ok.id, p_ok.row_version)
        .await
        .unwrap();
    assert_eq!(disabled.status, "DISABLED");
    let res_disabled_probe = app_ctx
        .test_provider_health(&workspace_id, &p_ok.id)
        .await
        .unwrap();
    assert_eq!(res_disabled_probe.status, "DISABLED"); // Lifecycle state is DISABLED
    assert_eq!(res_disabled_probe.health, "HEALTHY"); // Health state probe remains HEALTHY
}

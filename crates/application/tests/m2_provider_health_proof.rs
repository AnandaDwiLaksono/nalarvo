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

#[tokio::test]
async fn test_provider_health_does_not_follow_redirects_or_leak_credentials() {
    use axum::http::HeaderMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let server_b_requests = Arc::new(AtomicUsize::new(0));
    let server_b_leak_count = Arc::new(AtomicUsize::new(0));

    // Server B: Target of potential redirect — records any arriving requests or auth headers
    let b_requests = Arc::clone(&server_b_requests);
    let b_leaks = Arc::clone(&server_b_leak_count);
    let app_b = Router::new().route(
        "/redirect-target",
        get(move |headers: HeaderMap| {
            let req_count = Arc::clone(&b_requests);
            let leak_count = Arc::clone(&b_leaks);
            async move {
                req_count.fetch_add(1, Ordering::SeqCst);
                for (name, value) in &headers {
                    let name_str = name.as_str().to_lowercase();
                    if name_str == "authorization"
                        || name_str == "api-key"
                        || name_str.contains("secret")
                    {
                        leak_count.fetch_add(1, Ordering::SeqCst);
                    }
                    if value
                        .to_str()
                        .is_ok_and(|s| s.contains("canary-secret-to-never-leak"))
                    {
                        leak_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
                (StatusCode::OK, "Arrived at B")
            }
        }),
    );
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener_b, app_b).await.unwrap();
    });

    // Server A: Redirector — responds 302 Found redirecting to Server B
    let redirect_location = format!("http://{addr_b}/redirect-target");
    let app_a = Router::new().route(
        "/health",
        get(move || {
            let target = redirect_location.clone();
            async move {
                (
                    StatusCode::FOUND,
                    [(axum::http::header::LOCATION, target)],
                    "Redirecting",
                )
            }
        }),
    );
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener_a, app_a).await.unwrap();
    });

    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("redirect_security.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    // Submit credential with a canary secret
    let cred = app_ctx
        .submit_credential(&workspace_id, "Canary Key", "canary-secret-to-never-leak")
        .await
        .unwrap();

    // Create provider configured with endpoint pointing to Server A and bound to credential
    let provider = app_ctx
        .create_provider(
            &workspace_id,
            "Redirecting Provider",
            "openai",
            Some(&cred.id),
        )
        .await
        .unwrap();

    sqlx::query("UPDATE provider_connections SET endpoint = ? WHERE id = ?")
        .bind(format!("http://{addr_a}/health"))
        .bind(&provider.id)
        .execute(&app_ctx.pool)
        .await
        .unwrap();

    // Probe provider health
    let result = app_ctx
        .test_provider_health(&workspace_id, &provider.id)
        .await
        .unwrap();

    // Verify requirements:
    // 1. HTTP 3xx is treated as safe UNAVAILABLE (not followed)
    assert_eq!(
        result.health, "UNAVAILABLE",
        "Redirect must result in UNAVAILABLE status without following redirect"
    );

    // 2. Server B must receive ZERO requests
    assert_eq!(
        server_b_requests.load(Ordering::SeqCst),
        0,
        "Server B must receive NO follow-up requests"
    );

    // 3. Header/secret leak count must be exactly 0
    assert_eq!(
        server_b_leak_count.load(Ordering::SeqCst),
        0,
        "Zero Authorization/API-key/secret headers leaked to redirect target"
    );
}

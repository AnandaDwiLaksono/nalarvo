use axum::{
    Json, Router,
    extract::Path,
    http::StatusCode,
    routing::{get, post},
};
use nalarvo_client::*;
use nalarvo_contracts::*;
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[tokio::test]
async fn typed_m2_queries_use_scoped_routes_and_decode_envelopes() {
    let app = Router::new()
        .route("/api/v1/workspace", get(|| async { Json(json!({"id":"w","owner_user_id":"u","name":"Personal","slug":"personal","is_personal":true,"status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})) }))
        .route("/api/v1/workspace/providers", get(|| async { Json(json!({"providers":[{"id":"p","workspace_id":"w","name":"Primary","provider_kind":"openai","credential_ref_id":null,"status":"ACTIVE","health":"UNKNOWN","row_version":1,"created_at":"now","updated_at":"now"}]})) }))
        .route("/api/v1/companies/{company_id}/departments", get(|Path(id): Path<String>| async move { assert_eq!(id, "co"); Json(json!({"departments":[{"id":"d","company_id":"co","name":"Engineering","status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"}]})) }))
        .route("/api/v1/companies/{company_id}/roles", get(|| async { Json(json!({"roles":[]})) }))
        .route("/api/v1/companies/{company_id}/agents", get(|| async { Json(json!({"agents":[{"id":"a","company_id":"co","name":"Ada","primary_department_id":"d","role_id":"r","model_profile_id":null,"capacity":2,"status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"}]})) }))
        .route("/api/v1/companies/{company_id}/agents/{agent_id}", get(|Path((company, agent)): Path<(String,String)>| async move { assert_eq!((company.as_str(), agent.as_str()), ("co", "a")); Json(json!({"id":"a","company_id":"co","name":"Ada","primary_department_id":"d","role_id":"r","model_profile_id":null,"capacity":2,"status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})) }))
        .route("/api/v1/workspace/providers/{id}", post(|Path(id): Path<String>| async move { assert_eq!(id, "p:test"); Json(json!({"provider":{"id":"p","workspace_id":"w","name":"Primary","provider_kind":"openai","credential_ref_id":null,"status":"ACTIVE","health":"HEALTHY","row_version":1,"created_at":"now","updated_at":"now"},"healthy":true,"message":null})) }))
        .route("/api/v1/companies/{action}", post(|Path(action): Path<String>, Json(body): Json<Value>| async move { assert_eq!(action, "co:activate"); assert_eq!(body, json!({"expected_version":1})); Json(json!({"id":"co","workspace_id":"w","name":"Company","description":null,"status":"ACTIVE","row_version":2,"created_at":"now","updated_at":"now"})) }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    assert_eq!(get_workspace(&base, "token").await.unwrap().id, "w");
    assert_eq!(
        list_providers(&base, "token").await.unwrap().providers[0].id,
        "p"
    );
    assert_eq!(
        list_departments(&base, "token", "co")
            .await
            .unwrap()
            .departments
            .len(),
        1
    );
    assert!(
        list_roles(&base, "token", "co")
            .await
            .unwrap()
            .roles
            .is_empty()
    );
    assert_eq!(
        list_agents(&base, "token", "co")
            .await
            .unwrap()
            .agents
            .len(),
        1
    );
    assert_eq!(
        get_agent(&base, "token", "co", "a").await.unwrap().capacity,
        2
    );
    assert_eq!(
        test_provider(&base, "token", "p")
            .await
            .unwrap()
            .provider
            .health,
        "HEALTHY"
    );
    assert_eq!(
        activate_company(
            &base,
            "token",
            "co",
            &CompanyLifecycleRequest {
                expected_version: 1
            }
        )
        .await
        .unwrap()
        .row_version,
        2
    );
}

#[tokio::test]
async fn provider_create_and_secret_submission_never_echo_secret_in_error() {
    let app = Router::new()
        .route("/api/v1/workspace/providers", post(|Json(body): Json<Value>| async move { assert_eq!(body, json!({"name":"Primary","provider_kind":"openai","credential_ref_id":null})); Json(json!({"id":"p","workspace_id":"w","name":"Primary","provider_kind":"openai","credential_ref_id":null,"status":"ACTIVE","health":"UNKNOWN","row_version":1,"created_at":"now","updated_at":"now"})) }))
        .route("/api/v1/workspace/credentials", post(|Json(body): Json<Value>| async move { assert_eq!(body["secret"], "canary-secret-do-not-log"); (StatusCode::BAD_REQUEST, Json(json!({"error":{"code":"VALIDATION_FAILED","message":"invalid credential","details":null}}))) }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = create_provider(
        &base,
        "token",
        &CreateProviderConnectionRequest {
            name: "Primary".into(),
            provider_kind: "openai".into(),
            credential_ref_id: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(provider.id, "p");
    let error = submit_credential(
        &base,
        "token",
        &SubmitCredentialRequest::new("Key".into(), "canary-secret-do-not-log".into()),
    )
    .await
    .unwrap_err();
    assert!(!format!("{error:?}").contains("canary-secret-do-not-log"));
    assert!(!format!("{error}").contains("canary-secret-do-not-log"));
}

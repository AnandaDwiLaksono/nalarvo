use nalarvo_application::{
    ApplicationContext, CredentialBrokerRequest, SecretStore, SecretStoreError, SecretValue,
};
use nalarvo_domain::{PrincipalRef, WorkspaceId};
use nalarvo_persistence::run_migrations;
use sqlx::SqlitePool;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const WORKSPACE: &str = "0191e4b8-0002-7000-8000-000000000001";

#[derive(Clone)]
struct CountingSecretStore {
    gets: Arc<AtomicUsize>,
}

impl SecretStore for CountingSecretStore {
    fn get(
        &self,
        credential: &nalarvo_application::CredentialRef,
    ) -> Result<SecretValue, SecretStoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if credential.id == "cred_1" {
            Ok(SecretValue::new("top-secret"))
        } else {
            Err(SecretStoreError::NotFound)
        }
    }
    fn put(
        &self,
        _: &nalarvo_application::CredentialRef,
        _: &SecretValue,
    ) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &nalarvo_application::CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}

async fn setup() -> (SqlitePool, Arc<AtomicUsize>) {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let user = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let workspace = WorkspaceId(WORKSPACE.into());
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &user,
        &workspace,
        "test@example.com",
        "Test Owner",
    )
    .await
    .unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO companies (id, workspace_id, name, status, row_version, created_at, updated_at) VALUES ('comp_1', ?, 'Company', 'ACTIVE', 1, ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO credential_refs (id, workspace_id, name, secret_locator, status, row_version, created_at, updated_at) VALUES ('cred_1', ?, 'API', 'secretstore://cred_1', 'ACTIVE', 1, ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO companies (id, workspace_id, name, status, row_version, created_at, updated_at) VALUES ('comp_2', ?, 'Other Company', 'ACTIVE', 1, ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO provider_connections (id, workspace_id, credential_ref_id, name, provider_kind, created_at, updated_at) VALUES ('provider_1', ?, 'cred_1', 'Provider', 'test', ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO models (id, workspace_id, provider_connection_id, model_key, created_at, updated_at) VALUES ('model_1', ?, 'provider_1', 'test', ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO model_profiles (id, workspace_id, name, status, row_version, created_at, updated_at) VALUES ('profile_1', ?, 'Profile', 'ACTIVE', 1, ?, ?)")
        .bind(WORKSPACE).bind(&now).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO model_profile_versions (profile_id, workspace_id, version, model_id, created_at) VALUES ('profile_1', ?, 1, 'model_1', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("UPDATE model_profiles SET current_version = 1 WHERE id = 'profile_1'")
        .execute(&pool)
        .await
        .unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    (pool, gets)
}

fn request() -> CredentialBrokerRequest {
    CredentialBrokerRequest {
        workspace_id: WORKSPACE.into(),
        company_id: "comp_1".into(),
        principal: PrincipalRef::agent("agent_1"),
        credential_ref_id: "cred_1".into(),
        model_profile_id: "profile_1".into(),
        action: "credential.use".into(),
        tool: "provider.call".into(),
    }
}

#[tokio::test]
async fn credential_broker_rejects_before_secret_store_without_effective_grants() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();

    let result = app.credential_broker().resolve_for_core(request()).await;
    assert!(result.is_err());
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn credential_broker_resolves_only_after_resource_and_credential_use_checks() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO company_workspace_resource_grants (company_id, workspace_id, model_profile_id, created_at) VALUES ('comp_1', ?, 'profile_1', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) VALUES ('grant_1', ?, 'comp_1', 'pd_credential.use', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();

    for (table, value, restore) in [
        ("workspaces", "SUSPENDED", "ACTIVE"),
        ("companies", "PAUSED", "ACTIVE"),
        ("provider_connections", "DISABLED", "ENABLED"),
        ("model_profiles", "RETIRED", "ACTIVE"),
        ("models", "RETIRED", "ACTIVE"),
    ] {
        sqlx::query(&format!("UPDATE {table} SET status='{value}'"))
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            app.credential_broker()
                .resolve_for_core(request())
                .await
                .is_err(),
            "{table}"
        );
        assert_eq!(gets.load(Ordering::SeqCst), 0, "{table}");
        sqlx::query(&format!("UPDATE {table} SET status='{restore}'"))
            .execute(&pool)
            .await
            .unwrap();
    }
    let secret = app
        .credential_broker()
        .resolve_for_core(request())
        .await
        .unwrap();
    assert_eq!(secret.expose(), b"top-secret");
    assert_eq!(gets.load(Ordering::SeqCst), 1);

    let seam_output = app
        .credential_broker()
        .dispatch_core_tool_test_seam(request())
        .await
        .unwrap();
    assert!(seam_output);
}

#[tokio::test]
async fn credential_broker_rejects_inactive_credential_status() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO company_workspace_resource_grants (company_id, workspace_id, model_profile_id, created_at) VALUES ('comp_1', ?, 'profile_1', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) VALUES ('grant_1', ?, 'comp_1', 'pd_credential.use', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();

    sqlx::query("UPDATE credential_refs SET status = 'REVOKED' WHERE id = 'cred_1'")
        .execute(&pool)
        .await
        .unwrap();

    let result = app.credential_broker().resolve_for_core(request()).await;
    assert!(result.is_err());
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn credential_broker_rejects_wrong_company_before_secret_store() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO company_workspace_resource_grants (company_id, workspace_id, model_profile_id, created_at) VALUES ('comp_1', ?, 'profile_1', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();
    let mut req = request();
    req.company_id = "comp_2".into();
    assert!(app.credential_broker().resolve_for_core(req).await.is_err());
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn credential_broker_rejects_without_credential_use_permission() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO company_workspace_resource_grants (company_id, workspace_id, model_profile_id, created_at) VALUES ('comp_1', ?, 'profile_1', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();
    assert!(
        app.credential_broker()
            .resolve_for_core(request())
            .await
            .is_err()
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn credential_broker_rejects_when_resource_grant_missing() {
    let (pool, gets) = setup().await;
    let store = Arc::new(CountingSecretStore { gets: gets.clone() });
    let app = ApplicationContext::new(pool.clone()).with_secret_store(store);
    app.ensure_canonical_permissions(WORKSPACE).await.unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) VALUES ('grant_1', ?, 'comp_1', 'pd_credential.use', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', ?)")
        .bind(WORKSPACE).bind(&now).execute(&pool).await.unwrap();

    let result = app.credential_broker().resolve_for_core(request()).await;
    assert!(result.is_err());
    assert_eq!(gets.load(Ordering::SeqCst), 0);
}

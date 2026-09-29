use nalarvo_application::{
    ApplicationContext, CredentialRef, FakeSecretStore, SecretStore, SecretValue,
};
use nalarvo_domain::WorkspaceId;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_secret_canary_absence_across_all_persisted_and_emitted_surfaces() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("canary_test.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let secret_store = Arc::new(FakeSecretStore::default());

    let canary_secret = "CANARY_SYNTHETIC_SECRET_987654321_DO_NOT_LOG";

    // 1. Submit secret to SecretStore
    let cred_ref = app_ctx
        .submit_credential(&workspace_id, "TestKey", canary_secret)
        .await
        .unwrap();
    let _ = secret_store.put(
        &CredentialRef::new(&cred_ref.id),
        &SecretValue::new(canary_secret),
    );

    // 2. Create provider referencing credential
    let provider = app_ctx
        .create_provider(&workspace_id, "TestProvider", "openai", Some(&cred_ref.id))
        .await
        .unwrap();

    // 3. Inspect Debug and string representations of records
    let provider_str = format!("{provider:?}");
    let cred_str = format!("{cred_ref:?}");

    assert!(!provider_str.contains(canary_secret));
    assert!(!cred_str.contains(canary_secret));
    assert!(!cred_ref.name.contains(canary_secret));
    assert!(!cred_ref.id.contains(canary_secret));
    assert!(!provider.name.contains(canary_secret));
    assert!(!provider.id.contains(canary_secret));

    // 4. Inspect raw SQLite database content on disk
    let raw_db_bytes = std::fs::read(&db_path).unwrap();
    let raw_db_string = String::from_utf8_lossy(&raw_db_bytes);
    assert!(
        !raw_db_string.contains(canary_secret),
        "Secret canary leaked into SQLite disk database!"
    );

    // 5. Query domain events and workspace domain events from database
    let event_rows: Vec<(String,)> = sqlx::query_as("SELECT payload FROM domain_events")
        .fetch_all(&app_ctx.pool)
        .await
        .unwrap();
    for (payload,) in event_rows {
        assert!(
            !payload.contains(canary_secret),
            "Secret canary leaked into domain events payload!"
        );
    }

    let ws_event_rows: Vec<(String,)> =
        sqlx::query_as("SELECT payload FROM workspace_domain_events")
            .fetch_all(&app_ctx.pool)
            .await
            .unwrap();
    for (payload,) in ws_event_rows {
        assert!(
            !payload.contains(canary_secret),
            "Secret canary leaked into workspace domain events payload!"
        );
    }
}

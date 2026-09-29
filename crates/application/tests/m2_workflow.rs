use nalarvo_application::ApplicationContext;
use sqlx::Row;

#[tokio::test]
async fn workspace_query_is_owner_scoped() {
    let ctx = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let owner = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let workspace = nalarvo_domain::WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let record = ctx.get_workspace(&owner, &workspace).await.unwrap();
    assert_eq!(record.status, "ACTIVE");
    let stranger = nalarvo_domain::UserId::new();
    assert!(ctx.get_workspace(&stranger, &workspace).await.is_err());
}

#[tokio::test]
async fn repeated_first_run_provisions_once_and_preserves_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    let first = ApplicationContext::init(&db).await.unwrap();
    let before: (String, String) =
        sqlx::query_as("SELECT w.id, w.owner_user_id FROM workspaces w WHERE w.is_personal = 1")
            .fetch_one(&first.pool)
            .await
            .unwrap();
    assert_eq!(
        sqlx::query(
            "SELECT id FROM workspace_domain_events WHERE event_type = 'WorkspaceProvisioned'"
        )
        .fetch_all(&first.pool)
        .await
        .unwrap()
        .len(),
        1
    );
    drop(first);
    let second = ApplicationContext::init(&db).await.unwrap();
    let after: (String, String) =
        sqlx::query_as("SELECT w.id, w.owner_user_id FROM workspaces w WHERE w.is_personal = 1")
            .fetch_one(&second.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        sqlx::query("SELECT id FROM workspaces WHERE is_personal = 1")
            .fetch_all(&second.pool)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sqlx::query("SELECT workspace_id FROM workspace_memberships")
            .fetch_all(&second.pool)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sqlx::query(
            "SELECT id FROM workspace_domain_events WHERE event_type = 'WorkspaceProvisioned'"
        )
        .fetch_all(&second.pool)
        .await
        .unwrap()
        .len(),
        1
    );
    let row = sqlx::query("SELECT status FROM workspaces WHERE id = ?")
        .bind(&after.0)
        .fetch_one(&second.pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "ACTIVE");
}

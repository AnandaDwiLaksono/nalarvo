use nalarvo_domain::{Company, DomainEvent, PrincipalRef, UserId, WorkspaceId};
use nalarvo_persistence::*;
use sqlx::Row;

#[tokio::test]
async fn test_sqlite_pragmas_and_migrations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pragmas.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let pool = create_pool(&db_url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    // Check foreign keys
    let fk_row = sqlx::query("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    let fk: i64 = fk_row.get(0);
    assert_eq!(fk, 1, "Foreign keys must be ON");

    // Check journal mode is WAL
    let jm_row = sqlx::query("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    let jm: String = jm_row.get(0);
    assert_eq!(jm.to_uppercase(), "WAL", "Journal mode must be WAL");

    // Check busy timeout
    let bt_row = sqlx::query("PRAGMA busy_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    let bt: i64 = bt_row.get(0);
    assert_eq!(bt, 5000, "Busy timeout must be configured");

    // Check synchronous mode
    let syn_row = sqlx::query("PRAGMA synchronous")
        .fetch_one(&pool)
        .await
        .unwrap();
    let syn: i64 = syn_row.get(0);
    assert_eq!(syn, 1, "Synchronous mode must be NORMAL (1)");
}

#[tokio::test]
async fn test_workspace_and_company_foreign_key_constraints() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_fk.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let pool = create_pool(&db_url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    let user_id = UserId::new();
    let workspace_id = WorkspaceId::new();

    // Inserting company before workspace exists must fail FK constraint
    let company = Company::create(workspace_id.clone(), "Orphan Co".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    let err = insert_company_tx(&mut tx, &company).await.unwrap_err();
    assert!(matches!(err, PersistenceError::Database(_)));
    tx.rollback().await.unwrap();

    // Bootstrap workspace
    bootstrap_personal_workspace(
        &pool,
        &user_id,
        &workspace_id,
        "user@test.local",
        "Test User",
    )
    .await
    .unwrap();

    // Now inserting company succeeds
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &company).await.unwrap();
    tx.commit().await.unwrap();

    let found = get_company(&pool, &workspace_id, &company.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.name, "Orphan Co");
    assert_eq!(found.row_version, 1);
}

#[tokio::test]
async fn test_atomic_state_event_outbox_and_rollback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_atomic.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let pool = create_pool(&db_url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    let user_id = UserId::new();
    let workspace_id = WorkspaceId::new();
    bootstrap_personal_workspace(
        &pool,
        &user_id,
        &workspace_id,
        "user@test.local",
        "Test User",
    )
    .await
    .unwrap();

    let company = Company::create(workspace_id.clone(), "Atomic Co".into(), None).unwrap();
    let event =
        DomainEvent::company_created(&company, PrincipalRef::user(&user_id.0), "corr-1", "caus-1");

    // 1. Rollback test: insert and rollback
    {
        let mut tx = pool.begin().await.unwrap();
        insert_company_tx(&mut tx, &company).await.unwrap();
        insert_domain_event_and_outbox_tx(&mut tx, &event)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    // Verify nothing persisted
    let found = get_company(&pool, &workspace_id, &company.id)
        .await
        .unwrap();
    assert!(found.is_none());

    let events = sqlx::query("SELECT count(*) FROM domain_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(events.get::<i64, _>(0), 0);

    let outbox = sqlx::query("SELECT count(*) FROM outbox_messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outbox.get::<i64, _>(0), 0);

    // 2. Commit test: insert and commit
    {
        let mut tx = pool.begin().await.unwrap();
        insert_company_tx(&mut tx, &company).await.unwrap();
        insert_domain_event_and_outbox_tx(&mut tx, &event)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    // Verify all 3 records committed atomically
    let found = get_company(&pool, &workspace_id, &company.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, company.id);

    let pending = fetch_pending_outbox(&pool, 10, "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1.event_type, "CompanyCreated");
    assert_eq!(pending[0].1.company_id, company.id);
}

#[tokio::test]
async fn test_optimistic_concurrency_stale_rejection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_occ.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let pool = create_pool(&db_url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    let user_id = UserId::new();
    let workspace_id = WorkspaceId::new();
    bootstrap_personal_workspace(
        &pool,
        &user_id,
        &workspace_id,
        "user@test.local",
        "Test User",
    )
    .await
    .unwrap();

    let company = Company::create(workspace_id.clone(), "OCC Co".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &company).await.unwrap();
    tx.commit().await.unwrap();

    // Stale version (expected version 0 instead of 1)
    let mut stale = company.clone();
    stale.name = "Updated Name".into();
    let mut tx = pool.begin().await.unwrap();
    let err = update_company_tx(&mut tx, &stale, 0).await.unwrap_err();
    assert!(matches!(
        err,
        PersistenceError::StaleVersion {
            current: 1,
            expected: 0
        }
    ));
    tx.rollback().await.unwrap();

    // Valid update with expected version 1
    let mut valid = company.clone();
    valid.name = "Updated Name".into();
    let mut tx = pool.begin().await.unwrap();
    update_company_tx(&mut tx, &valid, 1).await.unwrap();
    tx.commit().await.unwrap();

    let found = get_company(&pool, &workspace_id, &company.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.name, "Updated Name");
    assert_eq!(found.row_version, 2);
}

#[tokio::test]
async fn test_consumer_inbox_deduplication() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_inbox.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let pool = create_pool(&db_url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    let event_id = "event-0191e4b8";
    let consumer = "projection_builder_1";

    // First processing
    let first = consumer_inbox_dedup(&pool, consumer, event_id)
        .await
        .unwrap();
    assert!(first, "First event processing must succeed");

    // Second processing (duplicate)
    let second = consumer_inbox_dedup(&pool, consumer, event_id)
        .await
        .unwrap();
    assert!(!second, "Duplicate event delivery must be deduped (false)");
}

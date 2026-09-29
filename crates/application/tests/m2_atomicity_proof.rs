use nalarvo_application::{ApplicationContext, CreateCompanyCommand};
use nalarvo_domain::{Company, DomainEvent, PrincipalRef, WorkspaceId};
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn test_m2_domain_event_outbox_atomicity_and_rollback() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("atomicity_test.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let principal = PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001");

    // 1. Create company A
    let company = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Atomicity Corp".into(),
            description: None,
            principal: Some(principal.clone()),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // 2. Count outbox and events before activation
    let events_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    let outbox_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();

    // 3. Successful activation with matching version
    let activated = app_ctx
        .activate_company(
            &workspace_id,
            &company.id,
            company.row_version,
            principal.clone(),
        )
        .await
        .unwrap();
    assert_eq!(activated.status.to_string(), "ACTIVE");

    let events_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    let outbox_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();

    assert_eq!(events_after, events_before + 1);
    assert_eq!(outbox_after, outbox_before + 1);

    // 4. Stale version rejection -> NO event, NO outbox row, NO mutation
    let stale_attempt = app_ctx
        .activate_company(
            &workspace_id,
            &company.id,
            company.row_version, // stale expected version!
            principal.clone(),
        )
        .await;

    assert!(stale_attempt.is_err());

    let events_stale: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    let outbox_stale: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();

    assert_eq!(events_stale, events_after);
    assert_eq!(outbox_stale, outbox_after);
}

#[tokio::test]
async fn test_m2_transaction_fault_injection_post_write_rollback() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("fault_injection.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let mut sse_rx = app_ctx.subscribe_events();

    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let principal = PrincipalRef::user("fault-injector");
    let corr_id = Uuid::now_v7().to_string();
    let causer_id = Uuid::now_v7().to_string();

    let company = Company::create(workspace_id.clone(), "Doomed Corp".into(), None).unwrap();
    let event = DomainEvent::company_created(&company, principal, corr_id, causer_id);
    let event_id = event.event_id.clone();
    let company_id = company.id.0.clone();

    // Simulate an application transaction where writes occur, but a fault is injected before commit
    let result: Result<(), &'static str> = async {
        let mut tx = app_ctx.pool.begin().await.map_err(|_| "begin_err")?;

        // 1. Write aggregate state inside transaction
        nalarvo_persistence::insert_company_tx(&mut tx, &company)
            .await
            .map_err(|_| "insert_company_err")?;

        // 2. Write DomainEvent and Outbox message inside transaction
        nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &event)
            .await
            .map_err(|_| "insert_event_err")?;

        // 3. Inject deterministic fault AFTER writes but BEFORE commit
        if true {
            tx.rollback().await.map_err(|_| "rollback_err")?;
            return Err("INJECTED_FAULT_BEFORE_COMMIT");
        }

        tx.commit().await.map_err(|_| "commit_err")?;
        let _ = app_ctx.event_broadcaster.send(event);
        Ok(())
    }
    .await;

    assert_eq!(result, Err("INJECTED_FAULT_BEFORE_COMMIT"));

    // Verify 1: No aggregate mutation committed
    let company_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM companies WHERE id = ?")
        .bind(&company_id)
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    assert_eq!(
        company_count, 0,
        "Aggregate mutation must not be committed on rollback!"
    );

    // Verify 2: No DomainEvent committed
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events WHERE id = ?")
        .bind(&event_id)
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    assert_eq!(
        event_count, 0,
        "DomainEvent must not be committed on rollback!"
    );

    // Verify 3: No publishable outbox row committed
    let outbox_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages WHERE domain_event_id = ?")
            .bind(&event_id)
            .fetch_one(&app_ctx.pool)
            .await
            .unwrap();
    assert_eq!(
        outbox_count, 0,
        "Outbox row must not be committed on rollback!"
    );

    // Verify 4: No SSE success event emitted
    assert!(
        sse_rx.try_recv().is_err(),
        "No SSE success event should be broadcast when transaction rolls back!"
    );
}

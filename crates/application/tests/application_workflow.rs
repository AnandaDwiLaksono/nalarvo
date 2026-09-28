use nalarvo_application::*;
use nalarvo_domain::{Company, DomainEvent, PrincipalRef, WorkspaceId};
use nalarvo_persistence::{insert_company_tx, insert_domain_event_and_outbox_tx};
use std::sync::Arc;

#[tokio::test]
async fn test_idempotent_company_creation_and_payload_mismatch_rejection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_idempotency.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    let cmd1 = CreateCompanyCommand {
        workspace_id: workspace_id.clone(),
        name: "Acme Corp".into(),
        description: Some("Building gadgets".into()),
        principal: None,
        idempotency_key: Some("idem-key-100".into()),
        correlation_id: Some("corr-100".into()),
        causation_id: Some("caus-100".into()),
    };

    // First call creates company
    let c1 = app_ctx.create_company(cmd1.clone()).await.unwrap();
    assert_eq!(c1.name, "Acme Corp");

    // Second call with same key and same payload returns cached company
    let c2 = app_ctx.create_company(cmd1).await.unwrap();
    assert_eq!(c2.id, c1.id);
    assert_eq!(c2.created_at, c1.created_at);

    // Verify only one Company row exists in DB
    let list = app_ctx.list_companies(&workspace_id).await.unwrap();
    assert_eq!(list.len(), 1);

    // Call with SAME idempotency key but DIFFERENT payload must fail with IdempotencyKeyReuseMismatch
    let cmd_mismatch = CreateCompanyCommand {
        workspace_id: workspace_id.clone(),
        name: "Different Corp Name".into(),
        description: Some("Different desc".into()),
        principal: None,
        idempotency_key: Some("idem-key-100".into()),
        correlation_id: None,
        causation_id: None,
    };

    let err = app_ctx.create_company(cmd_mismatch).await.unwrap_err();
    assert!(matches!(
        err,
        ApplicationError::IdempotencyKeyReuseMismatch(_)
    ));
}

#[tokio::test]
async fn test_idempotency_cross_command_key_reuse() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_cross_command_idem.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    let shared_key = "shared-idempotency-key-555";

    // 1. Create company using shared idempotency key
    let company = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Base Corp".into(),
            description: None,
            principal: None,
            idempotency_key: Some(shared_key.into()),
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // 2. Execute UpdateCompanyMetadata using the SAME key value
    let updated = app_ctx
        .update_company_metadata(UpdateCompanyMetadataCommand {
            workspace_id: workspace_id.clone(),
            company_id: company.id.clone(),
            name: "Updated Corp Name".into(),
            description: Some("Added description".into()),
            expected_version: 1,
            principal: None,
            idempotency_key: Some(shared_key.into()),
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // Must not alias to CreateCompany response; must successfully return updated company (v2)
    assert_eq!(updated.id, company.id);
    assert_eq!(updated.name, "Updated Corp Name");
    assert_eq!(updated.row_version, 2);
}

#[tokio::test]
async fn test_company_isolation_scope_enforcement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_isolation.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let ws_a = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let ws_b = WorkspaceId("0191e4b8-0003-7000-8000-000000000002".into());

    let company_a = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: ws_a.clone(),
            name: "Company A".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    let err = app_ctx.get_company(&ws_b, &company_a.id).await.unwrap_err();
    assert!(matches!(err, ApplicationError::NotFound(_)));

    let list_b = app_ctx.list_companies(&ws_b).await.unwrap();
    assert!(list_b.is_empty());
}

#[tokio::test]
async fn test_audit_sink_receives_structured_event_without_secrets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_audit.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let audit_sink = Arc::new(InMemoryAuditSink::new());
    let app_ctx = ApplicationContext::with_audit_sink(app_ctx.pool, audit_sink.clone());
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id,
            name: "Audited Corp".into(),
            description: None,
            principal: Some(PrincipalRef::user("u-admin-1")),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    let records = audit_sink.records();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.action, "CompanyCreated");
    assert_eq!(r.principal.principal_id, "u-admin-1");

    // Ensure no secrets or tokens present in audit output string
    let json_str = serde_json::to_string(r).unwrap();
    assert!(!json_str.contains("bearer"));
    assert!(!json_str.contains("secret"));
}

#[tokio::test]
async fn test_outbox_progresses_with_zero_sse_subscribers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_zero_sse.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    // ZERO SSE subscribers connected (subscribe_events() is NOT called)
    let company = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id,
            name: "Zero SSE Co".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // Dispatcher processes outbox
    let dispatched = app_ctx
        .dispatch_outbox_batch(10, "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(dispatched, 1);

    // Further dispatch yields 0 pending rows (outbox reaches published state)
    let dispatched_second = app_ctx
        .dispatch_outbox_batch(10, "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(dispatched_second, 0);

    let loaded = app_ctx
        .get_company(&company.workspace_id, &company.id)
        .await
        .unwrap();
    assert_eq!(loaded.name, "Zero SSE Co");
}

#[tokio::test]
async fn test_pending_outbox_dispatched_after_daemon_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pending_restart.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx1 = ApplicationContext::init(&db_url).await.unwrap();
    let ws_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let company = Company::create(ws_id.clone(), "Un-dispatched Co".into(), None).unwrap();
    let event = DomainEvent::company_created(&company, PrincipalRef::user("u-1"), "c-1", "c-1");

    // Commit company + event + PENDING outbox without running dispatcher
    let mut tx = app_ctx1.pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &company).await.unwrap();
    insert_domain_event_and_outbox_tx(&mut tx, &event)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Simulate daemon termination before dispatch
    drop(app_ctx1);

    // Recreate ApplicationContext (simulating daemon restart)
    let app_ctx2 = ApplicationContext::init(&db_url).await.unwrap();
    let mut rx2 = app_ctx2.subscribe_events();

    // Dispatcher runs post-restart
    let dispatched = app_ctx2
        .dispatch_outbox_batch(10, "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(dispatched, 1);

    // Event is received post-restart
    let received_event = rx2.recv().await.unwrap();
    assert_eq!(received_event.company_id, company.id);

    // Verify company count remains exactly 1 (no duplicates)
    let list = app_ctx2.list_companies(&ws_id).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, company.id);
}

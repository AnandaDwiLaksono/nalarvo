use chrono::{TimeZone, Utc};
use nalarvo_domain::{AuditRecord, PrincipalRef, ScopeRef};
use nalarvo_persistence::{
    create_pool,
    m5::{
        ActionRecord, ApprovalDecisionRecord, ApprovalRequestRecord, BudgetReservation,
        cancel_approval, create_action_with_approval, decide_approval, expire_approval,
        insert_audit_record_tx, reserve_budget, start_action, succeed_action,
    },
};
use sqlx::Row;

async fn setup() -> sqlx::SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for query in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,created_at,updated_at) VALUES ('b','w','c','B',10,'t','t')",
        "INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount) VALUES ('l','b','TOKENS',5)",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    pool
}

#[tokio::test]
async fn action_approval_and_decision_are_persisted_atomically() {
    let pool = setup().await;
    let action = ActionRecord {
        id: "a".into(),
        workspace_id: "w".into(),
        company_id: "c".into(),
        run_id: None,
        step_id: None,
        requested_by_type: "USER".into(),
        requested_by_id: "u".into(),
        action_type: "TOOL".into(),
        target: "deploy".into(),
        input: None,
        authorization: "APPROVAL_REQUIRED".into(),
        status: "PENDING".into(),
        idempotency_key: Some("key".into()),
        created_at: "t".into(),
        updated_at: "t".into(),
    };
    let approval = ApprovalRequestRecord {
        id: "ar".into(),
        action_id: "a".into(),
        workspace_id: "w".into(),
        company_id: "c".into(),
        requested_by_id: "u".into(),
        status: "PENDING".into(),
        reason: None,
        requested_at: "t".into(),
    };
    sqlx::query("CREATE TRIGGER fail_requested BEFORE INSERT ON outbox_messages WHEN (SELECT event_type FROM domain_events WHERE id=NEW.domain_event_id)='ApprovalRequested' BEGIN SELECT RAISE(ABORT,'forced requested outbox fault'); END")
        .execute(&pool).await.unwrap();
    assert!(
        create_action_with_approval(&pool, &action, &approval)
            .await
            .is_err()
    );
    for table in [
        "actions",
        "approval_requests",
        "domain_events",
        "outbox_messages",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    sqlx::query("DROP TRIGGER fail_requested")
        .execute(&pool)
        .await
        .unwrap();
    create_action_with_approval(&pool, &action, &approval)
        .await
        .unwrap();
    assert!(
        create_action_with_approval(&pool, &action, &approval)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM approval_requests")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE event_type='ApprovalRequested'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    decide_approval(
        &pool,
        &ApprovalDecisionRecord {
            id: "ad".into(),
            approval_request_id: "ar".into(),
            decided_by_type: "USER".into(),
            decided_by_id: "v".into(),
            decision: "APPROVED".into(),
            reason: None,
            decided_at: "t".into(),
        },
    )
    .await
    .unwrap();
    let row = sqlx::query("SELECT status, decided_at FROM approval_requests WHERE id='ar'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "APPROVED");
    assert_eq!(row.get::<Option<String>, _>(1).as_deref(), Some("t"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM approval_decisions WHERE approval_request_id='ar'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    // Duplicate decide_approval must fail
    let dup_res = decide_approval(
        &pool,
        &ApprovalDecisionRecord {
            id: "ad2".into(),
            approval_request_id: "ar".into(),
            decided_by_type: "USER".into(),
            decided_by_id: "v".into(),
            decision: "APPROVED".into(),
            reason: None,
            decided_at: "t".into(),
        },
    )
    .await;
    assert!(dup_res.is_err());

    // Verify domain events and outbox messages
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT e.event_type FROM domain_events e JOIN outbox_messages o ON o.domain_event_id = e.id ORDER BY e.rowid"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        events,
        vec![
            "ActionProposed",
            "ActionApprovalRequired",
            "ApprovalRequested",
            "ApprovalGranted",
            "ActionApproved"
        ]
    );
}

#[tokio::test]
async fn budget_reservation_checks_total_and_resource_limit_atomically() {
    let pool = setup().await;
    let reservation = |id: &str, amount| BudgetReservation {
        id: id.into(),
        budget_id: "b".into(),
        action_id: None,
        workspace_id: "w".into(),
        company_id: "c".into(),
        resource_type: "TOKENS".into(),
        amount,
        expires_at: None,
        created_at: "t".into(),
    };
    assert!(
        reserve_budget(&pool, &reservation("r1", 4.0))
            .await
            .unwrap()
    );
    assert!(
        !reserve_budget(&pool, &reservation("r2", 2.0))
            .await
            .unwrap()
    );
    assert!(
        !reserve_budget(&pool, &reservation("r3", 2.0))
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_reservations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let row = sqlx::query("SELECT b.reserved_amount, l.reserved_amount FROM budgets b JOIN budget_limits l ON l.budget_id=b.id WHERE b.id='b'").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<f64, _>(0), 4.0);
    assert_eq!(row.get::<f64, _>(1), 4.0);

    let budget_events: Vec<String> = sqlx::query_scalar(
        "SELECT e.event_type FROM domain_events e JOIN outbox_messages o ON o.domain_event_id = e.id WHERE e.aggregate_type = 'Budget' ORDER BY e.rowid"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(budget_events, vec!["BudgetExhausted"]);
}

#[tokio::test]
async fn audit_insert_commits_only_with_its_surrounding_transaction() {
    let pool = setup().await;
    let record = AuditRecord {
        audit_id: "audit-1".into(),
        action: "credential.accessed".into(),
        principal: PrincipalRef::user("u"),
        scope: ScopeRef::company("c"),
        occurred_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
        details: serde_json::json!({"credential_id": "cred-1"}),
    };

    let mut rolled_back = pool.begin().await.unwrap();
    insert_audit_record_tx(&mut rolled_back, &record)
        .await
        .unwrap();
    rolled_back.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    let mut committed = pool.begin().await.unwrap();
    insert_audit_record_tx(&mut committed, &record)
        .await
        .unwrap();
    committed.commit().await.unwrap();
    let row = sqlx::query("SELECT action, principal_type, principal_id, scope_type, scope_id, occurred_at, details FROM audit_records WHERE audit_id = ?")
        .bind(&record.audit_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), record.action);
    assert_eq!(row.get::<String, _>(1), "USER");
    assert_eq!(row.get::<String, _>(2), "u");
    assert_eq!(row.get::<String, _>(3), "COMPANY");
    assert_eq!(row.get::<String, _>(4), "c");
    assert_eq!(row.get::<String, _>(5), record.occurred_at.to_rfc3339());
    assert_eq!(row.get::<String, _>(6), record.details.to_string());
}

#[tokio::test]
async fn action_execution_persistence_lifecycle() {
    let pool = setup().await;
    let action = ActionRecord {
        id: "act-exec".into(),
        workspace_id: "w".into(),
        company_id: "c".into(),
        run_id: None,
        step_id: None,
        requested_by_type: "AGENT".into(),
        requested_by_id: "a1".into(),
        action_type: "fs.read".into(),
        target: "file.txt".into(),
        input: None,
        authorization: "ALLOWED".into(),
        status: "PENDING".into(),
        idempotency_key: None,
        created_at: "t".into(),
        updated_at: "t".into(),
    };
    sqlx::query("INSERT INTO actions (id, workspace_id, company_id, requested_by_type, requested_by_id, action_type, target, authorization, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&action.id).bind(&action.workspace_id).bind(&action.company_id).bind(&action.requested_by_type).bind(&action.requested_by_id).bind(&action.action_type).bind(&action.target).bind(&action.authorization).bind(&action.status).bind(&action.created_at).bind(&action.updated_at)
        .execute(&pool).await.unwrap();

    start_action(&pool, "c", "act-exec", PrincipalRef::agent("a1"))
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM actions WHERE id = 'act-exec'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "STARTED");

    succeed_action(&pool, "c", "act-exec", PrincipalRef::agent("a1"))
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM actions WHERE id = 'act-exec'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "SUCCEEDED");
}

#[tokio::test]
async fn approval_expiration_and_cancellation_persistence() {
    let pool = setup().await;
    let action = ActionRecord {
        id: "act-exp".into(),
        workspace_id: "w".into(),
        company_id: "c".into(),
        run_id: None,
        step_id: None,
        requested_by_type: "USER".into(),
        requested_by_id: "u".into(),
        action_type: "TOOL".into(),
        target: "deploy".into(),
        input: None,
        authorization: "APPROVAL_REQUIRED".into(),
        status: "PENDING".into(),
        idempotency_key: None,
        created_at: "t".into(),
        updated_at: "t".into(),
    };
    let approval = ApprovalRequestRecord {
        id: "appr-exp".into(),
        action_id: "act-exp".into(),
        workspace_id: "w".into(),
        company_id: "c".into(),
        requested_by_id: "u".into(),
        status: "PENDING".into(),
        reason: None,
        requested_at: "t".into(),
    };
    create_action_with_approval(&pool, &action, &approval)
        .await
        .unwrap();

    expire_approval(&pool, "appr-exp", Some("Timeout".into()))
        .await
        .unwrap();
    let appr_status: String =
        sqlx::query_scalar("SELECT status FROM approval_requests WHERE id = 'appr-exp'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let act_auth: String =
        sqlx::query_scalar("SELECT authorization FROM actions WHERE id = 'act-exp'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let act_status: String = sqlx::query_scalar("SELECT status FROM actions WHERE id = 'act-exp'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(appr_status, "EXPIRED");
    assert_eq!(act_auth, "EXPIRED");
    assert_eq!(act_status, "FAILED");

    // Second action to test cancel_approval
    let action2 = ActionRecord {
        id: "act-can".into(),
        ..action
    };
    let approval2 = ApprovalRequestRecord {
        id: "appr-can".into(),
        action_id: "act-can".into(),
        ..approval
    };
    create_action_with_approval(&pool, &action2, &approval2)
        .await
        .unwrap();

    cancel_approval(
        &pool,
        "appr-can",
        PrincipalRef::user("u"),
        Some("No longer needed".into()),
    )
    .await
    .unwrap();
    let appr_status2: String =
        sqlx::query_scalar("SELECT status FROM approval_requests WHERE id = 'appr-can'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let act_auth2: String =
        sqlx::query_scalar("SELECT authorization FROM actions WHERE id = 'act-can'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let act_status2: String = sqlx::query_scalar("SELECT status FROM actions WHERE id = 'act-can'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(appr_status2, "CANCELLED");
    assert_eq!(act_auth2, "CANCELLED");
    assert_eq!(act_status2, "CANCELLED");
}

use chrono::Utc;
use nalarvo_application::{
    ApplicationContext, CommandMeta, InMemoryAuditSink, ProposeActionCommand,
};
use nalarvo_domain::{AuditRecord, PrincipalRef, ScopeRef};
use std::sync::Arc;

fn record() -> AuditRecord {
    AuditRecord {
        audit_id: "audit-1".into(),
        action: "CredentialResolved".into(),
        principal: PrincipalRef::agent("agent-1"),
        scope: ScopeRef::company("company-1"),
        occurred_at: Utc::now(),
        details: serde_json::json!({"credential_id": "credential-1"}),
    }
}

#[tokio::test]
async fn m5_action_audit_is_durable_once_and_replay_adds_no_audit() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("m5.db").display());
    let app = ApplicationContext::init(&url).await.unwrap();
    let pool = app.pool.clone();
    sqlx::query("INSERT INTO companies (id, workspace_id, name, status, row_version, created_at, updated_at) VALUES ('comp_1', '0191e4b8-0002-7000-8000-000000000001', 'Company', 'ACTIVE', 1, '2026-01-01', '2026-01-01')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) SELECT 'grant-1', '0191e4b8-0002-7000-8000-000000000001', 'comp_1', id, 'AGENT', 'agent-1', 'COMPANY', 'comp_1', 'ALLOW', '2026-01-01' FROM permission_definitions WHERE name = 'filesystem.read'")
        .execute(&pool).await.unwrap();
    let diagnostics = Arc::new(InMemoryAuditSink::new());
    let app = ApplicationContext::with_audit_sink(pool.clone(), diagnostics);
    app.ensure_canonical_permissions("00000000-0000-0000-0000-000000000000")
        .await
        .unwrap();
    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("agent-1"),
        action_type: "filesystem.read".into(),
        target: "private.txt".into(),
        parameters: None,
        idempotency_key: Some("same".into()),
        meta: CommandMeta::default(),
    };
    let outcome = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(outcome.decision, nalarvo_domain::AuthorityDecision::Deny);
    app.propose_action(cmd).await.unwrap();
    let audit_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_records WHERE action = 'ActionDenied'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(audit_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn durable_record_precedes_single_diagnostic_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("audit.db").display()
    );
    let app = ApplicationContext::init(&url).await.unwrap();
    let diagnostics = Arc::new(InMemoryAuditSink::new());
    let app = ApplicationContext::with_audit_sink(app.pool, diagnostics.clone());
    let audit = record();
    app.record_audit(audit.clone()).await.unwrap();
    assert_eq!(diagnostics.records().len(), 1);
    // Failed durable writes must not emit diagnostic-only success.
    sqlx::query("CREATE TRIGGER fail_audit_test BEFORE INSERT ON audit_records BEGIN SELECT RAISE(ABORT, 'forced audit fault'); END")
        .execute(&app.pool).await.unwrap();
    let failed = AuditRecord {
        audit_id: "audit-2".into(),
        ..audit.clone()
    };
    assert!(app.record_audit(failed).await.is_err());
    assert_eq!(diagnostics.records().len(), 1);
    sqlx::query("DROP TRIGGER fail_audit_test")
        .execute(&app.pool)
        .await
        .unwrap();
    // A duplicate ID is an error, not a second diagnostic or silent overwrite.
    assert!(app.record_audit(audit.clone()).await.is_err());
    assert_eq!(diagnostics.records().len(), 1);
    app.pool.close().await;
    drop(app);

    let restarted = ApplicationContext::init(&url).await.unwrap();
    #[allow(clippy::type_complexity)]
    let rows: Vec<(String, String, String, String, String, String, String, String)> =
        sqlx::query_as("SELECT audit_id, action, principal_type, principal_id, scope_type, scope_id, occurred_at, details FROM audit_records")
            .fetch_all(&restarted.pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, audit.audit_id);
    assert_eq!(rows[0].1, audit.action);
    assert_eq!(rows[0].2, "AGENT");
    assert_eq!(rows[0].3, audit.principal.principal_id);
    assert_eq!(rows[0].4, "COMPANY");
    assert_eq!(rows[0].5, audit.scope.scope_id);
    assert_eq!(rows[0].6, audit.occurred_at.to_rfc3339());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows[0].7).unwrap(),
        audit.details
    );
    restarted.pool.close().await;
}

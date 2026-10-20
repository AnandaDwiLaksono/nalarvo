//! M5 governance integration tests.

use nalarvo_application::{ApplicationContext, CommandMeta, ProposeActionCommand};
use nalarvo_domain::{AuthorityDecision, PrincipalRef, PrincipalType};
use nalarvo_persistence::run_migrations;
use sqlx::SqlitePool;

async fn event_types(pool: &SqlitePool, action_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT e.event_type FROM domain_events e JOIN outbox_messages o ON o.domain_event_id = e.id WHERE e.aggregate_id = ? OR json_extract(e.payload, '$.action_id') = ? ORDER BY e.rowid")
        .bind(action_id)
        .bind(action_id)
        .fetch_all(pool)
        .await
        .unwrap()
}

async fn setup_db() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();

    let default_user = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let default_workspace =
        nalarvo_domain::WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &default_user,
        &default_workspace,
        "test@example.com",
        "Test Owner",
    )
    .await
    .unwrap();

    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO companies (id, workspace_id, name, status, row_version, created_at, updated_at)
         VALUES ('comp_1', '0191e4b8-0002-7000-8000-000000000001', 'Company 1', 'ACTIVE', 1, ?, ?),
                ('other_comp', '0191e4b8-0002-7000-8000-000000000001', 'Other Company', 'ACTIVE', 1, ?, ?)
         ON CONFLICT(id) DO NOTHING"
    )
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .unwrap();

    pool
}

#[tokio::test]
async fn test_propose_action_require_approval_and_decide() {
    let pool = setup_db().await;
    let app = ApplicationContext::new(pool.clone());

    // Insert permission definition
    sqlx::query(
            "INSERT INTO permission_definitions (id, workspace_id, name, resource, operation, created_at) VALUES ('pd_1', '0191e4b8-0002-7000-8000-000000000001', 'Write File', 'config.json', 'filesystem.write', '2026-01-01')",
        )
        .execute(&pool)
        .await
        .unwrap();

    // Insert policy requiring approval for file write
    sqlx::query(
            "INSERT INTO policies (id, workspace_id, company_id, name, status, current_version, created_at, updated_at) VALUES ('pol_1', '0191e4b8-0002-7000-8000-000000000001', 'comp_1', 'Write Policy', 'ACTIVE', 1, '2026-01-01', '2026-01-01')",
        )
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(
            "INSERT INTO policy_versions (id, policy_id, version, rules, created_at) VALUES ('pv_1', 'pol_1', 1, '{\"permission\": \"filesystem.write\", \"effect\": \"REQUIRE_APPROVAL\"}', '2026-01-01')",
        )
        .execute(&pool)
        .await
        .unwrap();

    // Insert grant for agent
    sqlx::query(
        "INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) VALUES ('g_1', '0191e4b8-0002-7000-8000-000000000001', 'comp_1', 'pd_1', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', '2026-01-01')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let actor = PrincipalRef {
        principal_type: PrincipalType::Agent,
        principal_id: "agent_1".into(),
    };

    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: actor.clone(),
        action_type: "filesystem.write".into(),
        target: "config.json".into(),
        parameters: Some(serde_json::json!({"content": "hello"})),
        idempotency_key: Some("approval-proposal".into()),
        meta: CommandMeta {
            principal: Some(actor.clone()),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        },
    };

    let outcome = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(outcome.decision, AuthorityDecision::RequireApproval);
    assert!(outcome.approval_request_id.is_some());
    assert_eq!(
        event_types(&pool, &outcome.action_id).await,
        vec![
            "ActionProposed",
            "ActionApprovalRequired",
            "ApprovalRequested"
        ]
    );

    let replay = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(replay.action_id, outcome.action_id);
    assert_eq!(replay.approval_request_id, outcome.approval_request_id);
    assert_eq!(event_types(&pool, &outcome.action_id).await.len(), 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM approval_requests WHERE action_id=?")
            .bind(&outcome.action_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let (aggregate, id, company, correlation, causation, payload): (String, String, String, String, String, String) = sqlx::query_as("SELECT aggregate_type,aggregate_id,company_id,correlation_id,causation_id,payload FROM domain_events WHERE event_type='ApprovalRequested'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(aggregate, "Approval");
    assert_eq!(Some(id), outcome.approval_request_id);
    assert_eq!(company, "comp_1");
    let parent: (String, String) = sqlx::query_as(
        "SELECT id,correlation_id FROM domain_events WHERE event_type='ActionApprovalRequired'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((causation, correlation), parent);
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        payload["approval_request_id"],
        outcome.approval_request_id.as_deref().unwrap()
    );
    assert_eq!(payload["action_id"], outcome.action_id);
    assert_eq!(payload["company_id"], "comp_1");
    assert_eq!(payload["status"], "PENDING");
    assert!(!payload.to_string().contains("hello"));

    sqlx::query("CREATE TRIGGER fail_requested BEFORE INSERT ON outbox_messages WHEN (SELECT event_type FROM domain_events WHERE id=NEW.domain_event_id)='ApprovalRequested' BEGIN SELECT RAISE(ABORT,'forced requested outbox fault'); END")
        .execute(&pool).await.unwrap();
    let mut failed = cmd;
    failed.idempotency_key = Some("failed-approval-proposal".into());
    assert!(app.propose_action(failed).await.is_err());
    assert_eq!(event_types(&pool, &outcome.action_id).await.len(), 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM approval_requests")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox_messages")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    sqlx::query("DROP TRIGGER fail_requested")
        .execute(&pool)
        .await
        .unwrap();

    let approval_id = outcome.approval_request_id.unwrap();
    let pending = app.get_approval("comp_1", &approval_id).await.unwrap();
    assert_eq!(pending.status, "PENDING");
    let authority: (String, String) = sqlx::query_as(
        "SELECT action, details FROM audit_records WHERE json_extract(details, '$.action_id') = ?",
    )
    .bind(&outcome.action_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(authority.0, "ActionApprovalRequired");
    let details: serde_json::Value = serde_json::from_str(&authority.1).unwrap();
    assert_eq!(details["fingerprint"], outcome.fingerprint);
    assert!(details.get("target").is_none());
    assert!(!authority.1.contains("hello"));
    assert!(!authority.1.contains("config.json"));

    // SEC-02 Self-approval check should reject agent self-approving
    let self_decide_res = app
        .decide_action_approval(
            "comp_1",
            &approval_id,
            actor.clone(),
            &outcome.fingerprint,
            true,
            Some("Self approval".into()),
        )
        .await;
    assert!(self_decide_res.is_err());

    // Approver user decides approval
    let user_actor = PrincipalRef {
        principal_type: PrincipalType::User,
        principal_id: "0191e4b8-0001-7000-8000-000000000001".into(),
    };

    let decision = app
        .decide_action_approval(
            "comp_1",
            &approval_id,
            user_actor.clone(),
            &outcome.fingerprint,
            true,
            Some("Looks good".into()),
        )
        .await
        .unwrap();

    assert_eq!(decision.decision, "APPROVED");
    let durable_decision: (String, String) = sqlx::query_as(
        "SELECT action, details FROM audit_records WHERE json_extract(details, '$.approval_id') = ? ORDER BY occurred_at DESC LIMIT 1",
    )
    .bind(&approval_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(durable_decision.0, "ApprovalGranted");
    assert!(durable_decision.1.contains("APPROVED"));

    let updated = app.get_approval("comp_1", &approval_id).await.unwrap();
    assert_eq!(updated.status, "APPROVED");

    // Guard against duplicate approvals
    let duplicate_res = app
        .decide_action_approval(
            "comp_1",
            &approval_id,
            user_actor,
            &outcome.fingerprint,
            true,
            Some("Duplicate approval".into()),
        )
        .await;
    assert!(duplicate_res.is_err());

    let dec_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM approval_decisions WHERE approval_request_id = ?")
            .bind(&approval_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dec_count, 1);

    let act = app
        .get_action("comp_1", &outcome.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(act.authorization, "APPROVED");

    assert_eq!(
        event_types(&pool, &outcome.action_id).await,
        vec![
            "ActionProposed",
            "ActionApprovalRequired",
            "ApprovalRequested",
            "ApprovalGranted",
            "ActionApproved",
        ]
    );
}

#[tokio::test]
async fn test_credential_use_authority_checks() {
    let pool = setup_db().await;
    let app = ApplicationContext::new(pool.clone());
    app.ensure_canonical_permissions("0191e4b8-0002-7000-8000-000000000001")
        .await
        .unwrap();

    let actor = PrincipalRef::agent("agent_1");
    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: actor.clone(),
        action_type: "credential.use".into(),
        target: "cred_api_key".into(),
        parameters: Some(serde_json::json!({"resource": "openai"})),
        idempotency_key: None,
        meta: CommandMeta {
            principal: Some(actor.clone()),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        },
    };

    // 1. Without credential.use grant -> DENY
    let outcome = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(outcome.decision, AuthorityDecision::Deny);

    assert_eq!(
        event_types(&pool, &outcome.action_id).await,
        vec!["ActionProposed", "ActionDenied"]
    );

    // 2. Add grant for wrong company -> DENY
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at)
         VALUES ('pg_wrong', '0191e4b8-0002-7000-8000-000000000001', 'other_comp', 'pd_credential.use', 'AGENT', 'agent_1', 'COMPANY', 'other_comp', 'ALLOW', ?)",
    )
    .bind(&now)
    .execute(&pool)
    .await
    .unwrap();

    let outcome = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(outcome.decision, AuthorityDecision::Deny);

    // 3. Add grant for matching company -> ALLOW
    sqlx::query(
        "INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at)
         VALUES ('pg_correct', '0191e4b8-0002-7000-8000-000000000001', 'comp_1', 'pd_credential.use', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', ?)",
    )
    .bind(&now)
    .execute(&pool)
    .await
    .unwrap();

    let outcome = app.propose_action(cmd).await.unwrap();
    assert_eq!(outcome.decision, AuthorityDecision::Allow);
}

#[tokio::test]
async fn test_audit_rollback_fault() {
    let pool = setup_db().await;
    let app = ApplicationContext::new(pool.clone());

    let actor = PrincipalRef::agent("agent_fault");
    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: actor.clone(),
        action_type: "filesystem.write".into(),
        target: "fault.json".into(),
        parameters: None,
        idempotency_key: None,
        meta: CommandMeta {
            principal: Some(actor.clone()),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        },
    };

    // Install trigger forcing audit insert failure inside transaction
    sqlx::query(
        "CREATE TRIGGER fail_audit_test BEFORE INSERT ON audit_records BEGIN SELECT RAISE(ABORT, 'forced audit fault'); END",
    )
    .execute(&pool)
    .await
    .unwrap();

    let res = app.propose_action(cmd).await;
    assert!(res.is_err());

    // Verify all mutations in the transaction rolled back atomically
    let action_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actions")
        .fetch_one(&pool)
        .await
        .unwrap();
    let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    let events_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let audit_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_records")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(action_count, 0);
    assert_eq!(outbox_count, 0);
    assert_eq!(events_count, 0);
    assert_eq!(audit_count, 0);
}

#[tokio::test]
async fn grant_constraints_fail_closed_before_authority_filtering() {
    let pool = setup_db().await;
    let app = ApplicationContext::new(pool.clone());
    app.ensure_canonical_permissions("0191e4b8-0002-7000-8000-000000000001")
        .await
        .unwrap();
    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("agent_1"),
        action_type: "filesystem.write".into(),
        target: "config.json".into(),
        parameters: None,
        idempotency_key: None,
        meta: CommandMeta::default(),
    };
    for id in ["constrained", "unrestricted"] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES(?,'0191e4b8-0002-7000-8000-000000000001','comp_1','pd_filesystem.write','AGENT','agent_1','COMPANY','comp_1','ALLOW','t')")
            .bind(id).execute(&pool).await.unwrap();
    }
    for constraints in [None, Some("{}"), Some(" \n {} \t")] {
        sqlx::query("UPDATE permission_grants SET constraints=? WHERE id='constrained'")
            .bind(constraints)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            app.evaluate_action_authority(&cmd).await.unwrap(),
            AuthorityDecision::Allow
        );
    }
    for effect in ["ALLOW", "DENY"] {
        sqlx::query("UPDATE permission_grants SET effect=? WHERE id='constrained'")
            .bind(effect)
            .execute(&pool)
            .await
            .unwrap();
        for constraints in [
            "{",
            "null",
            "[]",
            "[{}]",
            "true",
            "0",
            "\"{}\"",
            "",
            r#"{"unknown":true}"#,
            r#"{"maximum_tool_calls":0}"#,
            r#"{"maximum_tool_calls":1}"#,
            r#"{"maximum_tool_calls":null}"#,
            r#"{"maximum_tool_calls":"0"}"#,
            r#"{"maximum_tool_calls":-1}"#,
            r#"{"maximum_tool_calls":1.5}"#,
            r#"{"maximum_tool_calls":0,"maximum_tool_calls":1}"#,
        ] {
            sqlx::query("UPDATE permission_grants SET constraints=? WHERE id='constrained'")
                .bind(constraints)
                .execute(&pool)
                .await
                .unwrap();
            let error = app
                .evaluate_action_authority(&cmd)
                .await
                .expect_err(constraints);
            assert!(
                matches!(error, nalarvo_application::ApplicationError::Validation(ref message) if message.contains("Invalid or unsupported permission grant constraints: constrained")),
                "{error}"
            );
            assert!(app.propose_action(cmd.clone()).await.is_err());
        }
    }
    // Invalid constraints cannot be hidden by an otherwise inactive validity window.
    sqlx::query(
        "UPDATE permission_grants SET starts_at='2999-01-01T00:00:00Z' WHERE id='constrained'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(app.evaluate_action_authority(&cmd).await.is_err());
    sqlx::query("UPDATE permission_grants SET starts_at=NULL, expires_at='2000-01-01T00:00:00Z' WHERE id='constrained'").execute(&pool).await.unwrap();
    assert!(app.evaluate_action_authority(&cmd).await.is_err());
    sqlx::query(
        "UPDATE permission_grants SET expires_at=NULL,constraints='{}' WHERE id='constrained'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Deny
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn policy_configuration_fails_closed_without_masking_absence() {
    let pool = setup_db().await;
    let app = ApplicationContext::new(pool.clone());
    app.ensure_canonical_permissions("0191e4b8-0002-7000-8000-000000000001")
        .await
        .unwrap();
    let cmd = ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("agent_1"),
        action_type: "filesystem.write".into(),
        target: "config.json".into(),
        parameters: None,
        idempotency_key: None,
        meta: CommandMeta::default(),
    };
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Deny
    );
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('security_grant','0191e4b8-0002-7000-8000-000000000001','comp_1','pd_filesystem.write','AGENT','agent_1','COMPANY','comp_1','ALLOW','t')")
        .execute(&pool).await.unwrap();
    // Absence is not a configuration error; explicit effective grant still governs.
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Allow
    );
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('security_policy','0191e4b8-0002-7000-8000-000000000001','comp_1','Security','ACTIVE',1,'t','t')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('security_version','security_policy',1,'{}','t')")
        .execute(&pool).await.unwrap();
    for (effect, expected) in [
        ("ALLOW", AuthorityDecision::Allow),
        ("DENY", AuthorityDecision::Deny),
        ("REQUIRE_APPROVAL", AuthorityDecision::RequireApproval),
    ] {
        sqlx::query("UPDATE policy_versions SET rules=?")
            .bind(serde_json::json!({"permission":"filesystem.write","effect":effect}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            app.evaluate_action_authority(&cmd).await.unwrap(),
            expected,
            "{effect}"
        );
        sqlx::query("UPDATE permission_grants SET revoked_at='t'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            app.evaluate_action_authority(&cmd).await.unwrap(),
            AuthorityDecision::Deny
        );
        sqlx::query("UPDATE permission_grants SET revoked_at=NULL")
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('other_allow','0191e4b8-0002-7000-8000-000000000001','comp_1','Other Allow','ACTIVE',1,'t','t')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('other_version','other_allow',1,'{\"permission\":\"filesystem.write\",\"effect\":\"ALLOW\"}','t')")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"DENY\"}' WHERE id='security_version'")
        .execute(&pool).await.unwrap();
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Deny
    );
    for rules in [
        "{",
        "null",
        "[]",
        "true",
        "{}",
        r#"["filesystem.write","ALLOW"]"#,
        r#"{"permission":"filesystem.write"}"#,
        r#"{"permission":"filesystem.write","effect":"REQUIRE_APROVAL"}"#,
        r#"{"permission":"filesystem.write","effect":"UNKNOWN"}"#,
        r#"{"permission":"filesystem.write","effect":1}"#,
        r#"{"permission":"filesystem.write","effect":null}"#,
        r#"{"permission":"filesystem.write","effect":{}}"#,
        r#"{"effect":"ALLOW"}"#,
        r#"{"permission":null,"effect":"ALLOW"}"#,
        r#"{"permission":1,"effect":"ALLOW"}"#,
        r#"{"permission":"","effect":"ALLOW"}"#,
        r#"{"permission":"filesystem.writ","effect":"ALLOW"}"#,
        r#"{"permission":"filesystem.write","effect":"ALLOW","condition":false}"#,
        r#"{"permission":"filesystem.write","effect":"DENY","effect":"ALLOW"}"#,
    ] {
        sqlx::query("UPDATE policy_versions SET rules=? WHERE id='security_version'")
            .bind(rules)
            .execute(&pool)
            .await
            .unwrap();
        let error = app.evaluate_action_authority(&cmd).await.expect_err(rules);
        assert!(
            matches!(error, nalarvo_application::ApplicationError::Validation(ref message) if message.contains("Invalid policy configuration") && message.contains("security_policy")),
            "{error}"
        );
        assert!(app.propose_action(cmd.clone()).await.is_err(), "{rules}");
    }
    sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"filesystem.read\",\"effect\":\"DENY\"}' WHERE id='security_version'")
        .execute(&pool).await.unwrap();
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Allow
    );
    for version in [0, -1, 2] {
        sqlx::query("UPDATE policies SET current_version=? WHERE id='security_policy'")
            .bind(version)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            app.evaluate_action_authority(&cmd).await.is_err(),
            "version {version}"
        );
    }
    sqlx::query("UPDATE policies SET current_version=1 WHERE id='security_policy'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE permission_grants SET effect='ALOW'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(app.evaluate_action_authority(&cmd).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

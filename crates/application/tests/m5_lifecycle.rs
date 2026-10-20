use nalarvo_application::{ApplicationContext, CommandMeta, ProposeActionCommand};
use nalarvo_domain::PrincipalRef;
use nalarvo_persistence::run_migrations;
use sqlx::SqlitePool;

async fn setup() -> (SqlitePool, ApplicationContext) {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let user = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let workspace = nalarvo_domain::WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &user,
        &workspace,
        "test@example.com",
        "Owner",
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO companies (id, workspace_id, name, status, row_version, created_at, updated_at) VALUES ('comp_1', ?, 'Company', 'ACTIVE', 1, '2026-01-01', '2026-01-01')")
        .bind(&workspace.0).execute(&pool).await.unwrap();
    (pool.clone(), ApplicationContext::new(pool))
}

fn command(key: &str) -> ProposeActionCommand {
    ProposeActionCommand {
        company_id: "comp_1".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("agent_1"),
        action_type: "filesystem.read".into(),
        target: "note.txt".into(),
        parameters: Some(serde_json::json!({"path":"note.txt"})),
        idempotency_key: Some(key.into()),
        meta: CommandMeta::default(),
    }
}

async fn events(pool: &SqlitePool, action_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT e.event_type FROM domain_events e JOIN outbox_messages o ON o.domain_event_id = e.id WHERE e.aggregate_id = ? ORDER BY e.rowid")
        .bind(action_id).fetch_all(pool).await.unwrap()
}

#[tokio::test]
async fn project_scope_is_bound_to_replay_and_approval_fingerprint() {
    let (pool, app) = setup().await;
    sqlx::query("INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('project-a','comp_1','A','MEDIUM','ACTIVE','t','t'),('project-b','comp_1','B','MEDIUM','ACTIVE','t','t')")
        .execute(&pool).await.unwrap();
    let mut first_cmd = command("project-key");
    first_cmd.project_id = Some("project-a".into());
    let first = app.propose_action(first_cmd.clone()).await.unwrap();
    assert_eq!(
        first.fingerprint,
        ApplicationContext::compute_action_fingerprint(
            "comp_1",
            Some("project-a"),
            "filesystem.read",
            "note.txt",
            first_cmd.parameters.as_ref(),
        )
    );
    let mut other = first_cmd.clone();
    other.project_id = Some("project-b".into());
    assert!(matches!(
        app.propose_action(other).await,
        Err(nalarvo_application::ApplicationError::IdempotencyKeyReuseMismatch(_))
    ));
    assert_eq!(
        app.propose_action(first_cmd).await.unwrap().action_id,
        first.action_id
    );
    assert_eq!(events(&pool, &first.action_id).await.len(), 2);
}

#[tokio::test]
async fn action_execution_transitions_emit_event_outbox_and_audit_once() {
    let (pool, app) = setup().await;
    sqlx::query("INSERT INTO permission_definitions (id, workspace_id, name, resource, operation, created_at) VALUES ('pd_read', '0191e4b8-0002-7000-8000-000000000001', 'Read', 'filesystem', 'filesystem.read', 't')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants (id, workspace_id, company_id, permission_definition_id, principal_type, principal_id, scope_type, scope_id, effect, created_at) VALUES ('pg_read', '0191e4b8-0002-7000-8000-000000000001', 'comp_1', 'pd_read', 'AGENT', 'agent_1', 'COMPANY', 'comp_1', 'ALLOW', 't')")
        .execute(&pool).await.unwrap();
    let proposed = app.propose_action(command("lifecycle")).await.unwrap();
    assert_eq!(proposed.decision, nalarvo_domain::AuthorityDecision::Allow);
    app.record_action_started(
        "comp_1",
        &proposed.action_id,
        PrincipalRef::agent("agent_1"),
    )
    .await
    .unwrap();
    app.record_action_succeeded(
        "comp_1",
        &proposed.action_id,
        PrincipalRef::agent("agent_1"),
    )
    .await
    .unwrap();
    assert_eq!(
        events(&pool, &proposed.action_id).await,
        vec![
            "ActionProposed",
            "ActionAllowed",
            "ActionStarted",
            "ActionSucceeded"
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id = ?")
            .bind(&proposed.action_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "SUCCEEDED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_records WHERE json_extract(details, '$.action_id') = ?"
        )
        .bind(&proposed.action_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
}

#[tokio::test]
async fn replay_returns_original_decision_without_second_event_and_mismatch_rejected() {
    let (pool, app) = setup().await;
    let first = app.propose_action(command("once")).await.unwrap();
    let replay = app.propose_action(command("once")).await.unwrap();
    assert_eq!(first.action_id, replay.action_id);
    assert_eq!(first.fingerprint, replay.fingerprint);
    assert_eq!(first.decision, replay.decision);
    assert_eq!(
        events(&pool, &first.action_id).await,
        vec!["ActionProposed", "ActionDenied"]
    );
    let mut different = command("once");
    different.target = "different.txt".into();
    assert!(app.propose_action(different).await.is_err());
    assert_eq!(events(&pool, &first.action_id).await.len(), 2);
}

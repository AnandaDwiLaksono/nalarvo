use nalarvo_application::{ApplicationContext, CommandMeta, ProposeActionCommand};
use nalarvo_domain::PrincipalRef;

#[test]
fn action_fingerprint_is_sha256_of_canonical_material() {
    let a =
        serde_json::json!({"tool_version":"1.0.0","run_id":"r","step_id":"s","content_hash":"h"});
    let fp = ApplicationContext::compute_action_fingerprint(
        "c",
        Some("p"),
        "filesystem.write",
        "note.txt",
        Some(&a),
    );
    assert_eq!(fp.len(), 64);
    let mut changed = a.clone();
    changed["step_id"] = "other".into();
    assert_ne!(
        fp,
        ApplicationContext::compute_action_fingerprint(
            "c",
            Some("p"),
            "filesystem.write",
            "note.txt",
            Some(&changed)
        )
    );
    let mut restarted = a.clone();
    restarted["worker_principal_id"] = "replacement-worker".into();
    assert_eq!(
        fp,
        ApplicationContext::compute_action_fingerprint(
            "c",
            Some("p"),
            "filesystem.write",
            "note.txt",
            Some(&restarted)
        )
    );
}

#[tokio::test]
async fn denied_action_persists_authority_without_starting_execution() {
    let pool = nalarvo_persistence::create_pool("sqlite::memory:")
        .await
        .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone());
    let cmd = ProposeActionCommand {
        company_id: "c".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("a"),
        action_type: "filesystem.write".into(),
        target: "note.txt".into(),
        parameters: None,
        idempotency_key: Some("deny".into()),
        meta: CommandMeta::default(),
    };
    let result = app.propose_action(cmd.clone()).await.unwrap();
    app.propose_action(cmd).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM authority_decisions WHERE action_id=?")
            .bind(&result.action_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        app.get_action("c", &result.action_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "PENDING"
    );
    assert!(
        app.record_action_started("c", &result.action_id, PrincipalRef::agent("a"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn duplicate_proposal_before_execution_reuses_same_action_id() {
    let pool = nalarvo_persistence::create_pool("sqlite::memory:")
        .await
        .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone());
    let cmd = ProposeActionCommand {
        company_id: "c".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("a"),
        action_type: "filesystem.write".into(),
        target: "note.txt".into(),
        parameters: None,
        idempotency_key: Some("dup_pre".into()),
        meta: CommandMeta::default(),
    };
    let first = app.propose_action(cmd.clone()).await.unwrap();
    let second = app.propose_action(cmd).await.unwrap();
    assert_eq!(first.action_id, second.action_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn same_key_different_material_returns_idempotency_mismatch() {
    let pool = nalarvo_persistence::create_pool("sqlite::memory:")
        .await
        .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone());
    let mut cmd1 = ProposeActionCommand {
        company_id: "c".into(),
        project_id: None,
        run_id: None,
        step_id: None,
        actor: PrincipalRef::agent("a"),
        action_type: "filesystem.write".into(),
        target: "note.txt".into(),
        parameters: Some(serde_json::json!({"content":"alpha"})),
        idempotency_key: Some("mismatch_key".into()),
        meta: CommandMeta::default(),
    };
    app.propose_action(cmd1.clone()).await.unwrap();
    cmd1.parameters = Some(serde_json::json!({"content":"beta"}));
    let res = app.propose_action(cmd1).await;
    assert!(
        matches!(
            res,
            Err(nalarvo_application::ApplicationError::IdempotencyKeyReuseMismatch(_))
        ),
        "{res:?}"
    );
}

#[tokio::test]
async fn runtime_action_without_canonical_step_is_rejected_before_persistence() {
    let pool = nalarvo_persistence::create_pool("sqlite::memory:")
        .await
        .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let app = ApplicationContext::new(pool.clone());
    let result = app
        .propose_action(ProposeActionCommand {
            company_id: "c".into(),
            project_id: Some("p".into()),
            run_id: Some("missing-run".into()),
            step_id: None,
            actor: PrincipalRef::agent("a"),
            action_type: "filesystem.write".into(),
            target: "note.txt".into(),
            parameters: None,
            idempotency_key: Some("run-operation".into()),
            meta: CommandMeta::default(),
        })
        .await;
    assert!(
        matches!(
            result,
            Err(nalarvo_application::ApplicationError::Validation(_))
        ),
        "runtime provenance must be validated before Action creation: {result:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

use nalarvo_application::{ApplicationContext, ProposeActionCommand};
use nalarvo_domain::{AuthorityDecision, PrincipalRef};

async fn setup() -> (ApplicationContext, ProposeActionCommand) {
    let pool = nalarvo_persistence::create_pool("sqlite::memory:")
        .await
        .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for query in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@local','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,is_personal,created_at,updated_at) VALUES('w','u','W','w',0,'t','t'),('foreign','u','F','f',0,'t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
        "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd','w','filesystem.write','filesystem','write','t')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('g','w','c','pd','SYSTEM','worker','COMPANY','c','ALLOW','t')",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    (
        ApplicationContext::new(pool),
        ProposeActionCommand {
            company_id: "c".into(),
            project_id: None,
            run_id: None,
            step_id: None,
            actor: PrincipalRef::system("worker"),
            action_type: "filesystem.write".into(),
            target: "note.txt".into(),
            parameters: None,
            idempotency_key: None,
            meta: Default::default(),
        },
    )
}

#[tokio::test]
async fn cross_workspace_grant_and_definition_fail_closed() {
    for mutation in [
        "UPDATE permission_grants SET workspace_id='foreign'",
        "UPDATE permission_definitions SET workspace_id='foreign'",
    ] {
        let (app, cmd) = setup().await;
        assert_eq!(
            app.evaluate_action_authority(&cmd).await.unwrap(),
            AuthorityDecision::Allow
        );
        sqlx::query(mutation).execute(&app.pool).await.unwrap();
        assert!(
            !matches!(
                app.evaluate_action_authority(&cmd).await,
                Ok(AuthorityDecision::Allow)
            ),
            "{mutation}"
        );
    }
}

#[tokio::test]
async fn invalid_deny_scope_is_not_filtered_out() {
    let (app, cmd) = setup().await;
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('deny','w','c','pd','SYSTEM','worker','BROKEN','c','DENY','t')").execute(&app.pool).await.unwrap();
    assert!(app.evaluate_action_authority(&cmd).await.is_err());
}

#[tokio::test]
async fn policy_workspace_and_status_are_validated_before_filtering() {
    for (workspace, company, status) in [
        ("foreign", Some("c"), "ACTIVE"),
        ("w", Some("c"), "BROKEN"),
        ("w", None, "ACTIVE"),
    ] {
        let (app, cmd) = setup().await;
        sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('p',?,?, 'P',?,1,'t','t')").bind(workspace).bind(company).bind(status).execute(&app.pool).await.unwrap();
        sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv','p',1,'{\"permission\":\"filesystem.write\",\"effect\":\"DENY\"}','t')").execute(&app.pool).await.unwrap();
        assert!(
            !matches!(
                app.evaluate_action_authority(&cmd).await,
                Ok(AuthorityDecision::Allow)
            ),
            "{workspace} {company:?} {status}"
        );
    }
}

#[tokio::test]
async fn suspended_workspace_denies_authority() {
    let (app, cmd) = setup().await;
    sqlx::query("UPDATE workspaces SET status='SUSPENDED'")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(!matches!(
        app.evaluate_action_authority(&cmd).await,
        Ok(AuthorityDecision::Allow)
    ));
}

#[tokio::test]
async fn workspace_policy_is_inherited_without_company_or_workspace_leakage() {
    let (app, cmd) = setup().await;
    for q in [
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('other','w','O','ACTIVE','t','t'),('foreign','foreign','F','ACTIVE','t','t')",
        "INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol','w',NULL,'P','ACTIVE',1,'t','t')",
        "INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv','pol',1,'{\"permission\":\"filesystem.write\",\"effect\":\"DENY\"}','t')",
    ] {
        sqlx::query(q).execute(&app.pool).await.unwrap();
    }
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Deny
    );
    for (workspace, company, expected) in [
        ("foreign", None, AuthorityDecision::Allow),
        ("w", Some("other"), AuthorityDecision::Allow),
        ("w", Some("c"), AuthorityDecision::Deny),
        ("w", None, AuthorityDecision::Deny),
    ] {
        sqlx::query("UPDATE policies SET workspace_id=?,company_id=?")
            .bind(workspace)
            .bind(company)
            .execute(&app.pool)
            .await
            .unwrap();
        assert_eq!(app.evaluate_action_authority(&cmd).await.unwrap(), expected);
    }
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('allow','w','c','Allow','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('allow-v','allow',1,'{\"permission\":\"filesystem.write\",\"effect\":\"ALLOW\"}','t')").execute(&app.pool).await.unwrap();
    assert_eq!(
        app.evaluate_action_authority(&cmd).await.unwrap(),
        AuthorityDecision::Deny
    );
    sqlx::query("UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"BROKEN\"}' WHERE policy_id='pol'").execute(&app.pool).await.unwrap();
    assert!(app.evaluate_action_authority(&cmd).await.is_err());
}

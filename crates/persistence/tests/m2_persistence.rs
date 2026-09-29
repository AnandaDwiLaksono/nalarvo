use nalarvo_domain::{Company, CompanyId, UserId, WorkspaceId};
use nalarvo_persistence::*;
use sqlx::Row;

async fn setup(pool: &sqlx::SqlitePool) -> (WorkspaceId, CompanyId) {
    run_migrations(pool).await.unwrap();
    let user = UserId::new();
    let workspace = WorkspaceId::new();
    provision_personal_workspace(pool, &user, &workspace, "m2@test.local", "M2")
        .await
        .unwrap();
    let company = Company::create(workspace.clone(), "First".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &company).await.unwrap();
    tx.commit().await.unwrap();
    (workspace, company.id)
}

#[tokio::test]
async fn provisioning_is_idempotent_and_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_pool(&format!("sqlite://{}", dir.path().join("a.db").display()))
        .await
        .unwrap();
    let (workspace, _) = setup(&pool).await;
    let owner = get_workspace(&pool, &workspace)
        .await
        .unwrap()
        .unwrap()
        .owner_user_id;
    provision_personal_workspace(
        &pool,
        &UserId(owner.clone()),
        &WorkspaceId::new(),
        "m2@test.local",
        "M2",
    )
    .await
    .unwrap();
    let list = list_workspaces_for_user(&pool, &UserId(owner))
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, workspace.0);
    assert_eq!(list[0].status, "ACTIVE");
}

#[tokio::test]
async fn resource_grants_isolate_companies_and_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("resources.db").display());
    let pool = create_pool(&url).await.unwrap();
    let (workspace, company) = setup(&pool).await;
    let second = Company::create(workspace.clone(), "Second".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &second).await.unwrap();
    tx.commit().await.unwrap();
    create_credential_ref(
        &pool,
        &workspace,
        "cred",
        "API key",
        "secretstore://provider/main",
    )
    .await
    .unwrap();
    create_provider_connection(
        &pool,
        &workspace,
        "provider",
        "Provider",
        "openai",
        Some("cred"),
    )
    .await
    .unwrap();
    create_model(&pool, &workspace, "model", "provider", "model-1")
        .await
        .unwrap();
    create_model_profile(&pool, &workspace, "profile", "Default")
        .await
        .unwrap();
    create_model_profile_version(&pool, &workspace, "profile", 1, "model", "{}")
        .await
        .unwrap();
    assert!(
        get_model_profile(&pool, &workspace, "profile")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        get_provider_connection(&pool, &workspace, "provider")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(list_models(&pool, &workspace).await.unwrap().len(), 1);
    assert!(
        grant_model_profile(&pool, &company, "profile")
            .await
            .is_ok()
    );
    assert!(
        !company_has_model_profile(&pool, &second.id, "profile")
            .await
            .unwrap()
    );
    assert!(
        company_has_model_profile(&pool, &company, "profile")
            .await
            .unwrap()
    );
    drop(pool);
    let pool = create_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    assert_eq!(
        list_credential_refs(&pool, &workspace).await.unwrap().len(),
        1
    );
    assert!(
        company_has_model_profile(&pool, &company, "profile")
            .await
            .unwrap()
    );
    let schema = sqlx::query("SELECT sql FROM sqlite_master WHERE name = 'credential_refs'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<String, _>(0);
    assert!(!schema.contains("secret_value"));
}

#[tokio::test]
async fn workforce_rejects_cross_company_links_and_retirement_with_agents() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("workforce.db").display()
    ))
    .await
    .unwrap();
    let (workspace, company) = setup(&pool).await;
    let second = Company::create(workspace, "Second".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &second).await.unwrap();
    tx.commit().await.unwrap();
    create_department(&pool, &company, "dep", "Engineering")
        .await
        .unwrap();
    create_role(&pool, &company, "role", "Engineer")
        .await
        .unwrap();
    create_department(&pool, &second.id, "other", "Other")
        .await
        .unwrap();
    assert!(
        assign_department_role(&pool, &company, "other", "role")
            .await
            .is_err()
    );
    assign_department_role(&pool, &company, "dep", "role")
        .await
        .unwrap();
    assert!(
        create_agent(&pool, &second.id, "bad", "Bad", "dep", "role", None, 1)
            .await
            .is_err()
    );
    create_agent(&pool, &company, "agent", "Good", "dep", "role", None, 2)
        .await
        .unwrap();
    assert!(retire_department(&pool, &company, "dep").await.is_err());
    assert_eq!(list_agents(&pool, &second.id).await.unwrap().len(), 0);
    assert_eq!(list_agents(&pool, &company).await.unwrap().len(), 1);
    assert_eq!(list_departments(&pool, &company).await.unwrap().len(), 1);
    assert_eq!(list_roles(&pool, &company).await.unwrap().len(), 1);
    assert_eq!(
        list_department_roles(&pool, &company).await.unwrap().len(),
        1
    );
    assert!(
        get_agent(&pool, &second.id, "agent")
            .await
            .unwrap()
            .is_none()
    );
    retire_agent(&pool, &company, "agent").await.unwrap();
    retire_department(&pool, &company, "dep").await.unwrap();
    assert_eq!(
        get_agent(&pool, &company, "agent")
            .await
            .unwrap()
            .unwrap()
            .status,
        "RETIRED"
    );
}

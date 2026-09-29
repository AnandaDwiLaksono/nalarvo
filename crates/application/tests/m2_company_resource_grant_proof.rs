use nalarvo_application::{ApplicationContext, CreateCompanyCommand};
use nalarvo_domain::WorkspaceId;
use tempfile::tempdir;

#[tokio::test]
async fn test_company_workspace_resource_grant_boundary() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("resource_grant_proof.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    // 1. Workspace resources exist: Provider + Model + ModelProfile
    nalarvo_persistence::create_provider_connection(
        &app_ctx.pool,
        &workspace_id,
        "prov-shared",
        "Shared Workspace Provider",
        "openai",
        None,
    )
    .await
    .unwrap();

    nalarvo_persistence::create_model(
        &app_ctx.pool,
        &workspace_id,
        "model-shared",
        "prov-shared",
        "gpt-4o",
    )
    .await
    .unwrap();

    nalarvo_persistence::create_model_profile(
        &app_ctx.pool,
        &workspace_id,
        "prof-shared",
        "Shared Profile",
    )
    .await
    .unwrap();

    // 2. Create Company A and Company B in the same Workspace
    let comp_a = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Company A".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    let comp_b = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Company B".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    let dept_a = app_ctx
        .create_department(&comp_a.id, "Dept A")
        .await
        .unwrap();
    let role_a = app_ctx.create_role(&comp_a.id, "Role A").await.unwrap();
    let dept_b = app_ctx
        .create_department(&comp_b.id, "Dept B")
        .await
        .unwrap();
    let role_b = app_ctx.create_role(&comp_b.id, "Role B").await.unwrap();

    // 3. Grant prof-shared ONLY to Company A
    nalarvo_persistence::grant_model_profile(&app_ctx.pool, &comp_a.id, "prof-shared")
        .await
        .unwrap();

    // Verify semantics: Provider exists != automatic access for Company B
    assert!(
        nalarvo_persistence::company_has_model_profile(&app_ctx.pool, &comp_a.id, "prof-shared")
            .await
            .unwrap()
    );
    assert!(
        !nalarvo_persistence::company_has_model_profile(&app_ctx.pool, &comp_b.id, "prof-shared")
            .await
            .unwrap(),
        "Workspace provider/profile exists must NOT imply automatic access for Company B!"
    );

    // 4. Company A can bind an Agent to prof-shared
    let agent_a = app_ctx
        .create_agent(
            &comp_a.id,
            "Agent A",
            &dept_a.id,
            &role_a.id,
            Some("prof-shared"),
            3,
        )
        .await;
    assert!(
        agent_a.is_ok(),
        "Company A has grant, agent creation succeeds"
    );

    // 5. Company B CANNOT bind an Agent to prof-shared without a grant
    let agent_b = app_ctx
        .create_agent(
            &comp_b.id,
            "Agent B",
            &dept_b.id,
            &role_b.id,
            Some("prof-shared"),
            3,
        )
        .await;
    assert!(
        agent_b.is_err(),
        "Company B without grant must fail to bind agent to prof-shared!"
    );
}

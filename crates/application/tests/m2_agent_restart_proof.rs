use nalarvo_application::{ApplicationContext, CreateCompanyCommand};
use nalarvo_domain::{PrincipalRef, WorkspaceId};
use tempfile::tempdir;

#[tokio::test]
async fn test_agent_persistence_and_configuration_stability_across_restart() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("agent_restart_test.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    let (
        captured_agent_id,
        captured_company_id,
        captured_dept_id,
        captured_role_id,
        captured_profile_id,
        captured_status,
        captured_version,
        captured_capacity,
    ) = {
        let app_ctx = ApplicationContext::init(&db_url).await.unwrap();

        // 1. Create Model Connection and Profiles in Persistence
        nalarvo_persistence::create_provider_connection(
            &app_ctx.pool,
            &workspace_id,
            "prov-1",
            "OpenAI",
            "openai",
            None,
        )
        .await
        .unwrap();

        nalarvo_persistence::create_model(
            &app_ctx.pool,
            &workspace_id,
            "model-1",
            "prov-1",
            "gpt-4o",
        )
        .await
        .unwrap();

        nalarvo_persistence::create_model_profile(
            &app_ctx.pool,
            &workspace_id,
            "prof-1",
            "Default GPT-4o Profile",
        )
        .await
        .unwrap();

        nalarvo_persistence::create_model_profile(
            &app_ctx.pool,
            &workspace_id,
            "prof-2",
            "Alternate Profile",
        )
        .await
        .unwrap();

        // 2. Create Company, Department, Role
        let principal = PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001");
        let company = app_ctx
            .create_company(CreateCompanyCommand {
                workspace_id,
                name: "Agent Corp".into(),
                description: None,
                principal: Some(principal.clone()),
                idempotency_key: None,
                correlation_id: None,
                causation_id: None,
            })
            .await
            .unwrap();

        let dept = app_ctx
            .create_department(&company.id, "Core Engineering")
            .await
            .unwrap();
        let role = app_ctx
            .create_role(&company.id, "Senior Architect")
            .await
            .unwrap();

        // Assign department role mapping
        nalarvo_persistence::assign_department_role(&app_ctx.pool, &company.id, &dept.id, &role.id)
            .await
            .unwrap();

        // Grant resource access to Company
        nalarvo_persistence::grant_model_profile(&app_ctx.pool, &company.id, "prof-1")
            .await
            .unwrap();
        nalarvo_persistence::grant_model_profile(&app_ctx.pool, &company.id, "prof-2")
            .await
            .unwrap();

        // 3. Create Agent referencing prof-1
        let agent = app_ctx
            .create_agent(
                &company.id,
                "Ada Lovelace",
                &dept.id,
                &role.id,
                Some("prof-1"),
                5,
            )
            .await
            .unwrap();

        assert_eq!(agent.status, "ACTIVE");
        assert_eq!(agent.capacity, 5);
        assert_eq!(agent.model_profile_id.as_deref(), Some("prof-1"));

        (
            agent.id,
            company.id,
            dept.id,
            role.id,
            agent.model_profile_id,
            agent.status,
            agent.row_version,
            agent.capacity,
        )
        // app_ctx dropped here -> close DB pool
    };

    // 4. Re-open DB pool / restart ApplicationContext
    let app_ctx_restarted = ApplicationContext::init(&db_url).await.unwrap();

    let loaded_agent = app_ctx_restarted
        .get_agent(&captured_company_id, &captured_agent_id)
        .await
        .unwrap();

    // 5. Assert all material identity/configuration references remain exact
    assert_eq!(loaded_agent.id, captured_agent_id);
    assert_eq!(loaded_agent.company_id, captured_company_id.0);
    assert_eq!(loaded_agent.primary_department_id, captured_dept_id);
    assert_eq!(loaded_agent.role_id, captured_role_id);
    assert_eq!(loaded_agent.model_profile_id, captured_profile_id);
    assert_eq!(loaded_agent.status, captured_status);
    assert_eq!(loaded_agent.row_version, captured_version);
    assert_eq!(loaded_agent.capacity, captured_capacity);

    // 6. Prove changing ModelProfile -> Agent ID remains UNCHANGED
    let updated_agent = app_ctx_restarted
        .update_agent(
            &captured_company_id,
            &captured_agent_id,
            "Ada Lovelace",
            &captured_dept_id,
            &captured_role_id,
            Some("prof-2"),
            captured_capacity,
            captured_version,
        )
        .await
        .unwrap();

    assert_eq!(
        updated_agent.id, captured_agent_id,
        "Agent ID must remain unchanged when changing ModelProfile!"
    );
    assert_eq!(
        updated_agent.model_profile_id.as_deref(),
        Some("prof-2"),
        "ModelProfile updated to prof-2!"
    );
}

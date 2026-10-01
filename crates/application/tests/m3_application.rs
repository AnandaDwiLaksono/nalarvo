use nalarvo_application::*;
use nalarvo_domain::{
    AgentAllocationStatus, AssignmentStatus, CompanyId, DependencyType, ObjectiveStatus,
    StaffingRequirementStatus, TeamStatus, WorkItemStatus, WorkspaceId,
};

async fn app() -> (ApplicationContext, CompanyId) {
    let app = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let company_id = app
        .create_company(CreateCompanyCommand {
            workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
            name: "M3 Co".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap()
        .id;
    for sql in [
        "INSERT INTO departments(id, company_id, name, created_at, updated_at) VALUES ('d1', ?, 'Engineering', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        "INSERT INTO roles(id, company_id, name, created_at, updated_at) VALUES ('r1', ?, 'Developer', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id, department_id, role_id, created_at) VALUES (?, 'd1', 'r1', '2026-01-01T00:00:00Z')",
        "INSERT INTO agents(id, company_id, name, primary_department_id, role_id, capacity, status, created_at, updated_at) VALUES ('a1', ?, 'Agent 1', 'd1', 'r1', 2, 'ACTIVE', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    ] {
        sqlx::query(sql)
            .bind(&company_id.0)
            .execute(&app.pool)
            .await
            .unwrap();
    }
    (app, company_id)
}

#[tokio::test]
async fn m3_commands_are_scoped_idempotent_and_emit_outbox_events() {
    let (app, company) = app().await;
    let project = app
        .create_project(&company, "P".into(), None)
        .await
        .unwrap();
    let objective = app
        .create_objective(CreateObjectiveCommand::new(
            company.clone(),
            project.id.clone(),
            "O",
        ))
        .await
        .unwrap();
    let team = app
        .create_team(CreateTeamCommand::new(
            company.clone(),
            project.id.clone(),
            "T",
        ))
        .await
        .unwrap();
    let staffing = app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            company.clone(),
            project.id.clone(),
            Some(team.id.clone()),
            "r1",
            1,
        ))
        .await
        .unwrap();
    let allocation_command = CreateAgentAllocationCommand::new(
        company.clone(),
        project.id.clone(),
        team.id.clone(),
        "a1",
        Some(staffing.id.clone()),
    )
    .idempotency("allocation-key");
    let allocation = app
        .create_agent_allocation(allocation_command.clone())
        .await
        .unwrap();
    assert_eq!(
        allocation.id,
        app.create_agent_allocation(allocation_command)
            .await
            .unwrap()
            .id
    );
    let allocation = app
        .transition_agent_allocation(
            &company,
            &project.id,
            &allocation.id,
            AgentAllocationStatus::Active,
            allocation.row_version,
        )
        .await
        .unwrap();
    let work_command = CreateWorkItemCommand::new(company.clone(), project.id.clone(), "W")
        .idempotency("work-key");
    let work = app.create_work_item(work_command.clone()).await.unwrap();
    assert_eq!(
        work.id,
        app.create_work_item(work_command).await.unwrap().id
    );
    let dependency = app
        .create_work_dependency(CreateWorkDependencyCommand::new(
            company.clone(),
            project.id.clone(),
            work.id.clone(),
            work.id.clone(),
            DependencyType::Soft,
        ))
        .await;
    assert!(matches!(dependency, Err(ApplicationError::Validation(_))));
    let assignment = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            company.clone(),
            project.id.clone(),
            work.id.clone(),
            "a1",
            allocation.id.clone(),
            true,
        ))
        .await
        .unwrap();

    assert_eq!(
        app.list_objectives(&company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        app.list_teams(&company, &project.id).await.unwrap().len(),
        1
    );
    assert_eq!(
        app.list_staffing_requirements(&company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        app.list_agent_allocations(&company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        app.list_work_assignments(&company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        app.get_team(&CompanyId::new(), &project.id, &team.id).await,
        Err(ApplicationError::NotFound(_))
    ));

    assert_eq!(
        app.transition_objective(
            &company,
            &project.id,
            &objective.id,
            ObjectiveStatus::Active,
            1
        )
        .await
        .unwrap()
        .status,
        ObjectiveStatus::Active
    );
    assert_eq!(
        app.transition_team(&company, &project.id, &team.id, TeamStatus::Active, 1)
            .await
            .unwrap()
            .status,
        TeamStatus::Active
    );
    assert_eq!(
        app.transition_agent_allocation(
            &company,
            &project.id,
            &allocation.id,
            AgentAllocationStatus::Paused,
            allocation.row_version,
        )
        .await
        .unwrap()
        .status,
        AgentAllocationStatus::Paused
    );
    assert_eq!(
        app.transition_work_item(&company, &project.id, &work.id, WorkItemStatus::Ready, 1)
            .await
            .unwrap()
            .status,
        WorkItemStatus::Ready
    );
    assert_eq!(
        app.transition_work_assignment(
            &company,
            &project.id,
            &assignment.id,
            AssignmentStatus::Released,
            1
        )
        .await
        .unwrap()
        .status,
        AssignmentStatus::Released
    );
    assert_eq!(
        app.transition_staffing_requirement(
            &company,
            &project.id,
            &staffing.id,
            StaffingRequirementStatus::Blocked,
            1
        )
        .await
        .unwrap()
        .status,
        StaffingRequirementStatus::Blocked
    );

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events WHERE company_id = ?")
        .bind(&company.0)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(events, outbox);
    assert!(events >= 13);
}

#[tokio::test]
async fn completion_releases_assignments_before_allocations_then_disbands_teams() {
    let (app, company) = app().await;
    let project = app
        .create_project(&company, "P".into(), None)
        .await
        .unwrap();
    let team = app
        .create_team(CreateTeamCommand::new(
            company.clone(),
            project.id.clone(),
            "T",
        ))
        .await
        .unwrap();
    let allocation = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            company.clone(),
            project.id.clone(),
            team.id.clone(),
            "a1",
            None,
        ))
        .await
        .unwrap();
    let allocation = app
        .transition_agent_allocation(
            &company,
            &project.id,
            &allocation.id,
            AgentAllocationStatus::Active,
            allocation.row_version,
        )
        .await
        .unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(
            company.clone(),
            project.id.clone(),
            "W",
        ))
        .await
        .unwrap();
    app.create_work_assignment(CreateWorkAssignmentCommand::new(
        company.clone(),
        project.id.clone(),
        work.id,
        "a1",
        allocation.id,
        true,
    ))
    .await
    .unwrap();
    let active = app
        .activate_project(&company, &project.id, project.row_version)
        .await
        .unwrap();
    assert_eq!(
        app.complete_project(&company, &project.id, active.row_version)
            .await
            .unwrap()
            .status
            .to_string(),
        "COMPLETED"
    );
    for (table, id, expected) in [
        ("assignments", "status", "RELEASED"),
        ("agent_allocations", "status", "RELEASED"),
        ("teams", "status", "DISBANDED"),
    ] {
        let actual: String =
            sqlx::query_scalar(&format!("SELECT {id} FROM {table} WHERE company_id = ?"))
                .bind(&company.0)
                .fetch_one(&app.pool)
                .await
                .unwrap();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn m3_scoped_gets_dependencies_and_delete_emit_outbox() {
    let (app, company) = app().await;
    let project = app
        .create_project(&company, "P".into(), None)
        .await
        .unwrap();
    let objective = app
        .create_objective(CreateObjectiveCommand::new(
            company.clone(),
            project.id.clone(),
            "O",
        ))
        .await
        .unwrap();
    let team = app
        .create_team(CreateTeamCommand::new(
            company.clone(),
            project.id.clone(),
            "T",
        ))
        .await
        .unwrap();
    let staffing = app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            company.clone(),
            project.id.clone(),
            Some(team.id.clone()),
            "r1",
            1,
        ))
        .await
        .unwrap();
    let allocation = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            company.clone(),
            project.id.clone(),
            team.id.clone(),
            "a1",
            Some(staffing.id.clone()),
        ))
        .await
        .unwrap();
    let first = app
        .create_work_item(CreateWorkItemCommand::new(
            company.clone(),
            project.id.clone(),
            "first",
        ))
        .await
        .unwrap();
    let second = app
        .create_work_item(CreateWorkItemCommand::new(
            company.clone(),
            project.id.clone(),
            "second",
        ))
        .await
        .unwrap();
    let dependency = app
        .create_work_dependency(CreateWorkDependencyCommand::new(
            company.clone(),
            project.id.clone(),
            first.id.clone(),
            second.id.clone(),
            DependencyType::Hard,
        ))
        .await
        .unwrap();

    let allocation = app
        .transition_agent_allocation(
            &company,
            &project.id,
            &allocation.id,
            AgentAllocationStatus::Active,
            allocation.row_version,
        )
        .await
        .unwrap();
    let assignment = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            company.clone(),
            project.id.clone(),
            first.id.clone(),
            "a1",
            allocation.id.clone(),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(
        app.get_objective(&company, &project.id, &objective.id)
            .await
            .unwrap()
            .id,
        objective.id
    );
    assert_eq!(
        app.get_staffing_requirement(&company, &project.id, &staffing.id)
            .await
            .unwrap()
            .id,
        staffing.id
    );
    assert_eq!(
        app.get_agent_allocation(&company, &project.id, &allocation.id)
            .await
            .unwrap()
            .id,
        allocation.id
    );
    assert_eq!(
        app.get_work_assignment(&company, &project.id, &assignment.id)
            .await
            .unwrap()
            .id,
        assignment.id
    );
    assert_eq!(
        app.list_work_dependencies(&company, &project.id)
            .await
            .unwrap()[0]
            .id,
        dependency.id
    );
    assert_eq!(
        app.get_work_dependency(&company, &project.id, &dependency.id)
            .await
            .unwrap()
            .id,
        dependency.id
    );

    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    app.delete_work_dependency(&company, &project.id, &dependency.id)
        .await
        .unwrap();
    assert!(
        app.list_work_dependencies(&company, &project.id)
            .await
            .unwrap()
            .is_empty()
    );
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(after, before + 1);
    assert!(matches!(
        app.get_objective(&company, "wrong", &objective.id).await,
        Err(ApplicationError::NotFound(_))
    ));
}

#[tokio::test]
async fn m3_reconciles_active_allocations_reassigns_history_and_blocks_invalid_invariants() {
    let (app, company) = app().await;
    let project = app
        .create_project(&company, "P".into(), None)
        .await
        .unwrap();
    let team = app
        .create_team(CreateTeamCommand::new(
            company.clone(),
            project.id.clone(),
            "T",
        ))
        .await
        .unwrap();
    let staffing = app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            company.clone(),
            project.id.clone(),
            Some(team.id.clone()),
            "r1",
            2,
        ))
        .await
        .unwrap();
    let allocation = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            company.clone(),
            project.id.clone(),
            team.id.clone(),
            "a1",
            Some(staffing.id.clone()),
        ))
        .await
        .unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(
            company.clone(),
            project.id.clone(),
            "W",
        ))
        .await
        .unwrap();

    assert!(matches!(
        app.create_work_assignment(CreateWorkAssignmentCommand::new(
            company.clone(),
            project.id.clone(),
            work.id.clone(),
            "a1",
            allocation.id.clone(),
            true
        ))
        .await,
        Err(ApplicationError::Validation(_))
    ));
    let allocation = app
        .transition_agent_allocation(
            &company,
            &project.id,
            &allocation.id,
            AgentAllocationStatus::Active,
            allocation.row_version,
        )
        .await
        .unwrap();
    let staffing = app
        .transition_staffing_requirement(
            &company,
            &project.id,
            &staffing.id,
            StaffingRequirementStatus::Open,
            staffing.row_version,
        )
        .await
        .unwrap();
    let staffing = app
        .reconcile_staffing_requirement(&company, &project.id, &staffing.id, staffing.row_version)
        .await
        .unwrap();
    assert_eq!(staffing.status, StaffingRequirementStatus::PartiallyFilled);
    let assignment = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            company.clone(),
            project.id.clone(),
            work.id.clone(),
            "a1",
            allocation.id.clone(),
            true,
        ))
        .await
        .unwrap();
    let replacement = app
        .reassign_work_assignment(
            &company,
            &project.id,
            &assignment.id,
            &allocation.id,
            assignment.row_version,
        )
        .await
        .unwrap();
    assert_ne!(replacement.id, assignment.id);
    assert_eq!(
        app.get_work_assignment(&company, &project.id, &assignment.id)
            .await
            .unwrap()
            .status,
        AssignmentStatus::Released
    );
    assert_eq!(replacement.status, AssignmentStatus::Active);
    assert!(matches!(
        app.transition_team(
            &company,
            &project.id,
            &team.id,
            TeamStatus::Disbanded,
            team.row_version
        )
        .await,
        Err(ApplicationError::Validation(_))
    ));
    assert_eq!(
        app.release_work_assignment(
            &company,
            &project.id,
            &replacement.id,
            replacement.row_version
        )
        .await
        .unwrap()
        .status,
        AssignmentStatus::Released
    );
}

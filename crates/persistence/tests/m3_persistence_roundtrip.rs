use nalarvo_domain::*;
use nalarvo_persistence::*;

async fn setup(pool: &sqlx::SqlitePool) -> (CompanyId, CompanyId, Project) {
    run_migrations(pool).await.unwrap();
    let ws = WorkspaceId::new();
    bootstrap_personal_workspace(pool, &UserId::new(), &ws, "m3@test.local", "M3")
        .await
        .unwrap();
    let first = Company::create(ws.clone(), "First".into(), None).unwrap();
    let second = Company::create(ws, "Second".into(), None).unwrap();
    let project = Project::create(first.id.clone(), "Project".into(), None).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_company_tx(&mut tx, &first).await.unwrap();
    insert_company_tx(&mut tx, &second).await.unwrap();
    insert_project_tx(&mut tx, &project).await.unwrap();
    tx.commit().await.unwrap();
    (first.id, second.id, project)
}

#[tokio::test]
async fn work_items_survive_restart_and_reject_cross_company() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("m3.db").display());
    let pool = create_pool(&url).await.unwrap();
    let (company, other, project) = setup(&pool).await;
    let item = WorkItem::create(company.clone(), project.id.clone(), "Build".into()).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_work_item_tx(&mut tx, &item).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        get_work_item(&pool, &other, &item.id)
            .await
            .unwrap()
            .is_none()
    );
    let impostor = WorkItem::create(other.clone(), project.id.clone(), "Intrusion".into()).unwrap();
    let mut tx = pool.begin().await.unwrap();
    assert!(insert_work_item_tx(&mut tx, &impostor).await.is_err());
    tx.rollback().await.unwrap();
    drop(pool);
    let pool = create_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let loaded = get_work_item(&pool, &company, &item.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.title, "Build");
    assert_eq!(
        list_work_items(&pool, &company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn malformed_staffing_capabilities_return_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("bad_staffing.db").display()
    ))
    .await
    .unwrap();
    let (company, _, project) = setup(&pool).await;
    create_department(&pool, &company, "dept", "Engineering")
        .await
        .unwrap();
    create_role(&pool, &company, "role", "Developer")
        .await
        .unwrap();
    sqlx::query("INSERT INTO staffing_requirements (id, company_id, project_id, role_id, desired_count, required_capability_ids, created_at, updated_at) VALUES ('bad', ?, ?, 'role', 1, '{not-json', 't', 't')")
        .bind(&company.0)
        .bind(&project.id)
        .execute(&pool)
        .await
        .unwrap();

    assert!(
        get_staffing_requirement(&pool, &company, "bad")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hard_dependency_cycle_rejected_before_insert() {
    let dir = tempfile::tempdir().unwrap();
    let pool = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("cycle.db").display()
    ))
    .await
    .unwrap();
    let (company, _, project) = setup(&pool).await;
    let a = WorkItem::create(company.clone(), project.id.clone(), "A".into()).unwrap();
    let b = WorkItem::create(company.clone(), project.id.clone(), "B".into()).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_work_item_tx(&mut tx, &a).await.unwrap();
    insert_work_item_tx(&mut tx, &b).await.unwrap();
    let edge = |from: &WorkItem, to: &WorkItem| WorkDependency {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        work_item_id: from.id.clone(),
        depends_on_work_item_id: to.id.clone(),
        dependency_type: DependencyType::Hard,
        created_at: chrono::Utc::now(),
    };
    insert_work_dependency_tx(&mut tx, &edge(&a, &b))
        .await
        .unwrap();
    assert!(
        insert_work_dependency_tx(&mut tx, &edge(&b, &a))
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert_eq!(
        list_work_dependencies(&pool, &company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn full_m3_graph_durability_occ_and_cross_company() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("full_graph.db").display());
    let pool = create_pool(&url).await.unwrap();
    let (company, other, project) = setup(&pool).await;

    // 1. Objective
    let obj = Objective {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        parent_objective_id: None,
        title: "Primary Objective".into(),
        description: Some("Do great work".into()),
        is_primary: true,
        is_required: true,
        status: ObjectiveStatus::Draft,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let mut tx = pool.begin().await.unwrap();
    insert_objective_tx(&mut tx, &obj).await.unwrap();
    tx.commit().await.unwrap();

    // OCC on Objective
    let mut tx = pool.begin().await.unwrap();
    assert!(
        update_objective_status_tx(&mut tx, &company, &obj.id, ObjectiveStatus::Active, 999)
            .await
            .is_err()
    );
    update_objective_status_tx(&mut tx, &company, &obj.id, ObjectiveStatus::Active, 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let fetched_obj = get_objective(&pool, &company, &obj.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_obj.status, ObjectiveStatus::Active);
    assert_eq!(fetched_obj.row_version, 2);
    assert!(
        get_objective(&pool, &other, &obj.id)
            .await
            .unwrap()
            .is_none()
    );

    // 2. Team
    let team = Team {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        name: "Core Team".into(),
        is_primary: true,
        status: TeamStatus::Forming,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let mut tx = pool.begin().await.unwrap();
    insert_team_tx(&mut tx, &team).await.unwrap();
    tx.commit().await.unwrap();

    // OCC on Team
    let mut tx = pool.begin().await.unwrap();
    assert!(
        update_team_status_tx(&mut tx, &company, &team.id, TeamStatus::Disbanded, 999)
            .await
            .is_err()
    );
    update_team_status_tx(&mut tx, &company, &team.id, TeamStatus::Disbanded, 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let fetched_team = get_team(&pool, &company, &team.id).await.unwrap().unwrap();
    assert_eq!(fetched_team.status, TeamStatus::Disbanded);
    assert_eq!(fetched_team.row_version, 2);
    assert!(get_team(&pool, &other, &team.id).await.unwrap().is_none());

    // 3. Department, Role, Agent setup for staffing & allocation
    create_department(&pool, &company, "dep-eng", "Engineering")
        .await
        .unwrap();
    create_role(&pool, &company, "role-dev", "Developer")
        .await
        .unwrap();
    assign_department_role(&pool, &company, "dep-eng", "role-dev")
        .await
        .unwrap();
    create_agent(
        &pool,
        &company,
        "agent-1",
        "Agent One",
        "dep-eng",
        "role-dev",
        None,
        2,
    )
    .await
    .unwrap();

    // 4. StaffingRequirement
    let req = StaffingRequirement {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        team_id: Some(team.id.clone()),
        role_id: "role-dev".into(),
        department_id: Some("dep-eng".into()),
        desired_count: 1,
        required_capability_ids: vec!["rust".into(), "sql".into()],
        status: StaffingRequirementStatus::Draft,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let mut tx = pool.begin().await.unwrap();
    insert_staffing_requirement_tx(&mut tx, &req).await.unwrap();
    tx.commit().await.unwrap();

    // OCC on StaffingRequirement
    let mut tx = pool.begin().await.unwrap();
    assert!(
        update_staffing_requirement_status_tx(
            &mut tx,
            &company,
            &req.id,
            StaffingRequirementStatus::Filled,
            999
        )
        .await
        .is_err()
    );
    update_staffing_requirement_status_tx(
        &mut tx,
        &company,
        &req.id,
        StaffingRequirementStatus::Filled,
        1,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let fetched_req = get_staffing_requirement(&pool, &company, &req.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_req.status, StaffingRequirementStatus::Filled);
    assert_eq!(fetched_req.row_version, 2);
    assert_eq!(fetched_req.required_capability_ids, vec!["rust", "sql"]);
    assert!(
        get_staffing_requirement(&pool, &other, &req.id)
            .await
            .unwrap()
            .is_none()
    );

    // 5. AgentAllocation
    let alloc = AgentAllocation {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        team_id: team.id.clone(),
        agent_id: "agent-1".into(),
        staffing_requirement_id: Some(req.id.clone()),
        status: AgentAllocationStatus::Active,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        released_at: None,
    };
    let mut tx = pool.begin().await.unwrap();
    insert_agent_allocation_tx(&mut tx, &alloc).await.unwrap();
    tx.commit().await.unwrap();

    // OCC on AgentAllocation
    let mut tx = pool.begin().await.unwrap();
    assert!(
        update_agent_allocation_status_tx(
            &mut tx,
            &company,
            &alloc.id,
            AgentAllocationStatus::Released,
            999
        )
        .await
        .is_err()
    );
    update_agent_allocation_status_tx(
        &mut tx,
        &company,
        &alloc.id,
        AgentAllocationStatus::Released,
        1,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let fetched_alloc = get_agent_allocation(&pool, &company, &alloc.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_alloc.status, AgentAllocationStatus::Released);
    assert_eq!(fetched_alloc.row_version, 2);
    assert!(
        get_agent_allocation(&pool, &other, &alloc.id)
            .await
            .unwrap()
            .is_none()
    );

    // 6. WorkItem & Assignment
    let item = WorkItem::create(company.clone(), project.id.clone(), "Feature X".into()).unwrap();
    let mut tx = pool.begin().await.unwrap();
    insert_work_item_tx(&mut tx, &item).await.unwrap();
    tx.commit().await.unwrap();

    let assign = WorkAssignment {
        id: uuid::Uuid::now_v7().to_string(),
        company_id: company.clone(),
        project_id: project.id.clone(),
        work_item_id: item.id.clone(),
        agent_id: "agent-1".into(),
        is_primary: true,
        status: AssignmentStatus::Active,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        released_at: None,
    };
    let mut tx = pool.begin().await.unwrap();
    insert_work_assignment_tx(&mut tx, &assign, &alloc.id)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // OCC on WorkAssignment
    let mut tx = pool.begin().await.unwrap();
    assert!(
        update_work_assignment_status_tx(
            &mut tx,
            &company,
            &assign.id,
            AssignmentStatus::Released,
            999
        )
        .await
        .is_err()
    );
    update_work_assignment_status_tx(&mut tx, &company, &assign.id, AssignmentStatus::Released, 1)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let fetched_assign = get_work_assignment(&pool, &company, &assign.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_assign.status, AssignmentStatus::Released);
    assert_eq!(fetched_assign.row_version, 2);
    assert!(
        get_work_assignment(&pool, &other, &assign.id)
            .await
            .unwrap()
            .is_none()
    );

    // 7. Blocker
    let blocker = create_blocker(&pool, &company, &project.id, &item.id, "Waiting on API")
        .await
        .unwrap();
    assert_eq!(blocker.reason, "Waiting on API");
    assert!(
        list_blockers(&pool, &other, &project.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        list_blockers(&pool, &company, &project.id)
            .await
            .unwrap()
            .len(),
        1
    );
    resolve_blocker(&pool, &company, &blocker.id).await.unwrap();

    // Durability verification across pool restart
    drop(pool);
    let pool = create_pool(&url).await.unwrap();
    run_migrations(&pool).await.unwrap();

    assert!(
        get_objective(&pool, &company, &obj.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(get_team(&pool, &company, &team.id).await.unwrap().is_some());
    assert!(
        get_staffing_requirement(&pool, &company, &req.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        get_agent_allocation(&pool, &company, &alloc.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        get_work_item(&pool, &company, &item.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        get_work_assignment(&pool, &company, &assign.id)
            .await
            .unwrap()
            .is_some()
    );
    let blockers = list_blockers(&pool, &company, &project.id).await.unwrap();
    assert_eq!(blockers.len(), 1);
    assert!(blockers[0].resolved_at.is_some());
}

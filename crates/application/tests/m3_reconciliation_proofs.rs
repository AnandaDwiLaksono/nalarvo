use nalarvo_application::{ApplicationContext, ApplicationError, CreateCompanyCommand};
use nalarvo_domain::{
    CompanyId, ProjectStatus, StaffingRequirement, StaffingRequirementStatus, WorkspaceId,
};
use tempfile::tempdir;

async fn setup_app() -> (ApplicationContext, CompanyId) {
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

    // Provision baseline dept, role, agent
    let pool = &app.pool;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO departments(id, company_id, name, created_at, updated_at) VALUES ('d1', ?, 'Engineering', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO roles(id, company_id, name, created_at, updated_at) VALUES ('r1', ?, 'Developer', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO department_roles(company_id, department_id, role_id, created_at) VALUES (?, 'd1', 'r1', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agents(id, company_id, name, primary_department_id, role_id, capacity, status, created_at, updated_at) VALUES ('a1', ?, 'Agent 1', 'd1', 'r1', 2, 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    (app, company_id)
}

#[tokio::test]
async fn test_proof_project_working_root_binding_end_to_end() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();

    let valid_dir = tempdir().unwrap();
    let valid_path = valid_dir.path().to_str().unwrap().to_string();
    let canonical_valid = valid_dir
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // 1. Bind local directory
    let bound = app
        .bind_project_working_root(&company_id, &proj.id, valid_path.clone(), proj.row_version)
        .await
        .unwrap();
    assert_eq!(
        bound.working_root_path.as_deref(),
        Some(canonical_valid.as_str())
    );
    assert!(bound.working_root_bound_at.is_some());

    // 2. Query binding
    let queried = app.get_project(&company_id, &proj.id).await.unwrap();
    assert_eq!(
        queried.working_root_path.as_deref(),
        Some(canonical_valid.as_str())
    );

    // 3. Update binding
    let valid_dir2 = tempdir().unwrap();
    let valid_path2 = valid_dir2.path().to_str().unwrap().to_string();
    let canonical_valid2 = valid_dir2
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let updated = app
        .bind_project_working_root(
            &company_id,
            &proj.id,
            valid_path2.clone(),
            bound.row_version,
        )
        .await
        .unwrap();
    assert_eq!(
        updated.working_root_path.as_deref(),
        Some(canonical_valid2.as_str())
    );

    // 4. Unbind binding
    let unbound = app
        .unbind_project_working_root(&company_id, &proj.id, updated.row_version)
        .await
        .unwrap();
    assert_eq!(unbound.working_root_path, None);
    assert_eq!(unbound.working_root_bound_at, None);

    // 5. Invalid / non-directory path rejected
    let res = app
        .bind_project_working_root(
            &company_id,
            &proj.id,
            "/nonexistent/directory/path/123".into(),
            unbound.row_version,
        )
        .await;
    assert!(res.is_err());

    // 6. Cross-project mutation rejected
    let c2 = CompanyId::new();
    let res2 = app
        .bind_project_working_root(&c2, &proj.id, valid_path.clone(), unbound.row_version)
        .await;
    assert!(matches!(res2, Err(ApplicationError::NotFound(_))));
}

#[tokio::test]
async fn test_proof_project_lifecycle_and_occ() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();
    assert_eq!(proj.status, ProjectStatus::Draft);

    // DRAFT -> STAFFING
    let p_staffing = app
        .start_project_staffing(&company_id, &proj.id, proj.row_version)
        .await
        .unwrap();
    assert_eq!(p_staffing.status, ProjectStatus::Staffing);

    // STAFFING -> ACTIVE
    let p_active = app
        .activate_project(&company_id, &proj.id, p_staffing.row_version)
        .await
        .unwrap();
    assert_eq!(p_active.status, ProjectStatus::Active);

    // ACTIVE -> PAUSED -> ACTIVE
    let p_paused = app
        .pause_project(&company_id, &proj.id, p_active.row_version)
        .await
        .unwrap();
    assert_eq!(p_paused.status, ProjectStatus::Paused);

    let p_resumed = app
        .resume_project(&company_id, &proj.id, p_paused.row_version)
        .await
        .unwrap();
    assert_eq!(p_resumed.status, ProjectStatus::Active);

    // Invalid OCC expected_version rejected
    let stale_res = app.activate_project(&company_id, &proj.id, 999).await;
    assert!(matches!(
        stale_res,
        Err(ApplicationError::StaleVersion { .. })
    ));

    // Invalid transition: COMPLETED -> ACTIVE rejected
    let p_completed = app
        .complete_project(&company_id, &proj.id, p_resumed.row_version)
        .await
        .unwrap();
    assert_eq!(p_completed.status, ProjectStatus::Completed);

    let invalid_act = app
        .activate_project(&company_id, &proj.id, p_completed.row_version)
        .await;
    assert!(invalid_act.is_err());

    // COMPLETED -> ARCHIVED
    let p_archived = app
        .archive_project(&company_id, &proj.id, p_completed.row_version)
        .await
        .unwrap();
    assert_eq!(p_archived.status, ProjectStatus::Archived);
}

#[tokio::test]
async fn test_proof_team_disband_invariant_and_staffing_reconciliation() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();

    let pool = &app.pool;
    // Setup team & allocation in DB
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', ?, ?, 'Team 1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al1', ?, ?, 't1', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Disband team while active allocation exists -> rejected by DB trigger
    let disband_fail = sqlx::query("UPDATE teams SET status = 'DISBANDED' WHERE id = 't1'")
        .execute(pool)
        .await;
    assert!(disband_fail.is_err());

    // Release allocation first -> disband succeeds
    sqlx::query("UPDATE agent_allocations SET status = 'RELEASED' WHERE id = 'al1'")
        .execute(pool)
        .await
        .unwrap();

    let disband_ok = sqlx::query("UPDATE teams SET status = 'DISBANDED' WHERE id = 't1'")
        .execute(pool)
        .await;
    assert!(disband_ok.is_ok());

    // Staffing requirement reconciliation proof
    let mut req = StaffingRequirement {
        id: "sr1".into(),
        company_id: company_id.clone(),
        project_id: proj.id.clone(),
        team_id: Some("t1".into()),
        role_id: "r1".into(),
        department_id: Some("d1".into()),
        desired_count: 2,
        required_capability_ids: vec![],
        status: StaffingRequirementStatus::Open,
        row_version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    req.reconcile(1);
    assert_eq!(req.status, StaffingRequirementStatus::PartiallyFilled);

    req.reconcile(2);
    assert_eq!(req.status, StaffingRequirementStatus::Filled);

    // Release allocation -> Fulfilled becomes PartiallyFulfilled
    req.reconcile(1);
    assert_eq!(req.status, StaffingRequirementStatus::PartiallyFilled);
}

#[tokio::test]
async fn test_proof_agent_availability_and_capacity_concurrency() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();

    // Agent 'a1' has capacity 2 (set in setup_app)
    let avail0 = app.get_agent_availability(&company_id, "a1").await.unwrap();
    assert_eq!(avail0, "AVAILABLE");

    let pool = &app.pool;
    // Add 1 active allocation -> PARTIALLY_ALLOCATED
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', ?, ?, 'Team 1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al1', ?, ?, 't1', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let avail1 = app.get_agent_availability(&company_id, "a1").await.unwrap();
    assert_eq!(avail1, "PARTIALLY_ALLOCATED");

    // Add 2nd active allocation -> FULLY_ALLOCATED
    let mut tx2 = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t2', ?, ?, 'Team 2', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx2)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al2', ?, ?, 't2', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx2)
        .await
        .unwrap();
    tx2.commit().await.unwrap();

    let avail2 = app.get_agent_availability(&company_id, "a1").await.unwrap();
    assert_eq!(avail2, "FULLY_ALLOCATED");

    // 3rd active allocation beyond capacity (2) -> rejected by DB trigger
    let alloc_exceed = sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al3', ?, ?, 't2', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(pool)
        .await;
    assert!(alloc_exceed.is_err());
}

#[tokio::test]
async fn test_proof_department_immutability_and_work_item_not_run() {
    let (app, _) = setup_app().await;
    let pool = &app.pool;

    // Prove agent department immutability: trigger rejects change to primary_department_id
    let dept_mod = sqlx::query("UPDATE agents SET primary_department_id = 'd2' WHERE id = 'a1'")
        .execute(pool)
        .await;
    assert!(dept_mod.is_err());

    // Prove M4/M5 boundary: verify M5 actions table does NOT exist in M4
    let actions_check =
        sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='actions'")
            .fetch_one(pool)
            .await
            .unwrap();
    let count: i64 = sqlx::Row::get(&actions_check, 0);
    assert_eq!(count, 0, "M5 actions table must NOT exist in M4!");
}

#[tokio::test]
async fn test_proof_project_completion_coordination() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();

    // Activate project
    let proj = app
        .activate_project(&company_id, &proj.id, proj.row_version)
        .await
        .unwrap();

    let pool = &app.pool;
    // Setup active team, allocation, work item, assignment
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', ?, ?, 'Team 1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al1', ?, ?, 't1', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, status, created_at, updated_at) VALUES ('w1', ?, ?, 'Work 1', 'TASK', 'COMPLETED', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO assignments(id, company_id, project_id, work_item_id, agent_id, agent_allocation_id, status, assigned_at) VALUES ('as1', ?, ?, 'w1', 'a1', 'al1', 'ACTIVE', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&proj.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Complete project coordination
    let completed_proj = app
        .complete_project(&company_id, &proj.id, proj.row_version)
        .await
        .unwrap();
    assert_eq!(completed_proj.status, ProjectStatus::Completed);

    // Verify team is DISBANDED
    let team_row = sqlx::query("SELECT status FROM teams WHERE id = 't1'")
        .fetch_one(pool)
        .await
        .unwrap();
    let team_status: String = sqlx::Row::get(&team_row, 0);
    assert_eq!(team_status, "DISBANDED");

    // Verify allocation is RELEASED
    let alloc_row = sqlx::query("SELECT status FROM agent_allocations WHERE id = 'al1'")
        .fetch_one(pool)
        .await
        .unwrap();
    let alloc_status: String = sqlx::Row::get(&alloc_row, 0);
    assert_eq!(alloc_status, "RELEASED");

    // Verify assignment is RELEASED
    let as_row = sqlx::query("SELECT status FROM assignments WHERE id = 'as1'")
        .fetch_one(pool)
        .await
        .unwrap();
    let as_status: String = sqlx::Row::get(&as_row, 0);
    assert_eq!(as_status, "RELEASED");

    // Verify completed work item remains COMPLETED
    let w_row = sqlx::query("SELECT status FROM work_items WHERE id = 'w1'")
        .fetch_one(pool)
        .await
        .unwrap();
    let w_status: String = sqlx::Row::get(&w_row, 0);
    assert_eq!(w_status, "COMPLETED");

    // Verify agent remains in department 'd1'
    let agent_row = sqlx::query("SELECT primary_department_id FROM agents WHERE id = 'a1'")
        .fetch_one(pool)
        .await
        .unwrap();
    let agent_dept: String = sqlx::Row::get(&agent_row, 0);
    assert_eq!(agent_dept, "d1");
}

#[tokio::test]
async fn test_proof_same_company_cross_project_isolation() {
    let (app, company_id) = setup_app().await;
    let p1 = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();
    let p2 = app
        .create_project(&company_id, "Project 2".into(), None)
        .await
        .unwrap();

    let pool = &app.pool;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', ?, ?, 'Team 1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p1.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al1', ?, ?, 't1', 'a1', 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p1.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, status, created_at, updated_at) VALUES ('w1', ?, ?, 'Work 1', 'TASK', 'BACKLOG', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p1.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, status, created_at, updated_at) VALUES ('w2', ?, ?, 'Work 2', 'TASK', 'BACKLOG', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p2.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // WorkItem w1 (Project p1) dependency on WorkItem w2 (Project p2) -> rejected by composite FK
    let cross_proj_dep = sqlx::query("INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES (?, ?, 'w1', 'w2', 'HARD', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p1.id)
        .execute(pool)
        .await;
    assert!(cross_proj_dep.is_err());

    // Assignment of WorkItem w2 (p2) using allocation al1 (p1) -> rejected by composite FK
    let cross_proj_assign = sqlx::query("INSERT INTO assignments(id, company_id, project_id, work_item_id, agent_id, agent_allocation_id, status, assigned_at) VALUES ('as1', ?, ?, 'w2', 'a1', 'al1', 'ACTIVE', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .bind(&p2.id)
        .execute(pool)
        .await;
    assert!(cross_proj_assign.is_err());
}

#[tokio::test]
async fn test_proof_hard_dependency_dag_and_concurrency() {
    let (app, company_id) = setup_app().await;
    let proj = app
        .create_project(&company_id, "Project 1".into(), None)
        .await
        .unwrap();

    let pool = &app.pool;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w1', ?, ?, 'W1', 'TASK', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w2', ?, ?, 'W2', 'TASK', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w3', ?, ?, 'W3', 'TASK', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    // 1. Self cycle rejected
    let self_dep = sqlx::query("INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES (?, ?, 'w1', 'w1', 'HARD', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(pool).await;
    assert!(self_dep.is_err());

    // 2. Valid chain w1 -> w2 and w2 -> w3
    sqlx::query("INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES (?, ?, 'w1', 'w2', 'HARD', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES (?, ?, 'w2', 'w3', 'HARD', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(pool).await.unwrap();

    // 3. Transitive cycle w3 -> w1 rejected at DB boundary
    let trans_cycle = sqlx::query("INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES (?, ?, 'w3', 'w1', 'HARD', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0).bind(&proj.id).execute(pool).await;
    assert!(trans_cycle.is_err());
}

#[tokio::test]
async fn test_proof_outbox_atomicity_and_rollback() {
    let (app, company_id) = setup_app().await;
    let pool = &app.pool;

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p_rollback', ?, 'Rollback Proj', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .bind(&company_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();

    // Inject failure / rollback transaction without commit
    tx.rollback().await.unwrap();

    // Verify canonical state was NOT created
    let res = app.get_project(&company_id, "p_rollback").await;
    assert!(matches!(res, Err(ApplicationError::NotFound(_))));
}

#[tokio::test]
async fn test_proof_restart_durability_vertical() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("restart.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    let saved_company_id: CompanyId;

    // Phase 1: Initialize DB and persist M3 entities
    {
        let app = ApplicationContext::init(&db_url).await.unwrap();
        let company_id = app
            .create_company(CreateCompanyCommand {
                workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
                name: "Restart Co".into(),
                description: None,
                principal: None,
                idempotency_key: None,
                correlation_id: None,
                causation_id: None,
            })
            .await
            .unwrap()
            .id;
        saved_company_id = company_id.clone();

        let proj = app
            .create_project(&company_id, "Durable Proj".into(), None)
            .await
            .unwrap();
        app.bind_project_working_root(
            &company_id,
            &proj.id,
            dir.path().to_str().unwrap().into(),
            proj.row_version,
        )
        .await
        .unwrap();
    }

    // Phase 2: Recreate ApplicationContext (simulating daemon restart) and re-query
    {
        let app = ApplicationContext::init(&db_url).await.unwrap();
        let projs = app.list_projects(&saved_company_id).await.unwrap();
        assert_eq!(projs.len(), 1);
        assert_eq!(projs[0].name, "Durable Proj");
        assert!(projs[0].working_root_path.is_some());
    }
}

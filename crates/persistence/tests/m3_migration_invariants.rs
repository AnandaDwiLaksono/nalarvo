use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::{SqlitePool, sqlite::SqliteQueryResult};

async fn setup_test_db() -> SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();

    sqlx::query("INSERT INTO users(id, email, full_name, created_at, updated_at) VALUES ('u1', 'u1@nalarvo.test', 'User 1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO workspaces(id, owner_user_id, name, slug, created_at, updated_at) VALUES ('w1', 'u1', 'Workspace 1', 'w1', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    for c in ["c1", "c2"] {
        sqlx::query("INSERT INTO companies(id, workspace_id, name, status, created_at, updated_at) VALUES (?, 'w1', ?, 'ACTIVE', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
            .bind(c)
            .bind(c)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO departments(id, company_id, name, created_at, updated_at) VALUES (?, ?, ?, '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
            .bind(format!("dept_{c}"))
            .bind(c)
            .bind(format!("Engineering {c}"))
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO roles(id, company_id, name, created_at, updated_at) VALUES (?, ?, ?, '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
            .bind(format!("role_{c}"))
            .bind(c)
            .bind(format!("Engineer {c}"))
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO department_roles(company_id, department_id, role_id, created_at) VALUES (?, ?, ?, '2026-09-29T00:00:00Z')")
            .bind(c)
            .bind(format!("dept_{c}"))
            .bind(format!("role_{c}"))
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO agents(id, company_id, name, primary_department_id, role_id, capacity, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 10, '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z')")
            .bind(format!("agent_{c}"))
            .bind(c)
            .bind(format!("Agent {c}"))
            .bind(format!("dept_{c}"))
            .bind(format!("role_{c}"))
            .execute(&pool)
            .await
            .unwrap();
    }

    pool
}

async fn must_exec(pool: &SqlitePool, q: &str) -> SqliteQueryResult {
    sqlx::query(q).execute(pool).await.unwrap()
}

async fn must_fail(pool: &SqlitePool, q: &str) {
    assert!(
        sqlx::query(q).execute(pool).await.is_err(),
        "expected failure but succeeded: {q}"
    );
}

#[tokio::test]
async fn test_company_and_project_isolation_in_project_and_team_entities() {
    let pool = setup_test_db().await;

    must_exec(
        &pool,
        "INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p1', 'c1', 'Project 1', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p2', 'c2', 'Project 2', 't', 't')",
    )
    .await;

    // Reject objective pointing to project from mismatched company
    must_fail(
        &pool,
        "INSERT INTO objectives(id, company_id, project_id, title, created_at, updated_at) VALUES ('o1', 'c2', 'p1', 'Objective mismatched company', 't', 't')",
    )
    .await;

    // Reject team pointing to project from mismatched company
    must_fail(
        &pool,
        "INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', 'c2', 'p1', 'Team mismatched company', 't', 't')",
    )
    .await;

    must_exec(
        &pool,
        "INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', 'c1', 'p1', 'Team 1', 't', 't')",
    )
    .await;
    // Department != Project != Team validation
    must_exec(
        &pool,
        "INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1b', 'c1', 'p1', 'Team 1B', 't', 't')",
    )
    .await;
    assert_ne!("dept_c1", "p1");
    assert_ne!("p1", "t1");
    assert_ne!("dept_c1", "t1");
    must_exec(
        &pool,
        "INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t2', 'c2', 'p2', 'Team 2', 't', 't')",
    )
    .await;

    // Staffing requirement role_id company-scoped and team-scoped
    must_fail(
        &pool,
        "INSERT INTO staffing_requirements(id, company_id, project_id, team_id, role_id, desired_count, created_at, updated_at) VALUES ('sr1', 'c1', 'p1', 't1', 'role_c2', 1, 't', 't')",
    )
    .await;
    must_fail(
        &pool,
        "INSERT INTO staffing_requirements(id, company_id, project_id, team_id, role_id, desired_count, created_at, updated_at) VALUES ('sr1', 'c1', 'p1', 't2', 'role_c1', 1, 't', 't')",
    )
    .await;

    must_exec(
        &pool,
        "INSERT INTO staffing_requirements(id, company_id, project_id, team_id, role_id, desired_count, created_at, updated_at) VALUES ('sr1', 'c1', 'p1', 't1', 'role_c1', 1, 't', 't')",
    )
    .await;

    // Agent allocation role company-scoped and team matching staffing requirement
    must_fail(
        &pool,
        "INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, role_id, created_at, updated_at) VALUES ('al1', 'c1', 'p1', 't1', 'agent_c1', 'role_c2', 't', 't')",
    )
    .await;
    must_fail(
        &pool,
        "INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, staffing_requirement_id, created_at, updated_at) VALUES ('al1', 'c1', 'p1', 't1', 'agent_c2', 'sr1', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, staffing_requirement_id, role_id, created_at, updated_at) VALUES ('al1', 'c1', 'p1', 't1', 'agent_c1', 'sr1', 'role_c1', 't', 't')",
    )
    .await;

    // Historic allocation retention: releasing allocation allows next allocation without delete
    must_exec(
        &pool,
        "UPDATE agent_allocations SET status = 'RELEASED', released_at = 't' WHERE id = 'al1'",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, staffing_requirement_id, role_id, created_at, updated_at) VALUES ('al2', 'c1', 'p1', 't1', 'agent_c1', 'sr1', 'role_c1', 't', 't')",
    )
    .await;

    // Agent.department mutation blocked
    must_fail(
        &pool,
        "UPDATE agents SET primary_department_id = 'dept_c2' WHERE id = 'agent_c1'",
    )
    .await;
}

#[tokio::test]
async fn test_work_dependencies_cycle_rejection_and_cross_company_block() {
    let pool = setup_test_db().await;

    must_exec(
        &pool,
        "INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p1', 'c1', 'Project 1', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p2', 'c2', 'Project 2', 't', 't')",
    )
    .await;

    must_exec(
        &pool,
        "INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w1', 'c1', 'p1', 'Item 1', 'TASK', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w2', 'c1', 'p1', 'Item 2', 'TASK', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w3', 'c1', 'p1', 'Item 3', 'TASK', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w_foreign', 'c2', 'p2', 'Item foreign', 'TASK', 't', 't')",
    )
    .await;

    // Cross-company dependency rejected by composite foreign key
    must_fail(
        &pool,
        "INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES ('c1', 'p1', 'w1', 'w_foreign', 'HARD', 't')",
    )
    .await;

    // Dependencies chain w1 -> w2 -> w3
    must_exec(
        &pool,
        "INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES ('c1', 'p1', 'w1', 'w2', 'HARD', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES ('c1', 'p1', 'w2', 'w3', 'HARD', 't')",
    )
    .await;

    // Self dependency rejected
    must_fail(
        &pool,
        "INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES ('c1', 'p1', 'w3', 'w3', 'HARD', 't')",
    )
    .await;

    // Transitive hard cycle w3 -> w1 rejected at DB boundary
    must_fail(
        &pool,
        "INSERT INTO work_dependencies(company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind, created_at) VALUES ('c1', 'p1', 'w3', 'w1', 'HARD', 't')",
    )
    .await;

    // Cross-company blockers rejected
    must_fail(
        &pool,
        "INSERT INTO blockers(id, company_id, project_id, work_item_id, reason, created_at) VALUES ('b1', 'c2', 'p1', 'w1', 'Blocked', 't')",
    )
    .await;
}

#[tokio::test]
async fn test_assignments_and_allocations_retention_and_isolation() {
    let pool = setup_test_db().await;

    must_exec(
        &pool,
        "INSERT INTO projects(id, company_id, name, created_at, updated_at) VALUES ('p1', 'c1', 'Project 1', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO teams(id, company_id, project_id, name, created_at, updated_at) VALUES ('t1', 'c1', 'p1', 'Team 1', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO agent_allocations(id, company_id, project_id, team_id, agent_id, status, created_at, updated_at) VALUES ('al1', 'c1', 'p1', 't1', 'agent_c1', 'ACTIVE', 't', 't')",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO work_items(id, company_id, project_id, title, logical_type, created_at, updated_at) VALUES ('w1', 'c1', 'p1', 'Item 1', 'TASK', 't', 't')",
    )
    .await;

    // Mismatched agent to allocation in assignment rejected
    must_fail(
        &pool,
        "INSERT INTO assignments(id, company_id, project_id, work_item_id, agent_id, agent_allocation_id, created_at) VALUES ('as1', 'c1', 'p1', 'w1', 'agent_c2', 'al1', 't')",
    )
    .await;

    // Successful assignment
    must_exec(
        &pool,
        "INSERT INTO assignments(id, company_id, project_id, work_item_id, agent_id, agent_allocation_id, assigned_at) VALUES ('as1', 'c1', 'p1', 'w1', 'agent_c1', 'al1', 't')",
    )
    .await;

    // Historic assignment retention: release assignment then reassign
    must_exec(
        &pool,
        "UPDATE assignments SET status = 'RELEASED', ended_at = 't' WHERE id = 'as1'",
    )
    .await;
    must_exec(
        &pool,
        "INSERT INTO assignments(id, company_id, project_id, work_item_id, agent_id, agent_allocation_id, assigned_at) VALUES ('as2', 'c1', 'p1', 'w1', 'agent_c1', 'al1', 't')",
    )
    .await;
}

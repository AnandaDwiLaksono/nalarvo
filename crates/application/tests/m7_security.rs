//! F1/F2 permanent regressions: real application transactions, isolated SQLite.
use nalarvo_application::{m7::StaffTeamCommand, *};
use nalarvo_domain::*;
use serde_json::json;

#[tokio::test]
async fn f1_assignment_replacement_failure_preserves_old_assignment() {
    let (app, c, p, _, al) = fixture().await;
    let work = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "Assigned"))
        .await
        .unwrap();
    let allocation = app.get_agent_allocation(&c, &p, &al).await.unwrap();
    let old = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            c.clone(),
            p.clone(),
            work.id,
            allocation.agent_id,
            al.clone(),
            true,
        ))
        .await
        .unwrap();
    assert!(
        app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2)
            .await
            .is_err()
    );
    sqlx::query("CREATE TRIGGER reject_reassignment BEFORE INSERT ON assignments BEGIN SELECT RAISE(ABORT,'injected assignment failure'); END").execute(&app.pool).await.unwrap();
    let before = state(&app).await;
    assert!(
        app.reassign_work_assignment(&c, &p, &old.id, &al, 1)
            .await
            .is_err()
    );
    assert_eq!(before, state(&app).await);
    sqlx::query("DROP TRIGGER reject_reassignment")
        .execute(&app.pool)
        .await
        .unwrap();
    let replacement = app
        .reassign_work_assignment(&c, &p, &old.id, &al, 1)
        .await
        .unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(
        app.get_work_assignment(&c, &p, &old.id)
            .await
            .unwrap()
            .status,
        AssignmentStatus::Released
    );
}

#[tokio::test]
async fn f1_revocation_committed_before_legacy_writer_is_observed() {
    let dir = tempfile::tempdir().unwrap();
    let (app, c, p, _, al) = fixture_at(&format!(
        "sqlite://{}",
        dir.path().join("authority.db").display()
    ))
    .await;
    let mut tx = app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE permission_grants SET revoked_at='t'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = {
        let app = app.clone();
        let c = c.clone();
        let p = p.clone();
        tokio::spawn(async move {
            app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2)
                .await
        })
    };
    tokio::task::yield_now().await;
    tx.commit().await.unwrap();
    let before = state(&app).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(ApplicationError::Unauthorized)
    ));
    assert_eq!(before, state(&app).await);
    app.pool.close().await;
}

#[tokio::test]
async fn f2_completion_waits_for_committed_model_settlement_and_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let (app, c, p, t, _) = fixture_at(&format!(
        "sqlite://{}",
        dir.path().join("completion.db").display()
    ))
    .await;
    app.activate_project(&c, &p, 1).await.unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,reserved_amount,created_at,updated_at) VALUES('b','0191e4b8-0002-7000-8000-000000000001',?,?,'B',10,1,'t','t')").bind(&c.0).bind(&p).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,status,created_at) VALUES('model:interrupted:b','b','0191e4b8-0002-7000-8000-000000000001',?,'MODEL_CALL',1,'HELD','t')").bind(&c.0).execute(&app.pool).await.unwrap();
    let mut tx = app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE resource_reservations SET status='RELEASED',released_at='t' WHERE id='model:interrupted:b'").execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE budgets SET reserved_amount=0 WHERE id='b'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = {
        let app = app.clone();
        let cmd = cmd(&c, &p, &t);
        tokio::spawn(async move { app.complete_project_governed(cmd, 2).await })
    };
    tokio::task::yield_now().await;
    tx.commit().await.unwrap();
    assert_eq!(
        task.await.unwrap().unwrap().status,
        ProjectStatus::Completed
    );
    assert_eq!(
        app.get_team(&c, &p, &t).await.unwrap().status,
        TeamStatus::Disbanded
    );
    assert!(
        app.list_agent_allocations(&c, &p)
            .await
            .unwrap()
            .iter()
            .all(|a| a.status == AgentAllocationStatus::Released)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_reservations WHERE status='HELD'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
    app.pool.close().await;
}

#[tokio::test]
async fn f1_concurrent_graph_and_release_are_serialized() {
    let (app, c, p, _, al) = fixture().await;
    let a = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "A"))
        .await
        .unwrap();
    let b = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "B"))
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        app.create_work_dependency(CreateWorkDependencyCommand::new(
            c.clone(),
            p.clone(),
            a.id.clone(),
            b.id.clone(),
            DependencyType::Hard
        )),
        app.create_work_dependency(CreateWorkDependencyCommand::new(
            c.clone(),
            p.clone(),
            b.id.clone(),
            a.id.clone(),
            DependencyType::Hard
        ))
    );
    assert_ne!(left.is_ok(), right.is_ok());
    assert_eq!(app.list_work_dependencies(&c, &p).await.unwrap().len(), 1);
    let (left, right) = tokio::join!(
        app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2),
        app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2)
    );
    assert_ne!(left.is_ok(), right.is_ok());
    assert_eq!(
        app.get_agent_allocation(&c, &p, &al)
            .await
            .unwrap()
            .row_version,
        3
    );
}

#[tokio::test]
async fn f2_historical_unknown_and_approval_are_scoped_and_resolved_explicitly() {
    let (app, c, p, t, _) = fixture().await;
    app.activate_project(&c, &p, 1).await.unwrap();
    let other = app.create_project(&c, "Other".into(), None).await.unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Optional historical work",
        ))
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_requirements(work_item_id,company_id,project_id,role_id,priority,required,acceptance_contains) SELECT ?,?,?,role_id,0,0,'done' FROM agents LIMIT 1").bind(&work.id).bind(&c.0).bind(&p).execute(&app.pool).await.unwrap();
    let agent: String = sqlx::query_scalar("SELECT agent_id FROM agent_allocations LIMIT 1")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,trigger_type,lifecycle_state,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at) VALUES('historical',?,?,?,?,'MANUAL','FAILED',1,'USER',?,'t','historical','t','t')").bind(&c.0).bind(&p).bind(&work.id).bind(agent).bind(OWNER).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO actions(id,workspace_id,company_id,run_id,requested_by_type,requested_by_id,action_type,target,authorization,status,created_at,updated_at) VALUES('unknown','0191e4b8-0002-7000-8000-000000000001',?,'historical','USER',?,'file.write','{}','ALLOW','OUTCOME_UNKNOWN','t','t')").bind(&c.0).bind(OWNER).execute(&app.pool).await.unwrap();
    let before = state(&app).await;
    assert!(
        app.complete_project_governed(cmd(&c, &p, &t), 2)
            .await
            .is_err()
    );
    assert_eq!(before, state(&app).await);
    // Synthetic explicit reconciliation; no retry, deletion or automatic resolution by completion.
    sqlx::query("UPDATE runs SET lifecycle_state='QUEUED' WHERE id='historical'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE actions SET status='SUCCEEDED' WHERE id='unknown'")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(
        app.complete_project_governed(cmd(&c, &p, &t), 2)
            .await
            .is_err()
    );
    sqlx::query("UPDATE runs SET lifecycle_state='FAILED' WHERE id='historical'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO approval_requests(id,action_id,workspace_id,company_id,requested_by_id,status,requested_at) VALUES('approval','unknown','0191e4b8-0002-7000-8000-000000000001',?,?,'PENDING','t')").bind(&c.0).bind(OWNER).execute(&app.pool).await.unwrap();
    assert!(
        app.complete_project_governed(cmd(&c, &p, &t), 2)
            .await
            .is_err()
    );
    sqlx::query("UPDATE approval_requests SET status='APPROVED' WHERE id='approval'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,reserved_amount,created_at,updated_at) VALUES('other-budget','0191e4b8-0002-7000-8000-000000000001',?,?,'Other',10,1,'t','t')").bind(&c.0).bind(&other.id).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,status,created_at) VALUES('other-held','other-budget','0191e4b8-0002-7000-8000-000000000001',?,'MODEL_CALL',1,'HELD','t')").bind(&c.0).execute(&app.pool).await.unwrap();
    assert_eq!(
        app.complete_project_governed(cmd(&c, &p, &t), 2)
            .await
            .unwrap()
            .status,
        ProjectStatus::Completed
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM resource_reservations WHERE id='other-held'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        "HELD"
    );
}
const OWNER: &str = "0191e4b8-0001-7000-8000-000000000001";
async fn fixture() -> (ApplicationContext, CompanyId, String, String, String) {
    fixture_at("sqlite::memory:").await
}
async fn fixture_at(url: &str) -> (ApplicationContext, CompanyId, String, String, String) {
    let app = ApplicationContext::init(url).await.unwrap();
    let ws = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let c = app
        .create_company(CreateCompanyCommand {
            workspace_id: ws.clone(),
            name: "Security".into(),
            description: None,
            principal: Some(PrincipalRef::user(OWNER)),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap()
        .id;
    app.ensure_canonical_permissions(&ws.0).await.unwrap();
    for permission in ["project.modify", "project.allocate_agent"] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES(?,?,?,?, 'USER',?,'COMPANY',?,'ALLOW','t')").bind(permission).bind(&ws.0).bind(&c.0).bind(format!("pd_{permission}")).bind(OWNER).bind(&c.0).execute(&app.pool).await.unwrap();
    }
    app.activate_company(&ws, &c, 1, PrincipalRef::user(OWNER))
        .await
        .unwrap();
    let p = app
        .create_project(&c, "Project".into(), None)
        .await
        .unwrap()
        .id;
    let d = app.create_department(&c, "D").await.unwrap();
    let r = app.create_role(&c, "R").await.unwrap();
    let a = app
        .create_agent(&c, "Agent", &d.id, &r.id, None, 1)
        .await
        .unwrap();
    let t = app
        .create_team(CreateTeamCommand::new(c.clone(), p.clone(), "Team"))
        .await
        .unwrap()
        .id;
    let al = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            c.clone(),
            p.clone(),
            t.clone(),
            a.id,
            None,
        ))
        .await
        .unwrap();
    app.transition_agent_allocation(&c, &p, &al.id, AgentAllocationStatus::Active, 1)
        .await
        .unwrap();
    (app, c, p, t, al.id)
}
fn cmd(c: &CompanyId, p: &str, t: &str) -> StaffTeamCommand {
    StaffTeamCommand {
        company_id: c.clone(),
        project_id: p.into(),
        team_id: t.into(),
        meta: CommandMeta {
            principal: Some(PrincipalRef::user(OWNER)),
            idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
            ..Default::default()
        },
    }
}
async fn state(app: &ApplicationContext) -> serde_json::Value {
    use sqlx::Row;
    let mut result = serde_json::Map::new();
    for table in [
        "work_dependencies",
        "agent_allocations",
        "assignments",
        "work_items",
        "teams",
        "projects",
        "domain_events",
        "audit_records",
        "outbox_messages",
        "idempotency_records",
    ] {
        let columns = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
            .fetch_all(&app.pool)
            .await
            .unwrap();
        let expression = columns
            .iter()
            .map(|c| format!("quote(\"{}\")", c.get::<String, _>("name")))
            .collect::<Vec<_>>()
            .join("||'|'||");
        let rows: Vec<String> =
            sqlx::query_scalar(&format!("SELECT {expression} FROM \"{table}\" ORDER BY 1"))
                .fetch_all(&app.pool)
                .await
                .unwrap();
        result.insert(table.into(), json!(rows));
    }
    // Snapshot actual mutable business values, not just row counts.
    result.insert(
        "allocations".into(),
        json!(
            sqlx::query_as::<_, (String, String, i64)>(
                "SELECT id,status,row_version FROM agent_allocations ORDER BY id"
            )
            .fetch_all(&app.pool)
            .await
            .unwrap()
        ),
    );
    result.insert(
        "work".into(),
        json!(
            sqlx::query_as::<_, (String, String, i64)>(
                "SELECT id,status,row_version FROM work_items ORDER BY id"
            )
            .fetch_all(&app.pool)
            .await
            .unwrap()
        ),
    );
    result.insert(
        "project".into(),
        json!(
            sqlx::query_as::<_, (String, String, i64)>(
                "SELECT id,status,row_version FROM projects ORDER BY id"
            )
            .fetch_all(&app.pool)
            .await
            .unwrap()
        ),
    );
    result.insert(
        "team".into(),
        json!(
            sqlx::query_as::<_, (String, String, i64)>(
                "SELECT id,status,row_version FROM teams ORDER BY id"
            )
            .fetch_all(&app.pool)
            .await
            .unwrap()
        ),
    );
    serde_json::Value::Object(result)
}
#[tokio::test]
async fn f1_current_authority_guards_legacy_mutations_without_business_writes() {
    let (app, c, p, _, al) = fixture().await;
    let a = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "A"))
        .await
        .unwrap();
    let b = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "B"))
        .await
        .unwrap();
    let create = CreateWorkDependencyCommand::new(
        c.clone(),
        p.clone(),
        a.id.clone(),
        b.id.clone(),
        DependencyType::Hard,
    )
    .idempotency("dependency");
    let dep = app.create_work_dependency(create.clone()).await.unwrap();
    for mutation in [
        "effect='DENY'",
        "effect='ALLOW',revoked_at='t'",
        "effect='ALLOW',revoked_at=NULL,expires_at='2000-01-01T00:00:00Z'",
    ] {
        sqlx::query(&format!("UPDATE permission_grants SET {mutation}"))
            .execute(&app.pool)
            .await
            .unwrap();
        let before = state(&app).await;
        assert!(
            matches!(
                app.create_work_dependency(create.clone()).await,
                Err(ApplicationError::Unauthorized)
            ),
            "replay must check current authority"
        );
        assert!(matches!(
            app.delete_work_dependency(&c, &p, &dep.id).await,
            Err(ApplicationError::Unauthorized)
        ));
        assert!(matches!(
            app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2)
                .await,
            Err(ApplicationError::Unauthorized)
        ));
        assert_eq!(before, state(&app).await);
    }
    sqlx::query("DELETE FROM permission_grants")
        .execute(&app.pool)
        .await
        .unwrap();
    let before = state(&app).await;
    assert!(matches!(
        app.delete_work_dependency(&c, &p, &dep.id).await,
        Err(ApplicationError::Unauthorized)
    ));
    assert_eq!(before, state(&app).await);
}
#[tokio::test]
async fn f1_authorized_graph_history_scope_and_allocation_controls() {
    let (app, c, p, _, al) = fixture().await;
    let a = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "A"))
        .await
        .unwrap();
    let b = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "B"))
        .await
        .unwrap();
    let make = || {
        CreateWorkDependencyCommand::new(
            c.clone(),
            p.clone(),
            a.id.clone(),
            b.id.clone(),
            DependencyType::Hard,
        )
    };
    let dep = app.create_work_dependency(make()).await.unwrap();
    let before = state(&app).await;
    assert!(
        app.delete_work_dependency(&c, "other-project", &dep.id)
            .await
            .is_err()
    );
    assert!(
        app.delete_work_dependency(&CompanyId("foreign".into()), &p, &dep.id)
            .await
            .is_err()
    );
    assert!(
        app.transition_agent_allocation(
            &c,
            "other-project",
            &al,
            AgentAllocationStatus::Released,
            2
        )
        .await
        .is_err()
    );
    assert_eq!(before, state(&app).await);
    app.delete_work_dependency(&c, &p, &dep.id).await.unwrap();
    sqlx::query("UPDATE work_items SET status='COMPLETED' WHERE id=?")
        .bind(&a.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let before = state(&app).await;
    assert!(
        app.create_work_dependency(make()).await.is_err(),
        "completed history immutable"
    );
    assert_eq!(before, state(&app).await);
    app.transition_agent_allocation(&c, &p, &al, AgentAllocationStatus::Released, 2)
        .await
        .unwrap();
    assert_eq!(
        app.get_agent_allocation(&c, &p, &al).await.unwrap().status,
        AgentAllocationStatus::Released
    );
}
#[tokio::test]
async fn f2_actionless_reservation_blocks_both_completion_paths_until_settled() {
    let (app, c, p, t, _) = fixture().await;
    app.activate_project(&c, &p, 1).await.unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,reserved_amount,created_at,updated_at) VALUES('b','0191e4b8-0002-7000-8000-000000000001',?,?,'B',10,1,'t','t')").bind(&c.0).bind(&p).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,status,created_at) VALUES('model:interrupted:b','b','0191e4b8-0002-7000-8000-000000000001',?,'MODEL_CALL',1,'HELD','t')").bind(&c.0).execute(&app.pool).await.unwrap();
    let before = state(&app).await;
    assert!(
        app.complete_project_governed(cmd(&c, &p, &t), 2)
            .await
            .is_err()
    );
    assert!(app.complete_project(&c, &p, 2).await.is_err());
    assert_eq!(before, state(&app).await);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM resource_reservations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "HELD"
    );
    nalarvo_persistence::m5::release_model_reservation(&app.pool, "interrupted")
        .await
        .unwrap();
    let result = app
        .complete_project_governed(cmd(&c, &p, &t), 2)
        .await
        .unwrap();
    assert_eq!(result.status, ProjectStatus::Completed);
    assert_eq!(
        app.get_team(&c, &p, &t).await.unwrap().status,
        TeamStatus::Disbanded
    );
    assert!(
        app.list_agent_allocations(&c, &p)
            .await
            .unwrap()
            .iter()
            .all(|a| a.status == AgentAllocationStatus::Released)
    );
}

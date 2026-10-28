use nalarvo_application::{m4::CreateRunCommand, m7::StaffTeamCommand, *};
use nalarvo_domain::{CompanyId, PrincipalRef, StaffingRequirementStatus, WorkspaceId};

const OWNER: &str = "0191e4b8-0001-7000-8000-000000000001";

#[tokio::test]
async fn succeeded_evaluation_failure_requires_revision_not_execution_retry() {
    let (app, c, p, t) = fixture("sqlite::memory:").await;
    let work = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "Revision"))
        .await
        .unwrap();
    app.staff_team(command(&c, &p, &t, "staff")).await.unwrap();
    sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
        .bind(&p)
        .execute(&app.pool)
        .await
        .unwrap();
    app.configure_work(
        command(&c, &p, &t, "config"),
        &work.id,
        nalarvo_application::m7_work::WorkRequirements {
            role_id: "engineer".into(),
            priority: 0,
            required: true,
            acceptance_contains: "done".into(),
            input_version_ids: vec![],
        },
    )
    .await
    .unwrap();
    let run = app
        .schedule_work(command(&c, &p, &t, "schedule"))
        .await
        .unwrap()
        .remove(0);
    app.begin_scheduled_work(&c.0, &p, &work.id).await.unwrap();
    sqlx::query("UPDATE runs SET lifecycle_state='SUCCEEDED' WHERE id=?")
        .bind(&run.id)
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_evaluations VALUES ('eval',?,?,?,?,NULL,'NEEDS_REVISION','DETERMINISTIC',0,'done','missing output',?,'2026-01-01')").bind(&c.0).bind(&p).bind(&work.id).bind(&run.id).bind(OWNER).execute(&app.pool).await.unwrap();
    let mut retry =
        CreateRunCommand::new(c.clone(), p.clone(), work.id.clone(), "engineer-a", "RETRY");
    retry.retry_of_run_id = Some(run.id.clone());
    assert!(
        app.create_run(retry).await.is_err(),
        "SUCCEEDED must not enter global execution retry"
    );
    assert!(app.retry_run(&c, &p, &run.id, 1, None).await.is_err());
    let revision = app
        .revise_work_attempt(
            command(&c, &p, &t, "revise"),
            &run.id,
            "Use corrected implementation",
            1,
        )
        .await
        .unwrap();
    assert_eq!(revision.trigger_type, "REVISION");
    assert_eq!(revision.retry_of_run_id.as_deref(), Some(run.id.as_str()));
    assert_eq!(revision.attempt_number, 2);
    assert_eq!(
        app.get_run(&c, &p, &run.id).await.unwrap().status,
        nalarvo_domain::RunStatus::Succeeded
    );
}

#[tokio::test]
async fn completion_rejects_unaccepted_work_then_releases_persistent_agents() {
    let (app, c, p, t) = fixture("sqlite::memory:").await;
    app.staff_team(command(&c, &p, &t, "staff")).await.unwrap();
    sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
        .bind(&p)
        .execute(&app.pool)
        .await
        .unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "Required"))
        .await
        .unwrap();
    let project = app.get_project(&c, &p).await.unwrap();
    let cmd = command(&c, &p, &t, "complete");
    assert!(
        app.complete_project_governed(cmd.clone(), project.row_version)
            .await
            .is_err()
    );
    assert!(
        app.complete_project(&c, &p, project.row_version)
            .await
            .is_err()
    );
    sqlx::query("UPDATE work_items SET status='COMPLETED' WHERE id=?")
        .bind(&work.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let completed = app
        .complete_project_governed(cmd.clone(), project.row_version)
        .await
        .unwrap();
    assert_eq!(completed.status, nalarvo_domain::ProjectStatus::Completed);
    let replay = app
        .complete_project_governed(cmd, project.row_version)
        .await
        .unwrap();
    assert_eq!(completed.row_version, replay.row_version);
    assert!(
        app.list_agent_allocations(&c, &p)
            .await
            .unwrap()
            .iter()
            .all(
                |a| a.status == nalarvo_domain::AgentAllocationStatus::Released
                    && a.released_at.is_some()
            )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM teams WHERE status='DISBANDED'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agents")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        6
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM domain_events e JOIN outbox_messages o ON o.domain_event_id=e.id WHERE e.event_type='ProjectCompleted'").fetch_one(&app.pool).await.unwrap(),1);
}

#[tokio::test]
async fn permission_and_policy_deny_roll_back_all_staffing_effects() {
    let (app, c, p, t) = fixture("sqlite::memory:").await;
    sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE permission_definition_id='pd_project.allocate_agent'").execute(&app.pool).await.unwrap();
    assert!(app.staff_team(command(&c, &p, &t, "deny")).await.is_err());
    assert!(app.list_agent_allocations(&c, &p).await.unwrap().is_empty());
    sqlx::query("UPDATE permission_grants SET effect='ALLOW'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES ('deny','0191e4b8-0002-7000-8000-000000000001',?,'deny','ACTIVE',1,'t','t')")
        .bind(&c.0).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions VALUES ('deny-v','deny',1,?, 't')")
        .bind(
            serde_json::json!({"permission":"project.allocate_agent","effect":"DENY"}).to_string(),
        )
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(app.staff_team(command(&c, &p, &t, "policy")).await.is_err());
    assert!(app.list_agent_allocations(&c, &p).await.unwrap().is_empty());
}

#[tokio::test]
async fn scheduling_orders_ready_work_and_serializes_duplicate_delivery() {
    use nalarvo_application::m7_work::WorkRequirements;
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("schedule.db").display());
    let (app, c, p, t) = fixture(&url).await;
    app.staff_team(command(&c, &p, &t, "staff")).await.unwrap();
    sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
        .bind(&p)
        .execute(&app.pool)
        .await
        .unwrap();
    let mut ids = Vec::new();
    for (name, role, priority) in [("A", "engineer", 2), ("B", "pm", 3), ("C", "qa", 100)] {
        let work = app
            .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), name))
            .await
            .unwrap();
        app.configure_work(
            command(&c, &p, &t, "configure"),
            &work.id,
            WorkRequirements {
                role_id: role.into(),
                priority,
                required: true,
                acceptance_contains: "done".into(),
                input_version_ids: vec![],
            },
        )
        .await
        .unwrap();
        ids.push(work.id);
    }
    sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD','2026-01-01')")
        .bind(&c.0)
        .bind(&p)
        .bind(&ids[2])
        .bind(&ids[0])
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(!app.work_readiness(&c.0, &p, &ids[2]).await.unwrap().ready);
    assert!(
        sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD','2026-01-01')")
            .bind(&c.0)
            .bind(&p)
            .bind(&ids[0])
            .bind(&ids[2])
            .execute(&app.pool)
            .await
            .is_err()
    );
    let cmd = command(&c, &p, &t, "schedule");
    let (a, b) = tokio::join!(app.schedule_work(cmd.clone()), app.schedule_work(cmd));
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        a.iter()
            .map(|r| r.work_item_id.as_str())
            .collect::<Vec<_>>(),
        vec![ids[1].as_str(), ids[0].as_str()]
    );
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap()
    );
    assert!(
        app.schedule_work(command(&c, &p, &t, "again"))
            .await
            .unwrap()
            .is_empty()
    );
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM durable_jobs")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(jobs, 2);
    app.pool.close().await;
    let reopened = ApplicationContext::init(&url).await.unwrap();
    assert!(
        reopened
            .schedule_work(command(&c, &p, &t, "restart"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(reopened.list_runs(&c, &p).await.unwrap().len(), 2);
}

async fn fixture(url: &str) -> (ApplicationContext, CompanyId, String, String) {
    let app = ApplicationContext::init(url).await.unwrap();
    let company = app
        .create_company(CreateCompanyCommand {
            workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
            name: "M7".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap()
        .id;
    app.ensure_canonical_permissions("0191e4b8-0002-7000-8000-000000000001")
        .await
        .unwrap();
    for permission in ["project.allocate_agent", "project.modify"] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?,'0191e4b8-0002-7000-8000-000000000001',?,?,'USER',?,'COMPANY',?,'ALLOW','2026-01-01')")
            .bind(format!("grant-{permission}")).bind(&company.0).bind(format!("pd_{permission}")).bind(OWNER).bind(&company.0).execute(&app.pool).await.unwrap();
    }
    sqlx::query("UPDATE companies SET status='ACTIVE' WHERE id=?")
        .bind(&company.0)
        .execute(&app.pool)
        .await
        .unwrap();
    for role in ["pm", "engineer", "qa"] {
        for sql in [
            "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES (?,?,?,'2026-01-01','2026-01-01')",
            "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES (?,?,?,'2026-01-01','2026-01-01')",
        ] {
            sqlx::query(sql)
                .bind(role)
                .bind(&company.0)
                .bind(role)
                .execute(&app.pool)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES (?,?,?,'2026-01-01')").bind(&company.0).bind(role).bind(role).execute(&app.pool).await.unwrap();
        for suffix in ["a", "b"] {
            let id = format!("{role}-{suffix}");
            sqlx::query("INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES (?,?,?,?,?,1,'2026-01-01','2026-01-01')")
                .bind(&id).bind(&company.0).bind(&id).bind(role).bind(role).execute(&app.pool).await.unwrap();
        }
    }
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
    for role in ["pm", "engineer", "qa"] {
        let req = app
            .create_staffing_requirement(CreateStaffingRequirementCommand::new(
                company.clone(),
                project.id.clone(),
                Some(team.id.clone()),
                role,
                1,
            ))
            .await
            .unwrap();
        app.transition_staffing_requirement(
            &company,
            &project.id,
            &req.id,
            StaffingRequirementStatus::Open,
            1,
        )
        .await
        .unwrap();
    }
    (app, company, project.id, team.id)
}

fn command(company: &CompanyId, project: &str, team: &str, key: &str) -> StaffTeamCommand {
    StaffTeamCommand {
        company_id: company.clone(),
        project_id: project.into(),
        team_id: team.into(),
        meta: CommandMeta {
            principal: Some(PrincipalRef::user(OWNER)),
            idempotency_key: Some(key.into()),
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn deterministic_matching_activation_replay_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("staff.db").display());
    let (app, c, p, t) = fixture(&url).await;
    let cmd = command(&c, &p, &t, "staff-1");
    let result = app.staff_team(cmd.clone()).await.unwrap();
    assert!(result.shortages.is_empty());
    assert!(result.team_active);
    let mut agents: Vec<_> = result
        .allocations
        .iter()
        .map(|a| a.agent_id.as_str())
        .collect();
    agents.sort();
    assert_eq!(agents, vec!["engineer-a", "pm-a", "qa-a"]);
    assert_eq!(
        serde_json::to_value(&result).unwrap(),
        serde_json::to_value(app.staff_team(cmd.clone()).await.unwrap()).unwrap()
    );
    assert_eq!(app.list_agent_allocations(&c, &p).await.unwrap().len(), 3);
    app.pool.close().await;
    let reopened = ApplicationContext::init(&url).await.unwrap();
    assert_eq!(
        reopened.list_agent_allocations(&c, &p).await.unwrap().len(),
        3
    );
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::to_value(reopened.staff_team(cmd).await.unwrap()).unwrap()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents")
        .fetch_one(&reopened.pool)
        .await
        .unwrap();
    assert_eq!(count, 6);
}

#[tokio::test]
async fn shortage_capabilities_scope_and_current_authority() {
    let (app, c, p, t) = fixture("sqlite::memory:").await;
    sqlx::query("UPDATE staffing_requirements SET required_capability_ids='[\"rust\"]' WHERE role_id='engineer'")
        .execute(&app.pool).await.unwrap();
    sqlx::query("UPDATE agents SET status='PAUSED' WHERE role_id='qa'")
        .execute(&app.pool)
        .await
        .unwrap();
    let mut cmd = command(&c, &p, &t, "shortage");
    let result = app.staff_team(cmd.clone()).await.unwrap();
    assert!(!result.team_active);
    assert_eq!(result.allocations.len(), 1);
    assert_eq!(result.shortages.len(), 2);
    assert!(
        result
            .shortages
            .iter()
            .all(|s| s.missing == 1 && s.filled == 0)
    );
    let mut wrong = cmd.clone();
    wrong.project_id = "foreign-project".into();
    assert!(app.staff_team(wrong).await.is_err());
    wrong = cmd.clone();
    wrong.meta.principal = Some(PrincipalRef::system("forged"));
    assert!(app.staff_team(wrong).await.is_err());
    wrong = cmd.clone();
    wrong.team_id = "foreign-team".into();
    assert!(app.staff_team(wrong).await.is_err());
    sqlx::query("UPDATE workspaces SET status='SUSPENDED'")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(app.staff_team(cmd.clone()).await.is_err());
    sqlx::query("UPDATE workspaces SET status='ACTIVE'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_capabilities VALUES (?,'engineer-b','rust')")
        .bind(&c.0)
        .execute(&app.pool)
        .await
        .unwrap();
    cmd.meta.idempotency_key = Some("fill-later".into());
    let next = app.staff_team(cmd).await.unwrap();
    assert_eq!(next.allocations.len(), 1);
    assert_eq!(next.allocations[0].agent_id, "engineer-b");
    assert_eq!(next.shortages.len(), 1);
    assert_eq!(app.list_agent_allocations(&c, &p).await.unwrap().len(), 2);
}

#[tokio::test]
async fn competing_staffing_serializes_last_capacity_and_release_keeps_history() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("race.db").display());
    let (app, c, p, t) = fixture(&url).await;
    sqlx::query("UPDATE agents SET status='PAUSED' WHERE id LIKE '%-b'")
        .execute(&app.pool)
        .await
        .unwrap();
    let other = app.create_project(&c, "Second".into(), None).await.unwrap();
    let team = app
        .create_team(CreateTeamCommand::new(c.clone(), other.id.clone(), "Other"))
        .await
        .unwrap();
    let req = app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            c.clone(),
            other.id.clone(),
            Some(team.id.clone()),
            "engineer",
            1,
        ))
        .await
        .unwrap();
    app.transition_staffing_requirement(&c, &other.id, &req.id, StaffingRequirementStatus::Open, 1)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        app.staff_team(command(&c, &p, &t, "one")),
        app.staff_team(command(&c, &other.id, &team.id, "two"))
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        a.allocations
            .iter()
            .filter(|v| v.agent_id == "engineer-a")
            .count()
            + b.allocations
                .iter()
                .filter(|v| v.agent_id == "engineer-a")
                .count(),
        1
    );
    let oversubscribed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents a WHERE (SELECT COUNT(*) FROM agent_allocations al WHERE al.agent_id=a.id AND al.status='ACTIVE')>a.capacity").fetch_one(&app.pool).await.unwrap();
    assert_eq!(oversubscribed, 0);
    let chosen = a
        .allocations
        .iter()
        .chain(&b.allocations)
        .find(|v| v.agent_id == "engineer-a")
        .unwrap();
    app.transition_agent_allocation(
        &c,
        &chosen.project_id,
        &chosen.id,
        nalarvo_domain::AgentAllocationStatus::Released,
        chosen.row_version,
    )
    .await
    .unwrap();
    let next = app
        .staff_team(command(&c, &chosen.project_id, &chosen.team_id, "restaff"))
        .await
        .unwrap();
    assert!(
        next.allocations
            .iter()
            .any(|v| v.agent_id == "engineer-a" && v.id != chosen.id)
    );
    let old = app
        .get_agent_allocation(&c, &chosen.project_id, &chosen.id)
        .await
        .unwrap();
    assert_eq!(old.status, nalarvo_domain::AgentAllocationStatus::Released);
    assert!(old.released_at.is_some());
}

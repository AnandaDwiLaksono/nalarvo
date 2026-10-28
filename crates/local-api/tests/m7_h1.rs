use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use nalarvo_application::{m7::StaffTeamCommand, m7_work::WorkRequirements, *};
use nalarvo_domain::{CompanyId, PrincipalRef, StaffingRequirementStatus, WorkspaceId};
use serde_json::{Value, json};
use tower::ServiceExt;
const OWNER: &str = "0191e4b8-0001-7000-8000-000000000001";

#[tokio::test]
async fn f1_http_dependency_and_allocation_mutations_deny_without_writes() {
    let (app, c, p, t) = fixture("sqlite::memory:").await;
    let staffed = app
        .staff_team(command(&c, &p, &t, "f1-staff"))
        .await
        .unwrap();
    let allocation = &staffed.allocations[0];
    let a = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "A"))
        .await
        .unwrap();
    let b = app
        .create_work_item(CreateWorkItemCommand::new(c.clone(), p.clone(), "B"))
        .await
        .unwrap();
    let dep = app
        .create_work_dependency(CreateWorkDependencyCommand::new(
            c.clone(),
            p.clone(),
            a.id.clone(),
            b.id.clone(),
            nalarvo_domain::DependencyType::Hard,
        ))
        .await
        .unwrap();
    sqlx::query("UPDATE permission_grants SET effect='DENY'")
        .execute(&app.pool)
        .await
        .unwrap();
    let router = nalarvo_local_api::router_with_app("scratch-local-only", Some(app.clone()));
    let base = format!("/api/v1/companies/{}/projects/{}", c.0, p);
    let before = mutation_snapshot(&app).await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("{base}/work/{}/dependencies/{}", a.id, dep.id))
                .header("Authorization", "Bearer scratch-local-only")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        post(
            &router,
            &format!("{base}/work/{}/dependencies", b.id),
            json!({"depends_on_work_item_id":a.id,"dependency_type":"HARD"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(
            &router,
            &format!("{base}/allocations/{}:release", allocation.id),
            json!({"expected_version":allocation.row_version})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(before, mutation_snapshot(&app).await);
    assert_eq!(app.list_work_dependencies(&c, &p).await.unwrap().len(), 1);
    assert_eq!(
        app.get_agent_allocation(&c, &p, &allocation.id)
            .await
            .unwrap()
            .status,
        nalarvo_domain::AgentAllocationStatus::Active
    );
}
async fn post(router: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("Authorization", "Bearer scratch-local-only")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1048576).await.unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("scratch-local-only"));
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"non_json":String::from_utf8_lossy(&bytes)})),
    )
}
#[tokio::test]
async fn h1_http_denied_create_has_zero_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("proof.db").display());
    let (app, c, p, t) = fixture(&url).await;
    let staffed = app.staff_team(command(&c, &p, &t, "staff")).await.unwrap();
    assert!(staffed.team_active);
    sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
        .bind(&p)
        .execute(&app.pool)
        .await
        .unwrap();
    let dependency = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Unaccepted prerequisite",
        ))
        .await
        .unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Blocked required work",
        ))
        .await
        .unwrap();
    app.configure_work(
        command(&c, &p, &t, "config"),
        &work.id,
        WorkRequirements {
            role_id: "engineer".into(),
            priority: 0,
            required: true,
            acceptance_contains: "done".into(),
            input_version_ids: vec![],
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD','t')")
        .bind(&c.0)
        .bind(&p)
        .bind(&work.id)
        .bind(&dependency.id)
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(!app.work_readiness(&c.0, &p, &work.id).await.unwrap().ready);
    let agent:String=sqlx::query_scalar("SELECT agent_id FROM agent_allocations aa JOIN agents a ON a.id=aa.agent_id WHERE aa.company_id=? AND aa.project_id=? AND a.role_id='engineer' AND aa.status='ACTIVE' LIMIT 1").bind(&c.0).bind(&p).fetch_one(&app.pool).await.unwrap();
    sqlx::query("UPDATE permission_grants SET effect='DENY'")
        .execute(&app.pool)
        .await
        .unwrap();
    let router = nalarvo_local_api::router_with_app("scratch-local-only", Some(app.clone()));
    let base = format!("/api/v1/companies/{}/projects/{}", c.0, p);
    let (guarded, _) = post(
        &router,
        &format!("{base}/orchestration"),
        json!({"command":{"type":"ScheduleWork"},"idempotency_key":"denied-schedule","team_id":t}),
    )
    .await;
    assert_eq!(guarded, StatusCode::UNAUTHORIZED);
    let before = mutation_snapshot(&app).await;
    let (created,response)=post(&router,&format!("{base}/runs"),json!({"work_item_id":work.id,"executing_agent_id":agent,"trigger_type":"MANUAL","assignment_id":null,"retry_of_run_id":null})).await;
    assert!(
        created.is_client_error(),
        "H1 security assertion: unauthorized create must be rejected, got {created}: {response}"
    );
    assert_eq!(
        mutation_snapshot(&app).await,
        before,
        "Rejected request must have zero durable mutation"
    );
    app.pool.close().await;
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

async fn mutation_snapshot(app: &ApplicationContext) -> Vec<(String, String)> {
    use sqlx::Row;
    let mut result = Vec::new();
    let tables:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetch_all(&app.pool).await.unwrap();
    for table in tables {
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
        assert!(rows.iter().all(|row| !row.contains("scratch-local-only")));
        result.push((table, format!("{rows:?}")));
    }
    result
}

async fn scheduled_fixture() -> (
    tempfile::TempDir,
    ApplicationContext,
    CompanyId,
    String,
    String,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("adversarial.db").display());
    let (app, c, p, t) = fixture(&url).await;
    app.staff_team(command(&c, &p, &t, "staff")).await.unwrap();
    sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
        .bind(&p)
        .execute(&app.pool)
        .await
        .unwrap();
    let work = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Governed work",
        ))
        .await
        .unwrap();
    app.configure_work(
        command(&c, &p, &t, "config"),
        &work.id,
        WorkRequirements {
            role_id: "engineer".into(),
            priority: 0,
            required: true,
            acceptance_contains: "done".into(),
            input_version_ids: vec![],
        },
    )
    .await
    .unwrap();
    (dir, app, c, p, t, work.id)
}

#[tokio::test]
async fn h1_legacy_create_rejects_even_allowed_ready_work_and_substitutions() {
    use nalarvo_application::m4::CreateRunCommand;
    let (_dir, app, c, p, t, w) = scheduled_fixture().await;
    let router = nalarvo_local_api::router_with_app("scratch-local-only", Some(app.clone()));
    let base = format!("/api/v1/companies/{}/projects/{p}/runs", c.0);
    let before = mutation_snapshot(&app).await;
    for (company, project, agent, assignment, trigger) in [
        (c.0.as_str(), p.as_str(), "engineer-a", None, "MANUAL"),
        (
            c.0.as_str(),
            p.as_str(),
            "engineer-a",
            Some("missing-assignment"),
            "SCHEDULER",
        ),
        ("foreign-company", p.as_str(), "engineer-a", None, "MANUAL"),
        (
            c.0.as_str(),
            "foreign-project",
            "engineer-a",
            None,
            "MANUAL",
        ),
        (c.0.as_str(), p.as_str(), "foreign-agent", None, "MANUAL"),
    ] {
        let mut cmd = CreateRunCommand::new(CompanyId(company.into()), project, &w, agent, trigger);
        cmd.assignment_id = assignment.map(String::from);
        assert!(app.create_run(cmd).await.is_err());
        let path = format!("/api/v1/companies/{company}/projects/{project}/runs");
        let (status,response)=post(&router,&path,json!({"work_item_id":w,"executing_agent_id":agent,"trigger_type":trigger,"assignment_id":assignment})).await;
        assert!(status.is_client_error(), "{status}: {response}");
        assert_eq!(mutation_snapshot(&app).await, before);
    }
    let request =
        json!({"work_item_id":w,"executing_agent_id":"engineer-a","trigger_type":"MANUAL"});
    let (a, b) = tokio::join!(
        post(&router, &base, request.clone()),
        post(&router, &base, request)
    );
    assert!(a.0.is_client_error() && b.0.is_client_error());
    assert_eq!(mutation_snapshot(&app).await, before);
    // The allowed path creates its own scoped primary assignment, atomically.
    let (a, b) = tokio::join!(
        app.schedule_work(command(&c, &p, &t, "one")),
        app.schedule_work(command(&c, &p, &t, "two"))
    );
    let runs = a.unwrap().into_iter().chain(b.unwrap()).collect::<Vec<_>>();
    assert_eq!(runs.len(), 1);
    assert!(runs[0].assignment_id.is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM durable_jobs")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM domain_events e JOIN outbox_messages o ON o.domain_event_id=e.id WHERE e.aggregate_id=? AND e.event_type IN ('RunCreated','RunQueued')").bind(&runs[0].id).fetch_one(&app.pool).await.unwrap(),2);
    app.pool.close().await;
}

#[tokio::test]
async fn h1_legacy_queue_cancel_retry_and_lifecycle_cannot_mutate_governed_run() {
    use nalarvo_domain::RunStatus;
    let (_dir, app, c, p, t, _w) = scheduled_fixture().await;
    let run = app
        .schedule_work(command(&c, &p, &t, "schedule"))
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE permission_grants SET effect='DENY'")
        .execute(&app.pool)
        .await
        .unwrap();
    let router = nalarvo_local_api::router_with_app("scratch-local-only", Some(app.clone()));
    let before = mutation_snapshot(&app).await;
    let path = format!("/api/v1/companies/{}/projects/{p}/runs/{}", c.0, run.id);
    let body = json!({"expected_version":run.row_version});
    let queue_path = format!("{path}/queue");
    let (a, b) = tokio::join!(
        post(&router, &queue_path, body.clone()),
        post(&router, &queue_path, body.clone())
    );
    assert!(a.0.is_client_error() && b.0.is_client_error());
    assert!(
        post(&router, &format!("{path}/cancel"), body)
            .await
            .0
            .is_client_error()
    );
    assert!(app.queue_run(&c, &p, &run.id, 1, None).await.is_err());
    assert!(app.cancel_run(&c, &p, &run.id, 1, None).await.is_err());
    assert!(app.start_run(&c, &p, &run.id, 1).await.is_err());
    assert!(app.pause_run(&c, &p, &run.id, 1).await.is_err());
    assert!(app.resume_run(&c, &p, &run.id, 1).await.is_err());
    assert!(
        app.reject_run_admission(&c, &p, &run.id, 1, "untrusted")
            .await
            .is_err()
    );
    let event = nalarvo_domain::DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        event_type: "RunQueued".into(),
        schema_version: 1,
        company_id: c.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: 2,
        occurred_at: chrono::Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.id.clone(),
        principal: run.requested_by.clone(),
        scope: nalarvo_domain::ScopeRef::company(&c.0),
        payload: json!({}),
    };
    let job = nalarvo_persistence::m4::DurableJob {
        id: uuid::Uuid::now_v7().to_string(),
        job_type: "EXECUTE_RUN".into(),
        company_id: c.clone(),
        run_id: run.id.clone(),
        payload: json!({}),
        available_at: chrono::Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    };
    assert!(
        nalarvo_persistence::m4::queue_run(&app.pool, &c, &run.id, 1, &job, &event)
            .await
            .is_err()
    );
    assert!(
        nalarvo_persistence::m4::create_run_with_event(&app.pool, &run, &event)
            .await
            .is_err()
    );
    for state in [RunStatus::Running, RunStatus::Cancelled] {
        assert!(
            nalarvo_persistence::m4::transition_run(&app.pool, &c, &run.id, 1, state, None, None)
                .await
                .is_err()
        );
    }
    assert_eq!(mutation_snapshot(&app).await, before);
    // Simulate a pre-existing failed attempt; legacy retries must still fail closed.
    sqlx::query("UPDATE runs SET lifecycle_state='RUNNING' WHERE id=?")
        .bind(&run.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(app.wait_run_for_approval(&c, &p, &run.id, 1).await.is_err());
    assert!(
        app.succeed_run(&c, &p, &run.id, 1, "untrusted", None)
            .await
            .is_err()
    );
    assert!(
        app.fail_run(&c, &p, &run.id, 1, "untrusted", "untrusted")
            .await
            .is_err()
    );
    assert!(
        app.timed_out_run(&c, &p, &run.id, 1, None, None)
            .await
            .is_err()
    );
    assert_eq!(mutation_snapshot(&app).await, before);
    sqlx::query("UPDATE runs SET lifecycle_state='FAILED' WHERE id=?")
        .bind(&run.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(app.retry_run(&c, &p, &run.id, 1, None).await.is_err());
    let mut retry = nalarvo_application::m4::CreateRunCommand::new(
        c.clone(),
        &p,
        &run.work_item_id,
        &run.executing_agent_id,
        "RETRY",
    );
    retry.assignment_id = run.assignment_id.clone();
    retry.retry_of_run_id = Some(run.id.clone());
    assert!(app.create_run(retry).await.is_err());
    assert_eq!(mutation_snapshot(&app).await, before);
    app.pool.close().await;
}

#[tokio::test]
async fn h1_hard_dependency_and_team_assignment_scope_are_fail_closed() {
    let (_dir, app, c, p, t, w) = scheduled_fixture().await;
    let prerequisite = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Prerequisite",
        ))
        .await
        .unwrap();
    sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD','t')")
        .bind(&c.0)
        .bind(&p)
        .bind(&w)
        .bind(&prerequisite.id)
        .execute(&app.pool)
        .await
        .unwrap();
    assert_eq!(
        app.work_readiness(&c.0, &p, &w).await.unwrap().reason,
        "HARD_DEPENDENCY"
    );
    assert!(
        app.schedule_work(command(&c, &p, &t, "blocked"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("UPDATE work_items SET status='COMPLETED' WHERE id=?")
        .bind(&prerequisite.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let before = mutation_snapshot(&app).await;
    for (company, project, team) in [
        ("foreign-company", p.as_str(), t.as_str()),
        (c.0.as_str(), "foreign-project", t.as_str()),
        (c.0.as_str(), p.as_str(), "foreign-team"),
    ] {
        assert!(
            app.schedule_work(command(
                &CompanyId(company.into()),
                project,
                team,
                "foreign"
            ))
            .await
            .is_err()
        );
        assert_eq!(mutation_snapshot(&app).await, before);
    }
    // A valid agent allocated to another team cannot substitute its assignment.
    let other = app
        .create_team(CreateTeamCommand::new(c.clone(), p.clone(), "Other team"))
        .await
        .unwrap();
    sqlx::query("UPDATE teams SET status='ACTIVE' WHERE id=?")
        .bind(&other.id)
        .execute(&app.pool)
        .await
        .unwrap();
    let al = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            c.clone(),
            p.clone(),
            other.id.clone(),
            "engineer-b",
            None,
        ))
        .await
        .unwrap();
    app.transition_agent_allocation(
        &c,
        &p,
        &al.id,
        nalarvo_domain::AgentAllocationStatus::Active,
        al.row_version,
    )
    .await
    .unwrap();
    let assignment = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            c.clone(),
            p.clone(),
            w.clone(),
            "engineer-b",
            al.id.clone(),
            true,
        ))
        .await
        .unwrap();
    assert!(
        app.schedule_work(command(&c, &p, &t, "wrong-agent"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        sqlx::query("UPDATE assignments SET agent_id='engineer-a' WHERE id=?")
            .bind(&assignment.id)
            .execute(&app.pool)
            .await
            .is_err(),
        "DB must reject agent/allocation substitution"
    );
    sqlx::query("DELETE FROM assignments WHERE id=?")
        .bind(&assignment.id)
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agents SET capacity=2 WHERE id='engineer-a'")
        .execute(&app.pool)
        .await
        .unwrap();
    let other_allocation = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            c.clone(),
            p.clone(),
            other.id.clone(),
            "engineer-a",
            None,
        ))
        .await
        .unwrap();
    app.transition_agent_allocation(
        &c,
        &p,
        &other_allocation.id,
        nalarvo_domain::AgentAllocationStatus::Active,
        1,
    )
    .await
    .unwrap();
    app.create_work_assignment(CreateWorkAssignmentCommand::new(
        c.clone(),
        p.clone(),
        w.clone(),
        "engineer-a",
        other_allocation.id,
        true,
    ))
    .await
    .unwrap();
    assert!(
        app.schedule_work(command(&c, &p, &t, "wrong-allocation"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    app.pool.close().await;
}

#[tokio::test]
async fn h1_permission_revoked_while_waiting_for_write_transaction_is_current() {
    let (_dir, app, c, p, t, _w) = scheduled_fixture().await;
    let mut tx = app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE permission_grants SET effect='DENY'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = {
        let app = app.clone();
        let cmd = command(&c, &p, &t, "after-revocation");
        tokio::spawn(async move { app.schedule_work(cmd).await })
    };
    tokio::task::yield_now().await;
    tx.commit().await.unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(mutation_snapshot(&app).await, before);
    app.pool.close().await;
}

#[tokio::test]
async fn h1_legacy_unconfigured_http_create_queue_cancel_remains_supported() {
    let (_dir, app, c, p, t, w) = scheduled_fixture().await;
    sqlx::query("DELETE FROM work_requirements WHERE work_item_id=?")
        .bind(&w)
        .execute(&app.pool)
        .await
        .unwrap();
    let router = nalarvo_local_api::router_with_app("scratch-local-only", Some(app.clone()));
    let base = format!("/api/v1/companies/{}/projects/{p}/runs", c.0);
    let (status, run) = post(
        &router,
        &base,
        json!({"work_item_id":w,"executing_agent_id":"engineer-a","trigger_type":"MANUAL"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let id = run["id"].as_str().unwrap();
    assert_eq!(
        post(
            &router,
            &format!("{base}/{id}/queue"),
            json!({"expected_version":1})
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        post(
            &router,
            &format!("{base}/{id}/cancel"),
            json!({"expected_version":2})
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM runs WHERE id=?")
            .bind(id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "CANCELLED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE run_id=?")
            .bind(id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "CANCELLED"
    );
    assert!(
        app.schedule_work(command(&c, &p, &t, "no-configured-work"))
            .await
            .unwrap()
            .is_empty()
    );
    app.pool.close().await;
}

#[tokio::test]
async fn h1_approval_revoked_before_admission_rolls_back_everything() {
    let (_dir, app, c, p, t, _w) = scheduled_fixture().await;
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES ('approval-policy','0191e4b8-0002-7000-8000-000000000001',?,'Approval','ACTIVE',1,'t','t')").bind(&c.0).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES ('approval-version','approval-policy',1,'{\"permission\":\"project.modify\",\"effect\":\"REQUIRE_APPROVAL\"}','t')").execute(&app.pool).await.unwrap();
    let proposal = app
        .propose_action(ProposeActionCommand {
            company_id: c.0.clone(),
            project_id: Some(p.clone()),
            run_id: None,
            step_id: None,
            actor: PrincipalRef::user(OWNER),
            action_type: "project.modify".into(),
            target: p.clone(),
            parameters: Some(json!({"schedule_team":t})),
            idempotency_key: Some("proposal".into()),
            meta: command(&c, &p, &t, "proposal").meta,
        })
        .await
        .unwrap();
    assert_eq!(
        proposal.decision,
        nalarvo_domain::AuthorityDecision::RequireApproval
    );
    let mut cmd = command(&c, &p, &t, "approved-schedule");
    cmd.meta.causation_id = Some(proposal.action_id.clone());
    let before = mutation_snapshot(&app).await;
    assert!(
        app.schedule_work(cmd.clone()).await.is_err(),
        "Pending approval cannot schedule"
    );
    assert_eq!(mutation_snapshot(&app).await, before);
    // Seed approved authority, then commit revocation ahead of the admission writer.
    // This tests consumption/revalidation, not the independent approval-decision API.
    sqlx::query("UPDATE actions SET authorization='APPROVED' WHERE id=?")
        .bind(&proposal.action_id)
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE approval_requests SET status='APPROVED' WHERE action_id=?")
        .bind(&proposal.action_id)
        .execute(&app.pool)
        .await
        .unwrap();
    let mut tx = app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE approval_requests SET status='REJECTED' WHERE action_id=?")
        .bind(&proposal.action_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    let task = {
        let app = app.clone();
        let cmd = cmd.clone();
        tokio::spawn(async move { app.schedule_work(cmd).await })
    };
    tokio::task::yield_now().await;
    tx.commit().await.unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(mutation_snapshot(&app).await, before);
    sqlx::query("UPDATE approval_requests SET status='APPROVED' WHERE action_id=?")
        .bind(&proposal.action_id)
        .execute(&app.pool)
        .await
        .unwrap();
    assert_eq!(
        app.schedule_work(cmd).await.unwrap().len(),
        1,
        "Current matching approval allows scheduling"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id=?")
            .bind(&proposal.action_id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "SUCCEEDED"
    );
    app.pool.close().await;
}

#[tokio::test]
async fn h1_persistence_create_and_configuration_race_cannot_bypass_guard() {
    let (_dir, app, c, p, _t, w) = scheduled_fixture().await;
    let run = nalarvo_domain::Run::create(
        c.clone(),
        p.clone(),
        w.clone(),
        "engineer-a".into(),
        "MANUAL".into(),
        PrincipalRef::user(OWNER),
        "h1".into(),
    )
    .unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(
        nalarvo_persistence::m4::create_run(&app.pool, &run)
            .await
            .is_err()
    );
    assert_eq!(mutation_snapshot(&app).await, before);
    sqlx::query("DELETE FROM work_requirements WHERE work_item_id=?")
        .bind(&w)
        .execute(&app.pool)
        .await
        .unwrap();
    let mut tx = app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO work_requirements(company_id,project_id,work_item_id,role_id,required,acceptance_contains) VALUES (?,?,?,'engineer',1,'done')").bind(&c.0).bind(&p).bind(&w).execute(&mut *tx).await.unwrap();
    let task = {
        let app = app.clone();
        tokio::spawn(async move {
            app.create_run(nalarvo_application::m4::CreateRunCommand::new(
                c,
                p,
                w,
                "engineer-a",
                "MANUAL",
            ))
            .await
        })
    };
    tokio::task::yield_now().await;
    tx.commit().await.unwrap();
    let before = mutation_snapshot(&app).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(mutation_snapshot(&app).await, before);
    app.pool.close().await;
}

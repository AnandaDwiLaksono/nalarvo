use nalarvo_application::m4::*;
use nalarvo_application::*;
use nalarvo_domain::{CompanyId, ExecutionStep, RunStatus, WorkItemStatus, WorkspaceId};

async fn setup_app() -> (ApplicationContext, CompanyId, String, String, String) {
    let app = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let company_id = app
        .create_company(CreateCompanyCommand {
            workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
            name: "M4 Co".into(),
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
        "INSERT INTO provider_connections(id, workspace_id, name, provider_kind, created_at, updated_at) VALUES ('pc1', '0191e4b8-0002-7000-8000-000000000001', 'Provider', 'TEST', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        "INSERT INTO models(id, workspace_id, provider_connection_id, model_key, created_at, updated_at) VALUES ('m1', '0191e4b8-0002-7000-8000-000000000001', 'pc1', 'test', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        "INSERT INTO model_profiles(id, workspace_id, name, created_at, updated_at) VALUES ('mp1', '0191e4b8-0002-7000-8000-000000000001', 'Default Profile', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        "INSERT INTO model_profile_versions(profile_id, workspace_id, version, model_id, created_at) VALUES ('mp1', '0191e4b8-0002-7000-8000-000000000001', 1, 'm1', '2026-01-01T00:00:00Z')",
        "UPDATE model_profiles SET current_version = 1 WHERE id = 'mp1'",
        "INSERT INTO company_workspace_resource_grants(company_id, workspace_id, model_profile_id, created_at) VALUES (?, '0191e4b8-0002-7000-8000-000000000001', 'mp1', '2026-01-01T00:00:00Z')",
        "INSERT INTO agents(id, company_id, name, primary_department_id, role_id, model_profile_id, capacity, status, created_at, updated_at) VALUES ('a1', ?, 'Agent 1', 'd1', 'r1', 'mp1', 2, 'ACTIVE', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
    ] {
        sqlx::query(sql)
            .bind(&company_id.0)
            .execute(&app.pool)
            .await
            .unwrap();
    }

    let project = app
        .create_project(&company_id, "Project M4".into(), None)
        .await
        .unwrap();

    let team = app
        .create_team(CreateTeamCommand::new(
            company_id.clone(),
            project.id.clone(),
            "Team M4",
        ))
        .await
        .unwrap();

    let staffing = app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            company_id.clone(),
            project.id.clone(),
            Some(team.id.clone()),
            "r1",
            1,
        ))
        .await
        .unwrap();

    let allocation = app
        .create_agent_allocation(CreateAgentAllocationCommand::new(
            company_id.clone(),
            project.id.clone(),
            team.id.clone(),
            "a1",
            Some(staffing.id.clone()),
        ))
        .await
        .unwrap();

    let _active_alloc = app
        .transition_agent_allocation(
            &company_id,
            &project.id,
            &allocation.id,
            nalarvo_domain::AgentAllocationStatus::Active,
            allocation.row_version,
        )
        .await
        .unwrap();

    let work_item = app
        .create_work_item(CreateWorkItemCommand::new(
            company_id.clone(),
            project.id.clone(),
            "Work M4",
        ))
        .await
        .unwrap();

    let ready_work_item = app
        .transition_work_item(
            &company_id,
            &project.id,
            &work_item.id,
            WorkItemStatus::Ready,
            work_item.row_version,
        )
        .await
        .unwrap();

    let assignment = app
        .create_work_assignment(CreateWorkAssignmentCommand::new(
            company_id.clone(),
            project.id.clone(),
            ready_work_item.id.clone(),
            "a1",
            allocation.id.clone(),
            true,
        ))
        .await
        .unwrap();

    (
        app,
        company_id,
        project.id,
        ready_work_item.id,
        assignment.id,
    )
}

#[tokio::test]
async fn test_create_get_list_run_scoped() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    assert_eq!(run.project_id, project_id);
    assert_eq!(run.work_item_id, work_item_id);
    assert_eq!(run.executing_agent_id, "a1");
    assert_eq!(run.attempt_number, 1);
    assert_eq!(run.status, RunStatus::Queued);

    let fetched = app
        .get_run(&company_id, &project_id, &run.id)
        .await
        .unwrap();
    assert_eq!(fetched.id, run.id);

    let runs = app.list_runs(&company_id, &project_id).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, run.id);
}

#[tokio::test]
async fn test_start_run_coordinates_ready_work_item_to_in_progress() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let started_run = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await
        .unwrap();
    assert_eq!(started_run.status, RunStatus::Running);

    let work_item = app.get_work_item(&company_id, &work_item_id).await.unwrap();
    assert_eq!(work_item.status, WorkItemStatus::InProgress);

    // Run success does NOT complete WorkItem
    let succeeded_run = app
        .succeed_run(
            &company_id,
            &project_id,
            &run.id,
            started_run.row_version,
            "Success summary",
            None,
        )
        .await
        .unwrap();
    assert_eq!(succeeded_run.status, RunStatus::Succeeded);

    let work_item_after = app.get_work_item(&company_id, &work_item_id).await.unwrap();
    assert_eq!(work_item_after.status, WorkItemStatus::InProgress);
}

#[tokio::test]
async fn test_run_failure_never_fails_work_item() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let started = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await
        .unwrap();

    let failed = app
        .fail_run(
            &company_id,
            &project_id,
            &run.id,
            started.row_version,
            "EXECUTION_ERROR",
            "Task failed",
        )
        .await
        .unwrap();
    assert_eq!(failed.status, RunStatus::Failed);

    // WorkItem must STILL be InProgress, never marked Failed by Run failure
    let work_item = app.get_work_item(&company_id, &work_item_id).await.unwrap();
    assert_eq!(work_item.status, WorkItemStatus::InProgress);
}

#[tokio::test]
async fn test_deadline_timeout_outbox_fault_rolls_back_terminal_state() {
    let (app, company_id, project_id, work_item_id, _) = setup_app().await;
    let run = app
        .create_run(CreateRunCommand::new(
            company_id.clone(),
            &project_id,
            &work_item_id,
            "a1",
            "MANUAL",
        ))
        .await
        .unwrap();
    let started = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await
        .unwrap();

    // Deterministic pre-commit fault: reject only the RunTimedOut outbox insert.
    sqlx::query(
        "CREATE TRIGGER reject_timeout_outbox BEFORE INSERT ON outbox_messages \
         WHEN (SELECT event_type FROM domain_events WHERE id = NEW.domain_event_id) = 'RunTimedOut' \
         BEGIN SELECT RAISE(ABORT, 'injected timeout outbox failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let error = app
        .timed_out_run(
            &company_id,
            &project_id,
            &run.id,
            started.row_version,
            Some("RUN_DEADLINE_EXCEEDED"),
            Some("deadline exceeded"),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected timeout outbox failure")
    );

    let actual = app
        .get_run(&company_id, &project_id, &run.id)
        .await
        .unwrap();
    assert_eq!(actual.status, RunStatus::Running);
    assert_eq!(actual.row_version, started.row_version);
    assert!(actual.completed_at.is_none());
    assert!(actual.failure_class.is_none());
    assert!(actual.failure_detail.is_none());
    let result_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_results WHERE run_id = ?")
            .bind(&run.id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(result_count, 0);
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_events WHERE aggregate_id = ? AND event_type = 'RunTimedOut'",
    )
    .bind(&run.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(event_count, 0);
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_messages WHERE domain_event_id IN \
         (SELECT id FROM domain_events WHERE aggregate_id = ? AND event_type = 'RunTimedOut')",
    )
    .bind(&run.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(outbox_count, 0);

    sqlx::query("DROP TRIGGER reject_timeout_outbox")
        .execute(&app.pool)
        .await
        .unwrap();
    let timed_out = app
        .timed_out_run(
            &company_id,
            &project_id,
            &run.id,
            started.row_version,
            Some("RUN_DEADLINE_EXCEEDED"),
            Some("deadline exceeded"),
        )
        .await
        .unwrap();
    assert_eq!(timed_out.status, RunStatus::TimedOut);
    assert_eq!(timed_out.row_version, started.row_version + 1);
    let persisted: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runtime_results WHERE run_id = ?), \
         (SELECT count(*) FROM domain_events WHERE aggregate_id = ? AND event_type = 'RunTimedOut'), \
         (SELECT count(*) FROM outbox_messages o JOIN domain_events e ON e.id = o.domain_event_id \
          WHERE e.aggregate_id = ? AND e.event_type = 'RunTimedOut')",
    )
    .bind(&run.id).bind(&run.id).bind(&run.id)
    .fetch_one(&app.pool).await.unwrap();
    assert_eq!(persisted, (1, 1, 1));
}

#[tokio::test]
async fn test_explicit_retry_preserves_failed_run_and_increments_attempt() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let started = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await
        .unwrap();

    let failed = app
        .fail_run(
            &company_id,
            &project_id,
            &run.id,
            started.row_version,
            "MODEL_ERROR",
            "Timeout",
        )
        .await
        .unwrap();
    assert_eq!(failed.status, RunStatus::Failed);

    let retried = app
        .retry_run(
            &company_id,
            &project_id,
            &failed.id,
            failed.row_version,
            None,
        )
        .await
        .unwrap();

    assert_ne!(retried.id, failed.id);
    assert_eq!(retried.attempt_number, 2);
    assert_eq!(retried.retry_of_run_id.as_deref(), Some(failed.id.as_str()));
    assert_eq!(retried.status, RunStatus::Queued);

    // Original run is still terminal FAILED
    let old_run = app
        .get_run(&company_id, &project_id, &failed.id)
        .await
        .unwrap();
    assert_eq!(old_run.status, RunStatus::Failed);
}

#[tokio::test]
async fn test_admission_rejection_marks_admission_rejected() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    // Pause agent after run creation to fail admission, not creation scope.
    sqlx::query("UPDATE agents SET status = 'PAUSED' WHERE id = 'a1' AND company_id = ?")
        .bind(&company_id.0)
        .execute(&app.pool)
        .await
        .unwrap();

    let start_res = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await;
    assert!(start_res.is_err());

    let rejected = app
        .get_run(&company_id, &project_id, &run.id)
        .await
        .unwrap();
    assert_eq!(rejected.status, RunStatus::Failed);
    assert_eq!(
        rejected.failure_class.as_deref(),
        Some("ADMISSION_REJECTED")
    );
    assert!(rejected.started_at.is_none());
    assert!(rejected.completed_at.is_some());
}

#[tokio::test]
async fn test_pause_and_resume_guarded_states() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    // Cannot pause a QUEUED run
    assert!(
        app.pause_run(&company_id, &project_id, &run.id, run.row_version)
            .await
            .is_err()
    );

    let started = app
        .start_run(&company_id, &project_id, &run.id, run.row_version)
        .await
        .unwrap();

    let paused = app
        .pause_run(&company_id, &project_id, &run.id, started.row_version)
        .await
        .unwrap();
    assert_eq!(paused.status, RunStatus::Paused);

    let resumed = app
        .resume_run(&company_id, &project_id, &run.id, paused.row_version)
        .await
        .unwrap();
    assert_eq!(resumed.status, RunStatus::Running);
}

#[tokio::test]
async fn test_cancel_run_duplicate_safe() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;
    let run = app
        .create_run(CreateRunCommand::new(
            company_id.clone(),
            &project_id,
            &work_item_id,
            "a1",
            "MANUAL",
        ))
        .await
        .unwrap();
    let queued = app
        .queue_run(&company_id, &project_id, &run.id, run.row_version, None)
        .await
        .unwrap();

    let cancelled = app
        .cancel_run(
            &company_id,
            &project_id,
            &run.id,
            queued.row_version,
            Some("Operator cancel".into()),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status, RunStatus::Cancelled);

    let cancelled_again = app
        .cancel_run(
            &company_id,
            &project_id,
            &run.id,
            cancelled.row_version,
            Some("Operator cancel".into()),
        )
        .await
        .unwrap();
    assert_eq!(cancelled_again.status, RunStatus::Cancelled);

    let persisted: (i64, i64, i64, String) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runtime_results WHERE run_id = ? AND run_status = 'CANCELLED'), \
         (SELECT count(*) FROM domain_events WHERE aggregate_id = ? AND event_type = 'RunCancelled'), \
         (SELECT count(*) FROM outbox_messages o JOIN domain_events e ON e.id = o.domain_event_id \
          WHERE e.aggregate_id = ? AND e.event_type = 'RunCancelled'), \
         (SELECT status FROM durable_jobs WHERE run_id = ?)",
    )
    .bind(&run.id)
    .bind(&run.id)
    .bind(&run.id)
    .bind(&run.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(persisted, (1, 1, 1, "CANCELLED".into()));
}

#[tokio::test]
async fn test_running_cancel_closes_active_steps_and_persists_terminal_truth() {
    let (app, company_id, project_id, work_item_id, _) = setup_app().await;
    let run = app
        .create_run(CreateRunCommand::new(
            company_id.clone(),
            &project_id,
            &work_item_id,
            "a1",
            "MANUAL",
        ))
        .await
        .unwrap();
    let queued = app
        .queue_run(&company_id, &project_id, &run.id, run.row_version, None)
        .await
        .unwrap();
    let running = app
        .start_run(&company_id, &project_id, &run.id, queued.row_version)
        .await
        .unwrap();
    let mut step =
        ExecutionStep::create(company_id.clone(), run.id.clone(), 1, "MODEL_CALL".into()).unwrap();
    step.start().unwrap();
    app.append_execution_step(&step).await.unwrap();
    let (_, lease) = nalarvo_persistence::m4::claim_job(
        &app.pool,
        "cancel-test-worker",
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap()
    .unwrap();

    let cancelled = app
        .cancel_run(
            &company_id,
            &project_id,
            &run.id,
            running.row_version,
            Some("Operator cancel".into()),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status, RunStatus::Cancelled);
    assert!(cancelled.completed_at.is_some());

    let persisted: (String, i64, i64, i64, String) = sqlx::query_as(
        "SELECT (SELECT lifecycle_state FROM execution_steps WHERE run_id = ?), \
         (SELECT count(*) FROM runtime_results WHERE run_id = ? AND run_status = 'CANCELLED'), \
         (SELECT count(*) FROM domain_events WHERE aggregate_id = ? AND event_type = 'RunCancelled'), \
         (SELECT count(*) FROM outbox_messages o JOIN domain_events e ON e.id = o.domain_event_id \
          WHERE e.aggregate_id = ? AND e.event_type = 'RunCancelled'), \
         (SELECT status FROM durable_jobs WHERE run_id = ?)",
    )
    .bind(&run.id)
    .bind(&run.id)
    .bind(&run.id)
    .bind(&run.id)
    .bind(&run.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(persisted, ("CANCELLED".into(), 1, 1, 1, "CANCELLED".into()));
    assert!(
        !nalarvo_persistence::m4::release_current_lease(&app.pool, &lease, "TEST")
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM run_execution_leases WHERE run_id = ? AND released_at IS NULL"
        )
        .bind(&run.id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_queue_run_idempotent() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let meta = CommandMeta {
        idempotency_key: Some("queue-key-1".into()),
        ..Default::default()
    };

    let queued1 = app
        .queue_run(
            &company_id,
            &project_id,
            &run.id,
            run.row_version,
            Some(meta.clone()),
        )
        .await
        .unwrap();
    assert_eq!(queued1.status, RunStatus::Queued);

    // Idempotent retry with same key
    let queued2 = app
        .queue_run(
            &company_id,
            &project_id,
            &run.id,
            run.row_version,
            Some(meta),
        )
        .await
        .unwrap();
    assert_eq!(queued2.id, queued1.id);
}

#[tokio::test]
async fn test_scoped_isolation_cross_company_and_project() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let other_company = CompanyId("other-company".into());
    assert!(
        app.get_run(&other_company, &project_id, &run.id)
            .await
            .is_err()
    );
    assert!(app.list_runs(&other_company, &project_id).await.is_err());
    assert!(
        app.get_run(&company_id, "other-project", &run.id)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_append_execution_step() {
    let (app, company_id, project_id, work_item_id, _assignment_id) = setup_app().await;

    let cmd = CreateRunCommand::new(
        company_id.clone(),
        &project_id,
        &work_item_id,
        "a1",
        "MANUAL",
    );
    let run = app.create_run(cmd).await.unwrap();

    let step =
        ExecutionStep::create(company_id.clone(), run.id.clone(), 1, "MODEL_CALL".into()).unwrap();
    assert!(app.append_execution_step(&step).await.is_ok());
}

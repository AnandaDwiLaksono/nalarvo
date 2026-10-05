use nalarvo_domain::*;

fn run() -> Run {
    Run::create(
        CompanyId("company-1".into()),
        "project-1".into(),
        "work-1".into(),
        "agent-1".into(),
        "MANUAL".into(),
        PrincipalRef::user("user-1"),
        "correlation-1".into(),
    )
    .unwrap()
}

#[test]
fn run_tracks_attempt_and_only_allows_truthful_lifecycle_transitions() {
    let mut run = run();
    assert_eq!(run.attempt_number, 1);
    assert_eq!(run.status, RunStatus::Queued);
    assert!(!run.status.is_terminal());

    run.start().unwrap();
    assert_eq!(run.status, RunStatus::Running);
    run.pause().unwrap();
    assert_eq!(run.status, RunStatus::Paused);
    run.resume().unwrap();
    assert_eq!(run.status, RunStatus::Running);
    run.succeed().unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert!(run.start().is_err());
    assert!(run.cancel().is_err());
    assert_eq!(run.status, RunStatus::Succeeded);
}

#[test]
fn run_failure_timeout_cancel_and_retry_preserve_attempt_history() {
    let mut failed = run();
    failed.start().unwrap();
    failed
        .fail("PROVIDER_ERROR".into(), "unavailable".into())
        .unwrap();
    assert_eq!(failed.status, RunStatus::Failed);
    let retry = failed.retry().unwrap();
    assert_ne!(retry.id, failed.id);
    assert_eq!(retry.attempt_number, failed.attempt_number + 1);
    assert_eq!(retry.retry_of_run_id.as_deref(), Some(failed.id.as_str()));
    assert_eq!(failed.status, RunStatus::Failed);

    let mut timed_out = run();
    timed_out.start().unwrap();
    timed_out
        .time_out("RUN_TIMEOUT".into(), "deadline".into())
        .unwrap();
    assert_eq!(timed_out.status, RunStatus::TimedOut);

    let mut cancelled = run();
    cancelled.cancel().unwrap();
    assert_eq!(cancelled.status, RunStatus::Cancelled);
    assert!(cancelled.resume().is_err());
}

#[test]
fn execution_step_has_guarded_lifecycle_and_no_reasoning_payload() {
    let run = run();
    let mut step = ExecutionStep::create(run.company_id, run.id, 1, "MODEL_CALL".into()).unwrap();
    assert_eq!(step.status, ExecutionStepStatus::Pending);
    assert!(step.succeed(serde_json::json!({})).is_err());
    assert!(step.fail("ERROR".into(), "failure".into()).is_err());
    step.start().unwrap();
    assert!(step.start().is_err());
    step.succeed(serde_json::json!({"output_ref":"result-1"}))
        .unwrap();
    assert_eq!(step.status, ExecutionStepStatus::Succeeded);
    assert!(step.fail("ERROR".into(), "failure".into()).is_err());
    assert!(step.cancel().is_err());
    assert!(step.completed_at.is_some());
    assert!(
        serde_json::to_string(&step)
            .unwrap()
            .contains("output_metadata")
    );
}

#[test]
fn execution_step_failure_cancel_and_skip_are_distinct_terminal_states() {
    let mut failed =
        ExecutionStep::create(CompanyId("c".into()), "r".into(), 1, "MODEL".into()).unwrap();
    failed.start().unwrap();
    failed
        .fail("PROVIDER_TIMEOUT".into(), "deadline".into())
        .unwrap();
    assert_eq!(failed.status, ExecutionStepStatus::Failed);
    assert_eq!(failed.failure_class.as_deref(), Some("PROVIDER_TIMEOUT"));
    assert!(failed.completed_at.is_some());
    assert!(failed.succeed(serde_json::json!({})).is_err());

    let mut skipped =
        ExecutionStep::create(CompanyId("c".into()), "r".into(), 2, "MODEL".into()).unwrap();
    skipped.skip().unwrap();
    assert_eq!(skipped.status, ExecutionStepStatus::Skipped);
    assert!(skipped.started_at.is_none());
    assert!(skipped.completed_at.is_some());
    assert!(skipped.start().is_err());
    assert!(skipped.cancel().is_err());

    let mut cancelled =
        ExecutionStep::create(CompanyId("c".into()), "r".into(), 3, "MODEL".into()).unwrap();
    cancelled.cancel().unwrap();
    assert_eq!(cancelled.status, ExecutionStepStatus::Cancelled);
    assert!(cancelled.started_at.is_none());
    assert!(cancelled.completed_at.is_some());
    assert!(cancelled.skip().is_err());
}

#[test]
fn run_waiting_states_and_parsing_and_step_status_traits() {
    assert_eq!(
        "WAITING_APPROVAL".parse::<RunStatus>().unwrap(),
        RunStatus::WaitingApproval
    );
    assert_eq!(
        "WAITING_DEPENDENCY".parse::<RunStatus>().unwrap(),
        RunStatus::WaitingDependency
    );
    assert_eq!(
        "PENDING".parse::<ExecutionStepStatus>().unwrap(),
        ExecutionStepStatus::Pending
    );
    assert!("QUEUED".parse::<ExecutionStepStatus>().is_err());
    assert!("TIMED_OUT".parse::<ExecutionStepStatus>().is_err());
    assert_eq!(ExecutionStepStatus::Succeeded.to_string(), "SUCCEEDED");

    let mut run = run();
    run.start().unwrap();
    run.wait_for_approval().unwrap();
    assert_eq!(run.status, RunStatus::WaitingApproval);
    run.resume().unwrap();
    assert_eq!(run.status, RunStatus::Running);

    run.wait_for_dependency().unwrap();
    assert_eq!(run.status, RunStatus::WaitingDependency);
    run.resume().unwrap();
    assert_eq!(run.status, RunStatus::Running);
}

#[test]
fn queued_admission_failure_is_terminal_without_started_at() {
    let mut run = run();
    run.reject_admission("ADMISSION_REJECTED".into(), "invalid scope".into())
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run.started_at.is_none());
    assert!(run.completed_at.is_some());
}

#[test]
fn run_creation_rejects_blank_required_ids_and_retry_only_follows_terminal_failure() {
    assert!(
        Run::create(
            CompanyId(" ".into()),
            "project".into(),
            "work".into(),
            "agent".into(),
            "MANUAL".into(),
            PrincipalRef::user("user"),
            "corr".into()
        )
        .is_err()
    );
    let queued = run();
    assert!(queued.retry().is_err());
}

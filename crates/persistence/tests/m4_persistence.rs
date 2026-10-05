use chrono::{Duration, Utc};
use nalarvo_domain::{
    CompanyId, DomainEvent, ExecutionStep, PrincipalRef, Run, RunStatus, ScopeRef,
};
use nalarvo_persistence::m4::{
    DurableJob, ModelInvocation, RuntimeCheckpoint, RuntimeResult, UsageRecord, append_checkpoint,
    append_invocation, append_invocation_with_lease, append_step, append_step_with_lease,
    append_usage, append_usage_with_lease, claim_job, create_run, create_run_with_event,
    expire_leases, finalize_run_with_result_with_lease, get_run, heartbeat_lease, list_runs,
    queue_run, release_current_lease, release_lease, retry_run, store_terminal_result,
    store_terminal_result_with_lease, transition_run, transition_run_with_lease,
    verify_current_lease,
};
use nalarvo_persistence::{PersistenceError, create_pool, run_migrations};
use serde_json::json;
use sqlx::{Row, SqlitePool};

async fn setup() -> SqlitePool {
    let pool = create_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('d','c','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('r','c','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('c','d','r','2026-10-01T00:00:00Z')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('a','c','A','d','r',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO projects(id,company_id,name,created_at,updated_at) VALUES ('p','c','P','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO work_items(id,company_id,project_id,title,logical_type,created_at,updated_at) VALUES ('wi','c','p','W','TASK','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,created_at,updated_at) VALUES ('pc','w','PC','TEST','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','w','pc','m','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    pool
}

fn run() -> Run {
    Run::create(
        CompanyId("c".into()),
        "p".into(),
        "wi".into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "corr".into(),
    )
    .unwrap()
}

fn event(run: &Run, kind: &str) -> DomainEvent {
    DomainEvent {
        event_id: format!("{kind}-{}", run.id),
        event_type: kind.into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: run.row_version,
        occurred_at: Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::user("u"),
        scope: ScopeRef::company(run.company_id.0.clone()),
        payload: json!({"run_id":run.id,"state":run.status.to_string()}),
    }
}

fn job(run: &Run) -> DurableJob {
    DurableJob {
        id: format!("job-{}", run.id),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({"run_id":run.id}),
        available_at: Utc::now() - Duration::seconds(1),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    }
}

#[tokio::test]
async fn create_list_get_run_and_event_outbox_enforce_scope() {
    let pool = setup().await;
    let run = run();
    create_run_with_event(&pool, &run, &event(&run, "RunCreated"))
        .await
        .unwrap();
    assert_eq!(
        get_run(&pool, &CompanyId("c".into()), &run.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        run.id
    );
    assert_eq!(
        list_runs(&pool, &CompanyId("c".into()), "p")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        get_run(&pool, &CompanyId("other".into()), &run.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(count(&pool, "domain_events").await, 1);
    assert_eq!(count(&pool, "outbox_messages").await, 1);
}

#[tokio::test]
async fn queue_claim_heartbeat_release_and_expire_are_durable() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        &job(&run),
        &event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    assert!(
        queue_run(
            &pool,
            &run.company_id,
            &run.id,
            2,
            &job(&run),
            &event(&run, "Again")
        )
        .await
        .is_err()
    );
    assert_eq!(count(&pool, "durable_jobs").await, 1);
    assert_eq!(count(&pool, "outbox_messages").await, 1);

    let (claimed, lease) = claim_job(&pool, "worker-1", Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, job(&run).id);
    assert!(
        claim_job(&pool, "worker-2", Duration::seconds(1))
            .await
            .unwrap()
            .is_none()
    );
    let beat = heartbeat_lease(&pool, &lease.id, "worker-1", 1, Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(beat.lease_version, 2);
    assert!(
        release_lease(&pool, &lease.id, "worker-1", "YIELD")
            .await
            .unwrap()
    );
    let (_, expiring) = claim_job(&pool, "worker-2", Duration::milliseconds(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        expire_leases(&pool, expiring.expires_at + Duration::seconds(1))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn transition_occ_and_runtime_records_preserve_safe_metadata_only() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    let running = transition_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        transition_run(
            &pool,
            &run.company_id,
            &run.id,
            1,
            RunStatus::Paused,
            None,
            None
        )
        .await,
        Err(PersistenceError::StaleVersion { .. })
    ));
    let mut step =
        ExecutionStep::create(run.company_id.clone(), run.id.clone(), 1, "MODEL".into()).unwrap();
    step.start().unwrap();
    append_step(&pool, &step).await.unwrap();
    append_invocation(
        &pool,
        &ModelInvocation {
            id: "i".into(),
            company_id: run.company_id.clone(),
            run_id: run.id.clone(),
            step_id: step.id.clone(),
            agent_id: run.executing_agent_id.clone(),
            provider_connection_id: "pc".into(),
            model_id: "m".into(),
            model_profile_version_id: None,
            invocation_index: 1,
            status: "RUNNING".into(),
            request_metadata: Some(json!({"temperature":0})),
            response_metadata: None,
            input_tokens: None,
            output_tokens: None,
            estimated_cost: None,
            latency_ms: None,
            provider_request_id: None,
            started_at: Utc::now(),
            completed_at: None,
            failure_class: None,
            failure_detail: None,
        },
    )
    .await
    .unwrap();
    append_checkpoint(
        &pool,
        &RuntimeCheckpoint {
            id: "cp".into(),
            company_id: run.company_id.clone(),
            run_id: run.id.clone(),
            checkpoint_version: 1,
            run_state: "RUNNING".into(),
            last_completed_step: None,
            active_step: Some(1),
            execution_phase: "MODEL".into(),
            context_refs: Some(json!(["wi"])),
            continuation_metadata: Some(json!({"invocation":1})),
            usage_snapshot: None,
            safe_to_resume: true,
        },
    )
    .await
    .unwrap();
    append_usage(
        &pool,
        &UsageRecord {
            id: "usage".into(),
            workspace_id: "w".into(),
            company_id: run.company_id.clone(),
            project_id: run.project_id.clone(),
            work_item_id: run.work_item_id.clone(),
            agent_id: run.executing_agent_id.clone(),
            run_id: run.id.clone(),
            step_id: Some(step.id),
            provider_connection_id: "pc".into(),
            model_id: "m".into(),
            usage_type: "TOKENS".into(),
            quantity: 1,
            unit: "TOKEN".into(),
            estimated_cost: None,
            occurred_at: Utc::now(),
            metadata: Some(json!({"input_tokens":1})),
        },
    )
    .await
    .unwrap();
    assert_eq!(running.status, RunStatus::Running);
    assert_eq!(count(&pool, "model_invocations").await, 1);
    assert_eq!(count(&pool, "runtime_checkpoints").await, 1);
    assert_eq!(count(&pool, "usage_records").await, 1);

    let unsafe_step =
        ExecutionStep::create(run.company_id.clone(), run.id.clone(), 2, "MODEL".into()).unwrap();
    assert!(append_step(&pool, &unsafe_step).await.is_ok());
}

#[tokio::test]
async fn cancel_retry_terminal_result_and_payload_rejection_are_atomic() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    let failed = transition_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        RunStatus::Failed,
        Some("TEST"),
        Some("x"),
    )
    .await
    .unwrap();
    let retry = retry_run(
        &pool,
        &run.company_id,
        &run.id,
        failed.row_version,
        &event(&run, "RunRetried"),
    )
    .await
    .unwrap();
    assert_eq!(retry.retry_of_run_id.as_deref(), Some(run.id.as_str()));
    let terminal = transition_run(
        &pool,
        &run.company_id,
        &retry.id,
        1,
        RunStatus::Cancelled,
        None,
        None,
    )
    .await
    .unwrap();
    store_terminal_result(
        &pool,
        &RuntimeResult {
            id: "result".into(),
            company_id: retry.company_id.clone(),
            run_id: retry.id.clone(),
            run_status: terminal.status,
            result_summary: "cancelled".into(),
            output_payload: None,
            output_metadata: None,
            resource_usage_summary: None,
            failure_class: None,
            failure_detail: None,
            warnings: None,
            correlation_id: retry.correlation_id.clone(),
            causation_id: None,
        },
        &event(&retry, "RunResultStored"),
    )
    .await
    .unwrap();
    assert_eq!(count(&pool, "runtime_results").await, 1);
    assert_eq!(count(&pool, "outbox_messages").await, 2);

    let bad = Run::create(
        CompanyId("c".into()),
        "p".into(),
        "wi".into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "bad".into(),
    )
    .unwrap();
    let mut secret_event = event(&bad, "Bad");
    secret_event.payload = json!({"api_key":"must-not-persist"});
    assert!(
        create_run_with_event(&pool, &bad, &secret_event)
            .await
            .is_err()
    );
    assert!(
        get_run(&pool, &bad.company_id, &bad.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn stale_lease_cannot_heartbeat_or_release() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        &job(&run),
        &event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    let (_, lease_a) = claim_job(&pool, "worker-a", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let lease_a = heartbeat_lease(&pool, &lease_a.id, "worker-a", 1, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    expire_leases(&pool, lease_a.expires_at + Duration::seconds(1))
        .await
        .unwrap();
    sqlx::query("UPDATE durable_jobs SET available_at = ? WHERE run_id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
    let (_, lease_b) = claim_job(&pool, "worker-b", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();

    assert!(
        !verify_current_lease(
            &pool,
            &lease_a.run_id,
            "worker-a",
            &lease_a.id,
            lease_a.lease_version
        )
        .await
        .unwrap()
    );
    assert!(
        heartbeat_lease(
            &pool,
            &lease_a.id,
            "worker-a",
            lease_a.lease_version,
            Duration::seconds(30)
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        !release_current_lease(&pool, &lease_a, "STALE")
            .await
            .unwrap()
    );

    assert!(
        verify_current_lease(
            &pool,
            &lease_b.run_id,
            "worker-b",
            &lease_b.id,
            lease_b.lease_version
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn lease_fences_canonical_step_and_terminal_result_mutations() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        &job(&run),
        &event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    let (_, lease_a) = claim_job(&pool, "worker-a", Duration::milliseconds(1))
        .await
        .unwrap()
        .unwrap();
    expire_leases(&pool, lease_a.expires_at + Duration::seconds(1))
        .await
        .unwrap();
    sqlx::query("UPDATE durable_jobs SET available_at = ? WHERE run_id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
    let (_, lease_b) = claim_job(&pool, "worker-b", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let step =
        ExecutionStep::create(run.company_id.clone(), run.id.clone(), 1, "MODEL".into()).unwrap();
    assert!(
        append_step_with_lease(&pool, &lease_a, &step)
            .await
            .is_err()
    );
    append_step_with_lease(&pool, &lease_b, &step)
        .await
        .unwrap();
    assert_eq!(count(&pool, "execution_steps").await, 1);

    let inv = ModelInvocation {
        id: "fenced-inv".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        step_id: step.id.clone(),
        agent_id: run.executing_agent_id.clone(),
        provider_connection_id: "pc".into(),
        model_id: "m".into(),
        model_profile_version_id: None,
        invocation_index: 1,
        status: "RUNNING".into(),
        request_metadata: None,
        response_metadata: None,
        input_tokens: None,
        output_tokens: None,
        estimated_cost: None,
        latency_ms: None,
        provider_request_id: None,
        started_at: Utc::now(),
        completed_at: None,
        failure_class: None,
        failure_detail: None,
    };
    assert!(
        append_invocation_with_lease(&pool, &lease_a, &inv)
            .await
            .is_err()
    );
    append_invocation_with_lease(&pool, &lease_b, &inv)
        .await
        .unwrap();
    assert_eq!(count(&pool, "model_invocations").await, 1);

    let usage = UsageRecord {
        id: "fenced-usage".into(),
        workspace_id: "w".into(),
        company_id: run.company_id.clone(),
        project_id: run.project_id.clone(),
        work_item_id: run.work_item_id.clone(),
        agent_id: run.executing_agent_id.clone(),
        run_id: run.id.clone(),
        step_id: Some(step.id.clone()),
        provider_connection_id: "pc".into(),
        model_id: "m".into(),
        usage_type: "TOKENS".into(),
        quantity: 10,
        unit: "TOKENS".into(),
        estimated_cost: None,
        occurred_at: Utc::now(),
        metadata: None,
    };
    assert!(
        append_usage_with_lease(&pool, &lease_a, &usage)
            .await
            .is_err()
    );
    append_usage_with_lease(&pool, &lease_b, &usage)
        .await
        .unwrap();
    assert_eq!(count(&pool, "usage_records").await, 1);

    assert!(
        transition_run_with_lease(
            &pool,
            &lease_a,
            &run.company_id,
            &run.id,
            2,
            RunStatus::Cancelled,
            None,
            None
        )
        .await
        .is_err()
    );
    let run_cancelled = transition_run_with_lease(
        &pool,
        &lease_b,
        &run.company_id,
        &run.id,
        2,
        RunStatus::Cancelled,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(run_cancelled.status, RunStatus::Cancelled);
    let result = RuntimeResult {
        id: "fenced-result".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        run_status: RunStatus::Cancelled,
        result_summary: "cancelled".into(),
        output_payload: None,
        output_metadata: None,
        resource_usage_summary: None,
        failure_class: None,
        failure_detail: None,
        warnings: None,
        correlation_id: run.correlation_id.clone(),
        causation_id: None,
    };
    assert!(
        store_terminal_result_with_lease(&pool, &lease_a, &result, &event(&run, "Result"))
            .await
            .is_err()
    );
    store_terminal_result_with_lease(&pool, &lease_b, &result, &event(&run, "Result"))
        .await
        .unwrap();
    assert_eq!(count(&pool, "runtime_results").await, 1);
}

#[tokio::test]
async fn lease_fences_atomic_finalize_run_with_result() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        &job(&run),
        &event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    let (_, lease_a) = claim_job(&pool, "worker-a", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let run_running = transition_run_with_lease(
        &pool,
        &lease_a,
        &run.company_id,
        &run.id,
        2,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .unwrap();
    expire_leases(&pool, lease_a.expires_at + Duration::seconds(1))
        .await
        .unwrap();
    sqlx::query("UPDATE durable_jobs SET available_at = ? WHERE run_id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
    let (_, lease_b) = claim_job(&pool, "worker-b", Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();

    let result = RuntimeResult {
        id: "final-res".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        run_status: RunStatus::Succeeded,
        result_summary: "done".into(),
        output_payload: None,
        output_metadata: None,
        resource_usage_summary: None,
        failure_class: None,
        failure_detail: None,
        warnings: None,
        correlation_id: run.correlation_id.clone(),
        causation_id: None,
    };

    assert!(
        finalize_run_with_result_with_lease(
            &pool,
            &lease_a,
            &run.company_id,
            &run.id,
            run_running.row_version,
            &result,
            &event(&run, "RunSucceeded")
        )
        .await
        .is_err()
    );
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
    assert_eq!(count(&pool, "runtime_results").await, 0);

    let finalized = finalize_run_with_result_with_lease(
        &pool,
        &lease_b,
        &run.company_id,
        &run.id,
        run_running.row_version,
        &result,
        &event(&run, "RunSucceeded"),
    )
    .await
    .unwrap();
    assert_eq!(finalized.status, RunStatus::Succeeded);
    assert_eq!(count(&pool, "runtime_results").await, 1);
}

#[tokio::test]
async fn expired_lease_cannot_timeout_run() {
    let pool = setup().await;
    let run = run();
    create_run(&pool, &run).await.unwrap();
    queue_run(
        &pool,
        &run.company_id,
        &run.id,
        1,
        &job(&run),
        &event(&run, "RunQueued"),
    )
    .await
    .unwrap();
    let (_, lease) = claim_job(&pool, "worker-a", Duration::milliseconds(1))
        .await
        .unwrap()
        .unwrap();
    let _running = transition_run(
        &pool,
        &run.company_id,
        &run.id,
        2,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .unwrap();
    expire_leases(&pool, lease.expires_at + Duration::seconds(1))
        .await
        .unwrap();
    assert!(
        !verify_current_lease(
            &pool,
            &lease.run_id,
            "worker-a",
            &lease.id,
            lease.lease_version
        )
        .await
        .unwrap()
    );
    assert_eq!(
        get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Running
    );
    assert_eq!(count(&pool, "runtime_results").await, 0);
}

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    sqlx::query(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

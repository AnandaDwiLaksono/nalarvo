use chrono::{Duration, Utc};
use nalarvo_domain::{CompanyId, PrincipalRef, Run, RunStatus};
use nalarvo_persistence::{
    create_pool,
    m4::{self, DurableJob},
    run_migrations,
};
use serde_json::json;
use sqlx::SqlitePool;

async fn fixture() -> (tempfile::TempDir, SqlitePool, Run, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.db");
    let pool = create_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','2026-01-01','2026-01-01')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','2026-01-01','2026-01-01')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','2026-01-01','2026-01-01')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('d','c','D','2026-01-01','2026-01-01')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','2026-01-01','2026-01-01')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','d','role','2026-01-01')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('a','c','A','d','role',1,'2026-01-01','2026-01-01')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','2026-01-01','2026-01-01')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES('wi','c','p','W','IN_PROGRESS','TASK','2026-01-01','2026-01-01')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let run = Run::create(
        CompanyId("c".into()),
        "p".into(),
        "wi".into(),
        "a".into(),
        "MANUAL".into(),
        PrincipalRef::user("u"),
        "corr".into(),
    )
    .unwrap();
    m4::create_run(&pool, &run).await.unwrap();
    let job = DurableJob {
        id: "job".into(),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload: json!({}),
        available_at: Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    };
    let event = nalarvo_domain::DomainEvent {
        event_id: "queued".into(),
        event_type: "RunQueued".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: 1,
        occurred_at: Utc::now(),
        correlation_id: "corr".into(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::user("u"),
        scope: nalarvo_domain::ScopeRef::company("c"),
        payload: json!({"run_id":run.id}),
    };
    m4::queue_run(
        &pool,
        &run.company_id,
        &run.id,
        run.row_version,
        &job,
        &event,
    )
    .await
    .unwrap();
    let (_, lease) = m4::claim_job(&pool, "worker-a", Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    let queued = m4::get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    m4::transition_run_with_lease(
        &pool,
        &lease,
        &run.company_id,
        &run.id,
        queued.row_version,
        RunStatus::Running,
        None,
        None,
    )
    .await
    .unwrap();
    let step = m4::resolve_tool_step(&pool, &lease, &run.company_id)
        .await
        .unwrap();
    let input = json!({"tool_id":"filesystem.write","tool_version":"1.0.0","worker_principal_id":"worker-a","root_binding":"/root","path":"note.txt","content":"hello"});
    sqlx::query("INSERT INTO actions(id,workspace_id,company_id,run_id,step_id,requested_by_type,requested_by_id,action_type,target,input,authorization,status,idempotency_key,created_at,updated_at) VALUES('act','w','c',?,?,'SYSTEM','worker-a','filesystem.write','note.txt',?,'APPROVAL_REQUIRED','PENDING',?,'2026-01-01','2026-01-01')")
        .bind(&run.id).bind(&step).bind(input.to_string()).bind(format!("tool:{}:{}:call:filesystem.write",run.id,step)).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO approval_requests(id,action_id,workspace_id,company_id,requested_by_id,status,requested_at) VALUES('approval','act','w','c','worker-a','PENDING','2026-01-01')").execute(&pool).await.unwrap();
    let proposal = nalarvo_domain::DomainEvent {
        event_id: "proposed".into(),
        event_type: "ActionProposed".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Action".into(),
        aggregate_id: "act".into(),
        aggregate_version: 1,
        occurred_at: Utc::now(),
        correlation_id: "corr".into(),
        causation_id: run.id.clone(),
        principal: PrincipalRef::system("worker-a"),
        scope: nalarvo_domain::ScopeRef::company("c"),
        payload: json!({"fingerprint":"fp","project_id":"p"}),
    };
    let mut tx = pool.begin().await.unwrap();
    nalarvo_persistence::insert_domain_event_and_outbox_tx(&mut tx, &proposal)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    (dir, pool, run, step)
}

#[tokio::test]
async fn crash_between_action_proposal_and_approval_suspend_recovers_exact_continuation() {
    let (dir, pool, run, step) = fixture().await;
    drop(pool);
    let pool = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("crash.db").display()
    ))
    .await
    .unwrap();
    sqlx::query(
        "UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE run_id=?",
    )
    .bind(&run.id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE durable_jobs SET lease_until='2000-01-01T00:00:00+00:00' WHERE id='job'")
        .execute(&pool)
        .await
        .unwrap();
    m4::expire_leases(&pool, Utc::now()).await.unwrap();
    assert!(
        m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
            .await
            .unwrap()
    );
    assert!(
        !m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
            .await
            .unwrap()
    );
    let recovered = m4::get_run(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, RunStatus::WaitingApproval);
    let cp = m4::approval_checkpoint(&pool, &run.company_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    let metadata = cp.continuation_metadata.unwrap();
    assert_eq!(metadata["action_id"], "act");
    assert_eq!(metadata["step_id"], step);
    assert_eq!(metadata["approval_id"], "approval");
    assert_eq!(metadata["fingerprint"], "fp");
    assert_eq!(
        metadata["proposal"]["arguments"],
        json!({"path":"note.txt","content":"hello"})
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE id='job'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "WAITING_APPROVAL"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM approval_requests WHERE id='approval'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );
    assert!(
        m4::claim_job(&pool, "other", Duration::seconds(60))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn approved_resume_crash_requeues_only_unstarted_action() {
    let (_dir, pool, run, step) = fixture().await;
    let metadata = json!({"proposal":{"call_id":"call","tool_name":"filesystem.write","target":"note.txt","arguments":{"path":"note.txt","content":"hello"}},"step_id":step,"action_id":"act","approval_id":"approval","fingerprint":"fp","original_worker_principal_id":"worker-a","idempotency_key":format!("tool:{}:{}:call:filesystem.write",run.id,step)});
    sqlx::query("INSERT INTO runtime_checkpoints(id,company_id,run_id,checkpoint_version,run_state,active_step,execution_phase,continuation_metadata,safe_to_resume,created_at) VALUES('cp','c',?,1,'WAITING_APPROVAL',1,'APPROVAL_PENDING',?,1,'2026-01-01')")
        .bind(&run.id).bind(metadata.to_string()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE approval_requests SET status='APPROVED' WHERE id='approval'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE actions SET authorization='APPROVED' WHERE id='act'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE run_id=?",
    )
    .bind(&run.id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE durable_jobs SET lease_until='2000-01-01T00:00:00+00:00' WHERE id='job'")
        .execute(&pool)
        .await
        .unwrap();
    m4::expire_leases(&pool, Utc::now()).await.unwrap();
    assert!(
        m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
            .await
            .unwrap()
    );
    assert_eq!(
        m4::get_run(&pool, &run.company_id, &run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Queued
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE id='job'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );
    let (_, lease) = m4::claim_job(&pool, "worker-b", Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !m4::verify_current_lease(
            &pool,
            &run.id,
            "worker-a",
            &sqlx::query_scalar::<_, String>(
                "SELECT id FROM run_execution_leases WHERE worker_principal_id='worker-a'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        )
        .await
        .unwrap()
    );
    assert!(
        m4::verify_current_lease(&pool, &run.id, "worker-b", &lease.id, 1)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions WHERE run_id=?")
            .bind(&run.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn started_or_unknown_action_never_recovers() {
    for status in ["STARTED", "OUTCOME_UNKNOWN"] {
        let (_dir, pool, run, _) = fixture().await;
        sqlx::query("UPDATE actions SET status=? WHERE id='act'")
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE run_id=?",
        )
        .bind(&run.id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            !m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
                .await
                .unwrap()
        );
        assert_eq!(
            m4::get_run(&pool, &run.company_id, &run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Running
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_checkpoints WHERE run_id=?")
                .bind(&run.id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn fresh_lease_and_mismatched_approval_never_recover() {
    let (_dir, pool, run, _) = fixture().await;
    assert!(
        !m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
            .await
            .unwrap()
    );
    sqlx::query(
        "UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE run_id=?",
    )
    .bind(&run.id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE approval_requests SET status='CANCELLED' WHERE id='approval'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn recovery_uses_instants_and_rejects_malformed_lease_history() {
    use chrono::FixedOffset;
    for (expiry, recover) in [
        (
            (Utc::now() - Duration::minutes(1))
                .with_timezone(&FixedOffset::east_opt(7 * 3600).unwrap())
                .to_rfc3339(),
            true,
        ),
        (
            (Utc::now() + Duration::minutes(1))
                .with_timezone(&FixedOffset::west_opt(7 * 3600).unwrap())
                .to_rfc3339(),
            false,
        ),
        ("0000-invalid".into(), false),
        ("zzzz-invalid".into(), false),
    ] {
        let (dir, pool, run, _) = fixture().await;
        sqlx::query("UPDATE run_execution_leases SET expires_at=? WHERE run_id=?")
            .bind(&expiry)
            .bind(&run.id)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let pool = create_pool(&format!(
            "sqlite://{}",
            dir.path().join("crash.db").display()
        ))
        .await
        .unwrap();
        let (a, b) = tokio::join!(
            m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery-a"),
            m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "recovery-b"),
        );
        assert_eq!(
            usize::from(a.unwrap()) + usize::from(b.unwrap()),
            usize::from(recover),
            "{expiry}"
        );
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM run_execution_leases WHERE run_id=? AND released_at IS NULL",
        )
        .bind(&run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(open, i64::from(!recover), "{expiry}");
        assert!(
            !m4::recover_orphaned_approval(&pool, &run.company_id, &run.id, "again")
                .await
                .unwrap()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tool_invocations")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
}

use nalarvo_application::ApplicationContext;
use nalarvo_domain::{ExecutionStepStatus, RunStatus};
use nalarvo_runtime::{
    core_dispatch::{TrustedRunContext, dispatch, dispatch_with_ack},
    worker::{RunExecutionRequest, execute_run_worker_process},
};
use serde_json::json;
use std::sync::Arc;

async fn setup() -> (ApplicationContext, tempfile::TempDir, TrustedRunContext) {
    let root = tempfile::tempdir().unwrap();
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        root.path().join("core.db").display()
    ))
    .await
    .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('d','c','D','t','t')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','t','t')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','d','role','t')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('a','c','A','d','role',1,'t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES('team','c','p','T','ACTIVE','t','t')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES('alloc','c','p','team','a','ACTIVE','t','t')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES('wi','c','p','W','IN_PROGRESS','TASK','t','t')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,status,assigned_at) VALUES('assignment','c','p','wi','a','alloc','ACTIVE','t')",
        "INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,row_version,created_at,updated_at) VALUES('r','c','p','wi','a','RUNNING','MANUAL',1,'USER','u','2026-01-01T00:00:00Z','corr',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO run_execution_leases(id,run_id,worker_principal_id,lease_version,acquired_at,heartbeat_at,expires_at,created_at) VALUES('l','r','worker',1,'t','t','2099-01-01T00:00:00Z','t')",
        "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd','w','filesystem.write','filesystem','write','t')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg','w','c','pd','SYSTEM','worker','PROJECT','p','ALLOW','t')",
        "INSERT INTO provider_connections(id,workspace_id,provider_kind,name,status,row_version,created_at,updated_at) VALUES('pc','w','mock','Mock','ENABLED',1,'t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    sqlx::query("UPDATE projects SET working_root_path=? WHERE id='p'")
        .bind(root.path().to_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    for table in ["projects", "work_items"] {
        sqlx::query(&format!("UPDATE {table} SET created_at='2026-01-01T00:00:00Z', updated_at='2026-01-01T00:00:00Z'"))
            .execute(&pool).await.unwrap();
    }
    let ctx = TrustedRunContext {
        run_id: "r".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        lease_id: "l".into(),
        lease_version: 1,
        worker_principal_id: "worker".into(),
        live_lease_version: None,
    };
    (ApplicationContext::new(pool), root, ctx)
}

async fn real_worker(
    app: ApplicationContext,
    ctx: TrustedRunContext,
) -> Result<nalarvo_runtime::worker::WorkerExecutionOutcome, String> {
    let core = app.clone();
    let req = RunExecutionRequest {
        run_id: ctx.run_id,
        company_id: ctx.company_id,
        project_id: ctx.project_id,
        work_item_id: ctx.work_item_id,
        agent_id: ctx.agent_id,
        model_profile_version_id: None,
        provider_connection_id: "pc".into(),
        base_url: None,
        auth_token: None,
        model_key: "mock".into(),
        context_input: nalarvo_runtime::context::ContextInput::minimal("c", "p", "write"),
        worker_principal_id: ctx.worker_principal_id,
        lease_id: ctx.lease_id,
        lease_version: ctx.lease_version,
        pool: Some(app.pool),
        expected_run_version: 1,
        mock_mode: nalarvo_model_gateway::MockMode::TOOL_PROPOSAL,
        cancel_rx: None,
        model_handler: None,
        resume_proposal: None,
        action_handler: Some(Arc::new(move |ctx, proposal| {
            let app = core.clone();
            Box::pin(async move { dispatch(&app, &ctx, proposal).await })
        })),
    };
    execute_run_worker_process(
        std::path::Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")),
        req,
        None,
    )
    .await
}
async fn count(app: &ApplicationContext, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&app.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn security_current_scope_lifecycle_budget_and_lease_block_writes() {
    for mutation in [
        "INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('deny','w',NULL,'Deny','ACTIVE',1,'t','t'); INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('deny-v','deny',1,'{\"permission\":\"filesystem.write\",\"effect\":\"DENY\"}','t')",
        "UPDATE permission_grants SET workspace_id='foreign'",
        "UPDATE permission_definitions SET workspace_id='foreign'",
        "UPDATE permission_grants SET scope_type='BROKEN',effect='DENY'",
        "UPDATE workspaces SET status='SUSPENDED'",
        "UPDATE teams SET status='PAUSED'",
        "UPDATE run_execution_leases SET expires_at='zzz'",
        "UPDATE run_execution_leases SET expires_at='2000-01-01T23:00:00-12:00'",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,spent_amount,created_at,updated_at) VALUES('b','w','c','B',1,-1,'t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','BROKEN',10,'t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,spent_amount,created_at,updated_at) VALUES('b','w','c','B',1,1,'t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,ends_at,created_at,updated_at) VALUES('b','w','c','B',10,'broken','t','t')",
    ] {
        let (app, root, ctx) = setup().await;
        sqlx::query(mutation).execute(&app.pool).await.unwrap();
        let proposal = nalarvo_runtime::worker::ToolProposal {
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: json!({"path":"note.txt","content":"blocked"}),
            call_id: Some("security".into()),
        };
        let result = dispatch(&app, &ctx, proposal).await;
        assert!(
            !matches!(result,Ok(ref outcome) if outcome.status==RunStatus::Succeeded),
            "{mutation}: {result:?}"
        );
        assert!(!root.path().join("note.txt").exists(), "{mutation}");
        assert_eq!(count(&app, "tool_invocations").await, 0, "{mutation}");
    }
}

#[tokio::test]
async fn security_lease_utc_instant_not_text_order() {
    let (app, _root, _ctx) = setup().await;
    for (expiry, valid) in [
        ("2999-01-01T00:00:00-12:00", true),
        ("zzz", false),
        ("2000-01-01T23:59:59-12:00", false),
    ] {
        sqlx::query("UPDATE run_execution_leases SET expires_at=?")
            .bind(expiry)
            .execute(&app.pool)
            .await
            .unwrap();
        assert_eq!(
            nalarvo_persistence::m4::verify_current_lease(&app.pool, "r", "worker", "l", 1)
                .await
                .unwrap(),
            valid
        );
    }
    let now = chrono::Utc::now();
    let expiry = (now - chrono::Duration::seconds(1))
        .with_timezone(&chrono::FixedOffset::east_opt(12 * 3600).unwrap())
        .to_rfc3339();
    sqlx::query("UPDATE run_execution_leases SET expires_at=?")
        .bind(expiry)
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(
        !nalarvo_persistence::m4::verify_current_lease(&app.pool, "r", "worker", "l", 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn direct_allow_real_worker_persists_one_linked_action_and_replay() {
    let (app, root, ctx) = setup().await;
    let outcome = real_worker(app.clone(), ctx.clone()).await.unwrap();
    assert_eq!(outcome.status, RunStatus::Succeeded, "{outcome:?}");
    assert!(root.path().join("note.txt").exists());
    assert_eq!(count(&app, "actions").await, 1);
    assert_eq!(count(&app, "execution_steps").await, 1);
    assert_eq!(count(&app, "authority_decisions").await, 1);
    assert_eq!(count(&app, "approval_requests").await, 0);
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    let linked:i64=sqlx::query_scalar("SELECT COUNT(*) FROM tool_invocations t JOIN actions a ON t.action_id=a.id JOIN execution_steps s ON a.step_id=s.id JOIN runs r ON a.run_id=r.id WHERE t.run_id=r.id AND t.step_id=s.id AND a.project_id=r.project_id AND a.work_item_id=r.work_item_id AND a.agent_id=r.executing_agent_id AND a.status='SUCCEEDED' AND t.status='SUCCEEDED'").fetch_one(&app.pool).await.unwrap();
    assert_eq!(linked, 1);
    let replay = real_worker(app.clone(), ctx).await.unwrap();
    assert_eq!(replay.status, RunStatus::Succeeded);
    assert_eq!(count(&app, "actions").await, 1);
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionSucceeded'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(outcome.model_step_status, ExecutionStepStatus::Succeeded);
    let _: serde_json::Value = json!({"verified":true});
}

#[tokio::test]
async fn test_approval_real_worker_checkpoint_and_resume_same_action() {
    let (app, root, ctx) = setup().await;
    sqlx::query("INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd_appr','w','appr','filesystem','write','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg_appr','w','c','pd_appr','SYSTEM','worker','PROJECT','p','ALLOW','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol','w','c','p1','ACTIVE',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv1','pol',1,'{\"permission\":\"filesystem.write\",\"effect\":\"REQUIRE_APPROVAL\"}','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    let outcome = real_worker(app.clone(), ctx.clone()).await.unwrap();
    assert_eq!(outcome.status, RunStatus::WaitingApproval, "{outcome:?}");
    assert!(!root.path().join("note.txt").exists());
    let action = sqlx::query("SELECT id,authorization,status FROM actions WHERE run_id='r'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    use sqlx::Row;
    let action_id: String = action.get("id");
    assert_eq!(
        action.get::<String, _>("authorization"),
        "APPROVAL_REQUIRED"
    );
    let approval = sqlx::query("SELECT id,status FROM approval_requests WHERE action_id=?")
        .bind(&action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let app_id: String = approval.get("id");
    assert_eq!(approval.get::<String, _>("status"), "PENDING");
    let saved_fp: String = sqlx::query_scalar("SELECT fingerprint FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    app.pool.close().await;
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        root.path().join("core.db").display()
    ))
    .await
    .unwrap();
    let core_restarted = ApplicationContext::new(pool);
    let run_version: i64 = sqlx::query_scalar("SELECT row_version FROM runs WHERE id='r'")
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap();
    core_restarted
        .wait_run_for_approval(
            &nalarvo_domain::CompanyId("c".into()),
            "p",
            "r",
            run_version,
        )
        .await
        .unwrap();
    let step_id: String = sqlx::query_scalar("SELECT step_id FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO durable_jobs(id,job_type,company_id,run_id,payload,available_at,status,correlation_id,created_at,updated_at) VALUES('job','EXECUTE_RUN','c','r','{}','t','WAITING_APPROVAL','corr','t','t')")
        .execute(&core_restarted.pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_checkpoints(id,company_id,run_id,checkpoint_version,run_state,execution_phase,continuation_metadata,created_at) VALUES('cp','c','r',1,'WAITING_APPROVAL','APPROVAL_PENDING',?,'t')")
        .bind(json!({"action_id":action_id,"approval_id":app_id,"fingerprint":saved_fp,"step_id":step_id,"idempotency_key":sqlx::query_scalar::<_,String>("SELECT idempotency_key FROM actions WHERE id=?").bind(&action_id).fetch_one(&core_restarted.pool).await.unwrap()}).to_string())
        .execute(&core_restarted.pool).await.unwrap();
    core_restarted
        .decide_action_approval(
            "c",
            &app_id,
            nalarvo_domain::PrincipalRef::user("u"),
            &saved_fp,
            true,
            Some("approved".into()),
        )
        .await
        .unwrap();

    sqlx::query("UPDATE run_execution_leases SET lease_version=2,worker_principal_id='replacement', heartbeat_at='2026-01-01T00:00:00Z' WHERE id='l'").execute(&core_restarted.pool).await.unwrap();
    sqlx::query("UPDATE permission_grants SET principal_id='replacement'")
        .execute(&core_restarted.pool)
        .await
        .unwrap();
    let mut ctx_resumed = ctx.clone();
    ctx_resumed.lease_version = 2;
    ctx_resumed.worker_principal_id = "replacement".into();
    let row = sqlx::query("SELECT input,idempotency_key FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap();
    let cmd = nalarvo_application::ProposeActionCommand {
        company_id: "c".into(),
        project_id: Some("p".into()),
        run_id: Some("r".into()),
        step_id: Some(step_id),
        actor: nalarvo_domain::PrincipalRef::system("replacement"),
        action_type: "filesystem.write".into(),
        target: "note.txt".into(),
        parameters: Some(serde_json::from_str(&row.get::<String, _>("input")).unwrap()),
        idempotency_key: row.get("idempotency_key"),
        meta: Default::default(),
    };
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: "l".into(),
        run_id: "r".into(),
        worker_principal_id: "replacement".into(),
        lease_version: 2,
        expires_at: chrono::Utc::now(),
    };
    for (change, restore) in [
        (
            "UPDATE policies SET company_id=NULL; UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"DENY\"}'",
            "UPDATE policies SET company_id='c'; UPDATE policy_versions SET rules='{\"permission\":\"filesystem.write\",\"effect\":\"REQUIRE_APPROVAL\"}'",
        ),
        (
            "INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,created_at,updated_at) VALUES('project-b','w','c','p','Project',0,'t','t')",
            "DELETE FROM budgets WHERE id='project-b'",
        ),
        (
            "UPDATE permission_grants SET workspace_id='foreign'",
            "UPDATE permission_grants SET workspace_id='w'",
        ),
        (
            "UPDATE permission_definitions SET workspace_id='foreign'",
            "UPDATE permission_definitions SET workspace_id='w'",
        ),
        (
            "UPDATE policies SET status='BROKEN'",
            "UPDATE policies SET status='ACTIVE'",
        ),
        (
            "UPDATE permission_grants SET scope_type='BROKEN',effect='DENY'",
            "UPDATE permission_grants SET scope_type='PROJECT',effect='ALLOW'",
        ),
        (
            "UPDATE workspaces SET status='SUSPENDED'",
            "UPDATE workspaces SET status='ACTIVE'",
        ),
        (
            "UPDATE teams SET status='PAUSED'",
            "UPDATE teams SET status='ACTIVE'",
        ),
        (
            "UPDATE run_execution_leases SET expires_at='zzz'",
            "UPDATE run_execution_leases SET expires_at='2099-01-01T00:00:00Z'",
        ),
        (
            "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,spent_amount,created_at,updated_at) VALUES('b','w','c','B',0,-2,'t','t')",
            "DELETE FROM budgets WHERE id='b'",
        ),
        (
            "INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','BROKEN',0,'t','t')",
            "DELETE FROM budgets WHERE id='b'",
        ),
        (
            "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,created_at,updated_at) VALUES('b','w','c','B',0,'t','t')",
            "DELETE FROM budgets WHERE id='b'",
        ),
    ] {
        sqlx::query(change)
            .execute(&core_restarted.pool)
            .await
            .unwrap();
        assert!(
            core_restarted
                .resume_approved_runtime_action(&cmd, &action_id, &app_id, &lease, run_version + 1)
                .await
                .is_err(),
            "{change}"
        );
        assert!(!root.path().join("note.txt").exists());
        assert_eq!(count(&core_restarted, "tool_invocations").await, 0);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM runs WHERE id='r'")
                .fetch_one(&core_restarted.pool)
                .await
                .unwrap(),
            "WAITING_APPROVAL"
        );
        sqlx::query(restore)
            .execute(&core_restarted.pool)
            .await
            .unwrap();
    }
    core_restarted
        .resume_approved_runtime_action(&cmd, &action_id, &app_id, &lease, run_version + 1)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE event_type='RunResumed'"
        )
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap(),
        1
    );
    let resume_outcome = real_worker(core_restarted.clone(), ctx_resumed)
        .await
        .unwrap();
    assert_eq!(resume_outcome.status, RunStatus::Succeeded);
    assert!(root.path().join("note.txt").exists());
    assert_eq!(count(&core_restarted, "actions").await, 1);
    let resumed_action = sqlx::query("SELECT id,authorization,status FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap();
    assert_eq!(resumed_action.get::<String, _>("authorization"), "APPROVED");
    assert_eq!(resumed_action.get::<String, _>("status"), "SUCCEEDED");
    assert_eq!(count(&core_restarted, "tool_invocations").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fingerprint FROM actions WHERE id=?")
            .bind(&action_id)
            .fetch_one(&core_restarted.pool)
            .await
            .unwrap(),
        saved_fp
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionProposed'"
        )
        .fetch_one(&core_restarted.pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn test_stale_lease_fails_closed_before_tool_execution() {
    let (app, root, ctx) = setup().await;
    let mut bad_ctx = ctx.clone();
    bad_ctx.lease_version = 999;
    let outcome = real_worker(app.clone(), bad_ctx).await;
    assert!(
        outcome
            .unwrap_err()
            .contains("lease expired or was preempted")
    );
    assert!(!root.path().join("note.txt").exists());
    assert_eq!(count(&app, "actions").await, 0);
    assert_eq!(count(&app, "tool_invocations").await, 0);
}

#[tokio::test]
async fn test_cancel_while_waiting_approval_blocks_late_resume() {
    let (app, root, ctx) = setup().await;
    sqlx::query("INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd_c','w','c','filesystem','write','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg_c','w','c','pd_c','SYSTEM','worker','PROJECT','p','ALLOW','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol_c','w','c','pc','ACTIVE',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pvc','pol_c',1,'{\"permission\":\"filesystem.write\",\"effect\":\"REQUIRE_APPROVAL\"}','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    let outcome = real_worker(app.clone(), ctx.clone()).await.unwrap();
    assert_eq!(outcome.status, RunStatus::WaitingApproval);
    let action_id: String = sqlx::query_scalar("SELECT id FROM actions WHERE run_id='r'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let approval_id: String =
        sqlx::query_scalar("SELECT id FROM approval_requests WHERE action_id=?")
            .bind(&action_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let saved_fp: String = sqlx::query_scalar("SELECT fingerprint FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    // Run is cancelled
    app.wait_run_for_approval(&nalarvo_domain::CompanyId("c".into()), "p", "r", 1)
        .await
        .unwrap();
    app.cancel_run(
        &nalarvo_domain::CompanyId("c".into()),
        "p",
        "r",
        2,
        Some("cancel before approval".into()),
    )
    .await
    .unwrap();
    // Late approval is attempted
    assert!(
        app.decide_action_approval(
            "c",
            &approval_id,
            nalarvo_domain::PrincipalRef::user("u"),
            &saved_fp,
            true,
            Some("late".into())
        )
        .await
        .is_err()
    );
    // Worker attempt fails closed
    let resume = real_worker(app.clone(), ctx).await;
    assert!(!matches!(resume, Ok(o) if o.status==RunStatus::Succeeded));
    assert!(!root.path().join("note.txt").exists());
    let action_status: String = sqlx::query_scalar("SELECT status FROM actions WHERE id=?")
        .bind(&action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_ne!(action_status, "SUCCEEDED");
    assert_eq!(count(&app, "actions").await, 1);
    assert_eq!(count(&app, "tool_invocations").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM runs WHERE id='r'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "CANCELLED"
    );
}

#[tokio::test]
async fn budget_without_tool_limit_rejects_exhausted_total_durably() {
    let (app, root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','ACTIVE',0,'t','t')").execute(&app.pool).await.unwrap();
    let proposal = nalarvo_runtime::worker::ToolProposal {
        call_id: Some("no-limit".into()),
        tool_name: "filesystem.write".into(),
        target: "note.txt".into(),
        arguments: json!({"path":"note.txt","content":"forbidden"}),
    };
    operation(&app, &ctx, "prepare").await;
    assert!(
        dispatch(&app, &ctx, proposal.clone())
            .await
            .unwrap_err()
            .contains("BUDGET_EXHAUSTED")
    );
    assert!(!root.path().join("note.txt").exists());
    assert_eq!(count(&app, "tool_invocations").await, 0);
    assert_eq!(count(&app, "resource_reservations").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM actions WHERE status='REJECTED' AND result='BUDGET_EXHAUSTED'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM authority_decisions WHERE decision='BUDGET_EXHAUSTED'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    for event in ["ActionBudgetRejected", "BudgetExhausted"] {
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_events WHERE event_type=? AND id IN (SELECT domain_event_id FROM outbox_messages)").bind(event).fetch_one(&app.pool).await.unwrap(), 1, "{event} outbox");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_records WHERE action=?")
                .bind(event)
                .fetch_one(&app.pool)
                .await
                .unwrap(),
            1,
            "{event} audit"
        );
    }
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_events WHERE event_type='BudgetExhausted' AND aggregate_type='Budget' AND aggregate_id='b' AND json_extract(payload,'$.action_id') IN (SELECT id FROM actions) AND json_extract(payload,'$.resource_type')='TOOL_CALL'").fetch_one(&app.pool).await.unwrap(), 1);
    assert!(
        !matches!(dispatch(&app, &ctx, proposal).await, Ok(o) if o.status==RunStatus::Succeeded)
    );
    assert!(!root.path().join("note.txt").exists());
    assert_eq!(count(&app, "tool_invocations").await, 0);
    for event in ["ActionBudgetRejected", "BudgetExhausted"] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_events WHERE event_type=?")
                .bind(event)
                .fetch_one(&app.pool)
                .await
                .unwrap(),
            1
        );
    }
}

#[tokio::test]
async fn budget_without_tool_limit_reserves_and_settles_total() {
    let (app, _root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
    let (cmd, lease) = operation(&app, &ctx, "no-limit-available").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='b'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (1.0, 0.0)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM resource_reservations WHERE budget_id='b' AND action_id=?"
        )
        .bind(&action.action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        "HELD"
    );
    app.finish_runtime_action(&cmd, &action.action_id, &lease, "SUCCEEDED", &json!({}))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='b'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (0.0, 1.0)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM resource_reservations WHERE budget_id='b' AND action_id=?"
        )
        .bind(&action.action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        "REALIZED"
    );
    assert_eq!(count(&app, "budget_limits").await, 0);
    app.finish_runtime_action(&cmd, &action.action_id, &lease, "SUCCEEDED", &json!({}))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, f64>("SELECT spent_amount FROM budgets WHERE id='b'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1.0
    );
}

#[tokio::test]
async fn test_budget_accounting_atomic_race_protection() {
    let (app, _root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,currency,status,total_limit,reserved_amount,spent_amount,created_at,updated_at) VALUES('b','w','c','B','USD','ACTIVE',1,0,0,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount,reserved_amount,spent_amount) VALUES('bl','b','TOOL_CALL',1,0,0)").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,created_at,updated_at) VALUES('project','w','c','p','Project',1,'t','t')").execute(&app.pool).await.unwrap();
    // Distinct authorized Actions compete for the last slot.
    let mut ctx2 = ctx.clone();
    ctx2.run_id = "r2".into();
    sqlx::query("INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,row_version,created_at,updated_at) VALUES('r2','c','p','wi','a','RUNNING','MANUAL',2,'USER','u','2026-01-01T00:00:00Z','c2',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO run_execution_leases(id,run_id,worker_principal_id,lease_version,acquired_at,heartbeat_at,expires_at,created_at) VALUES('l2','r2','worker',1,'t','t','2099-01-01T00:00:00Z','t')").execute(&app.pool).await.unwrap();
    ctx2.lease_id = "l2".into();
    let (first, second) = tokio::join!(
        real_worker(app.clone(), ctx),
        real_worker(app.clone(), ctx2)
    );
    let results = [first, second];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r,Ok(o) if o.status==RunStatus::Succeeded))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r,Err(e) if e.contains("BUDGET_EXHAUSTED")))
            .count(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions WHERE status='REJECTED' AND result LIKE '%BUDGET_EXHAUSTED%'").fetch_one(&app.pool).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM authority_decisions WHERE decision='BUDGET_EXHAUSTED'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionBudgetRejected'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox_messages WHERE domain_event_id IN (SELECT id FROM domain_events WHERE event_type='ActionBudgetRejected')").fetch_one(&app.pool).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_records WHERE action='ActionBudgetRejected'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    let loser: String = sqlx::query_scalar("SELECT id FROM actions WHERE status='REJECTED'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let loser_run: String = sqlx::query_scalar("SELECT run_id FROM actions WHERE id=?")
        .bind(&loser)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let mut replay_ctx = TrustedRunContext {
        run_id: loser_run.clone(),
        ..TrustedRunContext {
            run_id: "r".into(),
            company_id: "c".into(),
            project_id: "p".into(),
            work_item_id: "wi".into(),
            agent_id: "a".into(),
            lease_id: "l".into(),
            lease_version: 1,
            worker_principal_id: "worker".into(),
            live_lease_version: None,
        }
    };
    if loser_run == "r2" {
        replay_ctx.lease_id = "l2".into();
    }
    assert_eq!(
        real_worker(app.clone(), replay_ctx).await.unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionBudgetRejected'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, f64>(
            "SELECT reserved_amount+spent_amount FROM budget_limits WHERE id='bl'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1.0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions WHERE status='SUCCEEDED'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budgets WHERE id='project'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        (0.0, 1.0)
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM resource_reservations WHERE budget_id='project' AND status='REALIZED'").fetch_one(&app.pool).await.unwrap(),1);
}

#[tokio::test]
async fn company_and_project_budgets_both_gate_tools_and_settle_once() {
    for exhausted in ["company", "project", "none"] {
        let (app, root, ctx) = setup().await;
        let _ = operation(&app, &ctx, "scope-lineage").await;
        for q in [
            "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('other','c','Other','MEDIUM','ACTIVE','t','t')",
            "INSERT INTO budgets(id,workspace_id,company_id,project_id,name,total_limit,created_at,updated_at) VALUES('company','w','c',NULL,'Company',1,'t','t'),('project','w','c','p','Project',1,'t','t'),('other','w','c','other','Other',0,'t','t')",
        ] {
            sqlx::query(q).execute(&app.pool).await.unwrap();
        }
        sqlx::query("UPDATE budgets SET total_limit=0 WHERE id=?")
            .bind(exhausted)
            .execute(&app.pool)
            .await
            .unwrap();
        let result = dispatch(
            &app,
            &ctx,
            nalarvo_runtime::worker::ToolProposal {
                call_id: Some("scoped-budget".into()),
                tool_name: "filesystem.write".into(),
                target: "note.txt".into(),
                arguments: json!({"path":"note.txt","content":"scoped"}),
            },
        )
        .await;
        if exhausted != "none" {
            let error = result.unwrap_err();
            assert!(error.contains("BUDGET_EXHAUSTED"), "{exhausted}: {error}");
            assert!(!root.path().join("note.txt").exists());
            assert_eq!(count(&app, "tool_invocations").await, 0);
            assert_eq!(count(&app, "resource_reservations").await, 0);
        } else {
            assert_eq!(result.unwrap().status, RunStatus::Succeeded);
            assert_eq!(
                std::fs::read_to_string(root.path().join("note.txt")).unwrap(),
                "scoped"
            );
            assert_eq!(count(&app, "resource_reservations").await, 2);
            let values: Vec<(String, f64, f64)> =
                sqlx::query_as("SELECT id,reserved_amount,spent_amount FROM budgets ORDER BY id")
                    .fetch_all(&app.pool)
                    .await
                    .unwrap();
            assert_eq!(
                values,
                vec![
                    ("company".into(), 0.0, 1.0),
                    ("other".into(), 0.0, 0.0),
                    ("project".into(), 0.0, 1.0)
                ]
            );
        }
    }
}

async fn operation(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
    key: &str,
) -> (
    nalarvo_application::ProposeActionCommand,
    nalarvo_persistence::m4::ExecutionLease,
) {
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: ctx.lease_version,
        expires_at: chrono::Utc::now(),
    };
    let step = nalarvo_persistence::m4::resolve_tool_step(
        &app.pool,
        &lease,
        &nalarvo_domain::CompanyId(ctx.company_id.clone()),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE execution_steps SET input_metadata=? WHERE id=?")
        .bind(json!({"provider_connection_id":"pc"}).to_string())
        .bind(&step)
        .execute(&app.pool)
        .await
        .unwrap();
    let root: String = sqlx::query_scalar("SELECT working_root_path FROM projects WHERE id='p'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    #[allow(unused_mut)]
    let mut parameters = json!({"path":"note.txt","content":"checked","tool_id":"filesystem.write","tool_version":"1.0.0","worker_principal_id":ctx.worker_principal_id,"root_binding":std::fs::canonicalize(&root).unwrap().to_str().unwrap(),"provider_connection_id":"pc"});
    #[cfg(windows)]
    {
        parameters["filesystem_binding"] =
            nalarvo_application::project_filesystem::ProjectFilesystem::open(std::path::Path::new(
                &root,
            ))
            .unwrap()
            .write_binding("note.txt")
            .unwrap();
    }
    (
        nalarvo_application::ProposeActionCommand {
            company_id: ctx.company_id.clone(),
            project_id: Some(ctx.project_id.clone()),
            run_id: Some(ctx.run_id.clone()),
            step_id: Some(step),
            actor: nalarvo_domain::PrincipalRef::system(&ctx.worker_principal_id),
            action_type: "filesystem.write".into(),
            target: "note.txt".into(),
            parameters: Some(parameters),
            idempotency_key: Some(key.into()),
            meta: Default::default(),
        },
        lease,
    )
}

#[cfg(windows)]
#[tokio::test]
async fn windows_started_write_rechecks_lease_cancel_root_and_authority_before_effect() {
    for change in [
        "UPDATE run_execution_leases SET lease_version=2 WHERE id='l'",
        "UPDATE runs SET lifecycle_state='CANCELLED' WHERE id='r'",
        "UPDATE projects SET working_root_path=NULL WHERE id='p'",
        "DELETE FROM permission_grants WHERE id='pg'",
    ] {
        let (app, root, ctx) = setup().await;
        let (cmd, lease) = operation(&app, &ctx, "late-fence").await;
        let action = app.propose_action(cmd.clone()).await.unwrap();
        assert!(
            app.start_runtime_action(&cmd, &action.action_id, &lease)
                .await
                .unwrap()
        );
        // Deterministic interleaving: change commits AFTER STARTED, BEFORE effect.
        sqlx::query(change).execute(&app.pool).await.unwrap();
        let called = std::sync::atomic::AtomicBool::new(false);
        assert!(
            app.execute_runtime_write(&cmd, &action.action_id, &lease, || {
                called.store(true, std::sync::atomic::Ordering::SeqCst);
                std::fs::write(root.path().join("note.txt"), b"bad").map_err(|e| e.to_string())
            })
            .await
            .is_err(),
            "{change}"
        );
        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!root.path().join("note.txt").exists());
    }
}

#[tokio::test]
async fn budget_rejection_audit_failure_rolls_back_entire_decision() {
    let (app, _root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','ACTIVE',0,'t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount) VALUES('bl','b','TOOL_CALL',0)").execute(&app.pool).await.unwrap();
    let (cmd, lease) = operation(&app, &ctx, "budget-audit").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_budget_audit BEFORE INSERT ON audit_records WHEN NEW.action='ActionBudgetRejected' BEGIN SELECT RAISE(ABORT,'synthetic audit failure'); END").execute(&app.pool).await.unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .is_err()
    );
    assert_eq!(
        app.get_action("c", &action.action_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "PENDING"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM authority_decisions WHERE decision='BUDGET_EXHAUSTED'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionBudgetRejected'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_budget_audit")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .unwrap_err()
            .to_string()
            .contains("BUDGET_EXHAUSTED")
    );
    assert_eq!(
        app.get_action("c", &action.action_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "REJECTED"
    );
    assert_eq!(count(&app, "tool_invocations").await, 0);
}

#[tokio::test]
async fn live_company_agent_work_and_assignment_gate_runtime_start() {
    let (app, root, ctx) = setup().await;
    for (table, id, value) in [
        ("companies", "c", "PAUSED"),
        ("agents", "a", "PAUSED"),
        ("work_items", "wi", "BLOCKED"),
        ("assignments", "assignment", "RELEASED"),
        ("agent_allocations", "alloc", "PAUSED"),
    ] {
        let (cmd, lease) = operation(&app, &ctx, table).await;
        let action = app.propose_action(cmd.clone()).await.unwrap();
        sqlx::query(&format!("UPDATE {table} SET status=? WHERE id=?"))
            .bind(value)
            .bind(id)
            .execute(&app.pool)
            .await
            .unwrap();
        assert!(
            app.start_runtime_action(&cmd, &action.action_id, &lease)
                .await
                .is_err(),
            "{table} must be current"
        );
        assert_eq!(
            app.get_action("c", &action.action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "PENDING"
        );
        assert_eq!(count(&app, "tool_invocations").await, 0);
        sqlx::query(&format!("UPDATE {table} SET status=? WHERE id=?"))
            .bind(if table == "work_items" {
                "IN_PROGRESS"
            } else {
                "ACTIVE"
            })
            .bind(id)
            .execute(&app.pool)
            .await
            .unwrap();
    }
    assert!(!root.path().join("note.txt").exists());
}

#[tokio::test]
async fn allowed_action_preempted_before_execution_is_fenced() {
    let (app, root, ctx) = setup().await;
    let (cmd, lease) = operation(&app, &ctx, "preempt").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    assert_eq!(action.decision, nalarvo_domain::AuthorityDecision::Allow);
    sqlx::query(
        "UPDATE run_execution_leases SET worker_principal_id='new',lease_version=2 WHERE id='l'",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .is_err()
    );
    assert!(
        app.finish_runtime_action(&cmd, &action.action_id, &lease, "SUCCEEDED", &json!({}))
            .await
            .is_err()
    );
    assert!(!root.path().join("note.txt").exists());
    assert_eq!(count(&app, "tool_invocations").await, 0);
    assert_eq!(count(&app, "usage_records").await, 0);
    assert_eq!(
        app.get_action("c", &action.action_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "PENDING"
    );
}

#[tokio::test]
async fn cancelled_allowed_action_cannot_start_or_spawn() {
    let (app, root, ctx) = setup().await;
    let (cmd, lease) = operation(&app, &ctx, "cancel").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    app.cancel_run(&nalarvo_domain::CompanyId("c".into()), "p", "r", 1, None)
        .await
        .unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .is_err()
    );
    for (name, args) in [
        (
            "filesystem.write",
            json!({"path":"note.txt","content":"no"}),
        ),
        ("shell.execute", json!({"executable":"git","args":["init"]})),
    ] {
        assert!(
            dispatch(
                &app,
                &ctx,
                nalarvo_runtime::worker::ToolProposal {
                    call_id: Some("cancelled".into()),
                    tool_name: name.into(),
                    target: "note.txt".into(),
                    arguments: args
                }
            )
            .await
            .is_err()
        );
    }
    assert!(!root.path().join("note.txt").exists());
    assert!(!root.path().join(".git").exists());
    assert_eq!(count(&app, "tool_invocations").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionSucceeded'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn duplicate_terminal_result_has_one_fact_and_usage() {
    let (app, _root, ctx) = setup().await;
    let (cmd, lease) = operation(&app, &ctx, "terminal").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    assert!(
        app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .unwrap()
    );
    for _ in 0..2 {
        app.finish_runtime_action(
            &cmd,
            &action.action_id,
            &lease,
            "SUCCEEDED",
            &json!({"bytes_written":7}),
        )
        .await
        .unwrap();
    }
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionSucceeded'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert!(
        !app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn distinct_calls_same_step_replay_and_material_mismatch() {
    let (app, root, ctx) = setup().await;
    operation(&app, &ctx, "prepare").await;
    let proposal = |id: &str, path: &str| nalarvo_runtime::worker::ToolProposal {
        call_id: Some(id.into()),
        tool_name: "filesystem.write".into(),
        target: path.into(),
        arguments: json!({"path":path,"content":"checked"}),
    };
    for (id, path) in [("first", "one.txt"), ("second", "two.txt")] {
        assert_eq!(
            dispatch(&app, &ctx, proposal(id, path))
                .await
                .unwrap()
                .status,
            RunStatus::Succeeded
        );
    }
    assert_eq!(count(&app, "actions").await, 2);
    assert_eq!(count(&app, "execution_steps").await, 1);
    std::fs::remove_file(root.path().join("one.txt")).unwrap();
    assert_eq!(
        dispatch(&app, &ctx, proposal("first", "one.txt"))
            .await
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    assert!(
        !root.path().join("one.txt").exists(),
        "terminal replay must not rewrite"
    );
    assert!(
        dispatch(&app, &ctx, proposal("first", "changed.txt"))
            .await
            .unwrap_err()
            .contains("Idempotency")
    );
    assert!(!root.path().join("changed.txt").exists());
    assert_eq!(count(&app, "tool_invocations").await, 2);
}

#[tokio::test]
async fn missing_consequential_call_id_cannot_collide_with_another_call() {
    let (app, root, ctx) = setup().await;
    operation(&app, &ctx, "prepare").await;
    for path in ["one.txt", "two.txt"] {
        let proposal = nalarvo_runtime::worker::ToolProposal {
            call_id: None,
            tool_name: "filesystem.write".into(),
            target: path.into(),
            arguments: json!({"path":path,"content":"checked"}),
        };
        assert!(
            dispatch(&app, &ctx, proposal)
                .await
                .unwrap_err()
                .contains("call identity")
        );
        assert!(!root.path().join(path).exists());
    }
    assert_eq!(count(&app, "actions").await, 0);
}

#[tokio::test]
async fn unknown_outcome_retains_reservation_and_never_restarts() {
    let (app, _root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount) VALUES('bl','b','TOOL_CALL',1)").execute(&app.pool).await.unwrap();
    let (cmd, lease) = operation(&app, &ctx, "unknown").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    app.start_runtime_action(&cmd, &action.action_id, &lease)
        .await
        .unwrap();
    app.finish_runtime_action(
        &cmd,
        &action.action_id,
        &lease,
        "OUTCOME_UNKNOWN",
        &json!({"failure_class":"ACK_LOST"}),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM tool_invocations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM resource_reservations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "HELD"
    );
    assert_eq!(
        sqlx::query_scalar::<_, f64>("SELECT spent_amount FROM budget_limits")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0.0
    );
    assert!(
        !app.start_runtime_action(&cmd, &action.action_id, &lease)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionOutcomeUnknown'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn native_effect_ack_lost_is_unknown_with_one_effect_and_zero_retries() {
    let (app, root, ctx) = setup().await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES('b','w','c','B','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount) VALUES('bl','b','TOOL_CALL',1)").execute(&app.pool).await.unwrap();
    operation(&app, &ctx, "prepare").await;
    let proposal = nalarvo_runtime::worker::ToolProposal {
        call_id: Some("ack-loss".into()),
        tool_name: "filesystem.write".into(),
        target: "effect.txt".into(),
        arguments: json!({"path":"effect.txt","content":"effect"}),
    };
    let effect = root.path().join("effect.txt");
    assert!(
        dispatch_with_ack(&app, &ctx, proposal.clone(), || {
            assert_eq!(std::fs::read_to_string(&effect).unwrap(), "effect");
            Err("ACK_LOST".into())
        })
        .await
        .unwrap_err()
        .contains("ACK_LOST")
    );
    let action_id: String = sqlx::query_scalar("SELECT id FROM actions")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id=?")
            .bind(&action_id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM tool_invocations WHERE action_id=?")
            .bind(&action_id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM resource_reservations WHERE action_id=?"
        )
        .bind(&action_id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        "HELD"
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_events WHERE event_type='ActionOutcomeUnknown' AND aggregate_id=?").bind(&action_id).fetch_one(&app.pool).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_records WHERE action='ActionOutcomeUnknown'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    std::fs::remove_file(&effect).unwrap();
    let replay = dispatch(&app, &ctx, proposal).await.unwrap();
    assert_eq!(replay.status, RunStatus::Failed);
    assert_eq!(replay.failure_class.as_deref(), Some("OUTCOME_UNKNOWN"));
    assert!(!effect.exists(), "replay must not invoke native effect");
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
}

#[tokio::test]
async fn audit_failure_rolls_back_terminal_state_and_replay_is_valid() {
    let (app, _root, ctx) = setup().await;
    let (cmd, lease) = operation(&app, &ctx, "rollback").await;
    let action = app.propose_action(cmd.clone()).await.unwrap();
    app.start_runtime_action(&cmd, &action.action_id, &lease)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_terminal_audit BEFORE INSERT ON audit_records WHEN NEW.action='ActionSucceeded' BEGIN SELECT RAISE(ABORT,'synthetic audit failure'); END").execute(&app.pool).await.unwrap();
    assert!(
        app.finish_runtime_action(&cmd, &action.action_id, &lease, "SUCCEEDED", &json!({}))
            .await
            .is_err()
    );
    assert_eq!(
        app.get_action("c", &action.action_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "STARTED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM tool_invocations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "STARTED"
    );
    assert_eq!(count(&app, "usage_records").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionSucceeded'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_terminal_audit")
        .execute(&app.pool)
        .await
        .unwrap();
    app.finish_runtime_action(&cmd, &action.action_id, &lease, "SUCCEEDED", &json!({}))
        .await
        .unwrap();
    assert_eq!(count(&app, "usage_records").await, 1);
}

#[cfg(windows)]
#[tokio::test]
async fn windows_real_worker_success_three_roots_and_replay() {
    use sqlx::Row;
    if std::env::var_os("NALARVO_S3_ISOLATED_CWD").is_none() {
        let daemon = tempfile::tempdir().unwrap();
        std::fs::write(daemon.path().join("sentinel"), b"daemon").unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "windows_real_worker_success_three_roots_and_replay",
                "--nocapture",
            ])
            .env("NALARVO_S3_ISOLATED_CWD", "1")
            .current_dir(daemon.path())
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
        assert_eq!(
            std::fs::read(daemon.path().join("sentinel")).unwrap(),
            b"daemon"
        );
        assert_eq!(std::fs::read_dir(daemon.path()).unwrap().count(), 1);
        return;
    }
    let (app, project_a, ctx) = setup().await;
    let project_b = tempfile::tempdir().unwrap();
    let daemon = std::env::current_dir().unwrap();
    assert_ne!(project_a.path(), project_b.path());
    assert_ne!(project_a.path(), daemon);
    assert_ne!(project_b.path(), daemon);
    std::fs::write(project_b.path().join("sentinel"), b"project-b").unwrap();
    sqlx::query("INSERT INTO projects(id,company_id,name,priority,status,working_root_path,created_at,updated_at) VALUES('pb','c','Project B','MEDIUM','ACTIVE',?,'t','t')")
        .bind(project_b.path().to_str().unwrap()).execute(&app.pool).await.unwrap();
    assert_eq!(
        real_worker(app.clone(), ctx.clone()).await.unwrap().status,
        RunStatus::Succeeded
    );
    let written = std::fs::read(project_a.path().join("note.txt")).unwrap();
    assert!(!written.is_empty());
    let row = sqlx::query("SELECT id,status,authorization,input FROM actions WHERE run_id='r'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let id: String = row.get("id");
    assert_eq!(row.get::<String, _>("status"), "SUCCEEDED");
    assert_eq!(row.get::<String, _>("authorization"), "ALLOWED");
    let args: serde_json::Value = serde_json::from_str(&row.get::<String, _>("input")).unwrap();
    assert_eq!(
        args["root_binding"],
        std::fs::canonicalize(project_a.path())
            .unwrap()
            .to_str()
            .unwrap()
    );
    assert!(args["filesystem_binding"].is_object());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tool_invocations WHERE action_id=? AND status='SUCCEEDED'"
        )
        .bind(&id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM domain_events WHERE aggregate_id=? AND event_type='ActionSucceeded'"
        )
        .bind(&id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM audit_records WHERE json_extract(details,'$.action_id')=? AND action='ActionSucceeded'").bind(&id).fetch_one(&app.pool).await.unwrap(), 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    assert_eq!(
        real_worker(app.clone(), ctx).await.unwrap().status,
        RunStatus::Succeeded
    );
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(count(&app, "usage_records").await, 1);
    assert_eq!(
        std::fs::read(project_a.path().join("note.txt")).unwrap(),
        written
    );
    for root in [project_b.path(), daemon.as_path()] {
        assert!(!root.join("note.txt").exists());
    }
    assert_eq!(
        std::fs::read(project_b.path().join("sentinel")).unwrap(),
        b"project-b"
    );
    assert_eq!(std::fs::read(daemon.join("sentinel")).unwrap(), b"daemon");
}

#[cfg(windows)]
#[tokio::test]
async fn windows_pending_authorization_rejects_changed_root_parent_target_and_content() {
    for change in ["root", "parent", "target", "content"] {
        let (app, root, ctx) = setup().await;
        std::fs::create_dir(root.path().join("parent")).unwrap();
        std::fs::write(root.path().join("parent/data"), b"old").unwrap();
        let (mut cmd, lease) = operation(&app, &ctx, change).await;
        let params = cmd.parameters.as_mut().unwrap();
        params["path"] = "parent/data".into();
        params["filesystem_binding"] =
            nalarvo_application::project_filesystem::ProjectFilesystem::open(root.path())
                .unwrap()
                .write_binding("parent/data")
                .unwrap();
        cmd.target = "parent/data".into();
        let outcome = app.propose_action(cmd.clone()).await.unwrap();
        let replacement = tempfile::tempdir().unwrap();
        match change {
            "root" => {
                sqlx::query("UPDATE projects SET working_root_path=? WHERE id='p'")
                    .bind(replacement.path().to_str().unwrap())
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "parent" => {
                std::fs::rename(root.path().join("parent"), root.path().join("old-parent"))
                    .unwrap();
                std::fs::create_dir(root.path().join("parent")).unwrap();
                std::fs::write(root.path().join("parent/data"), b"replacement").unwrap();
            }
            "target" => {
                std::fs::rename(
                    root.path().join("parent/data"),
                    root.path().join("old-data"),
                )
                .unwrap();
                std::fs::write(root.path().join("parent/data"), b"replacement").unwrap();
            }
            "content" => std::fs::write(root.path().join("parent/data"), b"changed").unwrap(),
            _ => unreachable!(),
        }
        assert!(
            app.start_runtime_action(&cmd, &outcome.action_id, &lease)
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(count(&app, "tool_invocations").await, 0);
        assert!(
            std::fs::read_dir(replacement.path())
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[tokio::test]
async fn known_tool_failure_is_terminal_and_not_reexecuted() {
    let (app, root, ctx) = setup().await;
    operation(&app, &ctx, "prepare").await;
    let proposal = nalarvo_runtime::worker::ToolProposal {
        call_id: Some("invalid-path".into()),
        tool_name: "filesystem.write".into(),
        target: "../escape.txt".into(),
        arguments: json!({"path":"../escape.txt","content":"must-not-write"}),
    };
    let error = dispatch(&app, &ctx, proposal.clone()).await.unwrap_err();
    assert!(error.contains("path escapes configured root"), "{error}");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "FAILED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM tool_invocations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        "FAILED"
    );
    assert_eq!(
        dispatch(&app, &ctx, proposal).await.unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(count(&app, "tool_invocations").await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_events WHERE event_type='ActionFailed'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert!(!root.path().join("note.txt").exists());
}

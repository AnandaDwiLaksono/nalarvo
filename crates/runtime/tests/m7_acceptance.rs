//! Focused M7 acceptance: production application, disk SQLite, real child, deterministic provider.

#[tokio::test]
async fn f3_model_advice_cannot_resolve_uncertain_or_failed_evidence() {
    for target_state in ["SUCCEEDED", "FAILED"] {
        let f = Fixture::new().await;
        let work = f.work("Target without output", 1).await;
        let run = f.app.schedule_work(f.cmd()).await.unwrap().remove(0);
        f.app
            .begin_scheduled_work(&f.c.0, &f.p, &work)
            .await
            .unwrap();
        // Seed missing-result terminal state; production evaluation and reviewer execution follow.
        sqlx::query("UPDATE runs SET lifecycle_state=? WHERE id=?")
            .bind(target_state)
            .bind(&run.id)
            .execute(&f.app.pool)
            .await
            .unwrap();
        let department: String =
            sqlx::query_scalar("SELECT primary_department_id FROM agents LIMIT 1")
                .fetch_one(&f.app.pool)
                .await
                .unwrap();
        let reviewer = f
            .app
            .create_agent(&f.c, "Reviewer", &department, &f.role, Some("mp"), 1)
            .await
            .unwrap();
        let allocation = f
            .app
            .create_agent_allocation(CreateAgentAllocationCommand::new(
                f.c.clone(),
                f.p.clone(),
                f.t.clone(),
                reviewer.id,
                None,
            ))
            .await
            .unwrap();
        f.app
            .transition_agent_allocation(
                &f.c,
                &f.p,
                &allocation.id,
                AgentAllocationStatus::Active,
                1,
            )
            .await
            .unwrap();
        sqlx::query("UPDATE agent_allocations SET status='RELEASED' WHERE agent_id=?")
            .bind(&run.executing_agent_id)
            .execute(&f.app.pool)
            .await
            .unwrap();
        f.work("Review", 1).await;
        f.responses.lock().unwrap().push_back(
            json!({"run_id":run.id,"outcome":"PASS","reason":"Model recommendation"}).to_string(),
        );
        let review = f.app.schedule_work(f.cmd()).await.unwrap().remove(0);
        let supervisor = f.supervisor();
        f.finish(&supervisor, &review).await;
        assert_ne!(run.executing_agent_id, review.executing_agent_id);
        let input = EvaluateWork {
            run_id: run.id.clone(),
            artifact_version_id: None,
            human_outcome: None,
            model_run_id: Some(review.id),
            reason: "Model advice".into(),
            accept: true,
        };
        let before = (
            f.count("work_evaluations").await,
            f.count("domain_events").await,
            f.count("audit_records").await,
            f.count("outbox_messages").await,
        );
        assert!(
            f.app.evaluate_work(f.cmd(), input.clone()).await.is_err(),
            "model PASS cannot waive missing or failed evidence"
        );
        assert_eq!(
            before,
            (
                f.count("work_evaluations").await,
                f.count("domain_events").await,
                f.count("audit_records").await,
                f.count("outbox_messages").await
            )
        );
        assert_eq!(
            f.app.get_work_item(&f.c, &work).await.unwrap().status,
            WorkItemStatus::InProgress
        );
        if target_state == "SUCCEEDED" {
            let mut advisory = input.clone();
            advisory.accept = false;
            let command = f.cmd();
            let result = f
                .app
                .evaluate_work(command.clone(), advisory.clone())
                .await
                .unwrap();
            assert_eq!(result.outcome, EvaluationOutcome::Unsure);
            assert!(!result.accepted);
            assert_eq!(
                result.id,
                f.app.evaluate_work(command, advisory).await.unwrap().id
            );
            let mut human = input;
            human.human_outcome = Some(EvaluationOutcome::Pass);
            human.model_run_id = None;
            sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE id='project.modify'")
                .execute(&f.app.pool)
                .await
                .unwrap();
            assert!(matches!(
                f.app.evaluate_work(f.cmd(), human.clone()).await,
                Err(ApplicationError::Unauthorized)
            ));
            sqlx::query("UPDATE permission_grants SET effect='ALLOW' WHERE id='project.modify'")
                .execute(&f.app.pool)
                .await
                .unwrap();
            assert!(f.app.evaluate_work(f.cmd(), human).await.unwrap().accepted);
            assert_eq!(
                f.app.get_work_item(&f.c, &work).await.unwrap().status,
                WorkItemStatus::Completed
            );
            assert!(
                sqlx::query("DELETE FROM work_evaluations WHERE id=?")
                    .bind(result.id)
                    .execute(&f.app.pool)
                    .await
                    .is_err()
            );
        }
        supervisor.shutdown().await;
    }
}
use axum::{Json, Router, routing::post};
use nalarvo_application::CredentialRef;
use nalarvo_application::{m7::StaffTeamCommand, m7_evaluation::*, m7_work::WorkRequirements, *};
use nalarvo_domain::*;
use nalarvo_runtime::{RuntimeSupervisor, SupervisorConfig, core_dispatch, worker::ToolProposal};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
const OWNER: &str = "0191e4b8-0001-7000-8000-000000000001";
struct Secrets;
impl SecretStore for Secrets {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        Ok(SecretValue::new(b"acceptance-fixture-only"))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    db: String,
    app: ApplicationContext,
    c: CompanyId,
    p: String,
    t: String,
    role: String,
    responses: Arc<Mutex<VecDeque<String>>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    provider: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.provider.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("work")).unwrap();
        let db = format!("sqlite://{}", dir.path().join("acceptance.db").display());
        let pool = nalarvo_persistence::create_pool(&db).await.unwrap();
        nalarvo_persistence::run_migrations(&pool).await.unwrap();
        let ws = WorkspaceId("ws".into());
        nalarvo_persistence::bootstrap_personal_workspace(
            &pool,
            &UserId(OWNER.into()),
            &ws,
            "acceptance@example.invalid",
            "Owner",
        )
        .await
        .unwrap();
        let app = ApplicationContext::new(pool.clone())
            .with_secret_store(Arc::new(Secrets))
            .with_content_store(Arc::new(
                nalarvo_application::content_store::LocalContentStore::open(
                    &dir.path().join("content"),
                )
                .unwrap(),
            ));
        let c = app
            .create_company(CreateCompanyCommand {
                workspace_id: ws.clone(),
                name: "Acceptance".into(),
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
        for (permission, kind, actor) in [
            ("project.modify", "USER", OWNER),
            ("project.allocate_agent", "USER", OWNER),
            ("provider.use", "SYSTEM", "acceptance-worker"),
            ("credential.use", "SYSTEM", "acceptance-worker"),
            ("filesystem.write", "SYSTEM", "acceptance-worker"),
        ] {
            sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?, 'ws',?,?,?,?, 'COMPANY',?,'ALLOW','2026-01-01T00:00:00Z')").bind(permission).bind(&c.0).bind(format!("pd_{permission}")).bind(kind).bind(actor).bind(&c.0).execute(&pool).await.unwrap();
        }
        app.activate_company(&ws, &c, 1, PrincipalRef::user(OWNER))
            .await
            .unwrap();
        let p = app
            .create_project(&c, "Acceptance".into(), None)
            .await
            .unwrap()
            .id;
        let d = app.create_department(&c, "Delivery").await.unwrap();
        let role = app.create_role(&c, "Engineer").await.unwrap().id;
        app.create_agent(&c, "Engineer", &d.id, &role, None, 1)
            .await
            .unwrap();
        let t = app
            .create_team(CreateTeamCommand::new(c.clone(), p.clone(), "Delivery"))
            .await
            .unwrap()
            .id;
        let req = app
            .create_staffing_requirement(CreateStaffingRequirementCommand::new(
                c.clone(),
                p.clone(),
                Some(t.clone()),
                role.clone(),
                1,
            ))
            .await
            .unwrap();
        app.transition_staffing_requirement(&c, &p, &req.id, StaffingRequirementStatus::Open, 1)
            .await
            .unwrap();
        app.activate_project(&c, &p, 1).await.unwrap();
        sqlx::query("UPDATE projects SET working_root_path=? WHERE id=?")
            .bind(dir.path().join("work").to_str().unwrap())
            .bind(&p)
            .execute(&pool)
            .await
            .unwrap();
        let responses = Arc::new(Mutex::new(VecDeque::<String>::new()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let q = responses.clone();
        let n = calls.clone();
        let router = Router::new().route("/chat/completions", post(move |Json(_): Json<Value>| { let q=q.clone();let n=n.clone(); async move {
            n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let text=q.lock().unwrap().pop_front().expect("deterministic provider response");
            Json(json!({"choices":[{"message":{"content":text}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}))
        }}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
        let provider = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        for q in [
            "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,created_at,updated_at) VALUES ('cred','ws','Local','secretstore://acceptance-fixture','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,endpoint,credential_ref_id,status,created_at,updated_at) VALUES ('pc','ws','Local','openai_compatible','{endpoint}','cred','ENABLED','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('model','ws','pc','local','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            "INSERT INTO model_profiles(id,workspace_id,name,created_at,updated_at) VALUES ('mp','ws','Local','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('mp','ws',1,'model','2026-01-01T00:00:00Z')",
            "UPDATE model_profiles SET current_version=1 WHERE id='mp'",
            "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('{c}','ws','mp','2026-01-01T00:00:00Z')",
            "UPDATE agents SET model_profile_id='mp' WHERE company_id='{c}'",
        ] {
            sqlx::query(&q.replace("{endpoint}", &endpoint).replace("{c}", &c.0))
                .execute(&pool)
                .await
                .unwrap();
        }
        let f = Self {
            _dir: dir,
            db,
            app,
            c,
            p,
            t,
            role,
            responses,
            calls,
            provider,
        };
        assert!(f.app.staff_team(f.cmd()).await.unwrap().team_active);
        f
    }
    fn cmd(&self) -> StaffTeamCommand {
        StaffTeamCommand {
            company_id: self.c.clone(),
            project_id: self.p.clone(),
            team_id: self.t.clone(),
            meta: CommandMeta {
                principal: Some(PrincipalRef::user(OWNER)),
                idempotency_key: Some(uuid::Uuid::now_v7().to_string()),
                ..Default::default()
            },
        }
    }
    fn supervisor(&self) -> RuntimeSupervisor {
        RuntimeSupervisor::new(
            self.app.pool.clone(),
            SupervisorConfig {
                max_concurrency: 1,
                lease_duration_secs: 60,
                heartbeat_interval_secs: 20,
                worker_principal_id: "acceptance-worker".into(),
            },
        )
        .with_handlers(
            core_dispatch::handler(self.app.clone()),
            nalarvo_runtime::core_model::handler(self.app.clone()),
        )
    }
    async fn work(&self, title: &str, priority: i64) -> String {
        let w = self
            .app
            .create_work_item(CreateWorkItemCommand::new(
                self.c.clone(),
                self.p.clone(),
                title,
            ))
            .await
            .unwrap();
        self.app
            .configure_work(
                self.cmd(),
                &w.id,
                WorkRequirements {
                    role_id: self.role.clone(),
                    priority,
                    required: true,
                    acceptance_contains: "done".into(),
                    input_version_ids: vec![],
                },
            )
            .await
            .unwrap();
        w.id
    }
    async fn finish(&self, s: &RuntimeSupervisor, run: &Run) {
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                while s.poll_and_dispatch_once(None).await.unwrap() {}
                let r = self.app.get_run(&self.c, &self.p, &run.id).await.unwrap();
                if matches!(
                    r.status,
                    RunStatus::Succeeded
                        | RunStatus::Failed
                        | RunStatus::TimedOut
                        | RunStatus::Cancelled
                ) && s.current_active_count() == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real worker completes");
    }
    async fn model_plan(&self, s: &RuntimeSupervisor, text: String) -> (Run, String) {
        let work = self
            .app
            .generate_project_plan(self.cmd(), &self.role, "Acceptance validation")
            .await
            .unwrap();
        self.responses.lock().unwrap().push_back(text);
        let runs = self.app.schedule_work(self.cmd()).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].work_item_id, work);
        self.finish(s, &runs[0]).await;
        assert_eq!(
            self.app
                .get_run(&self.c, &self.p, &runs[0].id)
                .await
                .unwrap()
                .status,
            RunStatus::Succeeded
        );
        let text: String = sqlx::query_scalar(
            "SELECT json_extract(output_payload,'$.text') FROM runtime_results WHERE run_id=?",
        )
        .bind(&runs[0].id)
        .fetch_one(&self.app.pool)
        .await
        .unwrap();
        let version = self
            .app
            .create_artifact(
                &self.c.0,
                nalarvo_application::m6::ArtifactInput {
                    title: "Model plan".into(),
                    artifact_type: "MODEL_OUTPUT".into(),
                    project_id: Some(self.p.clone()),
                    work_item_id: Some(work),
                    run_id: Some(runs[0].id.clone()),
                    agent_id: Some(runs[0].executing_agent_id.clone()),
                    sensitivity: nalarvo_domain::information::Sensitivity::Internal,
                    media_type: "application/json".into(),
                    content: text,
                },
            )
            .await
            .unwrap()
            .id;
        (runs[0].clone(), version)
    }
    fn plan(&self) -> Value {
        json!({"company_id":self.c.0,"project_id":self.p,"work":[{"key":"a","title":"Task","work_type":"TASK","role_id":self.role,"priority":0,"acceptance_contains":"done","depends_on":[]}],"staffing":[],"cancel_work_item_ids":[]})
    }
    async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&self.app.pool)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn shortage_and_invalid_allocation_cannot_schedule_work() {
    let f = Fixture::new().await;
    let qa = f.app.create_role(&f.c, "QA").await.unwrap();
    let req = f
        .app
        .create_staffing_requirement(CreateStaffingRequirementCommand::new(
            f.c.clone(),
            f.p.clone(),
            Some(f.t.clone()),
            qa.id.clone(),
            1,
        ))
        .await
        .unwrap();
    f.app
        .transition_staffing_requirement(&f.c, &f.p, &req.id, StaffingRequirementStatus::Open, 1)
        .await
        .unwrap();
    let result = f.app.staff_team(f.cmd()).await.unwrap();
    assert!(!result.team_active);
    assert_eq!(result.shortages.len(), 1);
    assert_eq!(result.shortages[0].missing, 1);
    let agent: String = sqlx::query_scalar("SELECT id FROM agents")
        .fetch_one(&f.app.pool)
        .await
        .unwrap();
    let mut allocation = CreateAgentAllocationCommand::new(
        f.c.clone(),
        f.p.clone(),
        f.t.clone(),
        agent,
        Some(req.id),
    );
    allocation.meta = f.cmd().meta;
    let before = f.count("agent_allocations").await;
    assert!(
        f.app.create_agent_allocation(allocation).await.is_err(),
        "role mismatch is not staffing"
    );
    assert_eq!(before, f.count("agent_allocations").await);
    let w = f
        .app
        .create_work_item(CreateWorkItemCommand::new(
            f.c.clone(),
            f.p.clone(),
            "QA review",
        ))
        .await
        .unwrap();
    f.app
        .configure_work(
            f.cmd(),
            &w.id,
            WorkRequirements {
                role_id: qa.id,
                priority: 100,
                required: true,
                acceptance_contains: "done".into(),
                input_version_ids: vec![],
            },
        )
        .await
        .unwrap();
    assert!(f.app.schedule_work(f.cmd()).await.unwrap().is_empty());
    assert_eq!(f.count("runs").await, 0);
}

#[tokio::test]
async fn active_worker_interruption_fences_unknown_effect_and_preserves_disk_schedule() {
    use nalarvo_persistence::m4::{self, ExecutionLease, RuntimeCheckpoint};
    let f = Fixture::new().await;
    let first = f.work("Interrupted effect", 10).await;
    let independent = f.work("Independent", 5).await;
    let dependent = f.work("Dependent", 100).await;
    sqlx::query("INSERT INTO work_dependencies VALUES (?,?,?,?,'HARD','2026-01-01T00:00:00Z')")
        .bind(&f.c.0)
        .bind(&f.p)
        .bind(&dependent)
        .bind(&first)
        .execute(&f.app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,status,total_limit,created_at,updated_at) VALUES ('b','ws',?,'Acceptance','ACTIVE',10,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").bind(&f.c.0).execute(&f.app.pool).await.unwrap();
    let runs = f.app.schedule_work(f.cmd()).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].work_item_id, first);
    let proposal = ToolProposal {
        call_id: Some("one-effect".into()),
        tool_name: "filesystem.write".into(),
        target: "effect.txt".into(),
        arguments: json!({"path":"effect.txt","content":"done"}),
    };
    f.responses
        .lock()
        .unwrap()
        .push_back(serde_json::to_string(&proposal).unwrap());
    let reached = Arc::new(tokio::sync::Notify::new());
    let reached_handler = reached.clone();
    let captured = Arc::new(Mutex::new(None));
    let captured_handler = captured.clone();
    let app = f.app.clone();
    let action: nalarvo_runtime::worker::ActionProposalHandler = Arc::new(move |ctx, proposal| {
        let app = app.clone();
        let reached = reached_handler.clone();
        let captured = captured_handler.clone();
        Box::pin(async move {
            let result =
                core_dispatch::dispatch_with_ack(&app, &ctx, proposal, || Err("ACK_LOST".into()))
                    .await;
            assert_eq!(result.unwrap_err(), "ACK_LOST");
            *captured.lock().unwrap() = Some(ctx);
            reached.notify_one();
            std::future::pending().await
        })
    });
    let s = RuntimeSupervisor::new(
        f.app.pool.clone(),
        SupervisorConfig {
            max_concurrency: 1,
            lease_duration_secs: 60,
            heartbeat_interval_secs: 20,
            worker_principal_id: "acceptance-worker".into(),
        },
    )
    .with_handlers(action, nalarvo_runtime::core_model::handler(f.app.clone()));
    assert!(s.poll_and_dispatch_once(None).await.unwrap());
    tokio::time::timeout(Duration::from_secs(25), reached.notified())
        .await
        .expect("real child reaches ambiguous native effect");
    let active = f.app.get_run(&f.c, &f.p, &runs[0].id).await.unwrap();
    assert_eq!(active.status, RunStatus::Running);
    let ctx = captured.lock().unwrap().clone().unwrap();
    let cp = RuntimeCheckpoint {
        id: "unsafe-effect".into(),
        company_id: f.c.clone(),
        run_id: active.id.clone(),
        checkpoint_version: 1,
        run_state: "RUNNING".into(),
        last_completed_step: None,
        active_step: Some(1),
        execution_phase: "TOOL_EFFECT_ACK_LOST".into(),
        context_refs: None,
        continuation_metadata: Some(json!({"call_id":"one-effect"})),
        usage_snapshot: None,
        safe_to_resume: false,
    };
    m4::append_checkpoint(&f.app.pool, &cp).await.unwrap();
    let checkpoint: Vec<(String, bool)> = sqlx::query_as(
        "SELECT continuation_metadata,safe_to_resume FROM runtime_checkpoints WHERE run_id=?",
    )
    .bind(&active.id)
    .fetch_all(&f.app.pool)
    .await
    .unwrap();
    let counts = (
        f.count("runs").await,
        f.count("actions").await,
        f.count("usage_records").await,
        f.count("resource_reservations").await,
    );
    let budget: (f64, f64) =
        sqlx::query_as("SELECT spent_amount,reserved_amount FROM budgets WHERE id='b'")
            .fetch_one(&f.app.pool)
            .await
            .unwrap();
    assert_eq!(counts.0, 1);
    assert_eq!(counts.1, 1);
    assert!(
        counts.2 > 0 && counts.3 > 0,
        "usage and budget proofs are non-vacuous"
    );
    assert!(budget.0 > 0.0 || budget.1 > 0.0);
    assert_eq!(
        std::fs::read_to_string(f._dir.path().join("work/effect.txt")).unwrap(),
        "done"
    );
    s.shutdown().await;
    drop(s);
    let pool = nalarvo_persistence::create_pool(&f.db).await.unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    let reopened = ApplicationContext::new(pool.clone());
    let recovery = RuntimeSupervisor::new(pool.clone(), SupervisorConfig::default());
    assert_eq!(recovery.reconcile_orphaned_runs().await.unwrap(), 1);
    assert_eq!(recovery.reconcile_orphaned_runs().await.unwrap(), 0);
    let terminal = reopened.get_run(&f.c, &f.p, &active.id).await.unwrap();
    assert_eq!(terminal.status, RunStatus::Failed);
    assert_eq!(terminal.failure_class.as_deref(), Some("WORKER_LOST"));
    let lease = ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: ctx.lease_version,
        expires_at: chrono::Utc::now(),
    };
    assert!(
        m4::transition_run_with_lease(
            &pool,
            &lease,
            &f.c,
            &active.id,
            terminal.row_version,
            RunStatus::Running,
            None,
            None
        )
        .await
        .is_err()
    );
    assert!(
        core_dispatch::dispatch(&reopened, &ctx, proposal)
            .await
            .is_err()
    );
    assert!(
        reopened
            .retry_run(&f.c, &f.p, &active.id, terminal.row_version, None)
            .await
            .is_err()
    );
    assert_eq!(
        counts,
        (
            f.count("runs").await,
            f.count("actions").await,
            f.count("usage_records").await,
            f.count("resource_reservations").await
        )
    );
    assert_eq!(
        budget,
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT spent_amount,reserved_amount FROM budgets WHERE id='b'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE run_id=?")
            .bind(&active.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(
        checkpoint,
        sqlx::query_as::<_, (String, bool)>(
            "SELECT continuation_metadata,safe_to_resume FROM runtime_checkpoints WHERE run_id=?"
        )
        .bind(&active.id)
        .fetch_all(&pool)
        .await
        .unwrap()
    );
    assert!(!recovery.poll_and_dispatch_once(None).await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM durable_jobs WHERE run_id=?")
            .bind(&active.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "COMPLETED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runtime_results WHERE run_id=?")
            .bind(&active.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(
        !reopened
            .work_readiness(&f.c.0, &f.p, &dependent)
            .await
            .unwrap()
            .ready
    );
    let scheduled = reopened.schedule_work(f.cmd()).await.unwrap();
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].work_item_id, independent);
    assert!(reopened.schedule_work(f.cmd()).await.unwrap().is_empty());
    assert_eq!(f.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    std::fs::remove_file(f._dir.path().join("work/effect.txt")).unwrap();
    recovery.shutdown().await;
    assert!(
        !f._dir.path().join("work/effect.txt").exists(),
        "unknown effect never replayed"
    );
    pool.close().await;
}

#[tokio::test]
async fn model_plan_negative_matrix_has_no_partial_mutation() {
    let f = Fixture::new().await;
    let s = f.supervisor();
    let valid = f.plan();
    let other = f
        .app
        .create_project(&f.c, "Other project".into(), None)
        .await
        .unwrap();
    let foreign_work = f
        .app
        .create_work_item(CreateWorkItemCommand::new(
            f.c.clone(),
            other.id.clone(),
            "Foreign",
        ))
        .await
        .unwrap();
    let mut variants = vec!["not json".into(), "[]".into(), "{}".into()];
    for field in [
        "company_id",
        "project_id",
        "work",
        "staffing",
        "cancel_work_item_ids",
    ] {
        let mut v = valid.clone();
        v.as_object_mut().unwrap().remove(field);
        variants.push(v.to_string());
    }
    for field in [
        "key",
        "title",
        "work_type",
        "role_id",
        "priority",
        "acceptance_contains",
        "depends_on",
    ] {
        let mut v = valid.clone();
        v["work"][0].as_object_mut().unwrap().remove(field);
        variants.push(v.to_string());
    }
    for (field, value) in [
        ("title", json!(" ")),
        ("work_type", json!("UNKNOWN")),
        ("priority", json!(1001)),
        ("role_id", json!("foreign-role")),
        ("depends_on", json!(["missing"])),
    ] {
        let mut v = valid.clone();
        v["work"][0][field] = value;
        variants.push(v.to_string());
    }
    for field in ["company_id", "project_id"] {
        let mut v = valid.clone();
        v[field] = json!("foreign");
        variants.push(v.to_string());
    }
    let mut duplicate = valid.clone();
    duplicate["work"]
        .as_array_mut()
        .unwrap()
        .push(valid["work"][0].clone());
    variants.push(duplicate.to_string());
    let mut cycle = valid.clone();
    let mut b = cycle["work"][0].clone();
    b["key"] = json!("b");
    b["depends_on"] = json!(["a"]);
    cycle["work"][0]["depends_on"] = json!(["b"]);
    cycle["work"].as_array_mut().unwrap().push(b);
    variants.push(cycle.to_string());
    let mut staffing = valid.clone();
    staffing["staffing"] = json!([{"role_id":f.role,"department_id":null,"desired_count":0,"required_capability_ids":[]}]);
    variants.push(staffing.to_string());
    let mut foreign = valid.clone();
    foreign["cancel_work_item_ids"] = json!([foreign_work.id]);
    variants.push(foreign.to_string());
    let mut unknown = valid.clone();
    unknown["execute"] = json!(true);
    variants.push(unknown.to_string());
    for raw in variants {
        let (run, version) = f.model_plan(&s, raw.clone()).await;
        let before = (
            f.count("work_items").await,
            f.count("work_dependencies").await,
            f.count("staffing_requirements").await,
            f.count("plan_proposals").await,
        );
        assert!(
            f.app
                .collect_plan_proposal(f.cmd(), &run.id, &version)
                .await
                .is_err(),
            "accepted invalid model plan: {raw}"
        );
        assert_eq!(
            before,
            (
                f.count("work_items").await,
                f.count("work_dependencies").await,
                f.count("staffing_requirements").await,
                f.count("plan_proposals").await
            )
        );
    }
    s.shutdown().await;
}

#[tokio::test]
async fn acceptance_revision_history_replan_authority_and_completion_guards() {
    let f = Fixture::new().await;
    let s = f.supervisor();
    let work = f.work("Deliver", 1).await;
    f.responses.lock().unwrap().push_back("done".into());
    let run = f.app.schedule_work(f.cmd()).await.unwrap().remove(0);
    let project_version = f.app.get_project(&f.c, &f.p).await.unwrap().row_version;
    assert!(
        f.app
            .complete_project_governed(f.cmd(), project_version)
            .await
            .is_err(),
        "queued critical Run blocks completion"
    );
    f.finish(&s, &run).await;
    let original = f.app.get_run(&f.c, &f.p, &run.id).await.unwrap();
    let original_json = serde_json::to_value(&original).unwrap();
    let eval = |outcome, accept| EvaluateWork {
        run_id: run.id.clone(),
        artifact_version_id: None,
        human_outcome: Some(outcome),
        model_run_id: None,
        reason: "Owner review".into(),
        accept,
    };
    for outcome in [EvaluationOutcome::Fail, EvaluationOutcome::Unsure] {
        assert!(
            f.app
                .evaluate_work(f.cmd(), eval(outcome, true))
                .await
                .is_err()
        );
        let result = f
            .app
            .evaluate_work(f.cmd(), eval(outcome, false))
            .await
            .unwrap();
        assert_eq!(result.outcome, outcome);
        assert!(!result.accepted);
        assert_eq!(
            f.app.get_work_item(&f.c, &work).await.unwrap().status,
            WorkItemStatus::InProgress
        );
    }
    assert!(
        f.app
            .complete_project_governed(
                f.cmd(),
                f.app.get_project(&f.c, &f.p).await.unwrap().row_version
            )
            .await
            .is_err()
    );
    let revision = f
        .app
        .revise_work_attempt(f.cmd(), &run.id, "Check again", original.row_version)
        .await
        .unwrap();
    assert_ne!(revision.id, run.id);
    assert_eq!(revision.retry_of_run_id.as_deref(), Some(run.id.as_str()));
    assert_eq!(revision.attempt_number, original.attempt_number + 1);
    assert_eq!(
        original_json,
        serde_json::to_value(f.app.get_run(&f.c, &f.p, &run.id).await.unwrap()).unwrap()
    );
    assert!(
        f.app
            .revise_work_attempt(f.cmd(), &run.id, "Duplicate", original.row_version)
            .await
            .is_err()
    );
    f.responses.lock().unwrap().push_back("done".into());
    f.finish(&s, &revision).await;
    f.app
        .evaluate_work(
            f.cmd(),
            EvaluateWork {
                run_id: revision.id.clone(),
                artifact_version_id: None,
                human_outcome: Some(EvaluationOutcome::Pass),
                model_run_id: None,
                reason: "Accepted".into(),
                accept: true,
            },
        )
        .await
        .unwrap();
    let history = serde_json::to_value(f.app.get_work_item(&f.c, &work).await.unwrap()).unwrap();
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES ('review-policy','ws',?,'Review','ACTIVE',1,'t','t')").bind(&f.c.0).execute(&f.app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES ('review-version','review-policy',1,'{\"permission\":\"project.modify\",\"effect\":\"REQUIRE_APPROVAL\"}','t')").execute(&f.app.pool).await.unwrap();
    let approval = f
        .app
        .propose_action(ProposeActionCommand {
            company_id: f.c.0.clone(),
            project_id: Some(f.p.clone()),
            run_id: None,
            step_id: None,
            actor: PrincipalRef::user(OWNER),
            action_type: "project.modify".into(),
            target: f.p.clone(),
            parameters: Some(json!({"review":true})),
            idempotency_key: Some("pending-review".into()),
            meta: f.cmd().meta,
        })
        .await
        .unwrap();
    assert_eq!(approval.decision, AuthorityDecision::RequireApproval);

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT project_id FROM actions WHERE id=?")
            .bind(&approval.action_id)
            .fetch_one(&f.app.pool)
            .await
            .unwrap(),
        f.p
    );
    sqlx::query("UPDATE policies SET status='DISABLED' WHERE id='review-policy'")
        .execute(&f.app.pool)
        .await
        .unwrap();
    assert!(
        f.app
            .complete_project_governed(f.cmd(), project_version)
            .await
            .is_err(),
        "pending approval blocks completion"
    );
    // Historical M6 proposals have NULL project_id; durable ApprovalRequested retains scope.
    sqlx::query("UPDATE actions SET project_id=NULL,status='SUCCEEDED' WHERE id=?")
        .bind(&approval.action_id)
        .execute(&f.app.pool)
        .await
        .unwrap();
    assert!(
        f.app
            .complete_project(&f.c, &f.p, project_version)
            .await
            .is_err(),
        "legacy completion shares approval guard"
    );
    sqlx::query("UPDATE approval_requests SET status='REJECTED' WHERE id=?")
        .bind(approval.approval_request_id)
        .execute(&f.app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE actions SET status='REJECTED' WHERE id=?")
        .bind(approval.action_id)
        .execute(&f.app.pool)
        .await
        .unwrap();
    let mut invalid = f.plan();
    invalid["cancel_work_item_ids"] = json!([work]);
    let (planner, version) = f.model_plan(&s, invalid.to_string()).await;
    assert!(
        f.app
            .collect_plan_proposal(f.cmd(), &planner.id, &version)
            .await
            .is_err()
    );
    assert_eq!(
        history,
        serde_json::to_value(f.app.get_work_item(&f.c, &work).await.unwrap()).unwrap()
    );
    let (planner, version) = f.model_plan(&s, f.plan().to_string()).await;
    let proposal = f
        .app
        .collect_plan_proposal(f.cmd(), &planner.id, &version)
        .await
        .unwrap();
    let count = f.count("work_items").await;
    assert!(
        f.app
            .decide_project_plan(f.cmd(), &proposal.id, true, false, "Unconfirmed")
            .await
            .is_err()
    );
    let mut unauthorized = f.cmd();
    unauthorized.meta.principal = Some(PrincipalRef::user("outsider"));
    assert!(
        f.app
            .decide_project_plan(unauthorized, &proposal.id, true, true, "Forged owner")
            .await
            .is_err()
    );
    sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE id='project.modify'")
        .execute(&f.app.pool)
        .await
        .unwrap();
    assert!(
        f.app
            .decide_project_plan(f.cmd(), &proposal.id, true, true, "Current deny")
            .await
            .is_err()
    );
    assert_eq!(count, f.count("work_items").await);
    assert_eq!(f.count("plan_decisions").await, 0);
    sqlx::query("UPDATE permission_grants SET effect='ALLOW' WHERE id='project.modify'")
        .execute(&f.app.pool)
        .await
        .unwrap();
    f.app
        .decide_project_plan(
            f.cmd(),
            &proposal.id,
            false,
            false,
            "Reject unused proposal",
        )
        .await
        .unwrap();
    // Optional planning work still carries live assignments; completion must retain their history.
    let project = f.app.get_project(&f.c, &f.p).await.unwrap();
    let agents: Vec<(String, String)> =
        sqlx::query_as("SELECT id,primary_department_id FROM agents ORDER BY id")
            .fetch_all(&f.app.pool)
            .await
            .unwrap();
    let assignments = f.count("assignments").await;
    let result = f
        .app
        .complete_project_governed(f.cmd(), project.row_version)
        .await
        .unwrap();
    assert_eq!(result.status, ProjectStatus::Completed);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM teams WHERE id=?")
            .bind(&f.t)
            .fetch_one(&f.app.pool)
            .await
            .unwrap(),
        "DISBANDED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_allocations WHERE status!='RELEASED'"
        )
        .fetch_one(&f.app.pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(assignments, f.count("assignments").await);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assignments WHERE status!='RELEASED'")
            .fetch_one(&f.app.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        agents,
        sqlx::query_as::<_, (String, String)>(
            "SELECT id,primary_department_id FROM agents ORDER BY id"
        )
        .fetch_all(&f.app.pool)
        .await
        .unwrap()
    );
    assert_eq!(
        original_json,
        serde_json::to_value(f.app.get_run(&f.c, &f.p, &run.id).await.unwrap()).unwrap()
    );
    s.shutdown().await;
}

// Archived H2 proof fixture: real child + local provider + immutable Artifact.
// Unlike the original bypass probe, these assertions require rejection.
mod h2 {
    use super::*;
    use sqlx::Row;

    async fn snapshot(f: &Fixture) -> Vec<(String, Vec<String>)> {
        let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .fetch_all(&f.app.pool).await.unwrap();
        let mut result = Vec::new();
        for table in tables {
            let columns = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
                .fetch_all(&f.app.pool)
                .await
                .unwrap();
            let expression = columns
                .iter()
                .map(|c| format!("quote(\"{}\")", c.get::<String, _>("name")))
                .collect::<Vec<_>>()
                .join("||'|'||");
            let rows =
                sqlx::query_scalar(&format!("SELECT {expression} FROM \"{table}\" ORDER BY 1"))
                    .fetch_all(&f.app.pool)
                    .await
                    .unwrap();
            result.push((table, rows));
        }
        result
    }

    async fn denied(f: &Fixture, cmd: StaffTeamCommand, run: &str, version: &str) {
        let before = snapshot(f).await;
        let result = f.app.collect_plan_proposal(cmd, run, version).await;
        assert!(
            result.is_err(),
            "collection unexpectedly accepted: {result:?}"
        );
        assert_eq!(
            snapshot(f).await,
            before,
            "rejection must preserve business/provenance rows"
        );
    }

    #[tokio::test]
    async fn h2_current_deny_rejects_collection_without_business_mutation() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE id='project.modify'")
            .execute(&f.app.pool)
            .await
            .unwrap();
        assert!(matches!(
            f.app
                .generate_project_plan(f.cmd(), &f.role, "Current deny control")
                .await,
            Err(ApplicationError::Unauthorized)
        ));
        let before = snapshot(&f).await;
        let counts = (
            f.count("plan_proposals").await,
            f.count("domain_events").await,
            f.count("audit_records").await,
            f.count("outbox_messages").await,
        );
        let result = f
            .app
            .collect_plan_proposal(f.cmd(), &run.id, &version)
            .await;
        let after = (
            f.count("plan_proposals").await,
            f.count("domain_events").await,
            f.count("audit_records").await,
            f.count("outbox_messages").await,
        );
        println!(
            "H2 current DENY: result={result:?}; proposal/event/audit/outbox delta={}/{}/{}/{}",
            after.0 - counts.0,
            after.1 - counts.1,
            after.2 - counts.2,
            after.3 - counts.3
        );
        assert!(
            matches!(result, Err(ApplicationError::Unauthorized)),
            "H2 security assertion: current DENY must reject collection"
        );
        assert_eq!(snapshot(&f).await, before);
        s.shutdown().await;
    }

    #[tokio::test]
    async fn h2_revoked_missing_ambiguous_authority_and_http_fail_closed() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        for sql in [
            "UPDATE permission_grants SET revoked_at='2026-01-01T00:00:00Z' WHERE id='project.modify'",
            "DELETE FROM permission_grants WHERE id='project.modify'",
            "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) SELECT 'project.modify','ws',id,'pd_project.modify','USER','0191e4b8-0001-7000-8000-000000000001','COMPANY',id,'UNKNOWN','t' FROM companies",
        ] {
            sqlx::query(sql).execute(&f.app.pool).await.unwrap();
            denied(&f, f.cmd(), &run.id, &version).await;
        }
        sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE id='project.modify'")
            .execute(&f.app.pool)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = nalarvo_local_api::router_with_app("h2-local-only", Some(f.app.clone()));
        let api = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let before = snapshot(&f).await;
        let response = reqwest::Client::new().post(format!("{base}/api/v1/companies/{}/projects/{}/orchestration",f.c.0,f.p))
            .bearer_auth("h2-local-only").json(&json!({"team_id":f.t,"idempotency_key":"h2-http",
                "command":{"type":"CollectPlanProposal","source_run_id":run.id,"source_version_id":version}}))
            .send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(snapshot(&f).await, before);
        api.abort();
        s.shutdown().await;
    }

    #[tokio::test]
    async fn h2_scope_origin_and_non_delegated_actor_substitution_rejected() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        let other = f
            .app
            .create_project(&f.c, "Other".into(), None)
            .await
            .unwrap();
        let mut cmd = f.cmd();
        cmd.project_id = other.id;
        denied(&f, cmd, &run.id, &version).await;
        let mut cmd = f.cmd();
        cmd.company_id = CompanyId("foreign".into());
        denied(&f, cmd, &run.id, &version).await;
        // Both foreign scope objects exist and this user has current authority there.
        let foreign = f
            .app
            .create_company(CreateCompanyCommand {
                workspace_id: WorkspaceId("ws".into()),
                name: "Foreign".into(),
                description: None,
                principal: Some(PrincipalRef::user(OWNER)),
                idempotency_key: None,
                correlation_id: None,
                causation_id: None,
            })
            .await
            .unwrap()
            .id;
        f.app
            .activate_company(
                &WorkspaceId("ws".into()),
                &foreign,
                1,
                PrincipalRef::user(OWNER),
            )
            .await
            .unwrap();
        let project = f
            .app
            .create_project(&foreign, "Foreign".into(), None)
            .await
            .unwrap();
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('foreign-grant','ws',?,'pd_project.modify','USER',?,'COMPANY',?,'ALLOW','t')")
            .bind(&foreign.0).bind(OWNER).bind(&foreign.0).execute(&f.app.pool).await.unwrap();
        let mut cmd = f.cmd();
        cmd.company_id = foreign;
        cmd.project_id = project.id;
        denied(&f, cmd, &run.id, &version).await;
        for principal in [
            None,
            Some(PrincipalRef::system("acceptance-worker")),
            Some(PrincipalRef::user("outsider")),
        ] {
            let mut cmd = f.cmd();
            cmd.meta.principal = principal;
            denied(&f, cmd, &run.id, &version).await;
        }
        // A currently authorized member is not the originating user; no delegation contract exists.
        sqlx::query("INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('delegate','delegate@example.invalid','Delegate','t','t')")
            .execute(&f.app.pool).await.unwrap();
        sqlx::query("INSERT INTO workspace_memberships(workspace_id,user_id,role,created_at) VALUES ('ws','delegate','OWNER','t')")
            .execute(&f.app.pool).await.unwrap();
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) SELECT 'delegate','ws',company_id,permission_definition_id,'USER','delegate','COMPANY',scope_id,'ALLOW','t' FROM permission_grants WHERE id='project.modify'")
            .execute(&f.app.pool).await.unwrap();
        let mut cmd = f.cmd();
        cmd.meta.principal = Some(PrincipalRef::user("delegate"));
        cmd.meta.causation_id = Some("forged-delegation".into());
        denied(&f, cmd, &run.id, &version).await;
        sqlx::query("UPDATE domain_events SET event_type='HistoricalFixture' WHERE event_type='ProjectPlanningRequested'")
            .execute(&f.app.pool).await.unwrap();
        denied(&f, f.cmd(), &run.id, &version).await;
        sqlx::query("UPDATE domain_events SET event_type='ProjectPlanningRequested' WHERE event_type='HistoricalFixture'")
            .execute(&f.app.pool).await.unwrap();
        sqlx::query("INSERT INTO domain_events SELECT 'ambiguous-origin',event_type,schema_version,company_id,aggregate_type,aggregate_id,aggregate_version,occurred_at,correlation_id,causation_id,principal_type,principal_id,scope_type,scope_id,payload,created_at FROM domain_events WHERE event_type='ProjectPlanningRequested'")
            .execute(&f.app.pool).await.unwrap();
        denied(&f, f.cmd(), &run.id, &version).await;
        s.shutdown().await;
    }

    #[tokio::test]
    async fn h2_concurrent_replay_authorized_workflow_and_history_preserved() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        let before = (
            f.count("plan_proposals").await,
            f.count("domain_events").await,
            f.count("outbox_messages").await,
        );
        let (a, b) = tokio::join!(
            f.app.collect_plan_proposal(f.cmd(), &run.id, &version),
            f.app.collect_plan_proposal(f.cmd(), &run.id, &version)
        );
        let proposal = a.unwrap();
        assert_eq!(proposal.id, b.unwrap().id);
        assert_eq!(
            (
                f.count("plan_proposals").await,
                f.count("domain_events").await,
                f.count("outbox_messages").await
            ),
            (before.0 + 1, before.1 + 1, before.2 + 1)
        );
        let history = snapshot(&f).await;
        assert_eq!(
            f.app
                .collect_plan_proposal(f.cmd(), &run.id, &version)
                .await
                .unwrap()
                .id,
            proposal.id
        );
        assert_eq!(snapshot(&f).await, history);
        let (other, other_version) = f.model_plan(&s, f.plan().to_string()).await;
        assert_eq!(
            f.app
                .decide_project_plan(f.cmd(), &proposal.id, true, true, "Authorized H2 control")
                .await
                .unwrap()
                .len(),
            1
        );
        sqlx::query("UPDATE permission_grants SET effect='DENY' WHERE id='project.modify'")
            .execute(&f.app.pool)
            .await
            .unwrap();
        denied(&f, f.cmd(), &run.id, &version).await;
        denied(&f, f.cmd(), &other.id, &other_version).await;
        assert!(matches!(
            f.app
                .decide_project_plan(f.cmd(), &proposal.id, false, false, "Denied replan")
                .await,
            Err(ApplicationError::Unauthorized)
        ));
        s.shutdown().await;
    }

    #[tokio::test]
    async fn h2_revocation_serializes_at_writer_boundary() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        let mut tx = f.app.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("UPDATE permission_grants SET revoked_at='2026-01-01T00:00:00Z' WHERE id='project.modify'")
            .execute(&mut *tx).await.unwrap();
        let task = {
            let app = f.app.clone();
            let cmd = f.cmd();
            let run = run.id.clone();
            let version = version.clone();
            tokio::spawn(async move { app.collect_plan_proposal(cmd, &run, &version).await })
        };
        tokio::task::yield_now().await;
        tx.commit().await.unwrap();
        let before = snapshot(&f).await;
        assert!(matches!(
            task.await.unwrap(),
            Err(ApplicationError::Unauthorized)
        ));
        assert_eq!(snapshot(&f).await, before);
        s.shutdown().await;
    }

    #[tokio::test]
    async fn h2_approval_material_lifecycle_and_atomic_consumption() {
        let f = Fixture::new().await;
        let s = f.supervisor();
        let (run, version) = f.model_plan(&s, f.plan().to_string()).await;
        sqlx::query("UPDATE projects SET status='CANCELLED' WHERE id=?")
            .bind(&f.p)
            .execute(&f.app.pool)
            .await
            .unwrap();
        denied(&f, f.cmd(), &run.id, &version).await;
        sqlx::query("UPDATE projects SET status='ACTIVE' WHERE id=?")
            .bind(&f.p)
            .execute(&f.app.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES ('h2-policy','ws',?,'Collection approval','ACTIVE',1,'t','t')")
            .bind(&f.c.0).execute(&f.app.pool).await.unwrap();
        sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES ('h2-policy-version','h2-policy',1,'{\"permission\":\"project.modify\",\"effect\":\"REQUIRE_APPROVAL\"}','t')")
            .execute(&f.app.pool).await.unwrap();
        denied(&f, f.cmd(), &run.id, &version).await;
        let action = f
            .app
            .propose_action(ProposeActionCommand {
                company_id: f.c.0.clone(),
                project_id: Some(f.p.clone()),
                run_id: None,
                step_id: None,
                actor: PrincipalRef::user(OWNER),
                action_type: "project.modify".into(),
                target: f.p.clone(),
                parameters: Some(json!({"collect_plan":run.id,"source_version_id":version})),
                idempotency_key: Some("h2-approval".into()),
                meta: f.cmd().meta,
            })
            .await
            .unwrap();
        assert_eq!(action.decision, AuthorityDecision::RequireApproval);
        let mut cmd = f.cmd();
        cmd.meta.causation_id = Some(action.action_id.clone());
        denied(&f, cmd.clone(), &run.id, &version).await;
        sqlx::query("UPDATE actions SET authorization='APPROVED' WHERE id=?")
            .bind(&action.action_id)
            .execute(&f.app.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE approval_requests SET status='REJECTED' WHERE action_id=?")
            .bind(&action.action_id)
            .execute(&f.app.pool)
            .await
            .unwrap();
        denied(&f, cmd.clone(), &run.id, &version).await;
        sqlx::query("UPDATE approval_requests SET status='APPROVED' WHERE action_id=?")
            .bind(&action.action_id)
            .execute(&f.app.pool)
            .await
            .unwrap();
        denied(&f, cmd.clone(), &run.id, "substituted-version").await;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id=?")
                .bind(&action.action_id)
                .fetch_one(&f.app.pool)
                .await
                .unwrap(),
            "PENDING"
        );
        let before = (
            f.count("domain_events").await,
            f.count("outbox_messages").await,
        );
        f.app
            .collect_plan_proposal(cmd, &run.id, &version)
            .await
            .unwrap();
        assert_eq!(f.count("plan_proposals").await, 1);
        assert_eq!(
            (
                f.count("domain_events").await,
                f.count("outbox_messages").await
            ),
            (before.0 + 2, before.1 + 2)
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM actions WHERE id=?")
                .bind(&action.action_id)
                .fetch_one(&f.app.pool)
                .await
                .unwrap(),
            "SUCCEEDED"
        );
        s.shutdown().await;
    }
}

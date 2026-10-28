//! Disk DB + actual worker child + Core model adapter + local provider + typed HTTP.
use axum::{Json, Router, routing::post};
use nalarvo_application::CredentialRef;
use nalarvo_application::*;
use nalarvo_contracts::m7::{
    EvaluationInput, OrchestrationCommand as C, OrchestrationRequest, OrchestrationResponse as R,
};
use nalarvo_domain::information::Sensitivity;
use nalarvo_domain::*;
use nalarvo_runtime::{RuntimeSupervisor, SupervisorConfig};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const OWNER: &str = "0191e4b8-0001-7000-8000-000000000001";
struct Secrets;
impl SecretStore for Secrets {
    fn get(&self, _: &CredentialRef) -> Result<SecretValue, SecretStoreError> {
        Ok(SecretValue::new(b"local-fixture-only"))
    }
    fn put(&self, _: &CredentialRef, _: &SecretValue) -> Result<(), SecretStoreError> {
        Ok(())
    }
    fn delete(&self, _: &CredentialRef) -> Result<(), SecretStoreError> {
        Ok(())
    }
}
async fn rpc(base: &str, c: &CompanyId, p: &str, t: &str, cmd: C) -> R {
    nalarvo_client::m7::orchestrate(
        base,
        "local-token",
        &c.0,
        p,
        &OrchestrationRequest {
            team_id: t.into(),
            idempotency_key: uuid::Uuid::now_v7().to_string(),
            command: cmd,
        },
    )
    .await
    .unwrap()
}
fn supervisor(app: &ApplicationContext) -> RuntimeSupervisor {
    RuntimeSupervisor::new(
        app.pool.clone(),
        SupervisorConfig {
            max_concurrency: 3,
            ..Default::default()
        },
    )
    .with_model_handler(nalarvo_runtime::core_model::handler(app.clone()))
}
async fn finish(s: &RuntimeSupervisor, app: &ApplicationContext, ids: &[String]) {
    for _ in 0..1200 {
        while s.poll_and_dispatch_once(None).await.unwrap() {}
        let mut terminal = true;
        for id in ids {
            let status: String = sqlx::query_scalar("SELECT lifecycle_state FROM runs WHERE id=?")
                .bind(id)
                .fetch_one(&app.pool)
                .await
                .unwrap();
            if !matches!(
                status.as_str(),
                "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "CANCELLED"
            ) {
                terminal = false;
            }
        }
        if terminal && s.current_active_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let runs: Vec<(String, String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT id,lifecycle_state,failure_class,failure_detail FROM runs")
            .fetch_all(&app.pool)
            .await
            .unwrap();
    let jobs: Vec<(String, String)> = sqlx::query_as("SELECT id,status FROM durable_jobs")
        .fetch_all(&app.pool)
        .await
        .unwrap();
    panic!("worker deadline: {runs:?} {jobs:?}");
}
fn evaluation(id: &str, accept: bool) -> C {
    C::EvaluateWork {
        evaluation: EvaluationInput {
            run_id: id.into(),
            artifact_version_id: None,
            human_outcome: None,
            model_run_id: None,
            reason: "Executed output validation".into(),
            accept,
        },
    }
}
async fn artifact(app: &ApplicationContext, c: &CompanyId, p: &str, run: &Run) -> String {
    let text: String = sqlx::query_scalar(
        "SELECT json_extract(output_payload,'$.text') FROM runtime_results WHERE run_id=?",
    )
    .bind(&run.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    app.create_artifact(
        &c.0,
        nalarvo_application::m6::ArtifactInput {
            title: "Immutable worker output".into(),
            artifact_type: "MODEL_OUTPUT".into(),
            project_id: Some(p.into()),
            work_item_id: Some(run.work_item_id.clone()),
            run_id: Some(run.id.clone()),
            agent_id: Some(run.executing_agent_id.clone()),
            sensitivity: Sensitivity::Internal,
            media_type: "application/json".into(),
            content: text,
        },
    )
    .await
    .unwrap()
    .id
}
#[tokio::test]
async fn production_staff_plan_parallel_restart_revision_evaluate_replan_complete() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite:{}",
        dir.path()
            .join("m7.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let pool = nalarvo_persistence::create_pool(&db).await.unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    let workspace = WorkspaceId("ws".into());
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &UserId(OWNER.into()),
        &workspace,
        "m7@example.invalid",
        "M7 Owner",
    )
    .await
    .unwrap();
    let content = Arc::new(
        nalarvo_application::content_store::LocalContentStore::open(&dir.path().join("content"))
            .unwrap(),
    );
    let app = ApplicationContext::new(pool.clone())
        .with_secret_store(Arc::new(Secrets))
        .with_content_store(content.clone());
    let c = app
        .create_company(CreateCompanyCommand {
            workspace_id: workspace.clone(),
            name: "Production M7".into(),
            description: None,
            principal: Some(PrincipalRef::user(OWNER)),
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap()
        .id;
    app.ensure_canonical_permissions(&workspace.0)
        .await
        .unwrap();
    for permission in ["project.allocate_agent", "project.modify"] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?,'ws',?,?,'USER',?,'COMPANY',?,'ALLOW','2026-01-01')").bind(format!("grant-{permission}")).bind(&c.0).bind(format!("pd_{permission}")).bind(OWNER).bind(&c.0).execute(&pool).await.unwrap();
    }
    app.activate_company(&workspace, &c, 1, PrincipalRef::user(OWNER))
        .await
        .unwrap();
    let project = app
        .create_project(&c, "M7 production".into(), None)
        .await
        .unwrap();
    let p = project.id;
    let department = app.create_department(&c, "Delivery").await.unwrap();
    let mut roles = Vec::new();
    for name in ["PM", "Engineer", "QA"] {
        let role = app.create_role(&c, name).await.unwrap();
        app.create_agent(&c, name, &department.id, &role.id, None, 1)
            .await
            .unwrap();
        roles.push(role.id);
    }
    let team = app
        .create_team(CreateTeamCommand::new(c.clone(), p.clone(), "Delivery"))
        .await
        .unwrap();
    let t = team.id;
    for role in &roles {
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
    }
    let active = app.activate_project(&c, &p, 1).await.unwrap();
    assert_eq!(active.status, ProjectStatus::Active);
    let responses = Arc::new(Mutex::new(
        std::collections::VecDeque::<(u16, String)>::new(),
    ));
    let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let q = responses.clone();
    let n = in_flight.clone();
    let max = peak.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let provider=Router::new().route("/chat/completions",post(move |Json(_req):Json<serde_json::Value>|{let q=q.clone();let n=n.clone();let max=max.clone();let barrier=barrier.clone();let calls=calls.clone();async move {
  use std::sync::atomic::Ordering::SeqCst;let count=n.fetch_add(1,SeqCst)+1;max.fetch_max(count,SeqCst);
  let (status,text)=q.lock().unwrap().pop_front().expect("local provider queue");
  if matches!(calls.fetch_add(1,SeqCst),1|2) {tokio::time::timeout(Duration::from_secs(15),barrier.wait()).await.expect("independent worker overlap");}
  tokio::time::sleep(Duration::from_millis(300)).await;n.fetch_sub(1,SeqCst);
  (axum::http::StatusCode::from_u16(status).unwrap(),Json(serde_json::json!({"choices":[{"message":{"content":text}}],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}})))
 }}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let provider_task = tokio::spawn(async move { axum::serve(listener, provider).await.unwrap() });
    for sql in [
        "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,created_at,updated_at) VALUES ('cred','ws','Local','secretstore://local-fixture','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO provider_connections(id,workspace_id,name,provider_kind,endpoint,credential_ref_id,status,created_at,updated_at) VALUES ('pc','ws','Local','openai_compatible','{endpoint}','cred','ENABLED','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('model','ws','pc','local-model','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO model_profiles(id,workspace_id,name,created_at,updated_at) VALUES ('mp','ws','Local','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('mp','ws',1,'model','2026-01-01T00:00:00Z')",
        "UPDATE model_profiles SET current_version=1 WHERE id='mp'",
        "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('{c}','ws','mp','2026-01-01T00:00:00Z')",
        "UPDATE agents SET model_profile_id='mp' WHERE company_id='{c}'",
    ] {
        sqlx::query(
            &sql.replace("{c}", &c.0)
                .replace("{owner}", OWNER)
                .replace("{endpoint}", &endpoint),
        )
        .execute(&pool)
        .await
        .unwrap();
    }
    for permission in ["provider.use", "credential.use"] {
        sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES (?,'ws',?,?,'SYSTEM','system:runtime-supervisor-1','COMPANY',?,'ALLOW','2026-01-01T00:00:00Z')").bind(format!("runtime-{permission}")).bind(&c.0).bind(format!("pd_{permission}")).bind(&c.0).execute(&pool).await.unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = nalarvo_local_api::router_with_app("local-token", Some(app.clone()));
    let api = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    assert!(matches!(
        rpc(&base, &c, &p, &t, C::StaffTeam).await,
        R::Staffing {
            team_active: true,
            ..
        }
    ));
    let planning = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::GenerateProjectPlan {
            planner_role: roles[0].clone(),
            reason: "Initial plan".into(),
        },
    )
    .await
    {
        R::Proposal(id) => id,
        _ => panic!(),
    };
    let plan=serde_json::json!({"company_id":c.0,"project_id":p,"work":[
 {"key":"engineer","title":"Build","work_type":"TASK","role_id":roles[1],"priority":3,"acceptance_contains":"done","depends_on":[]},
 {"key":"qa","title":"Independent QA","work_type":"TASK","role_id":roles[2],"priority":2,"acceptance_contains":"done","depends_on":[]},
 {"key":"dependent","title":"Integrate","work_type":"TASK","role_id":roles[0],"priority":1,"acceptance_contains":"done","depends_on":["engineer","qa"]}],"staffing":[],"cancel_work_item_ids":[]}).to_string();
    responses.lock().unwrap().push_back((200, plan));
    let runs = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].work_item_id, planning);
    let s = supervisor(&app);
    finish(&s, &app, &[runs[0].id.clone()]).await;
    assert_eq!(
        app.get_run(&c, &p, &runs[0].id).await.unwrap().status,
        RunStatus::Succeeded
    );
    let v = artifact(&app, &c, &p, &runs[0]).await;
    let proposal = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::CollectPlanProposal {
            source_run_id: runs[0].id.clone(),
            source_version_id: v,
        },
    )
    .await
    {
        R::Proposal(v) => v,
        _ => panic!(),
    };
    rpc(&base, &c, &p, &t, evaluation(&runs[0].id, true)).await;
    let work = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::DecidePlan {
            proposal_id: proposal,
            apply: true,
            confirmed: true,
            reason: "Owner confirms bounded plan".into(),
        },
    )
    .await
    {
        R::PlanApplied(v) => v,
        _ => panic!(),
    };
    assert_eq!(work.len(), 3);
    let first = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(first.len(), 2);
    let duplicate = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert!(duplicate.is_empty());
    let dependent = work
        .iter()
        .find(|id| !first.iter().any(|r| &r.work_item_id == *id))
        .unwrap()
        .clone();
    assert!(
        !app.work_readiness(&c.0, &p, &dependent)
            .await
            .unwrap()
            .ready
    );
    responses
        .lock()
        .unwrap()
        .extend([(200, "not yet".into()), (200, "done".into())]);
    assert!(s.poll_and_dispatch_once(None).await.unwrap());
    assert!(s.poll_and_dispatch_once(None).await.unwrap());

    finish(
        &s,
        &app,
        &first.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
    )
    .await;
    assert!(peak.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    for r in &first {
        assert_eq!(
            app.get_run(&c, &p, &r.id).await.unwrap().status,
            RunStatus::Succeeded
        );
    }

    let mut revision_target = None;
    for r in &first {
        let text: String = sqlx::query_scalar(
            "SELECT json_extract(output_payload,'$.text') FROM runtime_results WHERE run_id=?",
        )
        .bind(&r.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if text == "done" {
            rpc(&base, &c, &p, &t, evaluation(&r.id, true)).await;
        } else {
            rpc(&base, &c, &p, &t, evaluation(&r.id, false)).await;
            revision_target = Some(r.clone());
        }
    }
    assert!(
        !app.work_readiness(&c.0, &p, &dependent)
            .await
            .unwrap()
            .ready
    );
    let old = revision_target.unwrap();
    let old_current = app.get_run(&c, &p, &old.id).await.unwrap();
    assert!(
        app.retry_run(&c, &p, &old.id, old_current.row_version, None)
            .await
            .is_err()
    );
    let revision = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::ReviseWork {
            previous_run_id: old.id.clone(),
            instructions: "Return done".into(),
            expected_version: old_current.row_version,
        },
    )
    .await
    {
        R::Revision(v) => v,
        _ => panic!(),
    };
    // New supervisor consumes disk-persisted work without rescheduling or a test worker.
    s.shutdown().await;
    drop(s);
    let reopened = nalarvo_persistence::create_pool(&db).await.unwrap();
    nalarvo_persistence::run_migrations(&reopened)
        .await
        .unwrap();
    let restarted = ApplicationContext::new(reopened.clone())
        .with_secret_store(Arc::new(Secrets))
        .with_content_store(content);
    let s = supervisor(&restarted);
    responses.lock().unwrap().push_back((200, "done".into()));
    finish(&s, &restarted, std::slice::from_ref(&revision.id)).await;
    assert_eq!(
        app.get_run(&c, &p, &old.id).await.unwrap().status,
        RunStatus::Succeeded
    );
    rpc(&base, &c, &p, &t, evaluation(&revision.id, true)).await;
    assert!(
        app.work_readiness(&c.0, &p, &dependent)
            .await
            .unwrap()
            .ready
    );
    let next = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].work_item_id, dependent);
    responses.lock().unwrap().push_back((200, "done".into()));
    finish(&s, &app, &[next[0].id.clone()]).await;
    rpc(&base, &c, &p, &t, evaluation(&next[0].id, true)).await;
    // Replan adds future work; every completed row remains byte-for-byte intact.
    let before:Vec<(String,String,i64)>=sqlx::query_as("SELECT id,status,row_version FROM work_items WHERE project_id=? AND status='COMPLETED' ORDER BY id").bind(&p).fetch_all(&pool).await.unwrap();
    let replan = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::GenerateProjectPlan {
            planner_role: roles[0].clone(),
            reason: "Replan: add final review without rewriting delivery".into(),
        },
    )
    .await
    {
        R::Proposal(id) => id,
        _ => panic!(),
    };
    let revised_plan = serde_json::json!({"company_id":c.0,"project_id":p,"work":[{"key":"final","title":"Final review","work_type":"TASK","role_id":roles[2],"priority":0,"acceptance_contains":"done","depends_on":[]}],"staffing":[],"cancel_work_item_ids":[]});
    responses
        .lock()
        .unwrap()
        .push_back((200, revised_plan.to_string()));
    let planner = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(planner.len(), 1);
    assert_eq!(planner[0].work_item_id, replan);
    finish(&s, &app, &[planner[0].id.clone()]).await;
    let version = artifact(&app, &c, &p, &planner[0]).await;
    let proposal = match rpc(
        &base,
        &c,
        &p,
        &t,
        C::CollectPlanProposal {
            source_run_id: planner[0].id.clone(),
            source_version_id: version,
        },
    )
    .await
    {
        R::Proposal(id) => id,
        _ => panic!(),
    };
    rpc(&base, &c, &p, &t, evaluation(&planner[0].id, true)).await;
    assert!(
        nalarvo_client::m7::orchestrate(
            &base,
            "local-token",
            &c.0,
            &p,
            &OrchestrationRequest {
                team_id: t.clone(),
                idempotency_key: uuid::Uuid::now_v7().to_string(),
                command: C::DecidePlan {
                    proposal_id: proposal.clone(),
                    apply: true,
                    confirmed: false,
                    reason: "Unconfirmed".into()
                }
            }
        )
        .await
        .is_err()
    );
    rpc(
        &base,
        &c,
        &p,
        &t,
        C::DecidePlan {
            proposal_id: proposal,
            apply: true,
            confirmed: true,
            reason: "Owner confirms replan".into(),
        },
    )
    .await;
    let after:Vec<(String,String,i64)>=sqlx::query_as("SELECT id,status,row_version FROM work_items WHERE project_id=? AND status='COMPLETED' AND id!=? ORDER BY id").bind(&p).bind(&replan).fetch_all(&pool).await.unwrap();
    assert_eq!(before, after);
    let final_run = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(final_run.len(), 1);
    responses.lock().unwrap().push_back((200, "done".into()));
    finish(&s, &app, &[final_run[0].id.clone()]).await;
    // Independent PM model evaluates QA output; model is advisory, human owns acceptance.
    let review = app
        .create_work_item(CreateWorkItemCommand::new(
            c.clone(),
            p.clone(),
            "Independent model review",
        ))
        .await
        .unwrap();
    rpc(
        &base,
        &c,
        &p,
        &t,
        C::ConfigureWork {
            work_item_id: review.id.clone(),
            requirements: nalarvo_contracts::m7::WorkRequirements {
                role_id: roles[0].clone(),
                priority: 0,
                required: false,
                acceptance_contains: "outcome".into(),
                input_version_ids: vec![],
            },
        },
    )
    .await;
    let reviewer = match rpc(&base, &c, &p, &t, C::ScheduleWork).await {
        R::Runs(v) => v,
        _ => panic!(),
    };
    assert_eq!(reviewer.len(), 1);
    responses.lock().unwrap().push_back((200,serde_json::json!({"run_id":final_run[0].id,"outcome":"PASS","reason":"Independent local model checks done output"}).to_string()));
    finish(&s, &app, &[reviewer[0].id.clone()]).await;
    assert!(matches!(
        rpc(
            &base,
            &c,
            &p,
            &t,
            C::EvaluateWork {
                evaluation: EvaluationInput {
                    run_id: final_run[0].id.clone(),
                    artifact_version_id: None,
                    human_outcome: None,
                    model_run_id: Some(reviewer[0].id.clone()),
                    reason: "Model-assisted evidence".into(),
                    accept: false
                }
            }
        )
        .await,
        R::Evaluation {
            accepted: false,
            ..
        }
    ));
    rpc(
        &base,
        &c,
        &p,
        &t,
        C::EvaluateWork {
            evaluation: EvaluationInput {
                run_id: final_run[0].id.clone(),
                artifact_version_id: None,
                human_outcome: Some(nalarvo_contracts::m7::EvaluationOutcome::Pass),
                model_run_id: None,
                reason: "Owner accepts model-assisted output".into(),
                accept: true,
            },
        },
    )
    .await;
    rpc(&base, &c, &p, &t, evaluation(&reviewer[0].id, true)).await;
    let current = app.get_project(&c, &p).await.unwrap();
    assert!(matches!(
        rpc(
            &base,
            &c,
            &p,
            &t,
            C::CompleteProject {
                expected_version: current.row_version
            }
        )
        .await,
        R::Completed(_)
    ));
    assert_eq!(
        app.get_project(&c, &p).await.unwrap().status,
        ProjectStatus::Completed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM teams WHERE project_id=? AND status='DISBANDED'"
        )
        .bind(&p)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_allocations WHERE project_id=? AND status='RELEASED'"
        )
        .bind(&p)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agents WHERE company_id=? AND primary_department_id=?"
        )
        .bind(&c.0)
        .bind(&department.id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assignments WHERE project_id=? AND status='RELEASED'"
        )
        .bind(&p)
        .fetch_one(&pool)
        .await
        .unwrap(),
        7
    );
    let snapshot: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM context_snapshots")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(snapshot, 8);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM durable_jobs WHERE status!='COMPLETED'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "terminal jobs must never be redispatched"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_execution_leases WHERE released_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    s.shutdown().await;
    api.abort();
    provider_task.abort();
    drop(app);
    drop(restarted);
    pool.close().await;
    reopened.close().await;
}

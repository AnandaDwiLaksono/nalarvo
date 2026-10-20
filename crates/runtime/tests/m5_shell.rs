#![cfg(windows)]

#[path = "support/shell_support.rs"]
mod support;
use nalarvo_domain::RunStatus;
use nalarvo_runtime::{
    core_dispatch::{dispatch_with_shell, handler_with_shell},
    worker::{RunExecutionRequest, execute_run_worker_process},
};
use serde_json::json;
use support::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

#[tokio::test]
async fn governed_shell_success_exact_argv_cwd_environment_budget_and_replay() {
    let (app, root, ctx) = setup().await;
    grant(&app).await;
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,reserved_amount,spent_amount,status,created_at,updated_at) VALUES('b','w','c','B',1,0,0,'ACTIVE','t','t')").execute(&app.pool).await.unwrap();
    let args = vec![
        "success".into(),
        "space argument".into(),
        "quote\"slash\\".into(),
        "".into(),
    ];
    let policy = policy(&args, 5000);
    let proposed = proposal(&args);
    let model: nalarvo_runtime::worker::ModelRequestHandler =
        std::sync::Arc::new(move |_, _, _| {
            let proposed = proposed.clone();
            Box::pin(async move {
                Ok(nalarvo_model_gateway::ModelResponse {
                    text: None,
                    structured: Some(serde_json::to_value(proposed).unwrap()),
                    usage: None,
                    request_id: Some("shell-model-proposal".into()),
                })
            })
        });
    let req = RunExecutionRequest {
        run_id: ctx.run_id.clone(),
        company_id: ctx.company_id.clone(),
        project_id: ctx.project_id.clone(),
        work_item_id: ctx.work_item_id.clone(),
        agent_id: ctx.agent_id.clone(),
        model_profile_version_id: None,
        provider_connection_id: "pc".into(),
        base_url: None,
        auth_token: None,
        model_key: "mock".into(),
        context_input: nalarvo_runtime::context::ContextInput::minimal("c", "p", "shell"),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_id: ctx.lease_id.clone(),
        lease_version: 1,
        pool: Some(app.pool.clone()),
        expected_run_version: 1,
        mock_mode: nalarvo_model_gateway::MockMode::TOOL_PROPOSAL,
        cancel_rx: None,
        model_handler: Some(model),
        resume_proposal: None,
        action_handler: Some(handler_with_shell(app.clone(), policy.clone())),
    };
    let result = execute_run_worker_process(
        std::path::Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")),
        req,
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.status, RunStatus::Succeeded, "{result:?}");
    let output: serde_json::Value =
        serde_json::from_str(result.output_text.as_ref().unwrap()).unwrap();
    let stdout = output["stdout"].as_str().unwrap();
    let cwd = stdout.lines().find_map(|l| l.strip_prefix("cwd=")).unwrap();
    assert_eq!(
        std::fs::canonicalize(cwd).unwrap(),
        std::fs::canonicalize(root.path()).unwrap()
    );
    for key in [
        "CANARY_PARENT_SECRET",
        "NALARVO_DAEMON_TOKEN",
        "PATH",
        "USERPROFILE",
    ] {
        assert!(stdout.contains(&format!("{key}=ABSENT")), "{stdout}");
    }
    assert!(
        stdout.contains(&format!("args={:?}", &args[1..])),
        "{stdout}"
    );
    assert!(root.path().join("executed.txt").exists());
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM tool_invocations t JOIN actions a ON a.id=t.action_id JOIN execution_steps s ON s.id=a.step_id JOIN usage_records u ON json_extract(u.metadata,'$.action_id')=a.id WHERE a.status='SUCCEEDED' AND a.authorization='ALLOWED' AND a.project_id='p' AND t.status='SUCCEEDED'").fetch_one(&app.pool).await.unwrap(),1);
    assert_eq!(
        sqlx::query_scalar::<_, f64>("SELECT spent_amount FROM budgets WHERE id='b'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1.0
    );
    assert_eq!(
        dispatch_with_shell(&app, &ctx, proposal(&args), &policy)
            .await
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    let mut second = proposal(&args);
    second.call_id = Some("second-command".into());
    assert!(
        dispatch_with_shell(&app, &ctx, second, &policy)
            .await
            .unwrap_err()
            .contains("BUDGET_EXHAUSTED")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_invocations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn production_dispatch_rejects_unlisted_executable_or_arguments() {
    let (app, root, ctx) = setup().await;
    grant(&app).await;
    let args = vec!["entry".into()];
    let allowed = policy(&args, 5000);
    assert!(
        dispatch_with_shell(&app, &ctx, proposal(&args), &Default::default())
            .await
            .is_err()
    );
    assert!(
        dispatch_with_shell(&app, &ctx, proposal(&["success".into()]), &allowed)
            .await
            .is_err()
    );
    assert!(!root.path().join("entry-effect.txt").exists());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn nonzero_shell_exit_is_terminal_failure_not_success() {
    let (app, _, ctx) = setup().await;
    grant(&app).await;
    let args = vec!["failure".into()];
    let policy = policy(&args, 5000);
    let outcome = dispatch_with_shell(&app, &ctx, proposal(&args), &policy)
        .await
        .unwrap();
    assert_eq!(outcome.status, RunStatus::Failed);
    let result: serde_json::Value =
        serde_json::from_str(outcome.output_text.as_ref().unwrap()).unwrap();
    assert_eq!(result["status"], 7);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions a JOIN tool_invocations t ON t.action_id=a.id JOIN usage_records u ON json_extract(u.metadata,'$.action_id')=a.id WHERE a.status='FAILED' AND t.status='FAILED'").fetch_one(&app.pool).await.unwrap(), 1);
    assert_eq!(
        dispatch_with_shell(&app, &ctx, proposal(&args), &policy)
            .await
            .unwrap()
            .status,
        RunStatus::Failed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_invocations")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn shell_approval_resume_uses_same_fingerprint_and_governed_action() {
    use sqlx::Row;
    let (app, root, ctx) = setup().await;
    grant(&app).await;
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol','w','c','P','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv','pol',1,'{\"permission\":\"shell.execute\",\"effect\":\"REQUIRE_APPROVAL\"}','t')").execute(&app.pool).await.unwrap();
    let args = vec!["success".into()];
    let policy = policy(&args, 5000);
    assert_eq!(
        dispatch_with_shell(&app, &ctx, proposal(&args), &policy)
            .await
            .unwrap()
            .status,
        RunStatus::WaitingApproval
    );
    assert!(!root.path().join("executed.txt").exists());
    let row=sqlx::query("SELECT a.*,p.id AS approval_id FROM actions a JOIN approval_requests p ON p.action_id=a.id").fetch_one(&app.pool).await.unwrap();
    let action_id: String = row.get("id");
    let approval_id: String = row.get("approval_id");
    let fingerprint: String = row.get("fingerprint");
    let step: String = row.get("step_id");
    app.wait_run_for_approval(&nalarvo_domain::CompanyId("c".into()), "p", "r", 1)
        .await
        .unwrap();
    sqlx::query("INSERT INTO durable_jobs(id,job_type,company_id,run_id,payload,available_at,status,correlation_id,created_at,updated_at) VALUES('job','EXECUTE_RUN','c','r','{}','t','WAITING_APPROVAL','corr','t','t')").execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_checkpoints(id,company_id,run_id,checkpoint_version,run_state,execution_phase,continuation_metadata,created_at) VALUES('cp','c','r',1,'WAITING_APPROVAL','APPROVAL_PENDING',?,'t')")
        .bind(json!({"action_id":action_id,"approval_id":approval_id,"fingerprint":fingerprint,"step_id":step,"idempotency_key":row.get::<String,_>("idempotency_key")}).to_string()).execute(&app.pool).await.unwrap();
    assert!(
        app.decide_action_approval(
            "c",
            &approval_id,
            nalarvo_domain::PrincipalRef::user("u"),
            "wrong",
            true,
            None
        )
        .await
        .is_err()
    );
    app.decide_action_approval(
        "c",
        &approval_id,
        nalarvo_domain::PrincipalRef::user("u"),
        &fingerprint,
        true,
        None,
    )
    .await
    .unwrap();
    let cmd = nalarvo_application::ProposeActionCommand {
        company_id: "c".into(),
        project_id: Some("p".into()),
        run_id: Some("r".into()),
        step_id: Some(step),
        actor: nalarvo_domain::PrincipalRef::system("worker"),
        action_type: "shell.execute".into(),
        target: executable().to_str().unwrap().into(),
        parameters: Some(serde_json::from_str(&row.get::<String, _>("input")).unwrap()),
        idempotency_key: row.get("idempotency_key"),
        meta: Default::default(),
    };
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: "l".into(),
        run_id: "r".into(),
        worker_principal_id: "worker".into(),
        lease_version: 1,
        expires_at: chrono::Utc::now(),
    };
    app.resume_approved_runtime_action(&cmd, &action_id, &approval_id, &lease, 2)
        .await
        .unwrap();
    assert_eq!(
        dispatch_with_shell(&app, &ctx, proposal(&args), &policy)
            .await
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    assert!(root.path().join("executed.txt").exists());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fingerprint FROM actions WHERE id=?")
            .bind(action_id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        fingerprint
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM actions WHERE authorization='APPROVED' AND status='SUCCEEDED'"
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn temporal_grant_dispatch_never_executes_tool_or_side_effect() {
    for expiry in ["not-a-time", "2000-01-01T14:00:00+14:00"] {
        let (app, root, ctx) = setup().await;
        grant(&app).await;
        sqlx::query("UPDATE permission_grants SET expires_at=?")
            .bind(expiry)
            .execute(&app.pool)
            .await
            .unwrap();
        let args = vec!["entry".into()];
        let result = dispatch_with_shell(&app, &ctx, proposal(&args), &policy(&args, 5000)).await;
        if expiry == "not-a-time" {
            assert!(
                result
                    .as_ref()
                    .is_err_and(|e| e.contains("Invalid permission grant validity")),
                "{result:?}"
            );
        } else {
            assert_ne!(result.unwrap().status, nalarvo_domain::RunStatus::Succeeded);
        }
        assert!(!root.path().join("entry-effect.txt").exists());
        for table in ["tool_invocations", "resource_reservations", "usage_records"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&app.pool)
                    .await
                    .unwrap(),
                0,
                "{table}"
            );
        }
    }
}

#[tokio::test]
async fn invalid_policy_dispatch_never_executes_tool_or_side_effect() {
    let (app, root, ctx) = setup().await;
    grant(&app).await;
    for id in ["allow", "invalid"] {
        sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES(?,'w','c',?,'ACTIVE',1,'t','t')")
            .bind(id).bind(id).execute(&app.pool).await.unwrap();
        sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES(?,?,1,?,'t')")
            .bind(id).bind(id).bind(json!({"permission":"shell.execute","effect":"ALLOW"}).to_string()).execute(&app.pool).await.unwrap();
    }
    let args = vec!["entry".into()];
    let shell = policy(&args, 5000);
    for rules in [
        "{",
        r#"{"permission":"shell.execute"}"#,
        r#"{"permission":"shell.execute","effect":"REQUIRE_APROVAL"}"#,
    ] {
        sqlx::query("UPDATE policy_versions SET rules=? WHERE id='invalid'")
            .bind(rules)
            .execute(&app.pool)
            .await
            .unwrap();
        let error = dispatch_with_shell(&app, &ctx, proposal(&args), &shell)
            .await
            .unwrap_err();
        assert!(
            error.contains("Invalid policy configuration: invalid"),
            "{error}"
        );
        assert!(!root.path().join("entry-effect.txt").exists());
        for table in [
            "tool_invocations",
            "resource_reservations",
            "usage_records",
            "actions",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&app.pool)
                    .await
                    .unwrap(),
                0,
                "{table}"
            );
        }
    }
}

#[tokio::test]
async fn production_governance_deny_approval_scope_cancel_and_stale_lease_never_spawn() {
    for mode in ["deny", "approval", "cancel", "stale", "project", "budget"] {
        let (app, root, mut ctx) = setup().await;
        grant(&app).await;
        match mode {
            "deny" => {
                sqlx::query("UPDATE permission_grants SET effect='DENY'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "approval" => {
                sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('pol','w','c','P','ACTIVE',1,'t','t')").execute(&app.pool).await.unwrap();
                sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('pv','pol',1,'{\"permission\":\"shell.execute\",\"effect\":\"REQUIRE_APPROVAL\"}','t')").execute(&app.pool).await.unwrap();
            }
            "cancel" => {
                sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            "stale" => ctx.lease_version = 2,
            "project" => ctx.project_id = "foreign".into(),
            "budget" => {
                sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,reserved_amount,spent_amount,status,created_at,updated_at) VALUES('b','w','c','B',0,0,0,'ACTIVE','t','t')").execute(&app.pool).await.unwrap();
            }
            _ => unreachable!(),
        }
        let args = vec!["entry".into()];
        let result = dispatch_with_shell(&app, &ctx, proposal(&args), &policy(&args, 5000)).await;
        if mode == "approval" {
            assert_eq!(result.unwrap().status, RunStatus::WaitingApproval);
        } else {
            assert!(
                result.is_err() || result.unwrap().status == RunStatus::Failed,
                "{mode}"
            );
        }
        assert!(!root.path().join("entry-effect.txt").exists(), "{mode}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_invocations")
                .fetch_one(&app.pool)
                .await
                .unwrap(),
            0,
            "{mode}"
        );
    }
}

async fn terminate_tree(mode: &str) {
    let (app, root, mut ctx) = setup().await;
    grant(&app).await;
    let live = std::sync::Arc::new(tokio::sync::RwLock::new(1));
    if mode == "heartbeat" {
        ctx.live_lease_version = Some(live.clone());
    }
    // Provider lineage is normally established by the real worker wrapper.
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: 1,
        expires_at: chrono::Utc::now(),
    };
    let step = nalarvo_persistence::m4::resolve_tool_step(
        &app.pool,
        &lease,
        &nalarvo_domain::CompanyId("c".into()),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE execution_steps SET input_metadata=? WHERE id=?")
        .bind(json!({"provider_connection_id":"pc"}).to_string())
        .bind(step)
        .execute(&app.pool)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let args = vec!["tree".into(), listener.local_addr().unwrap().to_string()];
    let policy = policy(&args, if mode == "timeout" { 3000 } else { 30000 });
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(mode == "worker-precancel");
    let worker_cancel = mode == "worker-cancel" || mode == "worker-precancel";
    let req = RunExecutionRequest {
        run_id: ctx.run_id.clone(),
        company_id: ctx.company_id.clone(),
        project_id: ctx.project_id.clone(),
        work_item_id: ctx.work_item_id.clone(),
        agent_id: ctx.agent_id.clone(),
        model_profile_version_id: None,
        provider_connection_id: "pc".into(),
        base_url: None,
        auth_token: None,
        model_key: "mock".into(),
        context_input: nalarvo_runtime::context::ContextInput::minimal("c", "p", "shell"),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_id: ctx.lease_id.clone(),
        lease_version: 1,
        pool: Some(app.pool.clone()),
        expected_run_version: 1,
        mock_mode: nalarvo_model_gateway::MockMode::TOOL_PROPOSAL,
        cancel_rx: Some(cancel_rx),
        model_handler: None,
        resume_proposal: Some(proposal(&args)),
        action_handler: Some(handler_with_shell(app.clone(), policy.clone())),
    };
    let app_copy = app.clone();
    let task = tokio::spawn(async move {
        if worker_cancel {
            execute_run_worker_process(
                std::path::Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")),
                req,
                None,
            )
            .await
        } else {
            dispatch_with_shell(&app_copy, &ctx, proposal(&args), &policy).await
        }
    });
    if mode == "worker-precancel" {
        assert!(task.await.unwrap().is_err());
        assert!(!root.path().join("entry-effect.txt").exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions")
                .fetch_one(&app.pool)
                .await
                .unwrap(),
            0
        );
        return;
    }
    let mut streams = Vec::new();
    let mut handles = Vec::new();
    let mut roles = Vec::new();
    for _ in 0..2 {
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let mut stream = BufReader::new(stream);
        let mut ready = String::new();
        stream.read_line(&mut ready).await.unwrap();
        let pid: u32 = ready.split_whitespace().nth(1).unwrap().parse().unwrap();
        roles.push(ready.starts_with("parent "));
        use std::os::windows::io::FromRawHandle;
        let handle = unsafe {
            windows_sys::Win32::System::Threading::OpenProcess(
                windows_sys::Win32::System::Threading::PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        assert!(!handle.is_null());
        handles.push(unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) });
        streams.push(stream);
    }
    // Readiness comes from BOTH running processes, not a sleep or scheduling assumption.
    if mode == "heartbeat" {
        let mut version = live.write().await;
        sqlx::query("UPDATE run_execution_leases SET lease_version=2")
            .execute(&app.pool)
            .await
            .unwrap();
        *version = 2;
    }
    if mode == "parent-exit" || mode == "heartbeat" {
        let parent = roles.iter().position(|parent| *parent).unwrap();
        streams[parent].get_mut().write_all(b"q").await.unwrap();
    }
    if mode == "cancel" {
        sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED'")
            .execute(&app.pool)
            .await
            .unwrap();
    }
    if mode == "fence" {
        sqlx::query("UPDATE run_execution_leases SET lease_version=2")
            .execute(&app.pool)
            .await
            .unwrap();
    }
    if mode == "drop" {
        task.abort();
    }
    if worker_cancel {
        cancel_tx.send(true).unwrap();
    }
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .unwrap();
    if mode == "drop" {
        assert!(result.unwrap_err().is_cancelled());
    } else if mode == "parent-exit" || mode == "heartbeat" {
        assert_eq!(result.unwrap().unwrap().status, RunStatus::Succeeded);
    } else if let Ok(outcome) = result.unwrap() {
        assert_eq!(
            outcome.status,
            if mode == "parent-exit" || mode == "heartbeat" {
                RunStatus::Succeeded
            } else {
                RunStatus::Failed
            }
        );
    }
    use std::os::windows::io::AsRawHandle;
    for handle in handles {
        assert_eq!(
            unsafe {
                windows_sys::Win32::System::Threading::WaitForSingleObject(
                    handle.as_raw_handle(),
                    0,
                )
            },
            windows_sys::Win32::Foundation::WAIT_OBJECT_0,
            "{mode}: process still alive after dispatch returned"
        );
    }
    for mut stream in streams {
        let _ = stream.get_mut().write_all(b"release delayed effects").await;
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap_or(0), 0);
    }
    assert!(!root.path().join("parent-effect.txt").exists());
    assert!(!root.path().join("descendant-effect.txt").exists());
    assert!(!root.path().join("entry-effect.txt").exists());
}

#[tokio::test]
async fn allowlisted_image_replacement_requires_operator_reconfiguration() {
    let (app, root, ctx) = setup().await;
    grant(&app).await;
    let binary = tempfile::tempdir().unwrap();
    let exe = binary.path().join("trusted.exe");
    std::fs::copy(executable(), &exe).unwrap();
    let configured =
        serde_json::json!([{"executable":exe,"args":["entry"],"timeout_ms":5000}]).to_string();
    let allowed = nalarvo_runtime::shell::ShellPolicy::from_json(&configured).unwrap();
    std::fs::write(&exe, b"changed image").unwrap();
    let mut command = proposal(&["entry".into()]);
    command.target = exe.to_str().unwrap().into();
    command.arguments["executable"] = json!(exe);
    assert!(
        dispatch_with_shell(&app, &ctx, command, &allowed)
            .await
            .unwrap_err()
            .contains("executable changed")
    );
    assert!(!root.path().join("entry-effect.txt").exists());
}

#[tokio::test]
async fn production_timeout_reaps_parent_descendant_before_return() {
    terminate_tree("timeout").await;
}
#[tokio::test]
async fn production_cancel_reaps_parent_descendant_before_return() {
    terminate_tree("cancel").await;
}
#[tokio::test]
async fn production_lease_fencing_reaps_parent_descendant_before_return() {
    terminate_tree("fence").await;
}
#[tokio::test]
async fn dropped_dispatch_reaps_parent_descendant() {
    terminate_tree("drop").await;
}

#[tokio::test]
async fn real_worker_cancel_signal_reaps_parent_descendant() {
    terminate_tree("worker-cancel").await;
}

#[tokio::test]
async fn real_worker_precancel_never_spawns_shell() {
    terminate_tree("worker-precancel").await;
}

#[tokio::test]
async fn parent_success_reaps_surviving_descendant() {
    terminate_tree("parent-exit").await;
}

#[tokio::test]
async fn trusted_heartbeat_during_shell_keeps_current_lease_and_completion() {
    terminate_tree("heartbeat").await;
}

#[tokio::test]
async fn production_shell_config_executes_only_exact_absolute_command() {
    let (app, _, ctx) = setup().await;
    grant(&app).await;
    let args = vec!["flood".into()];
    let result = dispatch_with_shell(&app, &ctx, proposal(&args), &policy(&args, 5000))
        .await
        .unwrap();
    assert_eq!(result.status, RunStatus::Succeeded);
    let output: serde_json::Value =
        serde_json::from_str(result.output_text.as_ref().unwrap()).unwrap();
    assert_eq!(output["stdout"].as_str().unwrap().len(), 65536);
    assert_eq!(output["stderr"].as_str().unwrap().len(), 65536);
    assert_eq!(output["truncated"], true);
    assert!(
        nalarvo_runtime::shell::ShellPolicy::from_json(
            r#"[{"executable":"whoami.exe","args":[],"timeout_ms":1}]"#
        )
        .is_err()
    );
}

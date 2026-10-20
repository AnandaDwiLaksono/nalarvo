use crate::native_tools::{NativeTools, validate_tool_credential_requirement};
use crate::worker::{ActionProposalHandler, ToolProposal, WorkerExecutionOutcome};
use nalarvo_application::{ApplicationContext, CommandMeta, ProposeActionCommand};
use nalarvo_domain::{CompanyId, ExecutionStepStatus, PrincipalRef, ProjectStatus, RunStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedRunContext {
    pub run_id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub worker_principal_id: String,
    #[serde(skip)]
    pub live_lease_version: Option<Arc<tokio::sync::RwLock<i64>>>,
}

pub fn handler(app: ApplicationContext) -> ActionProposalHandler {
    handler_with_shell(app, crate::shell::ShellPolicy::default())
}

pub fn handler_with_shell(
    app: ApplicationContext,
    shell: crate::shell::ShellPolicy,
) -> ActionProposalHandler {
    Arc::new(move |ctx, proposal| {
        let app = app.clone();
        let shell = shell.clone();
        Box::pin(async move { dispatch_with_shell(&app, &ctx, proposal, &shell).await })
    })
}

pub fn validate_arguments(tool_name: &str, arguments: &Value) -> Result<(), String> {
    // No provider tool executor exists; reject before Action/credential/usage side effects.
    if tool_name == "provider.call" {
        return Err("PROVIDER_TOOL_EXECUTION_UNAVAILABLE".into());
    }
    validate_tool_credential_requirement(tool_name, arguments, None)
        .map_err(|e| format!("tool credential contract rejected proposal: {e}"))?;
    reject_untrusted_keys(arguments)?;
    match tool_name {
        "filesystem.list" => {
            check_keys(arguments, &["path"])?;
            string_arg(arguments, "path")?;
        }
        "filesystem.read" => {
            check_keys(arguments, &["path"])?;
            string_arg(arguments, "path")?;
        }
        "filesystem.write" => {
            check_keys(arguments, &["path", "content"])?;
            string_arg(arguments, "path")?;
            string_arg(arguments, "content")?;
        }
        "shell.execute" => {
            check_keys(arguments, &["executable", "args"])?;
            string_arg(arguments, "executable")?;
            let args_arr = arguments
                .get("args")
                .and_then(Value::as_array)
                .ok_or_else(|| "args must be an array".to_string())?;
            for item in args_arr {
                if !item.is_string() {
                    return Err("args array items must be strings".to_string());
                }
            }
        }
        _ => return Err(format!("unrecognized native tool: {tool_name}")),
    }
    Ok(())
}

fn check_keys(arguments: &Value, allowed: &[&str]) -> Result<(), String> {
    let obj = arguments
        .as_object()
        .ok_or_else(|| "arguments must be an object".to_string())?;
    for k in obj.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(format!("unexpected argument key: {k}"));
        }
    }
    Ok(())
}

fn reject_untrusted_keys(val: &Value) -> Result<(), String> {
    match val {
        Value::Object(map) => {
            for (k, v) in map {
                let lower = k.to_ascii_lowercase();
                if lower.contains("credential")
                    || lower.contains("secret")
                    || lower.contains("token")
                    || lower.contains("password")
                    || lower.contains("api_key")
                    || lower.contains("auth")
                {
                    return Err(format!("untrusted security argument key rejected: {k}"));
                }
                reject_untrusted_keys(v)?;
            }
        }
        Value::Array(arr) => {
            for item in arr {
                reject_untrusted_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub async fn dispatch(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
    proposal: ToolProposal,
) -> Result<WorkerExecutionOutcome, String> {
    dispatch_with_ack(app, ctx, proposal, || Ok(())).await
}

/// The callback runs after the native effect, before Core trusts its acknowledgement.
pub async fn dispatch_with_ack(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
    proposal: ToolProposal,
    ack: impl FnOnce() -> Result<(), String>,
) -> Result<WorkerExecutionOutcome, String> {
    dispatch_inner(
        app,
        ctx,
        proposal,
        &crate::shell::ShellPolicy::default(),
        ack,
    )
    .await
}

pub async fn dispatch_with_shell(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
    proposal: ToolProposal,
    shell: &crate::shell::ShellPolicy,
) -> Result<WorkerExecutionOutcome, String> {
    dispatch_inner(app, ctx, proposal, shell, || Ok(())).await
}

async fn dispatch_inner(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
    proposal: ToolProposal,
    shell: &crate::shell::ShellPolicy,
    ack: impl FnOnce() -> Result<(), String>,
) -> Result<WorkerExecutionOutcome, String> {
    let args = proposal.arguments;
    validate_arguments(&proposal.tool_name, &args)?;

    // 1. Verify lease & fencing token
    let lease_valid = nalarvo_persistence::m4::verify_current_lease(
        &app.pool,
        &ctx.run_id,
        &ctx.worker_principal_id,
        &ctx.lease_id,
        ctx.lease_version,
    )
    .await
    .map_err(|e| format!("lease verification query: {e}"))?;

    if !lease_valid {
        return Err(format!(
            "fencing violation: lease {} v{} for run {} is no longer valid for worker {}",
            ctx.lease_id, ctx.lease_version, ctx.run_id, ctx.worker_principal_id
        ));
    }

    // 2. Canonical Run verification
    let company_id_obj = CompanyId(ctx.company_id.clone());
    let run = nalarvo_persistence::m4::get_run(&app.pool, &company_id_obj, &ctx.run_id)
        .await
        .map_err(|e| format!("query run: {e}"))?
        .ok_or_else(|| {
            format!(
                "canonical run {} not found for company {}",
                ctx.run_id, ctx.company_id
            )
        })?;

    if run.project_id != ctx.project_id {
        return Err(format!(
            "project scope mismatch: run {} is bound to project {}, context claims {}",
            ctx.run_id, run.project_id, ctx.project_id
        ));
    }
    if run.work_item_id != ctx.work_item_id {
        return Err(format!(
            "work item scope mismatch: run {} is bound to work item {}, context claims {}",
            ctx.run_id, run.work_item_id, ctx.work_item_id
        ));
    }

    let work_item =
        nalarvo_persistence::get_work_item(&app.pool, &company_id_obj, &ctx.work_item_id)
            .await
            .map_err(|e| format!("query work item: {e}"))?
            .ok_or_else(|| {
                format!(
                    "canonical work item {} not found for company {}",
                    ctx.work_item_id, ctx.company_id
                )
            })?;

    if work_item.project_id != ctx.project_id {
        return Err(format!(
            "work item {} belongs to project {}, but context project is {}",
            ctx.work_item_id, work_item.project_id, ctx.project_id
        ));
    }

    // 3. Canonical Project verification
    let project = nalarvo_persistence::get_project(&app.pool, &company_id_obj, &ctx.project_id)
        .await
        .map_err(|e| format!("query project: {e}"))?
        .ok_or_else(|| {
            format!(
                "canonical project {} not found for company {}",
                ctx.project_id, ctx.company_id
            )
        })?;

    if project.status != ProjectStatus::Active {
        return Err(format!(
            "project {} lifecycle status '{:?}' disallows execution",
            ctx.project_id, project.status
        ));
    }

    let bound_root = project
        .working_root_path
        .as_ref()
        .ok_or_else(|| format!("project {} has no bound working_root_path", ctx.project_id))?;

    // Pin the original configured namespace before canonicalization can hide a reparse root.
    let tools = NativeTools::new(bound_root, std::iter::empty::<String>())
        .map_err(|e| format!("native tools init: {e}"))?;
    let canonical_root = tools.root().to_path_buf();
    let mut prepared_shell = if proposal.tool_name == "shell.execute" {
        let executable = string_arg(&args, "executable")?;
        if proposal.target != executable {
            return Err("Shell target must match executable".into());
        }
        let argv = args["args"]
            .as_array()
            .ok_or("args must be array")?
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or("args must be strings"))
            .collect::<Result<Vec<_>, _>>()?;
        Some(shell.prepare(&canonical_root, executable, &argv)?)
    } else {
        None
    };

    if !canonical_root.is_dir() {
        return Err(format!(
            "project working root '{}' is not a directory",
            canonical_root.display()
        ));
    }

    // 4. Action Governance
    let governance_action = proposal.tool_name.as_str();

    let mut shell_lease_guard = if proposal.tool_name == "shell.execute" {
        match &ctx.live_lease_version {
            Some(v) => Some(v.read().await),
            None => None,
        }
    } else {
        None
    };
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: shell_lease_guard
            .as_deref()
            .copied()
            .unwrap_or(ctx.lease_version),
        expires_at: chrono::Utc::now(),
    };
    let step_id = nalarvo_persistence::m4::resolve_tool_step(&app.pool, &lease, &company_id_obj)
        .await
        .map_err(|e| format!("resolve tool step: {e}"))?;
    let call_id = proposal
        .call_id
        .as_deref()
        .ok_or_else(|| "Missing logical Tool call identity".to_string())?;
    if call_id.is_empty()
        || call_id.len() > 128
        || !call_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err("Invalid logical Tool call identity".into());
    }
    let mut normalized_args = args.clone();
    normalized_args["tool_id"] = serde_json::Value::String(proposal.tool_name.clone());
    normalized_args["tool_version"] = serde_json::Value::String("1.0.0".into());
    normalized_args["worker_principal_id"] =
        serde_json::Value::String(ctx.worker_principal_id.clone());
    normalized_args["root_binding"] =
        serde_json::Value::String(canonical_root.to_string_lossy().to_string());
    #[cfg(windows)]
    if let Some(prepared) = &prepared_shell {
        normalized_args["shell_binding"] = Value::String(prepared.digest.clone());
    }
    let idempotency_key = format!(
        "tool:{}:{}:{}:{}",
        ctx.run_id, step_id, call_id, proposal.tool_name
    );
    #[cfg(windows)]
    let mut pinned_write = None;
    #[cfg(windows)]
    {
        // Terminal replay reuses material only to retrieve the recorded outcome, never to execute.
        let terminal: Option<String> = sqlx::query_scalar("SELECT input FROM actions WHERE company_id=? AND idempotency_key=? AND status IN ('SUCCEEDED','FAILED','OUTCOME_UNKNOWN','REJECTED')")
            .bind(&ctx.company_id).bind(&idempotency_key).fetch_optional(&app.pool).await.map_err(|e| e.to_string())?;
        if let Some(input) = terminal {
            let stored: Value = serde_json::from_str(&input).map_err(|e| e.to_string())?;
            normalized_args["filesystem_binding"] = stored["filesystem_binding"].clone();
        } else {
            normalized_args["filesystem_binding"] = tools.filesystem_binding()?;
            if proposal.tool_name == "filesystem.write"
                && nalarvo_application::project_filesystem::components(string_arg(&args, "path")?)
                    .is_ok()
            {
                let pinned = tools.prepare_write(string_arg(&args, "path")?)?;
                normalized_args["filesystem_binding"] = pinned.binding.clone();
                pinned_write = Some(pinned);
            }
        }
    }
    let provider_connection_id: Option<String> = sqlx::query_scalar("SELECT json_extract(input_metadata,'$.provider_connection_id') FROM execution_steps WHERE id=?")
        .bind(&step_id).fetch_optional(&app.pool).await.unwrap_or_default();
    if let Some(pc) = provider_connection_id {
        normalized_args["provider_connection_id"] = serde_json::Value::String(pc);
    }
    let action = ProposeActionCommand {
        company_id: ctx.company_id.clone(),
        project_id: Some(ctx.project_id.clone()),
        run_id: Some(ctx.run_id.clone()),
        step_id: Some(step_id.clone()),
        actor: PrincipalRef::system(&ctx.worker_principal_id),
        action_type: governance_action.to_string(),
        target: proposal.target.clone(),
        parameters: Some(normalized_args.clone()),
        idempotency_key: Some(idempotency_key),
        meta: CommandMeta::default(),
    };

    let outcome = app
        .propose_action(action.clone())
        .await
        .map_err(|e| format!("propose action: {e}"))?;

    if outcome.decision != nalarvo_domain::AuthorityDecision::Allow {
        let waiting = outcome.decision == nalarvo_domain::AuthorityDecision::RequireApproval;
        return Ok(WorkerExecutionOutcome {
            run_id: ctx.run_id.clone(),
            status: match outcome.decision {
                nalarvo_domain::AuthorityDecision::RequireApproval => RunStatus::WaitingApproval,
                _ => RunStatus::Failed,
            },
            result_summary: if waiting {
                "Approval required".into()
            } else {
                format!("action authority denied: {:?}", outcome.decision)
            },
            output_text: None,
            failure_class: (!waiting).then(|| "AUTHORITY_DENIED".into()),
            failure_detail: (!waiting)
                .then(|| format!("Authority decision: {:?}", outcome.decision)),
            input_tokens: None,
            output_tokens: None,
            latency_ms: None,
            context_step_status: ExecutionStepStatus::Succeeded,
            model_step_status: if waiting {
                ExecutionStepStatus::Running
            } else {
                ExecutionStepStatus::Failed
            },
        });
    }

    if app
        .start_runtime_action(&action, &outcome.action_id, &lease)
        .await
        .map_err(|e| e.to_string())?
    {
        // 5. Native Tools Execution on Canonical Root
        let execution: Result<serde_json::Value, String> = async {
            let result = match proposal.tool_name.as_str() {
                "filesystem.list" => {
                    let path = string_arg(&args, "path")?;
                    let entries = tools.list(path).map_err(|e| e.to_string())?;
                    serde_json::json!({ "entries": entries })
                }
                "filesystem.read" => {
                    let path = string_arg(&args, "path")?;
                    let bytes = tools.read(path).map_err(|e| e.to_string())?;
                    let len = bytes.len();
                    let b64 =
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                    serde_json::json!({ "content_base64": b64, "bytes": len })
                }
                "filesystem.write" => {
                    let path = string_arg(&args, "path")?;
                    let content = string_arg(&args, "content")?;
                    #[cfg(windows)]
                    {
                        let _ = path;
                        let pinned = pinned_write.take().ok_or("path escapes configured root")?;
                        app.execute_runtime_write(&action, &outcome.action_id, &lease, || {
                            tools.write_prepared(pinned, content.as_bytes())
                        })
                        .await?;
                    }
                    #[cfg(not(windows))]
                    tools
                        .write(path, content.as_bytes())
                        .map_err(|e| e.to_string())?;
                    serde_json::json!({ "bytes_written": content.len() })
                }
                "shell.execute" => {
                    let prepared = prepared_shell.take().ok_or("Shell preparation missing")?;
                    #[cfg(windows)]
                    let running = app.spawn_runtime_shell(&action, &outcome.action_id, &lease, || prepared.spawn()).await?;
                    #[cfg(not(windows))]
                    let running = prepared.spawn()?;
                    drop(shell_lease_guard.take());
                    let cancel = async {
                        loop {
                            let version=match &ctx.live_lease_version {Some(v)=>Some(v.read().await),None=>None};
                            let expected=version.as_deref().copied().unwrap_or(ctx.lease_version);
                            // Fail closed on DB errors, cancellation, changed Project, or fencing.
                            let active: Result<Option<i64>, _> = sqlx::query_scalar("SELECT l.lease_version FROM runs r JOIN projects p ON p.id=r.project_id AND p.company_id=r.company_id JOIN run_execution_leases l ON l.run_id=r.id WHERE r.id=? AND r.company_id=? AND r.project_id=? AND r.lifecycle_state='RUNNING' AND p.status='ACTIVE' AND p.working_root_path=? AND l.id=? AND l.worker_principal_id=? AND l.released_at IS NULL AND julianday(l.expires_at)>julianday('now')")
                                .bind(&ctx.run_id).bind(&ctx.company_id).bind(&ctx.project_id).bind(bound_root)
                                .bind(&ctx.lease_id).bind(&ctx.worker_principal_id)
                                .fetch_optional(&app.pool).await;
                            if active.ok().flatten()!=Some(expected) { return; }
                            drop(version);
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                    };
                    let res = running.wait(cancel).await?;
                    serde_json::to_value(res).map_err(|e| e.to_string())?
                }
                other => return Err(format!("unknown tool '{other}'")),
            };
            Ok(result)
        }
        .await;
        drop(shell_lease_guard.take());
        let mut lease = lease;
        let version = match &ctx.live_lease_version {
            Some(v) => Some(v.read().await),
            None => None,
        };
        lease.lease_version = version.as_deref().copied().unwrap_or(lease.lease_version);
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                // No retry: write IO and shell acknowledgement failures may follow an effect.
                let status = if (proposal.tool_name == "filesystem.write"
                    && error == "filesystem operation failed")
                    || (proposal.tool_name == "shell.execute"
                        && error != "command is not allowlisted")
                {
                    "OUTCOME_UNKNOWN"
                } else {
                    "FAILED"
                };
                app.finish_runtime_action(
                    &action,
                    &outcome.action_id,
                    &lease,
                    status,
                    &serde_json::json!({"failure_class": status}),
                )
                .await
                .map_err(|e| e.to_string())?;
                return Err(error);
            }
        };

        if let Err(error) = ack() {
            app.finish_runtime_action(
                &action,
                &outcome.action_id,
                &lease,
                "OUTCOME_UNKNOWN",
                &serde_json::json!({"failure_class":"ACK_LOST"}),
            )
            .await
            .map_err(|e| e.to_string())?;
            return Err(error);
        }
        let failed = proposal.tool_name == "shell.execute" && result["status"] != 0;
        let status = if failed { "FAILED" } else { "SUCCEEDED" };
        app.finish_runtime_action(&action, &outcome.action_id, &lease, status, &result)
            .await
            .map_err(|e| e.to_string())?;
        Ok(WorkerExecutionOutcome {
            run_id: ctx.run_id.clone(),
            status: if failed {
                RunStatus::Failed
            } else {
                RunStatus::Succeeded
            },
            result_summary: format!("tool execution {status}: {}", proposal.tool_name),
            output_text: Some(result.to_string()),
            failure_class: failed.then(|| "SHELL_EXIT_NONZERO".into()),
            failure_detail: failed.then(|| format!("Shell exit status: {}", result["status"])),
            input_tokens: None,
            output_tokens: None,
            latency_ms: None,
            context_step_status: ExecutionStepStatus::Succeeded,
            model_step_status: if failed {
                ExecutionStepStatus::Failed
            } else {
                ExecutionStepStatus::Succeeded
            },
        })
    } else {
        let row = sqlx::query("SELECT status, result FROM actions WHERE id=?")
            .bind(&outcome.action_id)
            .fetch_one(&app.pool)
            .await
            .map_err(|e| e.to_string())?;
        let st: String = row.get("status");
        let res_text: Option<String> = row.get("result");
        Ok(WorkerExecutionOutcome {
            run_id: ctx.run_id.clone(),
            status: if st == "SUCCEEDED" {
                RunStatus::Succeeded
            } else {
                RunStatus::Failed
            },
            result_summary: format!("Tool execution replay: {st}"),
            output_text: res_text,
            failure_class: if st == "SUCCEEDED" {
                None
            } else if st == "OUTCOME_UNKNOWN" {
                Some("OUTCOME_UNKNOWN".into())
            } else {
                Some("REPLAYED_FAILURE".into())
            },
            failure_detail: None,
            input_tokens: None,
            output_tokens: None,
            latency_ms: None,
            context_step_status: ExecutionStepStatus::Succeeded,
            model_step_status: if st == "SUCCEEDED" {
                ExecutionStepStatus::Succeeded
            } else {
                ExecutionStepStatus::Failed
            },
        })
    }
}

fn string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{key} must be a string"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nalarvo_application::SecretStore;

    async fn setup_test_environment() -> (ApplicationContext, tempfile::TempDir, TrustedRunContext)
    {
        let pool = nalarvo_persistence::create_pool("sqlite::memory:")
            .await
            .unwrap();
        nalarvo_persistence::run_migrations(&pool).await.unwrap();
        let app = ApplicationContext::new(pool.clone());
        let temp_dir = tempfile::tempdir().unwrap();
        let proj_root = temp_dir.path().to_str().unwrap().replace('\\', "/");

        for q in [
            "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES ('u','u@test','U','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES ('w','u','W','w','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('comp_1','w','C','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('comp_2','w','C2','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO provider_connections(id,workspace_id,provider_kind,name,status,row_version,created_at,updated_at) VALUES('pc_1','w','mock','Mock','ENABLED',1,'t','t')",
            "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES ('dept_1','comp_1','D','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES ('role_1','comp_1','R','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES ('comp_1','dept_1','role_1','2026-10-01T00:00:00Z')",
            "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES ('agent_1','comp_1','A','dept_1','role_1',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO projects(id,company_id,name,priority,working_root_path,status,row_version,created_at,updated_at) VALUES ('proj_1','comp_1','P','MEDIUM',?,'ACTIVE',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES ('team_1','comp_1','proj_1','T','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES ('alloc_1','comp_1','proj_1','team_1','agent_1','ACTIVE','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,row_version,created_at,updated_at) VALUES ('wi_1','comp_1','proj_1','W','IN_PROGRESS','TASK',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,status,assigned_at) VALUES ('assignment_1','comp_1','proj_1','wi_1','agent_1','alloc_1','ACTIVE','2026-10-01T00:00:00Z')",
            "INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,row_version,created_at,updated_at) VALUES ('run_1','comp_1','proj_1','wi_1','agent_1','RUNNING','MANUAL',1,'SYSTEM','worker_1','2026-10-01T00:00:00Z','corr_1',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO run_execution_leases(id,run_id,worker_principal_id,lease_version,acquired_at,heartbeat_at,expires_at,created_at) VALUES ('lease_1','run_1','worker_1',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z','2099-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,description,created_at) VALUES ('pd_1','w','filesystem.write','filesystem','write','desc','2026-10-01T00:00:00Z')",
            "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('pg_1','w','comp_1','pd_1','SYSTEM','worker_1','company','comp_1','ALLOW','2026-10-01T00:00:00Z')",
        ] {
            if q.contains('?') {
                sqlx::query(q)
                    .bind(&proj_root)
                    .execute(&pool)
                    .await
                    .unwrap();
            } else {
                sqlx::query(q).execute(&pool).await.unwrap();
            }
        }

        let ctx = TrustedRunContext {
            run_id: "run_1".into(),
            company_id: "comp_1".into(),
            project_id: "proj_1".into(),
            work_item_id: "wi_1".into(),
            agent_id: "agent_1".into(),
            lease_id: "lease_1".into(),
            lease_version: 1,
            worker_principal_id: "worker_1".into(),
            live_lease_version: None,
        };

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
            .bind(serde_json::json!({"provider_connection_id":"pc_1"}).to_string())
            .bind(step)
            .execute(&app.pool)
            .await
            .unwrap();
        (app, temp_dir, ctx)
    }

    #[test]
    fn rejects_nested_credential_smuggling() {
        assert!(
            validate_arguments(
                "filesystem.write",
                &serde_json::json!({"path":"a","content":"x","nested":{"credential_ref":"x"}})
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unsupported_credential_keys() {
        assert!(
            validate_arguments(
                "filesystem.read",
                &serde_json::json!({"path":"a","credential_ref":"cred-1"})
            )
            .is_err()
        );
        assert!(
            validate_arguments(
                "filesystem.read",
                &serde_json::json!({"path":"a","credential_ref_id":"cred-1"})
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(
            validate_arguments(
                "filesystem.list",
                &serde_json::json!({"path":"a","extra":"val"})
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_valid_filesystem_write() {
        assert!(
            validate_arguments(
                "filesystem.write",
                &serde_json::json!({"path":"note.txt","content":"hello"})
            )
            .is_ok()
        );
    }

    #[cfg(windows)]
    #[test]
    fn shell_fails_closed_even_with_well_formed_executable_and_argv() {
        let root = tempfile::tempdir().unwrap();
        assert!(
            crate::shell::ShellPolicy::default()
                .prepare(
                    root.path(),
                    "C:\\Windows\\System32\\cmd.exe",
                    &["/C".into(), "echo must-not-run".into()]
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_credential_supplied() {
        let (app, _dir, ctx) = setup_test_environment().await;

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.read".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "credential_ref": "secret_ref_1"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rejected"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn shell_job_assignment_failure_never_resumes_process() {
        let (app, dir, ctx) = setup_test_environment().await;
        sqlx::query("UPDATE permission_definitions SET name='shell.execute',resource='shell',operation='execute' WHERE id='pd_1'").execute(&app.pool).await.unwrap();
        let binary_dir = tempfile::tempdir().unwrap();
        let executable = binary_dir.path().join("fixture.exe");
        assert!(
            std::process::Command::new("rustc")
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/support/shell_fixture.rs"
                ))
                .args(["--edition=2024", "-o"])
                .arg(&executable)
                .status()
                .unwrap()
                .success()
        );
        let policy = crate::shell::ShellPolicy::from_json(
            &serde_json::json!([{"executable":executable,"args":["entry"],"timeout_ms":5000}])
                .to_string(),
        )
        .unwrap();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::shell::REJECT_JOB_ASSIGNMENT.set(false);
            }
        }
        let _reset = Reset;
        crate::shell::REJECT_JOB_ASSIGNMENT.set(true);
        let outcome = dispatch_with_shell(
            &app,
            &ctx,
            ToolProposal {
                call_id: Some("failed-job".into()),
                tool_name: "shell.execute".into(),
                target: executable.to_str().unwrap().into(),
                arguments: serde_json::json!({"executable":executable,"args":["entry"]}),
            },
            &policy,
        )
        .await;
        assert_eq!(outcome.unwrap_err(), "Job assignment failed before resume");
        assert!(!dir.path().join("entry-effect.txt").exists());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn shell_dispatch_never_spawns_before_containment_even_with_authority() {
        let (app, dir, mut ctx) = setup_test_environment().await;
        sqlx::query("UPDATE permission_definitions SET name='shell.execute',resource='shell',operation='execute' WHERE id='pd_1'")
            .execute(&app.pool).await.unwrap();
        for mode in ["normal", "cancel", "expired-lease", "foreign-project"] {
            if mode == "cancel" {
                sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED' WHERE id='run_1'")
                    .execute(&app.pool)
                    .await
                    .unwrap();
            }
            if mode == "expired-lease" {
                sqlx::query("UPDATE run_execution_leases SET expires_at='2000-01-01T00:00:00Z' WHERE id='lease_1'")
                    .execute(&app.pool).await.unwrap();
            }
            if mode == "foreign-project" {
                ctx.project_id = "foreign".into();
            }
            let result = dispatch(
                &app,
                &ctx,
                ToolProposal {
                    call_id: Some(format!("shell-{mode}")),
                    tool_name: "shell.execute".into(),
                    target: "C:\\Windows\\System32\\cmd.exe".into(),
                    arguments: serde_json::json!({"executable":"C:\\Windows\\System32\\cmd.exe",
                    "args":["/C", "echo forbidden>side-effect.txt"]}),
                },
            )
            .await;
            assert!(result.is_err());
        }
        assert!(!dir.path().join("side-effect.txt").exists());
        for table in ["actions", "tool_invocations", "usage_records"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&app.pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "shell rejection must precede {table}");
        }
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_unauthorized() {
        let (app, dir, ctx) = setup_test_environment().await;

        // Revoke the grant
        sqlx::query("DELETE FROM permission_grants WHERE id = 'pg_1'")
            .execute(&app.pool)
            .await
            .unwrap();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let outcome = dispatch(&app, &ctx, proposal).await.unwrap();
        assert_eq!(outcome.status, RunStatus::Failed);
        assert_eq!(outcome.failure_class.as_deref(), Some("AUTHORITY_DENIED"));
        assert!(!dir.path().join("note.txt").exists());
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_authorized_in_project_root_before_m5_s2() {
        let (app, dir, ctx) = setup_test_environment().await;
        // Authorization alone is insufficient: incomplete Tool lineage stays closed.
        sqlx::query("UPDATE execution_steps SET input_metadata=NULL WHERE run_id='run_1'")
            .execute(&app.pool)
            .await
            .unwrap();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello world"
            }),
        };

        assert!(
            dispatch(&app, &ctx, proposal)
                .await
                .unwrap_err()
                .contains("canonical provider lineage")
        );
        assert!(!dir.path().join("note.txt").exists());
    }

    #[tokio::test]
    async fn dispatch_rejects_spoofed_company_tenant() {
        let (app, _dir, mut ctx) = setup_test_environment().await;
        ctx.company_id = "comp_2".into(); // Spoofed company

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("canonical run run_1 not found for company comp_2")
        );
    }

    #[tokio::test]
    async fn dispatch_rejects_project_mismatch() {
        let (app, _dir, mut ctx) = setup_test_environment().await;
        ctx.project_id = "proj_nonexistent".into();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("project scope mismatch"));
    }

    #[tokio::test]
    async fn dispatch_rejects_stale_or_invalid_lease() {
        let (app, _dir, mut ctx) = setup_test_environment().await;
        ctx.lease_version = 999; // Stale version

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("fencing violation"));
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_project_lifecycle_not_active() {
        let (app, _dir, ctx) = setup_test_environment().await;

        sqlx::query("UPDATE projects SET status = 'PAUSED' WHERE id = 'proj_1'")
            .execute(&app.pool)
            .await
            .unwrap();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("lifecycle status 'Paused' disallows execution")
        );
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_project_working_root_missing_or_invalid() {
        let (app, _dir, ctx) = setup_test_environment().await;

        sqlx::query("UPDATE projects SET working_root_path = NULL WHERE id = 'proj_1'")
            .execute(&app.pool)
            .await
            .unwrap();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.write".into(),
            target: "note.txt".into(),
            arguments: serde_json::json!({
                "path": "note.txt",
                "content": "hello"
            }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("has no bound working_root_path")
        );
    }

    #[tokio::test]
    async fn dispatch_non_consequential_resolves_and_operates_strictly_in_project_root() {
        let (app, dir, ctx) = setup_test_environment().await;

        // Create a file in canonical project root
        std::fs::write(dir.path().join("existing.txt"), "project_A1_data").unwrap();

        // Grant permission for filesystem.read
        sqlx::query(
            "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,description,created_at) VALUES ('pd_read','w','filesystem.read','filesystem','read','desc','2026-10-01T00:00:00Z')",
        )
        .execute(&app.pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('pg_read','w','comp_1','pd_read','SYSTEM','worker_1','company','comp_1','ALLOW','2026-10-01T00:00:00Z')",
        )
        .execute(&app.pool)
        .await
        .unwrap();

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "filesystem.read".into(),
            target: "existing.txt".into(),
            arguments: serde_json::json!({ "path": "existing.txt" }),
        };

        let outcome = dispatch(&app, &ctx, proposal).await.unwrap();
        assert_eq!(outcome.status, RunStatus::Succeeded);
        let out_text = outcome.output_text.unwrap();
        assert!(out_text.contains("content_base64"));
    }

    #[tokio::test]
    async fn unavailable_provider_tool_fails_without_exposing_secret() {
        let (app, _dir, ctx) = setup_test_environment().await;
        let store = Arc::new(nalarvo_application::FakeSecretStore::default());
        store
            .put(
                &nalarvo_application::CredentialRef::new("cred_1"),
                &nalarvo_application::SecretValue::new("top-secret"),
            )
            .unwrap();
        let app = app.with_secret_store(store);
        app.ensure_canonical_permissions("w").await.unwrap();

        let now = chrono::Utc::now().to_rfc3339();
        for q in [
            "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,status,row_version,created_at,updated_at) VALUES ('cred_1','w','API','secretstore://cred_1','ACTIVE',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO provider_connections(id,workspace_id,credential_ref_id,name,provider_kind,created_at,updated_at) VALUES ('pc','w','cred_1','P','test','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO models(id,workspace_id,provider_connection_id,model_key,created_at,updated_at) VALUES ('m','w','pc','test','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO model_profiles(id,workspace_id,name,status,row_version,created_at,updated_at) VALUES ('profile_1','w','P','ACTIVE',1,'2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
            "INSERT INTO model_profile_versions(profile_id,workspace_id,version,model_id,created_at) VALUES ('profile_1','w',1,'m','2026-10-01T00:00:00Z')",
            "UPDATE model_profiles SET current_version = 1 WHERE id = 'profile_1'",
            "INSERT INTO company_workspace_resource_grants(company_id,workspace_id,model_profile_id,created_at) VALUES ('comp_1','w','profile_1','2026-10-01T00:00:00Z')",
            "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('g','w','comp_1','pd_credential.use','SYSTEM','worker_1','COMPANY','comp_1','ALLOW','2026-10-01T00:00:00Z')",
            "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES ('g2','w','comp_1','pd_provider.use','SYSTEM','worker_1','COMPANY','comp_1','ALLOW','2026-10-01T00:00:00Z')",
        ] {
            sqlx::query(q).bind(&now).execute(&app.pool).await.unwrap();
        }

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "provider.call".into(),
            target: "test".into(),
            arguments: serde_json::json!({"model_profile_id":"profile_1","credential_ref_id":"cred_1","prompt":"hi"}),
        };
        let error = dispatch(&app, &ctx, proposal).await.unwrap_err();
        assert_eq!(error, "PROVIDER_TOOL_EXECUTION_UNAVAILABLE");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actions")
                .fetch_one(&app.pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn dispatch_fails_closed_when_credentialed_native_tool_called() {
        let (app, _dir, ctx) = setup_test_environment().await;

        let proposal = ToolProposal {
            call_id: Some("fixture-call".into()),
            tool_name: "provider.call".into(),
            target: "model".into(),
            arguments: serde_json::json!({ "prompt": "hi" }),
        };

        let result = dispatch(&app, &ctx, proposal).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "PROVIDER_TOOL_EXECUTION_UNAVAILABLE");
    }
}

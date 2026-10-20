use crate::context::{ContextInput, build_context};
use chrono::Utc;
use nalarvo_domain::{ExecutionStepStatus, RunStatus};
use nalarvo_model_gateway::{
    MockMode, MockProvider, ModelMessage, ModelRequest, ModelResponse, ModelRole,
    OpenAiCompatibleProvider, Provider, ProviderError, ProviderErrorKind,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::OpenOptions,
    future::Future,
    io::Write,
    path::Path,
    pin::Pin,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn provider_counter_file() -> Option<std::path::PathBuf> {
    std::env::var_os("NALARVO_TEST_PROVIDER_COUNTER").map(std::path::PathBuf::from)
}

fn record_provider_invocation() {
    PROVIDER_INVOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
    if let Some(path) = provider_counter_file()
        && let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path)
    {
        let _ = writeln!(file, "1");
    }
}

fn provider_invocation_file_count() -> usize {
    provider_counter_file()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

pub fn total_provider_invocation_count() -> usize {
    provider_invocation_count() + provider_invocation_file_count()
}
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
    sync::{mpsc, watch},
};
use tracing::info;

static WORKER_PROCESS_SPAWN_COUNT: AtomicUsize = AtomicUsize::new(0);
static WORKER_PROCESS_REAP_COUNT: AtomicUsize = AtomicUsize::new(0);
static PROVIDER_INVOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn worker_process_spawn_count() -> usize {
    WORKER_PROCESS_SPAWN_COUNT.load(Ordering::SeqCst)
}

pub fn reset_worker_process_spawn_count() {
    WORKER_PROCESS_SPAWN_COUNT.store(0, Ordering::SeqCst);
    WORKER_PROCESS_REAP_COUNT.store(0, Ordering::SeqCst);
}

pub fn worker_process_reap_count() -> usize {
    WORKER_PROCESS_REAP_COUNT.load(Ordering::SeqCst)
}

pub fn provider_invocation_count() -> usize {
    PROVIDER_INVOCATION_COUNT.load(Ordering::SeqCst)
}

pub fn reset_provider_invocation_count() {
    PROVIDER_INVOCATION_COUNT.store(0, Ordering::SeqCst);
}

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeStreamEvent {
    OutputDelta(String),
    FinalOutput(String),
    Usage {
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    },
    Error(String),
}

pub type ActionProposalHandler = Arc<
    dyn Fn(
            crate::core_dispatch::TrustedRunContext,
            ToolProposal,
        ) -> Pin<Box<dyn Future<Output = Result<WorkerExecutionOutcome, String>> + Send>>
        + Send
        + Sync,
>;

pub type ModelRequestHandler = Arc<
    dyn Fn(
            crate::core_dispatch::TrustedRunContext,
            ModelRequest,
            watch::Receiver<bool>,
        ) -> Pin<Box<dyn Future<Output = Result<ModelResponse, ProviderError>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct RunExecutionRequest {
    pub run_id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub model_profile_version_id: Option<String>,
    pub provider_connection_id: String,
    pub base_url: Option<String>,
    pub auth_token: Option<String>,
    pub model_key: String,
    pub context_input: ContextInput,
    pub worker_principal_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub pool: Option<sqlx::SqlitePool>,
    pub expected_run_version: i64,
    pub mock_mode: MockMode,
    pub cancel_rx: Option<watch::Receiver<bool>>,
    pub action_handler: Option<ActionProposalHandler>,
    pub model_handler: Option<ModelRequestHandler>,
    pub resume_proposal: Option<ToolProposal>,
}

impl std::fmt::Debug for RunExecutionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunExecutionRequest")
            .field("run_id", &self.run_id)
            .field("company_id", &self.company_id)
            .field("project_id", &self.project_id)
            .field("work_item_id", &self.work_item_id)
            .field("agent_id", &self.agent_id)
            .field("model_profile_version_id", &self.model_profile_version_id)
            .field("provider_connection_id", &self.provider_connection_id)
            .field("base_url", &self.base_url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("model_key", &self.model_key)
            .field("context_input", &self.context_input)
            .field("worker_principal_id", &self.worker_principal_id)
            .field("lease_id", &self.lease_id)
            .field("lease_version", &self.lease_version)
            .field("expected_run_version", &self.expected_run_version)
            .field("mock_mode", &self.mock_mode)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerExecutionOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub result_summary: String,
    pub output_text: Option<String>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub latency_ms: Option<i64>,
    pub context_step_status: ExecutionStepStatus,
    pub model_step_status: ExecutionStepStatus,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessRequest {
    run_id: String,
    company_id: String,
    project_id: String,
    work_item_id: String,
    agent_id: String,
    model_profile_version_id: Option<String>,
    provider_connection_id: String,
    model_key: String,
    context_input: ContextInput,
    worker_principal_id: String,
    lease_id: String,
    expected_run_version: i64,
    mock_mode: MockMode,
    resume_proposal: Option<ToolProposal>,
}

impl From<&RunExecutionRequest> for ProcessRequest {
    fn from(value: &RunExecutionRequest) -> Self {
        Self {
            run_id: value.run_id.clone(),
            company_id: value.company_id.clone(),
            project_id: value.project_id.clone(),
            work_item_id: value.work_item_id.clone(),
            agent_id: value.agent_id.clone(),
            model_profile_version_id: value.model_profile_version_id.clone(),
            provider_connection_id: value.provider_connection_id.clone(),
            model_key: value.model_key.clone(),
            context_input: value.context_input.clone(),
            worker_principal_id: value.worker_principal_id.clone(),
            lease_id: value.lease_id.clone(),
            expected_run_version: value.expected_run_version,
            mock_mode: value.mock_mode,
            resume_proposal: value.resume_proposal.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolProposal {
    /// Stable model call identity; consequential operations require it.
    #[serde(default)]
    pub call_id: Option<String>,
    pub tool_name: String,
    pub target: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
enum ParentFrame {
    Start {
        session_id: String,
        request: Box<ProcessRequest>,
    },
    ProviderResult {
        session_id: String,
        result: Result<ModelResponse, WireProviderError>,
    },
    /// Core alone evaluates, persists, and executes a proposed action.
    ActionResult {
        session_id: String,
        outcome: WorkerExecutionOutcome,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum ChildFrame {
    ProviderCall {
        session_id: String,
        request: ModelRequest,
    },
    /// A worker may propose but never persist or execute a native tool.
    ToolProposal {
        session_id: String,
        proposal: ToolProposal,
    },
    Outcome {
        session_id: String,
        outcome: WorkerExecutionOutcome,
    },
}

#[allow(clippy::collapsible_if)]
fn tool_proposal(response: &ModelResponse) -> Option<ToolProposal> {
    if let Some(text) = &response.text {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
            if let Some(tool_name) = v.get("tool_name").and_then(|s| s.as_str()) {
                return Some(ToolProposal {
                    call_id: v.get("call_id").and_then(|v| v.as_str()).map(str::to_owned),
                    tool_name: tool_name.to_owned(),
                    target: v
                        .get("target")
                        .and_then(|s| s.as_str())
                        .unwrap_or_default()
                        .to_owned(),
                    arguments: v
                        .get("arguments")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                });
            }
        }
    }
    let value = response.structured.as_ref()?;
    Some(ToolProposal {
        call_id: value
            .get("call_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        tool_name: value.get("tool_name")?.as_str()?.to_owned(),
        target: value.get("target")?.as_str()?.to_owned(),
        arguments: value.get("arguments")?.clone(),
    })
}

fn validate_tool_proposal(proposal: &ToolProposal) -> Result<(), String> {
    if proposal.tool_name.trim().is_empty() {
        return Err("tool_name cannot be empty".into());
    }
    validate_no_raw_credentials(&proposal.arguments)?;
    Ok(())
}

fn validate_no_raw_credentials(value: &serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                let lower = key.to_ascii_lowercase();
                if (lower == "secret"
                    || lower == "api_key"
                    || lower == "auth_token"
                    || lower == "password"
                    || lower == "raw_credential")
                    && !val.is_null()
                {
                    return Err(format!(
                        "raw credential embedded in tool argument key: {key}"
                    ));
                }
                validate_no_raw_credentials(val)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr {
                validate_no_raw_credentials(val)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct WireProviderError {
    class: String,
    detail: String,
}

/// In-process adapter retained for tests and callers that do not own a worker binary.
pub async fn execute_run_worker(
    request: RunExecutionRequest,
    event_tx: Option<mpsc::Sender<RuntimeStreamEvent>>,
) -> WorkerExecutionOutcome {
    let started_at = Utc::now();
    info!(run_id = %request.run_id, worker_id = %request.worker_principal_id, "worker starts run");
    let model_request = match prepare_model_request(
        &request.run_id,
        &request.model_key,
        &request.context_input,
        started_at,
    ) {
        Ok(value) => value,
        Err(outcome) => return *outcome,
    };
    let cancel_rx = request
        .cancel_rx
        .clone()
        .unwrap_or_else(|| watch::channel(false).1);
    let response = invoke_model(&request, model_request, cancel_rx).await;
    let outcome = finish_response(request.run_id, started_at, response.map_err(wire_error));
    emit_events(&outcome, event_tx).await;
    outcome
}

/// Runs the disposable worker as an OS child. Credentials remain in this parent and provider
/// traffic is mediated here; the child receives only scoped execution data over framed stdin.
pub async fn execute_run_worker_process(
    executable: &Path,
    request: RunExecutionRequest,
    event_tx: Option<mpsc::Sender<RuntimeStreamEvent>>,
) -> Result<WorkerExecutionOutcome, String> {
    if let Some(pool) = &request.pool
        && request.action_handler.is_some()
    {
        let lease = nalarvo_persistence::m4::ExecutionLease {
            id: request.lease_id.clone(),
            run_id: request.run_id.clone(),
            worker_principal_id: request.worker_principal_id.clone(),
            lease_version: request.lease_version,
            expires_at: Utc::now(),
        };
        let step = nalarvo_persistence::m4::resolve_tool_step(
            pool,
            &lease,
            &nalarvo_domain::CompanyId(request.company_id.clone()),
        )
        .await
        .map_err(|e| format!("stale worker fencing: {e}"))?;
        let mut tx = pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| e.to_string())?;
        nalarvo_persistence::m4::require_current_lease_tx(&mut tx, &lease)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE execution_steps SET input_metadata=? WHERE id=? AND input_metadata IS NULL")
            .bind(serde_json::json!({"provider_connection_id":request.provider_connection_id,"model_key":request.model_key}).to_string())
            .bind(&step).execute(&mut *tx).await.map_err(|e|e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
    }
    let mut child = Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn worker: {e}"))?;
    WORKER_PROCESS_SPAWN_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut stdin = child.stdin.take().ok_or("worker stdin unavailable")?;
    let mut stdout = child.stdout.take().ok_or("worker stdout unavailable")?;
    let session_id = uuid::Uuid::now_v7().to_string();

    let exchange = async {
        write_frame(
            &mut stdin,
            &ParentFrame::Start {
                session_id: session_id.clone(),
                request: Box::new(ProcessRequest::from(&request)),
            },
        )
        .await?;
        let first_frame = read_frame::<_, ChildFrame>(&mut stdout).await?;
        let next_frame = match first_frame {
            ChildFrame::ProviderCall {
                session_id: returned,
                request: model_request,
            } if returned == session_id && request.resume_proposal.is_none() => {
                if let Some(pool) = &request.pool {
                    let valid = nalarvo_persistence::m4::verify_current_lease(
                        pool,
                        &request.run_id,
                        &request.worker_principal_id,
                        &request.lease_id,
                        request.lease_version,
                    )
                    .await
                    .map_err(|e| format!("verify lease: {e}"))?;

                    if !valid {
                        return Err(format!(
                            "stale worker fencing: lease {} version {} for run {} is no longer valid",
                            request.lease_id, request.lease_version, request.run_id
                        ));
                    }
                }

                let cancel_rx = request
                    .cancel_rx
                    .clone()
                    .unwrap_or_else(|| watch::channel(false).1);
                let provider_result = invoke_model(&request, model_request, cancel_rx)
                    .await
                    .map_err(wire_error);
                write_frame(
                    &mut stdin,
                    &ParentFrame::ProviderResult {
                        session_id: session_id.clone(),
                        result: provider_result,
                    },
                )
                .await?;
                read_frame::<_, ChildFrame>(&mut stdout).await?
            }
            frame @ ChildFrame::ToolProposal { .. } if request.resume_proposal.is_some() => frame,
            _ => {
                return Err(
                    "worker protocol: expected authenticated provider call or continuation".into(),
                );
            }
        };

        match next_frame {
            ChildFrame::ToolProposal {
                session_id: returned,
                proposal,
            } if returned == session_id => {
                validate_tool_proposal(&proposal)?;
                if request
                    .resume_proposal
                    .as_ref()
                    .is_some_and(|saved| saved != &proposal)
                {
                    return Err("worker continuation differs from durable checkpoint".into());
                }
                let handler = request
                    .action_handler
                    .as_ref()
                    .ok_or_else(|| "Core action proposal service unavailable".to_string())?;
                let trusted_context = crate::core_dispatch::TrustedRunContext {
                    run_id: request.run_id.clone(),
                    company_id: request.company_id.clone(),
                    project_id: request.project_id.clone(),
                    work_item_id: request.work_item_id.clone(),
                    agent_id: request.agent_id.clone(),
                    lease_id: request.lease_id.clone(),
                    lease_version: request.lease_version,
                    worker_principal_id: request.worker_principal_id.clone(),
                    live_lease_version: None,
                };
                let mut cancel_rx = request.cancel_rx.clone();
                if cancel_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
                    return Err("worker action cancelled before dispatch".into());
                }
                let cancelled = async {
                    if let Some(rx) = cancel_rx.as_mut() {
                        loop {
                            if *rx.borrow() {
                                return;
                            }
                            if rx.changed().await.is_err() {
                                std::future::pending::<()>().await;
                            }
                        }
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let outcome = tokio::select! {
                    biased;
                    _=cancelled=>return Err("worker action cancelled".into()),
                    result=handler(trusted_context, proposal)=>result?,
                };
                write_frame(
                    &mut stdin,
                    &ParentFrame::ActionResult {
                        session_id: session_id.clone(),
                        outcome: outcome.clone(),
                    },
                )
                .await?;
                match read_frame::<_, ChildFrame>(&mut stdout).await? {
                    ChildFrame::Outcome {
                        session_id: returned,
                        outcome: _child_outcome,
                    } if returned == session_id => {
                        stdin
                            .shutdown()
                            .await
                            .map_err(|e| format!("close worker stdin: {e}"))?;
                        Ok(outcome)
                    }
                    _ => Err("worker protocol: expected authenticated outcome".into()),
                }
            }
            ChildFrame::Outcome {
                session_id: returned,
                outcome,
            } if returned == session_id => {
                stdin
                    .shutdown()
                    .await
                    .map_err(|e| format!("close worker stdin: {e}"))?;
                Ok(outcome)
            }
            _ => Err("worker protocol: expected authenticated outcome".into()),
        }
    };

    let outcome = match tokio::time::timeout(Duration::from_secs(35), exchange).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error);
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err("worker process timed out".into());
        }
    };
    let status = child
        .wait()
        .await
        .map_err(|e| format!("wait worker: {e}"))?;
    WORKER_PROCESS_REAP_COUNT.fetch_add(1, Ordering::SeqCst);
    if !status.success() {
        return Err(format!("worker exited with {status}"));
    }
    emit_events(&outcome, event_tx).await;
    Ok(outcome)
}

/// Worker binary entrypoint. No database or provider access occurs in this process.
pub async fn run_worker_stdio() -> Result<(), String> {
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let (session_id, request) = match read_frame::<_, ParentFrame>(&mut stdin).await? {
        ParentFrame::Start {
            session_id,
            request,
        } => (session_id, request),
        _ => return Err("worker protocol: expected start".into()),
    };
    let started_at = Utc::now();
    if let Some(proposal) = request.resume_proposal {
        write_frame(
            &mut stdout,
            &ChildFrame::ToolProposal {
                session_id: session_id.clone(),
                proposal,
            },
        )
        .await?;
        let outcome = match read_frame::<_, ParentFrame>(&mut stdin).await? {
            ParentFrame::ActionResult {
                session_id: returned,
                outcome,
            } if returned == session_id => outcome,
            _ => return Err("worker protocol: expected authenticated continuation result".into()),
        };
        return write_frame(
            &mut stdout,
            &ChildFrame::Outcome {
                session_id,
                outcome,
            },
        )
        .await;
    }
    let model_request = match prepare_model_request(
        &request.run_id,
        &request.model_key,
        &request.context_input,
        started_at,
    ) {
        Ok(value) => value,
        Err(outcome) => {
            return write_frame(
                &mut stdout,
                &ChildFrame::Outcome {
                    session_id,
                    outcome: *outcome,
                },
            )
            .await;
        }
    };
    write_frame(
        &mut stdout,
        &ChildFrame::ProviderCall {
            session_id: session_id.clone(),
            request: model_request,
        },
    )
    .await?;
    let result = match read_frame::<_, ParentFrame>(&mut stdin).await? {
        ParentFrame::ProviderResult {
            session_id: returned,
            result,
        } if returned == session_id => result,
        _ => return Err("worker protocol: expected authenticated provider result".into()),
    };
    #[allow(clippy::collapsible_if)]
    if let Ok(response) = &result {
        if let Some(proposal) = tool_proposal(response) {
            write_frame(
                &mut stdout,
                &ChildFrame::ToolProposal {
                    session_id: session_id.clone(),
                    proposal,
                },
            )
            .await?;
            let outcome = match read_frame::<_, ParentFrame>(&mut stdin).await? {
                ParentFrame::ActionResult {
                    session_id: returned,
                    outcome,
                } if returned == session_id => outcome,
                _ => return Err("worker protocol: expected authenticated action result".into()),
            };
            return write_frame(
                &mut stdout,
                &ChildFrame::Outcome {
                    session_id,
                    outcome,
                },
            )
            .await;
        }
    }
    let outcome = finish_response(request.run_id, started_at, result);
    write_frame(
        &mut stdout,
        &ChildFrame::Outcome {
            session_id,
            outcome,
        },
    )
    .await
}

fn prepare_model_request(
    run_id: &str,
    model_key: &str,
    context_input: &ContextInput,
    started_at: chrono::DateTime<Utc>,
) -> Result<ModelRequest, Box<WorkerExecutionOutcome>> {
    let context = build_context(context_input)
        .map_err(|error| {
            failure_outcome(
                run_id.to_string(),
                started_at,
                "CONTEXT_BUILD_FAILED",
                error.to_string(),
                ExecutionStepStatus::Failed,
            )
        })
        .map_err(Box::new)?;
    Ok(ModelRequest {
        model: model_key.to_string(),
        messages: vec![
            ModelMessage {
                role: ModelRole::System,
                content: "Return only safe requested output.".into(),
            },
            ModelMessage::user(context),
        ],
        temperature: Some(0.0),
        max_tokens: Some(4096),
        stream: true,
    })
}

async fn invoke_model(
    request: &RunExecutionRequest,
    model_request: ModelRequest,
    cancel_rx: watch::Receiver<bool>,
) -> Result<ModelResponse, ProviderError> {
    if let Some(handler) = &request.model_handler {
        let ctx = crate::core_dispatch::TrustedRunContext {
            run_id: request.run_id.clone(),
            company_id: request.company_id.clone(),
            project_id: request.project_id.clone(),
            work_item_id: request.work_item_id.clone(),
            agent_id: request.agent_id.clone(),
            lease_id: request.lease_id.clone(),
            lease_version: request.lease_version,
            worker_principal_id: request.worker_principal_id.clone(),
            live_lease_version: None,
        };
        return handler(ctx, model_request, cancel_rx).await;
    }
    if request.base_url.is_some() || request.auth_token.is_some() {
        return Err(ProviderError {
            kind: ProviderErrorKind::Authentication,
            message: "provider invocation requires trusted Core model handler".into(),
            request_id: None,
            status: None,
        });
    }
    // Deterministic credential-free compatibility for M4 process fixtures only.
    record_provider_invocation();
    MockProvider
        .execute(
            model_request,
            request.mock_mode,
            Duration::from_secs(30),
            cancel_rx,
        )
        .await
}

fn wire_error(error: ProviderError) -> WireProviderError {
    WireProviderError {
        class: provider_failure_class(error.kind).into(),
        detail: error.to_string(),
    }
}

#[allow(dead_code)]
fn waiting_approval_outcome(run_id: String) -> WorkerExecutionOutcome {
    WorkerExecutionOutcome {
        run_id,
        status: RunStatus::WaitingApproval,
        result_summary: "Approval required".into(),
        output_text: None,
        failure_class: None,
        failure_detail: None,
        input_tokens: None,
        output_tokens: None,
        latency_ms: None,
        context_step_status: ExecutionStepStatus::Succeeded,
        model_step_status: ExecutionStepStatus::Succeeded,
    }
}

fn finish_response(
    run_id: String,
    started_at: chrono::DateTime<Utc>,
    response: Result<ModelResponse, WireProviderError>,
) -> WorkerExecutionOutcome {
    match response {
        Ok(response) => {
            let text = response
                .text
                .or_else(|| response.structured.map(|v| v.to_string()))
                .unwrap_or_default();
            WorkerExecutionOutcome {
                run_id,
                status: RunStatus::Succeeded,
                result_summary: "Model execution succeeded".into(),
                output_text: Some(text),
                failure_class: None,
                failure_detail: None,
                input_tokens: response
                    .usage
                    .as_ref()
                    .and_then(|u| u.input_tokens)
                    .map(|n| n as i64),
                output_tokens: response
                    .usage
                    .as_ref()
                    .and_then(|u| u.output_tokens)
                    .map(|n| n as i64),
                latency_ms: Some((Utc::now() - started_at).num_milliseconds()),
                context_step_status: ExecutionStepStatus::Succeeded,
                model_step_status: ExecutionStepStatus::Succeeded,
            }
        }
        Err(error) => failure_outcome(
            run_id,
            started_at,
            error.class,
            error.detail,
            ExecutionStepStatus::Succeeded,
        ),
    }
}

async fn emit_events(
    outcome: &WorkerExecutionOutcome,
    event_tx: Option<mpsc::Sender<RuntimeStreamEvent>>,
) {
    let Some(tx) = event_tx else { return };
    if let Some(text) = &outcome.output_text {
        let _ = tx.send(RuntimeStreamEvent::OutputDelta(text.clone())).await;
        let _ = tx.send(RuntimeStreamEvent::FinalOutput(text.clone())).await;
    }
    if outcome.input_tokens.is_some() || outcome.output_tokens.is_some() {
        let _ = tx
            .send(RuntimeStreamEvent::Usage {
                input_tokens: outcome.input_tokens.map(|n| n as u64),
                output_tokens: outcome.output_tokens.map(|n| n as u64),
            })
            .await;
    }
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), String> {
    let payload = serde_json::to_vec(value).map_err(|e| format!("encode worker frame: {e}"))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err("worker frame exceeds size limit".into());
    }
    writer
        .write_u32(payload.len() as u32)
        .await
        .map_err(|e| format!("write worker frame length: {e}"))?;
    writer
        .write_all(&payload)
        .await
        .map_err(|e| format!("write worker frame: {e}"))?;
    writer
        .flush()
        .await
        .map_err(|e| format!("flush worker frame: {e}"))
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
) -> Result<T, String> {
    let length = reader
        .read_u32()
        .await
        .map_err(|e| format!("read worker frame length: {e}"))? as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err("invalid worker frame length".into());
    }
    let mut payload = vec![0; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| format!("read worker frame: {e}"))?;
    serde_json::from_slice(&payload).map_err(|e| format!("decode worker frame: {e}"))
}

pub fn provider_failure_class(kind: ProviderErrorKind) -> &'static str {
    match kind {
        ProviderErrorKind::Timeout => "PROVIDER_TIMEOUT",
        ProviderErrorKind::Authentication => "PROVIDER_AUTHENTICATION",
        ProviderErrorKind::RateLimited => "PROVIDER_RATE_LIMIT",
        ProviderErrorKind::Unavailable => "PROVIDER_UNAVAILABLE",
        ProviderErrorKind::MalformedResponse => "PROVIDER_MALFORMED_RESPONSE",
        ProviderErrorKind::Cancelled => "PROVIDER_CANCELLED",
        ProviderErrorKind::Http | ProviderErrorKind::InvalidRequest => "INTERNAL_PROVIDER_ERROR",
    }
}

fn failure_outcome(
    run_id: String,
    started_at: chrono::DateTime<Utc>,
    class: impl Into<String>,
    detail: String,
    context_status: ExecutionStepStatus,
) -> WorkerExecutionOutcome {
    WorkerExecutionOutcome {
        run_id,
        status: RunStatus::Failed,
        result_summary: "Model execution failed".into(),
        output_text: None,
        failure_class: Some(class.into()),
        failure_detail: Some(detail.chars().take(512).collect()),
        input_tokens: None,
        output_tokens: None,
        latency_ms: Some((Utc::now() - started_at).num_milliseconds()),
        context_step_status: context_status,
        model_step_status: ExecutionStepStatus::Failed,
    }
}

/// Adapter helper. Daemon resolves the authorized credential transiently; it is not serializable.
pub async fn invoke_openai_compatible(
    endpoint: &str,
    auth: &str,
    request: ModelRequest,
    cancel: watch::Receiver<bool>,
) -> Result<ModelResponse, ProviderError> {
    OpenAiCompatibleProvider::new(endpoint)?
        .complete(request, auth, Duration::from_secs(30), cancel)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resume_frame_contains_only_safe_operation_not_authentication() {
        let mut request = RunExecutionRequest {
            run_id: "run".into(),
            company_id: "company".into(),
            project_id: "project".into(),
            work_item_id: "work".into(),
            agent_id: "agent".into(),
            model_profile_version_id: None,
            provider_connection_id: "provider".into(),
            base_url: None,
            auth_token: Some("SYNTHETIC_RESUME_AUTH_CANARY".into()),
            model_key: "model".into(),
            context_input: ContextInput::minimal("company", "project", "work"),
            worker_principal_id: "worker".into(),
            lease_id: "lease".into(),
            lease_version: 2,
            pool: None,
            expected_run_version: 3,
            mock_mode: MockMode::SUCCESS_TEXT,
            cancel_rx: None,
            action_handler: None,
            model_handler: None,
            resume_proposal: Some(ToolProposal {
                call_id: Some("write-1".into()),
                tool_name: "filesystem.write".into(),
                target: "note.txt".into(),
                arguments: serde_json::json!({"path":"note.txt","content":"checked"}),
            }),
        };
        let frame = serde_json::to_string(&ProcessRequest::from(&request)).unwrap();
        assert!(frame.contains("write-1"));
        assert!(!frame.contains("SYNTHETIC_RESUME_AUTH_CANARY"));
        assert!(!frame.contains("auth_token"));
        request.base_url = Some("http://127.0.0.1:1/".into());
        let error = invoke_model(
            &request,
            ModelRequest {
                model: "model".into(),
                messages: vec![nalarvo_model_gateway::ModelMessage::user("check")],
                temperature: None,
                max_tokens: None,
                stream: false,
            },
            watch::channel(false).1,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ProviderErrorKind::Authentication);
    }

    #[test]
    fn debug_redacts_auth_token_canary() {
        let request = RunExecutionRequest {
            run_id: "run".into(),
            company_id: "company".into(),
            project_id: "project".into(),
            work_item_id: "work".into(),
            agent_id: "agent".into(),
            model_profile_version_id: None,
            provider_connection_id: "provider".into(),
            base_url: None,
            auth_token: Some("SYNTHETIC_TOKEN_CANARY".into()),
            model_key: "model".into(),
            context_input: crate::context::ContextInput::minimal("company", "project", "work"),
            worker_principal_id: "worker".into(),
            lease_id: "lease".into(),
            lease_version: 1,
            pool: None,
            expected_run_version: 1,
            mock_mode: MockMode::SUCCESS_TEXT,
            cancel_rx: None,
            action_handler: None,
            model_handler: None,
            resume_proposal: None,
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("SYNTHETIC_TOKEN_CANARY"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn raw_credential_arguments_are_rejected() {
        let proposal = ToolProposal {
            call_id: None,
            tool_name: "custom_tool".into(),
            target: "endpoint".into(),
            arguments: serde_json::json!({ "api_key": "raw_secret" }),
        };
        assert!(validate_tool_proposal(&proposal).is_err());
    }
}

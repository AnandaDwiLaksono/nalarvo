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
    io::Write,
    path::Path,
    process::Stdio,
    sync::atomic::{AtomicUsize, Ordering},
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
static PROVIDER_INVOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn worker_process_spawn_count() -> usize {
    WORKER_PROCESS_SPAWN_COUNT.load(Ordering::SeqCst)
}

pub fn reset_worker_process_spawn_count() {
    WORKER_PROCESS_SPAWN_COUNT.store(0, Ordering::SeqCst);
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

#[derive(Debug, Clone)]
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
        }
    }
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
}

#[derive(Debug, Serialize, Deserialize)]
enum ChildFrame {
    ProviderCall {
        session_id: String,
        request: ModelRequest,
    },
    Outcome {
        session_id: String,
        outcome: WorkerExecutionOutcome,
    },
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
    WORKER_PROCESS_SPAWN_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut child = Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn worker: {e}"))?;
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
        let model_request = match read_frame::<_, ChildFrame>(&mut stdout).await? {
            ChildFrame::ProviderCall {
                session_id: returned,
                request,
            } if returned == session_id => request,
            _ => return Err("worker protocol: expected authenticated provider call".into()),
        };

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
        stdin
            .shutdown()
            .await
            .map_err(|e| format!("close worker stdin: {e}"))?;

        match read_frame::<_, ChildFrame>(&mut stdout).await? {
            ChildFrame::Outcome {
                session_id: returned,
                outcome,
            } if returned == session_id => Ok(outcome),
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
    record_provider_invocation();
    if let (Some(base_url), Some(auth_token)) = (&request.base_url, &request.auth_token)
        && request.mock_mode == MockMode::SUCCESS_TEXT
    {
        return OpenAiCompatibleProvider::new(base_url.clone())?
            .complete(
                model_request,
                auth_token,
                Duration::from_secs(30),
                cancel_rx,
            )
            .await;
    }
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

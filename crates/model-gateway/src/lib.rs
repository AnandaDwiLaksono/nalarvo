//! Normalized, read-only model execution. Authentication is supplied transiently by the caller.
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::sync::watch;

pub type ModelEventStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ModelStreamEvent, ProviderError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelRole {
    System,
    User,
    Assistant,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub content: String,
}
impl ModelMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ModelRole::User,
            content: content.into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: String,
    pub messages: Vec<ModelMessage>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub stream: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub text: Option<String>,
    pub structured: Option<serde_json::Value>,
    pub usage: Option<ModelUsage>,
    pub request_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModelStreamEvent {
    TextDelta(String),
    Usage(ModelUsage),
    Done(ModelResponse),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Authentication,
    RateLimited,
    Unavailable,
    Timeout,
    Cancelled,
    MalformedResponse,
    Http,
    InvalidRequest,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub message: String,
    pub request_id: Option<String>,
    pub status: Option<u16>,
}
impl ProviderError {
    fn new(kind: ProviderErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            request_id: None,
            status: None,
        }
    }
}

/// Provider consumes normalized requests; credentials are never held in provider state.
#[allow(async_fn_in_trait)]
pub trait Provider {
    async fn complete(
        &self,
        request: ModelRequest,
        auth: &str,
        timeout: Duration,
        cancel: watch::Receiver<bool>,
    ) -> Result<ModelResponse, ProviderError>;
    fn stream<'a>(
        &'a self,
        request: ModelRequest,
        auth: &'a str,
        timeout: Duration,
        cancel: watch::Receiver<bool>,
    ) -> ModelEventStream<'a>;
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MockMode {
    SUCCESS_TEXT,
    SUCCESS_STRUCTURED,
    STREAMING_SUCCESS,
    DELAYED_SUCCESS,
    TIMEOUT,
    RATE_LIMIT,
    AUTH_FAILURE,
    MALFORMED_RESPONSE,
    PROVIDER_UNAVAILABLE,
    CANCELLED,
}
pub struct MockProvider;
impl MockProvider {
    pub async fn execute(
        &self,
        _: ModelRequest,
        mode: MockMode,
        timeout: Duration,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<ModelResponse, ProviderError> {
        if *cancel.borrow() {
            return Err(ProviderError::new(
                ProviderErrorKind::Cancelled,
                "cancelled",
            ));
        }
        if mode == MockMode::DELAYED_SUCCESS {
            tokio::select! { biased;
                Ok(()) = cancel.changed() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled")),
                _ = tokio::time::sleep(timeout) => return Err(ProviderError::new(ProviderErrorKind::Timeout, "timed out")),
                _ = tokio::time::sleep(Duration::from_millis(30)) => {}
            }
        }
        let response = ModelResponse {
            text: Some("hello".into()),
            structured: None,
            usage: Some(ModelUsage {
                input_tokens: Some(10),
                output_tokens: Some(5),
                total_tokens: Some(15),
            }),
            request_id: None,
        };
        match mode {
            MockMode::SUCCESS_TEXT | MockMode::DELAYED_SUCCESS | MockMode::STREAMING_SUCCESS => {
                Ok(response)
            }
            MockMode::SUCCESS_STRUCTURED => Ok(ModelResponse {
                text: None,
                structured: Some(serde_json::json!({"ok": true})),
                ..response
            }),
            other => Err(ProviderError::new(
                match other {
                    MockMode::TIMEOUT => ProviderErrorKind::Timeout,
                    MockMode::RATE_LIMIT => ProviderErrorKind::RateLimited,
                    MockMode::AUTH_FAILURE => ProviderErrorKind::Authentication,
                    MockMode::MALFORMED_RESPONSE => ProviderErrorKind::MalformedResponse,
                    MockMode::PROVIDER_UNAVAILABLE => ProviderErrorKind::Unavailable,
                    _ => ProviderErrorKind::Cancelled,
                },
                "mock failure",
            )),
        }
    }
    pub fn stream(
        &self,
        request: ModelRequest,
        mode: MockMode,
        timeout: Duration,
        cancel: watch::Receiver<bool>,
    ) -> ModelEventStream<'_> {
        Box::pin(
            futures_util::stream::once(async move {
                self.execute(request, mode, timeout, cancel).await
            })
            .flat_map(|result| match result {
                Ok(response) => {
                    let text = response.text.clone().unwrap_or_default();
                    futures_util::stream::iter(vec![
                        Ok(ModelStreamEvent::TextDelta(text)),
                        Ok(ModelStreamEvent::Done(response)),
                    ])
                }
                Err(e) => futures_util::stream::iter(vec![Err(e)]),
            }),
        )
    }
}

const MAX_BODY: usize = 1024 * 1024;
const MAX_EVENT: usize = 64 * 1024;
#[derive(Debug)]
pub struct OpenAiCompatibleProvider {
    endpoint: url::Url,
    client: reqwest::Client,
}
impl OpenAiCompatibleProvider {
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self, ProviderError> {
        let endpoint = url::Url::parse(endpoint.as_ref()).map_err(|_| {
            ProviderError::new(ProviderErrorKind::InvalidRequest, "invalid endpoint")
        })?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                "invalid endpoint",
            ));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    "client configuration failed",
                )
            })?;
        Ok(Self { endpoint, client })
    }
    async fn send(
        &self,
        request: &ModelRequest,
        auth: &str,
        timeout: Duration,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<reqwest::Response, ProviderError> {
        if request.model.trim().is_empty()
            || request.messages.is_empty()
            || auth.contains(['\r', '\n'])
        {
            return Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                "invalid request",
            ));
        }
        if *cancel.borrow() {
            return Err(ProviderError::new(
                ProviderErrorKind::Cancelled,
                "cancelled",
            ));
        }
        let body = WireRequest {
            model: &request.model,
            messages: &request.messages,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            stream: request.stream,
            stream_options: request.stream.then_some(StreamOptions {
                include_usage: true,
            }),
        };
        let sent = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(auth)
            .json(&body)
            .send();
        let response = tokio::select! { biased;
            _ = cancel.changed() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled")),
            _ = tokio::time::sleep(timeout) => return Err(ProviderError::new(ProviderErrorKind::Timeout, "timed out")),
            result = sent => result.map_err(network_error)?,
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let request_id = request_id(response.headers());
            let kind = match status {
                401 | 403 => ProviderErrorKind::Authentication,
                429 => ProviderErrorKind::RateLimited,
                500..=599 => ProviderErrorKind::Unavailable,
                _ => ProviderErrorKind::Http,
            };
            // Never echo provider error bodies: they can contain secrets or prompt content.
            return Err(ProviderError {
                kind,
                message: format!("HTTP {status}"),
                request_id,
                status: Some(status),
            });
        }
        Ok(response)
    }
}

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'a str,
    messages: &'a [ModelMessage],
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
}
#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}
#[derive(Deserialize)]
struct WireResponse {
    choices: Vec<WireChoice>,
    usage: Option<WireUsage>,
}
#[derive(Deserialize)]
struct WireChoice {
    message: Option<WireMessage>,
    delta: Option<WireMessage>,
}
#[derive(Deserialize)]
struct WireMessage {
    content: Option<String>,
}
#[derive(Deserialize)]
struct WireUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
}
impl From<WireUsage> for ModelUsage {
    fn from(u: WireUsage) -> Self {
        Self {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        }
    }
}
fn request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get("x-request-id")
        .or_else(|| headers.get("openai-request-id"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(256).collect())
}
fn network_error(e: reqwest::Error) -> ProviderError {
    ProviderError::new(
        if e.is_timeout() {
            ProviderErrorKind::Timeout
        } else {
            ProviderErrorKind::Unavailable
        },
        "provider transport failure",
    )
}
fn malformed() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::MalformedResponse,
        "malformed provider response",
    )
}
async fn bounded(
    mut response: reqwest::Response,
    max: usize,
    deadline: Instant,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Vec<u8>, ProviderError> {
    let mut bytes = Vec::new();
    loop {
        if *cancel.borrow() {
            return Err(ProviderError::new(
                ProviderErrorKind::Cancelled,
                "cancelled",
            ));
        }
        let chunk = tokio::select! { biased;
            _ = cancel.changed() => return Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled")),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(ProviderError::new(ProviderErrorKind::Timeout, "timed out")),
            chunk = response.chunk() => chunk.map_err(network_error)?,
        };
        match chunk {
            Some(chunk) if bytes.len() + chunk.len() <= max => bytes.extend_from_slice(&chunk),
            Some(_) => return Err(malformed()),
            None => return Ok(bytes),
        }
    }
}
impl Provider for OpenAiCompatibleProvider {
    async fn complete(
        &self,
        mut request: ModelRequest,
        auth: &str,
        timeout: Duration,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<ModelResponse, ProviderError> {
        request.stream = false;
        let deadline = Instant::now() + timeout;
        let response = self.send(&request, auth, timeout, cancel.clone()).await?;
        let id = request_id(response.headers());
        let bytes = bounded(response, MAX_BODY, deadline, &mut cancel).await?;
        let wire: WireResponse = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        let text = wire
            .choices
            .first()
            .and_then(|c| c.message.as_ref())
            .and_then(|m| m.content.clone())
            .ok_or_else(malformed)?;
        Ok(ModelResponse {
            text: Some(text),
            structured: None,
            usage: wire.usage.map(Into::into),
            request_id: id,
        })
    }
    fn stream<'a>(
        &'a self,
        mut request: ModelRequest,
        auth: &'a str,
        timeout: Duration,
        cancel: watch::Receiver<bool>,
    ) -> ModelEventStream<'a> {
        request.stream = true;
        Box::pin(async_stream(request, self, auth, timeout, cancel))
    }
}
fn async_stream<'a>(
    request: ModelRequest,
    provider: &'a OpenAiCompatibleProvider,
    auth: &'a str,
    timeout: Duration,
    cancel: watch::Receiver<bool>,
) -> impl Stream<Item = Result<ModelStreamEvent, ProviderError>> + Send + 'a {
    // unfold keeps network reads lazy and avoids an extra spawned task/channel.
    futures_util::stream::unfold(StreamState::Start(Some((request, provider, auth, timeout, cancel))), |state| async move {
        match state {
            StreamState::Start(Some((request, provider, auth, timeout, cancel))) => {
                let deadline = Instant::now() + timeout;
                match provider.send(&request, auth, timeout, cancel.clone()).await {
                    Ok(response) => {
                        let id = request_id(response.headers());
                        Some((Ok(ModelStreamEvent::TextDelta(String::new())), StreamState::Reading { response, buffer: Vec::new(), total_bytes: 0, text: String::new(), usage: None, id, deadline, cancel }))
                    }
                    Err(e) => Some((Err(e), StreamState::End)),
                }
            }
            StreamState::Reading { mut response, mut buffer, mut total_bytes, mut text, mut usage, mut id, deadline, mut cancel } => {
                if id.is_none() { id = request_id(response.headers()); }
                loop {
                    if let Some((end, delimiter_len)) = buffer
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .map(|end| (end, 4))
                        .or_else(|| buffer.windows(2).position(|w| w == b"\n\n").map(|end| (end, 2)))
                    {
                        let frame: Vec<_> = buffer.drain(..end + delimiter_len).collect();
                        if let Ok(frame) = std::str::from_utf8(&frame) {
                            let data = frame.lines().filter_map(|line| line.strip_prefix("data:")).map(str::trim).collect::<Vec<_>>().join("\n");
                            if data.is_empty() { continue; }
                            if data == "[DONE]" {
                                return Some((Ok(ModelStreamEvent::Done(ModelResponse { text: Some(text), structured: None, usage, request_id: id })), StreamState::End));
                            }
                            let parsed: Result<WireResponse, _> = serde_json::from_str(&data);
                            match parsed {
                                Ok(wire) => {
                                    if let Some(u) = wire.usage { usage = Some(u.into()); return Some((Ok(ModelStreamEvent::Usage(usage.clone().unwrap())), StreamState::Reading { response, buffer, total_bytes, text, usage, id, deadline, cancel })); }
                                    if let Some(delta) = wire.choices.first().and_then(|c| c.delta.as_ref()).and_then(|d| d.content.as_ref()) { text.push_str(delta); return Some((Ok(ModelStreamEvent::TextDelta(delta.clone())), StreamState::Reading { response, buffer, total_bytes, text, usage, id, deadline, cancel })); }
                                }
                                Err(_) => return Some((Err(malformed()), StreamState::End)),
                            }
                        } else { return Some((Err(malformed()), StreamState::End)); }
                        continue;
                    }
                    if buffer.len() > MAX_EVENT { return Some((Err(malformed()), StreamState::End)); }
                    if *cancel.borrow() { return Some((Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled")), StreamState::End)); }
                    let chunk = tokio::select! { biased;
                        _ = cancel.changed() => return Some((Err(ProviderError::new(ProviderErrorKind::Cancelled, "cancelled")), StreamState::End)),
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Some((Err(ProviderError::new(ProviderErrorKind::Timeout, "timed out")), StreamState::End)),
                        chunk = response.chunk() => chunk,
                    };
                    match chunk {
                        Ok(Some(chunk)) => {
                            total_bytes += chunk.len();
                            if total_bytes > MAX_BODY {
                                return Some((Err(malformed()), StreamState::End));
                            }
                            buffer.extend_from_slice(&chunk);
                        }
                        Ok(None) => return Some((Err(malformed()), StreamState::End)),
                        Err(e) => return Some((Err(network_error(e)), StreamState::End)),
                    }
                }
            }
            StreamState::End | StreamState::Start(None) => None,
        }
    }).filter(|event| futures_util::future::ready(!matches!(event, Ok(ModelStreamEvent::TextDelta(s)) if s.is_empty())))
}
enum StreamState<'a> {
    Start(
        Option<(
            ModelRequest,
            &'a OpenAiCompatibleProvider,
            &'a str,
            Duration,
            watch::Receiver<bool>,
        )>,
    ),
    Reading {
        response: reqwest::Response,
        buffer: Vec<u8>,
        total_bytes: usize,
        text: String,
        usage: Option<ModelUsage>,
        id: Option<String>,
        deadline: Instant,
        cancel: watch::Receiver<bool>,
    },
    End,
}

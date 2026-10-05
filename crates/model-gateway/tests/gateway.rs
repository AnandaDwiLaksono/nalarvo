use axum::{
    Router,
    body::Body,
    http::{Response, StatusCode, header},
    routing::post,
};
use futures_util::StreamExt;
use nalarvo_model_gateway::{
    MockMode, MockProvider, ModelMessage, ModelRequest, ModelStreamEvent, ModelUsage,
    OpenAiCompatibleProvider, Provider, ProviderErrorKind,
};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::watch};

fn request(stream: bool) -> ModelRequest {
    ModelRequest {
        model: "test-model".into(),
        messages: vec![ModelMessage::user("hello")],
        temperature: None,
        max_tokens: Some(64),
        stream,
    }
}

async fn server(app: Router) -> (String, tokio::task::JoinHandle<()>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/v1/chat/completions"), task, addr)
}

#[tokio::test]
async fn mock_modes_are_deterministic() {
    let provider = MockProvider;
    for (mode, expected) in [
        (MockMode::SUCCESS_TEXT, "hello"),
        (MockMode::DELAYED_SUCCESS, "hello"),
    ] {
        let response = provider
            .execute(
                request(false),
                mode,
                Duration::from_secs(1),
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(response.text.as_deref(), Some(expected));
    }
    let structured = provider
        .execute(
            request(false),
            MockMode::SUCCESS_STRUCTURED,
            Duration::from_secs(1),
            watch::channel(false).1,
        )
        .await
        .unwrap();
    assert!(structured.structured.is_some());
    for mode in [
        MockMode::TIMEOUT,
        MockMode::RATE_LIMIT,
        MockMode::AUTH_FAILURE,
        MockMode::MALFORMED_RESPONSE,
        MockMode::PROVIDER_UNAVAILABLE,
        MockMode::CANCELLED,
    ] {
        assert!(
            provider
                .execute(
                    request(false),
                    mode,
                    Duration::from_millis(10),
                    watch::channel(false).1
                )
                .await
                .is_err()
        );
    }
    let mut stream = provider.stream(
        request(true),
        MockMode::STREAMING_SUCCESS,
        Duration::from_secs(1),
        watch::channel(false).1,
    );
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        ModelStreamEvent::TextDelta(_)
    ));
}

#[tokio::test]
async fn parses_openai_response_and_provider_metadata() {
    let app = Router::new().route("/v1/chat/completions", post(|| async {
        Response::builder().status(200).header("x-request-id", "req-123").body(Body::from(r#"{"choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}"#)).unwrap()
    }));
    let (url, task, _) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let out = p
        .complete(
            request(false),
            "secret",
            Duration::from_secs(2),
            watch::channel(false).1,
        )
        .await
        .unwrap();
    assert_eq!(out.text.as_deref(), Some("hello"));
    assert_eq!(out.request_id.as_deref(), Some("req-123"));
    assert_eq!(
        out.usage,
        Some(ModelUsage {
            input_tokens: Some(2),
            output_tokens: Some(3),
            total_tokens: Some(5)
        })
    );
    task.abort();
}

#[tokio::test]
async fn classifies_http_errors_and_malformed_json() {
    for (status, expected) in [
        (401, ProviderErrorKind::Authentication),
        (429, ProviderErrorKind::RateLimited),
        (500, ProviderErrorKind::Unavailable),
    ] {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(
                move || async move { (StatusCode::from_u16(status).unwrap(), "provider says no") },
            ),
        );
        let (url, task, _) = server(app).await;
        let p = OpenAiCompatibleProvider::new(url).unwrap();
        assert_eq!(
            p.complete(
                request(false),
                "secret",
                Duration::from_secs(2),
                watch::channel(false).1
            )
            .await
            .unwrap_err()
            .kind,
            expected
        );
        task.abort();
    }
    let app = Router::new().route("/v1/chat/completions", post(|| async { "not-json" }));
    let (url, task, _) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    assert_eq!(
        p.complete(
            request(false),
            "secret",
            Duration::from_secs(2),
            watch::channel(false).1
        )
        .await
        .unwrap_err()
        .kind,
        ProviderErrorKind::MalformedResponse
    );
    task.abort();
}

#[tokio::test]
async fn handles_streaming_sse() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Response::builder()
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
                ))
                .unwrap()
        }),
    );
    let (url, task, _) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let events: Vec<_> = p
        .stream(
            request(true),
            "secret",
            Duration::from_secs(2),
            watch::channel(false).1,
        )
        .collect()
        .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Ok(ModelStreamEvent::TextDelta(s)) if s == "hi"))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Ok(ModelStreamEvent::Done(_))))
    );
    task.abort();
}

#[tokio::test]
async fn timeout_refusal_redirect_and_cancellation_are_safe() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            "{}"
        }),
    );
    let (url, task, _) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    assert_eq!(
        p.complete(
            request(false),
            "secret",
            Duration::from_millis(20),
            watch::channel(false).1
        )
        .await
        .unwrap_err()
        .kind,
        ProviderErrorKind::Timeout
    );
    task.abort();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let p = OpenAiCompatibleProvider::new(format!("http://{addr}/v1/chat/completions")).unwrap();
    assert_eq!(
        p.complete(
            request(false),
            "secret",
            Duration::from_secs(5),
            watch::channel(false).1
        )
        .await
        .unwrap_err()
        .kind,
        ProviderErrorKind::Unavailable
    );
    let (tx, rx) = watch::channel(false);
    tx.send(true).unwrap();
    assert_eq!(
        p.complete(request(false), "secret", Duration::from_secs(1), rx)
            .await
            .unwrap_err()
            .kind,
        ProviderErrorKind::Cancelled
    );
}

#[tokio::test]
async fn redirect_does_not_forward_credentials() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static SERVER_A_COUNT: AtomicUsize = AtomicUsize::new(0);
    static SERVER_B_COUNT: AtomicUsize = AtomicUsize::new(0);
    SERVER_A_COUNT.store(0, Ordering::SeqCst);
    SERVER_B_COUNT.store(0, Ordering::SeqCst);

    let (target_tx, mut target_rx) = tokio::sync::mpsc::channel::<Option<String>>(1);
    let target = Router::new().route(
        "/sink",
        post(move |headers: axum::http::HeaderMap| {
            SERVER_B_COUNT.fetch_add(1, Ordering::SeqCst);
            let tx = target_tx.clone();
            async move {
                let _ = tx
                    .send(
                        headers
                            .get(header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned),
                    )
                    .await;
                "{}"
            }
        }),
    );
    let (target_url, target_task, _) = server(target).await;
    let redirect = target_url.replace("/v1/chat/completions", "/sink");
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || async move {
            SERVER_A_COUNT.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .status(307)
                .header(header::LOCATION, redirect)
                .body(Body::empty())
                .unwrap()
        }),
    );
    let (url, server_task, _) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let err = p
        .complete(
            request(false),
            "secret",
            Duration::from_secs(2),
            watch::channel(false).1,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err.kind,
        ProviderErrorKind::Http | ProviderErrorKind::Unavailable
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), target_rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        SERVER_A_COUNT.load(Ordering::SeqCst),
        1,
        "Server A must receive exactly 1 request"
    );
    assert_eq!(
        SERVER_B_COUNT.load(Ordering::SeqCst),
        0,
        "Server B must receive 0 requests (no-follow redirect)"
    );
    server_task.abort();
    target_task.abort();
}

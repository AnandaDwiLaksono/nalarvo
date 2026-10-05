use axum::{
    Router,
    body::Body,
    http::{Response, header},
    routing::post,
};
use futures_util::StreamExt;
use nalarvo_model_gateway::{
    MockMode, MockProvider, ModelMessage, ModelRequest, ModelStreamEvent, OpenAiCompatibleProvider,
    Provider, ProviderErrorKind,
};
use std::time::Duration;
use tokio::{net::TcpListener, sync::watch};

fn request() -> ModelRequest {
    ModelRequest {
        model: "m".into(),
        messages: vec![ModelMessage::user("hi")],
        temperature: None,
        max_tokens: None,
        stream: false,
    }
}
async fn server(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    (
        url,
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    )
}
#[tokio::test]
async fn mock_stream_error_has_no_successful_done() {
    let events: Vec<_> = MockProvider
        .stream(
            request(),
            MockMode::AUTH_FAILURE,
            Duration::from_secs(1),
            watch::channel(false).1,
        )
        .collect()
        .await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].as_ref().unwrap_err().kind,
        ProviderErrorKind::Authentication
    );
}
#[tokio::test]
async fn streaming_usage_and_request_id_survive_done() {
    let app = Router::new().route("/v1/chat/completions", post(|| async {
        Response::builder().header(header::CONTENT_TYPE, "text/event-stream").header("x-request-id", "r42").body(Body::from("data: {\"choices\":[{\"delta\":{\"content\":\"abc\"}}]}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":5,\"total_tokens\":9}}\r\n\r\ndata: [DONE]\r\n\r\n")).unwrap()
    }));
    let (url, task) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let events: Vec<_> = p
        .stream(
            request(),
            "test",
            Duration::from_secs(2),
            watch::channel(false).1,
        )
        .collect()
        .await;
    assert!(
        matches!(events.last().unwrap(), Ok(ModelStreamEvent::Done(response)) if response.text.as_deref() == Some("abc") && response.request_id.as_deref() == Some("r42") && response.usage.as_ref().unwrap().total_tokens == Some(9))
    );
    task.abort();
}
#[tokio::test]
async fn cancellation_while_response_pending_exits_promptly() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            "{}"
        }),
    );
    let (url, task) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let (tx, rx) = watch::channel(false);
    let work = p.complete(request(), "test", Duration::from_secs(10), rx);
    let trigger = async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(work, trigger)
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind, ProviderErrorKind::Cancelled);
    task.abort();
}
#[tokio::test]
async fn cumulative_stream_output_is_bounded() {
    let payload = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{}\"}}}}]}}\n\n",
        "x".repeat(1024)
    );
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let body = payload.repeat(1100);
            async move {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from(body))
                    .unwrap()
            }
        }),
    );
    let (url, task) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    let events: Vec<_> = p
        .stream(
            request(),
            "test",
            Duration::from_secs(30),
            watch::channel(false).1,
        )
        .collect()
        .await;
    println!("events len={}, last={:?}", events.len(), events.last());
    assert!(
        matches!(events.last().unwrap(), Err(e) if e.kind == ProviderErrorKind::MalformedResponse)
    );
    task.abort();
}

#[tokio::test]
async fn oversized_success_body_is_rejected() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async { "x".repeat(1_100_000) }),
    );
    let (url, task) = server(app).await;
    let p = OpenAiCompatibleProvider::new(url).unwrap();
    assert_eq!(
        p.complete(
            request(),
            "test",
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

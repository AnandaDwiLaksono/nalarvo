use axum::{
    Json, Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use nalarvo_contracts::HealthResponse;
use std::sync::Arc;

#[derive(Clone)]
struct ApiState {
    bearer: Arc<str>,
}

pub fn router(bearer: impl Into<Arc<str>>) -> Router {
    let state = ApiState {
        bearer: bearer.into(),
    };

    Router::new()
        .route("/api/v1/health", get(health))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        service: "nalarvo-core".into(),
    })
}

async fn authenticate(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let expected = format!("Bearer {}", state.bearer);
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if supplied != Some(expected.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

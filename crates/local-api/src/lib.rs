use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use nalarvo_application::{
    ApplicationContext, ApplicationError, CreateCompanyCommand, UpdateCompanyMetadataCommand,
};
use nalarvo_contracts::{
    CompanyDto, CompanyListResponse, CreateCompanyRequest, ErrorEnvelope, HealthResponse,
    PrincipalDto, ScopeDto, SseEventEnvelope, UpdateCompanyRequest, error_codes,
};
use nalarvo_domain::{Company, CompanyId, WorkspaceId};
use std::{convert::Infallible, sync::Arc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct ApiState {
    pub token: Arc<str>,
    pub app_ctx: Option<ApplicationContext>,
}

pub fn router(token: impl Into<Arc<str>>) -> Router {
    router_with_app(token, None)
}

pub fn router_with_app(token: impl Into<Arc<str>>, app_ctx: Option<ApplicationContext>) -> Router {
    let state = ApiState {
        token: token.into(),
        app_ctx,
    };

    Router::new()
        .route("/api/v1/health", get(health))
        .route(
            "/api/v1/workspaces/{workspace_id}/companies",
            post(create_company).get(list_companies),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/companies/{company_id}",
            get(get_company).put(update_company),
        )
        .route("/api/v1/events", get(events_sse))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_auth,
        ))
        .with_state(state)
}

async fn require_bearer_auth(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let expected = format!("Bearer {}", state.token);
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if supplied != Some(expected.as_str()) {
        let envelope = ErrorEnvelope::new(error_codes::UNAUTHORIZED, "Invalid or missing token");
        return (StatusCode::UNAUTHORIZED, Json(envelope)).into_response();
    }

    next.run(request).await
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        service: "nalarvo-core".into(),
    })
}

fn map_company(c: &Company) -> CompanyDto {
    CompanyDto {
        id: c.id.0.clone(),
        workspace_id: c.workspace_id.0.clone(),
        name: c.name.clone(),
        description: c.description.clone(),
        status: c.status.to_string(),
        row_version: c.row_version,
        created_at: c.created_at.to_rfc3339(),
        updated_at: c.updated_at.to_rfc3339(),
    }
}

fn map_app_error(err: ApplicationError) -> Response {
    match err {
        ApplicationError::Unauthorized => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorEnvelope::new(
                error_codes::UNAUTHORIZED,
                "Unauthorized",
            )),
        )
            .into_response(),
        ApplicationError::Validation(msg) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorEnvelope::new(error_codes::VALIDATION_FAILED, msg)),
        )
            .into_response(),
        ApplicationError::NotFound(id) => (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(
                error_codes::NOT_FOUND,
                format!("Resource not found: {id}"),
            )),
        )
            .into_response(),
        ApplicationError::StaleVersion { current, expected } => (
            StatusCode::CONFLICT,
            Json(ErrorEnvelope::with_details(
                error_codes::STALE_VERSION,
                format!("Stale version: expected {expected}, current {current}"),
                serde_json::json!({ "current": current, "expected": expected }),
            )),
        )
            .into_response(),
        ApplicationError::IdempotencyKeyReuseMismatch(key) => (
            StatusCode::CONFLICT,
            Json(ErrorEnvelope::new(
                error_codes::IDEMPOTENCY_KEY_REUSE_MISMATCH,
                format!("Idempotency key reuse mismatch for key: {key}"),
            )),
        )
            .into_response(),
        ApplicationError::ScopeViolation(msg) => (
            StatusCode::FORBIDDEN,
            Json(ErrorEnvelope::new(error_codes::FORBIDDEN, msg)),
        )
            .into_response(),
        ApplicationError::Persistence(_) | ApplicationError::Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorEnvelope::new(
                error_codes::INTERNAL_ERROR,
                "A database error occurred",
            )),
        )
            .into_response(),
        ApplicationError::Domain(d) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorEnvelope::new(
                error_codes::VALIDATION_FAILED,
                d.to_string(),
            )),
        )
            .into_response(),
    }
}

async fn create_company(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<CreateCompanyRequest>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorEnvelope::new(
                    error_codes::INTERNAL_ERROR,
                    "App context not initialized",
                )),
            )
                .into_response();
        }
    };

    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let causation_id = headers
        .get("x-causation-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let cmd = CreateCompanyCommand {
        workspace_id: WorkspaceId(workspace_id),
        name: payload.name,
        description: payload.description,
        principal: None,
        idempotency_key,
        correlation_id,
        causation_id,
    };

    match app_ctx.create_company(cmd).await {
        Ok(company) => (StatusCode::CREATED, Json(map_company(&company))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn list_companies(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorEnvelope::new(
                    error_codes::INTERNAL_ERROR,
                    "App context not initialized",
                )),
            )
                .into_response();
        }
    };

    match app_ctx.list_companies(&WorkspaceId(workspace_id)).await {
        Ok(list) => {
            let dtos = list.iter().map(map_company).collect();
            (
                StatusCode::OK,
                Json(CompanyListResponse { companies: dtos }),
            )
                .into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn get_company(
    State(state): State<ApiState>,
    Path((workspace_id, company_id)): Path<(String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorEnvelope::new(
                    error_codes::INTERNAL_ERROR,
                    "App context not initialized",
                )),
            )
                .into_response();
        }
    };

    match app_ctx
        .get_company(&WorkspaceId(workspace_id), &CompanyId(company_id))
        .await
    {
        Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn update_company(
    State(state): State<ApiState>,
    Path((workspace_id, company_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<UpdateCompanyRequest>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorEnvelope::new(
                    error_codes::INTERNAL_ERROR,
                    "App context not initialized",
                )),
            )
                .into_response();
        }
    };

    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let causation_id = headers
        .get("x-causation-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let cmd = UpdateCompanyMetadataCommand {
        workspace_id: WorkspaceId(workspace_id),
        company_id: CompanyId(company_id),
        name: payload.name,
        description: payload.description,
        expected_version: payload.expected_version,
        principal: None,
        idempotency_key,
        correlation_id,
        causation_id,
    };

    match app_ctx.update_company_metadata(cmd).await {
        Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn events_sse(State(state): State<ApiState>) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorEnvelope::new(
                    error_codes::INTERNAL_ERROR,
                    "App context not initialized",
                )),
            )
                .into_response();
        }
    };

    let rx = app_ctx.subscribe_events();
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(event) => {
            let sse_envelope = SseEventEnvelope {
                event_id: event.event_id,
                event_type: event.event_type.clone(),
                schema_version: event.schema_version,
                company_id: event.company_id.0,
                principal: PrincipalDto {
                    principal_type: event.principal.principal_type.to_string(),
                    principal_id: event.principal.principal_id,
                },
                scope: ScopeDto {
                    scope_type: event.scope.scope_type.to_string(),
                    scope_id: event.scope.scope_id,
                },
                occurred_at: event.occurred_at.to_rfc3339(),
                payload: event.payload,
            };
            let json = serde_json::to_string(&sse_envelope).ok()?;
            Some(Ok::<_, Infallible>(
                Event::default().event(&event.event_type).data(json),
            ))
        }
        Err(_) => None,
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

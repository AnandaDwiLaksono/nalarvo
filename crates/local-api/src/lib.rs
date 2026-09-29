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
    AgentRecord, ApplicationContext, ApplicationError, CreateCompanyCommand, CredentialRefRecord,
    DepartmentRecord, ProviderConnectionRecord, RoleRecord, UpdateCompanyMetadataCommand,
};
use nalarvo_contracts::{
    AgentAvailabilityResponse, AgentDto, AgentListResponse, CompanyDto, CompanyLifecycleRequest,
    CompanyListResponse, CreateAgentRequest, CreateCompanyRequest, CreateDepartmentRequest,
    CreateProviderConnectionRequest, CreateRoleRequest, CredentialListResponse, CredentialRefDto,
    DepartmentDto, DepartmentListResponse, ErrorEnvelope, HealthResponse, PrincipalDto,
    ProviderConnectionDto, ProviderLifecycleRequest, ProviderListResponse, ProviderTestResponse,
    RoleDto, RoleListResponse, ScopeDto, SseEventEnvelope, SubmitCredentialRequest,
    UpdateAgentRequest, UpdateCompanyRequest, WorkforceLifecycleRequest, error_codes,
};
use nalarvo_domain::{Company, CompanyId, PrincipalRef, WorkspaceId};
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
        .route("/api/v1/workspace", get(get_workspace))
        .route(
            "/api/v1/workspace/credentials",
            get(list_credentials).post(submit_credential),
        )
        .route(
            "/api/v1/workspace/credentials/{id}",
            post(handle_credential_action),
        )
        .route(
            "/api/v1/workspace/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/api/v1/workspace/providers/{id}",
            post(handle_provider_action),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/companies",
            post(create_company).get(list_companies),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/companies/{company_id}",
            get(get_company).put(update_company),
        )
        .route("/api/v1/companies/{action}", post(handle_company_action))
        .route(
            "/api/v1/companies/{company_id}/departments",
            get(list_departments).post(create_department),
        )
        .route(
            "/api/v1/companies/{company_id}/departments/{department_id}",
            get(get_department)
                .patch(update_department)
                .post(handle_department_action),
        )
        .route(
            "/api/v1/companies/{company_id}/roles",
            get(list_roles).post(create_role),
        )
        .route(
            "/api/v1/companies/{company_id}/roles/{role_id}",
            get(get_role).patch(update_role),
        )
        .route(
            "/api/v1/companies/{company_id}/agents",
            get(list_agents).post(create_agent),
        )
        .route(
            "/api/v1/companies/{company_id}/agents/{agent_id}",
            get(get_agent).patch(update_agent).post(handle_agent_action),
        )
        .route(
            "/api/v1/companies/{company_id}/agents/{agent_id}/availability",
            get(agent_availability),
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

fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorEnvelope::new(
            error_codes::INTERNAL_ERROR,
            "Core unavailable",
        )),
    )
        .into_response()
}

fn default_workspace_id() -> WorkspaceId {
    WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into())
}

fn default_principal() -> PrincipalRef {
    PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001")
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        service: "nalarvo-core".into(),
    })
}

async fn get_workspace(State(state): State<ApiState>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let principal = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let workspace_id = default_workspace_id();
    match ctx.get_workspace(&principal, &workspace_id).await {
        Ok(workspace) => Json(nalarvo_contracts::WorkspaceDto {
            id: workspace.id,
            owner_user_id: workspace.owner_user_id,
            name: workspace.name,
            slug: workspace.slug,
            is_personal: true,
            status: workspace.status,
            row_version: workspace.row_version,
            created_at: workspace.created_at,
            updated_at: workspace.updated_at,
        })
        .into_response(),
        Err(err) => map_app_error(err),
    }
}

fn map_credential(r: CredentialRefRecord) -> CredentialRefDto {
    CredentialRefDto {
        id: r.id,
        workspace_id: r.workspace_id,
        name: r.name,
        status: r.status,
        row_version: r.row_version,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

fn map_provider(r: ProviderConnectionRecord) -> ProviderConnectionDto {
    ProviderConnectionDto {
        id: r.id,
        workspace_id: r.workspace_id,
        name: r.name,
        provider_kind: r.provider_kind,
        credential_ref_id: r.credential_ref_id,
        status: r.status,
        health: r.health,
        row_version: r.row_version,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

fn map_department(r: DepartmentRecord) -> DepartmentDto {
    DepartmentDto {
        id: r.id,
        company_id: r.company_id,
        name: r.name,
        status: r.status,
        row_version: r.row_version,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

fn map_role(r: RoleRecord) -> RoleDto {
    RoleDto {
        id: r.id,
        company_id: r.company_id,
        name: r.name,
        status: r.status,
        row_version: r.row_version,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

fn map_agent(r: AgentRecord) -> AgentDto {
    AgentDto {
        id: r.id,
        company_id: r.company_id,
        name: r.name,
        primary_department_id: r.primary_department_id,
        role_id: r.role_id,
        model_profile_id: r.model_profile_id,
        capacity: r.capacity,
        status: r.status,
        row_version: r.row_version,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

async fn list_credentials(State(state): State<ApiState>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_credentials(&default_workspace_id()).await {
        Ok(list) => (
            StatusCode::OK,
            Json(CredentialListResponse {
                credentials: list.into_iter().map(map_credential).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn submit_credential(
    State(state): State<ApiState>,
    Json(payload): Json<SubmitCredentialRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .submit_credential(&default_workspace_id(), &payload.name, &payload.secret)
        .await
    {
        Ok(r) => (StatusCode::CREATED, Json(map_credential(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_credential_action(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    if let Some(clean_id) = id.strip_suffix(":disable") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<CompanyLifecycleRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .disable_credential(&default_workspace_id(), clean_id, payload.expected_version)
            .await
        {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = id.strip_suffix(":revoke") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<CompanyLifecycleRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .revoke_credential(&default_workspace_id(), clean_id, payload.expected_version)
            .await
        {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(err) => map_app_error(err),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
        )
            .into_response()
    }
}

async fn list_providers(State(state): State<ApiState>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_providers(&default_workspace_id()).await {
        Ok(list) => (
            StatusCode::OK,
            Json(ProviderListResponse {
                providers: list.into_iter().map(map_provider).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn create_provider(
    State(state): State<ApiState>,
    Json(payload): Json<CreateProviderConnectionRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .create_provider(
            &default_workspace_id(),
            &payload.name,
            &payload.provider_kind,
            payload.credential_ref_id.as_deref(),
        )
        .await
    {
        Ok(r) => (StatusCode::CREATED, Json(map_provider(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_provider_action(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    if let Some(clean_id) = id.strip_suffix(":enable") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<ProviderLifecycleRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .enable_provider(&default_workspace_id(), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_provider(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = id.strip_suffix(":disable") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<ProviderLifecycleRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .disable_provider(&default_workspace_id(), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_provider(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = id.strip_suffix(":test") {
        match ctx
            .test_provider_health(&default_workspace_id(), clean_id)
            .await
        {
            Ok(provider) => {
                let dto = map_provider(provider);
                let healthy = dto.health == "HEALTHY";
                (
                    StatusCode::OK,
                    Json(ProviderTestResponse {
                        provider: dto,
                        healthy,
                        message: None,
                    }),
                )
                    .into_response()
            }
            Err(err) => map_app_error(err),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
        )
            .into_response()
    }
}

fn map_company(c: &Company) -> CompanyDto {
    CompanyDto {
        id: c.id.0.clone(),
        workspace_id: c.workspace_id.0.clone(),
        name: c.name.clone(),
        description: c.description.clone(),
        mission: c.mission.clone(),
        director_user_id: c.director_user_id.as_ref().map(|id| id.0.clone()),
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
        None => return service_unavailable(),
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
        None => return service_unavailable(),
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
        None => return service_unavailable(),
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
        None => return service_unavailable(),
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

async fn handle_company_action(
    State(state): State<ApiState>,
    Path(action): Path<String>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
        return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
    };
    let Ok(payload) = serde_json::from_slice::<CompanyLifecycleRequest>(&body) else {
        return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
    };

    if let Some(company_id) = action.strip_suffix(":activate") {
        match ctx
            .activate_company(
                &default_workspace_id(),
                &CompanyId(company_id.to_string()),
                payload.expected_version,
                default_principal(),
            )
            .await
        {
            Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(company_id) = action.strip_suffix(":pause") {
        match ctx
            .pause_company(
                &default_workspace_id(),
                &CompanyId(company_id.to_string()),
                payload.expected_version,
                default_principal(),
            )
            .await
        {
            Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(company_id) = action.strip_suffix(":resume") {
        match ctx
            .resume_company(
                &default_workspace_id(),
                &CompanyId(company_id.to_string()),
                payload.expected_version,
                default_principal(),
            )
            .await
        {
            Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(company_id) = action.strip_suffix(":archive") {
        match ctx
            .archive_company(
                &default_workspace_id(),
                &CompanyId(company_id.to_string()),
                payload.expected_version,
                default_principal(),
            )
            .await
        {
            Ok(company) => (StatusCode::OK, Json(map_company(&company))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
        )
            .into_response()
    }
}

async fn list_departments(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_departments(&CompanyId(company_id)).await {
        Ok(list) => (
            StatusCode::OK,
            Json(DepartmentListResponse {
                departments: list.into_iter().map(map_department).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn create_department(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
    Json(payload): Json<CreateDepartmentRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .create_department(&CompanyId(company_id), &payload.name)
        .await
    {
        Ok(r) => (StatusCode::CREATED, Json(map_department(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_department(
    State(state): State<ApiState>,
    Path((company_id, department_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .get_department(&CompanyId(company_id), &department_id)
        .await
    {
        Ok(r) => (StatusCode::OK, Json(map_department(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

#[derive(serde::Deserialize)]
pub struct UpdateDepartmentPayload {
    pub name: String,
    pub expected_version: i64,
}

async fn update_department(
    State(state): State<ApiState>,
    Path((company_id, department_id)): Path<(String, String)>,
    Json(payload): Json<UpdateDepartmentPayload>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .update_department(
            &CompanyId(company_id),
            &department_id,
            &payload.name,
            payload.expected_version,
        )
        .await
    {
        Ok(r) => (StatusCode::OK, Json(map_department(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_department_action(
    State(state): State<ApiState>,
    Path((company_id, department_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    if let Some(clean_id) = department_id.strip_suffix(":retire") {
        match ctx
            .retire_department(&CompanyId(company_id), clean_id)
            .await
        {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(err) => map_app_error(err),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
        )
            .into_response()
    }
}

async fn list_roles(State(state): State<ApiState>, Path(company_id): Path<String>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_roles(&CompanyId(company_id)).await {
        Ok(list) => (
            StatusCode::OK,
            Json(RoleListResponse {
                roles: list.into_iter().map(map_role).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn create_role(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
    Json(payload): Json<CreateRoleRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.create_role(&CompanyId(company_id), &payload.name).await {
        Ok(r) => (StatusCode::CREATED, Json(map_role(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_role(
    State(state): State<ApiState>,
    Path((company_id, role_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.get_role(&CompanyId(company_id), &role_id).await {
        Ok(r) => (StatusCode::OK, Json(map_role(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

#[derive(serde::Deserialize)]
pub struct UpdateRolePayload {
    pub name: String,
    pub expected_version: i64,
}

async fn update_role(
    State(state): State<ApiState>,
    Path((company_id, role_id)): Path<(String, String)>,
    Json(payload): Json<UpdateRolePayload>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .update_role(
            &CompanyId(company_id),
            &role_id,
            &payload.name,
            payload.expected_version,
        )
        .await
    {
        Ok(r) => (StatusCode::OK, Json(map_role(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn list_agents(State(state): State<ApiState>, Path(company_id): Path<String>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_agents(&CompanyId(company_id)).await {
        Ok(list) => (
            StatusCode::OK,
            Json(AgentListResponse {
                agents: list.into_iter().map(map_agent).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn create_agent(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
    Json(payload): Json<CreateAgentRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .create_agent(
            &CompanyId(company_id),
            &payload.name,
            &payload.primary_department_id,
            &payload.role_id,
            payload.model_profile_id.as_deref(),
            payload.capacity,
        )
        .await
    {
        Ok(r) => (StatusCode::CREATED, Json(map_agent(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_agent(
    State(state): State<ApiState>,
    Path((company_id, agent_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.get_agent(&CompanyId(company_id), &agent_id).await {
        Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn update_agent(
    State(state): State<ApiState>,
    Path((company_id, agent_id)): Path<(String, String)>,
    Json(payload): Json<UpdateAgentRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .update_agent(
            &CompanyId(company_id),
            &agent_id,
            &payload.name,
            &payload.primary_department_id,
            &payload.role_id,
            payload.model_profile_id.as_deref(),
            payload.capacity,
            payload.expected_version,
        )
        .await
    {
        Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_agent_action(
    State(state): State<ApiState>,
    Path((company_id, agent_id)): Path<(String, String)>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
        return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
    };
    let Ok(payload) = serde_json::from_slice::<WorkforceLifecycleRequest>(&body) else {
        return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
    };

    if let Some(clean_id) = agent_id.strip_suffix(":activate") {
        match ctx
            .activate_agent(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = agent_id.strip_suffix(":pause") {
        match ctx
            .pause_agent(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = agent_id.strip_suffix(":resume") {
        match ctx
            .resume_agent(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = agent_id.strip_suffix(":retire") {
        match ctx
            .retire_agent(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(r) => (StatusCode::OK, Json(map_agent(r))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
        )
            .into_response()
    }
}

async fn agent_availability(
    State(state): State<ApiState>,
    Path((company_id, agent_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .get_agent_availability(&CompanyId(company_id), &agent_id)
        .await
    {
        Ok(availability) => (
            StatusCode::OK,
            Json(AgentAvailabilityResponse { availability }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn events_sse(State(state): State<ApiState>) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
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

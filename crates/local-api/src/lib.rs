use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post, put},
};
use nalarvo_application::{
    AgentRecord, ApplicationContext, ApplicationError, CommandMeta, CreateAgentAllocationCommand,
    CreateCompanyCommand, CreateObjectiveCommand, CreateStaffingRequirementCommand,
    CreateTeamCommand, CreateWorkAssignmentCommand, CreateWorkDependencyCommand,
    CreateWorkItemCommand, CredentialRefRecord, DepartmentRecord, ProviderConnectionRecord,
    RoleRecord, UpdateCompanyMetadataCommand,
};
use nalarvo_contracts::{
    AgentAllocationDto, AgentAllocationListResponse, AgentAvailabilityResponse, AgentDto,
    AgentListResponse, AllocationLifecycleRequest, AssignmentLifecycleRequest,
    BindProjectWorkingRootRequest, CompanyDto, CompanyLifecycleRequest, CompanyListResponse,
    CreateAgentAllocationRequest, CreateAgentRequest, CreateCompanyRequest,
    CreateDepartmentRequest, CreateObjectiveRequest, CreateProjectRequest,
    CreateProviderConnectionRequest, CreateRoleRequest, CreateStaffingRequirementRequest,
    CreateTeamRequest, CreateWorkAssignmentRequest, CreateWorkDependencyRequest,
    CreateWorkItemRequest, CredentialListResponse, CredentialRefDto, DepartmentDto,
    DepartmentListResponse, ErrorEnvelope, HealthResponse, ObjectiveDto, ObjectiveLifecycleRequest,
    ObjectiveListResponse, PrincipalDto, ProjectDto, ProjectLifecycleRequest, ProjectListResponse,
    ProviderConnectionDto, ProviderLifecycleRequest, ProviderListResponse, ProviderTestResponse,
    RoleDto, RoleListResponse, ScopeDto, SseEventEnvelope, StaffingLifecycleRequest,
    StaffingRequirementDto, StaffingRequirementListResponse, SubmitCredentialRequest, TeamDto,
    TeamLifecycleRequest, TeamListResponse, UnbindProjectWorkingRootRequest, UpdateAgentRequest,
    UpdateCompanyRequest, WorkAssignmentDto, WorkAssignmentListResponse, WorkDependencyDto,
    WorkDependencyListResponse, WorkItemDto, WorkItemLifecycleRequest, WorkListResponse,
    WorkforceLifecycleRequest, error_codes,
};
use nalarvo_domain::{
    AgentAllocationStatus, Company, CompanyId, DependencyType, ObjectiveStatus, PrincipalRef,
    StaffingRequirementStatus, TeamStatus, WorkItemStatus, WorkItemType, WorkspaceId,
};

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
        .route(
            "/api/v1/companies/{company_id}/projects",
            get(list_projects).post(create_project),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}",
            get(get_project).post(handle_project_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/working-root",
            put(bind_project_working_root_handler).delete(unbind_project_working_root_handler),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work",
            get(list_project_work_items).post(create_work_item),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}",
            get(get_project_work_item).post(handle_work_item_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/objectives",
            get(list_objectives).post(create_objective),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/objectives/{objective_id}",
            get(get_objective).post(handle_objective_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/teams",
            get(list_teams).post(create_team),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/teams/{team_id}",
            get(get_team).post(handle_team_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/staffing-requirements",
            get(list_staffing_requirements).post(create_staffing_requirement),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/staffing-requirements/{requirement_id}",
            get(get_staffing_requirement).post(handle_staffing_requirement_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/allocations",
            get(list_agent_allocations).post(create_agent_allocation),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/allocations/{allocation_id}",
            get(get_agent_allocation).post(handle_agent_allocation_action),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/dependencies",
            get(list_work_dependencies).post(create_work_dependency),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/dependencies/{dependency_id}",
            get(get_work_dependency).delete(delete_work_dependency),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/assignments",
            get(list_work_assignments).post(create_work_assignment),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/assignments/{assignment_id}",
            get(get_work_assignment).post(handle_work_assignment_action),
        )
        .route("/api/v1/companies/{company_id}/work", get(list_work_items))
        .route(
            "/api/v1/companies/{company_id}/work/{work_id}",
            get(get_work_item),
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
        ApplicationError::SecretStore(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorEnvelope::new(
                error_codes::INTERNAL_ERROR,
                "A secret store error occurred",
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

fn map_project(p: nalarvo_domain::Project) -> ProjectDto {
    ProjectDto {
        id: p.id,
        company_id: p.company_id.0,
        name: p.name,
        description: p.description,
        priority: "NORMAL".into(),
        owner_user_id: None,
        target_outcome: None,
        target_date: None,
        working_root_path: p.working_root_path,
        working_root_bound_at: p.working_root_bound_at.map(|v| v.to_rfc3339()),
        status: p.status.to_string(),
        row_version: p.row_version,
        created_at: p.created_at.to_rfc3339(),
        updated_at: p.updated_at.to_rfc3339(),
    }
}

async fn list_projects(State(state): State<ApiState>, Path(company_id): Path<String>) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.list_projects(&CompanyId(company_id)).await {
        Ok(list) => (
            StatusCode::OK,
            Json(ProjectListResponse {
                projects: list.into_iter().map(map_project).collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn create_project(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
    Json(payload): Json<CreateProjectRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .create_project(&CompanyId(company_id), payload.name, payload.description)
        .await
    {
        Ok(p) => (StatusCode::CREATED, Json(map_project(p))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_project(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.get_project(&CompanyId(company_id), &project_id).await {
        Ok(p) => (StatusCode::OK, Json(map_project(p))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn bind_project_working_root_handler(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
    Json(req): Json<BindProjectWorkingRootRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .bind_project_working_root(
            &CompanyId(company_id),
            &project_id,
            req.path,
            req.expected_version,
        )
        .await
    {
        Ok(project) => (StatusCode::OK, Json(map_project(project))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn unbind_project_working_root_handler(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
    Json(req): Json<UnbindProjectWorkingRootRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .unbind_project_working_root(&CompanyId(company_id), &project_id, req.expected_version)
        .await
    {
        Ok(project) => (StatusCode::OK, Json(map_project(project))).into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_project_action(
    State(state): State<ApiState>,
    Path((company_id, id)): Path<(String, String)>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    if let Some(clean_id) = id.strip_suffix(":activate") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<ProjectLifecycleRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .activate_project(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(p) => (StatusCode::OK, Json(map_project(p))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = id.strip_suffix(":bind-working-root") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<BindProjectWorkingRootRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .bind_project_working_root(
                &CompanyId(company_id),
                clean_id,
                payload.path,
                payload.expected_version,
            )
            .await
        {
            Ok(p) => (StatusCode::OK, Json(map_project(p))).into_response(),
            Err(err) => map_app_error(err),
        }
    } else if let Some(clean_id) = id.strip_suffix(":unbind-working-root") {
        let Ok(body) = axum::body::to_bytes(req.into_body(), 1024 * 64).await else {
            return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
        };
        let Ok(payload) = serde_json::from_slice::<UnbindProjectWorkingRootRequest>(&body) else {
            return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
        };
        match ctx
            .unbind_project_working_root(&CompanyId(company_id), clean_id, payload.expected_version)
            .await
        {
            Ok(p) => (StatusCode::OK, Json(map_project(p))).into_response(),
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

async fn list_project_work_items(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .list_work_items(&CompanyId(company_id), Some(&project_id))
        .await
    {
        Ok(list) => (
            StatusCode::OK,
            Json(WorkListResponse {
                work_items: list
                    .into_iter()
                    .map(|w| WorkItemDto {
                        id: w.id,
                        company_id: w.company_id.0,
                        project_id: w.project_id,
                        objective_id: w.objective_id,
                        parent_work_item_id: w.parent_work_item_id,
                        title: w.title,
                        description: w.description,
                        work_type: w.work_type.to_string(),
                        status: w.status.to_string(),
                        row_version: w.row_version,
                        created_at: w.created_at.to_rfc3339(),
                        updated_at: w.updated_at.to_rfc3339(),
                    })
                    .collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_project_work_item(
    State(state): State<ApiState>,
    Path((company_id, project_id, work_id)): Path<(String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.get_work_item(&CompanyId(company_id), &work_id).await {
        Ok(w) => {
            if w.project_id != project_id {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorEnvelope::new(
                        error_codes::NOT_FOUND,
                        format!("Resource not found: {work_id}"),
                    )),
                )
                    .into_response();
            }
            (
                StatusCode::OK,
                Json(WorkItemDto {
                    id: w.id,
                    company_id: w.company_id.0,
                    project_id: w.project_id,
                    objective_id: w.objective_id,
                    parent_work_item_id: w.parent_work_item_id,
                    title: w.title,
                    description: w.description,
                    work_type: w.work_type.to_string(),
                    status: w.status.to_string(),
                    row_version: w.row_version,
                    created_at: w.created_at.to_rfc3339(),
                    updated_at: w.updated_at.to_rfc3339(),
                }),
            )
                .into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn create_work_item(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(req): Json<CreateWorkItemRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let work_type = match req.work_type.to_uppercase().parse::<WorkItemType>() {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorEnvelope::new(
                    error_codes::VALIDATION_FAILED,
                    format!("Invalid work type: {}", req.work_type),
                )),
            )
                .into_response();
        }
    };
    let cmd = CreateWorkItemCommand {
        company_id: CompanyId(company_id),
        project_id,
        title: req.title,
        description: req.description,
        objective_id: req.objective_id,
        parent_work_item_id: req.parent_work_item_id,
        work_type,
        meta: CommandMeta {
            idempotency_key,
            principal: None,
            correlation_id: None,
            causation_id: None,
        },
    };
    match ctx.create_work_item(cmd).await {
        Ok(w) => (
            StatusCode::CREATED,
            Json(WorkItemDto {
                id: w.id,
                company_id: w.company_id.0,
                project_id: w.project_id,
                objective_id: w.objective_id,
                parent_work_item_id: w.parent_work_item_id,
                title: w.title,
                description: w.description,
                work_type: w.work_type.to_string(),
                status: w.status.to_string(),
                row_version: w.row_version,
                created_at: w.created_at.to_rfc3339(),
                updated_at: w.updated_at.to_rfc3339(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn handle_work_item_action(
    State(state): State<ApiState>,
    Path((company_id, project_id, work_id)): Path<(String, String, String)>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let uri = req.uri().to_string();
    let action = uri.rsplit(':').next().unwrap_or_default().to_string();
    let (_parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorEnvelope::new(
                    error_codes::VALIDATION_FAILED,
                    "Failed to read body",
                )),
            )
                .into_response();
        }
    };
    let payload: WorkItemLifecycleRequest = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorEnvelope::new(
                    error_codes::VALIDATION_FAILED,
                    format!("Invalid payload: {err}"),
                )),
            )
                .into_response();
        }
    };
    let target = match action.as_str() {
        "start" => WorkItemStatus::InProgress,
        "complete" => WorkItemStatus::Completed,
        "cancel" => WorkItemStatus::Cancelled,
        "block" => WorkItemStatus::Blocked,
        "unblock" => WorkItemStatus::InProgress,
        "ready" => WorkItemStatus::Ready,
        "fail" => WorkItemStatus::Failed,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorEnvelope::new(
                    error_codes::VALIDATION_FAILED,
                    format!("Invalid work item action: {action}"),
                )),
            )
                .into_response();
        }
    };
    match ctx
        .transition_work_item(
            &CompanyId(company_id),
            &project_id,
            &work_id,
            target,
            payload.expected_version,
        )
        .await
    {
        Ok(w) => (
            StatusCode::OK,
            Json(WorkItemDto {
                id: w.id,
                company_id: w.company_id.0,
                project_id: w.project_id,
                objective_id: w.objective_id,
                parent_work_item_id: w.parent_work_item_id,
                title: w.title,
                description: w.description,
                work_type: w.work_type.to_string(),
                status: w.status.to_string(),
                row_version: w.row_version,
                created_at: w.created_at.to_rfc3339(),
                updated_at: w.updated_at.to_rfc3339(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

#[derive(serde::Deserialize, Default)]
pub struct WorkListQuery {
    pub project_id: Option<String>,
}

async fn list_work_items(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
    Query(query): Query<WorkListQuery>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .list_work_items(&CompanyId(company_id), query.project_id.as_deref())
        .await
    {
        Ok(list) => (
            StatusCode::OK,
            Json(WorkListResponse {
                work_items: list
                    .into_iter()
                    .map(|w| WorkItemDto {
                        id: w.id,
                        company_id: w.company_id.0,
                        project_id: w.project_id,
                        objective_id: w.objective_id,
                        parent_work_item_id: w.parent_work_item_id,
                        title: w.title,
                        description: w.description,
                        work_type: w.work_type.to_string(),
                        status: w.status.to_string(),
                        row_version: w.row_version,
                        created_at: w.created_at.to_rfc3339(),
                        updated_at: w.updated_at.to_rfc3339(),
                    })
                    .collect(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn get_work_item(
    State(state): State<ApiState>,
    Path((company_id, work_id)): Path<(String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx.get_work_item(&CompanyId(company_id), &work_id).await {
        Ok(w) => (
            StatusCode::OK,
            Json(WorkItemDto {
                id: w.id,
                company_id: w.company_id.0,
                project_id: w.project_id,
                objective_id: w.objective_id,
                parent_work_item_id: w.parent_work_item_id,
                title: w.title,
                description: w.description,
                work_type: w.work_type.to_string(),
                status: w.status.to_string(),
                row_version: w.row_version,
                created_at: w.created_at.to_rfc3339(),
                updated_at: w.updated_at.to_rfc3339(),
            }),
        )
            .into_response(),
        Err(err) => map_app_error(err),
    }
}

fn m3_meta(headers: &HeaderMap) -> nalarvo_application::CommandMeta {
    nalarvo_application::CommandMeta {
        idempotency_key: headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        correlation_id: headers
            .get("x-correlation-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        causation_id: headers
            .get("x-causation-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        ..Default::default()
    }
}

fn unknown_action() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorEnvelope::new(error_codes::NOT_FOUND, "Unknown action")),
    )
        .into_response()
}

macro_rules! m3_dto {
    (Objective, $v:ident) => {
        ObjectiveDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            parent_objective_id: $v.parent_objective_id,
            title: $v.title,
            description: $v.description,
            is_primary: $v.is_primary,
            is_required: $v.is_required,
            status: $v.status.to_string(),
            row_version: $v.row_version,
            created_at: $v.created_at.to_rfc3339(),
            updated_at: $v.updated_at.to_rfc3339(),
        }
    };
    (Team, $v:ident) => {
        TeamDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            name: $v.name,
            is_primary: $v.is_primary,
            status: $v.status.to_string(),
            row_version: $v.row_version,
            created_at: $v.created_at.to_rfc3339(),
            updated_at: $v.updated_at.to_rfc3339(),
        }
    };
    (Staffing, $v:ident) => {
        StaffingRequirementDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            team_id: $v.team_id,
            role_id: $v.role_id,
            department_id: $v.department_id,
            desired_count: $v.desired_count,
            required_capability_ids: $v.required_capability_ids,
            status: $v.status.to_string(),
            row_version: $v.row_version,
            created_at: $v.created_at.to_rfc3339(),
            updated_at: $v.updated_at.to_rfc3339(),
        }
    };
    (Allocation, $v:ident) => {
        AgentAllocationDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            team_id: $v.team_id,
            agent_id: $v.agent_id,
            staffing_requirement_id: $v.staffing_requirement_id,
            status: $v.status.to_string(),
            row_version: $v.row_version,
            created_at: $v.created_at.to_rfc3339(),
            updated_at: $v.updated_at.to_rfc3339(),
            released_at: $v.released_at.map(|t| t.to_rfc3339()),
        }
    };
    (Dependency, $v:ident) => {
        WorkDependencyDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            work_item_id: $v.work_item_id,
            depends_on_work_item_id: $v.depends_on_work_item_id,
            dependency_type: $v.dependency_type.to_string(),
            created_at: $v.created_at.to_rfc3339(),
        }
    };
    (Assignment, $v:ident) => {
        WorkAssignmentDto {
            id: $v.id,
            company_id: $v.company_id.0,
            project_id: $v.project_id,
            work_item_id: $v.work_item_id,
            agent_id: $v.agent_id,
            agent_allocation_id: String::new(),
            is_primary: $v.is_primary,
            status: $v.status.to_string(),
            row_version: $v.row_version,
            created_at: $v.created_at.to_rfc3339(),
            updated_at: $v.updated_at.to_rfc3339(),
            released_at: $v.released_at.map(|t| t.to_rfc3339()),
        }
    };
}

macro_rules! m3_resource {
    ($list:ident, $get:ident, $list_method:ident, $get_method:ident, $variant:ident, $response:ident, $field:ident) => {
        async fn $list(
            State(state): State<ApiState>,
            Path((company, project)): Path<(String, String)>,
        ) -> Response {
            let Some(ctx) = state.app_ctx.as_ref() else {
                return service_unavailable();
            };
            match ctx.$list_method(&CompanyId(company), &project).await {
                Ok(items) => (
                    StatusCode::OK,
                    Json($response {
                        $field: items.into_iter().map(|v| m3_dto!($variant, v)).collect(),
                    }),
                )
                    .into_response(),
                Err(err) => map_app_error(err),
            }
        }
        async fn $get(
            State(state): State<ApiState>,
            Path((company, project, id)): Path<(String, String, String)>,
        ) -> Response {
            let Some(ctx) = state.app_ctx.as_ref() else {
                return service_unavailable();
            };
            match ctx.$get_method(&CompanyId(company), &project, &id).await {
                Ok(v) => (StatusCode::OK, Json(m3_dto!($variant, v))).into_response(),
                Err(err) => map_app_error(err),
            }
        }
    };
}
m3_resource!(
    list_objectives,
    get_objective,
    list_objectives,
    get_objective,
    Objective,
    ObjectiveListResponse,
    objectives
);
m3_resource!(
    list_teams,
    get_team,
    list_teams,
    get_team,
    Team,
    TeamListResponse,
    teams
);
m3_resource!(
    list_staffing_requirements,
    get_staffing_requirement,
    list_staffing_requirements,
    get_staffing_requirement,
    Staffing,
    StaffingRequirementListResponse,
    staffing_requirements
);
m3_resource!(
    list_agent_allocations,
    get_agent_allocation,
    list_agent_allocations,
    get_agent_allocation,
    Allocation,
    AgentAllocationListResponse,
    allocations
);

async fn create_objective(
    State(state): State<ApiState>,
    Path((company, project)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateObjectiveRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let mut cmd = CreateObjectiveCommand::new(CompanyId(company), project, payload.title);
    cmd.description = payload.description;
    cmd.parent_objective_id = payload.parent_objective_id;
    cmd.is_primary = payload.is_primary;
    cmd.is_required = payload.is_required;
    cmd.meta = m3_meta(&headers);
    match ctx.create_objective(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Objective, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}
async fn create_team(
    State(state): State<ApiState>,
    Path((company, project)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateTeamRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let mut cmd = CreateTeamCommand::new(CompanyId(company), project, payload.name);
    cmd.is_primary = payload.is_primary;
    cmd.meta = m3_meta(&headers);
    match ctx.create_team(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Team, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}
async fn create_staffing_requirement(
    State(state): State<ApiState>,
    Path((company, project)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateStaffingRequirementRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let mut cmd = CreateStaffingRequirementCommand::new(
        CompanyId(company),
        project,
        payload.team_id,
        payload.role_id,
        payload.desired_count,
    );
    cmd.department_id = payload.department_id;
    cmd.required_capability_ids = payload.required_capability_ids;
    cmd.meta = m3_meta(&headers);
    match ctx.create_staffing_requirement(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Staffing, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}
async fn create_agent_allocation(
    State(state): State<ApiState>,
    Path((company, project)): Path<(String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateAgentAllocationRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let mut cmd = CreateAgentAllocationCommand::new(
        CompanyId(company),
        project,
        payload.team_id,
        payload.agent_id,
        payload.staffing_requirement_id,
    );
    cmd.meta = m3_meta(&headers);
    match ctx.create_agent_allocation(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Allocation, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}

macro_rules! m3_action {
    ($handler:ident, $request:ty, $method:ident, $variant:ident, {$($action:literal => $status:expr),+ $(,)?}) => {
        async fn $handler(State(state): State<ApiState>, Path((company, project, id)): Path<(String, String, String)>, req: Request) -> Response {
            let Some(ctx) = state.app_ctx.as_ref() else { return service_unavailable(); };
            let (clean, status) = match id.rsplit_once(':') {
                $(Some((clean, $action)) => (clean, $status),)+
                _ => return unknown_action(),
            };
            let Ok(body) = axum::body::to_bytes(req.into_body(), 64 * 1024).await else { return (StatusCode::BAD_REQUEST, "Invalid body").into_response(); };
            let Ok(payload) = serde_json::from_slice::<$request>(&body) else { return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response(); };
            match ctx.$method(&CompanyId(company), &project, clean, status, payload.expected_version).await {
                Ok(v) => (StatusCode::OK, Json(m3_dto!($variant, v))).into_response(),
                Err(e) => map_app_error(e),
            }
        }
    };
}
m3_action!(handle_objective_action, ObjectiveLifecycleRequest, transition_objective, Objective, {
    "activate" => ObjectiveStatus::Active, "achieve" => ObjectiveStatus::Achieved, "fail" => ObjectiveStatus::Failed, "cancel" => ObjectiveStatus::Cancelled, "archive" => ObjectiveStatus::Archived
});
m3_action!(handle_team_action, TeamLifecycleRequest, transition_team, Team, {
    "activate" => TeamStatus::Active, "pause" => TeamStatus::Paused, "resume" => TeamStatus::Active, "disband" => TeamStatus::Disbanded, "archive" => TeamStatus::Archived
});
m3_action!(handle_staffing_requirement_action, StaffingLifecycleRequest, transition_staffing_requirement, Staffing, {
    "open" => StaffingRequirementStatus::Open, "block" => StaffingRequirementStatus::Blocked, "unblock" => StaffingRequirementStatus::Open, "cancel" => StaffingRequirementStatus::Cancelled
});
m3_action!(handle_agent_allocation_action, AllocationLifecycleRequest, transition_agent_allocation, Allocation, {
    "activate" => AgentAllocationStatus::Active, "pause" => AgentAllocationStatus::Paused, "resume" => AgentAllocationStatus::Active, "release" => AgentAllocationStatus::Released, "cancel" => AgentAllocationStatus::Cancelled
});

async fn list_work_dependencies(
    State(state): State<ApiState>,
    Path((company, project, work_id)): Path<(String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .list_work_dependencies(&CompanyId(company), &project)
        .await
    {
        Ok(items) => {
            let filtered: Vec<WorkDependencyDto> = items
                .into_iter()
                .filter(|v| v.work_item_id == work_id)
                .map(|v| m3_dto!(Dependency, v))
                .collect();
            (
                StatusCode::OK,
                Json(WorkDependencyListResponse {
                    dependencies: filtered,
                }),
            )
                .into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn create_work_dependency(
    State(state): State<ApiState>,
    Path((company, project, work_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateWorkDependencyRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    if payload.dependency_type != "HARD" {
        return (
            StatusCode::BAD_REQUEST,
            "Only HARD dependencies are supported",
        )
            .into_response();
    }
    let dep_type = DependencyType::Hard;
    let mut cmd = CreateWorkDependencyCommand::new(
        CompanyId(company),
        project,
        work_id,
        payload.depends_on_work_item_id,
        dep_type,
    );
    cmd.meta = m3_meta(&headers);
    match ctx.create_work_dependency(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Dependency, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}

async fn get_work_dependency(
    State(state): State<ApiState>,
    Path((company, project, work_id, dependency_id)): Path<(String, String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .get_work_dependency(&CompanyId(company), &project, &dependency_id)
        .await
    {
        Ok(v) => {
            if v.work_item_id != work_id {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorEnvelope::new(
                        error_codes::NOT_FOUND,
                        format!("Resource not found: {dependency_id}"),
                    )),
                )
                    .into_response();
            }
            (StatusCode::OK, Json(m3_dto!(Dependency, v))).into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn delete_work_dependency(
    State(state): State<ApiState>,
    Path((company, project, _work_id, dependency_id)): Path<(String, String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .delete_work_dependency(&CompanyId(company), &project, &dependency_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => map_app_error(err),
    }
}

async fn list_work_assignments(
    State(state): State<ApiState>,
    Path((company, project, work_id)): Path<(String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .list_work_assignments(&CompanyId(company), &project)
        .await
    {
        Ok(items) => {
            let filtered: Vec<WorkAssignmentDto> = items
                .into_iter()
                .filter(|v| v.work_item_id == work_id)
                .map(|v| m3_dto!(Assignment, v))
                .collect();
            (
                StatusCode::OK,
                Json(WorkAssignmentListResponse {
                    assignments: filtered,
                }),
            )
                .into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn create_work_assignment(
    State(state): State<ApiState>,
    Path((company, project, work_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(payload): Json<CreateWorkAssignmentRequest>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let mut cmd = CreateWorkAssignmentCommand::new(
        CompanyId(company),
        project,
        work_id,
        payload.agent_id,
        payload.agent_allocation_id,
        payload.is_primary,
    );
    cmd.meta = m3_meta(&headers);
    match ctx.create_work_assignment(cmd).await {
        Ok(v) => (StatusCode::CREATED, Json(m3_dto!(Assignment, v))).into_response(),
        Err(e) => map_app_error(e),
    }
}

async fn get_work_assignment(
    State(state): State<ApiState>,
    Path((company, project, work_id, assignment_id)): Path<(String, String, String, String)>,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    match ctx
        .get_work_assignment(&CompanyId(company), &project, &assignment_id)
        .await
    {
        Ok(v) => {
            if v.work_item_id != work_id {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorEnvelope::new(
                        error_codes::NOT_FOUND,
                        format!("Resource not found: {assignment_id}"),
                    )),
                )
                    .into_response();
            }
            (StatusCode::OK, Json(m3_dto!(Assignment, v))).into_response()
        }
        Err(err) => map_app_error(err),
    }
}

async fn handle_work_assignment_action(
    State(state): State<ApiState>,
    Path((company, project, work_id, id)): Path<(String, String, String, String)>,
    req: Request,
) -> Response {
    let Some(ctx) = state.app_ctx.as_ref() else {
        return service_unavailable();
    };
    let (clean_id, action) = match id.rsplit_once(':') {
        Some((clean, act)) => (clean, act),
        None => return unknown_action(),
    };
    let Ok(body) = axum::body::to_bytes(req.into_body(), 64 * 1024).await else {
        return (StatusCode::BAD_REQUEST, "Invalid body").into_response();
    };
    let Ok(payload) = serde_json::from_slice::<AssignmentLifecycleRequest>(&body) else {
        return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
    };
    match ctx
        .get_work_assignment(&CompanyId(company.clone()), &project, clean_id)
        .await
    {
        Ok(v) if v.work_item_id != work_id => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorEnvelope::new(
                    error_codes::NOT_FOUND,
                    format!("Resource not found: {clean_id}"),
                )),
            )
                .into_response();
        }
        Err(e) => return map_app_error(e),
        _ => {}
    }
    match action {
        "release" => {
            match ctx
                .release_work_assignment(
                    &CompanyId(company),
                    &project,
                    clean_id,
                    payload.expected_version,
                )
                .await
            {
                Ok(v) => (StatusCode::OK, Json(m3_dto!(Assignment, v))).into_response(),
                Err(e) => map_app_error(e),
            }
        }
        _ => unknown_action(),
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

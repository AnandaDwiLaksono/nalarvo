use nalarvo_contracts::{
    AgentAllocationDto, AgentAllocationListResponse, AllocationLifecycleRequest,
    AssignmentLifecycleRequest, BindProjectWorkingRootRequest, CreateAgentAllocationRequest,
    CreateObjectiveRequest, CreateStaffingRequirementRequest, CreateTeamRequest,
    CreateWorkAssignmentRequest, CreateWorkDependencyRequest, CreateWorkItemRequest, ObjectiveDto,
    ObjectiveLifecycleRequest, ObjectiveListResponse, StaffingLifecycleRequest,
    StaffingRequirementDto, StaffingRequirementListResponse, TeamDto, TeamLifecycleRequest,
    TeamListResponse, UnbindProjectWorkingRootRequest, WorkAssignmentDto,
    WorkAssignmentListResponse, WorkDependencyDto, WorkDependencyListResponse,
    WorkItemLifecycleRequest,
};
use nalarvo_contracts::{
    AgentDto, AgentListResponse, CompanyDto, CompanyLifecycleRequest, CompanyListResponse,
    CreateAgentRequest, CreateCompanyRequest, CreateDepartmentRequest, CreateProjectRequest,
    CreateProviderConnectionRequest, CreateRoleRequest, CredentialRefDto, DepartmentDto,
    DepartmentListResponse, ErrorEnvelope, HealthResponse, ProjectDto, ProjectLifecycleRequest,
    ProjectListResponse, ProviderConnectionDto, ProviderLifecycleRequest, ProviderListResponse,
    ProviderTestResponse, RoleDto, RoleListResponse, SubmitCredentialRequest, UpdateCompanyRequest,
    WorkItemDto, WorkListResponse, WorkspaceDto,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

pub mod m4;
pub use m4::*;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("daemon request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("daemon returned error [{code}]: {message}")]
    Api {
        status: StatusCode,
        code: String,
        message: String,
        details: Option<serde_json::Value>,
    },

    #[error("daemon returned unexpected HTTP status {0}")]
    Http(StatusCode),

    #[error("unsupported semantic action")]
    InvalidAction,
}

impl ClientError {
    pub async fn from_response(res: reqwest::Response) -> Self {
        let status = res.status();
        if let Ok(envelope) = res.json::<ErrorEnvelope>().await {
            Self::Api {
                status,
                code: envelope.error.code,
                message: envelope.error.message,
                details: envelope.error.details,
            }
        } else {
            Self::Http(status)
        }
    }
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, ClientError> {
    if response.status().is_success() {
        Ok(response.json().await?)
    } else {
        Err(ClientError::from_response(response).await)
    }
}

async fn get<T: DeserializeOwned>(
    base_url: &str,
    bearer: &str,
    path: &str,
) -> Result<T, ClientError> {
    decode(
        reqwest::Client::new()
            .get(format!("{base_url}{path}"))
            .bearer_auth(bearer)
            .send()
            .await?,
    )
    .await
}

async fn post<T: DeserializeOwned, B: serde::Serialize>(
    base_url: &str,
    bearer: &str,
    path: &str,
    body: &B,
) -> Result<T, ClientError> {
    decode(
        reqwest::Client::new()
            .post(format!("{base_url}{path}"))
            .bearer_auth(bearer)
            .json(body)
            .send()
            .await?,
    )
    .await
}

pub async fn health(base_url: &str, bearer: &str) -> Result<HealthResponse, ClientError> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/health"))
        .bearer_auth(bearer)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(ClientError::from_response(response).await);
    }

    Ok(response.json().await?)
}

pub async fn create_company(
    base_url: &str,
    bearer: &str,
    workspace_id: &str,
    req: &CreateCompanyRequest,
    idempotency_key: Option<&str>,
) -> Result<CompanyDto, ClientError> {
    let mut builder = reqwest::Client::new()
        .post(format!(
            "{base_url}/api/v1/workspaces/{workspace_id}/companies"
        ))
        .bearer_auth(bearer)
        .json(req);

    if let Some(key) = idempotency_key {
        builder = builder.header("idempotency-key", key);
    }

    let response = builder.send().await?;

    if !response.status().is_success() {
        return Err(ClientError::from_response(response).await);
    }

    Ok(response.json().await?)
}

pub async fn list_companies(
    base_url: &str,
    bearer: &str,
    workspace_id: &str,
) -> Result<CompanyListResponse, ClientError> {
    let response = reqwest::Client::new()
        .get(format!(
            "{base_url}/api/v1/workspaces/{workspace_id}/companies"
        ))
        .bearer_auth(bearer)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(ClientError::from_response(response).await);
    }

    Ok(response.json().await?)
}

pub async fn get_company(
    base_url: &str,
    bearer: &str,
    workspace_id: &str,
    company_id: &str,
) -> Result<CompanyDto, ClientError> {
    let response = reqwest::Client::new()
        .get(format!(
            "{base_url}/api/v1/workspaces/{workspace_id}/companies/{company_id}"
        ))
        .bearer_auth(bearer)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(ClientError::from_response(response).await);
    }

    Ok(response.json().await?)
}

pub async fn update_company(
    base_url: &str,
    bearer: &str,
    workspace_id: &str,
    company_id: &str,
    req: &UpdateCompanyRequest,
) -> Result<CompanyDto, ClientError> {
    let response = reqwest::Client::new()
        .put(format!(
            "{base_url}/api/v1/workspaces/{workspace_id}/companies/{company_id}"
        ))
        .bearer_auth(bearer)
        .json(req)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(ClientError::from_response(response).await);
    }

    Ok(response.json().await?)
}

pub async fn get_workspace(base_url: &str, bearer: &str) -> Result<WorkspaceDto, ClientError> {
    get(base_url, bearer, "/api/v1/workspace").await
}

pub async fn list_providers(
    base_url: &str,
    bearer: &str,
) -> Result<ProviderListResponse, ClientError> {
    get(base_url, bearer, "/api/v1/workspace/providers").await
}

pub async fn create_provider(
    base_url: &str,
    bearer: &str,
    req: &CreateProviderConnectionRequest,
) -> Result<ProviderConnectionDto, ClientError> {
    post(base_url, bearer, "/api/v1/workspace/providers", req).await
}

pub async fn toggle_provider(
    base_url: &str,
    bearer: &str,
    provider_id: &str,
    action: &str,
    req: &ProviderLifecycleRequest,
) -> Result<ProviderConnectionDto, ClientError> {
    if !matches!(action, "enable" | "disable") {
        return Err(ClientError::InvalidAction);
    }
    post(
        base_url,
        bearer,
        &format!("/api/v1/workspace/providers/{provider_id}:{action}"),
        req,
    )
    .await
}

pub async fn test_provider(
    base_url: &str,
    bearer: &str,
    provider_id: &str,
) -> Result<ProviderTestResponse, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/workspace/providers/{provider_id}:test"),
        &serde_json::json!({}),
    )
    .await
}

pub async fn submit_credential(
    base_url: &str,
    bearer: &str,
    req: &SubmitCredentialRequest,
) -> Result<CredentialRefDto, ClientError> {
    post(base_url, bearer, "/api/v1/workspace/credentials", req).await
}

pub async fn list_departments(
    base_url: &str,
    bearer: &str,
    company_id: &str,
) -> Result<DepartmentListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/departments"),
    )
    .await
}

pub async fn create_department(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    req: &CreateDepartmentRequest,
) -> Result<DepartmentDto, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/departments"),
        req,
    )
    .await
}

pub async fn list_roles(
    base_url: &str,
    bearer: &str,
    company_id: &str,
) -> Result<RoleListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/roles"),
    )
    .await
}

pub async fn create_role(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    req: &CreateRoleRequest,
) -> Result<RoleDto, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/roles"),
        req,
    )
    .await
}

pub async fn list_agents(
    base_url: &str,
    bearer: &str,
    company_id: &str,
) -> Result<AgentListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/agents"),
    )
    .await
}

pub async fn get_agent(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    agent_id: &str,
) -> Result<AgentDto, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/agents/{agent_id}"),
    )
    .await
}

pub async fn get_agent_availability(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    agent_id: &str,
) -> Result<nalarvo_contracts::AgentAvailabilityResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/agents/{agent_id}/availability"),
    )
    .await
}

pub async fn create_agent(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    req: &CreateAgentRequest,
) -> Result<AgentDto, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/agents"),
        req,
    )
    .await
}

pub async fn activate_company(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    req: &CompanyLifecycleRequest,
) -> Result<CompanyDto, ClientError> {
    company_lifecycle(base_url, bearer, company_id, "activate", req).await
}

pub async fn company_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    action: &str,
    req: &CompanyLifecycleRequest,
) -> Result<CompanyDto, ClientError> {
    if !matches!(action, "activate" | "pause" | "resume" | "archive") {
        return Err(ClientError::InvalidAction);
    }
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}:{action}"),
        req,
    )
    .await
}

pub async fn list_projects(
    base_url: &str,
    bearer: &str,
    company_id: &str,
) -> Result<ProjectListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects"),
    )
    .await
}

pub async fn create_project(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    req: &CreateProjectRequest,
) -> Result<ProjectDto, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects"),
        req,
    )
    .await
}

pub async fn get_project(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
) -> Result<ProjectDto, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}"),
    )
    .await
}

pub async fn activate_project(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    req: &ProjectLifecycleRequest,
) -> Result<ProjectDto, ClientError> {
    project_lifecycle(base_url, bearer, company_id, project_id, "activate", req).await
}

pub async fn project_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    action: &str,
    req: &ProjectLifecycleRequest,
) -> Result<ProjectDto, ClientError> {
    if !matches!(action, "activate" | "pause" | "resume" | "archive") {
        return Err(ClientError::InvalidAction);
    }
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}:{action}"),
        req,
    )
    .await
}

pub async fn list_work_items(
    base_url: &str,
    bearer: &str,
    company_id: &str,
) -> Result<WorkListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/work"),
    )
    .await
}

pub async fn list_project_work_items(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
) -> Result<WorkListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/work"),
    )
    .await
}

pub async fn get_work_item(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    work_id: &str,
) -> Result<WorkItemDto, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/work/{work_id}"),
    )
    .await
}

pub async fn get_project_work_item(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
) -> Result<WorkItemDto, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}"),
    )
    .await
}

fn project_resource_path(company_id: &str, project_id: &str, resource: &str) -> String {
    format!("/api/v1/companies/{company_id}/projects/{project_id}/{resource}")
}

#[allow(clippy::too_many_arguments)]
async fn project_resource_action<T: DeserializeOwned, B: serde::Serialize>(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    resource: &str,
    id: &str,
    action: &str,
    req: &B,
) -> Result<T, ClientError> {
    post(
        base_url,
        bearer,
        &format!(
            "{}:{action}",
            project_resource_path(company_id, project_id, resource) + "/" + id
        ),
        req,
    )
    .await
}

macro_rules! project_resource {
    ($list:ident, $create:ident, $get_one:ident, $resource:literal, $list_ty:ty, $create_ty:ty, $dto:ty) => {
        pub async fn $list(
            base_url: &str,
            bearer: &str,
            company_id: &str,
            project_id: &str,
        ) -> Result<$list_ty, ClientError> {
            get(
                base_url,
                bearer,
                &project_resource_path(company_id, project_id, $resource),
            )
            .await
        }

        pub async fn $create(
            base_url: &str,
            bearer: &str,
            company_id: &str,
            project_id: &str,
            req: &$create_ty,
        ) -> Result<$dto, ClientError> {
            post(
                base_url,
                bearer,
                &project_resource_path(company_id, project_id, $resource),
                req,
            )
            .await
        }

        pub async fn $get_one(
            base_url: &str,
            bearer: &str,
            company_id: &str,
            project_id: &str,
            id: &str,
        ) -> Result<$dto, ClientError> {
            get(
                base_url,
                bearer,
                &(project_resource_path(company_id, project_id, $resource) + "/" + id),
            )
            .await
        }
    };
}

project_resource!(
    list_objectives,
    create_objective,
    get_objective,
    "objectives",
    ObjectiveListResponse,
    CreateObjectiveRequest,
    ObjectiveDto
);
project_resource!(
    list_teams,
    create_team,
    get_team,
    "teams",
    TeamListResponse,
    CreateTeamRequest,
    TeamDto
);
project_resource!(
    list_staffing_requirements,
    create_staffing_requirement,
    get_staffing_requirement,
    "staffing-requirements",
    StaffingRequirementListResponse,
    CreateStaffingRequirementRequest,
    StaffingRequirementDto
);
project_resource!(
    list_agent_allocations,
    create_agent_allocation,
    get_agent_allocation,
    "allocations",
    AgentAllocationListResponse,
    CreateAgentAllocationRequest,
    AgentAllocationDto
);

pub async fn objective_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    objective_id: &str,
    action: &str,
    req: &ObjectiveLifecycleRequest,
) -> Result<ObjectiveDto, ClientError> {
    if !matches!(
        action,
        "activate" | "achieve" | "fail" | "cancel" | "archive"
    ) {
        return Err(ClientError::InvalidAction);
    }
    project_resource_action(
        base_url,
        bearer,
        company_id,
        project_id,
        "objectives",
        objective_id,
        action,
        req,
    )
    .await
}

pub async fn activate_objective(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    objective_id: &str,
    req: &ObjectiveLifecycleRequest,
) -> Result<ObjectiveDto, ClientError> {
    objective_lifecycle(
        base_url,
        bearer,
        company_id,
        project_id,
        objective_id,
        "activate",
        req,
    )
    .await
}

pub async fn team_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    team_id: &str,
    action: &str,
    req: &TeamLifecycleRequest,
) -> Result<TeamDto, ClientError> {
    if !matches!(
        action,
        "activate" | "pause" | "resume" | "disband" | "archive"
    ) {
        return Err(ClientError::InvalidAction);
    }
    project_resource_action(
        base_url, bearer, company_id, project_id, "teams", team_id, action, req,
    )
    .await
}

pub async fn staffing_requirement_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    requirement_id: &str,
    action: &str,
    req: &StaffingLifecycleRequest,
) -> Result<StaffingRequirementDto, ClientError> {
    if !matches!(action, "open" | "block" | "unblock" | "cancel") {
        return Err(ClientError::InvalidAction);
    }
    project_resource_action(
        base_url,
        bearer,
        company_id,
        project_id,
        "staffing-requirements",
        requirement_id,
        action,
        req,
    )
    .await
}

pub async fn agent_allocation_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    allocation_id: &str,
    action: &str,
    req: &AllocationLifecycleRequest,
) -> Result<AgentAllocationDto, ClientError> {
    if !matches!(
        action,
        "activate" | "pause" | "resume" | "release" | "cancel"
    ) {
        return Err(ClientError::InvalidAction);
    }
    project_resource_action(
        base_url,
        bearer,
        company_id,
        project_id,
        "allocations",
        allocation_id,
        action,
        req,
    )
    .await
}

pub async fn bind_project_working_root(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    req: &BindProjectWorkingRootRequest,
) -> Result<ProjectDto, ClientError> {
    let response = reqwest::Client::new()
        .put(format!(
            "{base_url}/api/v1/companies/{company_id}/projects/{project_id}/working-root"
        ))
        .bearer_auth(bearer)
        .json(req)
        .send()
        .await?;
    decode(response).await
}

pub async fn create_work_item(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    req: &CreateWorkItemRequest,
) -> Result<WorkItemDto, ClientError> {
    post(
        base_url,
        bearer,
        &project_resource_path(company_id, project_id, "work"),
        req,
    )
    .await
}

pub async fn work_item_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    action: &str,
    req: &WorkItemLifecycleRequest,
) -> Result<WorkItemDto, ClientError> {
    if !matches!(
        action,
        "ready" | "start" | "block" | "submit" | "complete" | "fail" | "cancel"
    ) {
        return Err(ClientError::InvalidAction);
    }
    project_resource_action(
        base_url, bearer, company_id, project_id, "work", work_id, action, req,
    )
    .await
}

fn work_resource_path(company_id: &str, project_id: &str, work_id: &str, resource: &str) -> String {
    format!("/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/{resource}")
}

pub async fn list_work_dependencies(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
) -> Result<WorkDependencyListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &work_resource_path(company_id, project_id, work_id, "dependencies"),
    )
    .await
}

pub async fn create_work_dependency(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    req: &CreateWorkDependencyRequest,
) -> Result<WorkDependencyDto, ClientError> {
    post(
        base_url,
        bearer,
        &work_resource_path(company_id, project_id, work_id, "dependencies"),
        req,
    )
    .await
}

async fn delete_empty(base_url: &str, bearer: &str, path: &str) -> Result<(), ClientError> {
    let response = reqwest::Client::new()
        .delete(format!("{base_url}{path}"))
        .bearer_auth(bearer)
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(ClientError::from_response(response).await)
    }
}

pub async fn unbind_project_working_root(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    req: &UnbindProjectWorkingRootRequest,
) -> Result<ProjectDto, ClientError> {
    let response = reqwest::Client::new()
        .delete(format!(
            "{base_url}/api/v1/companies/{company_id}/projects/{project_id}/working-root"
        ))
        .bearer_auth(bearer)
        .json(req)
        .send()
        .await?;
    decode(response).await
}

pub async fn delete_work_dependency(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    dependency_id: &str,
) -> Result<(), ClientError> {
    delete_empty(
        base_url,
        bearer,
        &format!(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}/dependencies/{dependency_id}"
        ),
    )
    .await
}

pub async fn list_work_assignments(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
) -> Result<WorkAssignmentListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &work_resource_path(company_id, project_id, work_id, "assignments"),
    )
    .await
}

pub async fn create_work_assignment(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    req: &CreateWorkAssignmentRequest,
) -> Result<WorkAssignmentDto, ClientError> {
    post(
        base_url,
        bearer,
        &work_resource_path(company_id, project_id, work_id, "assignments"),
        req,
    )
    .await
}

pub async fn get_work_assignment(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    assignment_id: &str,
) -> Result<WorkAssignmentDto, ClientError> {
    get(
        base_url,
        bearer,
        &(work_resource_path(company_id, project_id, work_id, "assignments") + "/" + assignment_id),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn work_assignment_lifecycle(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    work_id: &str,
    assignment_id: &str,
    action: &str,
    req: &AssignmentLifecycleRequest,
) -> Result<WorkAssignmentDto, ClientError> {
    if !matches!(action, "release" | "cancel") {
        return Err(ClientError::InvalidAction);
    }
    post(
        base_url,
        bearer,
        &format!(
            "{}:{action}",
            work_resource_path(company_id, project_id, work_id, "assignments")
                + "/"
                + assignment_id
        ),
        req,
    )
    .await
}

use nalarvo_contracts::{
    AgentAllocationDto, AgentAllocationListResponse, AllocationLifecycleRequest,
    AssignmentLifecycleRequest, BindProjectWorkingRootRequest, CancelRunRequest,
    CommandAcceptedResponse, CompanyDto, CompanyLifecycleRequest, CompanyListResponse,
    CreateAgentAllocationRequest, CreateAgentRequest, CreateCompanyRequest,
    CreateDepartmentRequest, CreateObjectiveRequest, CreateProjectRequest,
    CreateProviderConnectionRequest, CreateRoleRequest, CreateRunRequest,
    CreateStaffingRequirementRequest, CreateTeamRequest, CreateWorkAssignmentRequest,
    CreateWorkDependencyRequest, CreateWorkItemRequest, ExecutionStepListResponse, HealthResponse,
    ObjectiveDto, ObjectiveLifecycleRequest, ObjectiveListResponse, ProjectDto,
    ProjectLifecycleRequest, ProjectListResponse, ProviderLifecycleRequest, QueueRunRequest,
    RoleDto, RoleListResponse, RunDto, RunListResponse, RunShowResponse, RuntimeResultResponse,
    StaffingLifecycleRequest, StaffingRequirementDto, StaffingRequirementListResponse,
    SubmitCredentialRequest, TeamDto, TeamLifecycleRequest, TeamListResponse, TimelineResponse,
    UnbindProjectWorkingRootRequest, UsageRecordListResponse, WorkAssignmentDto,
    WorkAssignmentListResponse, WorkDependencyDto, WorkDependencyListResponse, WorkItemDto,
    WorkItemLifecycleRequest, WorkListResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::Mutex,
};

#[derive(Serialize)]
struct DesktopWorkspace {
    id: String,
    name: String,
    lifecycle_state: String,
}

#[derive(Serialize)]
struct DesktopProvider {
    id: String,
    name: String,
    endpoint: String,
    lifecycle_state: String,
    health_state: String,
    configured_model_ids: Vec<String>,
    row_version: i64,
}

#[derive(Serialize)]
struct DesktopProviderListResponse {
    providers: Vec<DesktopProvider>,
}

#[derive(Deserialize)]
struct DesktopCreateProviderPayload {
    name: String,
    endpoint: String,
    secret: String,
    model_ids: Vec<String>,
}

#[derive(Serialize)]
struct DesktopDepartment {
    id: String,
    name: String,
    lifecycle_state: String,
    row_version: i64,
}

#[derive(Serialize)]
struct DesktopDepartmentListResponse {
    departments: Vec<DesktopDepartment>,
}

#[derive(Serialize)]
struct DesktopAgent {
    id: String,
    name: String,
    description: Option<String>,
    instructions: Option<String>,
    lifecycle_state: String,
    department_id: String,
    role_id: String,
    model_profile_id: Option<String>,
    max_active_allocations: i64,
    availability: Option<String>,
    row_version: i64,
}

#[derive(Serialize)]
struct DesktopAgentListResponse {
    agents: Vec<DesktopAgent>,
}

#[derive(Deserialize)]
struct DesktopCreateAgentPayload {
    name: String,
    department_id: String,
    role_id: String,
    instructions: Option<String>,
    max_active_allocations: i64,
}

#[tauri::command]
async fn core_health(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
) -> Result<HealthResponse, String> {
    nalarvo_client::health(&daemon_url, &token)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_workspace(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
) -> Result<DesktopWorkspace, String> {
    let w = nalarvo_client::get_workspace(&daemon_url, &token)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DesktopWorkspace {
        id: w.id,
        name: w.name,
        lifecycle_state: w.status,
    })
}

#[tauri::command]
async fn core_list_providers(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
) -> Result<DesktopProviderListResponse, String> {
    let res = nalarvo_client::list_providers(&daemon_url, &token)
        .await
        .map_err(|e| e.to_string())?;
    let providers = res
        .providers
        .into_iter()
        .map(|p| DesktopProvider {
            id: p.id,
            name: p.name,
            endpoint: "https://api.openai.com/v1".into(),
            lifecycle_state: p.status,
            health_state: p.health,
            configured_model_ids: vec![],
            row_version: p.row_version,
        })
        .collect();
    Ok(DesktopProviderListResponse { providers })
}

#[tauri::command]
async fn core_create_provider(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    req: DesktopCreateProviderPayload,
) -> Result<DesktopProvider, String> {
    let credential_ref_id = if !req.secret.is_empty() {
        let cred_req = SubmitCredentialRequest::new(format!("{}-key", req.name), req.secret);
        let cred = nalarvo_client::submit_credential(&daemon_url, &token, &cred_req)
            .await
            .map_err(|e| e.to_string())?;
        Some(cred.id)
    } else {
        None
    };

    let p_req = CreateProviderConnectionRequest {
        name: req.name,
        provider_kind: "openai".into(),
        credential_ref_id,
    };
    let p = nalarvo_client::create_provider(&daemon_url, &token, &p_req)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DesktopProvider {
        id: p.id,
        name: p.name,
        endpoint: req.endpoint,
        lifecycle_state: p.status,
        health_state: p.health,
        configured_model_ids: req.model_ids,
        row_version: p.row_version,
    })
}

#[tauri::command]
async fn core_toggle_provider(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    provider_id: String,
    action: String,
    expected_version: i64,
) -> Result<DesktopProvider, String> {
    let req = ProviderLifecycleRequest { expected_version };
    let p = nalarvo_client::toggle_provider(&daemon_url, &token, &provider_id, &action, &req)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DesktopProvider {
        id: p.id,
        name: p.name,
        endpoint: "https://api.openai.com/v1".into(),
        lifecycle_state: p.status,
        health_state: p.health,
        configured_model_ids: vec![],
        row_version: p.row_version,
    })
}

#[tauri::command]
async fn core_test_provider(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    provider_id: String,
) -> Result<DesktopProvider, String> {
    let res = nalarvo_client::test_provider(&daemon_url, &token, &provider_id)
        .await
        .map_err(|e| e.to_string())?;
    let p = res.provider;
    Ok(DesktopProvider {
        id: p.id,
        name: p.name,
        endpoint: "https://api.openai.com/v1".into(),
        lifecycle_state: p.status,
        health_state: p.health,
        configured_model_ids: vec![],
        row_version: p.row_version,
    })
}

#[tauri::command]
async fn core_list_companies(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    workspace_id: String,
) -> Result<CompanyListResponse, String> {
    nalarvo_client::list_companies(&daemon_url, &token, &workspace_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_company(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    workspace_id: String,
    req: CreateCompanyRequest,
) -> Result<CompanyDto, String> {
    nalarvo_client::create_company(&daemon_url, &token, &workspace_id, &req, None)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_company_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    action: String,
    expected_version: i64,
) -> Result<CompanyDto, String> {
    let req = CompanyLifecycleRequest { expected_version };
    nalarvo_client::company_lifecycle(&daemon_url, &token, &company_id, &action, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_departments(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
) -> Result<DesktopDepartmentListResponse, String> {
    let res = nalarvo_client::list_departments(&daemon_url, &token, &company_id)
        .await
        .map_err(|e| e.to_string())?;
    let departments = res
        .departments
        .into_iter()
        .map(|d| DesktopDepartment {
            id: d.id,
            name: d.name,
            lifecycle_state: d.status,
            row_version: d.row_version,
        })
        .collect();
    Ok(DesktopDepartmentListResponse { departments })
}

#[tauri::command]
async fn core_create_department(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    req: CreateDepartmentRequest,
) -> Result<DesktopDepartment, String> {
    let d = nalarvo_client::create_department(&daemon_url, &token, &company_id, &req)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DesktopDepartment {
        id: d.id,
        name: d.name,
        lifecycle_state: d.status,
        row_version: d.row_version,
    })
}

#[tauri::command]
async fn core_list_roles(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
) -> Result<RoleListResponse, String> {
    nalarvo_client::list_roles(&daemon_url, &token, &company_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_role(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    req: CreateRoleRequest,
) -> Result<RoleDto, String> {
    nalarvo_client::create_role(&daemon_url, &token, &company_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_agents(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
) -> Result<DesktopAgentListResponse, String> {
    let res = nalarvo_client::list_agents(&daemon_url, &token, &company_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut agents = Vec::new();
    for a in res.agents {
        let availability =
            nalarvo_client::get_agent_availability(&daemon_url, &token, &company_id, &a.id)
                .await
                .map(|r| r.availability)
                .ok();
        agents.push(DesktopAgent {
            id: a.id,
            name: a.name,
            description: None,
            instructions: None,
            lifecycle_state: a.status,
            department_id: a.primary_department_id,
            role_id: a.role_id,
            model_profile_id: a.model_profile_id,
            max_active_allocations: a.capacity,
            availability,
            row_version: a.row_version,
        });
    }
    Ok(DesktopAgentListResponse { agents })
}

#[tauri::command]
async fn core_create_agent(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    req: DesktopCreateAgentPayload,
) -> Result<DesktopAgent, String> {
    let c_req = CreateAgentRequest {
        name: req.name,
        primary_department_id: req.department_id,
        role_id: req.role_id,
        model_profile_id: None,
        capacity: req.max_active_allocations,
    };
    let a = nalarvo_client::create_agent(&daemon_url, &token, &company_id, &c_req)
        .await
        .map_err(|e| e.to_string())?;
    Ok(DesktopAgent {
        id: a.id,
        name: a.name,
        description: None,
        instructions: req.instructions,
        lifecycle_state: a.status,
        department_id: a.primary_department_id,
        role_id: a.role_id,
        model_profile_id: a.model_profile_id,
        max_active_allocations: a.capacity,
        availability: None,
        row_version: a.row_version,
    })
}

#[tauri::command]
async fn core_list_projects(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
) -> Result<ProjectListResponse, String> {
    nalarvo_client::list_projects(&daemon_url, &token, &company_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_project(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    req: CreateProjectRequest,
) -> Result<ProjectDto, String> {
    nalarvo_client::create_project(&daemon_url, &token, &company_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_project(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<ProjectDto, String> {
    nalarvo_client::get_project(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_activate_project(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    expected_version: i64,
) -> Result<ProjectDto, String> {
    nalarvo_client::activate_project(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &ProjectLifecycleRequest { expected_version },
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_project_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: ProjectLifecycleRequest,
) -> Result<ProjectDto, String> {
    nalarvo_client::activate_project(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_bind_project_working_root(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: BindProjectWorkingRootRequest,
) -> Result<ProjectDto, String> {
    nalarvo_client::bind_project_working_root(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_unbind_project_working_root(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: UnbindProjectWorkingRootRequest,
) -> Result<ProjectDto, String> {
    nalarvo_client::unbind_project_working_root(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_objectives(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<ObjectiveListResponse, String> {
    nalarvo_client::list_objectives(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_objective(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: CreateObjectiveRequest,
) -> Result<ObjectiveDto, String> {
    nalarvo_client::create_objective(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_objective_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    objective_id: String,
    req: ObjectiveLifecycleRequest,
) -> Result<ObjectiveDto, String> {
    nalarvo_client::activate_objective(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &objective_id,
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_teams(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<TeamListResponse, String> {
    nalarvo_client::list_teams(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_team(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: CreateTeamRequest,
) -> Result<TeamDto, String> {
    nalarvo_client::create_team(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_team_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    team_id: String,
    req: TeamLifecycleRequest,
) -> Result<TeamDto, String> {
    nalarvo_client::team_lifecycle(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &team_id,
        "activate",
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_staffing_requirements(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<StaffingRequirementListResponse, String> {
    nalarvo_client::list_staffing_requirements(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_staffing_requirement(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: CreateStaffingRequirementRequest,
) -> Result<StaffingRequirementDto, String> {
    nalarvo_client::create_staffing_requirement(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_staffing_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    requirement_id: String,
    req: StaffingLifecycleRequest,
) -> Result<StaffingRequirementDto, String> {
    nalarvo_client::staffing_requirement_lifecycle(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &requirement_id,
        "open",
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_allocations(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<AgentAllocationListResponse, String> {
    nalarvo_client::list_agent_allocations(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_allocation(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: CreateAgentAllocationRequest,
) -> Result<AgentAllocationDto, String> {
    nalarvo_client::create_agent_allocation(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_allocation_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    allocation_id: String,
    req: AllocationLifecycleRequest,
) -> Result<AgentAllocationDto, String> {
    nalarvo_client::agent_allocation_lifecycle(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &allocation_id,
        "activate",
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_work_items(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: Option<String>,
) -> Result<WorkListResponse, String> {
    if let Some(project_id) = project_id {
        nalarvo_client::list_project_work_items(&daemon_url, &token, &company_id, &project_id)
            .await
            .map_err(|e| e.to_string())
    } else {
        nalarvo_client::list_work_items(&daemon_url, &token, &company_id)
            .await
            .map_err(|e| e.to_string())
    }
}

#[tauri::command]
async fn core_create_work_item(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    req: CreateWorkItemRequest,
) -> Result<WorkItemDto, String> {
    nalarvo_client::create_work_item(&daemon_url, &token, &company_id, &project_id, &req)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_work_item(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: Option<String>,
    work_id: String,
) -> Result<WorkItemDto, String> {
    if let Some(project_id) = project_id {
        nalarvo_client::get_project_work_item(
            &daemon_url,
            &token,
            &company_id,
            &project_id,
            &work_id,
        )
        .await
        .map_err(|e| e.to_string())
    } else {
        nalarvo_client::get_work_item(&daemon_url, &token, &company_id, &work_id)
            .await
            .map_err(|e| e.to_string())
    }
}

#[tauri::command]
async fn core_work_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
    req: WorkItemLifecycleRequest,
) -> Result<WorkItemDto, String> {
    nalarvo_client::work_item_lifecycle(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &work_id,
        "start",
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_dependencies(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
) -> Result<WorkDependencyListResponse, String> {
    nalarvo_client::list_work_dependencies(&daemon_url, &token, &company_id, &project_id, &work_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_dependency(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
    req: CreateWorkDependencyRequest,
) -> Result<WorkDependencyDto, String> {
    nalarvo_client::create_work_dependency(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &work_id,
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_delete_dependency(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
    dependency_id: String,
) -> Result<(), String> {
    nalarvo_client::delete_work_dependency(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &work_id,
        &dependency_id,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_assignments(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
) -> Result<WorkAssignmentListResponse, String> {
    nalarvo_client::list_work_assignments(&daemon_url, &token, &company_id, &project_id, &work_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_assignment(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
    req: CreateWorkAssignmentRequest,
) -> Result<WorkAssignmentDto, String> {
    nalarvo_client::create_work_assignment(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &work_id,
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_assignment_lifecycle(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    work_id: String,
    assignment_id: String,
    req: AssignmentLifecycleRequest,
) -> Result<WorkAssignmentDto, String> {
    nalarvo_client::work_assignment_lifecycle(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &work_id,
        &assignment_id,
        "release",
        &req,
    )
    .await
    .map_err(|e| e.to_string())
}

struct DaemonChild(Mutex<Option<Child>>);

impl Drop for DaemonChild {
    fn drop(&mut self) {
        if let Ok(mut child) = self.0.lock()
            && let Some(mut child) = child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tauri::command]
async fn core_list_runs(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
) -> Result<RunListResponse, String> {
    nalarvo_client::list_runs(&daemon_url, &token, &company_id, &project_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_run(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
) -> Result<RunShowResponse, String> {
    nalarvo_client::get_run(&daemon_url, &token, &company_id, &project_id, &run_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_create_run(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    payload: CreateRunRequest,
) -> Result<RunDto, String> {
    nalarvo_client::create_run(&daemon_url, &token, &company_id, &project_id, &payload)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_queue_run(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
    payload: QueueRunRequest,
) -> Result<CommandAcceptedResponse, String> {
    nalarvo_client::queue_run(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &run_id,
        &payload,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_cancel_run(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
    payload: CancelRunRequest,
) -> Result<CommandAcceptedResponse, String> {
    nalarvo_client::cancel_run(
        &daemon_url,
        &token,
        &company_id,
        &project_id,
        &run_id,
        &payload,
    )
    .await
    .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_execution_steps(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
) -> Result<ExecutionStepListResponse, String> {
    nalarvo_client::list_execution_steps(&daemon_url, &token, &company_id, &project_id, &run_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_run_timeline(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
) -> Result<TimelineResponse, String> {
    nalarvo_client::get_run_timeline(&daemon_url, &token, &company_id, &project_id, &run_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_get_runtime_result(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
) -> Result<RuntimeResultResponse, String> {
    nalarvo_client::get_runtime_result(&daemon_url, &token, &company_id, &project_id, &run_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn core_list_usage_records(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
    company_id: String,
    project_id: String,
    run_id: String,
) -> Result<UsageRecordListResponse, String> {
    nalarvo_client::list_usage_records(&daemon_url, &token, &company_id, &project_id, &run_id)
        .await
        .map_err(|e| e.to_string())
}

fn start_daemon() -> Result<(String, Child), String> {
    let daemon_name = if cfg!(windows) {
        "nalarvo-daemon.exe"
    } else {
        "nalarvo-daemon"
    };
    let daemon_path = std::env::var_os("NALARVO_DAEMON_BIN")
        .map(Into::into)
        .unwrap_or(
            std::env::current_exe()
                .map_err(|e| e.to_string())?
                .with_file_name(daemon_name),
        );

    let mut child = Command::new(daemon_path)
        .args(["--bind", "127.0.0.1:47171", "--emit-token"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("failed to start Nalarvo Core: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Nalarvo Core token pipe unavailable".to_string())?;
    let mut token = String::new();
    BufReader::new(stdout)
        .read_line(&mut token)
        .map_err(|e| format!("failed to read Nalarvo Core token: {e}"))?;
    let token = token.trim().to_owned();

    if token.is_empty() {
        let _ = child.kill();
        let _ = child.wait();
        return Err("Nalarvo Core returned an empty token".into());
    }

    Ok((token, child))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let external_token = std::env::var("NALARVO_DAEMON_TOKEN").ok();
    let daemon_url =
        std::env::var("NALARVO_DAEMON_URL").unwrap_or_else(|_| "http://127.0.0.1:47171".into());

    let builder = tauri::Builder::default().manage(daemon_url);
    let builder = if let Some(token) = external_token {
        builder.manage(token)
    } else {
        let (token, child) = start_daemon().expect("failed to bootstrap Nalarvo Core");
        builder
            .manage(token)
            .manage(DaemonChild(Mutex::new(Some(child))))
    };

    builder
        .invoke_handler(tauri::generate_handler![
            core_health,
            core_get_workspace,
            core_list_providers,
            core_create_provider,
            core_toggle_provider,
            core_test_provider,
            core_list_companies,
            core_create_company,
            core_company_lifecycle,
            core_list_departments,
            core_create_department,
            core_list_roles,
            core_create_role,
            core_list_agents,
            core_create_agent,
            core_list_projects,
            core_create_project,
            core_get_project,
            core_activate_project,
            core_project_lifecycle,
            core_bind_project_working_root,
            core_unbind_project_working_root,
            core_list_objectives,
            core_create_objective,
            core_objective_lifecycle,
            core_list_teams,
            core_create_team,
            core_team_lifecycle,
            core_list_staffing_requirements,
            core_create_staffing_requirement,
            core_staffing_lifecycle,
            core_list_allocations,
            core_create_allocation,
            core_allocation_lifecycle,
            core_list_work_items,
            core_create_work_item,
            core_get_work_item,
            core_work_lifecycle,
            core_list_dependencies,
            core_create_dependency,
            core_delete_dependency,
            core_list_assignments,
            core_create_assignment,
            core_assignment_lifecycle,
            core_list_runs,
            core_get_run,
            core_create_run,
            core_queue_run,
            core_cancel_run,
            core_list_execution_steps,
            core_get_run_timeline,
            core_get_runtime_result,
            core_list_usage_records
        ])
        .run(tauri::generate_context!())
        .expect("error while running Nalarvo desktop");
}

use nalarvo_contracts::{
    CompanyDto, CompanyLifecycleRequest, CompanyListResponse, CreateAgentRequest,
    CreateCompanyRequest, CreateDepartmentRequest, CreateProviderConnectionRequest,
    CreateRoleRequest, HealthResponse, ProviderLifecycleRequest, RoleDto, RoleListResponse,
    SubmitCredentialRequest,
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
    let agents = res
        .agents
        .into_iter()
        .map(|a| DesktopAgent {
            id: a.id,
            name: a.name,
            description: None,
            instructions: None,
            lifecycle_state: a.status,
            department_id: a.primary_department_id,
            role_id: a.role_id,
            model_profile_id: a.model_profile_id,
            max_active_allocations: a.capacity,
            availability: Some("AVAILABLE".into()),
            row_version: a.row_version,
        })
        .collect();
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
        availability: Some("AVAILABLE".into()),
        row_version: a.row_version,
    })
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
            core_create_agent
        ])
        .run(tauri::generate_context!())
        .expect("error while running Nalarvo desktop");
}

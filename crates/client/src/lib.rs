use nalarvo_contracts::{
    AgentDto, AgentListResponse, CompanyDto, CompanyLifecycleRequest, CompanyListResponse,
    CreateAgentRequest, CreateCompanyRequest, CreateDepartmentRequest,
    CreateProviderConnectionRequest, CreateRoleRequest, CredentialRefDto, DepartmentDto,
    DepartmentListResponse, ErrorEnvelope, HealthResponse, ProviderConnectionDto,
    ProviderLifecycleRequest, ProviderListResponse, ProviderTestResponse, RoleDto,
    RoleListResponse, SubmitCredentialRequest, UpdateCompanyRequest, WorkspaceDto,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

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

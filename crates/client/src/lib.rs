use nalarvo_contracts::{
    CompanyDto, CompanyListResponse, CreateCompanyRequest, ErrorEnvelope, HealthResponse,
    UpdateCompanyRequest,
};
use reqwest::StatusCode;

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

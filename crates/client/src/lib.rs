use nalarvo_contracts::HealthResponse;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("daemon request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("daemon returned HTTP status {0}")]
    Http(reqwest::StatusCode),
}

pub async fn health(base_url: &str, bearer: &str) -> Result<HealthResponse, ClientError> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/health"))
        .bearer_auth(bearer)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(ClientError::Http(response.status()));
    }

    Ok(response.json().await?)
}

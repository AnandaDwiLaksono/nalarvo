use crate::{ClientError, get, post};
use nalarvo_contracts::*;

pub async fn create_run(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    req: &CreateRunRequest,
) -> Result<RunDto, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs"),
        req,
    )
    .await
}

pub async fn list_runs(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
) -> Result<RunListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs"),
    )
    .await
}

pub async fn get_run(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<RunShowResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}"),
    )
    .await
}

pub async fn queue_run(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
    req: &QueueRunRequest,
) -> Result<CommandAcceptedResponse, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/queue"),
        req,
    )
    .await
}

pub async fn cancel_run(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
    req: &CancelRunRequest,
) -> Result<CommandAcceptedResponse, ClientError> {
    post(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/cancel"),
        req,
    )
    .await
}

pub async fn list_execution_steps(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<ExecutionStepListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/steps"),
    )
    .await
}

pub async fn get_run_timeline(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<TimelineResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/timeline"),
    )
    .await
}

pub async fn get_runtime_result(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<RuntimeResultResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/result"),
    )
    .await
}

pub async fn list_usage_records(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<UsageRecordListResponse, ClientError> {
    get(
        base_url,
        bearer,
        &format!("/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/usage"),
    )
    .await
}

pub async fn subscribe_run_stream(
    base_url: &str,
    bearer: &str,
    company_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<reqwest::Response, ClientError> {
    let res = reqwest::Client::new()
        .get(format!(
            "{base_url}/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/events"
        ))
        .bearer_auth(bearer)
        .send()
        .await?;

    if res.status().is_success() {
        Ok(res)
    } else {
        Err(ClientError::from_response(res).await)
    }
}

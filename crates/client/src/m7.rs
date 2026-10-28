use crate::{ClientError, post};
use nalarvo_contracts::m7::{OrchestrationRequest, OrchestrationResponse};
pub async fn orchestrate(
    base: &str,
    token: &str,
    company: &str,
    project: &str,
    request: &OrchestrationRequest,
) -> Result<OrchestrationResponse, ClientError> {
    post(
        base,
        token,
        &format!("/api/v1/companies/{company}/projects/{project}/orchestration"),
        request,
    )
    .await
}

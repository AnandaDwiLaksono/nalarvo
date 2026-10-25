use crate::{ClientError, get, post};
use nalarvo_contracts::m6::*;
pub async fn list_artifacts(b: &str, t: &str, c: &str) -> Result<Vec<Artifact>, ClientError> {
    get(b, t, &format!("/api/v1/companies/{c}/artifacts")).await
}
pub async fn create_artifact(
    b: &str,
    t: &str,
    c: &str,
    i: &ArtifactInput,
) -> Result<ArtifactVersion, ClientError> {
    post(b, t, &format!("/api/v1/companies/{c}/artifacts"), i).await
}
pub async fn artifact_versions(
    b: &str,
    t: &str,
    c: &str,
    a: &str,
) -> Result<Vec<ArtifactVersion>, ClientError> {
    artifact_versions_page(b, t, c, a, &ArtifactVersionQuery::default()).await
}
pub async fn artifact_versions_page(
    b: &str,
    t: &str,
    c: &str,
    a: &str,
    page: &ArtifactVersionQuery,
) -> Result<Vec<ArtifactVersion>, ClientError> {
    get(
        b,
        t,
        &format!(
            "/api/v1/companies/{c}/artifacts/{a}/versions?after_version={}&limit={}",
            page.after_version, page.limit
        ),
    )
    .await
}
pub async fn add_artifact_version(
    b: &str,
    t: &str,
    c: &str,
    a: &str,
    i: &ArtifactInput,
) -> Result<ArtifactVersion, ClientError> {
    post(
        b,
        t,
        &format!("/api/v1/companies/{c}/artifacts/{a}/versions"),
        i,
    )
    .await
}
pub async fn artifact_content(
    b: &str,
    t: &str,
    c: &str,
    a: &str,
    v: &str,
) -> Result<ContentResponse, ClientError> {
    get(
        b,
        t,
        &format!("/api/v1/companies/{c}/artifacts/{a}/versions/{v}/content"),
    )
    .await
}
pub async fn create_source(
    b: &str,
    t: &str,
    c: &str,
    i: &SourceInput,
) -> Result<KnowledgeSource, ClientError> {
    post(b, t, &format!("/api/v1/companies/{c}/knowledge/sources"), i).await
}
pub async fn create_knowledge(
    b: &str,
    t: &str,
    c: &str,
    i: &InformationInput,
) -> Result<InformationRecord, ClientError> {
    post(b, t, &format!("/api/v1/companies/{c}/knowledge"), i).await
}
pub async fn revise_knowledge(
    b: &str,
    t: &str,
    c: &str,
    id: &str,
    i: &ReviseKnowledgeRequest,
) -> Result<InformationRecord, ClientError> {
    post(
        b,
        t,
        &format!("/api/v1/companies/{c}/knowledge/{id}/versions"),
        i,
    )
    .await
}
pub async fn capture_memory_candidate(
    b: &str,
    t: &str,
    c: &str,
    i: &InformationInput,
) -> Result<InformationRecord, ClientError> {
    post(b, t, &format!("/api/v1/companies/{c}/memory"), i).await
}
pub async fn list_knowledge(
    b: &str,
    t: &str,
    c: &str,
) -> Result<Vec<InformationRecord>, ClientError> {
    get(b, t, &format!("/api/v1/companies/{c}/knowledge")).await
}
pub async fn list_memory(b: &str, t: &str, c: &str) -> Result<Vec<InformationRecord>, ClientError> {
    get(b, t, &format!("/api/v1/companies/{c}/memory")).await
}
pub async fn get_information(
    b: &str,
    t: &str,
    c: &str,
    k: &str,
    id: &str,
) -> Result<InformationRecord, ClientError> {
    get(b, t, &format!("/api/v1/companies/{c}/information/{k}/{id}")).await
}
pub async fn transition_information(
    b: &str,
    t: &str,
    c: &str,
    k: &str,
    id: &str,
    i: &InformationTransition,
) -> Result<InformationRecord, ClientError> {
    post(
        b,
        t,
        &format!("/api/v1/companies/{c}/information/{k}/{id}/lifecycle"),
        i,
    )
    .await
}
pub async fn search_information(
    b: &str,
    t: &str,
    c: &str,
    i: &SearchRequest,
) -> Result<Vec<InformationRecord>, ClientError> {
    post(
        b,
        t,
        &format!("/api/v1/companies/{c}/information/search"),
        i,
    )
    .await
}
pub async fn context_snapshots(
    b: &str,
    t: &str,
    c: &str,
    r: &str,
) -> Result<Vec<ContextSnapshot>, ClientError> {
    get(
        b,
        t,
        &format!("/api/v1/companies/{c}/runs/{r}/context-snapshots"),
    )
    .await
}

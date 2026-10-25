use crate::ApiState;
fn app_error(error: nalarvo_application::ApplicationError) -> Response {
    crate::map_app_error(error).status()
}
fn app(s: &ApiState) -> Result<&nalarvo_application::ApplicationContext, Response> {
    s.app_ctx
        .as_ref()
        .ok_or(axum::http::StatusCode::SERVICE_UNAVAILABLE)
}
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode as Response,
    routing::{get, post},
};
use nalarvo_application::m6_information::RetrievalScope;
use nalarvo_contracts::m6::*;
fn convert<T: serde::Serialize, U: serde::de::DeserializeOwned>(input: T) -> Result<U, Response> {
    serde_json::from_value(serde_json::to_value(input).map_err(|_| {
        app_error(nalarvo_application::ApplicationError::Validation(
            "Invalid information".into(),
        ))
    })?)
    .map_err(|_| {
        app_error(nalarvo_application::ApplicationError::Validation(
            "Invalid information".into(),
        ))
    })
}
pub fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/v1/companies/{company}/artifacts",
            get(artifacts).post(create_artifact),
        )
        .route(
            "/api/v1/companies/{company}/artifacts/{artifact}/versions",
            get(versions).post(add_version),
        )
        .route(
            "/api/v1/companies/{company}/artifacts/{artifact}/versions/{version}/content",
            get(content),
        )
        .route(
            "/api/v1/companies/{company}/knowledge/sources",
            post(create_source),
        )
        .route(
            "/api/v1/companies/{company}/knowledge",
            get(knowledge).post(create_knowledge),
        )
        .route(
            "/api/v1/companies/{company}/knowledge/{id}/versions",
            post(revise),
        )
        .route(
            "/api/v1/companies/{company}/memory",
            get(memory).post(candidate),
        )
        .route(
            "/api/v1/companies/{company}/information/{kind}/{id}",
            get(information),
        )
        .route(
            "/api/v1/companies/{company}/information/{kind}/{id}/lifecycle",
            post(transition),
        )
        .route(
            "/api/v1/companies/{company}/information/search",
            post(search),
        )
        .route(
            "/api/v1/companies/{company}/runs/{run}/context-snapshots",
            get(snapshots),
        )
}
async fn artifacts(
    State(s): State<ApiState>,
    Path(c): Path<String>,
) -> Result<Json<Vec<Artifact>>, Response> {
    Ok(Json(app(&s)?.list_artifacts(&c).await.map_err(app_error)?))
}
async fn create_artifact(
    State(s): State<ApiState>,
    Path(c): Path<String>,
    Json(i): Json<ArtifactInput>,
) -> Result<Json<ArtifactVersion>, Response> {
    Ok(Json(
        app(&s)?
            .create_artifact(&c, convert(i)?)
            .await
            .map_err(app_error)?,
    ))
}
async fn versions(
    State(s): State<ApiState>,
    Path((c, a)): Path<(String, String)>,
    Query(page): Query<ArtifactVersionQuery>,
) -> Result<Json<Vec<ArtifactVersion>>, Response> {
    Ok(Json(
        app(&s)?
            .artifact_versions_page(&c, &a, page.after_version, page.limit)
            .await
            .map_err(app_error)?,
    ))
}
async fn add_version(
    State(s): State<ApiState>,
    Path((c, a)): Path<(String, String)>,
    Json(i): Json<ArtifactInput>,
) -> Result<Json<ArtifactVersion>, Response> {
    Ok(Json(
        app(&s)?
            .add_artifact_version(&c, &a, convert(i)?)
            .await
            .map_err(app_error)?,
    ))
}
async fn content(
    State(s): State<ApiState>,
    Path((c, a, v)): Path<(String, String, String)>,
) -> Result<Json<ContentResponse>, Response> {
    let bytes = app(&s)?
        .read_artifact_content(&c, &a, &v)
        .await
        .map_err(app_error)?;
    Ok(Json(ContentResponse {
        content: String::from_utf8(bytes).map_err(|_| {
            app_error(nalarvo_application::ApplicationError::Validation(
                "Invalid text".into(),
            ))
        })?,
    }))
}
async fn create_source(
    State(s): State<ApiState>,
    Path(c): Path<String>,
    Json(i): Json<SourceInput>,
) -> Result<Json<KnowledgeSource>, Response> {
    Ok(Json(
        app(&s)?
            .create_knowledge_source(&c, convert(i)?)
            .await
            .map_err(app_error)?,
    ))
}
async fn knowledge(
    State(s): State<ApiState>,
    Path(c): Path<String>,
) -> Result<Json<Vec<InformationRecord>>, Response> {
    Ok(Json(
        app(&s)?
            .list_information(&c, "KNOWLEDGE")
            .await
            .map_err(app_error)?,
    ))
}
async fn memory(
    State(s): State<ApiState>,
    Path(c): Path<String>,
) -> Result<Json<Vec<InformationRecord>>, Response> {
    Ok(Json(
        app(&s)?
            .list_information(&c, "MEMORY")
            .await
            .map_err(app_error)?,
    ))
}
async fn create_knowledge(
    State(s): State<ApiState>,
    Path(c): Path<String>,
    Json(i): Json<InformationInput>,
) -> Result<Json<InformationRecord>, Response> {
    Ok(Json(
        app(&s)?
            .create_knowledge(&c, convert(i)?)
            .await
            .map_err(app_error)?,
    ))
}
async fn candidate(
    State(s): State<ApiState>,
    Path(c): Path<String>,
    Json(i): Json<InformationInput>,
) -> Result<Json<InformationRecord>, Response> {
    Ok(Json(
        app(&s)?
            .capture_memory_candidate(&c, convert(i)?)
            .await
            .map_err(app_error)?,
    ))
}
async fn revise(
    State(s): State<ApiState>,
    Path((c, id)): Path<(String, String)>,
    Json(i): Json<ReviseKnowledgeRequest>,
) -> Result<Json<InformationRecord>, Response> {
    Ok(Json(
        app(&s)?
            .revise_knowledge(&c, &id, convert(i.input)?, i.expected_version)
            .await
            .map_err(app_error)?,
    ))
}
async fn information(
    State(s): State<ApiState>,
    Path((c, k, id)): Path<(String, String, String)>,
) -> Result<Json<InformationRecord>, Response> {
    Ok(Json(
        app(&s)?
            .get_information(&c, &k, &id)
            .await
            .map_err(app_error)?,
    ))
}
async fn transition(
    State(s): State<ApiState>,
    Path((c, k, id)): Path<(String, String, String)>,
    Json(i): Json<InformationTransition>,
) -> Result<Json<InformationRecord>, Response> {
    Ok(Json(
        app(&s)?
            .transition_information(&c, &k, &id, i.lifecycle_state, i.expected_version)
            .await
            .map_err(app_error)?,
    ))
}
async fn search(
    State(s): State<ApiState>,
    Path(c): Path<String>,
    Json(i): Json<SearchRequest>,
) -> Result<Json<Vec<InformationRecord>>, Response> {
    Ok(Json(
        app(&s)?
            .search_information(
                &c,
                &RetrievalScope {
                    project_id: i.project_id,
                    agent_id: i.agent_id,
                },
                &i.query,
            )
            .await
            .map_err(app_error)?,
    ))
}
async fn snapshots(
    State(s): State<ApiState>,
    Path((c, r)): Path<(String, String)>,
) -> Result<Json<Vec<ContextSnapshot>>, Response> {
    Ok(Json(
        app(&s)?
            .context_snapshots(&c, &r)
            .await
            .map_err(app_error)?,
    ))
}

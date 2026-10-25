//! Typed M6 wire values share the path-free domain information vocabulary.
pub use nalarvo_domain::information::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArtifactVersionQuery {
    pub after_version: i64,
    pub limit: i64,
}
impl Default for ArtifactVersionQuery {
    fn default() -> Self {
        Self {
            after_version: 0,
            limit: 100,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
    pub title: String,
    pub artifact_type: String,
    pub project_id: Option<String>,
    pub work_item_id: Option<String>,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub media_type: String,
    pub content: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInput {
    pub title: String,
    pub project_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InformationInput {
    pub title: String,
    pub content: String,
    pub scope: InformationScope,
    pub project_id: Option<String>,
    pub agent_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub source_id: Option<String>,
    pub source_run_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub query: String,
    pub project_id: Option<String>,
    pub agent_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InformationTransition {
    pub lifecycle_state: InformationLifecycle,
    pub expected_version: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviseKnowledgeRequest {
    pub input: InformationInput,
    pub expected_version: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentResponse {
    pub content: String,
}

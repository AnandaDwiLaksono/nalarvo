//! Information values contain identities, never local storage paths.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    pub hash: String,
    pub size_bytes: u64,
}
impl ContentRef {
    pub fn valid(&self) -> bool {
        self.hash.len() == 64
            && self
                .hash
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            && self.size_bytes <= 1024 * 1024
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Sensitivity {
    Public,
    Internal,
    Restricted,
    Secret,
}
impl Sensitivity {
    pub fn retrievable(self) -> bool {
        matches!(self, Self::Public | Self::Internal)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "PUBLIC",
            Self::Internal => "INTERNAL",
            Self::Restricted => "RESTRICTED",
            Self::Secret => "SECRET",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactVersion {
    pub id: String,
    pub artifact_id: String,
    pub company_id: String,
    pub version_no: i64,
    pub content_ref: ContentRef,
    pub media_type: String,
    pub sensitivity: Sensitivity,
    pub provenance: serde_json::Value,
    pub created_at: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub company_id: String,
    pub title: String,
    pub artifact_type: String,
    pub project_id: Option<String>,
    pub current_version_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InformationScope {
    Company,
    Project,
    Agent,
}
impl InformationScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Company => "COMPANY",
            Self::Project => "PROJECT",
            Self::Agent => "AGENT",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InformationLifecycle {
    Draft,
    Active,
    Retired,
}
impl InformationLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Active => "ACTIVE",
            Self::Retired => "RETIRED",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeSource {
    pub id: String,
    pub company_id: String,
    pub title: String,
    pub project_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub id: String,
    pub company_id: String,
    pub run_id: String,
    pub invocation_id: String,
    pub references: serde_json::Value,
    pub domain_versions: serde_json::Value,
    pub model_binding: serde_json::Value,
    pub prompt_hash: String,
    pub builder_version: String,
    pub estimated_tokens: i64,
    pub created_at: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InformationRecord {
    pub id: String,
    pub kind: String,
    pub company_id: String,
    pub title: String,
    pub scope: InformationScope,
    pub project_id: Option<String>,
    pub agent_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub lifecycle_state: InformationLifecycle,
    pub row_version: i64,
    pub version_id: String,
    pub version_no: i64,
    pub content_ref: ContentRef,
    pub provenance: serde_json::Value,
    pub updated_at: String,
}

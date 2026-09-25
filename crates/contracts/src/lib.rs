use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
}

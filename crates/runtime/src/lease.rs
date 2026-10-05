// crates/runtime/src/lease.rs
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("run already has an active execution owner")]
    AlreadyOwned,
    #[error("lease owner mismatch or expired")]
    InvalidOwner,
    #[error("persistence error: {0}")]
    Persistence(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLease {
    pub id: String,
    pub run_id: String,
    pub worker_principal_id: String,
    pub lease_version: i64,
    pub expires_after: Duration,
}

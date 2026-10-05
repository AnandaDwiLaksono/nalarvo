// crates/runtime/src/recovery.rs
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecoveryError {
    #[error("cannot resume terminal run")]
    TerminalRun,
    #[error("unsupported recovery state: {0}")]
    UnsupportedState(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryDecision {
    ResumeSafe,
    RequeueJob,
    FailSafe(String),
}

pub fn decide_recovery(
    run_status: &str,
    safe_to_resume: bool,
    completed_steps: u32,
) -> Result<RecoveryDecision, RecoveryError> {
    match run_status {
        "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "CANCELLED" => Err(RecoveryError::TerminalRun),
        "QUEUED" => Ok(RecoveryDecision::RequeueJob),
        "RUNNING" => {
            if safe_to_resume {
                Ok(RecoveryDecision::ResumeSafe)
            } else if completed_steps == 0 {
                Ok(RecoveryDecision::RequeueJob)
            } else {
                Ok(RecoveryDecision::FailSafe(
                    "worker lost during non-idempotent phase".into(),
                ))
            }
        }
        "PAUSED" | "WAITING_APPROVAL" | "WAITING_DEPENDENCY" => Ok(RecoveryDecision::RequeueJob),
        other => Err(RecoveryError::UnsupportedState(other.to_string())),
    }
}

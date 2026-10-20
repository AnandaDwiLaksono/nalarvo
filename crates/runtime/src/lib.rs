// crates/runtime/src/lib.rs
// Runtime Supervisor, Run Worker, Context Engine, and Execution Lease tracking.

pub mod context;
pub mod core_dispatch;
pub mod core_model;
pub mod lease;
pub mod native_tools;
pub mod recovery;
pub mod shell;
pub mod supervisor;
pub mod worker;

pub use supervisor::{
    RuntimeSupervisor, SupervisorConfig, SupervisorHandle, ensure_current_lease,
    start_runtime_supervisor, start_runtime_supervisor_with_handler,
    start_runtime_supervisor_with_handlers,
};

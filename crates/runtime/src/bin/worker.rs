// crates/runtime/src/bin/worker.rs
// Nalarvo Run Worker process entrypoint and self-test/control boundary.

use nalarvo_domain::RunStatus;
use nalarvo_model_gateway::MockMode;
use nalarvo_runtime::context::ContextInput;
use nalarvo_runtime::worker::{RunExecutionRequest, execute_run_worker, run_worker_stdio};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|a| a == "--self-test") {
        let req = RunExecutionRequest {
            run_id: "self-test-run".into(),
            company_id: "company-self-test".into(),
            project_id: "project-self-test".into(),
            work_item_id: "work-self-test".into(),
            agent_id: "agent-self-test".into(),
            model_profile_version_id: None,
            provider_connection_id: "conn-self-test".into(),
            base_url: None,
            auth_token: None,
            model_key: "mock-model".into(),
            context_input: ContextInput::minimal(
                "company-self-test",
                "project-self-test",
                "Self Test Work",
            ),
            worker_principal_id: "system:worker-self-test".into(),
            lease_id: "lease-self-test".into(),
            lease_version: 1,
            pool: None,
            expected_run_version: 1,
            mock_mode: MockMode::SUCCESS_TEXT,
            cancel_rx: None,
        };

        let outcome = execute_run_worker(req, None).await;
        if outcome.status == RunStatus::Succeeded {
            println!("OK: self-test passed");
            return Ok(());
        } else {
            eprintln!("FAIL: self-test failed: {:?}", outcome.failure_class);
            std::process::exit(1);
        }
    }

    run_worker_stdio().await?;
    Ok(())
}

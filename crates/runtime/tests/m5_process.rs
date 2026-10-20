use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

async fn send(writer: &mut (impl AsyncWriteExt + Unpin), frame: Value) {
    let bytes = serde_json::to_vec(&frame).unwrap();
    writer.write_u32(bytes.len() as u32).await.unwrap();
    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();
}
async fn receive(reader: &mut (impl AsyncReadExt + Unpin)) -> Value {
    let size = reader.read_u32().await.unwrap();
    assert!(size > 0 && size <= 1024 * 1024);
    let mut bytes = vec![0; size as usize];
    reader.read_exact(&mut bytes).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn parent_core_handler_receives_tool_proposal_and_returns_action_result() {
    let root = tempfile::tempdir().unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let captured = seen.clone();
    let handler: nalarvo_runtime::worker::ActionProposalHandler =
        std::sync::Arc::new(move |ctx, proposal| {
            let captured = captured.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((ctx, proposal.tool_name));
                Ok(nalarvo_runtime::worker::WorkerExecutionOutcome {
                    run_id: "run".into(),
                    status: nalarvo_domain::RunStatus::Succeeded,
                    result_summary: "executed".into(),
                    output_text: Some("done".into()),
                    failure_class: None,
                    failure_detail: None,
                    input_tokens: None,
                    output_tokens: None,
                    latency_ms: None,
                    context_step_status: nalarvo_domain::ExecutionStepStatus::Succeeded,
                    model_step_status: nalarvo_domain::ExecutionStepStatus::Succeeded,
                })
            })
        });
    let req = nalarvo_runtime::worker::RunExecutionRequest {
        run_id: "run-trusted-1".into(),
        company_id: "company-trusted-1".into(),
        project_id: "proj-trusted-1".into(),
        work_item_id: "wi-trusted-1".into(),
        agent_id: "agent-trusted-1".into(),
        model_profile_version_id: None,
        provider_connection_id: "pc".into(),
        base_url: None,
        auth_token: None,
        model_key: "model".into(),
        context_input: nalarvo_runtime::context::ContextInput::minimal(
            "company-trusted-1",
            "proj-trusted-1",
            "Write note",
        ),
        worker_principal_id: "worker-principal-1".into(),
        lease_id: "lease-trusted-1".into(),
        lease_version: 42,
        pool: None,
        expected_run_version: 2,
        mock_mode: nalarvo_model_gateway::MockMode::TOOL_PROPOSAL,
        cancel_rx: None,
        action_handler: Some(handler),
        model_handler: None,
        resume_proposal: None,
    };
    let result = nalarvo_runtime::worker::execute_run_worker_process(
        std::path::Path::new(env!("CARGO_BIN_EXE_nalarvo-worker")),
        req,
        None,
    )
    .await;
    assert!(result.unwrap().output_text.is_some());
    let (captured_ctx, tool_name) = seen.lock().unwrap().take().unwrap();
    assert_eq!(tool_name, "filesystem.write");
    assert_eq!(captured_ctx.run_id, "run-trusted-1");
    assert_eq!(captured_ctx.company_id, "company-trusted-1");
    assert_eq!(captured_ctx.project_id, "proj-trusted-1");
    assert_eq!(captured_ctx.work_item_id, "wi-trusted-1");
    assert_eq!(captured_ctx.agent_id, "agent-trusted-1");
    assert_eq!(captured_ctx.lease_id, "lease-trusted-1");
    assert_eq!(captured_ctx.lease_version, 42);
    assert_eq!(captured_ctx.worker_principal_id, "worker-principal-1");
    assert!(!root.path().join("note.txt").exists());
}

#[tokio::test]
async fn real_child_proposes_tool_to_core_without_executing_it() {
    let root = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_nalarvo-worker"))
        .current_dir(root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    let context = nalarvo_runtime::context::ContextInput::minimal("c", "p", "Write note");
    send(
        &mut input,
        json!({"Start": {"session_id":"session", "request": {
            "run_id":"run", "company_id":"c", "project_id":"p", "work_item_id":"wi",
            "agent_id":"a", "model_profile_version_id":null, "provider_connection_id":"pc",
            "model_key":"model", "context_input":context, "worker_principal_id":"worker",
            "lease_id":"lease", "expected_run_version":2, "mock_mode":"SUCCESS_TEXT"
        }}}),
    )
    .await;
    assert_eq!(
        receive(&mut output).await["ProviderCall"]["session_id"],
        "session"
    );
    send(
        &mut input,
        json!({"ProviderResult": {"session_id":"session", "result":{"Ok":{
            "text":null, "structured":{"tool_name":"filesystem.write", "target":"note.txt",
                "arguments":{"path":"note.txt","content":"approved content"}},
            "usage":null, "request_id":null
        }}}}),
    )
    .await;
    let proposal = tokio::time::timeout(Duration::from_secs(5), receive(&mut output))
        .await
        .unwrap();
    assert_eq!(
        proposal["ToolProposal"]["session_id"], "session",
        "child must send framed proposal, not terminal success"
    );
    assert_eq!(
        proposal["ToolProposal"]["proposal"]["tool_name"],
        "filesystem.write"
    );
    assert!(
        !root.path().join("note.txt").exists(),
        "worker has no tool execution authority"
    );
    send(
        &mut input,
        json!({"ActionResult":{"session_id":"session", "outcome":{
            "run_id":"run", "status":"WAITING_APPROVAL", "result_summary":"Approval required",
            "output_text":null, "failure_class":null, "failure_detail":null,
            "input_tokens":null, "output_tokens":null, "latency_ms":null,
            "context_step_status":"SUCCEEDED", "model_step_status":"SUCCEEDED"
        }}}),
    )
    .await;
    assert_eq!(
        receive(&mut output).await["Outcome"]["outcome"]["status"],
        "WAITING_APPROVAL"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!root.path().join("note.txt").exists());
}

#[tokio::test]
async fn real_child_continuation_proposes_saved_operation_without_model_request() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nalarvo-worker"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    let proposal = json!({"call_id":"write-1","tool_name":"filesystem.write",
        "target":"note.txt","arguments":{"path":"note.txt","content":"checked"}});
    send(&mut input, json!({"Start":{"session_id":"resume","request":{
        "run_id":"run","company_id":"c","project_id":"p","work_item_id":"wi",
        "agent_id":"a","model_profile_version_id":null,"provider_connection_id":"pc",
        "model_key":"model","context_input":nalarvo_runtime::context::ContextInput::minimal("c","p","write"),
        "worker_principal_id":"worker-b","lease_id":"lease-b","expected_run_version":4,
        "mock_mode":"TIMEOUT","resume_proposal":proposal
    }}})).await;
    let first = tokio::time::timeout(Duration::from_secs(10), receive(&mut output))
        .await
        .unwrap();
    assert_eq!(first["ToolProposal"]["proposal"], proposal);
    assert!(first.get("ProviderCall").is_none());
    send(
        &mut input,
        json!({"ActionResult":{"session_id":"resume","outcome":{
            "run_id":"run","status":"SUCCEEDED","result_summary":"done","output_text":null,
            "failure_class":null,"failure_detail":null,"input_tokens":null,"output_tokens":null,
            "latency_ms":null,"context_step_status":"SUCCEEDED","model_step_status":"SUCCEEDED"
        }}}),
    )
    .await;
    assert_eq!(
        receive(&mut output).await["Outcome"]["outcome"]["status"],
        "SUCCEEDED"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

use nalarvo_contracts::*;
use serde_json::{Value, json};

fn round_trip<T>(value: Value) -> Value
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    serde_json::to_value(serde_json::from_value::<T>(value).unwrap()).unwrap()
}

#[test]
fn public_run_commands_and_views_are_scoped_and_typed() {
    let run = round_trip::<RunDto>(json!({
        "id":"run-1","company_id":"company-1","project_id":"project-1",
        "work_item_id":"work-1","assignment_id":"assignment-1",
        "executing_agent_id":"agent-1","lifecycle_state":"QUEUED",
        "trigger_type":"MANUAL","attempt_number":1,"retry_of_run_id":null,
        "model_profile_version_id":"profile-version-1","requested_by_type":"USER",
        "requested_by_id":"user-1","queued_at":"2026-10-01T00:00:00Z",
        "started_at":null,"completed_at":null,"failure_class":null,
        "failure_detail":null,"correlation_id":"correlation-1","causation_id":null,
        "row_version":1,"created_at":"2026-10-01T00:00:00Z",
        "updated_at":"2026-10-01T00:00:00Z"
    }));
    assert_eq!(run["lifecycle_state"], "QUEUED");
    round_trip::<CreateRunRequest>(json!({
        "work_item_id":"work-1","assignment_id":"assignment-1",
        "executing_agent_id":"agent-1","trigger_type":"MANUAL",
        "retry_of_run_id":null
    }));
    round_trip::<RunListResponse>(json!({"runs":[run.clone()]}));
    round_trip::<RunShowResponse>(json!({"run":run}));

    for value in [
        round_trip::<QueueRunRequest>(json!({"expected_version":1})),
        round_trip::<PauseRunRequest>(json!({"expected_version":1})),
        round_trip::<ResumeRunRequest>(json!({"expected_version":1})),
        round_trip::<CancelRunRequest>(json!({"expected_version":1,"reason":"operator"})),
    ] {
        assert_eq!(value["expected_version"], 1);
    }

    let accepted = round_trip::<CommandAcceptedResponse>(json!({
        "run_id":"run-1","command":"QUEUE","accepted_at":"2026-10-01T00:00:00Z"
    }));
    assert_eq!(accepted["command"], "QUEUE");
}

#[test]
fn run_detail_records_have_stable_envelopes() {
    let step = round_trip::<ExecutionStepDto>(json!({
        "id":"step-1","company_id":"company-1","run_id":"run-1","sequence_no":1,
        "step_type":"MODEL","lifecycle_state":"SUCCEEDED","parent_step_id":null,
        "input_metadata":{"context_refs":["work-1"]},
        "output_metadata":{"finish_reason":"stop"},"failure_class":null,
        "failure_detail":null,"started_at":"2026-10-01T00:00:01Z",
        "completed_at":"2026-10-01T00:00:02Z","created_at":"2026-10-01T00:00:01Z"
    }));
    round_trip::<ExecutionStepListResponse>(json!({"steps":[step]}));

    let result = round_trip::<RuntimeResultDto>(json!({
        "id":"result-1","company_id":"company-1","run_id":"run-1",
        "run_status":"SUCCEEDED","result_summary":"Completed",
        "output_payload":{"text":"answer"},"output_metadata":{"finish_reason":"stop"},
        "resource_usage_summary":{"input_tokens":10,"output_tokens":4},
        "failure_class":null,"failure_detail":null,"warnings":[],
        "correlation_id":"correlation-1","causation_id":null,
        "created_at":"2026-10-01T00:00:02Z"
    }));
    round_trip::<RuntimeResultResponse>(json!({"result":result}));

    let usage = round_trip::<UsageRecordDto>(json!({
        "id":"usage-1","workspace_id":"workspace-1","company_id":"company-1",
        "project_id":"project-1","work_item_id":"work-1","agent_id":"agent-1",
        "run_id":"run-1","step_id":"step-1","provider_connection_id":"provider-1",
        "model_id":"model-1","usage_type":"TOKENS","quantity":14,"unit":"TOKEN",
        "estimated_cost":0.002,"occurred_at":"2026-10-01T00:00:02Z",
        "metadata":{"input_tokens":10,"output_tokens":4},
        "created_at":"2026-10-01T00:00:02Z"
    }));
    round_trip::<UsageRecordListResponse>(json!({"usage_records":[usage]}));

    let timeline = round_trip::<TimelineEventDto>(json!({
        "event_id":"event-1","run_id":"run-1","sequence_no":1,
        "event_type":"RUN_QUEUED","occurred_at":"2026-10-01T00:00:00Z",
        "payload":{"lifecycle_state":"QUEUED"}
    }));
    round_trip::<TimelineResponse>(json!({"events":[timeline]}));
}

#[test]
fn normalized_model_sse_events_are_tagged_and_provider_neutral() {
    let cases = [
        json!({"event_type":"STARTED","run_id":"run-1","step_id":"step-1","invocation_index":1}),
        json!({"event_type":"OUTPUT_DELTA","run_id":"run-1","step_id":"step-1","text":"hello"}),
        json!({"event_type":"USAGE","run_id":"run-1","step_id":"step-1","input_tokens":10,"output_tokens":4}),
        json!({"event_type":"COMPLETED","run_id":"run-1","step_id":"step-1","finish_reason":"stop"}),
        json!({"event_type":"FAILED","run_id":"run-1","step_id":"step-1","failure_class":"PROVIDER_TIMEOUT","message":"timed out"}),
    ];
    for value in cases {
        assert_eq!(round_trip::<NormalizedModelEvent>(value.clone()), value);
    }
}

#[test]
fn internal_worker_control_contract_is_lease_bound() {
    round_trip::<WorkerExecutionRequest>(json!({
        "run_id":"run-1","company_id":"company-1","project_id":"project-1",
        "work_item_id":"work-1","executing_agent_id":"agent-1",
        "model_profile_version_id":"profile-version-1","correlation_id":"correlation-1"
    }));
    round_trip::<WorkerLeaseRequest>(json!({
        "run_id":"run-1","worker_principal_id":"worker-1","lease_duration_seconds":30
    }));
    round_trip::<WorkerLeaseResponse>(json!({
        "lease_id":"lease-1","run_id":"run-1","lease_version":1,
        "expires_at":"2026-10-01T00:00:30Z"
    }));
    round_trip::<WorkerHeartbeatRequest>(json!({
        "run_id":"run-1","lease_id":"lease-1","lease_version":1
    }));
    round_trip::<WorkerStepRequest>(json!({
        "run_id":"run-1","lease_id":"lease-1","lease_version":1,
        "sequence_no":1,"step_type":"MODEL","lifecycle_state":"RUNNING",
        "parent_step_id":null,"input_metadata":{"context_refs":["work-1"]},
        "output_metadata":null,"failure_class":null,"failure_detail":null,
        "started_at":"2026-10-01T00:00:01Z","completed_at":null
    }));
    round_trip::<WorkerCheckpointRequest>(json!({
        "run_id":"run-1","lease_id":"lease-1","lease_version":1,
        "checkpoint_version":1,"run_state":"RUNNING","last_completed_step":null,
        "active_step":1,"execution_phase":"MODEL","context_refs":["work-1"],
        "continuation_metadata":{"invocation_index":1},
        "usage_snapshot":{"input_tokens":10},"safe_to_resume":true
    }));
    round_trip::<WorkerResultRequest>(json!({
        "run_id":"run-1","lease_id":"lease-1","lease_version":1,
        "run_status":"SUCCEEDED","result_summary":"Completed",
        "output_payload":{"text":"answer"},"output_metadata":{"finish_reason":"stop"},
        "resource_usage_summary":{"input_tokens":10,"output_tokens":4},"warnings":[]
    }));
    round_trip::<WorkerTerminalRequest>(json!({
        "run_id":"run-1","lease_id":"lease-1","lease_version":1,
        "lifecycle_state":"SUCCEEDED","failure_class":null,"failure_detail":null,
        "completed_at":"2026-10-01T00:00:02Z"
    }));
}

#[test]
fn execution_transport_rejects_secret_reasoning_and_raw_provider_fields() {
    let forbidden = [
        "provider_secret",
        "private_reasoning",
        "raw_prompt",
        "raw_body",
    ];
    for key in forbidden {
        let mut request = json!({
            "run_id":"run-1","company_id":"company-1","project_id":"project-1",
            "work_item_id":"work-1","executing_agent_id":"agent-1",
            "model_profile_version_id":null,"correlation_id":"correlation-1"
        });
        request[key] = json!("must-not-cross-boundary");
        assert!(serde_json::from_value::<WorkerExecutionRequest>(request).is_err());
    }

    let serialized = serde_json::to_string(&NormalizedModelEvent::OutputDelta {
        run_id: "run-1".into(),
        step_id: "step-1".into(),
        text: "safe output".into(),
    })
    .unwrap();
    assert!(forbidden.iter().all(|key| !serialized.contains(key)));
    for key in forbidden {
        let mut event = json!({
            "event_type":"OUTPUT_DELTA","run_id":"run-1","step_id":"step-1",
            "text":"safe output"
        });
        event[key] = json!("must-not-cross-boundary");
        assert!(serde_json::from_value::<NormalizedModelEvent>(event).is_err());
    }
}

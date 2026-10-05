use axum::{
    Json, Router,
    extract::Path,
    response::IntoResponse,
    routing::{get, post},
};
use nalarvo_client::m4::*;
use nalarvo_contracts::{CancelRunRequest, CreateRunRequest, QueueRunRequest};
use serde_json::{Value, json};
use tokio::net::TcpListener;

fn run() -> Value {
    json!({
        "id":"run","company_id":"co","project_id":"project","work_item_id":"work",
        "assignment_id":null,"executing_agent_id":"agent","lifecycle_state":"QUEUED",
        "trigger_type":"MANUAL","attempt_number":1,"retry_of_run_id":null,
        "model_profile_version_id":null,"requested_by_type":"USER","requested_by_id":"user",
        "queued_at":"now","started_at":null,"completed_at":null,"failure_class":null,
        "failure_detail":null,"correlation_id":"correlation","causation_id":null,
        "row_version":1,"created_at":"now","updated_at":"now"
    })
}

#[tokio::test]
async fn typed_run_client_uses_scoped_routes() {
    let app = Router::new()
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs",
            get(|Path((co, project)): Path<(String, String)>| async move {
                assert_eq!((co.as_str(), project.as_str()), ("co", "project"));
                Json(json!({"runs":[run()]}))
            })
            .post(|Json(body): Json<Value>| async move {
                assert_eq!(body["work_item_id"], "work");
                Json(run())
            }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}",
            get(|Path((co, project, run_id)): Path<(String, String, String)>| async move {
                assert_eq!((co.as_str(), project.as_str(), run_id.as_str()), ("co", "project", "run"));
                Json(json!({"run":run()}))
            }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/queue",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body, json!({"expected_version":1}));
                Json(json!({"run_id":"run","command":"QUEUE","accepted_at":"now"}))
            }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/cancel",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body, json!({"expected_version":1,"reason":"operator"}));
                Json(json!({"run_id":"run","command":"CANCEL","accepted_at":"now"}))
            }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/steps",
            get(|| async { Json(json!({"steps":[]})) }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/timeline",
            get(|| async { Json(json!({"events":[]})) }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/result",
            get(|| async { Json(json!({"result":{"id":"result","company_id":"co","run_id":"run","run_status":"SUCCEEDED","result_summary":"done","output_payload":null,"output_metadata":null,"resource_usage_summary":null,"failure_class":null,"failure_detail":null,"warnings":[],"correlation_id":"correlation","causation_id":null,"created_at":"now"}})) }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/usage",
            get(|| async { Json(json!({"usage_records":[]})) }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/events",
            get(|| async { ([("content-type", "text/event-stream")], "event: READY\ndata: {}\n\n").into_response() }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let create = CreateRunRequest {
        work_item_id: "work".into(),
        assignment_id: None,
        executing_agent_id: "agent".into(),
        trigger_type: "MANUAL".into(),
        retry_of_run_id: None,
    };
    assert_eq!(
        create_run(&base, "token", "co", "project", &create)
            .await
            .unwrap()
            .id,
        "run"
    );
    assert_eq!(
        list_runs(&base, "token", "co", "project")
            .await
            .unwrap()
            .runs[0]
            .id,
        "run"
    );
    assert_eq!(
        get_run(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .run
            .id,
        "run"
    );
    assert_eq!(
        queue_run(
            &base,
            "token",
            "co",
            "project",
            "run",
            &QueueRunRequest {
                expected_version: 1
            }
        )
        .await
        .unwrap()
        .command,
        "QUEUE"
    );
    assert_eq!(
        cancel_run(
            &base,
            "token",
            "co",
            "project",
            "run",
            &CancelRunRequest {
                expected_version: 1,
                reason: Some("operator".into())
            }
        )
        .await
        .unwrap()
        .command,
        "CANCEL"
    );
    assert!(
        list_execution_steps(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .steps
            .is_empty()
    );
    assert!(
        get_run_timeline(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert_eq!(
        get_runtime_result(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .result
            .id,
        "result"
    );
    assert!(
        list_usage_records(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .usage_records
            .is_empty()
    );
    assert!(
        subscribe_run_stream(&base, "token", "co", "project", "run")
            .await
            .unwrap()
            .status()
            .is_success()
    );
}

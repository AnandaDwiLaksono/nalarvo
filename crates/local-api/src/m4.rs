use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use nalarvo_contracts::*;
use nalarvo_domain::{CompanyId, PrincipalRef, RunStatus};
use nalarvo_persistence as persistence;
use tokio_stream::StreamExt;

use crate::{ApiState, m3_meta, map_app_error, service_unavailable};

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs",
            get(list_runs).post(create_run),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}",
            get(get_run),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/queue",
            post(queue_run),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/cancel",
            post(cancel_run),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/steps",
            get(list_steps),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/timeline",
            get(get_timeline),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/result",
            get(get_result),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/usage",
            get(list_usage),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/runs/{run_id}/events",
            get(run_events_sse),
        )
}

async fn create_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path((company_id, project_id)): Path<(String, String)>,
    Json(req): Json<CreateRunRequest>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    let meta = m3_meta(&headers);
    let principal = meta
        .principal
        .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));
    let correlation_id = meta
        .correlation_id
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());

    let mut run = match nalarvo_domain::Run::create(
        CompanyId(company_id.clone()),
        project_id,
        req.work_item_id,
        req.executing_agent_id,
        req.trigger_type,
        principal,
        correlation_id,
    ) {
        Ok(r) => r,
        Err(e) => return map_app_error(e.into()),
    };
    run.assignment_id = req.assignment_id;
    run.retry_of_run_id = req.retry_of_run_id;

    match persistence::m4::create_run(&app_ctx.pool, &run).await {
        Ok(()) => (StatusCode::CREATED, Json(map_run(run))).into_response(),
        Err(e) => map_app_error(e.into()),
    }
}

async fn list_runs(
    State(state): State<ApiState>,
    Path((company_id, project_id)): Path<(String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::list_runs(&app_ctx.pool, &CompanyId(company_id), &project_id).await {
        Ok(runs) => Json(RunListResponse {
            runs: runs.into_iter().map(map_run).collect(),
        })
        .into_response(),
        Err(e) => map_app_error(e.into()),
    }
}

async fn get_run(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id), &run_id).await {
        Ok(Some(run)) if run.project_id == project_id => {
            Json(RunShowResponse { run: map_run(run) }).into_response()
        }
        Ok(_) => map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => map_app_error(e.into()),
    }
}

async fn queue_run(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
    Json(req): Json<QueueRunRequest>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    let run = match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id)
        .await
    {
        Ok(Some(r)) if r.project_id == project_id => r,
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let payload = match serde_json::to_value(&nalarvo_contracts::WorkerExecutionRequest {
        run_id: run.id.clone(),
        company_id: run.company_id.0.clone(),
        project_id: run.project_id.clone(),
        work_item_id: run.work_item_id.clone(),
        executing_agent_id: run.executing_agent_id.clone(),
        model_profile_version_id: run.model_profile_version_id.clone(),
        correlation_id: run.correlation_id.clone(),
    }) {
        Ok(v) => v,
        Err(e) => {
            return map_app_error(nalarvo_domain::DomainError::Validation(e.to_string()).into());
        }
    };

    let job = persistence::m4::DurableJob {
        id: uuid::Uuid::now_v7().to_string(),
        job_type: "EXECUTE_RUN".into(),
        company_id: run.company_id.clone(),
        run_id: run.id.clone(),
        payload,
        available_at: chrono::Utc::now(),
        priority: 0,
        attempt: 0,
        max_attempts: 3,
        correlation_id: run.correlation_id.clone(),
    };

    let event = nalarvo_domain::DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        event_type: "RunQueued".into(),
        schema_version: 1,
        company_id: run.company_id.clone(),
        aggregate_type: "Run".into(),
        aggregate_id: run.id.clone(),
        aggregate_version: req.expected_version + 1,
        occurred_at: chrono::Utc::now(),
        correlation_id: run.correlation_id.clone(),
        causation_id: run
            .causation_id
            .unwrap_or_else(|| run.correlation_id.clone()),
        principal: run.requested_by,
        scope: nalarvo_domain::ScopeRef::company(&run.company_id.0),
        payload: serde_json::json!({ "lifecycle_state": "QUEUED" }),
    };

    match persistence::m4::queue_run(
        &app_ctx.pool,
        &CompanyId(company_id),
        &run_id,
        req.expected_version,
        &job,
        &event,
    )
    .await
    {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(CommandAcceptedResponse {
                run_id,
                command: "QUEUE".into(),
                accepted_at: chrono::Utc::now().to_rfc3339(),
            }),
        )
            .into_response(),
        Err(e) => map_app_error(e.into()),
    }
}

async fn cancel_run(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
    Json(req): Json<CancelRunRequest>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => r,
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    match persistence::m4::transition_run(
        &app_ctx.pool,
        &CompanyId(company_id),
        &run_id,
        req.expected_version,
        RunStatus::Cancelled,
        None,
        req.reason.as_deref(),
    )
    .await
    {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(CommandAcceptedResponse {
                run_id,
                command: "CANCEL".into(),
                accepted_at: chrono::Utc::now().to_rfc3339(),
            }),
        )
            .into_response(),
        Err(e) => map_app_error(e.into()),
    }
}

async fn list_steps(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => (),
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let rows = match sqlx::query(
        "SELECT id, company_id, run_id, sequence_no, step_type, lifecycle_state, parent_step_id, input_metadata, output_metadata, failure_class, failure_detail, started_at, completed_at, created_at FROM execution_steps WHERE company_id = ? AND run_id = ? ORDER BY sequence_no"
    )
    .bind(&company_id)
    .bind(&run_id)
    .fetch_all(&app_ctx.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return map_app_error(e.into()),
    };

    let steps = rows
        .into_iter()
        .map(|r| {
            use sqlx::Row;
            ExecutionStepDto {
                id: r.get(0),
                company_id: r.get(1),
                run_id: r.get(2),
                sequence_no: r.get(3),
                step_type: r.get(4),
                lifecycle_state: r.get(5),
                parent_step_id: r.get(6),
                input_metadata: r
                    .get::<Option<String>, _>(7)
                    .and_then(|s| serde_json::from_str(&s).ok()),
                output_metadata: r
                    .get::<Option<String>, _>(8)
                    .and_then(|s| serde_json::from_str(&s).ok()),
                failure_class: r.get(9),
                failure_detail: r.get(10),
                started_at: r.get(11),
                completed_at: r.get(12),
                created_at: r.get(13),
            }
        })
        .collect();

    Json(ExecutionStepListResponse { steps }).into_response()
}

async fn get_timeline(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => (),
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let rows = match sqlx::query(
        "SELECT id, event_type, occurred_at, payload FROM domain_events WHERE company_id = ? AND aggregate_type = 'Run' AND aggregate_id = ? ORDER BY occurred_at, id"
    )
    .bind(&company_id)
    .bind(&run_id)
    .fetch_all(&app_ctx.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return map_app_error(e.into()),
    };

    let mut sequence_no = 1;
    let events = rows
        .into_iter()
        .map(|r| {
            use sqlx::Row;
            let ev = TimelineEventDto {
                event_id: r.get(0),
                run_id: run_id.clone(),
                sequence_no,
                event_type: r.get(1),
                occurred_at: r.get(2),
                payload: serde_json::from_str(&r.get::<String, _>(3)).unwrap_or_default(),
            };
            sequence_no += 1;
            ev
        })
        .collect();

    Json(TimelineResponse { events }).into_response()
}

async fn get_result(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => (),
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let row = match sqlx::query(
        "SELECT id, company_id, run_id, run_status, result_summary, output_payload, output_metadata, resource_usage_summary, failure_class, failure_detail, warnings, correlation_id, causation_id, created_at FROM runtime_results WHERE company_id = ? AND run_id = ?"
    )
    .bind(&company_id)
    .bind(&run_id)
    .fetch_optional(&app_ctx.pool)
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    use sqlx::Row;
    let result = RuntimeResultDto {
        id: row.get(0),
        company_id: row.get(1),
        run_id: row.get(2),
        run_status: row.get(3),
        result_summary: row.get(4),
        output_payload: row
            .get::<Option<String>, _>(5)
            .and_then(|s| serde_json::from_str(&s).ok()),
        output_metadata: row
            .get::<Option<String>, _>(6)
            .and_then(|s| serde_json::from_str(&s).ok()),
        resource_usage_summary: row
            .get::<Option<String>, _>(7)
            .and_then(|s| serde_json::from_str(&s).ok()),
        failure_class: row.get(8),
        failure_detail: row.get(9),
        warnings: row
            .get::<Option<String>, _>(10)
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
        correlation_id: row.get(11),
        causation_id: row.get(12),
        created_at: row.get(13),
    };

    Json(RuntimeResultResponse { result }).into_response()
}

async fn list_usage(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => (),
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let rows = match sqlx::query(
        "SELECT id, workspace_id, company_id, project_id, work_item_id, agent_id, run_id, step_id, provider_connection_id, model_id, usage_type, quantity, unit, estimated_cost, occurred_at, metadata, created_at FROM usage_records WHERE company_id = ? AND run_id = ? ORDER BY occurred_at"
    )
    .bind(&company_id)
    .bind(&run_id)
    .fetch_all(&app_ctx.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return map_app_error(e.into()),
    };

    let usage_records = rows
        .into_iter()
        .map(|r| {
            use sqlx::Row;
            UsageRecordDto {
                id: r.get(0),
                workspace_id: r.get(1),
                company_id: r.get(2),
                project_id: r.get(3),
                work_item_id: r.get(4),
                agent_id: r.get(5),
                run_id: r.get(6),
                step_id: r.get(7),
                provider_connection_id: r.get(8),
                model_id: r.get(9),
                usage_type: r.get(10),
                quantity: r.get(11),
                unit: r.get(12),
                estimated_cost: r.get(13),
                occurred_at: r.get(14),
                metadata: r
                    .get::<Option<String>, _>(15)
                    .and_then(|s| serde_json::from_str(&s).ok()),
                created_at: r.get(16),
            }
        })
        .collect();

    Json(UsageRecordListResponse { usage_records }).into_response()
}

async fn run_events_sse(
    State(state): State<ApiState>,
    Path((company_id, project_id, run_id)): Path<(String, String, String)>,
) -> Response {
    let app_ctx = match state.app_ctx.as_ref() {
        Some(ctx) => ctx,
        None => return service_unavailable(),
    };
    match persistence::m4::get_run(&app_ctx.pool, &CompanyId(company_id.clone()), &run_id).await {
        Ok(Some(r)) if r.project_id == project_id => (),
        Ok(_) => return map_app_error(nalarvo_application::ApplicationError::NotFound(run_id)),
        Err(e) => return map_app_error(e.into()),
    };

    let rx = app_ctx.subscribe_events();
    let r_id = run_id.clone();
    let stream =
        tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(move |msg| match msg {
            Ok(event) if event.aggregate_type == "Run" && event.aggregate_id == r_id => {
                let sse_envelope = SseEventEnvelope {
                    event_id: event.event_id,
                    event_type: event.event_type.clone(),
                    schema_version: event.schema_version,
                    company_id: event.company_id.0,
                    principal: PrincipalDto {
                        principal_type: event.principal.principal_type.to_string(),
                        principal_id: event.principal.principal_id,
                    },
                    scope: ScopeDto {
                        scope_type: event.scope.scope_type.to_string(),
                        scope_id: event.scope.scope_id,
                    },
                    occurred_at: event.occurred_at.to_rfc3339(),
                    payload: event.payload,
                };
                let json = serde_json::to_string(&sse_envelope).ok()?;
                Some(Ok::<_, std::convert::Infallible>(
                    axum::response::sse::Event::default()
                        .event(&event.event_type)
                        .data(json),
                ))
            }
            _ => None,
        });

    axum::response::Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

fn map_run(r: nalarvo_domain::Run) -> RunDto {
    RunDto {
        id: r.id,
        company_id: r.company_id.0,
        project_id: r.project_id,
        work_item_id: r.work_item_id,
        assignment_id: r.assignment_id,
        executing_agent_id: r.executing_agent_id,
        lifecycle_state: r.status.to_string(),
        trigger_type: r.trigger_type,
        attempt_number: r.attempt_number as i64,
        retry_of_run_id: r.retry_of_run_id,
        model_profile_version_id: r.model_profile_version_id,
        requested_by_type: r.requested_by.principal_type.to_string(),
        requested_by_id: r.requested_by.principal_id,
        queued_at: r.queued_at.to_rfc3339(),
        started_at: r.started_at.map(|t| t.to_rfc3339()),
        completed_at: r.completed_at.map(|t| t.to_rfc3339()),
        failure_class: r.failure_class,
        failure_detail: r.failure_detail,
        correlation_id: r.correlation_id,
        causation_id: r.causation_id,
        row_version: r.row_version,
        created_at: r.created_at.to_rfc3339(),
        updated_at: r.updated_at.to_rfc3339(),
    }
}

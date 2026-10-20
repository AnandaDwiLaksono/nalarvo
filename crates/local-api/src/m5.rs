//! M5 local-api governance routes.

use crate::ApiState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use nalarvo_contracts::{ApprovalInboxItem, DecideApprovalRequest, DecideApprovalResponse};
use nalarvo_domain::{PrincipalRef, PrincipalType};

pub async fn list_approvals_handler(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let items = app
        .list_approvals(&company_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut dto = Vec::with_capacity(items.len());
    for i in items {
        let fingerprint = app
            .approval_fingerprint(&company_id, &i.action_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        dto.push(ApprovalInboxItem {
            id: i.id,
            fingerprint,
            action_id: i.action_id,
            company_id: i.company_id,
            requested_by_id: i.requested_by_id,
            status: i.status,
            reason: i.reason,
            requested_at: i.requested_at,
        });
    }

    Ok(Json(dto))
}

pub async fn list_actions_handler(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let actions = app
        .list_actions(&company_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let dto: Vec<nalarvo_contracts::ActionSummaryDto> = actions
        .into_iter()
        .map(|a| nalarvo_contracts::ActionSummaryDto {
            id: a.id,
            company_id: a.company_id,
            run_id: a.run_id,
            action_type: a.action_type,
            target: a.target,
            authorization: a.authorization,
            status: a.status,
            created_at: a.created_at,
        })
        .collect();

    Ok(Json(dto))
}

pub async fn get_action_handler(
    State(state): State<ApiState>,
    Path((company_id, action_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let a = app
        .get_action(&company_id, &action_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Action not found".to_string()))?;

    Ok(Json(nalarvo_contracts::ActionSummaryDto {
        id: a.id,
        company_id: a.company_id,
        run_id: a.run_id,
        action_type: a.action_type,
        target: a.target,
        authorization: a.authorization,
        status: a.status,
        created_at: a.created_at,
    }))
}

pub async fn list_permission_definitions_handler(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let _ = app
        .ensure_canonical_permissions("00000000-0000-0000-0000-000000000000")
        .await;

    let pds = app
        .list_permission_definitions()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let dto: Vec<nalarvo_contracts::PermissionDefinitionDto> = pds
        .into_iter()
        .map(|p| nalarvo_contracts::PermissionDefinitionDto {
            id: p.id,
            workspace_id: p.workspace_id,
            name: p.name,
            resource: p.resource,
            operation: p.operation,
            created_at: p.created_at,
        })
        .collect();

    Ok(Json(dto))
}

pub async fn list_permission_grants_handler(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let pgs = app
        .list_permission_grants(&company_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let dto: Vec<nalarvo_contracts::PermissionGrantDto> = pgs
        .into_iter()
        .map(|p| nalarvo_contracts::PermissionGrantDto {
            id: p.id,
            workspace_id: p.workspace_id,
            company_id: p.company_id.unwrap_or_default(),
            permission_definition_id: p.permission_definition_id,
            principal_type: p.principal_type,
            principal_id: p.principal_id,
            scope_type: p.scope_type,
            scope_id: p.scope_id,
            effect: p.effect,
            created_at: p.created_at,
        })
        .collect();

    Ok(Json(dto))
}

pub async fn list_policies_handler(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let policies = app
        .list_policies(&company_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let dto: Vec<nalarvo_contracts::PolicyDto> = policies
        .into_iter()
        .map(|p| nalarvo_contracts::PolicyDto {
            id: p.id,
            workspace_id: p.workspace_id,
            company_id: p.company_id,
            name: p.name,
            status: p.status,
            current_version: p.current_version,
            created_at: p.created_at,
            updated_at: p.updated_at,
        })
        .collect();

    Ok(Json(dto))
}

pub async fn list_budgets_handler(
    State(state): State<ApiState>,
    Path(company_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let budgets = app
        .list_budgets(&company_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let dto: Vec<nalarvo_contracts::BudgetDto> = budgets
        .into_iter()
        .map(|b| nalarvo_contracts::BudgetDto {
            id: b.id,
            workspace_id: b.workspace_id,
            company_id: b.company_id,
            project_id: b.project_id,
            name: b.name,
            currency: b.currency,
            status: b.status,
            total_limit: b.total_limit,
            reserved_amount: b.reserved_amount,
            spent_amount: b.spent_amount,
            created_at: b.created_at,
            updated_at: b.updated_at,
        })
        .collect();

    Ok(Json(dto))
}

pub async fn decide_approval_handler(
    State(state): State<ApiState>,
    Path((company_id, approval_id)): Path<(String, String)>,
    Json(payload): Json<DecideApprovalRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(app) = &state.app_ctx else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "App context unavailable".into(),
        ));
    };

    let p_type = match payload.decided_by_type.as_str() {
        "AGENT" => PrincipalType::Agent,
        "SYSTEM" => PrincipalType::System,
        _ => PrincipalType::User,
    };

    let decided_by = PrincipalRef {
        principal_type: p_type,
        principal_id: payload.decided_by_id,
    };

    let res = app
        .decide_action_approval(
            &company_id,
            &approval_id,
            decided_by,
            &payload.fingerprint,
            payload.approved,
            payload.reason,
        )
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    Ok(Json(DecideApprovalResponse {
        id: res.id,
        approval_request_id: res.approval_request_id,
        decision: res.decision,
        decided_at: res.decided_at,
    }))
}

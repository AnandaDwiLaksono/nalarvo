//! Forward-only M5 governance & atomic resource persistence.

use crate::PersistenceError;
use chrono::Utc;
use nalarvo_domain::{AuditRecord, DomainEvent, PrincipalRef, ScopeRef};
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ActionRecord {
    pub id: String,
    pub workspace_id: String,
    pub company_id: String,
    pub run_id: Option<String>,
    pub step_id: Option<String>,
    pub requested_by_type: String,
    pub requested_by_id: String,
    pub action_type: String,
    pub target: String,
    pub input: Option<String>,
    pub authorization: String,
    pub status: String,
    pub idempotency_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequestRecord {
    pub id: String,
    pub action_id: String,
    pub workspace_id: String,
    pub company_id: String,
    pub requested_by_id: String,
    pub status: String,
    pub reason: Option<String>,
    pub requested_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalDecisionRecord {
    pub id: String,
    pub approval_request_id: String,
    pub decided_by_type: String,
    pub decided_by_id: String,
    pub decision: String,
    pub reason: Option<String>,
    pub decided_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct BudgetReservation {
    pub id: String,
    pub budget_id: String,
    pub action_id: Option<String>,
    pub workspace_id: String,
    pub company_id: String,
    pub resource_type: String,
    pub amount: f64,
    pub expires_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct PermissionDefinitionRecord {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub resource: String,
    pub operation: String,
    pub description: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct PermissionGrantRecord {
    pub id: String,
    pub workspace_id: String,
    pub company_id: Option<String>,
    pub permission_definition_id: String,
    pub principal_type: String,
    pub principal_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub effect: String,
    pub constraints: Option<String>,
    pub starts_at: Option<String>,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct PolicyRecord {
    pub id: String,
    pub workspace_id: String,
    pub company_id: Option<String>,
    pub name: String,
    pub status: String,
    pub current_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct BudgetRecord {
    pub id: String,
    pub workspace_id: String,
    pub company_id: String,
    pub project_id: Option<String>,
    pub name: String,
    pub currency: String,
    pub status: String,
    pub total_limit: f64,
    pub reserved_amount: f64,
    pub spent_amount: f64,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Validate configuration before lifecycle, time or resource filtering.
pub async fn applicable_budgets(
    connection: &mut sqlx::SqliteConnection,
    workspace: &str,
    company: &str,
    project: Option<&str>,
    resource: &str,
    amount: f64,
) -> Result<Vec<sqlx::sqlite::SqliteRow>, PersistenceError> {
    let invalid = || {
        PersistenceError::Domain(nalarvo_domain::DomainError::Validation(
            "Invalid budget configuration".into(),
        ))
    };
    if !amount.is_finite() || amount < 0.0 {
        return Err(invalid());
    }
    let rows = sqlx::query("SELECT b.*,l.limit_amount,COALESCE(l.reserved_amount,0.0) AS limit_reserved,COALESCE(l.spent_amount,0.0) AS limit_spent FROM budgets b LEFT JOIN budget_limits l ON l.budget_id=b.id AND l.resource_type=? WHERE b.company_id=?")
        .bind(resource).bind(company).fetch_all(&mut *connection).await?;
    let now = Utc::now();
    let mut applicable = Vec::new();
    for row in rows {
        if row.try_get::<String, _>("workspace_id")? != workspace {
            return Err(invalid());
        }
        let budget_project: Option<String> = row.try_get("project_id")?;
        if let Some(id) = &budget_project {
            let owned: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE id=? AND company_id=?")
                    .bind(id)
                    .bind(company)
                    .fetch_one(&mut *connection)
                    .await?;
            if owned != 1 {
                return Err(invalid());
            }
        }
        let active = match row.try_get::<String, _>("status")?.as_str() {
            "ACTIVE" => true,
            "INACTIVE" | "CLOSED" | "ARCHIVED" => false,
            _ => return Err(invalid()),
        };
        for field in ["total_limit", "reserved_amount", "spent_amount"] {
            let value: f64 = row.try_get(field)?;
            if !value.is_finite() || value < 0.0 {
                return Err(invalid());
            }
        }
        let limits = sqlx::query("SELECT resource_type,limit_amount,reserved_amount,spent_amount FROM budget_limits WHERE budget_id=?")
            .bind(row.try_get::<String,_>("id")?).fetch_all(&mut *connection).await?;
        for limit in limits {
            let dimension: String = limit.try_get("resource_type")?;
            if !matches!(
                dimension.as_str(),
                "TOOL_CALL"
                    | "MODEL_CALL"
                    | "TOKENS"
                    | "INPUT_TOKENS"
                    | "OUTPUT_TOKENS"
                    | "COST"
                    | "RUN"
            ) {
                return Err(invalid());
            }
            for field in ["limit_amount", "reserved_amount", "spent_amount"] {
                let value: f64 = limit.try_get(field)?;
                if !value.is_finite() || value < 0.0 {
                    return Err(invalid());
                }
            }
        }
        let parse = |field| -> Result<Option<chrono::DateTime<Utc>>, PersistenceError> {
            row.try_get::<Option<String>, _>(field)?
                .map(|value| {
                    chrono::DateTime::parse_from_rfc3339(&value)
                        .map(|time| time.with_timezone(&Utc))
                        .map_err(|_| invalid())
                })
                .transpose()
        };
        let start = parse("starts_at")?;
        let end = parse("ends_at")?;
        if start.zip(end).is_some_and(|(start, end)| start >= end) {
            return Err(invalid());
        }
        if active
            && !start.is_some_and(|start| now < start)
            && !end.is_some_and(|end| now >= end)
            && budget_project
                .as_deref()
                .is_none_or(|id| Some(id) == project)
        {
            applicable.push(row);
        }
    }
    Ok(applicable)
}

pub fn budget_allows(row: &sqlx::sqlite::SqliteRow, amount: f64) -> Result<bool, PersistenceError> {
    let total: f64 = row.try_get("total_limit")?;
    let reserved: f64 = row.try_get("reserved_amount")?;
    let spent: f64 = row.try_get("spent_amount")?;
    let limit: Option<f64> = row.try_get("limit_amount")?;
    let limit_reserved: f64 = row.try_get("limit_reserved")?;
    let limit_spent: f64 = row.try_get("limit_spent")?;
    Ok(amount.is_finite()
        && amount >= 0.0
        && reserved + spent + amount <= total
        && limit.is_none_or(|limit| limit_reserved + limit_spent + amount <= limit))
}

/// Caller owns the IMMEDIATE transaction, so all scope reservations are atomic.
pub async fn hold_budget_tx(
    tx: &mut Transaction<'_, Sqlite>,
    res: &BudgetReservation,
) -> Result<(), PersistenceError> {
    sqlx::query("UPDATE budgets SET reserved_amount=reserved_amount+?,updated_at=? WHERE id=?")
        .bind(res.amount)
        .bind(&res.created_at)
        .bind(&res.budget_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE budget_limits SET reserved_amount=reserved_amount+? WHERE budget_id=? AND resource_type=?")
        .bind(res.amount).bind(&res.budget_id).bind(&res.resource_type).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO resource_reservations(id,budget_id,action_id,workspace_id,company_id,resource_type,amount,status,expires_at,created_at) VALUES(?,?,?,?,?,?,?,'HELD',?,?)")
        .bind(&res.id).bind(&res.budget_id).bind(&res.action_id).bind(&res.workspace_id).bind(&res.company_id).bind(&res.resource_type).bind(res.amount).bind(&res.expires_at).bind(&res.created_at).execute(&mut **tx).await?;
    Ok(())
}

pub async fn release_model_reservation(
    pool: &SqlitePool,
    invocation: &str,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    settle_model_reservation_tx(&mut tx, invocation, true).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn settle_model_reservation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    invocation: &str,
    release: bool,
) -> Result<(), PersistenceError> {
    let prefix = format!("model:{invocation}:");
    let rows = sqlx::query("SELECT id,budget_id,amount FROM resource_reservations WHERE resource_type='MODEL_CALL' AND status='HELD' AND substr(id,1,?)=?")
        .bind(prefix.len() as i64).bind(prefix).fetch_all(&mut **tx).await?;
    for row in rows {
        let amount: f64 = row.try_get("amount")?;
        let realized = if release { 0.0 } else { amount };
        let budget: String = row.try_get("budget_id")?;
        sqlx::query("UPDATE budgets SET reserved_amount=reserved_amount-?,spent_amount=spent_amount+?,updated_at=? WHERE id=?")
            .bind(amount).bind(realized).bind(Utc::now().to_rfc3339()).bind(&budget).execute(&mut **tx).await?;
        sqlx::query("UPDATE budget_limits SET reserved_amount=reserved_amount-?,spent_amount=spent_amount+? WHERE budget_id=? AND resource_type='MODEL_CALL'")
            .bind(amount).bind(realized).bind(&budget).execute(&mut **tx).await?;
        sqlx::query(
            "UPDATE resource_reservations SET status=?,released_at=? WHERE id=? AND status='HELD'",
        )
        .bind(if release { "RELEASED" } else { "REALIZED" })
        .bind(Utc::now().to_rfc3339())
        .bind(row.try_get::<String, _>("id")?)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub async fn create_action_with_approval(
    pool: &SqlitePool,
    action: &ActionRecord,
    approval: &ApprovalRequestRecord,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO actions (
            id, workspace_id, company_id, run_id, step_id,
            requested_by_type, requested_by_id, action_type, target,
            input, authorization, status, idempotency_key, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&action.id)
    .bind(&action.workspace_id)
    .bind(&action.company_id)
    .bind(&action.run_id)
    .bind(&action.step_id)
    .bind(&action.requested_by_type)
    .bind(&action.requested_by_id)
    .bind(&action.action_type)
    .bind(&action.target)
    .bind(&action.input)
    .bind(&action.authorization)
    .bind(&action.status)
    .bind(&action.idempotency_key)
    .bind(&action.created_at)
    .bind(&action.updated_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO approval_requests (
            id, action_id, workspace_id, company_id, requested_by_id, status, reason, requested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&approval.id)
    .bind(&approval.action_id)
    .bind(&approval.workspace_id)
    .bind(&approval.company_id)
    .bind(&approval.requested_by_id)
    .bind(&approval.status)
    .bind(&approval.reason)
    .bind(&approval.requested_at)
    .execute(&mut *tx)
    .await?;

    let principal_type = match action.requested_by_type.as_str() {
        "AGENT" => nalarvo_domain::PrincipalType::Agent,
        "SYSTEM" => nalarvo_domain::PrincipalType::System,
        _ => nalarvo_domain::PrincipalType::User,
    };
    let principal = PrincipalRef {
        principal_type,
        principal_id: action.requested_by_id.clone(),
    };

    let proposed_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ActionProposed".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(action.company_id.clone()),
        aggregate_type: "Action".into(),
        aggregate_id: action.id.clone(),
        aggregate_version: 1,
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: action.id.clone(),
        principal: principal.clone(),
        scope: ScopeRef::company(&action.company_id),
        payload: serde_json::json!({
            "action_id": action.id,
            "action_type": action.action_type,
            "target": action.target,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &proposed_event).await?;

    let req_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ActionApprovalRequired".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(action.company_id.clone()),
        aggregate_type: "Action".into(),
        aggregate_id: action.id.clone(),
        aggregate_version: 1,
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: action.id.clone(),
        principal,
        scope: ScopeRef::company(&action.company_id),
        payload: serde_json::json!({
            "action_id": action.id,
            "approval_id": approval.id,
            "action_type": action.action_type,
            "target": action.target,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &req_event).await?;

    let project_id: Option<String> =
        sqlx::query_scalar("SELECT project_id FROM runs WHERE id=? AND company_id=?")
            .bind(&action.run_id)
            .bind(&action.company_id)
            .fetch_optional(&mut *tx)
            .await?;
    let requested = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ApprovalRequested".into(),
        aggregate_type: "Approval".into(),
        aggregate_id: approval.id.clone(),
        causation_id: req_event.event_id.clone(),
        payload: serde_json::json!({"approval_request_id":approval.id,"action_id":action.id,"company_id":action.company_id,"project_id":project_id,"status":"PENDING"}),
        ..req_event
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &requested).await?;

    tx.commit().await?;
    Ok(())
}

pub async fn decide_approval(
    pool: &SqlitePool,
    decision: &ApprovalDecisionRecord,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    let req = sqlx::query(
        "SELECT action_id, requested_by_id, company_id, status FROM approval_requests WHERE id = ?",
    )
    .bind(&decision.approval_request_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| {
        PersistenceError::Domain(nalarvo_domain::DomainError::Validation(
            "approval request not found".into(),
        ))
    })?;

    let action_id: String = req.get(0);
    let requested_by: String = req.get(1);
    let company_id: String = req.get(2);
    let status: String = req.get(3);

    if status != "PENDING" {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }

    if requested_by == decision.decided_by_id {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("requester cannot decide own approval".into()),
        ));
    }

    let updated = sqlx::query("UPDATE approval_requests SET status = ?, decided_at = ? WHERE id = ? AND status = 'PENDING'")
        .bind(&decision.decision)
        .bind(&decision.decided_at)
        .bind(&decision.approval_request_id)
        .execute(&mut *tx)
        .await?;

    if updated.rows_affected() == 0 {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }

    sqlx::query(
        "INSERT INTO approval_decisions (
            id, approval_request_id, decided_by_type, decided_by_id, decision, reason, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&decision.id)
    .bind(&decision.approval_request_id)
    .bind(&decision.decided_by_type)
    .bind(&decision.decided_by_id)
    .bind(&decision.decision)
    .bind(&decision.reason)
    .bind(&decision.decided_at)
    .execute(&mut *tx)
    .await?;

    let action_status = if decision.decision == "APPROVED" {
        "PENDING"
    } else {
        "FAILED"
    };
    sqlx::query("UPDATE actions SET authorization = ?, status = ?, updated_at = ? WHERE id = ?")
        .bind(&decision.decision)
        .bind(action_status)
        .bind(&decision.decided_at)
        .bind(&action_id)
        .execute(&mut *tx)
        .await?;

    let principal_type = match decision.decided_by_type.as_str() {
        "AGENT" => nalarvo_domain::PrincipalType::Agent,
        "SYSTEM" => nalarvo_domain::PrincipalType::System,
        _ => nalarvo_domain::PrincipalType::User,
    };
    let principal = PrincipalRef {
        principal_type,
        principal_id: decision.decided_by_id.clone(),
    };

    let approval_event_type = if decision.decision == "APPROVED" {
        "ApprovalGranted"
    } else {
        "ApprovalRejected"
    };

    let appr_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: approval_event_type.into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Approval".into(),
        aggregate_id: decision.approval_request_id.clone(),
        aggregate_version: 1,
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: decision.approval_request_id.clone(),
        principal: principal.clone(),
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "approval_id": decision.approval_request_id,
            "action_id": action_id,
            "decision": decision.decision,
            "reason": decision.reason,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &appr_event).await?;

    let action_event_type = if decision.decision == "APPROVED" {
        "ActionApproved"
    } else {
        "ActionRejected"
    };

    let act_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: action_event_type.into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Action".into(),
        aggregate_id: action_id.clone(),
        aggregate_version: 2,
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: decision.approval_request_id.clone(),
        principal,
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "action_id": action_id,
            "approval_id": decision.approval_request_id,
            "decision": decision.decision,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &act_event).await?;

    tx.commit().await?;
    Ok(())
}

pub async fn reserve_budget(
    pool: &SqlitePool,
    res: &BudgetReservation,
) -> Result<bool, PersistenceError> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let rows = applicable_budgets(
        &mut tx,
        &res.workspace_id,
        &res.company_id,
        None,
        &res.resource_type,
        res.amount,
    )
    .await?;
    if !rows
        .iter()
        .any(|row| row.get::<String, _>("id") == res.budget_id)
    {
        return Ok(false);
    }
    let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_reservations WHERE id=? AND budget_id=? AND workspace_id=? AND company_id=? AND resource_type=? AND amount=? AND action_id IS ? AND status='HELD'")
        .bind(&res.id).bind(&res.budget_id).bind(&res.workspace_id).bind(&res.company_id).bind(&res.resource_type).bind(res.amount).bind(&res.action_id).fetch_one(&mut *tx).await?;
    if existing == 1 {
        tx.commit().await?;
        return Ok(true);
    }
    let budget_row = sqlx::query(
        "SELECT total_limit, reserved_amount, spent_amount FROM budgets WHERE id = ? AND company_id = ? AND status = 'ACTIVE'",
    )
    .bind(&res.budget_id)
    .bind(&res.company_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(budget) = budget_row else {
        return Ok(false);
    };

    let total: f64 = budget.get(0);
    let reserved: f64 = budget.get(1);
    let spent: f64 = budget.get(2);

    if reserved + spent + res.amount > total {
        let already_recorded: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM domain_events WHERE aggregate_type = 'Budget' AND aggregate_id = ? AND event_type = 'BudgetExhausted' AND json_extract(payload, '$.action_id') IS ? AND json_extract(payload, '$.resource_type') = ?",
        )
        .bind(&res.budget_id)
        .bind(&res.action_id)
        .bind(&res.resource_type)
        .fetch_one(&mut *tx)
        .await?;
        if already_recorded > 0 {
            tx.commit().await?;
            return Ok(false);
        }
        let event = nalarvo_domain::DomainEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: "BudgetExhausted".into(),
            schema_version: 1,
            company_id: nalarvo_domain::CompanyId(res.company_id.clone()),
            aggregate_type: "Budget".into(),
            aggregate_id: res.budget_id.clone(),
            aggregate_version: 1,
            occurred_at: chrono::Utc::now(),
            correlation_id: uuid::Uuid::now_v7().to_string(),
            causation_id: res
                .action_id
                .clone()
                .unwrap_or_else(|| res.budget_id.clone()),
            principal: nalarvo_domain::PrincipalRef::system("system"),
            scope: nalarvo_domain::ScopeRef::company(&res.company_id),
            payload: serde_json::json!({
                "budget_id": res.budget_id,
                "action_id": res.action_id,
                "resource_type": res.resource_type,
                "amount": res.amount,
                "total": total,
                "reserved": reserved,
                "spent": spent,
            }),
        };
        crate::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        return Ok(false);
    }

    let limit_row = sqlx::query(
        "SELECT limit_amount, reserved_amount, spent_amount FROM budget_limits WHERE budget_id = ? AND resource_type = ?",
    )
    .bind(&res.budget_id)
    .bind(&res.resource_type)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(limit) = limit_row {
        let l_total: f64 = limit.get(0);
        let l_reserved: f64 = limit.get(1);
        let l_spent: f64 = limit.get(2);
        if l_reserved + l_spent + res.amount > l_total {
            let already_recorded: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM domain_events WHERE aggregate_type = 'Budget' AND aggregate_id = ? AND event_type = 'BudgetExhausted' AND json_extract(payload, '$.action_id') IS ? AND json_extract(payload, '$.resource_type') = ?",
            )
            .bind(&res.budget_id)
            .bind(&res.action_id)
            .bind(&res.resource_type)
            .fetch_one(&mut *tx)
            .await?;
            if already_recorded > 0 {
                tx.commit().await?;
                return Ok(false);
            }
            let event = nalarvo_domain::DomainEvent {
                event_id: uuid::Uuid::now_v7().to_string(),
                event_type: "BudgetExhausted".into(),
                schema_version: 1,
                company_id: nalarvo_domain::CompanyId(res.company_id.clone()),
                aggregate_type: "Budget".into(),
                aggregate_id: res.budget_id.clone(),
                aggregate_version: 1,
                occurred_at: chrono::Utc::now(),
                correlation_id: uuid::Uuid::now_v7().to_string(),
                causation_id: res
                    .action_id
                    .clone()
                    .unwrap_or_else(|| res.budget_id.clone()),
                principal: nalarvo_domain::PrincipalRef::system("system"),
                scope: nalarvo_domain::ScopeRef::company(&res.company_id),
                payload: serde_json::json!({
                    "budget_id": res.budget_id,
                    "action_id": res.action_id,
                    "resource_type": res.resource_type,
                    "amount": res.amount,
                    "limit_total": l_total,
                    "limit_reserved": l_reserved,
                    "limit_spent": l_spent,
                }),
            };
            crate::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE budget_limits SET reserved_amount = reserved_amount + ? WHERE budget_id = ? AND resource_type = ?",
        )
        .bind(res.amount)
        .bind(&res.budget_id)
        .bind(&res.resource_type)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        "UPDATE budgets SET reserved_amount = reserved_amount + ?, updated_at = ? WHERE id = ?",
    )
    .bind(res.amount)
    .bind(&res.created_at)
    .bind(&res.budget_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO resource_reservations (
            id, budget_id, action_id, workspace_id, company_id, resource_type, amount, status, expires_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'HELD', ?, ?)",
    )
    .bind(&res.id)
    .bind(&res.budget_id)
    .bind(&res.action_id)
    .bind(&res.workspace_id)
    .bind(&res.company_id)
    .bind(&res.resource_type)
    .bind(res.amount)
    .bind(&res.expires_at)
    .bind(&res.created_at)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

pub async fn expire_approval(
    pool: &SqlitePool,
    approval_id: &str,
    reason: Option<String>,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    let req =
        sqlx::query("SELECT action_id, company_id, status FROM approval_requests WHERE id = ?")
            .bind(approval_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                PersistenceError::Domain(nalarvo_domain::DomainError::Validation(
                    "approval request not found".into(),
                ))
            })?;
    let action_id: String = req.get(0);
    let company_id: String = req.get(1);
    let status: String = req.get(2);
    if status != "PENDING" {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }
    let now = Utc::now();
    let updated = sqlx::query("UPDATE approval_requests SET status = 'EXPIRED', decided_at = ? WHERE id = ? AND status = 'PENDING'")
        .bind(now.to_rfc3339())
        .bind(approval_id)
        .execute(&mut *tx)
        .await?;
    if updated.rows_affected() == 0 {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }
    sqlx::query("UPDATE actions SET authorization = 'EXPIRED', status = 'FAILED', updated_at = ? WHERE id = ?")
        .bind(now.to_rfc3339())
        .bind(&action_id)
        .execute(&mut *tx)
        .await?;
    let event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ApprovalExpired".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Approval".into(),
        aggregate_id: approval_id.into(),
        aggregate_version: 1,
        occurred_at: now,
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: approval_id.into(),
        principal: PrincipalRef::system("system"),
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
            "reason": reason,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
    let action_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ActionExpired".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Action".into(),
        aggregate_id: action_id.clone(),
        aggregate_version: 2,
        occurred_at: now,
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: approval_id.into(),
        principal: PrincipalRef::system("system"),
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &action_event).await?;
    let audit = AuditRecord {
        audit_id: Uuid::now_v7().to_string(),
        action: "ApprovalExpired".into(),
        principal: PrincipalRef::system("system"),
        scope: ScopeRef::company(&company_id),
        occurred_at: now,
        details: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
            "reason": reason,
        }),
    };
    insert_audit_record_tx(&mut tx, &audit).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn cancel_approval(
    pool: &SqlitePool,
    approval_id: &str,
    cancelled_by: PrincipalRef,
    reason: Option<String>,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    let req =
        sqlx::query("SELECT action_id, company_id, status FROM approval_requests WHERE id = ?")
            .bind(approval_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                PersistenceError::Domain(nalarvo_domain::DomainError::Validation(
                    "approval request not found".into(),
                ))
            })?;
    let action_id: String = req.get(0);
    let company_id: String = req.get(1);
    let status: String = req.get(2);
    if status != "PENDING" {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }
    let now = Utc::now();
    let updated = sqlx::query("UPDATE approval_requests SET status = 'CANCELLED', decided_at = ? WHERE id = ? AND status = 'PENDING'")
        .bind(now.to_rfc3339())
        .bind(approval_id)
        .execute(&mut *tx)
        .await?;
    if updated.rows_affected() == 0 {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation("approval request is not pending".into()),
        ));
    }
    sqlx::query("UPDATE actions SET authorization = 'CANCELLED', status = 'CANCELLED', updated_at = ? WHERE id = ?")
        .bind(now.to_rfc3339())
        .bind(&action_id)
        .execute(&mut *tx)
        .await?;
    let event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ApprovalCancelled".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Approval".into(),
        aggregate_id: approval_id.into(),
        aggregate_version: 1,
        occurred_at: now,
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: approval_id.into(),
        principal: cancelled_by.clone(),
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
            "reason": reason,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
    let action_event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: "ActionCancelled".into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.clone()),
        aggregate_type: "Action".into(),
        aggregate_id: action_id.clone(),
        aggregate_version: 2,
        occurred_at: now,
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: approval_id.into(),
        principal: cancelled_by.clone(),
        scope: ScopeRef::company(&company_id),
        payload: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &action_event).await?;
    let audit = AuditRecord {
        audit_id: Uuid::now_v7().to_string(),
        action: "ApprovalCancelled".into(),
        principal: cancelled_by,
        scope: ScopeRef::company(&company_id),
        occurred_at: now,
        details: serde_json::json!({
            "approval_id": approval_id,
            "action_id": action_id,
            "reason": reason,
        }),
    };
    insert_audit_record_tx(&mut tx, &audit).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn start_action(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    principal: PrincipalRef,
) -> Result<(), PersistenceError> {
    transition_action_execution(
        pool,
        company_id,
        action_id,
        "STARTED",
        "ActionStarted",
        principal,
        None,
    )
    .await
}

pub async fn succeed_action(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    principal: PrincipalRef,
) -> Result<(), PersistenceError> {
    transition_action_execution(
        pool,
        company_id,
        action_id,
        "SUCCEEDED",
        "ActionSucceeded",
        principal,
        None,
    )
    .await
}

pub async fn fail_action(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    principal: PrincipalRef,
    failure: Option<&str>,
) -> Result<(), PersistenceError> {
    transition_action_execution(
        pool,
        company_id,
        action_id,
        "FAILED",
        "ActionFailed",
        principal,
        failure,
    )
    .await
}

pub async fn cancel_action(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    principal: PrincipalRef,
) -> Result<(), PersistenceError> {
    transition_action_execution(
        pool,
        company_id,
        action_id,
        "CANCELLED",
        "ActionCancelled",
        principal,
        None,
    )
    .await
}

pub async fn mark_action_outcome_unknown(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    principal: PrincipalRef,
    reason: Option<&str>,
) -> Result<(), PersistenceError> {
    transition_action_execution(
        pool,
        company_id,
        action_id,
        "OUTCOME_UNKNOWN",
        "ActionOutcomeUnknown",
        principal,
        reason,
    )
    .await
}

async fn transition_action_execution(
    pool: &SqlitePool,
    company_id: &str,
    action_id: &str,
    status: &str,
    event_type: &str,
    principal: PrincipalRef,
    failure: Option<&str>,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    let row =
        sqlx::query("SELECT status, authorization FROM actions WHERE id = ? AND company_id = ?")
            .bind(action_id)
            .bind(company_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                PersistenceError::Domain(nalarvo_domain::DomainError::NotFound(action_id.into()))
            })?;
    let current: String = row.get(0);
    let authorization: String = row.get(1);
    if current == status {
        tx.commit().await?;
        return Ok(());
    }
    let valid = match status {
        "STARTED" => {
            (matches!(current.as_str(), "PENDING" | "NOT_STARTED")
                && matches!(authorization.as_str(), "ALLOWED" | "APPROVED"))
        }
        "SUCCEEDED" | "FAILED" | "CANCELLED" | "OUTCOME_UNKNOWN" => {
            current == "STARTED" || (status == "CANCELLED" && current == "PENDING")
        }
        _ => false,
    };
    if !valid {
        return Err(PersistenceError::Domain(
            nalarvo_domain::DomainError::Validation(format!(
                "Invalid action execution transition {current} -> {status} with auth {authorization}"
            )),
        ));
    }
    let now = Utc::now();
    sqlx::query("UPDATE actions SET status = ?, updated_at = ? WHERE id = ? AND company_id = ? AND status = ?")
        .bind(status)
        .bind(now.to_rfc3339())
        .bind(action_id)
        .bind(company_id)
        .bind(&current)
        .execute(&mut *tx)
        .await?;
    let event = DomainEvent {
        event_id: Uuid::now_v7().to_string(),
        event_type: event_type.into(),
        schema_version: 1,
        company_id: nalarvo_domain::CompanyId(company_id.into()),
        aggregate_type: "Action".into(),
        aggregate_id: action_id.into(),
        aggregate_version: 1,
        occurred_at: now,
        correlation_id: Uuid::now_v7().to_string(),
        causation_id: action_id.into(),
        principal: principal.clone(),
        scope: ScopeRef::company(company_id),
        payload: serde_json::json!({
            "action_id": action_id,
            "status": status,
            "failure": failure,
        }),
    };
    crate::insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
    let audit = AuditRecord {
        audit_id: Uuid::now_v7().to_string(),
        action: event_type.into(),
        principal,
        scope: ScopeRef::company(company_id),
        occurred_at: now,
        details: serde_json::json!({
            "action_id": action_id,
            "status": status,
            "failure": failure,
        }),
    };
    insert_audit_record_tx(&mut tx, &audit).await?;
    tx.commit().await?;
    Ok(())
}

/// Await this within the caller's business transaction; propagate errors and
/// commit only after the audited mutation succeeds. AuditSink::record is
/// synchronous and cannot provide this durability contract.
pub async fn insert_audit_record_tx(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AuditRecord,
) -> Result<(), PersistenceError> {
    sqlx::query(
        "INSERT INTO audit_records (audit_id, action, principal_type, principal_id, scope_type, scope_id, occurred_at, details)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&record.audit_id)
    .bind(&record.action)
    .bind(record.principal.principal_type.to_string())
    .bind(&record.principal.principal_id)
    .bind(record.scope.scope_type.to_string())
    .bind(&record.scope.scope_id)
    .bind(record.occurred_at.to_rfc3339())
    .bind(to_string(&record.details).expect("JSON Value serializes"))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

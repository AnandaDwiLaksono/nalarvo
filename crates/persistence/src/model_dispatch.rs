//! Durable pre-outcome accounting; no provider retry or inferred usage.
use crate::{PersistenceError, m5};
use nalarvo_domain::{AuditRecord, CompanyId, DomainEvent, PrincipalRef, ScopeRef};
use sqlx::{Row, Sqlite, Transaction};

pub async fn transition_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    from: &str,
    to: &str,
) -> Result<(), PersistenceError> {
    let row = sqlx::query("UPDATE model_dispatches SET status=?,updated_at=? WHERE id=? AND status=? RETURNING company_id,project_id,run_id")
        .bind(to).bind(chrono::Utc::now().to_rfc3339()).bind(id).bind(from)
        .fetch_optional(&mut **tx).await?
        .ok_or_else(|| nalarvo_domain::DomainError::Validation("Stale model dispatch".into()))?;
    record_tx(
        tx,
        id,
        &row.get::<String, _>("company_id"),
        &row.get::<String, _>("project_id"),
        &row.get::<String, _>("run_id"),
        to,
    )
    .await
}

pub async fn record_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    company: &str,
    project: &str,
    run: &str,
    state: &str,
) -> Result<(), PersistenceError> {
    let at = chrono::Utc::now();
    let event = DomainEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        event_type: "ModelDispatchTransitioned".into(),
        schema_version: 1,
        company_id: CompanyId(company.into()),
        aggregate_type: "ModelDispatch".into(),
        aggregate_id: id.into(),
        aggregate_version: match state {
            "PREPARED" => 1,
            "DISPATCHING" => 2,
            _ => 3,
        },
        occurred_at: at,
        correlation_id: run.into(),
        causation_id: id.into(),
        principal: PrincipalRef::system("core"),
        scope: ScopeRef::company(company),
        payload: serde_json::json!({"dispatch_id":id,"project_id":project,"run_id":run,"status":state}),
    };
    crate::insert_domain_event_and_outbox_tx(tx, &event).await?;
    m5::insert_audit_record_tx(
        tx,
        &AuditRecord {
            audit_id: uuid::Uuid::now_v7().to_string(),
            action: event.event_type,
            principal: event.principal,
            scope: event.scope,
            occurred_at: at,
            details: event.payload,
        },
    )
    .await
}

/// Called by existing terminal Run persistence, including cancellation and orphan recovery.
pub async fn reconcile_run_tx(
    tx: &mut Transaction<'_, Sqlite>,
    company: &str,
    run: &str,
) -> Result<(), PersistenceError> {
    let rows = sqlx::query("SELECT id,status FROM model_dispatches WHERE company_id=? AND run_id=? AND status IN ('PREPARED','DISPATCHING')")
        .bind(company).bind(run).fetch_all(&mut **tx).await?;
    for row in rows {
        let id: String = row.get("id");
        let state: String = row.get("status");
        let next = if state == "PREPARED" {
            "ABORTED"
        } else {
            "OUTCOME_UNKNOWN"
        };
        transition_tx(tx, &id, &state, next).await?;
        if next == "ABORTED" {
            m5::settle_model_reservation_tx(tx, &id, true).await?;
        }
    }
    Ok(())
}

//! M4 runtime persistence API.
//!
//! App integration signatures: [`create_run`], [`create_run_with_event`], [`list_runs`],
//! [`get_run`], [`queue_run`], [`claim_job`], [`heartbeat_lease`], [`release_lease`],
//! [`expire_leases`], [`transition_run`], [`append_step`], [`append_invocation`],
//! [`append_checkpoint`], [`append_usage`], [`store_terminal_result`], and [`retry_run`].

use crate::{PersistenceError, insert_domain_event_and_outbox_tx};
use chrono::{DateTime, Duration, Utc};
use nalarvo_domain::{
    CompanyId, DomainError, DomainEvent, ExecutionStep, PrincipalRef, PrincipalType, Run, RunStatus,
};
use serde_json::Value;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DurableJob {
    pub id: String,
    pub job_type: String,
    pub company_id: CompanyId,
    pub run_id: String,
    pub payload: Value,
    pub available_at: DateTime<Utc>,
    pub priority: i64,
    pub attempt: i64,
    pub max_attempts: i64,
    pub correlation_id: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionLease {
    pub id: String,
    pub run_id: String,
    pub worker_principal_id: String,
    pub lease_version: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ModelInvocation {
    pub id: String,
    pub company_id: CompanyId,
    pub run_id: String,
    pub step_id: String,
    pub agent_id: String,
    pub provider_connection_id: String,
    pub model_id: String,
    pub model_profile_version_id: Option<String>,
    pub invocation_index: i64,
    pub status: String,
    pub request_metadata: Option<Value>,
    pub response_metadata: Option<Value>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub estimated_cost: Option<f64>,
    pub latency_ms: Option<i64>,
    pub provider_request_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeCheckpoint {
    pub id: String,
    pub company_id: CompanyId,
    pub run_id: String,
    pub checkpoint_version: i64,
    pub run_state: String,
    pub last_completed_step: Option<i64>,
    pub active_step: Option<i64>,
    pub execution_phase: String,
    pub context_refs: Option<Value>,
    pub continuation_metadata: Option<Value>,
    pub usage_snapshot: Option<Value>,
    pub safe_to_resume: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeResult {
    pub id: String,
    pub company_id: CompanyId,
    pub run_id: String,
    pub run_status: RunStatus,
    pub result_summary: String,
    pub output_payload: Option<Value>,
    pub output_metadata: Option<Value>,
    pub resource_usage_summary: Option<Value>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub warnings: Option<Value>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UsageRecord {
    pub id: String,
    pub workspace_id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub run_id: String,
    pub step_id: Option<String>,
    pub provider_connection_id: String,
    pub model_id: String,
    pub usage_type: String,
    pub quantity: i64,
    pub unit: String,
    pub estimated_cost: Option<f64>,
    pub occurred_at: DateTime<Utc>,
    pub metadata: Option<Value>,
}

pub async fn create_run(pool: &SqlitePool, run: &Run) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    insert_run_tx(&mut tx, run).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn create_run_with_event(
    pool: &SqlitePool,
    run: &Run,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    insert_run_tx(&mut tx, run).await?;
    insert_safe_event_tx(&mut tx, event).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn get_run(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
) -> Result<Option<Run>, PersistenceError> {
    let row = sqlx::query("SELECT id, company_id, project_id, work_item_id, assignment_id, executing_agent_id, lifecycle_state, trigger_type, attempt_number, retry_of_run_id, model_profile_version_id, requested_by_type, requested_by_id, queued_at, started_at, completed_at, failure_class, failure_detail, correlation_id, causation_id, row_version, created_at, updated_at FROM runs WHERE id = ? AND company_id = ?")
        .bind(run_id).bind(&company_id.0).fetch_optional(pool).await?;
    row.map(run_from_row).transpose()
}

pub async fn list_runs(
    pool: &SqlitePool,
    company_id: &CompanyId,
    project_id: &str,
) -> Result<Vec<Run>, PersistenceError> {
    let rows = sqlx::query("SELECT id, company_id, project_id, work_item_id, assignment_id, executing_agent_id, lifecycle_state, trigger_type, attempt_number, retry_of_run_id, model_profile_version_id, requested_by_type, requested_by_id, queued_at, started_at, completed_at, failure_class, failure_detail, correlation_id, causation_id, row_version, created_at, updated_at FROM runs WHERE company_id = ? AND project_id = ? ORDER BY queued_at, id")
        .bind(&company_id.0).bind(project_id).fetch_all(pool).await?;
    rows.into_iter().map(run_from_row).collect()
}

pub async fn queue_run(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    job: &DurableJob,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    if job.company_id != *company_id || job.run_id != run_id || job.job_type.trim().is_empty() {
        return validation("job scope does not match run");
    }
    validate_json(&job.payload)?;
    let mut tx = pool.begin().await?;
    let result = sqlx::query("UPDATE runs SET lifecycle_state = 'QUEUED', updated_at = ?, row_version = row_version + 1 WHERE id = ? AND company_id = ? AND row_version = ? AND lifecycle_state IN ('QUEUED', 'PAUSED', 'WAITING_APPROVAL', 'WAITING_DEPENDENCY')")
        .bind(now()).bind(run_id).bind(&company_id.0).bind(expected_version).execute(&mut *tx).await?;
    if result.rows_affected() != 1 {
        return stale_tx(&mut tx, run_id, company_id, expected_version).await;
    }
    sqlx::query("INSERT INTO durable_jobs (id, job_type, company_id, run_id, payload, available_at, priority, attempt, max_attempts, status, correlation_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)")
        .bind(&job.id).bind(&job.job_type).bind(&company_id.0).bind(run_id).bind(json(&job.payload)?).bind(time(job.available_at)).bind(job.priority).bind(job.attempt).bind(job.max_attempts).bind(&job.correlation_id).bind(now()).bind(now()).execute(&mut *tx).await?;
    insert_safe_event_tx(&mut tx, event).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn claim_job(
    pool: &SqlitePool,
    worker_principal_id: &str,
    lease_for: Duration,
) -> Result<Option<(DurableJob, ExecutionLease)>, PersistenceError> {
    if worker_principal_id.trim().is_empty() || lease_for <= Duration::zero() {
        return validation("worker and lease duration are required");
    }
    let mut tx = pool.begin().await?;
    let at = Utc::now();
    let until = at + lease_for;
    let row = sqlx::query("UPDATE durable_jobs SET status = 'CLAIMED', lease_owner = ?, lease_until = ?, attempt = attempt + 1, updated_at = ? WHERE id = (SELECT id FROM durable_jobs WHERE status = 'PENDING' AND available_at <= ? AND attempt < max_attempts ORDER BY priority DESC, available_at, id LIMIT 1) AND status = 'PENDING' RETURNING id, job_type, company_id, run_id, payload, available_at, priority, attempt, max_attempts, correlation_id")
        .bind(worker_principal_id).bind(time(until)).bind(time(at)).bind(time(at)).fetch_optional(&mut *tx).await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(None);
    };
    let job = job_from_row(row)?;
    let lease = ExecutionLease {
        id: Uuid::now_v7().to_string(),
        run_id: job.run_id.clone(),
        worker_principal_id: worker_principal_id.into(),
        lease_version: 1,
        expires_at: until,
    };
    sqlx::query("INSERT INTO run_execution_leases(id, run_id, worker_principal_id, lease_version, acquired_at, heartbeat_at, expires_at, created_at) VALUES (?, ?, ?, 1, ?, ?, ?, ?)")
        .bind(&lease.id).bind(&lease.run_id).bind(worker_principal_id).bind(time(at)).bind(time(at)).bind(time(until)).bind(time(at)).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some((job, lease)))
}

pub async fn heartbeat_lease(
    pool: &SqlitePool,
    lease_id: &str,
    worker_principal_id: &str,
    expected_version: i64,
    lease_for: Duration,
) -> Result<Option<ExecutionLease>, PersistenceError> {
    if lease_for <= Duration::zero() {
        return Ok(None);
    }
    let mut tx = pool.begin().await?;
    let at = Utc::now();
    let until = at + lease_for;
    let row = sqlx::query("SELECT run_id FROM run_execution_leases WHERE id = ? AND worker_principal_id = ? AND lease_version = ? AND released_at IS NULL AND expires_at > ?")
        .bind(lease_id).bind(worker_principal_id).bind(expected_version).bind(time(at))
        .fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(None) };
    let run_id: String = row.get(0);
    if !verify_current_lease_tx(
        &mut tx,
        &run_id,
        worker_principal_id,
        lease_id,
        expected_version,
        at,
    )
    .await?
    {
        return Ok(None);
    }
    let updated = sqlx::query("UPDATE run_execution_leases SET heartbeat_at = ?, expires_at = ?, lease_version = lease_version + 1 WHERE id = ? AND worker_principal_id = ? AND lease_version = ? AND released_at IS NULL AND expires_at > ?")
        .bind(time(at)).bind(time(until)).bind(lease_id).bind(worker_principal_id).bind(expected_version).bind(time(at)).execute(&mut *tx).await?;
    if updated.rows_affected() != 1 {
        return Ok(None);
    }
    sqlx::query("UPDATE durable_jobs SET lease_until = ?, updated_at = ? WHERE run_id = ? AND lease_owner = ? AND status = 'CLAIMED'")
        .bind(time(until)).bind(time(at)).bind(&run_id).bind(worker_principal_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(ExecutionLease {
        id: lease_id.into(),
        run_id,
        worker_principal_id: worker_principal_id.into(),
        lease_version: expected_version + 1,
        expires_at: until,
    }))
}

pub async fn verify_current_lease(
    pool: &SqlitePool,
    run_id: &str,
    principal: &str,
    lease_id: &str,
    version: i64,
) -> Result<bool, PersistenceError> {
    let at = Utc::now();
    let mut tx = pool.begin().await?;
    let valid = verify_current_lease_tx(&mut tx, run_id, principal, lease_id, version, at).await?;
    tx.commit().await?;
    Ok(valid)
}

async fn verify_current_lease_tx(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    principal: &str,
    lease_id: &str,
    version: i64,
    at: DateTime<Utc>,
) -> Result<bool, PersistenceError> {
    let matching: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM run_execution_leases WHERE run_id = ? AND worker_principal_id = ? AND id = ? AND lease_version = ? AND released_at IS NULL AND expires_at > ?",
    )
    .bind(run_id).bind(principal).bind(lease_id).bind(version).bind(time(at))
    .fetch_one(&mut **tx).await?;
    if matching != 1 {
        return Ok(false);
    }
    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM run_execution_leases WHERE run_id = ? AND released_at IS NULL AND expires_at > ?",
    )
    .bind(run_id).bind(time(at)).fetch_one(&mut **tx).await?;
    Ok(active == 1)
}

async fn require_current_lease_tx(
    tx: &mut Transaction<'_, Sqlite>,
    lease: &ExecutionLease,
) -> Result<(), PersistenceError> {
    if verify_current_lease_tx(
        tx,
        &lease.run_id,
        &lease.worker_principal_id,
        &lease.id,
        lease.lease_version,
        Utc::now(),
    )
    .await?
    {
        Ok(())
    } else {
        validation("execution lease expired or was preempted")
    }
}

pub async fn release_current_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    reason: &str,
) -> Result<bool, PersistenceError> {
    let mut tx = pool.begin().await?;
    let at = Utc::now();
    if !verify_current_lease_tx(
        &mut tx,
        &lease.run_id,
        &lease.worker_principal_id,
        &lease.id,
        lease.lease_version,
        at,
    )
    .await?
    {
        return Ok(false);
    }
    let changed = sqlx::query("UPDATE run_execution_leases SET released_at = ?, release_reason = ? WHERE id = ? AND run_id = ? AND worker_principal_id = ? AND lease_version = ? AND released_at IS NULL")
        .bind(time(at)).bind(reason).bind(&lease.id).bind(&lease.run_id).bind(&lease.worker_principal_id).bind(lease.lease_version).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Ok(false);
    }
    sqlx::query("UPDATE durable_jobs SET status = 'PENDING', lease_owner = NULL, lease_until = NULL, available_at = ?, updated_at = ? WHERE run_id = ? AND lease_owner = ? AND status = 'CLAIMED'")
        .bind(time(at)).bind(time(at)).bind(&lease.run_id).bind(&lease.worker_principal_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn release_lease(
    pool: &SqlitePool,
    lease_id: &str,
    worker_principal_id: &str,
    reason: &str,
) -> Result<bool, PersistenceError> {
    let mut tx = pool.begin().await?;
    let at = Utc::now();
    let row = sqlx::query("SELECT run_id, lease_version FROM run_execution_leases WHERE id = ? AND worker_principal_id = ? AND released_at IS NULL")
        .bind(lease_id).bind(worker_principal_id).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(false) };
    let run_id: String = row.get(0);
    let version: i64 = row.get(1);
    if !verify_current_lease_tx(&mut tx, &run_id, worker_principal_id, lease_id, version, at)
        .await?
    {
        return Ok(false);
    }
    let changed = sqlx::query("UPDATE run_execution_leases SET released_at = ?, release_reason = ? WHERE id = ? AND run_id = ? AND worker_principal_id = ? AND lease_version = ? AND released_at IS NULL")
        .bind(time(at)).bind(reason).bind(lease_id).bind(&run_id).bind(worker_principal_id).bind(version).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Ok(false);
    }
    sqlx::query("UPDATE durable_jobs SET status = 'PENDING', lease_owner = NULL, lease_until = NULL, available_at = ?, updated_at = ? WHERE run_id = ? AND lease_owner = ? AND status = 'CLAIMED'")
        .bind(time(at)).bind(time(at)).bind(run_id).bind(worker_principal_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn expire_leases(pool: &SqlitePool, at: DateTime<Utc>) -> Result<u64, PersistenceError> {
    let mut tx = pool.begin().await?;
    let released = sqlx::query("UPDATE run_execution_leases SET released_at = ?, release_reason = 'EXPIRED' WHERE released_at IS NULL AND expires_at <= ?").bind(time(at)).bind(time(at)).execute(&mut *tx).await?.rows_affected();
    sqlx::query("UPDATE durable_jobs SET status = 'PENDING', lease_owner = NULL, lease_until = NULL, available_at = ?, updated_at = ? WHERE status = 'CLAIMED' AND lease_until <= ?").bind(time(at)).bind(time(at)).bind(time(at)).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(released)
}

pub async fn transition_run(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    next: RunStatus,
    failure_class: Option<&str>,
    failure_detail: Option<&str>,
) -> Result<Run, PersistenceError> {
    let mut tx = pool.begin().await?;
    let run = transition_run_tx(
        &mut tx,
        company_id,
        run_id,
        expected_version,
        next,
        failure_class,
        failure_detail,
    )
    .await?;
    tx.commit().await?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)]
pub async fn transition_run_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    next: RunStatus,
    failure_class: Option<&str>,
    failure_detail: Option<&str>,
) -> Result<Run, PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    let run = transition_run_tx(
        &mut tx,
        company_id,
        run_id,
        expected_version,
        next,
        failure_class,
        failure_detail,
    )
    .await?;
    tx.commit().await?;
    Ok(run)
}

async fn transition_run_tx(
    tx: &mut Transaction<'_, Sqlite>,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    next: RunStatus,
    failure_class: Option<&str>,
    failure_detail: Option<&str>,
) -> Result<Run, PersistenceError> {
    let current = get_run_tx(tx, company_id, run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(run_id.into()))?;
    if current.row_version != expected_version {
        return Err(PersistenceError::StaleVersion {
            current: current.row_version,
            expected: expected_version,
        });
    }
    if !legal_transition(current.status, next) {
        return validation("invalid run lifecycle transition");
    }
    let at = Utc::now();
    let terminal = next.is_terminal();
    let result = sqlx::query("UPDATE runs SET lifecycle_state = ?, started_at = CASE WHEN ? = 'RUNNING' AND started_at IS NULL THEN ? ELSE started_at END, completed_at = CASE WHEN ? THEN ? ELSE completed_at END, failure_class = ?, failure_detail = ?, updated_at = ?, row_version = row_version + 1 WHERE id = ? AND company_id = ? AND row_version = ?")
        .bind(next.to_string()).bind(next.to_string()).bind(time(at)).bind(terminal).bind(time(at)).bind(failure_class).bind(failure_detail).bind(time(at)).bind(run_id).bind(&company_id.0).bind(expected_version).execute(&mut **tx).await?;
    if result.rows_affected() != 1 {
        return stale_tx(tx, run_id, company_id, expected_version).await;
    }
    get_run_tx(tx, company_id, run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(run_id.into()))
}

pub async fn append_step(pool: &SqlitePool, step: &ExecutionStep) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    append_step_tx(&mut tx, step).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn append_step_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    step: &ExecutionStep,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    append_step_tx(&mut tx, step).await?;
    tx.commit().await?;
    Ok(())
}

async fn append_step_tx(
    tx: &mut Transaction<'_, Sqlite>,
    step: &ExecutionStep,
) -> Result<(), PersistenceError> {
    validate_optional_json(step.input_metadata.as_ref())?;
    validate_optional_json(step.output_metadata.as_ref())?;
    if get_run_tx(tx, &step.company_id, &step.run_id)
        .await?
        .is_none()
    {
        return Err(PersistenceError::NotFound(step.run_id.clone()));
    }
    sqlx::query("INSERT INTO execution_steps(id, company_id, run_id, sequence_no, step_type, lifecycle_state, parent_step_id, input_metadata, output_metadata, failure_class, failure_detail, started_at, completed_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&step.id).bind(&step.company_id.0).bind(&step.run_id).bind(step.sequence_no as i64).bind(&step.step_type).bind(step.status.to_string()).bind(&step.parent_step_id).bind(optional_json(step.input_metadata.as_ref())?).bind(optional_json(step.output_metadata.as_ref())?).bind(&step.failure_class).bind(&step.failure_detail).bind(optional_time(step.started_at)).bind(optional_time(step.completed_at)).bind(time(step.created_at)).execute(&mut **tx).await?;
    Ok(())
}

pub async fn append_invocation(
    pool: &SqlitePool,
    invocation: &ModelInvocation,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    append_invocation_tx(&mut tx, invocation).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn append_invocation_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    invocation: &ModelInvocation,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    append_invocation_tx(&mut tx, invocation).await?;
    tx.commit().await?;
    Ok(())
}

async fn append_invocation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    invocation: &ModelInvocation,
) -> Result<(), PersistenceError> {
    validate_optional_json(invocation.request_metadata.as_ref())?;
    validate_optional_json(invocation.response_metadata.as_ref())?;
    sqlx::query("INSERT INTO model_invocations(id, company_id, run_id, step_id, agent_id, provider_connection_id, model_id, model_profile_version_id, invocation_index, status, request_metadata, response_metadata, input_tokens, output_tokens, estimated_cost, latency_ms, provider_request_id, started_at, completed_at, failure_class, failure_detail, created_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM runs WHERE id = ? AND company_id = ?) AND EXISTS (SELECT 1 FROM execution_steps WHERE id = ? AND run_id = ? AND company_id = ?)")
        .bind(&invocation.id).bind(&invocation.company_id.0).bind(&invocation.run_id).bind(&invocation.step_id).bind(&invocation.agent_id).bind(&invocation.provider_connection_id).bind(&invocation.model_id).bind(&invocation.model_profile_version_id).bind(invocation.invocation_index).bind(&invocation.status).bind(optional_json(invocation.request_metadata.as_ref())?).bind(optional_json(invocation.response_metadata.as_ref())?).bind(invocation.input_tokens).bind(invocation.output_tokens).bind(invocation.estimated_cost).bind(invocation.latency_ms).bind(&invocation.provider_request_id).bind(time(invocation.started_at)).bind(optional_time(invocation.completed_at)).bind(&invocation.failure_class).bind(&invocation.failure_detail).bind(now()).bind(&invocation.run_id).bind(&invocation.company_id.0).bind(&invocation.step_id).bind(&invocation.run_id).bind(&invocation.company_id.0).execute(&mut **tx).await?;
    Ok(())
}

pub async fn append_checkpoint(
    pool: &SqlitePool,
    checkpoint: &RuntimeCheckpoint,
) -> Result<(), PersistenceError> {
    validate_optional_json(checkpoint.context_refs.as_ref())?;
    validate_optional_json(checkpoint.continuation_metadata.as_ref())?;
    validate_optional_json(checkpoint.usage_snapshot.as_ref())?;
    sqlx::query("INSERT INTO runtime_checkpoints(id, company_id, run_id, checkpoint_version, run_state, last_completed_step, active_step, execution_phase, context_refs, continuation_metadata, usage_snapshot, safe_to_resume, created_at) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM runs WHERE id = ? AND company_id = ?)")
        .bind(&checkpoint.id).bind(&checkpoint.company_id.0).bind(&checkpoint.run_id).bind(checkpoint.checkpoint_version).bind(&checkpoint.run_state).bind(checkpoint.last_completed_step).bind(checkpoint.active_step).bind(&checkpoint.execution_phase).bind(optional_json(checkpoint.context_refs.as_ref())?).bind(optional_json(checkpoint.continuation_metadata.as_ref())?).bind(optional_json(checkpoint.usage_snapshot.as_ref())?).bind(checkpoint.safe_to_resume).bind(now()).bind(&checkpoint.run_id).bind(&checkpoint.company_id.0).execute(pool).await?;
    Ok(())
}

pub async fn append_usage(pool: &SqlitePool, usage: &UsageRecord) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    append_usage_tx(&mut tx, usage).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn append_usage_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    usage: &UsageRecord,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    append_usage_tx(&mut tx, usage).await?;
    tx.commit().await?;
    Ok(())
}

async fn append_usage_tx(
    tx: &mut Transaction<'_, Sqlite>,
    usage: &UsageRecord,
) -> Result<(), PersistenceError> {
    validate_optional_json(usage.metadata.as_ref())?;
    sqlx::query("INSERT INTO usage_records(id, workspace_id, company_id, project_id, work_item_id, agent_id, run_id, step_id, provider_connection_id, model_id, usage_type, quantity, unit, estimated_cost, occurred_at, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&usage.id).bind(&usage.workspace_id).bind(&usage.company_id.0).bind(&usage.project_id).bind(&usage.work_item_id).bind(&usage.agent_id).bind(&usage.run_id).bind(&usage.step_id).bind(&usage.provider_connection_id).bind(&usage.model_id).bind(&usage.usage_type).bind(usage.quantity).bind(&usage.unit).bind(usage.estimated_cost).bind(time(usage.occurred_at)).bind(optional_json(usage.metadata.as_ref())?).bind(now()).execute(&mut **tx).await?;
    Ok(())
}

pub async fn cancel_run_with_result(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<Run, PersistenceError> {
    if result.run_status != RunStatus::Cancelled {
        return validation("cancel result requires CANCELLED status");
    }
    let mut tx = pool.begin().await?;
    let current = get_run_tx(&mut tx, company_id, run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(run_id.into()))?;
    if current.row_version != expected_version {
        return Err(PersistenceError::StaleVersion {
            current: current.row_version,
            expected: expected_version,
        });
    }
    if !matches!(
        current.status,
        RunStatus::Queued
            | RunStatus::Running
            | RunStatus::Paused
            | RunStatus::WaitingApproval
            | RunStatus::WaitingDependency
    ) {
        return validation("only cancellable runs can be cancelled");
    }
    let at = Utc::now();
    let changed = sqlx::query("UPDATE runs SET lifecycle_state = 'CANCELLED', completed_at = ?, failure_class = ?, failure_detail = ?, updated_at = ?, row_version = row_version + 1 WHERE id = ? AND company_id = ? AND row_version = ?")
        .bind(time(at)).bind(&result.failure_class).bind(&result.failure_detail).bind(time(at)).bind(run_id).bind(&company_id.0).bind(expected_version).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return stale_tx(&mut tx, run_id, company_id, expected_version).await;
    }
    finalize_steps_jobs_tx(&mut tx, run_id, "CANCELLED", at).await?;
    let updated = get_run_tx(&mut tx, company_id, run_id)
        .await?
        .expect("updated run");
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(updated)
}

pub async fn timeout_run_with_result(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<Run, PersistenceError> {
    if result.run_status != RunStatus::TimedOut {
        return validation("timeout result requires TIMED_OUT status");
    }
    for value in [
        &result.output_payload,
        &result.output_metadata,
        &result.resource_usage_summary,
        &result.warnings,
    ] {
        validate_optional_json(value.as_ref())?;
    }
    let mut tx = pool.begin().await?;
    let current = get_run_tx(&mut tx, company_id, run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(run_id.into()))?;
    if current.row_version != expected_version {
        return Err(PersistenceError::StaleVersion {
            current: current.row_version,
            expected: expected_version,
        });
    }
    if current.status != RunStatus::Running {
        return validation("only running runs can time out");
    }
    let at = Utc::now();
    let changed = sqlx::query("UPDATE runs SET lifecycle_state = 'TIMED_OUT', completed_at = ?, failure_class = ?, failure_detail = ?, updated_at = ?, row_version = row_version + 1 WHERE id = ? AND company_id = ? AND row_version = ? AND lifecycle_state = 'RUNNING'")
        .bind(time(at)).bind(&result.failure_class).bind(&result.failure_detail)
        .bind(time(at)).bind(run_id).bind(&company_id.0).bind(expected_version)
        .execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return stale_tx(&mut tx, run_id, company_id, expected_version).await;
    }
    finalize_steps_jobs_tx(&mut tx, run_id, "TIMED_OUT", at).await?;
    let updated = get_run_tx(&mut tx, company_id, run_id)
        .await?
        .expect("updated run");
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(updated)
}

async fn finalize_steps_jobs_tx(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    terminal_status: &str,
    at: DateTime<Utc>,
) -> Result<(), PersistenceError> {
    sqlx::query("UPDATE execution_steps SET lifecycle_state = ?, completed_at = CASE WHEN completed_at IS NULL THEN ? ELSE completed_at END WHERE run_id = ? AND lifecycle_state IN ('PENDING', 'RUNNING')")
        .bind(terminal_status)
        .bind(time(at))
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE durable_jobs SET status = 'CANCELLED', updated_at = ? WHERE run_id = ? AND status IN ('PENDING', 'CLAIMED')")
        .bind(time(at))
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE run_execution_leases SET released_at = ?, release_reason = ? WHERE run_id = ? AND released_at IS NULL")
        .bind(time(at))
        .bind(terminal_status)
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub async fn finalize_run_with_result_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<Run, PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    let run = transition_run_tx(
        &mut tx,
        company_id,
        run_id,
        expected_version,
        result.run_status,
        result.failure_class.as_deref(),
        result.failure_detail.as_deref(),
    )
    .await?;
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(run)
}

pub async fn finalize_orphaned_run_with_result(
    pool: &SqlitePool,
    company_id: &CompanyId,
    run_id: &str,
    expected_version: i64,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<Run, PersistenceError> {
    if result.run_status != RunStatus::Failed {
        return validation("orphan recovery requires FAILED result");
    }
    let mut tx = pool.begin().await?;
    let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_execution_leases WHERE run_id = ? AND released_at IS NULL AND expires_at > ?")
        .bind(run_id).bind(time(Utc::now())).fetch_one(&mut *tx).await?;
    if active != 0 {
        return validation("orphaned run still has an active lease");
    }
    let run = transition_run_tx(
        &mut tx,
        company_id,
        run_id,
        expected_version,
        RunStatus::Failed,
        result.failure_class.as_deref(),
        result.failure_detail.as_deref(),
    )
    .await?;
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(run)
}

pub async fn store_terminal_result(
    pool: &SqlitePool,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn store_terminal_result_with_lease(
    pool: &SqlitePool,
    lease: &ExecutionLease,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    let mut tx = pool.begin().await?;
    require_current_lease_tx(&mut tx, lease).await?;
    store_terminal_result_tx(&mut tx, result, event).await?;
    tx.commit().await?;
    Ok(())
}

async fn store_terminal_result_tx(
    tx: &mut Transaction<'_, Sqlite>,
    result: &RuntimeResult,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    if !result.run_status.is_terminal() {
        return validation("runtime result requires terminal status");
    }
    for value in [
        &result.output_payload,
        &result.output_metadata,
        &result.resource_usage_summary,
        &result.warnings,
    ] {
        validate_optional_json(value.as_ref())?;
    }
    let run = get_run_tx(tx, &result.company_id, &result.run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(result.run_id.clone()))?;
    if run.status != result.run_status || !run.status.is_terminal() {
        return validation("terminal result does not match terminal run");
    }
    sqlx::query("INSERT INTO runtime_results(id, company_id, run_id, run_status, result_summary, output_payload, output_metadata, resource_usage_summary, failure_class, failure_detail, warnings, correlation_id, causation_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&result.id).bind(&result.company_id.0).bind(&result.run_id).bind(result.run_status.to_string()).bind(&result.result_summary).bind(optional_json(result.output_payload.as_ref())?).bind(optional_json(result.output_metadata.as_ref())?).bind(optional_json(result.resource_usage_summary.as_ref())?).bind(&result.failure_class).bind(&result.failure_detail).bind(optional_json(result.warnings.as_ref())?).bind(&result.correlation_id).bind(&result.causation_id).bind(now()).execute(&mut **tx).await?;
    insert_safe_event_tx(tx, event).await?;
    Ok(())
}

pub async fn retry_run(
    pool: &SqlitePool,
    company_id: &CompanyId,
    failed_run_id: &str,
    expected_version: i64,
    event: &DomainEvent,
) -> Result<Run, PersistenceError> {
    let failed = get_run(pool, company_id, failed_run_id)
        .await?
        .ok_or_else(|| PersistenceError::NotFound(failed_run_id.into()))?;
    if failed.row_version != expected_version {
        return Err(PersistenceError::StaleVersion {
            current: failed.row_version,
            expected: expected_version,
        });
    }
    let retry = failed.retry()?;
    let mut tx = pool.begin().await?;
    insert_run_tx(&mut tx, &retry).await?;
    insert_safe_event_tx(&mut tx, event).await?;
    tx.commit().await?;
    Ok(retry)
}

async fn insert_run_tx(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), PersistenceError> {
    validate_run_scope_tx(tx, run).await?;
    sqlx::query("INSERT INTO runs(id, company_id, project_id, work_item_id, assignment_id, executing_agent_id, lifecycle_state, trigger_type, attempt_number, retry_of_run_id, model_profile_version_id, requested_by_type, requested_by_id, queued_at, started_at, completed_at, failure_class, failure_detail, correlation_id, causation_id, row_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&run.id).bind(&run.company_id.0).bind(&run.project_id).bind(&run.work_item_id).bind(&run.assignment_id).bind(&run.executing_agent_id).bind(run.status.to_string()).bind(&run.trigger_type).bind(run.attempt_number as i64).bind(&run.retry_of_run_id).bind(&run.model_profile_version_id).bind(run.requested_by.principal_type.to_string()).bind(&run.requested_by.principal_id).bind(time(run.queued_at)).bind(optional_time(run.started_at)).bind(optional_time(run.completed_at)).bind(&run.failure_class).bind(&run.failure_detail).bind(&run.correlation_id).bind(&run.causation_id).bind(run.row_version).bind(time(run.created_at)).bind(time(run.updated_at)).execute(&mut **tx).await?;
    Ok(())
}

async fn validate_run_scope_tx(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), PersistenceError> {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projects p JOIN work_items w ON w.id = ? AND w.company_id = p.company_id AND w.project_id = p.id JOIN agents a ON a.id = ? AND a.company_id = p.company_id WHERE p.id = ? AND p.company_id = ?").bind(&run.work_item_id).bind(&run.executing_agent_id).bind(&run.project_id).bind(&run.company_id.0).fetch_one(&mut **tx).await?;
    if exists != 1 {
        return validation("run company/project/work item/agent scope is invalid");
    }
    if let Some(assignment_id) = &run.assignment_id {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM assignments WHERE id = ? AND company_id = ? AND project_id = ? AND work_item_id = ? AND agent_id = ?").bind(assignment_id).bind(&run.company_id.0).bind(&run.project_id).bind(&run.work_item_id).bind(&run.executing_agent_id).fetch_one(&mut **tx).await?;
        if exists != 1 {
            return validation("run assignment scope is invalid");
        }
    }
    if let Some(retry_id) = &run.retry_of_run_id {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id = ? AND company_id = ? AND project_id = ? AND work_item_id = ?").bind(retry_id).bind(&run.company_id.0).bind(&run.project_id).bind(&run.work_item_id).fetch_one(&mut **tx).await?;
        if exists != 1 {
            return validation("retry run scope is invalid");
        }
    }
    Ok(())
}

async fn get_run_tx(
    tx: &mut Transaction<'_, Sqlite>,
    company_id: &CompanyId,
    run_id: &str,
) -> Result<Option<Run>, PersistenceError> {
    let row = sqlx::query("SELECT id, company_id, project_id, work_item_id, assignment_id, executing_agent_id, lifecycle_state, trigger_type, attempt_number, retry_of_run_id, model_profile_version_id, requested_by_type, requested_by_id, queued_at, started_at, completed_at, failure_class, failure_detail, correlation_id, causation_id, row_version, created_at, updated_at FROM runs WHERE id = ? AND company_id = ?").bind(run_id).bind(&company_id.0).fetch_optional(&mut **tx).await?;
    row.map(run_from_row).transpose()
}

fn run_from_row(row: sqlx::sqlite::SqliteRow) -> Result<Run, PersistenceError> {
    let ptype: String = row.get(11);
    let principal_type = match ptype.as_str() {
        "USER" => PrincipalType::User,
        "AGENT" => PrincipalType::Agent,
        "SYSTEM" => PrincipalType::System,
        other => return validation(&format!("invalid principal type: {other}")),
    };
    Ok(Run {
        id: row.get(0),
        company_id: CompanyId(row.get(1)),
        project_id: row.get(2),
        work_item_id: row.get(3),
        assignment_id: row.get(4),
        executing_agent_id: row.get(5),
        status: row.get::<String, _>(6).parse()?,
        trigger_type: row.get(7),
        attempt_number: row.get::<i64, _>(8) as u32,
        retry_of_run_id: row.get(9),
        model_profile_version_id: row.get(10),
        requested_by: PrincipalRef {
            principal_type,
            principal_id: row.get(12),
        },
        queued_at: parse(row.get(13))?,
        started_at: optional_parse(row.get(14))?,
        completed_at: optional_parse(row.get(15))?,
        failure_class: row.get(16),
        failure_detail: row.get(17),
        correlation_id: row.get(18),
        causation_id: row.get(19),
        row_version: row.get(20),
        created_at: parse(row.get(21))?,
        updated_at: parse(row.get(22))?,
    })
}

fn job_from_row(row: sqlx::sqlite::SqliteRow) -> Result<DurableJob, PersistenceError> {
    Ok(DurableJob {
        id: row.get(0),
        job_type: row.get(1),
        company_id: CompanyId(row.get(2)),
        run_id: row.get(3),
        payload: serde_json::from_str(&row.get::<String, _>(4))
            .map_err(|e| DomainError::Validation(e.to_string()))?,
        available_at: parse(row.get(5))?,
        priority: row.get(6),
        attempt: row.get(7),
        max_attempts: row.get(8),
        correlation_id: row.get(9),
    })
}

async fn insert_safe_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    validate_json(&event.payload)?;
    insert_domain_event_and_outbox_tx(tx, event).await
}

async fn stale_tx<T>(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    company: &CompanyId,
    expected: i64,
) -> Result<T, PersistenceError> {
    let current = sqlx::query_scalar::<_, i64>(
        "SELECT row_version FROM runs WHERE id = ? AND company_id = ?",
    )
    .bind(run_id)
    .bind(&company.0)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or(0);
    Err(PersistenceError::StaleVersion { current, expected })
}

fn legal_transition(from: RunStatus, to: RunStatus) -> bool {
    matches!(
        (from, to),
        (
            RunStatus::Queued,
            RunStatus::Running | RunStatus::Failed | RunStatus::Cancelled
        ) | (
            RunStatus::Running,
            RunStatus::Paused
                | RunStatus::Succeeded
                | RunStatus::Failed
                | RunStatus::TimedOut
                | RunStatus::Cancelled
        ) | (RunStatus::Paused, RunStatus::Running | RunStatus::Cancelled)
            | (
                RunStatus::WaitingApproval | RunStatus::WaitingDependency,
                RunStatus::Queued | RunStatus::Cancelled
            )
    )
}

fn validate_optional_json(value: Option<&Value>) -> Result<(), PersistenceError> {
    if let Some(value) = value {
        validate_json(value)?;
    }
    Ok(())
}

fn validate_json(value: &Value) -> Result<(), PersistenceError> {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if [
                    "credential",
                    "credentials",
                    "secret",
                    "api_key",
                    "authorization",
                    "password",
                    "private_reasoning",
                    "chain_of_thought",
                    "cot",
                    "raw_prompt",
                    "raw_body",
                ]
                .contains(&key.to_ascii_lowercase().as_str())
                {
                    return validation("credential or reasoning payload is not persistable");
                }
                validate_json(value)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_json(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validation<T>(message: &str) -> Result<T, PersistenceError> {
    Err(PersistenceError::Domain(DomainError::Validation(
        message.into(),
    )))
}

fn now() -> String {
    time(Utc::now())
}

fn time(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn optional_time(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(time)
}

fn json(value: &Value) -> Result<String, PersistenceError> {
    serde_json::to_string(value)
        .map_err(|e| PersistenceError::Domain(DomainError::Validation(e.to_string())))
}

fn optional_json(value: Option<&Value>) -> Result<Option<String>, PersistenceError> {
    value.map(json).transpose()
}

fn parse(value: String) -> Result<DateTime<Utc>, PersistenceError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|e| {
            PersistenceError::Domain(DomainError::Validation(format!("invalid timestamp: {e}")))
        })
}

fn optional_parse(value: Option<String>) -> Result<Option<DateTime<Utc>>, PersistenceError> {
    value.map(parse).transpose()
}

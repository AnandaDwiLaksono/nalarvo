use crate::m7::{StaffTeamCommand, event};
use crate::{ApplicationContext, ApplicationError, ProposeActionCommand};
use chrono::Utc;
use sqlx::{Sqlite, Transaction};

/// Legacy local commands share the same authority evaluator and writer lock as M7.
pub(crate) async fn legacy_mutation(
    tx: &mut Transaction<'_, Sqlite>,
    company: &nalarvo_domain::CompanyId,
    project: &str,
    meta: &crate::CommandMeta,
    entity: &str,
    material: serde_json::Value,
) -> Result<StaffTeamCommand, ApplicationError> {
    let mut meta = meta.clone();
    // The local bearer transport represents this bootstrap user, never a body-supplied actor.
    let actor = meta.principal.get_or_insert_with(|| {
        nalarvo_domain::PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001")
    });
    crate::m7::owner_scope(tx, &company.0, project, actor).await?;
    let cmd = StaffTeamCommand {
        company_id: company.clone(),
        project_id: project.into(),
        team_id: String::new(),
        meta,
    };
    governed(
        tx,
        &cmd,
        if matches!(entity, "AgentAllocation" | "StaffingRequirement") {
            "project.allocate_agent"
        } else {
            "project.modify"
        },
        material,
    )
    .await?;
    Ok(cmd)
}

pub(crate) async fn mutable_work(
    tx: &mut Transaction<'_, Sqlite>,
    company: &str,
    project: &str,
    work: &str,
) -> Result<(), ApplicationError> {
    let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM work_items w WHERE w.id=? AND w.company_id=? AND w.project_id=? AND w.status NOT IN ('COMPLETED','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM work_evaluations e WHERE e.work_item_id=w.id AND e.accepted=1) AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.work_item_id=w.id)")
        .bind(work).bind(company).bind(project).fetch_one(&mut **tx).await?;
    if valid != 1 {
        return Err(ApplicationError::Validation(
            "Only future unaccepted work may be mutated".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn governed(
    tx: &mut Transaction<'_, Sqlite>,
    cmd: &StaffTeamCommand,
    permission: &str,
    material: serde_json::Value,
) -> Result<(), ApplicationError> {
    let proposal = ProposeActionCommand {
        company_id: cmd.company_id.0.clone(),
        project_id: Some(cmd.project_id.clone()),
        run_id: None,
        step_id: None,
        actor: cmd
            .meta
            .principal
            .clone()
            .ok_or(ApplicationError::Unauthorized)?,
        action_type: permission.into(),
        target: cmd.project_id.clone(),
        parameters: Some(material),
        idempotency_key: cmd.meta.idempotency_key.clone(),
        meta: cmd.meta.clone(),
    };
    let decision = ApplicationContext::evaluate_action_authority_connection(tx, &proposal).await?;
    if decision == nalarvo_domain::AuthorityDecision::Deny {
        return Err(ApplicationError::Unauthorized);
    }
    if decision == nalarvo_domain::AuthorityDecision::RequireApproval {
        let id = cmd.meta.causation_id.as_deref().ok_or_else(|| {
            ApplicationError::Validation(
                "Governed command requires approved Action causation_id".into(),
            )
        })?;
        let fingerprint = ApplicationContext::compute_action_fingerprint(
            &proposal.company_id,
            Some(&cmd.project_id),
            permission,
            &proposal.target,
            proposal.parameters.as_ref(),
        );
        let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actions a JOIN approval_requests p ON p.action_id=a.id AND p.company_id=a.company_id JOIN domain_events e ON e.aggregate_id=a.id AND e.company_id=a.company_id AND e.event_type='ActionProposed' WHERE a.id=? AND a.company_id=? AND a.run_id IS NULL AND a.requested_by_type='USER' AND a.requested_by_id=? AND a.action_type=? AND a.target=? AND a.authorization='APPROVED' AND a.status='PENDING' AND p.status='APPROVED' AND (p.expires_at IS NULL OR p.expires_at>?) AND json_extract(e.payload,'$.fingerprint')=?")
            .bind(id).bind(&cmd.company_id.0).bind(&proposal.actor.principal_id).bind(permission).bind(&cmd.project_id).bind(Utc::now().to_rfc3339()).bind(fingerprint).fetch_one(&mut **tx).await?;
        if valid != 1 {
            return Err(ApplicationError::Unauthorized);
        }
        sqlx::query(
            "UPDATE actions SET status='SUCCEEDED',updated_at=? WHERE id=? AND status='PENDING'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(id)
        .execute(&mut **tx)
        .await?;
        event(
            tx,
            cmd,
            "ActionSucceeded",
            "Action",
            id,
            1,
            serde_json::json!({"command":permission}),
        )
        .await?;
    }
    Ok(())
}

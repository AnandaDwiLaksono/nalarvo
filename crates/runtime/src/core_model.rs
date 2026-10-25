//! Trusted Core model execution; provider credentials never enter the worker frame.
use crate::{
    core_dispatch::TrustedRunContext,
    worker::{ModelRequestHandler, provider_failure_class},
};
use chrono::Utc;
use nalarvo_application::{
    ApplicationContext, CommandMeta, CredentialBrokerRequest, ProposeActionCommand,
};
use nalarvo_domain::{AuthorityDecision, CompanyId, PrincipalRef};
use nalarvo_model_gateway::{
    ModelRequest, ModelResponse, OpenAiCompatibleProvider, Provider, ProviderError,
    ProviderErrorKind,
};
use nalarvo_persistence::m4::{ExecutionLease, ModelInvocation, UsageRecord};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use uuid::Uuid;

fn denied() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Authentication,
        message: "model authorization denied".into(),
        request_id: None,
        status: None,
    }
}

// Provider bodies and transport messages are untrusted: persist only known-safe diagnostics.
fn safe_provider_error(error: ProviderError) -> ProviderError {
    ProviderError {
        kind: error.kind,
        message: match error.status {
            Some(status) => format!("HTTP {status}"),
            None => match error.kind {
                ProviderErrorKind::Timeout => "timed out".into(),
                ProviderErrorKind::Cancelled => "cancelled".into(),
                ProviderErrorKind::MalformedResponse => "malformed provider response".into(),
                _ => "provider transport failure".into(),
            },
        },
        // Provider correlation metadata is untrusted; internal invocation/run IDs suffice.
        request_id: None,
        status: error.status,
    }
}

// Heartbeats increment the version; only the same live lease ID may be refreshed.
async fn current_lease(
    app: &ApplicationContext,
    ctx: &TrustedRunContext,
) -> Result<ExecutionLease, ProviderError> {
    let version: i64 = sqlx::query_scalar("SELECT lease_version FROM run_execution_leases WHERE id=? AND run_id=? AND worker_principal_id=? AND released_at IS NULL")
        .bind(&ctx.lease_id).bind(&ctx.run_id).bind(&ctx.worker_principal_id)
        .fetch_one(&app.pool).await.map_err(|_| denied())?;
    if !nalarvo_persistence::m4::verify_current_lease(
        &app.pool,
        &ctx.run_id,
        &ctx.worker_principal_id,
        &ctx.lease_id,
        version,
    )
    .await
    .map_err(|_| denied())?
    {
        return Err(denied());
    }
    Ok(ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: version,
        expires_at: Utc::now(),
    })
}

pub fn handler(app: ApplicationContext) -> ModelRequestHandler {
    Arc::new(move |ctx, request, cancel| {
        let app = app.clone();
        Box::pin(async move { execute(&app, ctx, request, cancel).await })
    })
}

async fn execute(
    app: &ApplicationContext,
    ctx: TrustedRunContext,
    mut request: ModelRequest,
    cancel: watch::Receiver<bool>,
) -> Result<ModelResponse, ProviderError> {
    if *cancel.borrow() || request.model.trim().is_empty() || request.messages.is_empty() {
        return Err(denied());
    }
    let valid = nalarvo_persistence::m4::verify_current_lease(
        &app.pool,
        &ctx.run_id,
        &ctx.worker_principal_id,
        &ctx.lease_id,
        ctx.lease_version,
    )
    .await
    .map_err(|_| denied())?;
    if !valid {
        return Err(denied());
    }

    // All IDs and the exact model/profile binding come from Core's canonical rows, not the worker.
    let row = sqlx::query(
        "SELECT c.workspace_id, r.project_id, r.work_item_id, r.executing_agent_id,
                r.model_profile_version_id, a.model_profile_id, p.status AS project_status,
                c.status AS company_status, r.lifecycle_state,
                a.status AS agent_status, w.status AS work_status, r.assignment_id
         FROM runs r JOIN companies c ON c.id = r.company_id
         JOIN workspaces ws ON ws.id=c.workspace_id AND ws.status='ACTIVE'
         JOIN projects p ON p.id = r.project_id AND p.company_id = c.id
         JOIN work_items w ON w.id = r.work_item_id AND w.company_id = c.id AND w.project_id = p.id
         JOIN agents a ON a.id = r.executing_agent_id AND a.company_id = c.id
         WHERE r.id = ? AND r.company_id = ?",
    )
    .bind(&ctx.run_id)
    .bind(&ctx.company_id)
    .fetch_optional(&app.pool)
    .await
    .map_err(|_| denied())?
    .ok_or_else(denied)?;
    let workspace: String = row.get("workspace_id");
    let project: String = row.get("project_id");
    let item: String = row.get("work_item_id");
    let agent: String = row.get("executing_agent_id");
    let state: String = row.get("lifecycle_state");
    let project_status: String = row.get("project_status");
    let company_status: String = row.get("company_status");
    let agent_status: String = row.get("agent_status");
    let work_status: String = row.get("work_status");
    if project != ctx.project_id
        || item != ctx.work_item_id
        || agent != ctx.agent_id
        || state != "RUNNING"
        || project_status != "ACTIVE"
        || company_status != "ACTIVE"
        || agent_status != "ACTIVE"
        || matches!(work_status.as_str(), "COMPLETED" | "FAILED" | "CANCELLED")
    {
        return Err(denied());
    }
    let allocated: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_allocations WHERE company_id = ? AND project_id = ? AND agent_id = ? AND status = 'ACTIVE'")
        .bind(&ctx.company_id).bind(&project).bind(&agent).fetch_one(&app.pool).await.map_err(|_| denied())?;
    if allocated == 0 {
        return Err(denied());
    }
    let assignment: Option<String> = row.get("assignment_id");
    let assigned: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM assignments a JOIN agent_allocations aa ON aa.id = a.agent_allocation_id AND aa.company_id = a.company_id AND aa.project_id = a.project_id AND aa.agent_id = a.agent_id JOIN teams t ON t.id=aa.team_id AND t.company_id=aa.company_id AND t.project_id=aa.project_id AND t.status='ACTIVE' WHERE a.id = ? AND a.company_id = ? AND a.project_id = ? AND a.work_item_id = ? AND a.agent_id = ? AND a.status = 'ACTIVE' AND aa.status = 'ACTIVE'")
        .bind(assignment.ok_or_else(denied)?).bind(&ctx.company_id).bind(&project).bind(&item).bind(&agent).fetch_one(&app.pool).await.map_err(|_| denied())?;
    if assigned == 0 {
        return Err(denied());
    }
    let pinned: Option<String> = row.get("model_profile_version_id");
    let agent_profile: Option<String> = row.get("model_profile_id");
    let profile = pinned
        .as_deref()
        .or(agent_profile.as_deref())
        .ok_or_else(denied)?;
    let binding = sqlx::query(
        "SELECT pv.profile_id, pv.version, m.id AS model_id, pc.id AS provider_connection_id, pc.credential_ref_id, pc.endpoint, pc.provider_kind
         FROM model_profiles mp
         JOIN model_profile_versions pv ON pv.profile_id = mp.id AND pv.workspace_id = mp.workspace_id AND pv.version = mp.current_version
         JOIN models m ON m.id = pv.model_id AND m.workspace_id = pv.workspace_id
         JOIN provider_connections pc ON pc.id = m.provider_connection_id AND pc.workspace_id = m.workspace_id
         WHERE mp.workspace_id = ? AND mp.status = 'ACTIVE' AND m.status = 'ACTIVE'
           AND pc.status = 'ENABLED' AND m.model_key = ?
           AND (mp.id = ? OR mp.id || ':' || pv.version = ?)"
    ).bind(&workspace).bind(&request.model).bind(profile).bind(profile)
        .fetch_optional(&app.pool).await.map_err(|_| denied())?.ok_or_else(denied)?;
    let profile_id: String = binding.get("profile_id");
    let version: i64 = binding.get("version");
    // Pinned revisions cannot silently move to a new current version.
    if let Some(pinned) = &pinned
        && pinned != &profile_id
        && pinned != &format!("{profile_id}:{version}")
    {
        return Err(denied());
    }
    let kind: String = binding.get("provider_kind");
    if !matches!(kind.as_str(), "openai" | "openai_compatible") {
        return Err(denied());
    }
    let endpoint: String = binding
        .try_get::<Option<String>, _>("endpoint")
        .ok()
        .flatten()
        .ok_or_else(denied)?;
    let credential: String = binding
        .try_get::<Option<String>, _>("credential_ref_id")
        .ok()
        .flatten()
        .ok_or_else(denied)?;
    let provider = OpenAiCompatibleProvider::new(&endpoint).map_err(|_| denied())?;
    let provider_action = ProposeActionCommand {
        company_id: ctx.company_id.clone(),
        project_id: Some(project.clone()),
        run_id: Some(ctx.run_id.clone()),
        step_id: None,
        actor: PrincipalRef::system(&ctx.worker_principal_id),
        action_type: "provider.use".into(),
        target: profile_id.clone(),
        parameters: None,
        idempotency_key: None,
        meta: CommandMeta::default(),
    };
    if app
        .evaluate_action_authority(&provider_action)
        .await
        .map_err(|_| denied())?
        != AuthorityDecision::Allow
    {
        return Err(denied());
    }
    let workspace_for_usage = workspace.clone();
    let invocation_id = Uuid::now_v7().to_string();
    let lease = current_lease(app, &ctx).await?;
    let mut tx = app
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| denied())?;
    nalarvo_persistence::m4::require_current_lease_tx(&mut tx, &lease)
        .await
        .map_err(|_| denied())?;
    let budgets = nalarvo_persistence::m5::applicable_budgets(
        &mut tx,
        &workspace,
        &ctx.company_id,
        Some(&project),
        "MODEL_CALL",
        1.0,
    )
    .await
    .map_err(|_| denied())?;
    for budget in &budgets {
        // ponytail: no trusted tokenizer/pricing bound exists; reject token/cost
        // limits before HTTP until a provider-enforced ceiling is available.
        let unsupported: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM budget_limits WHERE budget_id=? AND resource_type IN ('TOKENS','INPUT_TOKENS','OUTPUT_TOKENS','COST')")
            .bind(budget.get::<String,_>("id")).fetch_one(&mut *tx).await.map_err(|_| denied())?;
        if unsupported != 0
            || !nalarvo_persistence::m5::budget_allows(budget, 1.0).map_err(|_| denied())?
        {
            return Err(denied());
        }
    }
    for budget in budgets {
        let id: String = budget.get("id");
        nalarvo_persistence::m5::hold_budget_tx(
            &mut tx,
            &nalarvo_persistence::m5::BudgetReservation {
                id: format!("model:{invocation_id}:{id}"),
                budget_id: id,
                action_id: None,
                workspace_id: workspace.clone(),
                company_id: ctx.company_id.clone(),
                resource_type: "MODEL_CALL".into(),
                amount: 1.0,
                expires_at: None,
                created_at: Utc::now().to_rfc3339(),
            },
        )
        .await
        .map_err(|_| denied())?;
    }
    tx.commit().await.map_err(|_| denied())?;
    let secret = app
        .credential_broker()
        .resolve_for_core(CredentialBrokerRequest {
            workspace_id: workspace,
            company_id: ctx.company_id.clone(),
            principal: PrincipalRef::system(&ctx.worker_principal_id),
            credential_ref_id: credential,
            model_profile_id: profile_id.clone(),
            action: "credential.use".into(),
            tool: "model.invoke".into(),
        })
        .await;
    let secret = match secret {
        Ok(secret) => secret,
        Err(_) => {
            nalarvo_persistence::m5::release_model_reservation(&app.pool, &invocation_id)
                .await
                .map_err(|_| denied())?;
            return Err(denied());
        }
    };
    let token = std::str::from_utf8(secret.expose())
        .ok()
        .filter(|token| !token.is_empty());
    if *cancel.borrow() || current_lease(app, &ctx).await.is_err() || token.is_none() {
        nalarvo_persistence::m5::release_model_reservation(&app.pool, &invocation_id)
            .await
            .map_err(|_| denied())?;
        return Err(denied());
    }
    let token = token.ok_or_else(denied)?;
    if !nalarvo_application::m6_context::context_value_allowed(
        &serde_json::Value::String(request.model.clone()),
        token,
    ) {
        nalarvo_persistence::m5::release_model_reservation(&app.pool, &invocation_id)
            .await
            .map_err(|_| denied())?;
        return Err(denied());
    }
    let prepared = app.prepare_model_context(&ctx.company_id, &ctx.run_id, &invocation_id,
        serde_json::json!({"profile_id":profile_id,"version":version,"model_id":binding.get::<String,_>("model_id"),"provider_id":binding.get::<String,_>("provider_connection_id")}), token).await;
    let (messages, snapshot_id) = match prepared {
        Ok(context) => context,
        Err(_) => {
            nalarvo_persistence::m5::release_model_reservation(&app.pool, &invocation_id)
                .await
                .map_err(|_| denied())?;
            return Err(denied());
        }
    };
    request.messages = serde_json::from_value(messages).map_err(|_| denied())?;
    request.max_tokens = Some(1024);
    request.temperature = Some(0.0);
    request.stream = false;
    if *cancel.borrow()
        || current_lease(app, &ctx).await.is_err()
        || !nalarvo_application::m6_context::context_value_allowed(
            &serde_json::json!({"model":request.model,"messages":request.messages}),
            token,
        )
    {
        nalarvo_persistence::m5::release_model_reservation(&app.pool, &invocation_id)
            .await
            .map_err(|_| denied())?;
        return Err(denied());
    }
    // Selected credential stays in the trusted adapter Authorization header only.
    // Never forward provider errors verbatim: transport diagnostics can contain credentials.
    let started_at = Utc::now();
    let result = provider
        .complete(request, token, Duration::from_secs(30), cancel.clone())
        .await
        .map_err(safe_provider_error)
        .map(|mut response| {
            response.request_id = None;
            response
        });
    if *cancel.borrow() && result.is_ok() {
        return Err(denied());
    }
    current_lease(app, &ctx).await?;
    if let Err(error) = result.as_ref() {
        let lease = current_lease(app, &ctx).await?;
        let company = CompanyId(ctx.company_id.clone());
        let step_id = nalarvo_persistence::m4::resolve_tool_step(&app.pool, &lease, &company)
            .await
            .map_err(|_| denied())?;
        let index: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(invocation_index),0)+1 FROM model_invocations WHERE run_id=?",
        )
        .bind(&ctx.run_id)
        .fetch_one(&app.pool)
        .await
        .map_err(|_| denied())?;
        let completed_at = Utc::now();
        let invocation = ModelInvocation {
            id: invocation_id.clone(),
            company_id: company,
            run_id: ctx.run_id.clone(),
            step_id,
            agent_id: agent.clone(),
            provider_connection_id: binding.get("provider_connection_id"),
            model_id: binding.get("model_id"),
            model_profile_version_id: Some(format!("{profile_id}:{version}")),
            invocation_index: index,
            status: "FAILED".into(),
            request_metadata: Some(serde_json::json!({"context_snapshot_id":snapshot_id})),
            response_metadata: None,
            input_tokens: None,
            output_tokens: None,
            estimated_cost: None,
            latency_ms: Some((completed_at - started_at).num_milliseconds()),
            provider_request_id: error.request_id.clone(),
            started_at,
            completed_at: Some(completed_at),
            failure_class: Some(provider_failure_class(error.kind).into()),
            failure_detail: Some(error.message.clone()),
        };
        nalarvo_persistence::m4::append_model_outcome_with_lease(
            &app.pool,
            &lease,
            &invocation,
            None,
        )
        .await
        .map_err(|_| denied())?;
    }
    let response = result?;
    if !nalarvo_application::m6_context::context_value_allowed(
        &serde_json::json!({"text":response.text,"structured":response.structured,"request_id":response.request_id}),
        token,
    ) {
        return Err(denied());
    }
    // Persist only a real provider response; the worker never supplies canonical IDs or usage.
    let lease = current_lease(app, &ctx).await?;
    let company = CompanyId(ctx.company_id.clone());
    let step_id = nalarvo_persistence::m4::resolve_tool_step(&app.pool, &lease, &company)
        .await
        .map_err(|_| denied())?;
    let index: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(invocation_index),0)+1 FROM model_invocations WHERE run_id=?",
    )
    .bind(&ctx.run_id)
    .fetch_one(&app.pool)
    .await
    .map_err(|_| denied())?;
    let completed_at = Utc::now();
    let model_id: String = binding.get("model_id");
    let provider_connection_id: String = binding.get("provider_connection_id");
    let invocation = ModelInvocation {
        id: invocation_id,
        company_id: company.clone(),
        run_id: ctx.run_id.clone(),
        step_id: step_id.clone(),
        agent_id: agent.clone(),
        provider_connection_id: provider_connection_id.clone(),
        model_id: model_id.clone(),
        model_profile_version_id: Some(format!("{profile_id}:{version}")),
        invocation_index: index,
        status: "SUCCEEDED".into(),
        request_metadata: Some(serde_json::json!({"context_snapshot_id":snapshot_id})),
        response_metadata: None,
        input_tokens: response
            .usage
            .as_ref()
            .and_then(|u| u.input_tokens)
            .and_then(|n| i64::try_from(n).ok()),
        output_tokens: response
            .usage
            .as_ref()
            .and_then(|u| u.output_tokens)
            .and_then(|n| i64::try_from(n).ok()),
        estimated_cost: None,
        latency_ms: Some((completed_at - started_at).num_milliseconds()),
        provider_request_id: response.request_id.clone(),
        started_at,
        completed_at: Some(completed_at),
        failure_class: None,
        failure_detail: None,
    };
    let usage =
        if let (Some(input), Some(output)) = (invocation.input_tokens, invocation.output_tokens) {
            Some(UsageRecord {
                id: Uuid::now_v7().to_string(),
                workspace_id: workspace_for_usage,
                company_id: company,
                project_id: project,
                work_item_id: item,
                agent_id: agent,
                run_id: ctx.run_id.clone(),
                step_id: Some(step_id),
                provider_connection_id,
                model_id,
                usage_type: "TOKENS".into(),
                quantity: input.checked_add(output).ok_or_else(denied)?,
                unit: "TOKENS".into(),
                estimated_cost: None,
                occurred_at: completed_at,
                metadata: Some(serde_json::json!({"input": input, "output": output})),
            })
        } else {
            None
        };
    nalarvo_persistence::m4::append_model_outcome_with_lease(
        &app.pool,
        &lease,
        &invocation,
        usage.as_ref(),
    )
    .await
    .map_err(|_| denied())?;
    Ok(response)
}

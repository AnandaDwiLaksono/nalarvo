use crate::{ApiState, map_app_error};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
};
use nalarvo_application::m7::StaffTeamCommand;
use nalarvo_contracts::m7::{
    OrchestrationCommand as C, OrchestrationRequest, OrchestrationResponse as R,
};
fn convert<T: serde::Serialize, U: serde::de::DeserializeOwned>(
    v: T,
) -> Result<U, nalarvo_application::ApplicationError> {
    serde_json::from_value(serde_json::to_value(v).map_err(|_| {
        nalarvo_application::ApplicationError::Validation("Invalid orchestration".into())
    })?)
    .map_err(|_| nalarvo_application::ApplicationError::Validation("Invalid orchestration".into()))
}
pub fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/v1/companies/{company}/projects/{project}/orchestration",
        post(command),
    )
}
async fn command(
    State(s): State<ApiState>,
    Path((c, p)): Path<(String, String)>,
    headers: HeaderMap,
    Json(req): Json<OrchestrationRequest>,
) -> Response {
    let Some(app) = s.app_ctx else {
        return crate::service_unavailable();
    };
    let mut meta = crate::m3_meta(&headers);
    meta.principal = Some(crate::default_principal());
    meta.idempotency_key = Some(req.idempotency_key);
    let cmd = StaffTeamCommand {
        company_id: nalarvo_domain::CompanyId(c),
        project_id: p,
        team_id: req.team_id,
        meta,
    };
    let result: Result<R, nalarvo_application::ApplicationError> = async {
        Ok(match req.command {
            C::StaffTeam => {
                let v = app.staff_team(cmd).await?;
                R::Staffing {
                    allocations: v.allocations,
                    shortages: convert(v.shortages)?,
                    team_active: v.team_active,
                }
            }
            C::ScheduleWork => R::Runs(app.schedule_work(cmd).await?),
            C::ConfigureWork {
                work_item_id,
                requirements,
            } => {
                app.configure_work(cmd, &work_item_id, convert(requirements)?)
                    .await?;
                R::Configured
            }
            C::GenerateProjectPlan {
                planner_role,
                reason,
            } => R::Proposal(
                app.generate_project_plan(cmd, &planner_role, &reason)
                    .await?,
            ),
            C::CollectPlanProposal {
                source_run_id,
                source_version_id,
            } => R::Proposal(
                app.collect_plan_proposal(cmd, &source_run_id, &source_version_id)
                    .await?
                    .id,
            ),
            C::DecidePlan {
                proposal_id,
                apply,
                confirmed,
                reason,
            } => R::PlanApplied(
                app.decide_project_plan(cmd, &proposal_id, apply, confirmed, &reason)
                    .await?,
            ),
            C::EvaluateWork { evaluation } => {
                let v = app.evaluate_work(cmd, convert(evaluation)?).await?;
                R::Evaluation {
                    id: v.id,
                    outcome: convert(v.outcome)?,
                    accepted: v.accepted,
                }
            }
            C::ReviseWork {
                previous_run_id,
                instructions,
                expected_version,
            } => R::Revision(
                app.revise_work_attempt(cmd, &previous_run_id, &instructions, expected_version)
                    .await?,
            ),
            C::CompleteProject { expected_version } => {
                R::Completed(app.complete_project_governed(cmd, expected_version).await?)
            }
        })
    }
    .await;
    match result {
        Ok(v) => Json(v).into_response(),
        Err(e) => map_app_error(e),
    }
}

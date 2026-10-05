use thiserror::Error;

pub const MAX_CONTEXT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ContextInput {
    pub company_id: String,
    pub project_id: String,
    pub project_name: String,
    pub project_instructions: String,
    pub objective_summary: Option<String>,
    pub team_name: String,
    pub agent_name: String,
    pub role_name: String,
    pub department_name: String,
    pub work_item_title: String,
    pub work_item_description: String,
    pub acceptance_criteria: String,
    pub assignment_summary: String,
    pub requested_output: String,
    pub safe_execution_metadata: String,
    pub excluded_values: Vec<String>,
}

impl ContextInput {
    pub fn minimal(company_id: &str, project_id: &str, work_item_title: &str) -> Self {
        Self {
            company_id: company_id.into(),
            project_id: project_id.into(),
            work_item_title: work_item_title.into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Error)]
pub enum ContextError {
    #[error("context scope is incomplete")]
    InvalidScope,
    #[error("context exceeds the safe size bound")]
    TooLarge,
}

/// Builds only operational, company/project-scoped context. Secret values and foreign data
/// are excluded by construction; `excluded_values` is a defense-in-depth assertion boundary.
pub fn build_context(input: &ContextInput) -> Result<String, ContextError> {
    if input.company_id.trim().is_empty()
        || input.project_id.trim().is_empty()
        || input.work_item_title.trim().is_empty()
    {
        return Err(ContextError::InvalidScope);
    }
    let mut output = format!(
        "company_id: {}\nproject_id: {}\nproject: {}\ninstructions: {}\n",
        input.company_id, input.project_id, input.project_name, input.project_instructions
    );
    if let Some(objective) = &input.objective_summary {
        output.push_str(&format!("objective: {objective}\n"));
    }
    output.push_str(&format!(
        "team: {}\nagent: {}\nrole: {}\ndepartment: {}\nwork_item: {}\ndescription: {}\nacceptance: {}\nassignment: {}\nrequested_output: {}\nexecution: {}\n",
        input.team_name,
        input.agent_name,
        input.role_name,
        input.department_name,
        input.work_item_title,
        input.work_item_description,
        input.acceptance_criteria,
        input.assignment_summary,
        input.requested_output,
        input.safe_execution_metadata,
    ));
    if input
        .excluded_values
        .iter()
        .any(|secret| output.contains(secret))
    {
        return Err(ContextError::InvalidScope);
    }
    if output.len() > MAX_CONTEXT_BYTES {
        return Err(ContextError::TooLarge);
    }
    Ok(output)
}

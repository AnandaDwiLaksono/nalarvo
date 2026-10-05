use clap::{Args as ClapArgs, Parser, Subcommand};
use nalarvo_contracts::{
    CancelRunRequest, CreateCompanyRequest, CreateProjectRequest, CreateRunRequest,
    ProjectLifecycleRequest, QueueRunRequest,
};

const DEFAULT_WORKSPACE_ID: &str = "0191e4b8-0002-7000-8000-000000000001";

#[derive(Parser)]
#[command(name = "nalarvo", version, about = "Nalarvo CLI")]
struct Cli {
    #[arg(
        long,
        env = "NALARVO_DAEMON_URL",
        default_value = "http://127.0.0.1:47171"
    )]
    daemon_url: String,

    #[arg(long, env = "NALARVO_DAEMON_TOKEN")]
    token: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check core daemon health
    Health,
    /// Manage companies
    Company(CompanyArgs),
    /// Manage providers
    Provider(ProviderArgs),
    /// Manage agents
    Agent(AgentArgs),
    /// Manage projects
    Project(ProjectArgs),
    /// Manage work
    Work(WorkArgs),
    /// Manage runs
    Run(RunArgs),
}

#[derive(ClapArgs)]
struct RunArgs {
    #[command(subcommand)]
    command: RunCommands,
}

#[derive(Subcommand)]
enum RunCommands {
    List {
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
    Show {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
    Create {
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
        #[arg(long)]
        work_item: String,
        #[arg(long)]
        agent: String,
        #[arg(long, default_value = "MANUAL")]
        trigger: String,
        #[arg(long)]
        assignment: Option<String>,
    },
    Queue {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
        #[arg(long, default_value_t = 1)]
        expected_version: i64,
    },
    Cancel {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
        #[arg(long)]
        reason: Option<String>,
    },
    Steps {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
    Timeline {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
    Result {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
    Usage {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: String,
    },
}

#[derive(ClapArgs)]
struct ProjectArgs {
    #[command(subcommand)]
    command: ProjectCommands,
}

#[derive(Subcommand)]
enum ProjectCommands {
    List {
        #[arg(long)]
        company: String,
    },
    Show {
        id: String,
        #[arg(long)]
        company: String,
    },
    Create {
        #[arg(long)]
        company: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        description: Option<String>,
    },
    Activate {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        expected_version: i64,
    },
}

#[derive(ClapArgs)]
struct WorkArgs {
    #[command(subcommand)]
    command: WorkCommands,
}

#[derive(Subcommand)]
enum WorkCommands {
    List {
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: Option<String>,
    },
    Show {
        id: String,
        #[arg(long)]
        company: String,
        #[arg(long)]
        project: Option<String>,
    },
}

#[derive(ClapArgs)]
struct CompanyArgs {
    #[command(subcommand)]
    command: CompanyCommands,
}

#[derive(Subcommand)]
enum CompanyCommands {
    /// List companies in workspace
    List {
        #[arg(long, default_value = DEFAULT_WORKSPACE_ID)]
        workspace_id: String,
    },
    /// Get company by ID
    Get {
        company_id: String,
        #[arg(long, default_value = DEFAULT_WORKSPACE_ID)]
        workspace_id: String,
    },
    /// Create a new company
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long, default_value = DEFAULT_WORKSPACE_ID)]
        workspace_id: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(ClapArgs)]
struct ProviderArgs {
    #[command(subcommand)]
    command: ProviderCommands,
}

#[derive(Subcommand)]
enum ProviderCommands {
    /// List providers
    List,
}

#[derive(ClapArgs)]
struct AgentArgs {
    #[command(subcommand)]
    command: AgentCommands,
}

#[derive(Subcommand)]
enum AgentCommands {
    /// List agents for a company
    List {
        #[arg(long)]
        company: String,
    },
    /// Show details for an agent
    Show {
        id: String,
        #[arg(long)]
        company: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Health => {
            let res = nalarvo_client::health(&cli.daemon_url, &cli.token).await?;
            println!("service: {}, status: {}", res.service, res.status);
        }
        Commands::Company(co) => match co.command {
            CompanyCommands::List { workspace_id } => {
                let res =
                    nalarvo_client::list_companies(&cli.daemon_url, &cli.token, &workspace_id)
                        .await?;
                if res.companies.is_empty() {
                    println!("No companies found in workspace {workspace_id}.");
                } else {
                    println!(
                        "{:<38} {:<24} {:<10} {:<6}",
                        "ID", "NAME", "STATUS", "VERSION"
                    );
                    for c in res.companies {
                        println!(
                            "{:<38} {:<24} {:<10} {:<6}",
                            c.id, c.name, c.status, c.row_version
                        );
                    }
                }
            }
            CompanyCommands::Get {
                company_id,
                workspace_id,
            } => {
                let c = nalarvo_client::get_company(
                    &cli.daemon_url,
                    &cli.token,
                    &workspace_id,
                    &company_id,
                )
                .await?;
                println!("ID:          {}", c.id);
                println!("Workspace:   {}", c.workspace_id);
                println!("Name:        {}", c.name);
                println!("Status:      {}", c.status);
                println!("Version:     {}", c.row_version);
                println!("Description: {}", c.description.unwrap_or_default());
                println!("Created:     {}", c.created_at);
            }
            CompanyCommands::Create {
                name,
                description,
                workspace_id,
                idempotency_key,
            } => {
                let req = CreateCompanyRequest { name, description };
                let c = nalarvo_client::create_company(
                    &cli.daemon_url,
                    &cli.token,
                    &workspace_id,
                    &req,
                    idempotency_key.as_deref(),
                )
                .await?;
                println!("Company created successfully:");
                println!("ID:          {}", c.id);
                println!("Name:        {}", c.name);
                println!("Status:      {}", c.status);
                println!("Version:     {}", c.row_version);
            }
        },
        Commands::Provider(po) => match po.command {
            ProviderCommands::List => {
                let res = nalarvo_client::list_providers(&cli.daemon_url, &cli.token).await?;
                if res.providers.is_empty() {
                    println!("No providers found.");
                } else {
                    println!(
                        "{:<38} {:<20} {:<12} {:<10} {:<12}",
                        "ID", "NAME", "KIND", "STATUS", "HEALTH"
                    );
                    for p in res.providers {
                        println!(
                            "{:<38} {:<20} {:<12} {:<10} {:<12}",
                            p.id, p.name, p.provider_kind, p.status, p.health
                        );
                    }
                }
            }
        },
        Commands::Agent(ao) => match ao.command {
            AgentCommands::List { company } => {
                let res =
                    nalarvo_client::list_agents(&cli.daemon_url, &cli.token, &company).await?;
                if res.agents.is_empty() {
                    println!("No agents found for company {company}.");
                } else {
                    println!(
                        "{:<38} {:<24} {:<10} {:<8}",
                        "ID", "NAME", "STATUS", "CAPACITY"
                    );
                    for a in res.agents {
                        println!(
                            "{:<38} {:<24} {:<10} {:<8}",
                            a.id, a.name, a.status, a.capacity
                        );
                    }
                }
            }
            AgentCommands::Show { id, company } => {
                let company_id = company.unwrap_or_default();
                let a = nalarvo_client::get_agent(&cli.daemon_url, &cli.token, &company_id, &id)
                    .await?;
                println!("ID:          {}", a.id);
                println!("Company:     {}", a.company_id);
                println!("Name:        {}", a.name);
                println!("Status:      {}", a.status);
                println!("Capacity:    {}", a.capacity);
                println!("Department:  {}", a.primary_department_id);
                println!("Role:        {}", a.role_id);
                println!("Version:     {}", a.row_version);
            }
        },
        Commands::Project(args) => match args.command {
            ProjectCommands::List { company } => {
                let res =
                    nalarvo_client::list_projects(&cli.daemon_url, &cli.token, &company).await?;
                for p in res.projects {
                    println!("{}\t{}\t{}\t{}", p.id, p.name, p.status, p.row_version);
                }
            }
            ProjectCommands::Show { id, company } => {
                let p =
                    nalarvo_client::get_project(&cli.daemon_url, &cli.token, &company, &id).await?;
                println!("{}\t{}\t{}\t{}", p.id, p.name, p.status, p.row_version);
            }
            ProjectCommands::Create {
                company,
                name,
                description,
            } => {
                let p = nalarvo_client::create_project(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &CreateProjectRequest { name, description },
                )
                .await?;
                println!("{}\t{}\t{}", p.id, p.name, p.status);
            }
            ProjectCommands::Activate {
                id,
                company,
                expected_version,
            } => {
                let p = nalarvo_client::activate_project(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &id,
                    &ProjectLifecycleRequest { expected_version },
                )
                .await?;
                println!("{}\t{}\t{}\t{}", p.id, p.name, p.status, p.row_version);
            }
        },
        Commands::Work(args) => match args.command {
            WorkCommands::List { company, project } => {
                let res = if let Some(proj) = project.as_deref() {
                    nalarvo_client::list_project_work_items(
                        &cli.daemon_url,
                        &cli.token,
                        &company,
                        proj,
                    )
                    .await?
                } else {
                    nalarvo_client::list_work_items(&cli.daemon_url, &cli.token, &company).await?
                };
                for w in res.work_items {
                    println!("{}\t{}\t{}", w.id, w.title, w.status);
                }
            }
            WorkCommands::Show {
                id,
                company,
                project,
            } => {
                let w = if let Some(proj) = project.as_deref() {
                    nalarvo_client::get_project_work_item(
                        &cli.daemon_url,
                        &cli.token,
                        &company,
                        proj,
                        &id,
                    )
                    .await?
                } else {
                    nalarvo_client::get_work_item(&cli.daemon_url, &cli.token, &company, &id)
                        .await?
                };
                println!("{}\t{}\t{}", w.id, w.title, w.status);
            }
        },
        Commands::Run(args) => match args.command {
            RunCommands::List { company, project } => {
                let res =
                    nalarvo_client::list_runs(&cli.daemon_url, &cli.token, &company, &project)
                        .await?;
                for r in res.runs {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        r.id,
                        r.work_item_id,
                        r.executing_agent_id,
                        r.lifecycle_state,
                        r.attempt_number
                    );
                }
            }
            RunCommands::Show {
                id,
                company,
                project,
            } => {
                let r =
                    nalarvo_client::get_run(&cli.daemon_url, &cli.token, &company, &project, &id)
                        .await?;
                println!("ID:           {}", r.run.id);
                println!("WorkItem:     {}", r.run.work_item_id);
                println!("Agent:        {}", r.run.executing_agent_id);
                println!("Status:       {}", r.run.lifecycle_state);
                println!("Attempt:      {}", r.run.attempt_number);
                println!("Created:      {}", r.run.created_at);
            }
            RunCommands::Create {
                company,
                project,
                work_item,
                agent,
                trigger,
                assignment,
            } => {
                let req = CreateRunRequest {
                    work_item_id: work_item,
                    executing_agent_id: agent,
                    trigger_type: trigger,
                    assignment_id: assignment,
                    retry_of_run_id: None,
                };
                let r = nalarvo_client::create_run(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &req,
                )
                .await?;
                println!(
                    "{}\t{}\t{}\t{}",
                    r.id, r.work_item_id, r.lifecycle_state, r.attempt_number
                );
            }
            RunCommands::Queue {
                id,
                company,
                project,
                expected_version,
            } => {
                let req = QueueRunRequest { expected_version };
                let res = nalarvo_client::queue_run(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                    &req,
                )
                .await?;
                println!("{}\t{}\t{}", res.run_id, res.command, res.accepted_at);
            }
            RunCommands::Cancel {
                id,
                company,
                project,
                reason,
            } => {
                let req = CancelRunRequest {
                    expected_version: 1,
                    reason,
                };
                let res = nalarvo_client::cancel_run(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                    &req,
                )
                .await?;
                println!("{}\t{}\t{}", res.run_id, res.command, res.accepted_at);
            }
            RunCommands::Steps {
                id,
                company,
                project,
            } => {
                let res = nalarvo_client::list_execution_steps(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                )
                .await?;
                for s in res.steps {
                    println!(
                        "{}\t{}\t{}\t{}",
                        s.sequence_no, s.step_type, s.lifecycle_state, s.created_at
                    );
                }
            }
            RunCommands::Timeline {
                id,
                company,
                project,
            } => {
                let res = nalarvo_client::get_run_timeline(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                )
                .await?;
                for e in res.events {
                    println!("{}\t{}\t{}", e.sequence_no, e.event_type, e.occurred_at);
                }
            }
            RunCommands::Result {
                id,
                company,
                project,
            } => {
                let res = nalarvo_client::get_runtime_result(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                )
                .await?;
                println!("Status:  {}", res.result.run_status);
                println!("Summary: {}", res.result.result_summary);
            }
            RunCommands::Usage {
                id,
                company,
                project,
            } => {
                let res = nalarvo_client::list_usage_records(
                    &cli.daemon_url,
                    &cli.token,
                    &company,
                    &project,
                    &id,
                )
                .await?;
                for u in res.usage_records {
                    println!("{}\t{}\t{}", u.model_id, u.quantity, u.unit);
                }
            }
        },
    }

    Ok(())
}

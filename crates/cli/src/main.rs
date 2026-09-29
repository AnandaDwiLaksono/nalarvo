use clap::{Args as ClapArgs, Parser, Subcommand};
use nalarvo_contracts::CreateCompanyRequest;

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
    }

    Ok(())
}

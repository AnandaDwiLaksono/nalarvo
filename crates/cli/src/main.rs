use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "nalarvo", about = "Nalarvo Core client")]
struct Args {
    #[arg(
        long,
        env = "NALARVO_DAEMON_URL",
        default_value = "http://127.0.0.1:47171"
    )]
    daemon_url: String,
    #[arg(long, env = "NALARVO_DAEMON_TOKEN")]
    token: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Health,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    match args.command {
        Command::Health => {
            let health = nalarvo_client::health(&args.daemon_url, &args.token).await?;
            println!("{} {}", health.service, health.status);
        }
    }
    Ok(())
}

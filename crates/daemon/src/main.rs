use clap::Parser;
use rand::Rng;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:47171")]
    bind: SocketAddr,
    #[arg(long, env = "NALARVO_DAEMON_TOKEN")]
    token: Option<String>,
    #[arg(long)]
    emit_token: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .without_time()
        .init();

    let args = Args::parse();
    if !args.bind.ip().is_loopback() {
        return Err("Nalarvo Core must bind loopback only".into());
    }

    let token = args.token.unwrap_or_else(new_token);
    let listener = TcpListener::bind(args.bind).await?;
    if args.emit_token {
        println!("{token}");
    }
    tracing::info!(address = %listener.local_addr()?, "Nalarvo Core listening");
    axum::serve(listener, nalarvo_local_api::router(Arc::<str>::from(token))).await?;
    Ok(())
}

fn new_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
}

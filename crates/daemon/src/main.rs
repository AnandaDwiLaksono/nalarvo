use clap::Parser;
use nalarvo_application::{ApplicationContext, start_outbox_dispatcher};
use nalarvo_runtime::{SupervisorConfig, start_runtime_supervisor};
use nalarvo_secret_store_windows::WindowsCredentialStore;
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
    #[arg(
        long,
        env = "NALARVO_DB_URL",
        default_value = "sqlite://data.sqlite?mode=rwc"
    )]
    db_url: String,
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

    // Initialize application context with Windows secret store and run migrations
    let app_ctx =
        ApplicationContext::init_with_secret_store(&args.db_url, Arc::new(WindowsCredentialStore))
            .await?;

    // Start background outbox dispatcher
    let dispatcher = start_outbox_dispatcher(app_ctx.clone(), 500, "daemon-dispatcher".into());

    // Start background runtime supervisor
    let supervisor = start_runtime_supervisor(app_ctx.pool.clone(), SupervisorConfig::default());

    let listener = TcpListener::bind(args.bind).await?;
    if args.emit_token {
        println!("{token}");
    }
    tracing::info!(address = %listener.local_addr()?, "Nalarvo Core listening");

    let router = nalarvo_local_api::router_with_app(Arc::<str>::from(token), Some(app_ctx));
    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl-C handler");
        })
        .await;

    dispatcher.abort();
    supervisor.shutdown().await;
    let _ = dispatcher.await;

    serve_result?;
    Ok(())
}

fn new_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
}

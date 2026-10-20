use clap::Parser;
use nalarvo_application::{ApplicationContext, start_outbox_dispatcher};
use nalarvo_runtime::{
    SupervisorConfig, core_dispatch, core_model, start_runtime_supervisor_with_handlers,
};
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
    /// Trusted operator JSON: [{"executable":"C:\\...\\tool.exe","args":["--version"],"timeout_ms":5000}].
    #[arg(long, env = "NALARVO_SHELL_ALLOWLIST", default_value = "[]")]
    shell_allowlist: String,
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
    let shell = nalarvo_runtime::shell::ShellPolicy::from_json(&args.shell_allowlist)?;

    let token = args.token.unwrap_or_else(new_token);

    // Initialize SQLite durable M5 audit + tracing diagnostics and Windows secret store.
    let app_ctx =
        ApplicationContext::init_with_secret_store(&args.db_url, Arc::new(WindowsCredentialStore))
            .await?;

    // Start background outbox dispatcher
    let dispatcher = start_outbox_dispatcher(app_ctx.clone(), 500, "daemon-dispatcher".into());

    // Core resolves each tool root from the trusted Run -> Project binding.
    let action_handler = core_dispatch::handler_with_shell(app_ctx.clone(), shell);

    // Both model authentication and tool authority remain in trusted Core.
    let supervisor = start_runtime_supervisor_with_handlers(
        app_ctx.pool.clone(),
        SupervisorConfig::default(),
        Some(action_handler),
        Some(core_model::handler(app_ctx.clone())),
    );

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_configuration_is_explicit_and_default_deny() {
        let args = Args::try_parse_from(["nalarvo-daemon"]).unwrap();
        assert_eq!(args.shell_allowlist, "[]");
        assert!(nalarvo_runtime::shell::ShellPolicy::from_json(&args.shell_allowlist).is_ok());
        let args =
            Args::try_parse_from(["nalarvo-daemon", "--shell-allowlist", "not-json"]).unwrap();
        assert!(nalarvo_runtime::shell::ShellPolicy::from_json(&args.shell_allowlist).is_err());
    }
}

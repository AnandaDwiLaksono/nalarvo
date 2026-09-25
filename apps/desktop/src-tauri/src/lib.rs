use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::Mutex,
};

/// Thin IPC bridge command. No business logic here.
/// All domain work happens inside the daemon's local HTTP API.
#[tauri::command]
async fn core_health(
    token: tauri::State<'_, String>,
    daemon_url: tauri::State<'_, String>,
) -> Result<nalarvo_contracts::HealthResponse, String> {
    nalarvo_client::health(&daemon_url, &token)
        .await
        .map_err(|e| e.to_string())
}

struct DaemonChild(Mutex<Option<Child>>);

impl Drop for DaemonChild {
    fn drop(&mut self) {
        if let Ok(mut child) = self.0.lock()
            && let Some(mut child) = child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn start_daemon() -> Result<(String, Child), String> {
    let daemon_name = if cfg!(windows) {
        "nalarvo-daemon.exe"
    } else {
        "nalarvo-daemon"
    };
    let daemon_path = std::env::var_os("NALARVO_DAEMON_BIN")
        .map(Into::into)
        .unwrap_or(
            std::env::current_exe()
                .map_err(|e| e.to_string())?
                .with_file_name(daemon_name),
        );

    let mut child = Command::new(daemon_path)
        .args(["--bind", "127.0.0.1:47171", "--emit-token"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("failed to start Nalarvo Core: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Nalarvo Core token pipe unavailable".to_string())?;
    let mut token = String::new();
    BufReader::new(stdout)
        .read_line(&mut token)
        .map_err(|e| format!("failed to read Nalarvo Core token: {e}"))?;
    let token = token.trim().to_owned();

    if token.is_empty() {
        let _ = child.kill();
        let _ = child.wait();
        return Err("Nalarvo Core returned an empty token".into());
    }

    Ok((token, child))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let external_token = std::env::var("NALARVO_DAEMON_TOKEN").ok();
    let daemon_url =
        std::env::var("NALARVO_DAEMON_URL").unwrap_or_else(|_| "http://127.0.0.1:47171".into());

    let builder = tauri::Builder::default().manage(daemon_url);
    let builder = if let Some(token) = external_token {
        builder.manage(token)
    } else {
        let (token, child) = start_daemon().expect("failed to bootstrap Nalarvo Core");
        builder
            .manage(token)
            .manage(DaemonChild(Mutex::new(Some(child))))
    };

    builder
        .invoke_handler(tauri::generate_handler![core_health])
        .run(tauri::generate_context!())
        .expect("error while running Nalarvo desktop");
}

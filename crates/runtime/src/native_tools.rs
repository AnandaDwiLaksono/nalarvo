use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(test)]
use std::time::Duration;
use std::{
    fs,
    path::{Path, PathBuf},
};
#[cfg(not(windows))]
use std::{fs::OpenOptions, io::Write, path::Component};
use thiserror::Error;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

pub const MAX_IO_BYTES: usize = 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_LIST_ENTRIES: usize = 1024;
#[cfg(test)]
pub const SHELL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolRisk {
    ReadOnly,
    Mutating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialRequirement {
    None,
    Required(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinToolMetadata {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub risk: ToolRisk,
    pub side_effecting: bool,
    pub idempotent: bool,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
    /// Credentials are IDs resolved by the trusted parent broker; never values in tool arguments.
    pub credentials_are_references_only: bool,
    pub credential_requirement: CredentialRequirement,
}

pub fn builtin_tools() -> Vec<BuiltinToolMetadata> {
    vec![
        BuiltinToolMetadata {
            name: "filesystem.list",
            version: "1.0.0",
            description: "List bounded Project directory contents. Windows requires local NTFS and rejects reparse traversal.",
            risk: ToolRisk::ReadOnly,
            side_effecting: false,
            idempotent: true,
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "entries": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["entries"]
            }),
            credentials_are_references_only: true,
            credential_requirement: CredentialRequirement::None,
        },
        BuiltinToolMetadata {
            name: "filesystem.read",
            version: "1.0.0",
            description: "Read up to 1MB from a Project file. Windows rejects reparse traversal and existing hard links.",
            risk: ToolRisk::ReadOnly,
            side_effecting: false,
            idempotent: true,
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content_base64": {"type": "string"},
                    "bytes": {"type": "integer"}
                },
                "required": ["bytes"]
            }),
            credentials_are_references_only: true,
            credential_requirement: CredentialRequirement::None,
        },
        BuiltinToolMetadata {
            name: "filesystem.write",
            version: "1.0.0",
            description: "Governed Project file write, up to 1MB. Windows publishes completed private staging files by atomic same-directory handle rename; not an OS sandbox.",
            risk: ToolRisk::Mutating,
            side_effecting: true,
            idempotent: true,
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"}
                },
                "required": ["path", "content"]
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "bytes_written": {"type": "integer"}
                },
                "required": ["bytes_written"]
            }),
            credentials_are_references_only: true,
            credential_requirement: CredentialRequirement::None,
        },
        BuiltinToolMetadata {
            name: "shell.execute",
            version: "1.0.0",
            description: "Execute an allowlisted command in a scrubbed environment with bounded output.",
            risk: ToolRisk::Mutating,
            side_effecting: true,
            idempotent: false,
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "executable": {"type": "string"},
                    "args": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["executable", "args"]
            }),
            output_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {"type": ["integer", "null"]},
                    "stdout": {"type": "string"},
                    "stderr": {"type": "string"},
                    "truncated": {"type": "boolean"}
                },
                "required": ["stdout", "stderr", "truncated"]
            }),
            credentials_are_references_only: true,
            credential_requirement: CredentialRequirement::None,
        },
    ]
}

pub fn validate_tool_credential_requirement(
    tool_name: &str,
    arguments: &serde_json::Value,
    proposed_ref: Option<&str>,
) -> Result<CredentialRequirement, ToolError> {
    let tools = builtin_tools();
    let meta = tools.iter().find(|t| t.name == tool_name);
    let has_arg_ref = arguments
        .get("credential_ref")
        .is_some_and(|v| !v.is_null())
        || arguments
            .get("credential_ref_id")
            .is_some_and(|v| !v.is_null());

    let req = match (tool_name, meta) {
        ("provider.call", None) => CredentialRequirement::Required("credential.use"),
        (_, Some(m)) => m.credential_requirement,
        _ => CredentialRequirement::None,
    };
    match req {
        CredentialRequirement::None => {
            if proposed_ref.is_some() || has_arg_ref {
                return Err(ToolError::CredentialNotAllowed);
            }
            Ok(CredentialRequirement::None)
        }
        CredentialRequirement::Required(action) => {
            if proposed_ref.is_none() && !has_arg_ref {
                return Err(ToolError::CredentialRequired);
            }
            Ok(CredentialRequirement::Required(action))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationOutcome {
    Pending,
    Succeeded,
    Failed,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub id: String,
    pub tool: String,
    pub arguments: serde_json::Value,
    pub outcome: InvocationOutcome,
}

impl ToolInvocation {
    pub fn new(tool: impl Into<String>, arguments: serde_json::Value) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            tool: tool.into(),
            arguments,
            outcome: InvocationOutcome::Pending,
        }
    }

    pub fn mark_outcome_unknown(&mut self) {
        self.outcome = InvocationOutcome::OutcomeUnknown;
    }

    /// Unknown side effects must be reconciled, never blindly replayed.
    pub fn may_execute(&self) -> bool {
        self.outcome == InvocationOutcome::Pending
    }
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("invalid path")]
    InvalidPath,
    #[error(
        "path escapes configured root, targets symlink/reparse point, or missing intermediate directory"
    )]
    PathDenied,
    #[error("IO limit exceeded")]
    TooLarge,
    #[error("filesystem operation failed")]
    Io(#[from] std::io::Error),
    #[error("command is not allowlisted")]
    CommandDenied,
    #[error("shell operation failed")]
    Shell,
    #[error("shell execution timed out")]
    Timeout,
    #[error("credential is not allowed for this tool")]
    CredentialNotAllowed,
    #[error("credential is required for this tool")]
    CredentialRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellResult {
    pub status: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

pub struct NativeTools {
    root: PathBuf,
    #[cfg(test)]
    allowed_commands: BTreeSet<String>,
    #[cfg(windows)]
    filesystem: nalarvo_application::project_filesystem::ProjectFilesystem,
}

impl NativeTools {
    pub fn new(
        root: impl AsRef<Path>,
        allowed_commands: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, ToolError> {
        #[cfg(windows)]
        let filesystem =
            nalarvo_application::project_filesystem::ProjectFilesystem::open(root.as_ref())?;
        #[cfg(windows)]
        let root = filesystem.root.clone();
        #[cfg(not(windows))]
        let root = fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(ToolError::InvalidPath);
        }
        #[cfg(not(test))]
        let _ = allowed_commands;
        Ok(Self {
            root,
            #[cfg(test)]
            allowed_commands: allowed_commands.into_iter().map(Into::into).collect(),
            #[cfg(windows)]
            filesystem,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    #[cfg(windows)]
    pub(crate) fn filesystem_binding(&self) -> Result<serde_json::Value, String> {
        self.filesystem
            .root_binding()
            .map_err(|_| "Project filesystem binding denied".into())
    }

    #[cfg(windows)]
    pub(crate) fn prepare_write(
        &self,
        path: &str,
    ) -> Result<nalarvo_application::project_filesystem::PreparedPath, String> {
        self.filesystem
            .prepare(path, true, false)
            .map_err(|_| "Project filesystem binding denied".into())
    }

    #[cfg(windows)]
    pub(crate) fn write_prepared(
        &self,
        pinned: nalarvo_application::project_filesystem::PreparedPath,
        bytes: &[u8],
    ) -> Result<(), String> {
        if bytes.len() > MAX_IO_BYTES {
            return Err(ToolError::TooLarge.to_string());
        }
        self.filesystem
            .write(pinned, bytes)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::PermissionDenied => e.to_string(),
                // Native IO acknowledgement is not proof no effect occurred. Preserve
                // governed OUTCOME_UNKNOWN and never automatically repeat a write.
                _ => ToolError::Io(e).to_string(),
            })
    }

    #[cfg(not(windows))]
    fn resolve_existing(&self, input: &str) -> Result<PathBuf, ToolError> {
        let relative = Path::new(input);
        if input.is_empty() {
            return Ok(self.root.clone());
        }
        if relative.is_absolute()
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(ToolError::PathDenied);
        }

        let mut current = self.root.clone();
        for component in relative.components() {
            current.push(component.as_os_str());
            let meta = fs::symlink_metadata(&current).map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => ToolError::InvalidPath,
                _ => ToolError::Io(e),
            })?;
            if is_link_or_reparse(&meta) {
                return Err(ToolError::PathDenied);
            }
            current = fs::canonicalize(&current)?;
            if !current.starts_with(&self.root) {
                return Err(ToolError::PathDenied);
            }
        }
        Ok(current)
    }

    #[cfg(not(windows))]
    fn resolve_for_write(&self, input: &str) -> Result<PathBuf, ToolError> {
        let relative = Path::new(input);
        if input.is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(ToolError::PathDenied);
        }

        let parent_rel = relative.parent().unwrap_or_else(|| Path::new(""));
        let file_name = relative
            .file_name()
            .ok_or(ToolError::InvalidPath)?
            .to_os_string();

        let parent_canon = if parent_rel.as_os_str().is_empty() {
            self.root.clone()
        } else {
            self.resolve_existing(parent_rel.to_str().ok_or(ToolError::InvalidPath)?)?
        };

        if !parent_canon.is_dir() {
            return Err(ToolError::InvalidPath);
        }

        let target = parent_canon.join(file_name);
        match fs::symlink_metadata(&target) {
            Ok(meta) => {
                if is_link_or_reparse(&meta) || meta.is_dir() {
                    return Err(ToolError::PathDenied);
                }
                let canon = fs::canonicalize(&target)?;
                if !canon.starts_with(&self.root) {
                    return Err(ToolError::PathDenied);
                }
                Ok(canon)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if !target.starts_with(&self.root) {
                    return Err(ToolError::PathDenied);
                }
                Ok(target)
            }
            Err(e) => Err(ToolError::Io(e)),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<String>, ToolError> {
        #[cfg(windows)]
        let _pinned = self.filesystem.prepare(path, false, true)?;
        #[cfg(windows)]
        let dir = self.root.join(path);
        #[cfg(not(windows))]
        let dir = self.resolve_existing(path)?;
        if !dir.is_dir() {
            return Err(ToolError::InvalidPath);
        }
        let mut names = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let meta = fs::symlink_metadata(entry.path())?;
            if is_link_or_reparse(&meta) {
                continue;
            }
            names.push(entry.file_name().to_string_lossy().into_owned());
            if names.len() > MAX_LIST_ENTRIES {
                return Err(ToolError::TooLarge);
            }
        }
        names.sort();
        Ok(names)
    }

    #[cfg(windows)]
    pub fn read(&self, path: &str) -> Result<Vec<u8>, ToolError> {
        use std::io::Read;
        let pinned = self.filesystem.prepare(path, false, false)?;
        let file = pinned.target.as_ref().ok_or(ToolError::InvalidPath)?;
        let mut bytes = Vec::new();
        file.take((MAX_IO_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_IO_BYTES {
            return Err(ToolError::TooLarge);
        }
        Ok(bytes)
    }

    #[cfg(not(windows))]
    pub fn read(&self, path: &str) -> Result<Vec<u8>, ToolError> {
        let file = self.resolve_existing(path)?;
        if !file.is_file() {
            return Err(ToolError::InvalidPath);
        }
        let meta = fs::metadata(&file)?;
        if meta.len() > MAX_IO_BYTES as u64 {
            return Err(ToolError::TooLarge);
        }
        let bytes = fs::read(file)?;
        if bytes.len() > MAX_IO_BYTES {
            return Err(ToolError::TooLarge);
        }
        Ok(bytes)
    }

    #[cfg(windows)]
    pub fn write(&self, path: &str, bytes: &[u8]) -> Result<(), ToolError> {
        if bytes.len() > MAX_IO_BYTES {
            return Err(ToolError::TooLarge);
        }
        let pinned = self.filesystem.prepare(path, true, false)?;
        self.filesystem.write(pinned, bytes)?;
        Ok(())
    }

    #[cfg(not(windows))]
    pub fn write(&self, path: &str, bytes: &[u8]) -> Result<(), ToolError> {
        if bytes.len() > MAX_IO_BYTES {
            return Err(ToolError::TooLarge);
        }
        let target = self.resolve_for_write(path)?;
        let parent = target.parent().ok_or(ToolError::InvalidPath)?;
        let temp_file_name = format!(".tmp_write_{}_{}", uuid::Uuid::now_v7(), std::process::id());
        let temp_path = parent.join(temp_file_name);

        let write_res = (|| -> Result<(), ToolError> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            file.write_all(bytes)?;
            file.flush()?;
            file.sync_all()?;
            drop(file);

            fs::rename(&temp_path, &target)?;
            Ok(())
        })();

        if write_res.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_res
    }

    #[cfg(test)]
    async fn execute(&self, executable: &str, args: &[String]) -> Result<ShellResult, ToolError> {
        if !self.allowed_commands.contains(executable) {
            return Err(ToolError::CommandDenied);
        }
        // This legacy helper is test-only. Production dispatch uses an operator-defined
        // exact-argv ShellPolicy plus the application authorization transaction.
        crate::shell::ShellPolicy::from_json(
            &serde_json::json!([{
                "executable":executable,"args":args,"timeout_ms":SHELL_TIMEOUT.as_millis()
            }])
            .to_string(),
        )
        .map_err(|_| ToolError::CommandDenied)?
        .execute(&self.root, executable, args, std::future::pending::<()>())
        .await
        .map_err(|_| ToolError::Shell)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell_fixture() -> &'static Path {
        static EXE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        EXE.get_or_init(|| {
            let dir = tempfile::tempdir().unwrap().keep();
            let exe = dir.join("fixture.exe");
            assert!(
                std::process::Command::new("rustc")
                    .arg(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/support/shell_fixture.rs"
                    ))
                    .args(["--edition=2024", "-o"])
                    .arg(&exe)
                    .status()
                    .unwrap()
                    .success()
            );
            exe
        })
    }

    #[test]
    fn filesystem_tools_stay_in_root_and_bound_content() {
        let root = tempfile::tempdir().unwrap();
        let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
        tools.write("note.txt", b"ok").unwrap();
        assert_eq!(tools.read("note.txt").unwrap(), b"ok");
        assert!(tools.write("../escape", b"bad").is_err());
        assert!(tools.write("nested/nonexistent/file.txt", b"bad").is_err());
        assert!(tools.read(&"x".repeat(MAX_IO_BYTES + 1)).is_err());
        assert_eq!(tools.list("").unwrap(), vec!["note.txt"]);
    }

    #[test]
    fn write_is_atomic_and_replaces_file() {
        let root = tempfile::tempdir().unwrap();
        let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
        tools.write("atomic.txt", b"v1").unwrap();
        assert_eq!(tools.read("atomic.txt").unwrap(), b"v1");
        tools.write("atomic.txt", b"v2").unwrap();
        assert_eq!(tools.read("atomic.txt").unwrap(), b"v2");
        let entries = tools.list("").unwrap();
        assert_eq!(entries, vec!["atomic.txt"]);
    }

    #[cfg(windows)]
    #[test]
    fn windows_root_cannot_be_replaced_after_authorization() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("project");
        fs::create_dir(&root).unwrap();
        let tools = NativeTools::new(&root, [] as [&str; 0]).unwrap();
        let moved = base.path().join("moved");
        assert!(fs::rename(&root, &moved).is_err());
        tools.write("note.txt", b"authorized").unwrap();
        assert_eq!(fs::read(root.join("note.txt")).unwrap(), b"authorized");
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_tools_reject_symlink_escape() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), root.path().join("link")).unwrap();
        let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
        assert!(tools.read("link").is_err());
        assert!(tools.write("link", b"exploit").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn listing_omits_symlink_even_when_target_is_inside_root() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("real.txt"), b"ok").unwrap();
        symlink(root.path().join("real.txt"), root.path().join("alias.txt")).unwrap();
        let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
        assert_eq!(tools.list("").unwrap(), vec!["real.txt"]);
    }

    #[tokio::test]
    async fn shell_is_allowlisted_scrubbed_and_output_bounded() {
        let root = tempfile::tempdir().unwrap();
        let executable = shell_fixture().to_str().unwrap();
        let tools = NativeTools::new(root.path(), [executable]).unwrap();
        let result = tools
            .execute(executable, &["environment".into()])
            .await
            .unwrap();
        assert!(!result.stdout.contains("C:\\Users\\"));
        assert!(result.stdout.contains("USERPROFILE=ABSENT"));
        assert!(tools.execute("not-allowed", &[]).await.is_err());
    }

    #[tokio::test]
    async fn shell_canary_parent_secrets_are_scrubbed_from_child_env() {
        // Isolated test process; Rust 2024 requires explicit acknowledgement for env mutation.
        unsafe {
            std::env::set_var("CANARY_PARENT_SECRET", "super-secret-parent-token-1234");
            std::env::set_var("GITHUB_TOKEN", "ghp_secret_token_canary");
        }
        let root = tempfile::tempdir().unwrap();
        let executable = shell_fixture().to_str().unwrap();
        let tools = NativeTools::new(root.path(), [executable]).unwrap();
        let result = tools
            .execute(executable, &["success".into()])
            .await
            .unwrap();
        assert!(!result.stdout.contains("super-secret-parent-token-1234"));
        assert!(!result.stdout.contains("ghp_secret_token_canary"));
        assert!(result.stdout.contains("CANARY=%CANARY_PARENT_SECRET%"));
        assert!(result.stdout.contains("CANARY_PARENT_SECRET=ABSENT"));
        assert!(result.stdout.contains("GITHUB_TOKEN=ABSENT"));
    }

    #[test]
    fn invocation_unknown_outcome_is_never_retried() {
        let mut invocation =
            ToolInvocation::new("filesystem.write", serde_json::json!({"path":"a"}));
        assert_eq!(invocation.outcome, InvocationOutcome::Pending);
        invocation.mark_outcome_unknown();
        assert!(!invocation.may_execute());
        assert_eq!(invocation.outcome, InvocationOutcome::OutcomeUnknown);
    }

    #[test]
    fn builtins_expose_metadata_schemas_risk_and_reference_only_credentials() {
        let tools = builtin_tools();
        let names: Vec<_> = tools.iter().map(|t| t.name).collect();
        assert!(names.contains(&"filesystem.list"));
        assert!(names.contains(&"filesystem.read"));
        assert!(names.contains(&"filesystem.write"));
        assert!(names.contains(&"shell.execute"));

        for tool in &tools {
            assert!(!tool.version.is_empty());
            assert!(!tool.description.is_empty());
            assert!(tool.input_schema.is_object());
            assert!(tool.output_schema.is_object());
            assert!(tool.credentials_are_references_only);
        }

        let write_meta = tools.iter().find(|t| t.name == "filesystem.write").unwrap();
        assert_eq!(write_meta.risk, ToolRisk::Mutating);
        assert!(write_meta.side_effecting);
        assert!(write_meta.idempotent);

        let shell_meta = tools.iter().find(|t| t.name == "shell.execute").unwrap();
        assert_eq!(shell_meta.risk, ToolRisk::Mutating);
        assert!(shell_meta.side_effecting);
        assert!(!shell_meta.idempotent);
    }
}

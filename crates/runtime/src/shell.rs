//! CONTROLLED_LOCAL: trusted executable + exact argv, not an OS sandbox.
//! No PATH search, inherited secrets, shell parsing, or unrestricted argument patterns.
use crate::native_tools::ShellResult;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRule {
    executable: PathBuf,
    args: Vec<String>,
    timeout_ms: u64,
    #[serde(skip)]
    digest: String,
}

#[derive(Clone, Debug, Default)]
pub struct ShellPolicy {
    rules: Vec<CommandRule>,
}

#[cfg(all(test, windows))]
thread_local! { pub(crate) static REJECT_JOB_ASSIGNMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

impl ShellPolicy {
    pub fn from_json(json: &str) -> Result<Self, String> {
        let mut rules: Vec<CommandRule> =
            serde_json::from_str(json).map_err(|_| "Invalid shell allowlist JSON".to_owned())?;
        if rules.len() > 128 {
            return Err("Shell allowlist too large".into());
        }
        for rule in &mut rules {
            if !rule.executable.is_absolute()
                || rule
                    .executable
                    .extension()
                    .is_none_or(|e| !e.eq_ignore_ascii_case("exe"))
                || rule.executable.to_string_lossy().contains('\0')
                || rule.args.len() > 64
                || rule.args.iter().any(|a| a.contains('\0') || a.len() > 4096)
                || !(1..=30_000).contains(&rule.timeout_ms)
            {
                return Err(
                    "Shell allowlist requires absolute .exe, exact argv and 1..30000ms timeout"
                        .into(),
                );
            }
            // Validate now and again at invocation. Config is trusted operator input.
            let canonical = std::fs::canonicalize(&rule.executable)
                .map_err(|_| "Allowlisted executable unavailable")?;
            if !canonical.is_file() {
                return Err("Allowlisted executable is not a file".into());
            }
            #[cfg(windows)]
            {
                rule.digest = windows::Prepared::pin(&rule.executable)?.2;
            }
        }
        Ok(Self { rules })
    }

    pub(crate) fn prepare(
        &self,
        root: &Path,
        executable: &str,
        args: &[String],
    ) -> Result<PreparedShell, String> {
        let rule = self
            .rules
            .iter()
            .find(|r| r.executable == Path::new(executable) && r.args == args)
            .ok_or("command is not allowlisted")?;
        #[cfg(windows)]
        {
            let prepared = windows::Prepared::new(
                root,
                &rule.executable,
                args,
                Duration::from_millis(rule.timeout_ms),
            )?;
            if prepared.digest != rule.digest {
                return Err(
                    "Allowlisted executable changed; operator reconfiguration required".into(),
                );
            }
            Ok(prepared)
        }
        #[cfg(not(windows))]
        {
            let _ = (root, rule);
            Err("Shell containment unavailable on this platform".into())
        }
    }

    #[cfg(test)]
    pub(crate) async fn execute(
        &self,
        root: &Path,
        executable: &str,
        args: &[String],
        cancel: impl std::future::Future<Output = ()>,
    ) -> Result<ShellResult, String> {
        self.prepare(root, executable, args)?
            .spawn()?
            .wait(cancel)
            .await
    }
}

#[cfg(windows)]
pub(crate) use windows::Prepared as PreparedShell;
#[cfg(not(windows))]
pub(crate) struct PreparedShell;
#[cfg(not(windows))]
impl PreparedShell {
    pub(crate) fn spawn(self) -> Result<Self, String> {
        Err("Shell containment unavailable".into())
    }
    pub(crate) async fn wait(
        self,
        _: impl std::future::Future<Output = ()>,
    ) -> Result<ShellResult, String> {
        Err("Shell containment unavailable".into())
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::{
        ffi::OsStr,
        fs::{File, OpenOptions},
        io::Read,
        mem::size_of,
        os::windows::{
            ffi::OsStrExt,
            fs::OpenOptionsExt,
            io::{AsRawHandle, FromRawHandle, OwnedHandle},
        },
        ptr::{null, null_mut},
    };
    use windows_sys::Win32::{
        Foundation::*,
        Security::SECURITY_ATTRIBUTES,
        Storage::FileSystem::*,
        System::{JobObjects::*, Pipes::CreatePipe, Threading::*},
    };
    const MAX_OUTPUT: usize = crate::native_tools::MAX_OUTPUT_BYTES;
    fn err() -> String {
        "shell containment or process operation failed".into()
    }
    fn owned(handle: HANDLE) -> Result<OwnedHandle, String> {
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            Err(err())
        } else {
            // SAFETY: callers transfer a freshly created kernel handle.
            Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
        }
    }
    fn wide(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(Some(0)).collect()
    }
    // Windows CRT argv escaping. No cmd.exe/batch parsing, no raw arguments.
    fn quote(value: &str) -> String {
        let mut out = String::from("\"");
        let mut slashes = 0;
        for c in value.chars() {
            if c == '\\' {
                slashes += 1;
                continue;
            }
            if c == '"' {
                out.extend(std::iter::repeat_n('\\', slashes * 2 + 1));
            } else {
                out.extend(std::iter::repeat_n('\\', slashes));
            }
            slashes = 0;
            out.push(c);
        }
        out.extend(std::iter::repeat_n('\\', slashes * 2));
        out.push('"');
        out
    }
    struct Attributes {
        storage: Vec<usize>,
        initialized: bool,
    }
    impl Attributes {
        fn pointer(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
            self.storage.as_mut_ptr().cast()
        }
        fn new(handles: &mut [HANDLE]) -> Result<Self, String> {
            let mut bytes = 0;
            // SAFETY: sizing call, then aligned buffer remains alive through CreateProcessW.
            unsafe {
                InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut bytes);
            }
            let mut result = Self {
                storage: vec![0; bytes.div_ceil(size_of::<usize>())],
                initialized: false,
            };
            unsafe {
                if InitializeProcThreadAttributeList(result.pointer(), 1, 0, &mut bytes) == 0 {
                    return Err(err());
                }
                result.initialized = true;
                if UpdateProcThreadAttribute(
                    result.pointer(),
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    handles.as_mut_ptr().cast(),
                    std::mem::size_of_val(handles),
                    null_mut(),
                    null(),
                ) == 0
                {
                    return Err(err());
                }
            }
            Ok(result)
        }
    }
    impl Drop for Attributes {
        fn drop(&mut self) {
            // SAFETY: initialized attribute storage, not shared with children.
            if self.initialized {
                unsafe {
                    DeleteProcThreadAttributeList(self.pointer());
                }
            }
        }
    }
    fn pipe() -> Result<(File, OwnedHandle), String> {
        let mut read = null_mut();
        let mut write = null_mut();
        let security = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        // SAFETY: valid output pointers; whitelist ensures only write end is inherited.
        if unsafe { CreatePipe(&mut read, &mut write, &security, 0) } == 0 {
            return Err(err());
        }
        let read = owned(read)?;
        let write = owned(write)?;
        if unsafe { SetHandleInformation(read.as_raw_handle(), HANDLE_FLAG_INHERIT, 0) } == 0 {
            return Err(err());
        }
        Ok((File::from(read), write))
    }
    struct Job(OwnedHandle);
    impl Job {
        fn new() -> Result<Self, String> {
            let job = Self(owned(unsafe { CreateJobObjectW(null(), null()) })?);
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            #[cfg(test)]
            if REJECT_JOB_ASSIGNMENT.get() {
                // Ask the kernel to reject assignment; never fake the native API result.
                limits.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
                limits.BasicLimitInformation.ActiveProcessLimit = 0;
            }
            // Neither BREAKAWAY_OK nor SILENT_BREAKAWAY_OK. Job handle is non-inheritable, unnamed.
            if unsafe {
                SetInformationJobObject(
                    job.0.as_raw_handle(),
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            } == 0
            {
                return Err(err());
            }
            Ok(job)
        }
        fn empty(&self) -> Result<bool, String> {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            if unsafe {
                QueryInformationJobObject(
                    self.0.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    null_mut(),
                )
            } == 0
            {
                return Err(err());
            }
            Ok(accounting.ActiveProcesses == 0)
        }
        fn terminate(&self) -> Result<(), String> {
            // Retain waitable handles before termination: ActiveProcesses can reach zero
            // just before the kernel process object becomes signaled.
            let mut buffer = vec![0usize; 256];
            loop {
                let ok = unsafe {
                    QueryInformationJobObject(
                        self.0.as_raw_handle(),
                        JobObjectBasicProcessIdList,
                        buffer.as_mut_ptr().cast(),
                        (buffer.len() * size_of::<usize>()) as u32,
                        null_mut(),
                    )
                };
                if ok != 0 {
                    break;
                }
                if std::io::Error::last_os_error().raw_os_error() != Some(ERROR_MORE_DATA as i32) {
                    return Err(err());
                }
                buffer.resize(buffer.len() * 2, 0);
            }
            let ids = buffer.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>();
            let ids = unsafe {
                std::slice::from_raw_parts(
                    (*ids).ProcessIdList.as_ptr(),
                    (*ids).NumberOfProcessIdsInList as usize,
                )
            };
            let handles: Vec<_> = ids
                .iter()
                .filter_map(|pid| {
                    owned(unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, *pid as u32) }).ok()
                })
                .collect();
            if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
                return Err(err());
            }
            // Termination is asynchronous. Do not acknowledge until ALL members are gone.
            while !self.empty()? {
                std::thread::yield_now();
            }
            for handle in handles {
                if unsafe { WaitForSingleObject(handle.as_raw_handle(), INFINITE) } != WAIT_OBJECT_0
                {
                    return Err(err());
                }
            }
            Ok(())
        }
    }
    impl Drop for Job {
        fn drop(&mut self) {
            let _ = self.terminate();
        } // KILL_ON_JOB_CLOSE also protects abrupt future drop.
    }
    pub(crate) struct Prepared {
        root: nalarvo_application::project_filesystem::ProjectFilesystem,
        executable: PathBuf,
        _executable: File,
        _parents: nalarvo_application::project_filesystem::ProjectFilesystem,
        args: Vec<String>,
        timeout: Duration,
        pub(crate) digest: String,
    }
    impl Prepared {
        pub(crate) fn pin(
            exe: &Path,
        ) -> Result<
            (
                nalarvo_application::project_filesystem::ProjectFilesystem,
                File,
                String,
            ),
            String,
        > {
            use sha2::{Digest, Sha256};
            let text = exe.to_str().ok_or_else(err)?;
            // Local drive path only: no UNC/device aliases, ADS, relative segments or wildcards.
            if text.len() < 3
                || !text.as_bytes()[0].is_ascii_alphabetic()
                || text.as_bytes()[1] != b':'
                || !matches!(text.as_bytes()[2], b'\\' | b'/')
                || nalarvo_application::project_filesystem::components(&text[3..]).is_err()
            {
                return Err(err());
            }
            let parents = nalarvo_application::project_filesystem::ProjectFilesystem::open(
                exe.parent().ok_or_else(err)?,
            )
            .map_err(|_| err())?;
            let file = OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(exe)
                .map_err(|_| err())?;
            let meta = file.metadata().map_err(|_| err())?;
            use std::os::windows::fs::MetadataExt;
            if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Err(err());
            }
            let mut hasher = Sha256::new();
            let mut reader = &file;
            let mut bytes = [0; 8192];
            loop {
                let n = reader.read(&mut bytes).map_err(|_| err())?;
                if n == 0 {
                    break;
                }
                hasher.update(&bytes[..n]);
            }
            Ok((parents, file, format!("{:x}", hasher.finalize())))
        }
        pub(crate) fn new(
            root: &Path,
            exe: &Path,
            args: &[String],
            timeout: Duration,
        ) -> Result<Self, String> {
            let (parents, file, digest) = Self::pin(exe)?;
            let root = nalarvo_application::project_filesystem::ProjectFilesystem::open(root)
                .map_err(|_| err())?;
            Ok(Self {
                root,
                executable: exe.to_owned(),
                _executable: file,
                _parents: parents,
                args: args.to_vec(),
                timeout,
                digest,
            })
        }
        pub(crate) fn spawn(self) -> Result<Running, String> {
            let job = Job::new()?;
            let (stdout, out_write) = pipe()?;
            let (stderr, err_write) = pipe()?;
            let security = SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: null_mut(),
                bInheritHandle: 1,
            };
            let nul = wide(OsStr::new("NUL"));
            let stdin = owned(unsafe {
                CreateFileW(
                    nul.as_ptr(),
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    &security,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    null_mut(),
                )
            })?;
            let mut handles = [
                stdin.as_raw_handle(),
                out_write.as_raw_handle(),
                err_write.as_raw_handle(),
            ];
            let mut attributes = Attributes::new(&mut handles)?;
            let mut startup = STARTUPINFOEXW::default();
            startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
            startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
            startup.StartupInfo.hStdInput = handles[0];
            startup.StartupInfo.hStdOutput = handles[1];
            startup.StartupInfo.hStdError = handles[2];
            startup.lpAttributeList = attributes.pointer();
            let application = wide(self.executable.as_os_str());
            let command = std::iter::once(self.executable.to_string_lossy().into_owned())
                .chain(self.args.iter().cloned())
                .map(|s| quote(&s))
                .collect::<Vec<_>>()
                .join(" ");
            let mut command = wide(OsStr::new(&command));
            if command.len() > 32767 {
                return Err("Shell argv too long".into());
            }
            let cwd = wide(self.root.root.as_os_str());
            // Minimal fixed environment. Do not trust parent SystemRoot/PATH or pass credentials.
            let mut system = vec![0u16; 32768];
            let length = unsafe {
                windows_sys::Win32::System::SystemInformation::GetWindowsDirectoryW(
                    system.as_mut_ptr(),
                    system.len() as u32,
                )
            } as usize;
            if length == 0 || length >= system.len() {
                return Err(err());
            }
            let system = String::from_utf16(&system[..length]).map_err(|_| err())?;
            let mut environment: Vec<u16> = format!("SystemRoot={system}\0WINDIR={system}\0\0")
                .encode_utf16()
                .collect();
            let mut process = PROCESS_INFORMATION::default();
            // SAFETY: all buffers live; absolute explicit application; inherited HANDLE_LIST only.
            if unsafe {
                CreateProcessW(
                    application.as_ptr(),
                    command.as_mut_ptr(),
                    null(),
                    null(),
                    1,
                    CREATE_SUSPENDED
                        | CREATE_UNICODE_ENVIRONMENT
                        | EXTENDED_STARTUPINFO_PRESENT
                        | CREATE_NO_WINDOW,
                    environment.as_mut_ptr().cast(),
                    cwd.as_ptr(),
                    &startup.StartupInfo,
                    &mut process,
                )
            } == 0
            {
                return Err(err());
            }
            let process_handle = owned(process.hProcess)?;
            let thread = owned(process.hThread)?;
            if unsafe {
                AssignProcessToJobObject(job.0.as_raw_handle(), process_handle.as_raw_handle())
            } == 0
            {
                unsafe {
                    TerminateProcess(process_handle.as_raw_handle(), 1);
                    WaitForSingleObject(process_handle.as_raw_handle(), INFINITE);
                }
                return Err("Job assignment failed before resume".into());
            }
            if unsafe { ResumeThread(thread.as_raw_handle()) } != 1 {
                job.terminate()?;
                return Err(err());
            }
            drop(out_write);
            drop(err_write);
            drop(stdin);
            let output = tokio::task::spawn_blocking(move || read(stdout));
            let errors = tokio::task::spawn_blocking(move || read(stderr));
            Ok(Running {
                job,
                process: process_handle,
                output,
                errors,
                timeout: self.timeout,
                _prepared: self,
            })
        }
    }
    fn read(mut file: File) -> Result<(Vec<u8>, bool), String> {
        let mut result = Vec::new();
        let mut buffer = [0; 8192];
        let mut truncated = false;
        loop {
            let count = file.read(&mut buffer).map_err(|_| err())?;
            if count == 0 {
                break;
            }
            let keep = count.min(MAX_OUTPUT - result.len());
            result.extend_from_slice(&buffer[..keep]);
            truncated |= keep < count; // Drain excess, never block the producer or allocate unboundedly.
        }
        Ok((result, truncated))
    }
    pub(crate) struct Running {
        job: Job,
        process: OwnedHandle,
        output: tokio::task::JoinHandle<Result<(Vec<u8>, bool), String>>,
        errors: tokio::task::JoinHandle<Result<(Vec<u8>, bool), String>>,
        timeout: Duration,
        _prepared: Prepared,
    }
    impl Running {
        pub(crate) async fn wait(
            self,
            cancel: impl std::future::Future<Output = ()>,
        ) -> Result<ShellResult, String> {
            let completed = async {
                loop {
                    match unsafe { WaitForSingleObject(self.process.as_raw_handle(), 0) } {
                        WAIT_OBJECT_0 => return Ok(()),
                        WAIT_TIMEOUT => tokio::time::sleep(Duration::from_millis(10)).await,
                        _ => return Err(err()),
                    }
                }
            };
            let reason = tokio::select! {
                result=completed=>result,
                _=tokio::time::sleep(self.timeout)=>Err("shell execution timed out".into()),
                _=cancel=>Err("shell execution cancelled or fenced".into()),
            };
            // Also reap descendants after a successful parent exit.
            self.job.terminate()?;
            let mut code = 0;
            if unsafe { GetExitCodeProcess(self.process.as_raw_handle(), &mut code) } == 0 {
                return Err(err());
            }
            let out = self.output.await.map_err(|_| err())??;
            let errors = self.errors.await.map_err(|_| err())??;
            reason?;
            Ok(ShellResult {
                status: Some(code as i32),
                stdout: String::from_utf8_lossy(&out.0).into_owned(),
                stderr: String::from_utf8_lossy(&errors.0).into_owned(),
                truncated: out.1 || errors.1,
            })
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn pinned_image_and_ancestors_reject_substitution_until_spawn() {
            let temp = tempfile::tempdir().unwrap();
            let parent = temp.path().join("bin");
            std::fs::create_dir(&parent).unwrap();
            let exe = parent.join("trusted.exe");
            std::fs::copy(std::env::current_exe().unwrap(), &exe).unwrap();
            let pinned = Prepared::new(temp.path(), &exe, &[], Duration::from_secs(1)).unwrap();
            assert!(std::fs::write(&exe, b"replacement").is_err());
            assert!(std::fs::remove_file(&exe).is_err());
            assert!(std::fs::rename(&exe, parent.join("moved.exe")).is_err());
            assert!(std::fs::rename(&parent, temp.path().join("moved")).is_err());
            assert_eq!(Prepared::pin(&exe).unwrap().2, pinned.digest);
            drop(pinned);
            let writer = OpenOptions::new().write(true).open(&exe).unwrap();
            assert!(Prepared::pin(&exe).is_err());
            drop(writer);
            std::fs::write(&exe, b"replacement").unwrap();
        }
    }
}

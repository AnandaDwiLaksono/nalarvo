//! Windows local-NTFS confinement. Relative opens, no reparse traversal, pinned ancestors.
//! This is not an OS sandbox; privileged/raw-volume writers are outside this boundary.
use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io,
    mem::size_of,
    os::windows::{
        ffi::OsStrExt,
        fs::OpenOptionsExt,
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Component, Path, PathBuf, Prefix},
};
use windows_sys::{
    Wdk::{
        Foundation::OBJECT_ATTRIBUTES,
        Storage::FileSystem::{
            FILE_CREATE, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN,
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, FileRenameInformationEx,
            NtCreateFile, NtSetInformationFile,
        },
    },
    Win32::{
        Foundation::{
            LocalFree, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, RtlNtStatusToDosError,
            UNICODE_STRING,
        },
        Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
        Storage::FileSystem::*,
        System::IO::IO_STATUS_BLOCK,
    },
};

pub fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "Project filesystem binding denied",
    )
}

fn info(file: &File) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: live owned file handle and correctly sized output structure.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(denied());
    }
    Ok(info)
}

fn identity(file: &File) -> io::Result<String> {
    let meta = info(file)?;
    let mut id = FILE_ID_INFO::default();
    // SAFETY: live handle; FileIdInfo requires exactly FILE_ID_INFO.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&mut id as *mut FILE_ID_INFO).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(format!(
        "{}:{:?}:{}:{}",
        id.VolumeSerialNumber,
        id.FileId.Identifier,
        meta.ftCreationTime.dwHighDateTime,
        meta.ftCreationTime.dwLowDateTime
    ))
}

fn relative_open(parent: &File, name: &OsStr, access: u32, directory: bool) -> io::Result<File> {
    let mut wide: Vec<u16> = name.encode_wide().collect();
    let length =
        u16::try_from(wide.len().checked_mul(2).ok_or_else(denied)?).map_err(|_| denied())?;
    let mut name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: wide.as_mut_ptr(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &mut name,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut status = IO_STATUS_BLOCK::default();
    let mut handle = std::ptr::null_mut();
    // SAFETY: all pointers live through the synchronous call. Single-component relative
    // name; root handle retained. OPEN_REPARSE_POINT never follows the final component.
    let result = unsafe {
        NtCreateFile(
            &mut handle,
            access | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            &attributes,
            &mut status,
            std::ptr::null(),
            FILE_ATTRIBUTE_NORMAL,
            if directory {
                FILE_SHARE_READ
            } else {
                FILE_SHARE_READ | FILE_SHARE_DELETE
            },
            FILE_OPEN,
            FILE_OPEN_REPARSE_POINT
                | FILE_SYNCHRONOUS_IO_NONALERT
                | if directory {
                    FILE_DIRECTORY_FILE
                } else {
                    FILE_NON_DIRECTORY_FILE
                },
            std::ptr::null(),
            0,
        )
    };
    if result < 0 {
        // SAFETY: conversion accepts any NTSTATUS, has no pointer arguments.
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(result) } as i32,
        ));
    }
    // SAFETY: successful NtCreateFile returns a new owned kernel file handle.
    let file = unsafe { File::from_raw_handle(handle) };
    let meta = info(&file)?;
    if !directory && meta.nNumberOfLinks != 1 {
        // Existing hard links can alias data outside Project root. Never mutate/read them.
        return Err(denied());
    }
    Ok(file)
}

pub fn components(input: &str) -> io::Result<Vec<&OsStr>> {
    if input.contains([':', '\0']) || input.starts_with(['/', '\\']) {
        return Err(denied());
    }
    let mut result = Vec::new();
    for component in Path::new(input).components() {
        let Component::Normal(name) = component else {
            return Err(denied());
        };
        let text = name.to_str().ok_or_else(denied)?;
        if text.ends_with(['.', ' ']) || text.contains(['<', '>', '"', '|', '?', '*']) {
            return Err(denied());
        }
        let stem = text.split('.').next().unwrap_or("").to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || ["COM", "LPT"].iter().any(|p| {
                stem.strip_prefix(p)
                    .is_some_and(|n| n.len() == 1 && n.as_bytes()[0].is_ascii_digit())
            })
        {
            return Err(denied());
        }
        result.push(name);
    }
    Ok(result)
}

pub struct ProjectFilesystem {
    pub root: PathBuf,
    // Keep every ancestor open without SHARE_WRITE/SHARE_DELETE. This prevents namespace
    // replacement and reparse conversion, including moving the Project root itself.
    ancestors: Vec<File>,
}

impl ProjectFilesystem {
    pub fn open(root: &Path) -> io::Result<Self> {
        let mut parts = root.components();
        let drive = match parts.next() {
            Some(Component::Prefix(p)) => match p.kind() {
                Prefix::Disk(d) | Prefix::VerbatimDisk(d) => d,
                _ => return Err(denied()),
            },
            _ => return Err(denied()),
        };
        if parts.next() != Some(Component::RootDir) {
            return Err(denied());
        }
        let anchor = PathBuf::from(format!("{}:\\", drive as char));
        let wide: Vec<u16> = anchor.as_os_str().encode_wide().chain(Some(0)).collect();
        // SAFETY: NUL-terminated drive-root string.
        if unsafe { GetDriveTypeW(wide.as_ptr()) } != 3 {
            return Err(denied());
        }
        let file = OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&anchor)?;
        let mut filesystem = [0u16; 32];
        // SAFETY: live handle and valid writable filesystem-name buffer; unused outputs null.
        if unsafe {
            GetVolumeInformationByHandleW(
                file.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                filesystem.as_mut_ptr(),
                filesystem.len() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if String::from_utf16_lossy(
            &filesystem[..filesystem.iter().position(|v| *v == 0).unwrap_or(32)],
        ) != "NTFS"
        {
            return Err(denied());
        }
        let mut ancestors = vec![file];
        for part in parts {
            let Component::Normal(name) = part else {
                return Err(denied());
            };
            let text = name.to_str().ok_or_else(denied)?;
            if components(text)?.len() != 1 {
                return Err(denied());
            }
            let next = relative_open(ancestors.last().unwrap(), name, FILE_LIST_DIRECTORY, true)?;
            ancestors.push(next);
        }
        // Canonicalize only AFTER the entire namespace is pinned and reparse-free.
        let root = fs::canonicalize(root)?;
        Ok(Self { root, ancestors })
    }

    pub fn root_binding(&self) -> io::Result<serde_json::Value> {
        let ids: io::Result<Vec<_>> = self.ancestors.iter().map(identity).collect();
        Ok(serde_json::json!({"ancestors": ids?}))
    }

    pub fn prepare(&self, input: &str, write: bool, directory: bool) -> io::Result<PreparedPath> {
        let parts = components(input)?;
        if parts.is_empty() && !directory {
            return Err(denied());
        }
        let mut parents = Vec::new();
        for name in parts.iter().take(parts.len().saturating_sub(1)) {
            let parent = parents
                .last()
                .unwrap_or_else(|| self.ancestors.last().unwrap());
            parents.push(relative_open(parent, name, FILE_LIST_DIRECTORY, true)?);
        }
        let parent = parents
            .last()
            .unwrap_or_else(|| self.ancestors.last().unwrap());
        let mut binding = self.root_binding()?;
        binding["parents"] = serde_json::json!(
            parents
                .iter()
                .map(identity)
                .collect::<io::Result<Vec<_>>>()?
        );
        let target = if let Some(name) = parts.last() {
            match relative_open(
                parent,
                name,
                if directory {
                    FILE_LIST_DIRECTORY
                } else {
                    FILE_READ_DATA
                },
                directory,
            ) {
                Ok(file) => Some(file),
                Err(e) if write && e.kind() == io::ErrorKind::NotFound => None,
                Err(e) => return Err(e),
            }
        } else {
            None
        };
        binding["target"] = match &target {
            Some(file) => identity(file)?.into(),
            None => serde_json::Value::Null,
        };
        if write && let Some(file) = &target {
            use sha2::{Digest, Sha256};
            use std::io::Read;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(denied());
            }
            binding["content_sha256"] = hex::encode(Sha256::digest(bytes)).into();
        }
        Ok(PreparedPath {
            _parents: parents,
            target,
            name: parts.last().map(|name| name.to_os_string()),
            relative: input.to_owned(),
            binding,
        })
    }

    pub fn write_binding(&self, input: &str) -> io::Result<serde_json::Value> {
        Ok(self.prepare(input, true, false)?.binding)
    }

    pub fn write(&self, path: PreparedPath, bytes: &[u8]) -> io::Result<()> {
        self.publish(path, bytes, |_, _| Ok(()))
    }

    fn publish(
        &self,
        path: PreparedPath,
        bytes: &[u8],
        ready: impl FnOnce(&File, &File) -> io::Result<()>,
    ) -> io::Result<()> {
        use std::io::Write;
        let parent = path
            ._parents
            .last()
            .unwrap_or_else(|| self.ancestors.last().unwrap());
        let name = path.name.as_deref().ok_or_else(denied)?;
        if path.binding["ancestors"] != self.root_binding()?["ancestors"] {
            return Err(denied());
        }
        // Protected DACL permits only owner and SYSTEM. This is NOT a boundary against
        // the Core's own OS token, administrators, injected code or kernel access.
        let stage = private_create(
            parent,
            OsStr::new(&format!(".nalarvo-write-{}", uuid::Uuid::now_v7())),
        )?;
        let mut pending = PendingWrite {
            content: Some(stage),
            published: false,
        };
        let content = pending.content.as_mut().unwrap();
        content.write_all(bytes)?;
        content.sync_all()?;
        // Target may be replaced while staging. Refuse changed material before publish.
        let current = self.prepare(&path.relative, true, false)?;
        if current.binding != path.binding {
            return Err(denied());
        }
        ready(parent, content)?;
        // No data writes after publication. Old target aliases never receive new bytes.
        // Root/parents stay pinned. Null RootDirectory plus a single-component name
        // means rename within the SOURCE file's directory, never daemon process CWD.
        let name: Vec<u16> = name.encode_wide().collect();
        let size = size_of::<FILE_RENAME_INFO>() + name.len() * 2;
        let mut buffer = vec![0usize; size.div_ceil(size_of::<usize>())];
        let rename = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();
        // SAFETY: aligned, zeroed buffer includes trailing name and terminator. Handles
        // retained; validated name is one relative component. No path-based fallback.
        let success = unsafe {
            // REPLACE_IF_EXISTS | POSIX_SEMANTICS; CREATE never replaces a new arrival.
            (*rename).Anonymous.Flags = if path.target.is_some() { 1 | 2 } else { 0 };
            (*rename).RootDirectory = std::ptr::null_mut();
            (*rename).FileNameLength = (name.len() * 2) as u32;
            std::ptr::copy_nonoverlapping(
                name.as_ptr(),
                (*rename).FileName.as_mut_ptr(),
                name.len(),
            );
            NtSetInformationFile(
                content.as_raw_handle(),
                &mut IO_STATUS_BLOCK::default(),
                rename.cast(),
                size as u32,
                FileRenameInformationEx,
            )
        };
        if success < 0 {
            return Err(io::Error::from_raw_os_error(
                unsafe { RtlNtStatusToDosError(success) } as i32,
            ));
        }
        pending.published = true;
        Ok(())
    }
}

pub struct PreparedPath {
    _parents: Vec<File>,
    pub target: Option<File>,
    name: Option<std::ffi::OsString>,
    relative: String,

    pub binding: serde_json::Value,
}

fn private_create(parent: &File, name: &OsStr) -> io::Result<File> {
    let sddl: Vec<u16> = "D:P(A;OICI;GA;;;OW)(A;OICI;GA;;;SY)"
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut descriptor = std::ptr::null_mut();
    // SAFETY: valid NUL-terminated SDDL and output pointer; freed below after create.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut wide: Vec<u16> = name.encode_wide().collect();
    let mut unicode = UNICODE_STRING {
        Length: (wide.len() * 2) as u16,
        MaximumLength: (wide.len() * 2) as u16,
        Buffer: wide.as_mut_ptr(),
    };
    let attrs = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &mut unicode,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        SecurityDescriptor: descriptor.cast(),
        ..Default::default()
    };
    let mut handle = std::ptr::null_mut();
    let mut status = IO_STATUS_BLOCK::default();
    // SAFETY: synchronous CREATE (never opens existing names), live parent and buffers.
    let result = unsafe {
        NtCreateFile(
            &mut handle,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
            &attrs,
            &mut status,
            std::ptr::null(),
            FILE_ATTRIBUTE_NORMAL,
            0,
            FILE_CREATE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT | FILE_NON_DIRECTORY_FILE,
            std::ptr::null(),
            0,
        )
    };
    // SAFETY: LocalAlloc-owned descriptor is no longer used by synchronous create.
    unsafe {
        LocalFree(descriptor);
    }
    if result < 0 {
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(result) } as i32,
        ));
    }
    // SAFETY: successful NtCreateFile returns a new owned handle.
    Ok(unsafe { File::from_raw_handle(handle) })
}

struct PendingWrite {
    content: Option<File>,
    published: bool,
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
        // SAFETY: owned live handles, exact disposition buffer. Cleanup never resolves
        // a path and must never delete the successfully published file.
        unsafe {
            if !self.published
                && let Some(content) = &self.content
            {
                SetFileInformationByHandle(
                    content.as_raw_handle(),
                    FileDispositionInfo,
                    (&disposition as *const FILE_DISPOSITION_INFO).cast(),
                    size_of::<FILE_DISPOSITION_INFO>() as u32,
                );
            }
            drop(self.content.take());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_does_not_write_old_inode_or_follow_replaced_target() {
        for replace in [false, true] {
            let project = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            fs::create_dir(project.path().join("parent")).unwrap();
            let target = project.path().join("parent/data");
            fs::write(&target, b"old").unwrap();
            fs::write(outside.path().join("untouched"), b"outside").unwrap();
            let filesystem = ProjectFilesystem::open(project.path()).unwrap();
            let prepared = filesystem.prepare("parent/data", true, false).unwrap();
            // Hook runs AFTER final validation, immediately before production rename.
            // No arbitrary sleeps; an attacker has the exact worst-case interleaving.
            filesystem
                .publish(prepared, b"new", |_, content| {
                    assert_eq!(fs::read(&target).unwrap(), b"old");
                    assert_eq!(info(content)?.nNumberOfLinks, 1);
                    assert!(fs::rename(project.path(), outside.path().join("moved-root")).is_err());
                    assert!(
                        fs::rename(
                            project.path().join("parent"),
                            outside.path().join("moved-parent")
                        )
                        .is_err()
                    );
                    if replace {
                        fs::rename(&target, outside.path().join("moved-target"))
                            .expect("attacker moves old target");
                        // NTFS retains the old name while its validated handle survives.
                        assert!(fs::hard_link(outside.path().join("untouched"), &target).is_err());
                    } else {
                        fs::hard_link(&target, outside.path().join("old-alias"))
                            .expect("attacker aliases old target");
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(fs::read(&target).unwrap(), b"new");
            assert_eq!(
                fs::read(outside.path().join("untouched")).unwrap(),
                b"outside"
            );
            assert_eq!(
                fs::read(
                    outside
                        .path()
                        .join(if replace { "moved-target" } else { "old-alias" })
                )
                .unwrap(),
                b"old"
            );
            assert_eq!(
                fs::read_dir(project.path().join("parent")).unwrap().count(),
                1
            );
        }
    }

    #[test]
    fn create_arrival_and_failed_publication_leave_no_staging_or_overwrite() {
        let project = tempfile::tempdir().unwrap();
        let filesystem = ProjectFilesystem::open(project.path()).unwrap();
        let prepared = filesystem.prepare("new", true, false).unwrap();
        assert!(
            filesystem
                .publish(prepared, b"bad", |_, _| {
                    fs::write(project.path().join("new"), b"arrival")
                })
                .is_err()
        );
        assert_eq!(fs::read(project.path().join("new")).unwrap(), b"arrival");
        assert_eq!(fs::read_dir(project.path()).unwrap().count(), 1);
    }

    #[test]
    fn private_staging_denies_restricted_principal_before_content_write() {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            Security::{
                Authorization::ConvertStringSidToSidW, CreateRestrictedToken,
                DACL_SECURITY_INFORMATION, DISABLE_MAX_PRIVILEGE, DuplicateToken, RevertToSelf,
                SID_AND_ATTRIBUTES, SecurityImpersonation, SetFileSecurityW, TOKEN_ALL_ACCESS,
            },
            System::Threading::{GetCurrentProcess, OpenProcessToken, SetThreadToken},
        };
        let project = tempfile::tempdir().unwrap();
        let root = ProjectFilesystem::open(project.path()).unwrap();
        let stage = private_create(root.ancestors.last().unwrap(), OsStr::new("staging")).unwrap();
        let outside = project.path().join("alias");
        // Everyone can create normal parent entries. Staging must independently deny
        // the restricted principal, even before its first content write.
        let sddl: Vec<u16> = "D:P(A;OICI;GA;;;WD)"
            .encode_utf16()
            .chain(Some(0))
            .collect();
        let mut descriptor = std::ptr::null_mut();
        let mut token = std::ptr::null_mut();
        let mut restricted = std::ptr::null_mut();
        let mut impersonation = std::ptr::null_mut();
        let mut sid = std::ptr::null_mut();
        let sid_text: Vec<u16> = "S-1-1-0".encode_utf16().chain(Some(0)).collect();
        let path: Vec<u16> = project
            .path()
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        // SAFETY: valid buffers and owned outputs. Revert BEFORE assertions/unwind.
        unsafe {
            assert_ne!(
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_ptr(),
                    1,
                    &mut descriptor,
                    std::ptr::null_mut()
                ),
                0
            );
            assert_ne!(
                SetFileSecurityW(path.as_ptr(), DACL_SECURITY_INFORMATION, descriptor),
                0
            );
            LocalFree(descriptor);
            assert_ne!(
                OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &mut token),
                0
            );
            assert_ne!(ConvertStringSidToSidW(sid_text.as_ptr(), &mut sid), 0);
            let restricting = SID_AND_ATTRIBUTES {
                Sid: sid,
                Attributes: 0,
            };
            assert_ne!(
                CreateRestrictedToken(
                    token,
                    DISABLE_MAX_PRIVILEGE,
                    0,
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    1,
                    &restricting,
                    &mut restricted
                ),
                0
            );
            assert_ne!(
                DuplicateToken(restricted, SecurityImpersonation, &mut impersonation),
                0
            );
            assert_ne!(SetThreadToken(std::ptr::null(), impersonation), 0);
            let ordinary = fs::write(project.path().join("ordinary"), b"allowed");
            let alias = fs::hard_link(project.path().join("staging"), &outside);
            let modify = fs::write(project.path().join("staging"), b"denied");
            let reverted = RevertToSelf();
            CloseHandle(impersonation);
            CloseHandle(restricted);
            CloseHandle(token);
            LocalFree(sid);
            assert_ne!(reverted, 0);
            ordinary.unwrap();
            assert_eq!(alias.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            assert!(modify.is_err());
        }
        assert_eq!(info(&stage).unwrap().nNumberOfLinks, 1);
        assert!(!outside.exists());
    }
}

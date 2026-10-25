//! Trusted Core adapter. Published blobs are immutable; no hash-addressed API exists.
use nalarvo_domain::information::ContentRef;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const MAX_CONTENT_BYTES: usize = 1024 * 1024;
pub trait ContentStore: Send + Sync {
    fn put(&self, bytes: &[u8]) -> io::Result<ContentRef>;
    fn read(&self, reference: &ContentRef) -> io::Result<Vec<u8>>;
    fn permits_project_root(&self, root: &Path) -> bool;
}
pub struct LocalContentStore {
    root: PathBuf,
    lock: Mutex<()>,
    #[cfg(windows)]
    filesystem: crate::project_filesystem::ProjectFilesystem,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "content storage integrity/boundary failure",
    )
}
impl LocalContentStore {
    pub fn open(root: &Path) -> io::Result<Self> {
        if !root.is_absolute() {
            return Err(invalid());
        }
        fs::create_dir_all(root)?;
        for ancestor in root.ancestors() {
            let m = fs::symlink_metadata(ancestor)?;
            if m.file_type().is_symlink() {
                return Err(invalid());
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                if m.file_attributes() & 0x400 != 0 {
                    return Err(invalid());
                }
            }
        }
        #[cfg(windows)]
        let filesystem = crate::project_filesystem::ProjectFilesystem::open(root)?;
        Ok(Self {
            root: fs::canonicalize(root)?,
            lock: Mutex::new(()),
            #[cfg(windows)]
            filesystem,
        })
    }
    fn read_unlocked(&self, reference: &ContentRef) -> io::Result<Vec<u8>> {
        if !reference.valid() {
            return Err(invalid());
        }
        #[cfg(windows)]
        let file = {
            self.filesystem
                .prepare(&reference.hash, false, false)?
                .target
                .ok_or_else(invalid)?
        };
        #[cfg(not(windows))]
        let file = {
            let path = self.root.join(&reference.hash);
            if fs::symlink_metadata(&path)?.file_type().is_symlink() {
                return Err(invalid());
            }
            fs::File::open(path)?
        };
        let mut bytes = Vec::new();
        file.take(MAX_CONTENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != reference.size_bytes
            || hex::encode(Sha256::digest(&bytes)) != reference.hash
        {
            return Err(invalid());
        }
        Ok(bytes)
    }
}
impl ContentStore for LocalContentStore {
    fn permits_project_root(&self, root: &Path) -> bool {
        let Ok(root) = fs::canonicalize(root) else {
            return false;
        };
        #[cfg(windows)]
        let (root, store) = (
            PathBuf::from(root.to_string_lossy().to_lowercase()),
            PathBuf::from(self.root.to_string_lossy().to_lowercase()),
        );
        #[cfg(not(windows))]
        let store = self.root.clone();
        !root.starts_with(&store) && !store.starts_with(root)
    }
    fn put(&self, bytes: &[u8]) -> io::Result<ContentRef> {
        if bytes.len() > MAX_CONTENT_BYTES {
            return Err(invalid());
        }
        // ponytail: one local adapter lock; per-hash locks only if contention matters.
        let _guard = self.lock.lock().map_err(|_| invalid())?;
        let reference = ContentRef {
            hash: hex::encode(Sha256::digest(bytes)),
            size_bytes: bytes.len() as u64,
        };
        match self.read_unlocked(&reference) {
            Ok(_) => return Ok(reference),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        #[cfg(windows)]
        {
            let path = self.filesystem.prepare(&reference.hash, true, false)?;
            if path.target.is_some() {
                return Err(invalid());
            }
            self.filesystem.write(path, bytes)?;
        }
        #[cfg(not(windows))]
        {
            use std::io::Write;
            let mut stage = tempfile::NamedTempFile::new_in(&self.root)?;
            stage.write_all(bytes)?;
            stage.as_file().sync_all()?;
            stage
                .persist_noclobber(self.root.join(&reference.hash))
                .map_err(|e| e.error)?;
            fs::File::open(&self.root)?.sync_all()?;
        }
        self.read_unlocked(&reference)?;
        Ok(reference)
    }
    fn read(&self, reference: &ContentRef) -> io::Result<Vec<u8>> {
        let _guard = self.lock.lock().map_err(|_| invalid())?;
        self.read_unlocked(reference)
    }
}

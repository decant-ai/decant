//! Cooperative local Unix storage. No deletion, takeover or corruption fallback.
use super::{E, ReplayBundleV1, bundle::MAX_BUNDLE};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
pub const MAX_STORE_BYTES_V1: u64 = 256 * 1024 * 1024;
const MAX_ENTRIES: usize = 64;
const LOCK: &str = "writer.lock";
pub struct BundleStoreV1 {
    root: PathBuf,
    _lock: File,
}
impl fmt::Debug for BundleStoreV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BundleStoreV1 { redacted }")
    }
}
fn name(id: &str) -> Result<String, E> {
    let hex = id
        .strip_prefix("sha256:v1:")
        .filter(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or(E::InvalidInput)?;
    Ok(format!("{hex}.bundle"))
}
#[cfg(unix)]
fn private(metadata: &fs::Metadata, directory: bool) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    metadata.permissions().mode() & 0o077 == 0
        && if directory {
            metadata.is_dir()
        } else {
            metadata.is_file() && metadata.nlink() == 1
        }
}
#[cfg(not(unix))]
fn private(_: &fs::Metadata, _: bool) -> bool {
    false
}
fn inspect(path: &Path, directory: bool) -> Result<fs::Metadata, E> {
    let m = fs::symlink_metadata(path).map_err(|_| E::StoreUnavailable)?;
    if m.file_type().is_symlink() || !private(&m, directory) {
        return Err(E::StoreUnavailable);
    }
    Ok(m)
}
fn create(path: &Path) -> Result<File, std::io::Error> {
    let mut o = OpenOptions::new();
    o.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    o.open(path)
}
impl BundleStoreV1 {
    pub fn open(root: &Path) -> Result<Self, E> {
        if !root.exists() {
            let mut b = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                b.mode(0o700);
            }
            b.create(root).map_err(|_| E::StoreUnavailable)?;
        }
        inspect(root, true)?;
        let root = root.canonicalize().map_err(|_| E::StoreUnavailable)?;
        let path = root.join(LOCK);
        let lock = match create(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if inspect(&path, false)?.len() != 0 {
                    return Err(E::StoreUnavailable);
                }
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|_| E::StoreUnavailable)?
            }
            Err(_) => return Err(E::StoreUnavailable),
        };
        let m = lock.metadata().map_err(|_| E::StoreUnavailable)?;
        if !private(&m, false) || m.len() != 0 {
            return Err(E::StoreUnavailable);
        }
        lock.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => E::StoreBusy,
            _ => E::StoreUnavailable,
        })?;
        let store = Self { root, _lock: lock };
        store.usage()?;
        Ok(store)
    }
    fn usage(&self) -> Result<(usize, u64), E> {
        inspect(&self.root, true)?;
        let mut count = 0;
        let mut bytes = 0u64;
        for item in fs::read_dir(&self.root).map_err(|_| E::StoreUnavailable)? {
            let item = item.map_err(|_| E::StoreUnavailable)?;
            let n = item
                .file_name()
                .into_string()
                .map_err(|_| E::StoreUnavailable)?;
            let meta = inspect(&item.path(), false)?;
            if n == LOCK {
                if meta.len() != 0 {
                    return Err(E::StoreUnavailable);
                }
                continue;
            }
            count += 1;
            if count > MAX_ENTRIES {
                return Err(E::StoreQuota);
            }
            let id = format!(
                "sha256:v1:{}",
                n.strip_suffix(".bundle").ok_or(E::StoreUnavailable)?
            );
            if name(&id)? != n || meta.len() > MAX_BUNDLE as u64 {
                return Err(E::StoreUnavailable);
            }
            bytes = bytes.checked_add(meta.len()).ok_or(E::StoreQuota)?;
            if bytes > MAX_STORE_BYTES_V1 {
                return Err(E::StoreQuota);
            }
        }
        Ok((count, bytes))
    }
    pub fn get(&self, id: &str) -> Result<ReplayBundleV1, E> {
        inspect(&self.root, true)?;
        let path = self.root.join(name(id)?);
        let m = inspect(&path, false)?;
        if m.len() > MAX_BUNDLE as u64 {
            return Err(E::InvalidBundle);
        }
        let f = File::open(path).map_err(|_| E::StoreUnavailable)?;
        let opened = f.metadata().map_err(|_| E::StoreUnavailable)?;
        if !private(&opened, false) || opened.len() != m.len() {
            return Err(E::StoreUnavailable);
        }
        let mut bytes = Vec::new();
        f.take(MAX_BUNDLE as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| E::StoreUnavailable)?;
        ReplayBundleV1::decode(&bytes, id)
    }
    pub fn put(&mut self, bundle: &ReplayBundleV1) -> Result<(), E> {
        let (count, bytes) = self.usage()?;
        let path = self.root.join(name(bundle.id())?);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let existing = self.get(bundle.id())?;
                if existing.bytes() != bundle.bytes() {
                    return Err(E::IdentityMismatch);
                }
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(E::StoreUnavailable),
        }
        if count == MAX_ENTRIES
            || bytes
                .checked_add(bundle.bytes.len() as u64)
                .is_none_or(|n| n > MAX_STORE_BYTES_V1)
        {
            return Err(E::StoreQuota);
        }
        let mut f = create(&path).map_err(|_| E::StoreUnavailable)?;
        f.write_all(bundle.bytes())
            .and_then(|_| f.sync_all())
            .map_err(|_| E::StoreUnavailable)?;
        File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(|_| E::StoreUnavailable)?;
        Ok(())
    }
}

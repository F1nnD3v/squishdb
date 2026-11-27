use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::types::{Error, Result};

/// OS-level advisory file lock guard (`.sqdb.lock`) supporting true multi-process
/// shared (concurrent readers) and exclusive (single writer) locking via
/// Windows `LockFileEx` / Unix `flock`.
pub struct OsFileLock {
    _file: File,
    _path: PathBuf,
    exclusive: bool,
}

#[cfg(windows)]
mod sys {
    use std::fs::File;
    use std::os::windows::io::AsRawHandle;

    type Handle = *mut core::ffi::c_void;
    type Dword = u32;
    type Bool = i32;

    const LOCKFILE_FAIL_IMMEDIATELY: Dword = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: Dword = 0x0000_0002;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: Dword,
        offset_high: Dword,
        h_event: Handle,
    }

    extern "system" {
        fn LockFileEx(
            h_file: Handle,
            dw_flags: Dword,
            dw_reserved: Dword,
            n_number_of_bytes_to_lock_low: Dword,
            n_number_of_bytes_to_lock_high: Dword,
            lp_overlapped: *mut Overlapped,
        ) -> Bool;

        fn UnlockFileEx(
            h_file: Handle,
            dw_reserved: Dword,
            n_number_of_bytes_to_unlock_low: Dword,
            n_number_of_bytes_to_unlock_high: Dword,
            lp_overlapped: *mut Overlapped,
        ) -> Bool;
    }

    pub fn try_lock(file: &File, exclusive: bool) -> std::io::Result<bool> {
        let mut ov = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: core::ptr::null_mut(),
        };
        let mut flags = LOCKFILE_FAIL_IMMEDIATELY;
        if exclusive {
            flags |= LOCKFILE_EXCLUSIVE_LOCK;
        }
        let ok = unsafe { LockFileEx(file.as_raw_handle(), flags, 0, 1, 0, &mut ov) };
        if ok != 0 {
            Ok(true)
        } else {
            let err = std::io::Error::last_os_error();
            // ERROR_LOCK_VIOLATION (33) or ERROR_IO_PENDING (997)
            if matches!(err.raw_os_error(), Some(33) | Some(997)) {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }

    pub fn lock_blocking(file: &File, exclusive: bool) -> std::io::Result<()> {
        let mut ov = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: core::ptr::null_mut(),
        };
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        };
        let ok = unsafe { LockFileEx(file.as_raw_handle(), flags, 0, 1, 0, &mut ov) };
        if ok != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn unlock(file: &File) {
        let mut ov = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: core::ptr::null_mut(),
        };
        unsafe {
            let _ = UnlockFileEx(file.as_raw_handle(), 0, 1, 0, &mut ov);
        }
    }
}

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    const LOCK_SH: i32 = 1;
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    const LOCK_UN: i32 = 8;

    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    pub fn try_lock(file: &File, exclusive: bool) -> std::io::Result<bool> {
        let op = if exclusive {
            LOCK_EX | LOCK_NB
        } else {
            LOCK_SH | LOCK_NB
        };
        let rc = unsafe { flock(file.as_raw_fd(), op) };
        if rc == 0 {
            Ok(true)
        } else {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }

    pub fn lock_blocking(file: &File, exclusive: bool) -> std::io::Result<()> {
        let op = if exclusive { LOCK_EX } else { LOCK_SH };
        loop {
            let rc = unsafe { flock(file.as_raw_fd(), op) };
            if rc == 0 {
                return Ok(());
            }
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
    }

    pub fn unlock(file: &File) {
        unsafe {
            let _ = flock(file.as_raw_fd(), LOCK_UN);
        }
    }
}

impl OsFileLock {
    pub fn lock_path_for(db_path: &Path) -> PathBuf {
        let mut os = db_path.as_os_str().to_os_string();
        os.push(".lock");
        PathBuf::from(os)
    }

    /// Acquires a non-blocking OS-level lock (exclusive for read-write, shared for read-only).
    /// Returns `Error::Busy` immediately if another handle/process holds a conflicting lock.
    pub fn try_acquire(db_path: &Path, exclusive: bool) -> Result<Self> {
        Self::try_acquire_path(Self::lock_path_for(db_path), db_path, exclusive)
    }

    /// Acquires a non-blocking OS-level lock on an explicit lock file path.
    pub fn try_acquire_path(
        lock_path: PathBuf,
        display_path: &Path,
        exclusive: bool,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        let acquired = sys::try_lock(&file, exclusive)?;
        if !acquired {
            return Err(Error::Busy(format!(
                "Database '{}' is locked by another {} connection",
                display_path.display(),
                if exclusive { "reader/writer" } else { "writer" }
            )));
        }
        Ok(OsFileLock {
            _file: file,
            _path: lock_path,
            exclusive,
        })
    }

    /// Acquires a blocking OS-level lock on an explicit lock file path (waits until available).
    pub fn acquire_blocking_path(lock_path: PathBuf, exclusive: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        sys::lock_blocking(&file, exclusive)?;
        Ok(OsFileLock {
            _file: file,
            _path: lock_path,
            exclusive,
        })
    }

    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }
}

impl Drop for OsFileLock {
    fn drop(&mut self) {
        sys::unlock(&self._file);
    }
}

/// Atomically replaces `dst` with `src` without ever deleting `dst` first!
/// Uses `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)` on Windows
/// and POSIX `std::fs::rename` on Unix.
pub fn atomic_replace_file(src: &Path, dst: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
        const MOVEFILE_WRITE_THROUGH: u32 = 0x8;
        extern "system" {
            fn MoveFileExW(
                lp_existing_file_name: *const u16,
                lp_new_file_name: *const u16,
                dw_flags: u32,
            ) -> i32;
        }
        let src_w: Vec<u16> = src
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let dst_w: Vec<u16> = dst
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let ok = unsafe {
            MoveFileExW(
                src_w.as_ptr(),
                dst_w.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if ok != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(src, dst)?;
        let parent = dst.parent().filter(|p| !p.as_os_str().is_empty());
        let dir_path = parent.unwrap_or_else(|| Path::new("."));
        if let Ok(dir) = File::open(dir_path) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

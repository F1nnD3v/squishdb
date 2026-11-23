use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::lock::OsFileLock;
use crate::sql::{execute_statement, parse_sql, parse_sql_with_params, Statement};
use crate::storage::StorageFile;
use crate::types::{
    read_str, read_uvarint, read_zigzag_varint, write_str, write_uvarint, write_zigzag_varint,
    Error, Result, Value,
};
use crate::wal::WalOp;
use crate::{Database, DbOptions, DbStats, QueryResult};

const MAX_COMMIT_HISTORY: usize = 4096;
const MVCC_LOG_MAGIC: &[u8; 4] = b"SQMV";
const MVCC_FLAG_VACUUM: u8 = 0x01;

#[derive(Debug, Clone, Default)]
struct WriteSet {
    /// Table name (lowercase) -> set of rowids updated or deleted by this transaction.
    modified_rowids: HashMap<String, HashSet<i64>>,
    /// Table name (lowercase) -> set of rowids inserted by this transaction.
    inserted_rowids: HashMap<String, HashSet<i64>>,
    /// Tables that had DDL executed (CREATE/DROP/ALTER/INDEX).
    ddl_tables: HashSet<String>,
    /// All tables written to (DML or DDL).
    touched_tables: HashSet<String>,
}

impl WriteSet {
    fn from_wal_ops(ops: &[WalOp]) -> Self {
        let mut ws = WriteSet::default();
        for op in ops {
            match op {
                WalOp::CreateTable(s) | WalOp::CreateTableV5(s) => {
                    let k = s.name.to_ascii_lowercase();
                    ws.ddl_tables.insert(k.clone());
                    ws.touched_tables.insert(k);
                }
                WalOp::DropTable(name)
                | WalOp::SetClusterBy {
                    table_name: name, ..
                }
                | WalOp::AddColumn {
                    table_name: name, ..
                }
                | WalOp::AddColumnV5 {
                    table_name: name, ..
                }
                | WalOp::CreateIndex {
                    table_name: name, ..
                }
                | WalOp::CreateIndexV5 {
                    table_name: name, ..
                } => {
                    let k = name.to_ascii_lowercase();
                    ws.ddl_tables.insert(k.clone());
                    ws.touched_tables.insert(k);
                }
                WalOp::DropColumn {
                    table_name: name, ..
                }
                | WalOp::RenameColumn {
                    table_name: name, ..
                } => {
                    let k = name.to_ascii_lowercase();
                    // Column drop/rename can affect referencing tables' FK metadata; treat conservatively.
                    ws.ddl_tables.insert("*".into());
                    ws.ddl_tables.insert(k.clone());
                    ws.touched_tables.insert(k);
                }
                WalOp::DropIndex(_) => {
                    ws.ddl_tables.insert("*".into());
                }
                WalOp::InsertRows { table_name, rows } => {
                    let k = table_name.to_ascii_lowercase();
                    ws.touched_tables.insert(k.clone());
                    let set = ws.inserted_rowids.entry(k).or_default();
                    for r in rows {
                        set.insert(r.rowid);
                    }
                }
                WalOp::DeleteRowIds { table_name, rowids } => {
                    let k = table_name.to_ascii_lowercase();
                    ws.touched_tables.insert(k.clone());
                    let inserted = ws.inserted_rowids.get(&k);
                    let set = ws.modified_rowids.entry(k).or_default();
                    for &id in rowids {
                        if !inserted.map_or(false, |ins| ins.contains(&id)) {
                            set.insert(id);
                        }
                    }
                }
                WalOp::UpdateRows { table_name, rows } => {
                    let k = table_name.to_ascii_lowercase();
                    ws.touched_tables.insert(k.clone());
                    let inserted = ws.inserted_rowids.get(&k);
                    let set = ws.modified_rowids.entry(k).or_default();
                    for r in rows {
                        if !inserted.map_or(false, |ins| ins.contains(&r.rowid)) {
                            set.insert(r.rowid);
                        }
                    }
                }
            }
        }
        ws
    }

    fn write_binary(&self, out: &mut Vec<u8>) {
        write_uvarint(self.ddl_tables.len() as u64, out);
        for t in &self.ddl_tables {
            write_str(t, out);
        }
        write_uvarint(self.touched_tables.len() as u64, out);
        for t in &self.touched_tables {
            write_str(t, out);
        }
        write_uvarint(self.modified_rowids.len() as u64, out);
        for (tbl, ids) in &self.modified_rowids {
            write_str(tbl, out);
            write_uvarint(ids.len() as u64, out);
            for &id in ids {
                write_zigzag_varint(id, out);
            }
        }
        write_uvarint(self.inserted_rowids.len() as u64, out);
        for (tbl, ids) in &self.inserted_rowids {
            write_str(tbl, out);
            write_uvarint(ids.len() as u64, out);
            for &id in ids {
                write_zigzag_varint(id, out);
            }
        }
    }

    fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let mut ws = WriteSet::default();
        let n_ddl = read_uvarint(buf, pos)? as usize;
        for _ in 0..n_ddl {
            ws.ddl_tables.insert(read_str(buf, pos)?);
        }
        let n_touched = read_uvarint(buf, pos)? as usize;
        for _ in 0..n_touched {
            ws.touched_tables.insert(read_str(buf, pos)?);
        }
        let n_mod = read_uvarint(buf, pos)? as usize;
        for _ in 0..n_mod {
            let tbl = read_str(buf, pos)?;
            let n_ids = read_uvarint(buf, pos)? as usize;
            let mut set = HashSet::with_capacity(n_ids);
            for _ in 0..n_ids {
                set.insert(read_zigzag_varint(buf, pos)?);
            }
            ws.modified_rowids.insert(tbl, set);
        }
        let n_ins = read_uvarint(buf, pos)? as usize;
        for _ in 0..n_ins {
            let tbl = read_str(buf, pos)?;
            let n_ids = read_uvarint(buf, pos)? as usize;
            let mut set = HashSet::with_capacity(n_ids);
            for _ in 0..n_ids {
                set.insert(read_zigzag_varint(buf, pos)?);
            }
            ws.inserted_rowids.insert(tbl, set);
        }
        Ok(ws)
    }

    fn conflicts_with(&self, earlier: &WriteSet) -> Option<String> {
        // 1. Any DDL conflict on overlapping tables
        if self.ddl_tables.contains("*") || earlier.ddl_tables.contains("*") {
            return Some("concurrent schema/index DDL".into());
        }
        for t in &self.ddl_tables {
            if earlier.touched_tables.contains(t) {
                return Some(format!("concurrent DDL conflict on table '{}'", t));
            }
        }
        for t in &earlier.ddl_tables {
            if self.touched_tables.contains(t) {
                return Some(format!("concurrent DDL conflict on table '{}'", t));
            }
        }

        // 2. Row-level write-write conflict (First-Committer-Wins)
        for (tbl, my_mods) in &self.modified_rowids {
            if let Some(other_mods) = earlier.modified_rowids.get(tbl) {
                if let Some(&rid) = my_mods.iter().find(|id| other_mods.contains(id)) {
                    return Some(format!(
                        "concurrent write-write conflict on table '{}' rowid {}",
                        tbl, rid
                    ));
                }
            }
            if let Some(other_ins) = earlier.inserted_rowids.get(tbl) {
                if let Some(&rid) = my_mods.iter().find(|id| other_ins.contains(id)) {
                    return Some(format!(
                        "concurrent write-write conflict on table '{}' rowid {}",
                        tbl, rid
                    ));
                }
            }
        }
        None
    }
}

struct CommittedTx {
    version: u64,
    write_set: WriteSet,
}

struct MvccMeta {
    committed_version: u64,
    oldest_retained_version: u64,
    history: VecDeque<CommittedTx>,
    active_snapshots: usize,
    wal_offset: u64,
    mvcc_log_offset: u64,
}

struct MvccCoordinator {
    db_path: PathBuf,
    commit_lock_path: PathBuf,
    snap_lock_path: PathBuf,
    mvcc_log_path: PathBuf,
    /// Canonical committed database state (shared across threads in this process; synced from disk across processes).
    committed: RwLock<Database>,
    /// In-process commit serialization lock (paired with cross-process OS lock on `commit_lock_path`).
    commit_lock: Mutex<()>,
    meta: Mutex<MvccMeta>,
}

fn mvcc_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut os = db_path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

/// Reads CRC32-verified cross-process MVCC commit log entries starting at `start_offset`.
/// Returns `(entries, new_offset, saw_vacuum_barrier)`.
fn read_mvcc_log_from(
    log_path: &Path,
    start_offset: u64,
) -> Result<(Vec<(u64, WriteSet)>, u64, bool)> {
    let mut f = match OpenOptions::new().read(true).open(log_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), 0, false));
        }
        Err(e) => return Err(e.into()),
    };
    let len = f.metadata()?.len();
    let effective_start = if start_offset > len { 0 } else { start_offset };
    if effective_start == len {
        return Ok((Vec::new(), len, false));
    }
    f.seek(SeekFrom::Start(effective_start))?;
    let mut raw = Vec::with_capacity((len - effective_start) as usize);
    f.read_to_end(&mut raw)?;

    let mut entries = Vec::new();
    let mut saw_vacuum = false;
    let mut pos = 0usize;
    // Frame header: magic(4) + version(8) + flags(1) + payload_len(4) + crc32(4) = 21 bytes
    while pos + 21 <= raw.len() {
        if &raw[pos..pos + 4] != MVCC_LOG_MAGIC {
            break;
        }
        let version = u64::from_le_bytes(raw[pos + 4..pos + 12].try_into().unwrap());
        let flags = raw[pos + 12];
        let payload_len = u32::from_le_bytes(raw[pos + 13..pos + 17].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(raw[pos + 17..pos + 21].try_into().unwrap());
        let p_start = pos + 21;
        let p_end = p_start.saturating_add(payload_len);
        if p_end > raw.len() {
            break;
        }
        let payload = &raw[p_start..p_end];
        if crc32fast::hash(payload) != expected_crc {
            break;
        }
        pos = p_end;

        if (flags & MVCC_FLAG_VACUUM) != 0 {
            saw_vacuum = true;
        }
        let mut p = 0usize;
        let ws = WriteSet::read_binary(payload, &mut p)?;
        entries.push((version, ws));
    }
    Ok((entries, effective_start + pos as u64, saw_vacuum))
}

fn append_mvcc_log_record(
    log_path: &Path,
    truncate_to: u64,
    version: u64,
    flags: u8,
    ws: &WriteSet,
    sync: bool,
) -> Result<u64> {
    let mut payload = Vec::new();
    ws.write_binary(&mut payload);
    let crc = crc32fast::hash(&payload);
    let mut hdr = [0u8; 21];
    hdr[0..4].copy_from_slice(MVCC_LOG_MAGIC);
    hdr[4..12].copy_from_slice(&version.to_le_bytes());
    hdr[12] = flags;
    hdr[13..17].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    hdr[17..21].copy_from_slice(&crc.to_le_bytes());

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(log_path)?;
    // Truncate any torn/corrupt tail beyond the last CRC32-verified offset before appending
    // so subsequent readers never stall on an incomplete crash frame.
    let cur_len = f.metadata()?.len();
    if cur_len > truncate_to {
        f.set_len(truncate_to)?;
    }
    let write_pos = f.seek(SeekFrom::Start(truncate_to.min(cur_len)))?;
    f.write_all(&hdr)?;
    f.write_all(&payload)?;
    if sync {
        f.sync_data()?;
    }
    Ok(write_pos + 21 + payload.len() as u64)
}

impl MvccCoordinator {
    fn mvcc_log_len(&self) -> u64 {
        std::fs::metadata(&self.mvcc_log_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Synchronizes both `meta` (commit version + WriteSet history) and `db` (catalog + WAL)
    /// from disk so commits, checkpoints, and vacuums from other OS processes are visible.
    /// Caller must hold at least a shared OS lock on `commit_lock_path` and write access to `db`.
    fn sync_from_disk_locked(&self, db: &mut Database, meta: &mut MvccMeta) -> Result<()> {
        let log_len = self.mvcc_log_len();
        let mut saw_vacuum = false;
        if log_len != meta.mvcc_log_offset {
            let start = if log_len < meta.mvcc_log_offset {
                meta.history.clear();
                0
            } else {
                meta.mvcc_log_offset
            };
            let (entries, new_off, vac) = read_mvcc_log_from(&self.mvcc_log_path, start)?;
            saw_vacuum = vac;
            for (ver, ws) in entries {
                if ver > meta.committed_version {
                    meta.committed_version = ver;
                    meta.history.push_back(CommittedTx {
                        version: ver,
                        write_set: ws,
                    });
                    while meta.history.len() > MAX_COMMIT_HISTORY {
                        if let Some(evicted) = meta.history.pop_front() {
                            meta.oldest_retained_version = evicted.version;
                        }
                    }
                }
            }
            meta.mvcc_log_offset = new_off;
        }
        db.sync_from_disk_mvcc(&mut meta.wal_offset, saw_vacuum)?;
        Ok(())
    }

    fn needs_disk_sync(&self, db: &Database, meta: &MvccMeta) -> bool {
        if self.mvcc_log_len() != meta.mvcc_log_offset {
            return true;
        }
        if db.wal_file_len() != meta.wal_offset {
            return true;
        }
        if let Ok(disk_gen) = StorageFile::read_latest_generation(&self.db_path) {
            if disk_gen != db.superblock_generation() {
                return true;
            }
        }
        false
    }
}

/// Multi-threaded and cross-process multi-writer MVCC database engine.
///
/// Multiple threads in one process can clone `SharedDatabase`, and multiple separate OS
/// processes can open the same `.sqdb` file with `SharedDatabase::open`:
/// - Readers never block writers and writers never block readers (Snapshot Isolation).
/// - Multiple concurrent writers across threads or processes execute on independent snapshots
///   and commit with row-level First-Committer-Wins conflict detection.
/// - Disjoint row or table writes commit concurrently without blocking each other.
#[derive(Clone)]
pub struct SharedDatabase {
    inner: Arc<MvccCoordinator>,
}

impl SharedDatabase {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    pub fn open_with_options<P: AsRef<Path>>(path: P, options: DbOptions) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let commit_lock_path = mvcc_sidecar_path(&db_path, "-mvcc.commit.lock");
        let snap_lock_path = mvcc_sidecar_path(&db_path, "-mvcc.snap.lock");
        let mvcc_log_path = mvcc_sidecar_path(&db_path, "-mvcc.log");

        // Serialize initial open/creation across processes so concurrent first-opens never race.
        let _open_guard = OsFileLock::acquire_blocking_path(commit_lock_path.clone(), true)?;
        let (db, wal_offset) = Database::open_for_mvcc(&db_path, options)?;

        let (entries, mvcc_log_offset, _) = read_mvcc_log_from(&mvcc_log_path, 0)?;
        let mut committed_version = 1u64;
        let mut oldest_retained_version = 1u64;
        let mut history = VecDeque::new();
        for (ver, ws) in entries {
            if ver > committed_version {
                committed_version = ver;
                history.push_back(CommittedTx {
                    version: ver,
                    write_set: ws,
                });
                while history.len() > MAX_COMMIT_HISTORY {
                    if let Some(evicted) = history.pop_front() {
                        oldest_retained_version = evicted.version;
                    }
                }
            }
        }
        drop(_open_guard);

        Ok(SharedDatabase {
            inner: Arc::new(MvccCoordinator {
                db_path,
                commit_lock_path,
                snap_lock_path,
                mvcc_log_path,
                committed: RwLock::new(db),
                commit_lock: Mutex::new(()),
                meta: Mutex::new(MvccMeta {
                    committed_version,
                    oldest_retained_version,
                    history,
                    active_snapshots: 0,
                    wal_offset,
                    mvcc_log_offset,
                }),
            }),
        })
    }

    /// Opens a new independent read-write MVCC session (safe to send across threads).
    pub fn connect(&self) -> MvccConnection {
        MvccConnection {
            coord: Arc::clone(&self.inner),
            read_only: false,
            active: None,
        }
    }

    /// Opens a new independent read-only MVCC snapshot session.
    pub fn connect_readonly(&self) -> MvccConnection {
        MvccConnection {
            coord: Arc::clone(&self.inner),
            read_only: true,
            active: None,
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().unwrap();
        let _os_guard =
            OsFileLock::acquire_blocking_path(self.inner.commit_lock_path.clone(), true)?;
        let mut db = self.inner.committed.write().unwrap();
        let mut meta = self.inner.meta.lock().unwrap();
        self.inner.sync_from_disk_locked(&mut db, &mut meta)?;
        db.checkpoint()?;
        meta.wal_offset = db.wal_file_len();
        Ok(())
    }

    pub fn vacuum(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().unwrap();
        let _os_guard =
            OsFileLock::acquire_blocking_path(self.inner.commit_lock_path.clone(), true)?;
        // Hold `committed.write()` BEFORE checking snapshots so no local snapshot can open
        // between the check and the file swap.
        let mut db = self.inner.committed.write().unwrap();
        let mut meta = self.inner.meta.lock().unwrap();
        if meta.active_snapshots > 0 {
            return Err(Error::Busy(
                "Cannot VACUUM while active MVCC transactions hold snapshots".into(),
            ));
        }
        // Check cross-process snapshots via non-blocking exclusive OS lock on `snap_lock_path`.
        let _snap_guard = match OsFileLock::try_acquire_path(
            self.inner.snap_lock_path.clone(),
            &self.inner.db_path,
            true,
        ) {
            Ok(g) => g,
            Err(Error::Busy(_)) => {
                return Err(Error::Busy(
                    "Cannot VACUUM while active MVCC transactions hold snapshots".into(),
                ));
            }
            Err(e) => return Err(e),
        };

        self.inner.sync_from_disk_locked(&mut db, &mut meta)?;
        db.vacuum()?;
        meta.wal_offset = db.wal_file_len();
        // Record a vacuum barrier in the cross-process MVCC log so peer processes reload the file.
        let new_ver = meta.committed_version + 1;
        let new_off = append_mvcc_log_record(
            &self.inner.mvcc_log_path,
            meta.mvcc_log_offset,
            new_ver,
            MVCC_FLAG_VACUUM,
            &WriteSet::default(),
            db.sync_wal_enabled(),
        )?;
        meta.committed_version = new_ver;
        meta.mvcc_log_offset = new_off;
        Ok(())
    }

    pub fn stats(&self) -> Result<DbStats> {
        let _os_shared =
            OsFileLock::acquire_blocking_path(self.inner.commit_lock_path.clone(), false)?;
        let need_sync = {
            let db = self.inner.committed.read().unwrap();
            let meta = self.inner.meta.lock().unwrap();
            self.inner.needs_disk_sync(&db, &meta)
        };
        if need_sync {
            let mut db = self.inner.committed.write().unwrap();
            let mut meta = self.inner.meta.lock().unwrap();
            self.inner.sync_from_disk_locked(&mut db, &mut meta)?;
        }
        let db = self.inner.committed.read().unwrap();
        db.stats()
    }

    pub fn committed_version(&self) -> u64 {
        if let Ok(_os_shared) =
            OsFileLock::acquire_blocking_path(self.inner.commit_lock_path.clone(), false)
        {
            let need_sync = {
                let db = self.inner.committed.read().unwrap();
                let meta = self.inner.meta.lock().unwrap();
                self.inner.needs_disk_sync(&db, &meta)
            };
            if need_sync {
                let mut db = self.inner.committed.write().unwrap();
                let mut meta = self.inner.meta.lock().unwrap();
                let _ = self.inner.sync_from_disk_locked(&mut db, &mut meta);
            }
        }
        self.inner.meta.lock().unwrap().committed_version
    }

    pub fn format_version(&self) -> u32 {
        self.inner.committed.read().unwrap().format_version()
    }
}

struct ActiveMvccTx {
    snapshot_version: u64,
    sandbox: Database,
    /// Shared OS lock held across processes while this snapshot is open (blocks cross-process VACUUM).
    _snap_lock: OsFileLock,
    /// Tracks whether the user called explicit `BEGIN` vs implicit savepoint transaction.
    explicit_begin: bool,
}

/// Independent MVCC client connection (equivalent to a PostgreSQL backend session).
/// Owns its own transaction snapshot, local write buffer, and nested savepoint stack.
pub struct MvccConnection {
    coord: Arc<MvccCoordinator>,
    read_only: bool,
    active: Option<ActiveMvccTx>,
}

impl MvccConnection {
    pub fn in_transaction(&self) -> bool {
        self.active.is_some()
    }

    pub fn snapshot_version(&self) -> Option<u64> {
        self.active.as_ref().map(|t| t.snapshot_version)
    }

    fn open_snapshot_sandbox(&self, read_only: bool) -> Result<(u64, Database, OsFileLock)> {
        // Lock order: commit_lock (shared) -> snap_lock (shared), matching VACUUM's commit_lock -> snap_lock.
        let _os_shared =
            OsFileLock::acquire_blocking_path(self.coord.commit_lock_path.clone(), false)?;

        let need_sync = {
            let db = self.coord.committed.read().unwrap();
            let meta = self.coord.meta.lock().unwrap();
            self.coord.needs_disk_sync(&db, &meta)
        };
        if need_sync {
            let mut db = self.coord.committed.write().unwrap();
            let mut meta = self.coord.meta.lock().unwrap();
            self.coord.sync_from_disk_locked(&mut db, &mut meta)?;
        }

        let db_guard = self.coord.committed.read().unwrap();
        let snap_lock =
            OsFileLock::acquire_blocking_path(self.coord.snap_lock_path.clone(), false)?;
        let (path, catalog, hot, opts, fk_enabled) = db_guard.snapshot_for_mvcc();
        let sandbox =
            Database::from_mvcc_snapshot(path, catalog, hot, opts, fk_enabled, read_only)?;
        let ver = {
            let mut m = self.coord.meta.lock().unwrap();
            m.active_snapshots += 1;
            m.committed_version
        };
        drop(db_guard);
        drop(_os_shared);
        Ok((ver, sandbox, snap_lock))
    }

    fn release_snapshot_ref(&self) {
        let mut m = self.coord.meta.lock().unwrap();
        m.active_snapshots = m.active_snapshots.saturating_sub(1);
    }

    pub fn begin(&mut self) -> Result<()> {
        if self.active.is_some() {
            return Err(Error::Sql("Transaction already active".into()));
        }
        let (ver, mut sandbox, snap_lock) = self.open_snapshot_sandbox(self.read_only)?;
        if !self.read_only {
            sandbox.begin()?;
        }
        self.active = Some(ActiveMvccTx {
            snapshot_version: ver,
            sandbox,
            _snap_lock: snap_lock,
            explicit_begin: true,
        });
        Ok(())
    }

    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::Sql("Database is opened read-only".into()));
        }
        if self.active.is_none() {
            let (ver, mut sandbox, snap_lock) = self.open_snapshot_sandbox(false)?;
            sandbox.begin()?;
            self.active = Some(ActiveMvccTx {
                snapshot_version: ver,
                sandbox,
                _snap_lock: snap_lock,
                explicit_begin: false,
            });
        }
        self.active.as_mut().unwrap().sandbox.savepoint(name)
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let tx = self
            .active
            .as_mut()
            .ok_or_else(|| Error::Sql("No active transaction or savepoint".into()))?;
        tx.sandbox.release_savepoint(name)?;
        if !tx.explicit_begin && !tx.sandbox.has_savepoints() {
            self.commit()?;
        }
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let tx = self
            .active
            .as_mut()
            .ok_or_else(|| Error::Sql("No active transaction or savepoint".into()))?;
        tx.sandbox.rollback_to_savepoint(name)
    }

    pub fn rollback(&mut self) -> Result<()> {
        let tx = self
            .active
            .take()
            .ok_or_else(|| Error::Sql("No active transaction".into()))?;
        drop(tx);
        self.release_snapshot_ref();
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let mut tx = self
            .active
            .take()
            .ok_or_else(|| Error::Sql("No active transaction".into()))?;
        let res = self.commit_active_tx(&mut tx);
        drop(tx);
        self.release_snapshot_ref();
        res
    }

    fn commit_active_tx(&self, tx: &mut ActiveMvccTx) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let ops = tx.sandbox.take_tx_wal_ops();
        if ops.is_empty() {
            return Ok(());
        }
        let write_set = WriteSet::from_wal_ops(&ops);

        // Short critical section across threads AND OS processes:
        // sync disk state -> validate conflicts -> apply atomically + append WAL -> append MVCC log.
        let _commit_guard = self.coord.commit_lock.lock().unwrap();
        let _os_commit_guard =
            OsFileLock::acquire_blocking_path(self.coord.commit_lock_path.clone(), true)?;

        let mut db = self.coord.committed.write().unwrap();
        let mut meta = self.coord.meta.lock().unwrap();
        self.coord.sync_from_disk_locked(&mut db, &mut meta)?;

        if tx.snapshot_version < meta.oldest_retained_version {
            return Err(Error::Busy(
                "MVCC serialization conflict: snapshot too old to validate".into(),
            ));
        }
        for committed in &meta.history {
            if committed.version > tx.snapshot_version {
                if let Some(reason) = write_set.conflicts_with(&committed.write_set) {
                    return Err(Error::Busy(format!(
                        "MVCC serialization conflict (snapshot v{} vs committed v{}): {}",
                        tx.snapshot_version, committed.version, reason
                    )));
                }
            }
        }

        let applied_ops = db.apply_mvcc_commit_ops(ops)?;
        let final_write_set = WriteSet::from_wal_ops(&applied_ops);
        let new_ver = meta.committed_version + 1;
        let new_log_off = append_mvcc_log_record(
            &self.coord.mvcc_log_path,
            meta.mvcc_log_offset,
            new_ver,
            0,
            &final_write_set,
            db.sync_wal_enabled(),
        )?;

        meta.committed_version = new_ver;
        meta.wal_offset = db.wal_file_len();
        meta.mvcc_log_offset = new_log_off;
        meta.history.push_back(CommittedTx {
            version: new_ver,
            write_set: final_write_set,
        });
        while meta.history.len() > MAX_COMMIT_HISTORY {
            if let Some(evicted) = meta.history.pop_front() {
                meta.oldest_retained_version = evicted.version;
            }
        }
        Ok(())
    }

    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let stmts = parse_sql(sql)?;
        self.execute_parsed(stmts)
    }

    pub fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<Vec<QueryResult>> {
        let stmts = parse_sql_with_params(sql, params)?;
        self.execute_parsed(stmts)
    }

    fn execute_parsed(&mut self, stmts: Vec<Statement>) -> Result<Vec<QueryResult>> {
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            match stmt {
                Statement::Begin => {
                    self.begin()?;
                    results.push(QueryResult::Message("Transaction started.".into()));
                }
                Statement::Commit => {
                    self.commit()?;
                    results.push(QueryResult::Message("Transaction committed.".into()));
                }
                Statement::Rollback => {
                    self.rollback()?;
                    results.push(QueryResult::Message("Transaction rolled back.".into()));
                }
                Statement::Savepoint { name } => {
                    self.savepoint(&name)?;
                    results.push(QueryResult::Message(format!(
                        "Savepoint '{}' created.",
                        name
                    )));
                }
                Statement::ReleaseSavepoint { name } => {
                    self.release_savepoint(&name)?;
                    results.push(QueryResult::Message(format!(
                        "Savepoint '{}' released.",
                        name
                    )));
                }
                Statement::RollbackToSavepoint { name } => {
                    self.rollback_to_savepoint(&name)?;
                    results.push(QueryResult::Message(format!(
                        "Rolled back to savepoint '{}'.",
                        name
                    )));
                }
                Statement::Checkpoint => {
                    if self.active.is_some() {
                        return Err(Error::Sql(
                            "Cannot checkpoint while a transaction is active".into(),
                        ));
                    }
                    let _g = self.coord.commit_lock.lock().unwrap();
                    let _os_g = OsFileLock::acquire_blocking_path(
                        self.coord.commit_lock_path.clone(),
                        true,
                    )?;
                    let mut db = self.coord.committed.write().unwrap();
                    let mut meta = self.coord.meta.lock().unwrap();
                    self.coord.sync_from_disk_locked(&mut db, &mut meta)?;
                    db.checkpoint()?;
                    meta.wal_offset = db.wal_file_len();
                    results.push(QueryResult::Message("Checkpoint complete.".into()));
                }
                Statement::Vacuum { table_sort } => {
                    if self.active.is_some() {
                        return Err(Error::Sql(
                            "Cannot VACUUM while a transaction is active".into(),
                        ));
                    }
                    let _g = self.coord.commit_lock.lock().unwrap();
                    let _os_g = OsFileLock::acquire_blocking_path(
                        self.coord.commit_lock_path.clone(),
                        true,
                    )?;
                    let mut db = self.coord.committed.write().unwrap();
                    let mut meta = self.coord.meta.lock().unwrap();
                    if meta.active_snapshots > 0 {
                        return Err(Error::Busy(
                            "Cannot VACUUM while active MVCC transactions hold snapshots".into(),
                        ));
                    }
                    let _snap_guard = match OsFileLock::try_acquire_path(
                        self.coord.snap_lock_path.clone(),
                        &self.coord.db_path,
                        true,
                    ) {
                        Ok(g) => g,
                        Err(Error::Busy(_)) => {
                            return Err(Error::Busy(
                                "Cannot VACUUM while active MVCC transactions hold snapshots"
                                    .into(),
                            ));
                        }
                        Err(e) => return Err(e),
                    };
                    self.coord.sync_from_disk_locked(&mut db, &mut meta)?;
                    if let Some((tbl, cols)) = table_sort {
                        db.set_cluster_by(&tbl, cols)?;
                    }
                    db.vacuum()?;
                    meta.wal_offset = db.wal_file_len();
                    let new_ver = meta.committed_version + 1;
                    let new_off = append_mvcc_log_record(
                        &self.coord.mvcc_log_path,
                        meta.mvcc_log_offset,
                        new_ver,
                        MVCC_FLAG_VACUUM,
                        &WriteSet::default(),
                        db.sync_wal_enabled(),
                    )?;
                    meta.committed_version = new_ver;
                    meta.mvcc_log_offset = new_off;
                    results.push(QueryResult::Message("Database vacuumed.".into()));
                }
                Statement::PragmaStats => {
                    let _os_shared = OsFileLock::acquire_blocking_path(
                        self.coord.commit_lock_path.clone(),
                        false,
                    )?;
                    let need_sync = {
                        let db = self.coord.committed.read().unwrap();
                        let meta = self.coord.meta.lock().unwrap();
                        self.coord.needs_disk_sync(&db, &meta)
                    };
                    if need_sync {
                        let mut db = self.coord.committed.write().unwrap();
                        let mut meta = self.coord.meta.lock().unwrap();
                        self.coord.sync_from_disk_locked(&mut db, &mut meta)?;
                    }
                    let stats = self.coord.committed.read().unwrap().stats()?;
                    results.push(QueryResult::Message(stats.format_report()));
                }
                other => {
                    if let Some(ref mut tx) = self.active {
                        results.push(execute_statement(&mut tx.sandbox, other)?);
                    } else {
                        // Auto-commit single statement on a fresh snapshot (readers never block writers!)
                        let is_read_only_stmt = matches!(
                            other,
                            Statement::Select(_) | Statement::Explain(_) | Statement::PragmaGet(_)
                        );
                        if self.read_only && !is_read_only_stmt {
                            return Err(Error::Sql("Database is opened read-only".into()));
                        }
                        let (ver, mut sandbox, snap_lock) =
                            self.open_snapshot_sandbox(is_read_only_stmt || self.read_only)?;
                        if !is_read_only_stmt {
                            sandbox.begin()?;
                        }
                        let mut auto_tx = ActiveMvccTx {
                            snapshot_version: ver,
                            sandbox,
                            _snap_lock: snap_lock,
                            explicit_begin: true,
                        };
                        let exec_res = execute_statement(&mut auto_tx.sandbox, other);
                        let res = match exec_res {
                            Ok(qr) => {
                                if !is_read_only_stmt {
                                    if let Err(e) = self.commit_active_tx(&mut auto_tx) {
                                        drop(auto_tx);
                                        self.release_snapshot_ref();
                                        return Err(e);
                                    }
                                }
                                Ok(qr)
                            }
                            Err(e) => Err(e),
                        };
                        drop(auto_tx);
                        self.release_snapshot_ref();
                        results.push(res?);
                    }
                }
            }
        }
        Ok(results)
    }
}

impl Drop for MvccConnection {
    fn drop(&mut self) {
        if let Some(tx) = self.active.take() {
            drop(tx);
            self.release_snapshot_ref();
        }
    }
}

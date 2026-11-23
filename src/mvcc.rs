use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use crate::sql::{execute_statement, parse_sql, parse_sql_with_params, Statement};
use crate::types::{Error, Result, Value};
use crate::wal::WalOp;
use crate::{Database, DbOptions, DbStats, QueryResult};

const MAX_COMMIT_HISTORY: usize = 4096;

#[derive(Debug, Clone)]
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
        let mut ws = WriteSet {
            modified_rowids: HashMap::new(),
            inserted_rowids: HashMap::new(),
            ddl_tables: HashSet::new(),
            touched_tables: HashSet::new(),
        };
        for op in ops {
            match op {
                WalOp::CreateTable(s) | WalOp::CreateTableV5(s) => {
                    let k = s.name.to_ascii_lowercase();
                    ws.ddl_tables.insert(k.clone());
                    ws.touched_tables.insert(k);
                }
                WalOp::DropTable(name)
                | WalOp::SetClusterBy { table_name: name, .. }
                | WalOp::AddColumn { table_name: name, .. }
                | WalOp::AddColumnV5 { table_name: name, .. }
                | WalOp::CreateIndex { table_name: name, .. }
                | WalOp::CreateIndexV5 { table_name: name, .. } => {
                    let k = name.to_ascii_lowercase();
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
}

struct MvccCoordinator {
    /// Canonical committed database state (owns OS file lock and WAL).
    committed: RwLock<Database>,
    /// Short-lived commit serialization lock (held only during conflict check + WAL append).
    commit_lock: Mutex<()>,
    meta: Mutex<MvccMeta>,
}

/// PostgreSQL-style multi-threaded MVCC database engine.
///
/// Multiple threads can clone `SharedDatabase` and open independent `MvccConnection` handles:
/// - Readers never block writers and writers never block readers (Snapshot Isolation).
/// - Multiple concurrent writers execute on independent snapshots and commit with row-level
///   First-Committer-Wins conflict detection.
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
        let db = Database::open_with_options(path, options)?;
        Ok(SharedDatabase {
            inner: Arc::new(MvccCoordinator {
                committed: RwLock::new(db),
                commit_lock: Mutex::new(()),
                meta: Mutex::new(MvccMeta {
                    committed_version: 1,
                    oldest_retained_version: 1,
                    history: VecDeque::new(),
                    active_snapshots: 0,
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
        let mut db = self.inner.committed.write().unwrap();
        db.checkpoint()
    }

    pub fn vacuum(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().unwrap();
        // Hold `committed.write()` BEFORE checking `active_snapshots` so no new snapshot can
        // open between the check and the file swap.
        let mut db = self.inner.committed.write().unwrap();
        {
            let meta = self.inner.meta.lock().unwrap();
            if meta.active_snapshots > 0 {
                return Err(Error::Busy(
                    "Cannot VACUUM while active MVCC transactions hold snapshots".into(),
                ));
            }
        }
        db.vacuum()
    }

    pub fn stats(&self) -> Result<DbStats> {
        let db = self.inner.committed.read().unwrap();
        db.stats()
    }

    pub fn committed_version(&self) -> u64 {
        self.inner.meta.lock().unwrap().committed_version
    }

    pub fn format_version(&self) -> u32 {
        self.inner.committed.read().unwrap().format_version()
    }
}

struct ActiveMvccTx {
    snapshot_version: u64,
    sandbox: Database,
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

    fn open_snapshot_sandbox(&self, read_only: bool) -> Result<(u64, Database)> {
        let db_guard = self.coord.committed.read().unwrap();
        let (path, catalog, hot, opts, fk_enabled) = db_guard.snapshot_for_mvcc();
        // Open the sandbox's read-only file handle while still holding `committed.read()` so a
        // concurrent VACUUM cannot replace the underlying file mid-open.
        let sandbox =
            Database::from_mvcc_snapshot(path, catalog, hot, opts, fk_enabled, read_only)?;
        let ver = {
            let mut m = self.coord.meta.lock().unwrap();
            m.active_snapshots += 1;
            m.committed_version
        };
        drop(db_guard);
        Ok((ver, sandbox))
    }

    fn release_snapshot_ref(&self) {
        let mut m = self.coord.meta.lock().unwrap();
        m.active_snapshots = m.active_snapshots.saturating_sub(1);
    }

    pub fn begin(&mut self) -> Result<()> {
        if self.active.is_some() {
            return Err(Error::Sql("Transaction already active".into()));
        }
        let (ver, mut sandbox) = self.open_snapshot_sandbox(self.read_only)?;
        if !self.read_only {
            sandbox.begin()?;
        }
        self.active = Some(ActiveMvccTx {
            snapshot_version: ver,
            sandbox,
            explicit_begin: true,
        });
        Ok(())
    }

    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::Sql("Database is opened read-only".into()));
        }
        if self.active.is_none() {
            let (ver, mut sandbox) = self.open_snapshot_sandbox(false)?;
            sandbox.begin()?;
            self.active = Some(ActiveMvccTx {
                snapshot_version: ver,
                sandbox,
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

        // Short critical section: validate conflicts + apply atomically + append WAL
        let _commit_guard = self.coord.commit_lock.lock().unwrap();
        {
            let meta = self.coord.meta.lock().unwrap();
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
        }

        let final_write_set = {
            let mut db = self.coord.committed.write().unwrap();
            let applied_ops = db.apply_mvcc_commit_ops(ops)?;
            WriteSet::from_wal_ops(&applied_ops)
        };

        let mut meta = self.coord.meta.lock().unwrap();
        meta.committed_version += 1;
        let new_ver = meta.committed_version;
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
        self.execute_statements(stmts)
    }

    pub fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<Vec<QueryResult>> {
        let stmts = parse_sql_with_params(sql, params)?;
        self.execute_statements(stmts)
    }

    fn execute_statements(&mut self, stmts: Vec<Statement>) -> Result<Vec<QueryResult>> {
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
                    results.push(QueryResult::Message(format!("Savepoint '{}' created.", name)));
                }
                Statement::ReleaseSavepoint { name } => {
                    self.release_savepoint(&name)?;
                    results.push(QueryResult::Message(format!("Savepoint '{}' released.", name)));
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
                            "Cannot checkpoint while an MVCC transaction is active".into(),
                        ));
                    }
                    let _g = self.coord.commit_lock.lock().unwrap();
                    self.coord.committed.write().unwrap().checkpoint()?;
                    results.push(QueryResult::Message("Checkpoint complete.".into()));
                }
                Statement::Vacuum { table_sort } => {
                    if self.active.is_some() {
                        return Err(Error::Sql(
                            "Cannot VACUUM while an MVCC transaction is active".into(),
                        ));
                    }
                    let _g = self.coord.commit_lock.lock().unwrap();
                    let mut db = self.coord.committed.write().unwrap();
                    {
                        let m = self.coord.meta.lock().unwrap();
                        if m.active_snapshots > 0 {
                            return Err(Error::Busy(
                                "Cannot VACUUM while active MVCC transactions hold snapshots".into(),
                            ));
                        }
                    }
                    if let Some((table, cols)) = table_sort {
                        db.set_cluster_by(&table, cols)?;
                    }
                    db.vacuum()?;
                    results.push(QueryResult::Message("VACUUM complete.".into()));
                }
                other => {
                    if let Some(ref mut tx) = self.active {
                        results.push(execute_statement(&mut tx.sandbox, other)?);
                    } else if is_readonly_stmt(&other) {
                        // Lock-free snapshot read outside an explicit transaction!
                        let (_, mut sandbox) = self.open_snapshot_sandbox(true)?;
                        let res = execute_statement(&mut sandbox, other);
                        drop(sandbox);
                        self.release_snapshot_ref();
                        results.push(res?);
                    } else {
                        // Auto-commit write transaction with full MVCC validation!
                        if self.read_only {
                            return Err(Error::Sql("Database is opened read-only".into()));
                        }
                        self.begin()?;
                        let res = execute_statement(&mut self.active.as_mut().unwrap().sandbox, other);
                        match res {
                            Ok(qr) => {
                                self.commit()?;
                                results.push(qr);
                            }
                            Err(e) => {
                                let _ = self.rollback();
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }
        Ok(results)
    }
}

impl Drop for MvccConnection {
    fn drop(&mut self) {
        if self.active.take().is_some() {
            self.release_snapshot_ref();
        }
    }
}

fn is_readonly_stmt(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Select(_) | Statement::Explain(_) | Statement::PragmaStats | Statement::PragmaGet(_)
    )
}

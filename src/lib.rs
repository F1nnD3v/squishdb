pub mod cache;
pub mod encoding;
pub mod ffi;
pub mod index;
pub mod lock;
pub mod mvcc;
pub mod parallel;
pub mod pax;
pub mod rpc;
pub mod sql;
pub mod sqlite_import;
pub mod storage;
pub mod types;
pub mod vector;
pub mod wal;

pub use mvcc::{MvccConnection, SharedDatabase};
pub use sqlite_import::{migrate_sqlite, MigrateOptions, MigrationReport};

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::cache::PageCache;
use crate::index::{
    cmp_key_tuple, hash_key_tuple, hash_value, BTreeLeafEntry, BTreeScanSpec, IndexBlockCache,
    IndexEntry, IndexKind, RangeBound, INDEX_BLOCK_ENTRIES,
};
use crate::lock::{atomic_replace_file, OsFileLock};
use crate::parallel::ParallelPool;
use crate::pax::{encode_pax_page, CompressionProfile, DEFAULT_PAGE_ROWS};
use crate::sql::{
    eval_bool, eval_bool_fast, eval_expr, execute_statement, extract_column_predicates,
    page_might_match, parse_expr_str, parse_sql, parse_sql_with_params, ColumnPredicates,
    EvalSchema, Expr,
};
use crate::storage::{
    read_pax_batch_from_file, read_pax_page_from_file, Catalog, StorageFile, TableEntry,
    FORMAT_VERSION,
};
use crate::types::{
    CheckConstraint, ColumnDef, Error, FkAction, ForeignKeyDef, Result, Row, TableSchema, Value,
};
use crate::vector::{ColumnBatch, ColumnVector, VecAggKind, VecFilter, VectorGroupTable};
use crate::wal::{WalFile, WalOp};

#[derive(Debug, Clone)]
pub struct DbOptions {
    pub page_rows: usize,
    pub compression: CompressionProfile,
    /// When true, fsyncs WAL on every commit (SQLite FULL synchronous mode).
    pub sync_wal: bool,
    /// Automatically checkpoint when total buffered hot rows across tables reaches this limit.
    pub auto_checkpoint_rows: usize,
    /// Number of decoded PAX pages kept in the in-memory LRU cache.
    pub cache_pages: usize,
    /// Number of decoded on-disk index leaf blocks kept in the LRU index cache.
    pub index_cache_blocks: usize,
    /// Parallel worker threads (0 = auto-detect up to 8 cores, 1 = single-threaded).
    pub parallel_workers: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            page_rows: DEFAULT_PAGE_ROWS,
            compression: CompressionProfile::Balanced,
            sync_wal: false,
            auto_checkpoint_rows: DEFAULT_PAGE_ROWS * 4,
            cache_pages: 128,
            index_cache_blocks: 64,
            parallel_workers: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Affected(usize),
    Message(String),
}

#[derive(Debug, Clone, Default)]
pub struct ScanMetrics {
    pub total_pages: usize,
    pub pruned_by_index: usize,
    pub pruned_by_zonemap: usize,
    pub pruned_by_bloom: usize,
    pub pages_scanned: usize,
    pub cache_hits: usize,
    pub index_used: Option<String>,
    pub index_blocks_read: usize,
    pub parallel_workers: usize,
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub name: String,
    pub live_rows: u64,
    pub deleted_slots: u64,
    pub hot_buffer_rows: u64,
    pub page_count: usize,
    pub raw_pax_bytes: u64,
    pub stored_pax_bytes: u64,
    pub cluster_by: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DbStats {
    pub db_file_bytes: u64,
    pub wal_file_bytes: u64,
    pub catalog_stored_bytes: u64,
    pub catalog_raw_bytes: u64,
    pub generation: u64,
    pub index_count: usize,
    pub index_disk_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
    pub read_only: bool,
    pub tables: Vec<TableStats>,
}

impl DbStats {
    pub fn format_report(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "SquishDB Stats (gen={}, mode={}):\n  DB file:  {} bytes\n  WAL file: {} bytes\n  Catalog:  {} bytes (raw {} bytes)\n  Indexes:  {} ({} bytes paged on disk | idx cache: {} hits / {} misses)\n  Page LRU: {} hits / {} misses\n",
            self.generation,
            if self.read_only { "RO/shared" } else { "RW/exclusive" },
            self.db_file_bytes,
            self.wal_file_bytes,
            self.catalog_stored_bytes,
            self.catalog_raw_bytes,
            self.index_count,
            self.index_disk_bytes,
            self.index_cache_hits,
            self.index_cache_misses,
            self.cache_hits,
            self.cache_misses
        ));
        for t in &self.tables {
            let ratio = if t.stored_pax_bytes > 0 {
                (t.raw_pax_bytes as f64) / (t.stored_pax_bytes as f64)
            } else {
                1.0
            };
            let cluster = if t.cluster_by.is_empty() {
                String::new()
            } else {
                format!(", cluster_by=({})", t.cluster_by.join(","))
            };
            s.push_str(&format!(
                "  Table '{}': {} live rows ({} hot, {} deleted), {} pages, PAX raw={} B -> stored={} B ({:.2}x{})\n",
                t.name,
                t.live_rows,
                t.hot_buffer_rows,
                t.deleted_slots,
                t.page_count,
                t.raw_pax_bytes,
                t.stored_pax_bytes,
                ratio,
                cluster
            ));
        }
        s
    }

    pub fn to_json(&self) -> String {
        fn esc(s: &str) -> String {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        }
        let mut s = format!(
            "{{\"db_file_bytes\":{},\"wal_file_bytes\":{},\"catalog_stored_bytes\":{},\"catalog_raw_bytes\":{},\"generation\":{},\"index_count\":{},\"index_disk_bytes\":{},\"cache_hits\":{},\"cache_misses\":{},\"index_cache_hits\":{},\"index_cache_misses\":{},\"read_only\":{},\"tables\":[",
            self.db_file_bytes,
            self.wal_file_bytes,
            self.catalog_stored_bytes,
            self.catalog_raw_bytes,
            self.generation,
            self.index_count,
            self.index_disk_bytes,
            self.cache_hits,
            self.cache_misses,
            self.index_cache_hits,
            self.index_cache_misses,
            self.read_only
        );
        for (i, t) in self.tables.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            let ratio = if t.stored_pax_bytes > 0 {
                (t.raw_pax_bytes as f64) / (t.stored_pax_bytes as f64)
            } else {
                1.0
            };
            s.push_str(&format!(
                "{{\"name\":{},\"live_rows\":{},\"deleted_slots\":{},\"hot_buffer_rows\":{},\"page_count\":{},\"raw_pax_bytes\":{},\"stored_pax_bytes\":{},\"compression_ratio\":{:.4},\"cluster_by\":[",
                esc(&t.name),
                t.live_rows,
                t.deleted_slots,
                t.hot_buffer_rows,
                t.page_count,
                t.raw_pax_bytes,
                t.stored_pax_bytes,
                ratio
            ));
            for (j, cb) in t.cluster_by.iter().enumerate() {
                if j > 0 {
                    s.push(',');
                }
                s.push_str(&esc(cb));
            }
            s.push_str("]}");
        }
        s.push_str("]}");
        s
    }
}

struct SavepointState {
    name: String,
    snapshot_catalog: Catalog,
    snapshot_hot: HashMap<String, Vec<Row>>,
    wal_ops_len: usize,
}

struct TxState {
    snapshot_catalog: Catalog,
    snapshot_hot: HashMap<String, Vec<Row>>,
    pending_wal_ops: Vec<WalOp>,
    savepoints: Vec<SavepointState>,
    implicit_savepoint_tx: bool,
}

pub struct Database {
    path: PathBuf,
    _lock: Option<OsFileLock>,
    read_only: bool,
    mvcc_sandbox: bool,
    storage: StorageFile,
    wal: WalFile,
    catalog: Catalog,
    hot_buffers: HashMap<String, Vec<Row>>,
    cache: PageCache,
    index_cache: IndexBlockCache,
    options: DbOptions,
    pool: ParallelPool,
    foreign_keys_enabled: bool,
    active_tx: Option<TxState>,
    dirty: bool,
}

pub fn sort_rows_by_cluster(schema: &TableSchema, rows: &mut [Row]) {
    if schema.cluster_by.is_empty() {
        rows.sort_by_key(|r| r.rowid);
        return;
    }
    let idxs: Vec<usize> = schema
        .cluster_by
        .iter()
        .filter_map(|c| schema.col_index(c))
        .collect();
    if idxs.is_empty() {
        rows.sort_by_key(|r| r.rowid);
        return;
    }
    rows.sort_by(|a, b| {
        for &ci in &idxs {
            let o = a.values[ci].sql_cmp(&b.values[ci]);
            if o != Ordering::Equal {
                return o;
            }
        }
        a.rowid.cmp(&b.rowid)
    });
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    /// Opens a read-only snapshot connection holding an OS-level **shared** lock.
    /// Multiple processes/handles can open read-only concurrently without blocking each other.
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let lock = OsFileLock::try_acquire(&path_buf, false)?;
        let (storage, catalog) = StorageFile::open_readonly(&path_buf)?;
        let mut wal = WalFile::open_readonly(&path_buf)?;
        let frames = wal.read_frames(storage.superblock.generation)?;
        let options = DbOptions::default();
        let pool = ParallelPool::new(options.parallel_workers);

        let mut db = Database {
            path: path_buf,
            _lock: Some(lock),
            read_only: true,
            mvcc_sandbox: false,
            storage,
            wal,
            catalog,
            hot_buffers: HashMap::new(),
            cache: PageCache::new(options.cache_pages),
            index_cache: IndexBlockCache::new(options.index_cache_blocks),
            options,
            pool,
            foreign_keys_enabled: true,
            active_tx: None,
            dirty: false,
        };

        if !frames.is_empty() {
            for frame in frames {
                for op in frame.ops {
                    db.apply_wal_op_in_memory(op)?;
                }
            }
        }
        Ok(db)
    }

    pub fn open_with_options<P: AsRef<Path>>(path: P, options: DbOptions) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let lock = OsFileLock::try_acquire(&path_buf, true)?;
        let (storage, catalog) = StorageFile::open(&path_buf)?;
        let mut wal = WalFile::open(&path_buf)?;
        let frames = wal.read_frames(storage.superblock.generation)?;
        let cache_cap = options.cache_pages;
        let idx_cap = options.index_cache_blocks;
        let pool = ParallelPool::new(options.parallel_workers);
        let migrated = storage.migrated_from_version.is_some();

        let mut db = Database {
            path: path_buf,
            _lock: Some(lock),
            read_only: false,
            mvcc_sandbox: false,
            storage,
            wal,
            catalog,
            hot_buffers: HashMap::new(),
            cache: PageCache::new(cache_cap),
            index_cache: IndexBlockCache::new(idx_cap),
            options,
            pool,
            foreign_keys_enabled: true,
            active_tx: None,
            dirty: migrated,
        };

        if !frames.is_empty() {
            for frame in frames {
                for op in frame.ops {
                    db.apply_wal_op_in_memory(op)?;
                }
            }
            db.dirty = true;
        }

        Ok(db)
    }

    /// Creates a lightweight MVCC snapshot copy (called by `SharedDatabase`).
    pub(crate) fn snapshot_for_mvcc(
        &self,
    ) -> (PathBuf, Catalog, HashMap<String, Vec<Row>>, DbOptions, bool) {
        (
            self.path.clone(),
            self.catalog.clone(),
            self.hot_buffers.clone(),
            self.options.clone(),
            self.foreign_keys_enabled,
        )
    }

    /// Opens an isolated MVCC sandbox against immutable on-disk PAX pages (no OS lock).
    pub(crate) fn from_mvcc_snapshot(
        path: PathBuf,
        catalog: Catalog,
        hot_buffers: HashMap<String, Vec<Row>>,
        options: DbOptions,
        foreign_keys_enabled: bool,
        read_only: bool,
    ) -> Result<Self> {
        let (storage, _) = StorageFile::open_readonly(&path)?;
        let wal = WalFile::open_readonly(&path)?;
        let cache_cap = options.cache_pages;
        let idx_cap = options.index_cache_blocks;
        // Keep per-connection sandbox single-threaded by default to avoid oversubscribing CPU
        // when many MVCC client threads run concurrently.
        let pool = ParallelPool::new(1);
        Ok(Database {
            path,
            _lock: None,
            read_only,
            mvcc_sandbox: true,
            storage,
            wal,
            catalog,
            hot_buffers,
            cache: PageCache::new(cache_cap),
            index_cache: IndexBlockCache::new(idx_cap),
            options,
            pool,
            foreign_keys_enabled,
            active_tx: None,
            dirty: false,
        })
    }

    pub(crate) fn take_tx_wal_ops(&mut self) -> Vec<WalOp> {
        self.active_tx
            .take()
            .map(|t| t.pending_wal_ops)
            .unwrap_or_default()
    }

    /// Atomically validates and applies a committing MVCC transaction's WAL ops onto the
    /// canonical committed database, returning the final applied ops (with any remapped auto-rowids).
    pub(crate) fn apply_mvcc_commit_ops(&mut self, ops: Vec<WalOp>) -> Result<Vec<WalOp>> {
        self.require_writable()?;
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let saved_catalog = self.catalog.clone();
        let saved_hot = self.hot_buffers.clone();
        let saved_next_table_id = self.storage.superblock.next_table_id;

        let res: Result<Vec<WalOp>> = (|| {
            let mut applied = Vec::with_capacity(ops.len());
            let mut deleted_parent_tables: Vec<(String, Vec<Row>)> = Vec::new();
            for op in ops {
                match op {
                    WalOp::CreateTable(schema) | WalOp::CreateTableV5(schema) => {
                        if self.find_table_idx(&schema.name).is_some() {
                            return Err(Error::Busy(format!(
                                "MVCC conflict: table '{}' already exists",
                                schema.name
                            )));
                        }
                        self.validate_table_constraints_ddl(&schema)?;
                        let mut s = schema;
                        s.id = self.storage.superblock.next_table_id;
                        self.storage.superblock.next_table_id += 1;
                        self.catalog.tables.push(TableEntry {
                            schema: s.clone(),
                            pages: Vec::new(),
                        });
                        applied.push(WalOp::CreateTableV5(s));
                    }
                    WalOp::DropTable(name) => {
                        self.check_drop_table_fk(&name)?;
                        let idx = self
                            .find_table_idx(&name)
                            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", name)))?;
                        let canonical = self.catalog.tables[idx].schema.name.clone();
                        let key = canonical.to_ascii_lowercase();
                        for p in &self.catalog.tables[idx].pages {
                            self.cache.invalidate(p.page_id);
                        }
                        self.catalog.tables.remove(idx);
                        self.hot_buffers.remove(&key);
                        self.catalog
                            .indexes
                            .retain(|ix| !ix.table_name.eq_ignore_ascii_case(&canonical));
                        applied.push(WalOp::DropTable(canonical));
                    }
                    WalOp::InsertRows {
                        table_name,
                        mut rows,
                    } => {
                        let idx = self.find_table_idx(&table_name).ok_or_else(|| {
                            Error::Schema(format!("No such table: '{}'", table_name))
                        })?;
                        let schema = self.catalog.tables[idx].schema.clone();
                        let pk_col = schema.integer_pk_col();
                        let mut next_rowid = schema.next_rowid;
                        // For rowid-only tables (no explicit INTEGER PRIMARY KEY), re-assign
                        // colliding internal rowids so concurrent inserts both succeed cleanly!
                        if pk_col.is_none() {
                            for r in &mut rows {
                                if r.rowid < next_rowid {
                                    r.rowid = next_rowid;
                                    next_rowid = next_rowid.saturating_add(1);
                                } else {
                                    next_rowid = r.rowid.saturating_add(1);
                                }
                            }
                        } else {
                            for r in &rows {
                                if r.rowid >= next_rowid {
                                    next_rowid = r.rowid.saturating_add(1);
                                }
                            }
                        }
                        self.check_row_constraints(&schema, &rows)?;
                        self.check_unique_constraints(&schema.name, &schema, &rows, None)?;
                        if self.foreign_keys_enabled {
                            self.check_foreign_keys_on_write(&schema, &rows)?;
                        }
                        self.catalog.tables[idx].schema.next_rowid = next_rowid;
                        let key = schema.name.to_ascii_lowercase();
                        self.hot_buffers
                            .entry(key)
                            .or_default()
                            .extend(rows.clone());
                        applied.push(WalOp::InsertRows {
                            table_name: schema.name,
                            rows,
                        });
                    }
                    WalOp::DeleteRowIds { table_name, rowids } => {
                        let idx = self.find_table_idx(&table_name).ok_or_else(|| {
                            Error::Schema(format!("No such table: '{}'", table_name))
                        })?;
                        let canonical = self.catalog.tables[idx].schema.name.clone();
                        let set: HashSet<i64> = rowids.iter().copied().collect();
                        let del_rows = if self.foreign_keys_enabled
                            && !self.find_referencing_fks(&canonical).is_empty()
                        {
                            self.fetch_rows_by_ids(&canonical, &set)?
                        } else {
                            Vec::new()
                        };
                        self.mark_rowids_deleted_in_memory(&canonical, &set)?;
                        if !del_rows.is_empty() {
                            deleted_parent_tables.push((canonical.clone(), del_rows));
                        }
                        applied.push(WalOp::DeleteRowIds {
                            table_name: canonical,
                            rowids,
                        });
                    }
                    WalOp::UpdateRows { table_name, rows } => {
                        let idx = self.find_table_idx(&table_name).ok_or_else(|| {
                            Error::Schema(format!("No such table: '{}'", table_name))
                        })?;
                        let schema = self.catalog.tables[idx].schema.clone();
                        let old_ids: HashSet<i64> = rows.iter().map(|r| r.rowid).collect();
                        self.check_row_constraints(&schema, &rows)?;
                        self.check_unique_constraints(
                            &schema.name,
                            &schema,
                            &rows,
                            Some(&old_ids),
                        )?;
                        if self.foreign_keys_enabled {
                            self.check_foreign_keys_on_write(&schema, &rows)?;
                        }
                        self.mark_rowids_deleted_in_memory(&schema.name, &old_ids)?;
                        let key = schema.name.to_ascii_lowercase();
                        self.hot_buffers
                            .entry(key)
                            .or_default()
                            .extend(rows.clone());
                        applied.push(WalOp::UpdateRows {
                            table_name: schema.name,
                            rows,
                        });
                    }
                    WalOp::CreateIndex {
                        name,
                        table_name,
                        column_name,
                    } => {
                        self.build_index_in_memory(
                            &name,
                            &table_name,
                            &[column_name.clone()],
                            &[false],
                            IndexKind::Hash,
                            false,
                        )?;
                        applied.push(WalOp::CreateIndex {
                            name,
                            table_name,
                            column_name,
                        });
                    }
                    WalOp::CreateIndexV5 {
                        name,
                        table_name,
                        columns,
                        desc,
                        kind,
                        unique,
                    } => {
                        self.build_index_in_memory(
                            &name,
                            &table_name,
                            &columns,
                            &desc,
                            kind,
                            unique,
                        )?;
                        applied.push(WalOp::CreateIndexV5 {
                            name,
                            table_name,
                            columns,
                            desc,
                            kind,
                            unique,
                        });
                    }
                    WalOp::DropIndex(name) => {
                        self.catalog
                            .indexes
                            .retain(|i| !i.name.eq_ignore_ascii_case(&name));
                        applied.push(WalOp::DropIndex(name));
                    }
                    WalOp::SetClusterBy {
                        table_name,
                        cluster_by,
                    } => {
                        if let Some(idx) = self.find_table_idx(&table_name) {
                            self.catalog.tables[idx].schema.cluster_by = cluster_by.clone();
                        }
                        applied.push(WalOp::SetClusterBy {
                            table_name,
                            cluster_by,
                        });
                    }
                    WalOp::AddColumn { table_name, column } => {
                        self.apply_wal_op_in_memory(WalOp::AddColumn {
                            table_name: table_name.clone(),
                            column: column.clone(),
                        })?;
                        applied.push(WalOp::AddColumn { table_name, column });
                    }
                    WalOp::AddColumnV5 {
                        table_name,
                        column,
                        check_constraints,
                        foreign_keys,
                    } => {
                        self.apply_wal_op_in_memory(WalOp::AddColumnV5 {
                            table_name: table_name.clone(),
                            column: column.clone(),
                            check_constraints: check_constraints.clone(),
                            foreign_keys: foreign_keys.clone(),
                        })?;
                        applied.push(WalOp::AddColumnV5 {
                            table_name,
                            column,
                            check_constraints,
                            foreign_keys,
                        });
                    }
                }
            }

            // Post-commit FK integrity check: verify no remaining child row (including one
            // inserted concurrently by another MVCC writer) still references a deleted parent key.
            if self.foreign_keys_enabled {
                for (parent_tbl, del_rows) in &deleted_parent_tables {
                    self.verify_no_referencing_children(parent_tbl, del_rows)?;
                }
            }

            self.wal.append_frame(
                self.storage.superblock.generation,
                &applied,
                self.options.sync_wal,
            )?;
            Ok(applied)
        })();

        match res {
            Ok(applied) => {
                self.dirty = true;
                self.maybe_auto_checkpoint()?;
                Ok(applied)
            }
            Err(e) => {
                self.catalog = saved_catalog;
                self.hot_buffers = saved_hot;
                self.storage.superblock.next_table_id = saved_next_table_id;
                self.cache.clear();
                Err(e)
            }
        }
    }

    pub fn format_version(&self) -> u32 {
        self.storage.superblock.version
    }

    pub fn migrated_from_version(&self) -> Option<u32> {
        self.storage.migrated_from_version
    }

    /// Explicitly persists a forward format migration (e.g. v4 `SQSHDB04` -> v5 `SQSHDB05`)
    /// atomically via the inactive A/B superblock slot. Returns `true` if upgraded.
    pub fn upgrade_format(&mut self) -> Result<bool> {
        self.require_writable()?;
        if self.storage.superblock.version < FORMAT_VERSION
            || self.storage.migrated_from_version.is_some()
        {
            self.checkpoint()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Writes a v4-format superblock+catalog snapshot for format forward-migration tests.
    #[doc(hidden)]
    pub fn write_v4_snapshot_for_test(&mut self) -> Result<()> {
        self.require_writable()?;
        self.checkpoint()?;
        let profile = self.options.compression;
        self.storage
            .commit_catalog_v4_for_test(&self.catalog, profile)?;
        self.wal.truncate()?;
        self.dirty = false;
        Ok(())
    }

    pub fn set_foreign_keys_enabled(&mut self, enabled: bool) {
        self.foreign_keys_enabled = enabled;
    }

    pub fn foreign_keys_enabled(&self) -> bool {
        self.foreign_keys_enabled
    }

    pub fn set_parallel_workers(&mut self, workers: usize) {
        self.options.parallel_workers = workers;
        self.pool = ParallelPool::new(workers);
    }

    pub fn parallel_workers(&self) -> usize {
        self.pool.worker_count()
    }

    fn require_writable(&self) -> Result<()> {
        if self.read_only {
            Err(Error::Sql("Database is opened read-only".into()))
        } else {
            Ok(())
        }
    }

    fn apply_wal_op_in_memory(&mut self, op: WalOp) -> Result<()> {
        match op {
            WalOp::CreateTable(schema) | WalOp::CreateTableV5(schema) => {
                if self.find_table_idx(&schema.name).is_none() {
                    if schema.id >= self.storage.superblock.next_table_id {
                        self.storage.superblock.next_table_id = schema.id + 1;
                    }
                    self.catalog.tables.push(TableEntry {
                        schema,
                        pages: Vec::new(),
                    });
                }
            }
            WalOp::DropTable(name) => {
                if let Some(idx) = self.find_table_idx(&name) {
                    let canonical = self.catalog.tables[idx].schema.name.clone();
                    let key = canonical.to_ascii_lowercase();
                    self.catalog.tables.remove(idx);
                    self.hot_buffers.remove(&key);
                    self.catalog
                        .indexes
                        .retain(|ix| !ix.table_name.eq_ignore_ascii_case(&canonical));
                }
            }
            WalOp::InsertRows { table_name, rows } => {
                if let Some(idx) = self.find_table_idx(&table_name) {
                    let key = self.catalog.tables[idx].schema.name.to_ascii_lowercase();
                    let mut next_id = self.catalog.tables[idx].schema.next_rowid;
                    for r in &rows {
                        if r.rowid >= next_id {
                            next_id = r.rowid.saturating_add(1);
                        }
                    }
                    self.catalog.tables[idx].schema.next_rowid = next_id;
                    self.hot_buffers.entry(key).or_default().extend(rows);
                }
            }
            WalOp::DeleteRowIds { table_name, rowids } => {
                let set: HashSet<i64> = rowids.into_iter().collect();
                self.mark_rowids_deleted_in_memory(&table_name, &set)?;
            }
            WalOp::UpdateRows { table_name, rows } => {
                let set: HashSet<i64> = rows.iter().map(|r| r.rowid).collect();
                self.mark_rowids_deleted_in_memory(&table_name, &set)?;
                if let Some(idx) = self.find_table_idx(&table_name) {
                    let key = self.catalog.tables[idx].schema.name.to_ascii_lowercase();
                    self.hot_buffers.entry(key).or_default().extend(rows);
                }
            }
            WalOp::CreateIndex {
                name,
                table_name,
                column_name,
            } => {
                if !self
                    .catalog
                    .indexes
                    .iter()
                    .any(|i| i.name.eq_ignore_ascii_case(&name))
                {
                    let _ = self.build_index_in_memory(
                        &name,
                        &table_name,
                        &[column_name],
                        &[false],
                        IndexKind::Hash,
                        false,
                    );
                }
            }
            WalOp::CreateIndexV5 {
                name,
                table_name,
                columns,
                desc,
                kind,
                unique,
            } => {
                if !self
                    .catalog
                    .indexes
                    .iter()
                    .any(|i| i.name.eq_ignore_ascii_case(&name))
                {
                    let _ = self
                        .build_index_in_memory(&name, &table_name, &columns, &desc, kind, unique);
                }
            }
            WalOp::DropIndex(name) => {
                self.catalog
                    .indexes
                    .retain(|i| !i.name.eq_ignore_ascii_case(&name));
            }
            WalOp::SetClusterBy {
                table_name,
                cluster_by,
            } => {
                if let Some(idx) = self.find_table_idx(&table_name) {
                    self.catalog.tables[idx].schema.cluster_by = cluster_by;
                }
            }
            WalOp::AddColumn { table_name, column } => {
                if let Some(idx) = self.find_table_idx(&table_name) {
                    if self.catalog.tables[idx]
                        .schema
                        .col_index(&column.name)
                        .is_none()
                    {
                        let fill = column.default_value.clone().unwrap_or(Value::Null);
                        let key = self.catalog.tables[idx].schema.name.to_ascii_lowercase();
                        self.catalog.tables[idx].schema.columns.push(column);
                        if let Some(hot) = self.hot_buffers.get_mut(&key) {
                            for r in hot.iter_mut() {
                                r.values.push(fill.clone());
                            }
                        }
                        self.cache.clear();
                    }
                }
            }
            WalOp::AddColumnV5 {
                table_name,
                column,
                check_constraints,
                foreign_keys,
            } => {
                if let Some(idx) = self.find_table_idx(&table_name) {
                    if self.catalog.tables[idx]
                        .schema
                        .col_index(&column.name)
                        .is_none()
                    {
                        let fill = column.default_value.clone().unwrap_or(Value::Null);
                        let key = self.catalog.tables[idx].schema.name.to_ascii_lowercase();
                        self.catalog.tables[idx].schema.columns.push(column);
                        self.catalog.tables[idx]
                            .schema
                            .check_constraints
                            .extend(check_constraints);
                        self.catalog.tables[idx]
                            .schema
                            .foreign_keys
                            .extend(foreign_keys);
                        if let Some(hot) = self.hot_buffers.get_mut(&key) {
                            for r in hot.iter_mut() {
                                r.values.push(fill.clone());
                            }
                        }
                        self.cache.clear();
                    }
                }
            }
        }
        Ok(())
    }

    fn find_table_idx(&self, name: &str) -> Option<usize> {
        self.catalog
            .tables
            .iter()
            .position(|t| t.schema.name.eq_ignore_ascii_case(name))
    }

    pub fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.find_table_idx(name)
            .map(|i| &self.catalog.tables[i].schema)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.catalog
            .tables
            .iter()
            .map(|t| t.schema.name.clone())
            .collect()
    }

    pub fn index_names(&self) -> Vec<String> {
        self.catalog
            .indexes
            .iter()
            .map(|i| i.name.clone())
            .collect()
    }

    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let stmts = parse_sql(sql)?;
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            results.push(execute_statement(self, stmt)?);
        }
        Ok(results)
    }

    pub fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<Vec<QueryResult>> {
        let stmts = parse_sql_with_params(sql, params)?;
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            results.push(execute_statement(self, stmt)?);
        }
        Ok(results)
    }

    // ========================================================================
    // Transactions & Nested SAVEPOINTs
    // ========================================================================

    pub fn begin(&mut self) -> Result<()> {
        self.require_writable()?;
        if self.active_tx.is_some() {
            return Err(Error::Sql("Transaction already active".into()));
        }
        self.active_tx = Some(TxState {
            snapshot_catalog: self.catalog.clone(),
            snapshot_hot: self.hot_buffers.clone(),
            pending_wal_ops: Vec::new(),
            savepoints: Vec::new(),
            implicit_savepoint_tx: false,
        });
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let tx = self
            .active_tx
            .take()
            .ok_or_else(|| Error::Sql("No active transaction".into()))?;
        if !tx.pending_wal_ops.is_empty() && !self.mvcc_sandbox {
            self.wal.append_frame(
                self.storage.superblock.generation,
                &tx.pending_wal_ops,
                self.options.sync_wal,
            )?;
            self.dirty = true;
            self.maybe_auto_checkpoint()?;
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        let tx = self
            .active_tx
            .take()
            .ok_or_else(|| Error::Sql("No active transaction".into()))?;
        self.catalog = tx.snapshot_catalog;
        self.hot_buffers = tx.snapshot_hot;
        self.cache.clear();
        Ok(())
    }

    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        self.require_writable()?;
        if self.active_tx.is_none() {
            self.active_tx = Some(TxState {
                snapshot_catalog: self.catalog.clone(),
                snapshot_hot: self.hot_buffers.clone(),
                pending_wal_ops: Vec::new(),
                savepoints: Vec::new(),
                implicit_savepoint_tx: true,
            });
        }
        let cat_snap = self.catalog.clone();
        let hot_snap = self.hot_buffers.clone();
        let tx = self.active_tx.as_mut().unwrap();
        let wal_ops_len = tx.pending_wal_ops.len();
        tx.savepoints.push(SavepointState {
            name: name.to_string(),
            snapshot_catalog: cat_snap,
            snapshot_hot: hot_snap,
            wal_ops_len,
        });
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let should_commit = {
            let tx = self
                .active_tx
                .as_mut()
                .ok_or_else(|| Error::Sql(format!("No such savepoint: '{}'", name)))?;
            let pos = tx
                .savepoints
                .iter()
                .rposition(|sp| sp.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| Error::Sql(format!("No such savepoint: '{}'", name)))?;
            tx.savepoints.truncate(pos);
            tx.implicit_savepoint_tx && tx.savepoints.is_empty()
        };
        if should_commit {
            self.commit()?;
        }
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let (cat_snap, hot_snap) = {
            let tx = self
                .active_tx
                .as_mut()
                .ok_or_else(|| Error::Sql(format!("No such savepoint: '{}'", name)))?;
            let pos = tx
                .savepoints
                .iter()
                .rposition(|sp| sp.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| Error::Sql(format!("No such savepoint: '{}'", name)))?;
            // Keep `pos` itself active on the stack (per SQL standard), discard inner savepoints.
            tx.savepoints.truncate(pos + 1);
            let sp = &tx.savepoints[pos];
            tx.pending_wal_ops.truncate(sp.wal_ops_len);
            (sp.snapshot_catalog.clone(), sp.snapshot_hot.clone())
        };
        self.catalog = cat_snap;
        self.hot_buffers = hot_snap;
        self.cache.clear();
        Ok(())
    }

    pub fn has_savepoints(&self) -> bool {
        self.active_tx
            .as_ref()
            .map(|t| !t.savepoints.is_empty())
            .unwrap_or(false)
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn in_transaction(&self) -> bool {
        self.active_tx.is_some()
    }

    fn record_wal_op(&mut self, op: WalOp) -> Result<()> {
        self.record_wal_ops(vec![op])
    }

    fn record_wal_ops(&mut self, ops: Vec<WalOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if let Some(ref mut tx) = self.active_tx {
            tx.pending_wal_ops.extend(ops);
        } else if !self.mvcc_sandbox {
            self.wal.append_frame(
                self.storage.superblock.generation,
                &ops,
                self.options.sync_wal,
            )?;
            self.dirty = true;
            self.maybe_auto_checkpoint()?;
        }
        Ok(())
    }

    fn total_hot_rows(&self) -> usize {
        self.hot_buffers.values().map(|v| v.len()).sum()
    }

    fn maybe_auto_checkpoint(&mut self) -> Result<()> {
        if !self.mvcc_sandbox
            && self.active_tx.is_none()
            && self.total_hot_rows() >= self.options.auto_checkpoint_rows
        {
            self.checkpoint()?;
        }
        Ok(())
    }

    // ========================================================================
    // DDL: Tables, CHECK & FOREIGN KEY validation, Schema Evolution, Indexes
    // ========================================================================

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        self.create_table_full(name, columns, Vec::new(), Vec::new(), Vec::new())
    }

    pub fn create_table_clustered(
        &mut self,
        name: &str,
        columns: Vec<ColumnDef>,
        cluster_by: Vec<String>,
    ) -> Result<()> {
        self.create_table_full(name, columns, cluster_by, Vec::new(), Vec::new())
    }

    fn validate_table_constraints_ddl(&self, schema: &TableSchema) -> Result<()> {
        // Validate CHECK expressions parse and only reference valid columns
        let dummy_row = Row {
            rowid: 0,
            values: vec![Value::Null; schema.columns.len()],
        };
        for chk in &schema.check_constraints {
            let expr = parse_expr_str(&chk.expr_sql)?;
            let _ = eval_bool(&expr, schema, Some(&dummy_row))?;
        }
        // Validate FOREIGN KEY targets
        for fk in &schema.foreign_keys {
            for col in &fk.columns {
                if schema.col_index(col).is_none() {
                    return Err(Error::Schema(format!(
                        "FOREIGN KEY column '{}' does not exist on table '{}'",
                        col, schema.name
                    )));
                }
            }
            let parent_schema: &TableSchema = if fk.ref_table.eq_ignore_ascii_case(&schema.name) {
                schema
            } else {
                self.table_schema(&fk.ref_table).ok_or_else(|| {
                    Error::Schema(format!(
                        "FOREIGN KEY references unknown parent table '{}'",
                        fk.ref_table
                    ))
                })?
            };
            if fk.ref_columns.is_empty() {
                if fk.columns.len() != 1 {
                    return Err(Error::Schema(format!(
                        "FOREIGN KEY on '{}' must specify parent columns when referencing >1 columns",
                        schema.name
                    )));
                }
                if !parent_schema.columns.iter().any(|c| c.primary_key) {
                    return Err(Error::Schema(format!(
                        "Referenced table '{}' has no PRIMARY KEY for implicit FOREIGN KEY",
                        parent_schema.name
                    )));
                }
            } else {
                if fk.columns.len() != fk.ref_columns.len() {
                    return Err(Error::Schema(format!(
                        "FOREIGN KEY column count mismatch ({} vs {}) on table '{}'",
                        fk.columns.len(),
                        fk.ref_columns.len(),
                        schema.name
                    )));
                }
                for rc in &fk.ref_columns {
                    if parent_schema.col_index(rc).is_none() {
                        return Err(Error::Schema(format!(
                            "FOREIGN KEY references unknown column '{}.{}'",
                            parent_schema.name, rc
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn create_table_full(
        &mut self,
        name: &str,
        columns: Vec<ColumnDef>,
        cluster_by: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        mut foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        self.require_writable()?;
        if columns.is_empty() {
            return Err(Error::Schema("Table must have at least one column".into()));
        }
        if self.find_table_idx(name).is_some() {
            return Err(Error::Schema(format!("Table '{}' already exists", name)));
        }
        for cb in &cluster_by {
            if !columns.iter().any(|c| c.name.eq_ignore_ascii_case(cb)) {
                return Err(Error::Schema(format!(
                    "Unknown CLUSTER BY column '{}' on table '{}'",
                    cb, name
                )));
            }
        }

        // Resolve implicit parent PK column when REFERENCES parent_table omits `(col)`
        for fk in &mut foreign_keys {
            if fk.ref_columns.is_empty() {
                let pk_name = if fk.ref_table.eq_ignore_ascii_case(name) {
                    columns
                        .iter()
                        .find(|c| c.primary_key)
                        .map(|c| c.name.clone())
                } else {
                    self.table_schema(&fk.ref_table).and_then(|ps| {
                        ps.columns
                            .iter()
                            .find(|c| c.primary_key)
                            .map(|c| c.name.clone())
                    })
                };
                if let Some(pk) = pk_name {
                    fk.ref_columns.push(pk);
                }
            }
        }

        let id = self.storage.superblock.next_table_id;
        let schema = TableSchema {
            id,
            name: name.to_string(),
            columns,
            next_rowid: 1,
            cluster_by,
            check_constraints,
            foreign_keys,
        };
        self.validate_table_constraints_ddl(&schema)?;

        self.storage.superblock.next_table_id += 1;
        self.catalog.tables.push(TableEntry {
            schema: schema.clone(),
            pages: Vec::new(),
        });
        let wal_op = if schema.check_constraints.is_empty() && schema.foreign_keys.is_empty() {
            WalOp::CreateTable(schema)
        } else {
            WalOp::CreateTableV5(schema)
        };
        self.record_wal_op(wal_op)?;
        Ok(())
    }

    pub fn add_column(&mut self, table_name: &str, column: ColumnDef) -> Result<()> {
        self.add_column_with_constraints(table_name, column, Vec::new(), Vec::new())
    }

    pub fn add_column_with_constraints(
        &mut self,
        table_name: &str,
        mut column: ColumnDef,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(table_name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table_name)))?;
        if self.catalog.tables[idx]
            .schema
            .col_index(&column.name)
            .is_some()
        {
            return Err(Error::Schema(format!(
                "Column '{}' already exists on '{}'",
                column.name, table_name
            )));
        }
        if let Some(ref dv) = column.default_value {
            column.default_value = Some(dv.coerce_for_column(column.col_type)?);
        }
        let existing_rows = self.count_rows(table_name)?;
        if column.primary_key && existing_rows > 0 {
            return Err(Error::Schema(
                "Cannot ADD COLUMN with PRIMARY KEY to non-empty table".into(),
            ));
        }
        if column.unique && column.default_value.is_some() && existing_rows > 1 {
            return Err(Error::Schema(
                "Cannot ADD COLUMN with UNIQUE and non-NULL DEFAULT to table with >1 rows".into(),
            ));
        }
        if column.not_null && column.default_value.is_none() && existing_rows > 0 {
            return Err(Error::Schema(
                "Cannot ADD COLUMN with NOT NULL and no DEFAULT to non-empty table".into(),
            ));
        }
        let mut foreign_keys = foreign_keys;
        let canonical_for_fk = self.catalog.tables[idx].schema.name.clone();
        for fk in &mut foreign_keys {
            if fk.ref_columns.is_empty() {
                let pk_name = if fk.ref_table.eq_ignore_ascii_case(&canonical_for_fk) {
                    self.catalog.tables[idx]
                        .schema
                        .columns
                        .iter()
                        .find(|c| c.primary_key)
                        .map(|c| c.name.clone())
                } else {
                    self.table_schema(&fk.ref_table).and_then(|ps| {
                        ps.columns
                            .iter()
                            .find(|c| c.primary_key)
                            .map(|c| c.name.clone())
                    })
                };
                if let Some(pk) = pk_name {
                    fk.ref_columns.push(pk);
                }
            }
        }

        let mut candidate_schema = self.catalog.tables[idx].schema.clone();
        candidate_schema.columns.push(column.clone());
        candidate_schema
            .check_constraints
            .extend(check_constraints.clone());
        candidate_schema.foreign_keys.extend(foreign_keys.clone());
        self.validate_table_constraints_ddl(&candidate_schema)?;

        if !check_constraints.is_empty() && existing_rows > 0 {
            // Verify backfilled rows satisfy the new CHECK constraints
            let fill = column.default_value.clone().unwrap_or(Value::Null);
            let mut rows = self.scan_all(table_name)?;
            for r in &mut rows {
                r.values.push(fill.clone());
            }
            self.check_row_constraints(&candidate_schema, &rows)?;
        }

        let canonical = self.catalog.tables[idx].schema.name.clone();
        let fill = column.default_value.clone().unwrap_or(Value::Null);
        let key = canonical.to_ascii_lowercase();
        self.catalog.tables[idx].schema = candidate_schema;
        if let Some(hot) = self.hot_buffers.get_mut(&key) {
            for r in hot.iter_mut() {
                r.values.push(fill.clone());
            }
        }
        for p in &self.catalog.tables[idx].pages {
            self.cache.invalidate(p.page_id);
        }
        let wal_op = if check_constraints.is_empty() && foreign_keys.is_empty() {
            WalOp::AddColumn {
                table_name: canonical,
                column,
            }
        } else {
            WalOp::AddColumnV5 {
                table_name: canonical,
                column,
                check_constraints,
                foreign_keys,
            }
        };
        self.record_wal_op(wal_op)?;
        Ok(())
    }

    pub fn set_cluster_by(&mut self, table_name: &str, cluster_by: Vec<String>) -> Result<()> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(table_name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table_name)))?;
        let schema = &self.catalog.tables[idx].schema;
        for cb in &cluster_by {
            if schema.col_index(cb).is_none() {
                return Err(Error::Schema(format!(
                    "Unknown CLUSTER BY column '{}' on table '{}'",
                    cb, schema.name
                )));
            }
        }
        let canonical = schema.name.clone();
        self.catalog.tables[idx].schema.cluster_by = cluster_by.clone();
        self.record_wal_op(WalOp::SetClusterBy {
            table_name: canonical,
            cluster_by,
        })?;
        Ok(())
    }

    fn check_drop_table_fk(&self, name: &str) -> Result<()> {
        if !self.foreign_keys_enabled {
            return Ok(());
        }
        for t in &self.catalog.tables {
            if t.schema.name.eq_ignore_ascii_case(name) {
                continue;
            }
            for fk in &t.schema.foreign_keys {
                if fk.ref_table.eq_ignore_ascii_case(name) {
                    return Err(Error::InvalidData(format!(
                        "FOREIGN KEY constraint failed: cannot DROP table '{}' because '{}' references it",
                        name, t.schema.name
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", name)))?;
        let canonical = self.catalog.tables[idx].schema.name.clone();
        self.check_drop_table_fk(&canonical)?;
        let key = canonical.to_ascii_lowercase();
        for p in &self.catalog.tables[idx].pages {
            self.cache.invalidate(p.page_id);
        }
        self.catalog.tables.remove(idx);
        self.hot_buffers.remove(&key);
        self.catalog
            .indexes
            .retain(|ix| !ix.table_name.eq_ignore_ascii_case(&canonical));
        self.record_wal_op(WalOp::DropTable(canonical))?;
        Ok(())
    }

    fn build_index_in_memory(
        &mut self,
        index_name: &str,
        table_name: &str,
        columns: &[String],
        desc: &[bool],
        kind: IndexKind,
        unique: bool,
    ) -> Result<()> {
        let t_idx = self
            .find_table_idx(table_name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table_name)))?;
        let schema = self.catalog.tables[t_idx].schema.clone();
        if columns.is_empty() {
            return Err(Error::Schema("Index must reference at least one column".into()));
        }
        let mut col_idxs = Vec::with_capacity(columns.len());
        let mut canonical_cols = Vec::with_capacity(columns.len());
        let mut needed = vec![false; schema.columns.len()];
        for c in columns {
            let ci = schema.col_index(c).ok_or_else(|| {
                Error::Schema(format!("Unknown column '{}' on table '{}'", c, schema.name))
            })?;
            col_idxs.push(ci);
            canonical_cols.push(schema.columns[ci].name.clone());
            needed[ci] = true;
        }
        let norm_desc: Vec<bool> = if desc.len() == col_idxs.len() {
            desc.to_vec()
        } else {
            vec![false; col_idxs.len()]
        };

        let pages = self.catalog.tables[t_idx].pages.clone();

        // Parallel page scan for index build when >= 4 pages
        let extracted: Vec<Result<Vec<(Vec<Value>, u64, u32)>>> =
            if pages.len() >= 4 && self.pool.worker_count() > 1 {
                let path = self.path.clone();
                let schema_ref = &schema;
                let needed_ref = &needed;
                let col_idxs_ref = &col_idxs;
                self.pool.execute_indexed(&pages, |_, p| {
                    if p.live_rows() == 0 {
                        return Ok(Vec::new());
                    }
                    let mut f = File::open(&path)?;
                    let rows = read_pax_page_from_file(&mut f, schema_ref, p, Some(needed_ref))?;
                    let mut local = Vec::with_capacity(rows.len());
                    for (slot, row) in rows.iter().enumerate() {
                        if p.is_deleted(slot as u32) {
                            continue;
                        }
                        let key: Vec<Value> = col_idxs_ref
                            .iter()
                            .map(|&ci| row.values[ci].clone())
                            .collect();
                        local.push((key, p.page_id, slot as u32));
                    }
                    Ok(local)
                })
            } else {
                let mut out = Vec::with_capacity(pages.len());
                for p in &pages {
                    if p.live_rows() == 0 {
                        out.push(Ok(Vec::new()));
                        continue;
                    }
                    let rows = self.storage.read_pax_page(&schema, p, Some(&needed))?;
                    let mut local = Vec::with_capacity(rows.len());
                    for (slot, row) in rows.iter().enumerate() {
                        if p.is_deleted(slot as u32) {
                            continue;
                        }
                        let key: Vec<Value> =
                            col_idxs.iter().map(|&ci| row.values[ci].clone()).collect();
                        local.push((key, p.page_id, slot as u32));
                    }
                    out.push(Ok(local));
                }
                out
            };

        let mut all_tuples: Vec<BTreeLeafEntry> = Vec::new();
        for res in extracted {
            all_tuples.extend(res?);
        }

        // If UNIQUE index, validate existing persisted + hot rows have no duplicate non-NULL keys
        if unique {
            let mut seen: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
            for (k, _, _) in &all_tuples {
                if let Some(h) = hash_key_tuple(k) {
                    let bucket = seen.entry(h).or_default();
                    if bucket
                        .iter()
                        .any(|ex| cmp_key_tuple(ex, k, &norm_desc).is_eq())
                    {
                        return Err(Error::InvalidData(format!(
                            "UNIQUE index '{}' failed on '{}': duplicate key {:?}",
                            index_name, schema.name, k
                        )));
                    }
                    bucket.push(k.clone());
                }
            }
            let tbl_key = schema.name.to_ascii_lowercase();
            if let Some(hot) = self.hot_buffers.get(&tbl_key) {
                for r in hot {
                    let k: Vec<Value> = col_idxs.iter().map(|&ci| r.values[ci].clone()).collect();
                    if let Some(h) = hash_key_tuple(&k) {
                        let bucket = seen.entry(h).or_default();
                        if bucket
                            .iter()
                            .any(|ex| cmp_key_tuple(ex, &k, &norm_desc).is_eq())
                        {
                            return Err(Error::InvalidData(format!(
                                "UNIQUE index '{}' failed on '{}': duplicate key {:?}",
                                index_name, schema.name, k
                            )));
                        }
                        bucket.push(k);
                    }
                }
            }
        }

        let mut entry = IndexEntry::new_composite(
            index_name.to_string(),
            schema.name.clone(),
            canonical_cols,
            norm_desc.clone(),
            kind,
            unique,
        );

        match kind {
            IndexKind::Hash => {
                let mut hashes: Vec<(u64, u64, u32)> = Vec::with_capacity(all_tuples.len());
                for (k, pid, slot) in all_tuples {
                    if let Some(h) = hash_key_tuple(&k) {
                        hashes.push((h, pid, slot));
                    }
                }
                hashes.sort_unstable();
                hashes.dedup();
                entry.pending_entries = hashes;
            }
            IndexKind::BTree => {
                all_tuples.sort_by(|a, b| {
                    cmp_key_tuple(&a.0, &b.0, &norm_desc)
                        .then_with(|| a.1.cmp(&b.1))
                        .then_with(|| a.2.cmp(&b.2))
                });
                entry.pending_btree = all_tuples;
            }
        }

        self.catalog.indexes.push(entry);
        Ok(())
    }

    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
        if_not_exists: bool,
    ) -> Result<()> {
        self.create_index_full(
            index_name,
            table_name,
            vec![column_name.to_string()],
            vec![false],
            IndexKind::BTree,
            false,
            if_not_exists,
        )
    }

    pub fn create_index_full(
        &mut self,
        index_name: &str,
        table_name: &str,
        columns: Vec<String>,
        desc: Vec<bool>,
        kind: IndexKind,
        unique: bool,
        if_not_exists: bool,
    ) -> Result<()> {
        self.require_writable()?;
        if self
            .catalog
            .indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(index_name))
        {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::Schema(format!(
                "Index '{}' already exists",
                index_name
            )));
        }
        self.build_index_in_memory(index_name, table_name, &columns, &desc, kind, unique)?;
        let last = self.catalog.indexes.last().unwrap().clone();
        self.record_wal_op(WalOp::CreateIndexV5 {
            name: last.name,
            table_name: last.table_name,
            columns: last.columns,
            desc: last.desc,
            kind: last.kind,
            unique: last.unique,
        })?;
        Ok(())
    }

    pub fn drop_index(&mut self, index_name: &str, if_exists: bool) -> Result<()> {
        self.require_writable()?;
        let pos = self
            .catalog
            .indexes
            .iter()
            .position(|i| i.name.eq_ignore_ascii_case(index_name));
        match pos {
            Some(i) => {
                let canonical = self.catalog.indexes[i].name.clone();
                self.catalog.indexes.remove(i);
                self.record_wal_op(WalOp::DropIndex(canonical))?;
                Ok(())
            }
            None if if_exists => Ok(()),
            None => Err(Error::Schema(format!("No such index: '{}'", index_name))),
        }
    }

    fn update_indexes_for_new_page(&mut self, schema: &TableSchema, page_id: u64, rows: &[Row]) {
        for idx in &mut self.catalog.indexes {
            if idx.table_name.eq_ignore_ascii_case(&schema.name) {
                let col_idxs: Option<Vec<usize>> = idx
                    .columns
                    .iter()
                    .map(|cn| schema.col_index(cn))
                    .collect();
                if let Some(cis) = col_idxs {
                    let tuples: Vec<Vec<Value>> = rows
                        .iter()
                        .map(|r| cis.iter().map(|&ci| r.values[ci].clone()).collect())
                        .collect();
                    idx.index_page_tuples(page_id, tuples);
                }
            }
        }
    }

    /// Demand-paged hash index lookup.
    fn lookup_hash_index(
        &mut self,
        ix_idx: usize,
        key_tuples: &[Vec<Value>],
    ) -> Result<(HashMap<u64, Vec<u32>>, usize)> {
        let mut by_page: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut blocks_read = 0usize;

        let blocks_meta = self.catalog.indexes[ix_idx].blocks.clone();
        let pending = self.catalog.indexes[ix_idx].pending_entries.clone();
        let removed = self.catalog.indexes[ix_idx].removed_pages.clone();

        for tuple in key_tuples {
            let target = match hash_key_tuple(tuple) {
                Some(h) => h,
                None => continue,
            };
            if !pending.is_empty() {
                IndexEntry::search_sorted_slice(&pending, target, &removed, &mut by_page);
            }
            let cand_blocks = self.catalog.indexes[ix_idx].candidate_blocks(target);
            for b_i in cand_blocks {
                let bmeta = &blocks_meta[b_i];
                if let Some(cached) = self.index_cache.get(bmeta.offset) {
                    IndexEntry::search_sorted_slice(cached, target, &removed, &mut by_page);
                } else {
                    blocks_read += 1;
                    let decoded = self.storage.read_index_block(bmeta)?;
                    IndexEntry::search_sorted_slice(&decoded, target, &removed, &mut by_page);
                    self.index_cache.insert(bmeta.offset, decoded);
                }
            }
        }
        for slots in by_page.values_mut() {
            slots.sort_unstable();
            slots.dedup();
        }
        Ok((by_page, blocks_read))
    }

    /// Demand-paged B-tree index lookup (supports composite prefixes + range scans!).
    fn lookup_btree_index(
        &mut self,
        ix_idx: usize,
        specs: &[BTreeScanSpec],
    ) -> Result<(HashMap<u64, Vec<u32>>, usize)> {
        let mut by_page: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut blocks_read = 0usize;

        let blocks_meta = self.catalog.indexes[ix_idx].btree_blocks.clone();
        let pending = self.catalog.indexes[ix_idx].pending_btree.clone();
        let removed = self.catalog.indexes[ix_idx].removed_pages.clone();
        let desc = self.catalog.indexes[ix_idx].desc.clone();

        for spec in specs {
            if !pending.is_empty() {
                IndexEntry::search_btree_slice(&pending, spec, &desc, &removed, &mut by_page);
            }
            let cand_blocks = self.catalog.indexes[ix_idx].candidate_btree_blocks(spec);
            for b_i in cand_blocks {
                let bmeta = &blocks_meta[b_i];
                if let Some(cached) = self.index_cache.get_btree(bmeta.offset) {
                    IndexEntry::search_btree_slice(cached, spec, &desc, &removed, &mut by_page);
                } else {
                    blocks_read += 1;
                    let decoded = self.storage.read_btree_index_block(bmeta)?;
                    IndexEntry::search_btree_slice(&decoded, spec, &desc, &removed, &mut by_page);
                    self.index_cache.insert_btree(bmeta.offset, decoded);
                }
            }
        }
        for slots in by_page.values_mut() {
            slots.sort_unstable();
            slots.dedup();
        }
        Ok((by_page, blocks_read))
    }

    /// Selects the best matching index (composite prefix equality + optional range bound)
    /// for a table given extracted column predicates.
    fn match_index_plan(
        &self,
        table_name: &str,
        preds: &HashMap<String, ColumnPredicates>,
    ) -> Option<(usize, IndexPlan)> {
        let mut best: Option<(usize, usize, IndexPlan)> = None; // (score, ix_idx, plan)

        for (ix_idx, ix) in self.catalog.indexes.iter().enumerate() {
            if !ix.table_name.eq_ignore_ascii_case(table_name) || ix.columns.is_empty() {
                continue;
            }
            match ix.kind {
                IndexKind::Hash => {
                    // Hash index requires equality on ALL indexed columns
                    let mut all_eqs: Vec<Vec<Value>> = Vec::with_capacity(ix.columns.len());
                    let mut ok = true;
                    for col in &ix.columns {
                        if let Some(cp) = preds.get(&col.to_ascii_lowercase()) {
                            if let Some(ref eqs) = cp.eq_values {
                                all_eqs.push(eqs.clone());
                                continue;
                            }
                        }
                        ok = false;
                        break;
                    }
                    if !ok {
                        continue;
                    }
                    // Build cartesian product of equality tuples (bounded to avoid blowup)
                    let mut tuples: Vec<Vec<Value>> = vec![Vec::new()];
                    for vals in &all_eqs {
                        let mut next = Vec::new();
                        for pref in &tuples {
                            for v in vals {
                                let mut t = pref.clone();
                                t.push(v.clone());
                                next.push(t);
                                if next.len() > 256 {
                                    break;
                                }
                            }
                        }
                        tuples = next;
                    }
                    let score = ix.columns.len() * 10 + 1;
                    if best.as_ref().map(|b| score > b.0).unwrap_or(true) {
                        best = Some((score, ix_idx, IndexPlan::Hash(tuples)));
                    }
                }
                IndexKind::BTree => {
                    // Match leftmost prefix equality columns, optionally followed by a range column!
                    let mut eq_prefixes: Vec<Vec<Value>> = vec![Vec::new()];
                    let mut matched_eq_cols = 0usize;
                    let mut trailing_range: Option<(RangeBound, RangeBound)> = None;

                    for col in &ix.columns {
                        if let Some(cp) = preds.get(&col.to_ascii_lowercase()) {
                            if let Some(ref eqs) = cp.eq_values {
                                let mut next = Vec::new();
                                for pref in &eq_prefixes {
                                    for v in eqs {
                                        let mut t = pref.clone();
                                        t.push(v.clone());
                                        next.push(t);
                                        if next.len() > 256 {
                                            break;
                                        }
                                    }
                                }
                                eq_prefixes = next;
                                matched_eq_cols += 1;
                                continue;
                            } else if cp.has_range() {
                                trailing_range = Some((cp.range_lo.clone(), cp.range_hi.clone()));
                                break;
                            }
                        }
                        break;
                    }

                    if matched_eq_cols == 0 && trailing_range.is_none() {
                        continue;
                    }
                    let (rlo, rhi) = trailing_range
                        .unwrap_or((RangeBound::Unbounded, RangeBound::Unbounded));
                    let has_range = !matches!(rlo, RangeBound::Unbounded)
                        || !matches!(rhi, RangeBound::Unbounded);
                    let specs: Vec<BTreeScanSpec> = eq_prefixes
                        .into_iter()
                        .map(|prefix_eq| BTreeScanSpec {
                            prefix_eq,
                            range_lo: rlo.clone(),
                            range_hi: rhi.clone(),
                        })
                        .collect();
                    let score = matched_eq_cols * 10 + if has_range { 5 } else { 0 };
                    if best.as_ref().map(|b| score > b.0).unwrap_or(true) {
                        best = Some((score, ix_idx, IndexPlan::BTree(specs)));
                    }
                }
            }
        }
        best.map(|(_, ix_idx, plan)| (ix_idx, plan))
    }

    /// Flushes all dirty secondary indexes into fresh compressed sorted on-disk leaf blocks.
    fn flush_dirty_indexes(&mut self) -> Result<()> {
        let profile = self.options.compression;
        let num_ix = self.catalog.indexes.len();
        for i in 0..num_ix {
            if !self.catalog.indexes[i].is_dirty() {
                continue;
            }
            let kind = self.catalog.indexes[i].kind;
            match kind {
                IndexKind::Hash => {
                    let old_blocks = std::mem::take(&mut self.catalog.indexes[i].blocks);
                    let pending = std::mem::take(&mut self.catalog.indexes[i].pending_entries);
                    let removed = std::mem::take(&mut self.catalog.indexes[i].removed_pages);

                    let mut all_entries = Vec::new();
                    for bmeta in &old_blocks {
                        let decoded = self.storage.read_index_block(bmeta)?;
                        for e in decoded {
                            if !removed.contains(&e.1) {
                                all_entries.push(e);
                            }
                        }
                    }
                    for e in pending {
                        if !removed.contains(&e.1) {
                            all_entries.push(e);
                        }
                    }
                    all_entries.sort_unstable();
                    all_entries.dedup();

                    let mut new_blocks = Vec::new();
                    for chunk in all_entries.chunks(INDEX_BLOCK_ENTRIES) {
                        let bmeta = self.storage.write_index_block(chunk, profile)?;
                        new_blocks.push(bmeta);
                    }
                    self.catalog.indexes[i].blocks = new_blocks;
                }
                IndexKind::BTree => {
                    let old_blocks = std::mem::take(&mut self.catalog.indexes[i].btree_blocks);
                    let pending = std::mem::take(&mut self.catalog.indexes[i].pending_btree);
                    let removed = std::mem::take(&mut self.catalog.indexes[i].removed_pages);
                    let desc = self.catalog.indexes[i].desc.clone();
                    let key_cols = self.catalog.indexes[i].columns.len();

                    let mut all_entries = Vec::new();
                    for bmeta in &old_blocks {
                        let decoded = self.storage.read_btree_index_block(bmeta)?;
                        for e in decoded {
                            if !removed.contains(&e.1) {
                                all_entries.push(e);
                            }
                        }
                    }
                    for e in pending {
                        if !removed.contains(&e.1) {
                            all_entries.push(e);
                        }
                    }
                    all_entries.sort_by(|a, b| {
                        cmp_key_tuple(&a.0, &b.0, &desc)
                            .then_with(|| a.1.cmp(&b.1))
                            .then_with(|| a.2.cmp(&b.2))
                    });

                    let mut new_blocks = Vec::new();
                    for chunk in all_entries.chunks(INDEX_BLOCK_ENTRIES) {
                        let bmeta =
                            self.storage
                                .write_btree_index_block(chunk, key_cols, profile)?;
                        new_blocks.push(bmeta);
                    }
                    self.catalog.indexes[i].btree_blocks = new_blocks;
                }
            }
        }
        Ok(())
    }

    // ========================================================================
    // Constraint checking: CHECK, UNIQUE / PRIMARY KEY, FOREIGN KEY
    // ========================================================================

    fn check_row_constraints(&self, schema: &TableSchema, rows: &[Row]) -> Result<()> {
        if schema.check_constraints.is_empty() || rows.is_empty() {
            return Ok(());
        }
        let parsed: Vec<(&CheckConstraint, Expr)> = schema
            .check_constraints
            .iter()
            .map(|c| parse_expr_str(&c.expr_sql).map(|e| (c, e)))
            .collect::<Result<Vec<_>>>()?;

        for row in rows {
            for (chk, expr) in &parsed {
                // Per SQL standard: CHECK fails ONLY when expression evaluates to FALSE (Some(false)).
                // TRUE (Some(true)) and UNKNOWN/NULL (None) both pass.
                if eval_bool(expr, schema, Some(row))? == Some(false) {
                    let label = chk
                        .name
                        .as_deref()
                        .unwrap_or(&chk.expr_sql);
                    return Err(Error::InvalidData(format!(
                        "CHECK constraint '{}' failed on table '{}'",
                        label, schema.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn key_exists_in_table(
        &mut self,
        table_name: &str,
        col_names: &[String],
        target_vals: &[Value],
        extra_batch: Option<&[Row]>,
    ) -> Result<bool> {
        let t_idx = self
            .find_table_idx(table_name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table_name)))?;
        let schema = self.catalog.tables[t_idx].schema.clone();
        let col_idxs: Vec<usize> = col_names
            .iter()
            .map(|c| {
                schema.col_index(c).ok_or_else(|| {
                    Error::Schema(format!("Unknown column '{}.{}'", schema.name, c))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Check self-referential incoming batch first
        if let Some(batch) = extra_batch {
            for r in batch {
                if col_idxs
                    .iter()
                    .zip(target_vals)
                    .all(|(&ci, tv)| r.values[ci].sql_cmp(tv).is_eq())
                {
                    return Ok(true);
                }
            }
        }

        // Check hot buffer
        let key = schema.name.to_ascii_lowercase();
        if let Some(hot) = self.hot_buffers.get(&key) {
            for r in hot {
                if col_idxs
                    .iter()
                    .zip(target_vals)
                    .all(|(&ci, tv)| r.values[ci].sql_cmp(tv).is_eq())
                {
                    return Ok(true);
                }
            }
        }

        // Check persisted pages (with zone-map / Bloom pruning)
        let pages = self.catalog.tables[t_idx].pages.clone();
        let mut needed = vec![false; schema.columns.len()];
        for &ci in &col_idxs {
            needed[ci] = true;
        }
        let pk_col = schema.integer_pk_col();

        for p in &pages {
            if p.live_rows() == 0 {
                continue;
            }
            let mut pruned = false;
            for (&ci, tv) in col_idxs.iter().zip(target_vals) {
                if Some(ci) == pk_col {
                    if let Value::Integer(iv) = tv {
                        if *iv < p.min_rowid || *iv > p.max_rowid {
                            pruned = true;
                            break;
                        }
                    }
                }
                if let Some(zm) = p.zone_maps.get(ci) {
                    if let (Some(lo), Some(hi)) = (&zm.min_val, &zm.max_val) {
                        if tv.sql_cmp(lo).is_lt() || tv.sql_cmp(hi).is_gt() {
                            pruned = true;
                            break;
                        }
                    }
                }
                if let Some(Some(bf)) = p.blooms.get(ci) {
                    if !bf.might_contain(tv) {
                        pruned = true;
                        break;
                    }
                }
            }
            if pruned {
                continue;
            }
            let rows = self.storage.read_pax_page(&schema, p, Some(&needed))?;
            for (slot, r) in rows.iter().enumerate() {
                if p.is_deleted(slot as u32) {
                    continue;
                }
                if col_idxs
                    .iter()
                    .zip(target_vals)
                    .all(|(&ci, tv)| r.values[ci].sql_cmp(tv).is_eq())
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn check_foreign_keys_on_write(&mut self, schema: &TableSchema, rows: &[Row]) -> Result<()> {
        if !self.foreign_keys_enabled || schema.foreign_keys.is_empty() || rows.is_empty() {
            return Ok(());
        }
        for fk in &schema.foreign_keys {
            let child_idxs: Vec<usize> = fk
                .columns
                .iter()
                .map(|c| {
                    schema.col_index(c).ok_or_else(|| {
                        Error::Schema(format!("Unknown FK column '{}.{}'", schema.name, c))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let is_self_ref = fk.ref_table.eq_ignore_ascii_case(&schema.name);
            let mut checked_keys: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();

            for row in rows {
                let key_vals: Vec<Value> = child_idxs
                    .iter()
                    .map(|&ci| row.values[ci].clone())
                    .collect();
                // SQL MATCH SIMPLE: if any FK column is NULL, constraint passes
                if key_vals.iter().any(|v| v.is_null()) {
                    continue;
                }
                if let Some(h) = hash_key_tuple(&key_vals) {
                    let bucket = checked_keys.entry(h).or_default();
                    if bucket
                        .iter()
                        .any(|prev| prev.iter().zip(&key_vals).all(|(a, b)| a.sql_cmp(b).is_eq()))
                    {
                        continue;
                    }
                    let extra = if is_self_ref { Some(rows) } else { None };
                    if !self.key_exists_in_table(&fk.ref_table, &fk.ref_columns, &key_vals, extra)?
                    {
                        return Err(Error::InvalidData(format!(
                            "FOREIGN KEY constraint failed on '{}' ({}) referencing '{}' ({}): value {:?} not found",
                            schema.name,
                            fk.columns.join(", "),
                            fk.ref_table,
                            fk.ref_columns.join(", "),
                            key_vals
                        )));
                    }
                    bucket.push(key_vals);
                }
            }
        }
        Ok(())
    }

    fn find_referencing_fks(&self, parent_table: &str) -> Vec<(String, ForeignKeyDef)> {
        let mut out = Vec::new();
        for t in &self.catalog.tables {
            for fk in &t.schema.foreign_keys {
                if fk.ref_table.eq_ignore_ascii_case(parent_table) {
                    out.push((t.schema.name.clone(), fk.clone()));
                }
            }
        }
        out
    }

    fn verify_no_referencing_children(&mut self, parent_table: &str, deleted_rows: &[Row]) -> Result<()> {
        if deleted_rows.is_empty() {
            return Ok(());
        }
        let parent_schema = match self.table_schema(parent_table) {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        let refs = self.find_referencing_fks(parent_table);
        for (child_table, fk) in refs {
            let p_idxs: Vec<usize> = fk
                .ref_columns
                .iter()
                .filter_map(|c| parent_schema.col_index(c))
                .collect();
            let deleted_ids: HashSet<i64> = deleted_rows.iter().map(|r| r.rowid).collect();
            let child_rows = self.scan_all(&child_table)?;
            let child_schema = self.table_schema(&child_table).unwrap().clone();
            let c_idxs: Vec<usize> = fk
                .columns
                .iter()
                .filter_map(|c| child_schema.col_index(c))
                .collect();

            for prow in deleted_rows {
                let pkey: Vec<&Value> = p_idxs.iter().map(|&i| &prow.values[i]).collect();
                if pkey.iter().any(|v| v.is_null()) {
                    continue;
                }
                for crow in &child_rows {
                    if child_table.eq_ignore_ascii_case(parent_table)
                        && deleted_ids.contains(&crow.rowid)
                    {
                        continue;
                    }
                    if c_idxs
                        .iter()
                        .zip(&pkey)
                        .all(|(&ci, pv)| crow.values[ci].sql_cmp(pv).is_eq())
                    {
                        return Err(Error::InvalidData(format!(
                            "FOREIGN KEY constraint failed: cannot DELETE from '{}' because '{}' references {:?}",
                            parent_table, child_table, pkey
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn fetch_rows_by_ids(&mut self, table: &str, ids: &HashSet<i64>) -> Result<Vec<Row>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let all = self.scan_all(table)?;
        Ok(all.into_iter().filter(|r| ids.contains(&r.rowid)).collect())
    }

    fn enforce_parent_delete_cascades(
        &mut self,
        parent_table: &str,
        deleted_rows: &[Row],
        visited: &mut HashSet<(String, i64)>,
    ) -> Result<()> {
        if !self.foreign_keys_enabled || deleted_rows.is_empty() {
            return Ok(());
        }
        let parent_schema = match self.table_schema(parent_table) {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        let refs = self.find_referencing_fks(parent_table);
        if refs.is_empty() {
            return Ok(());
        }

        for (child_table, fk) in refs {
            let p_idxs: Vec<usize> = fk
                .ref_columns
                .iter()
                .filter_map(|c| parent_schema.col_index(c))
                .collect();
            let child_schema = match self.table_schema(&child_table) {
                Some(s) => s.clone(),
                None => continue,
            };
            let c_idxs: Vec<usize> = fk
                .columns
                .iter()
                .filter_map(|c| child_schema.col_index(c))
                .collect();

            let mut deleted_keys: Vec<Vec<Value>> = Vec::new();
            for prow in deleted_rows {
                let pkey: Vec<Value> = p_idxs.iter().map(|&i| prow.values[i].clone()).collect();
                if !pkey.iter().any(|v| v.is_null()) {
                    deleted_keys.push(pkey);
                }
            }
            if deleted_keys.is_empty() {
                continue;
            }

            let child_rows = self.scan_all(&child_table)?;
            let mut matched_children: Vec<Row> = Vec::new();
            for crow in child_rows {
                if visited.contains(&(child_table.to_ascii_lowercase(), crow.rowid)) {
                    continue;
                }
                let matches_any = deleted_keys.iter().any(|pk| {
                    c_idxs
                        .iter()
                        .zip(pk)
                        .all(|(&ci, pv)| crow.values[ci].sql_cmp(pv).is_eq())
                });
                if matches_any {
                    matched_children.push(crow);
                }
            }
            if matched_children.is_empty() {
                continue;
            }

            match fk.on_delete {
                FkAction::NoAction | FkAction::Restrict => {
                    return Err(Error::InvalidData(format!(
                        "FOREIGN KEY constraint failed: cannot DELETE from '{}' while rows in '{}' reference it",
                        parent_table, child_table
                    )));
                }
                FkAction::Cascade => {
                    for r in &matched_children {
                        visited.insert((child_table.to_ascii_lowercase(), r.rowid));
                    }
                    let rowids: Vec<i64> = matched_children.iter().map(|r| r.rowid).collect();
                    let set: HashSet<i64> = rowids.iter().copied().collect();
                    self.mark_rowids_deleted_in_memory(&child_table, &set)?;
                    self.record_wal_op(WalOp::DeleteRowIds {
                        table_name: child_table.clone(),
                        rowids,
                    })?;
                    self.enforce_parent_delete_cascades(&child_table, &matched_children, visited)?;
                }
                FkAction::SetNull | FkAction::SetDefault => {
                    let is_null = fk.on_delete == FkAction::SetNull;
                    let mut updated = Vec::with_capacity(matched_children.len());
                    let mut old_ids = HashSet::with_capacity(matched_children.len());
                    for mut crow in matched_children {
                        old_ids.insert(crow.rowid);
                        for &ci in &c_idxs {
                            let col_def = &child_schema.columns[ci];
                            let new_v = if is_null {
                                Value::Null
                            } else {
                                col_def.default_value.clone().unwrap_or(Value::Null)
                            };
                            if (col_def.not_null || col_def.primary_key) && new_v.is_null() {
                                return Err(Error::InvalidData(format!(
                                    "NOT NULL constraint failed during FOREIGN KEY action on '{}.{}'",
                                    child_schema.name, col_def.name
                                )));
                            }
                            crow.values[ci] = new_v;
                        }
                        updated.push(crow);
                    }
                    self.check_row_constraints(&child_schema, &updated)?;
                    self.check_unique_constraints(
                        &child_schema.name,
                        &child_schema,
                        &updated,
                        Some(&old_ids),
                    )?;
                    self.mark_rowids_deleted_in_memory(&child_schema.name, &old_ids)?;
                    let ckey = child_schema.name.to_ascii_lowercase();
                    self.hot_buffers
                        .entry(ckey)
                        .or_default()
                        .extend(updated.clone());
                    self.record_wal_op(WalOp::UpdateRows {
                        table_name: child_schema.name,
                        rows: updated,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn enforce_parent_update_cascades(
        &mut self,
        parent_table: &str,
        changed_pairs: &[(Row, Row)],
    ) -> Result<()> {
        if !self.foreign_keys_enabled || changed_pairs.is_empty() {
            return Ok(());
        }
        let parent_schema = match self.table_schema(parent_table) {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        let refs = self.find_referencing_fks(parent_table);
        if refs.is_empty() {
            return Ok(());
        }

        for (child_table, fk) in refs {
            let p_idxs: Vec<usize> = fk
                .ref_columns
                .iter()
                .filter_map(|c| parent_schema.col_index(c))
                .collect();
            let child_schema = match self.table_schema(&child_table) {
                Some(s) => s.clone(),
                None => continue,
            };
            let c_idxs: Vec<usize> = fk
                .columns
                .iter()
                .filter_map(|c| child_schema.col_index(c))
                .collect();

            let mut key_changes: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
            for (old_r, new_r) in changed_pairs {
                let old_k: Vec<Value> = p_idxs.iter().map(|&i| old_r.values[i].clone()).collect();
                let new_k: Vec<Value> = p_idxs.iter().map(|&i| new_r.values[i].clone()).collect();
                if old_k.iter().any(|v| v.is_null()) {
                    continue;
                }
                if old_k
                    .iter()
                    .zip(&new_k)
                    .any(|(a, b)| !a.sql_cmp(b).is_eq())
                {
                    key_changes.push((old_k, new_k));
                }
            }
            if key_changes.is_empty() {
                continue;
            }

            let child_rows = self.scan_all(&child_table)?;
            let mut matched: Vec<(Row, Vec<Value>)> = Vec::new();
            for crow in child_rows {
                for (ok, nk) in &key_changes {
                    if c_idxs
                        .iter()
                        .zip(ok)
                        .all(|(&ci, pv)| crow.values[ci].sql_cmp(pv).is_eq())
                    {
                        matched.push((crow, nk.clone()));
                        break;
                    }
                }
            }
            if matched.is_empty() {
                continue;
            }

            match fk.on_update {
                FkAction::NoAction | FkAction::Restrict => {
                    return Err(Error::InvalidData(format!(
                        "FOREIGN KEY constraint failed: cannot UPDATE referenced key on '{}' while '{}' references it",
                        parent_table, child_table
                    )));
                }
                FkAction::Cascade | FkAction::SetNull | FkAction::SetDefault => {
                    let mut updated = Vec::with_capacity(matched.len());
                    let mut old_ids = HashSet::with_capacity(matched.len());
                    let mut child_pairs = Vec::with_capacity(matched.len());
                    for (mut crow, new_k) in matched {
                        let orig = crow.clone();
                        old_ids.insert(crow.rowid);
                        for (pos, &ci) in c_idxs.iter().enumerate() {
                            let col_def = &child_schema.columns[ci];
                            let nv = match fk.on_update {
                                FkAction::Cascade => new_k[pos].coerce_for_column(col_def.col_type)?,
                                FkAction::SetNull => Value::Null,
                                FkAction::SetDefault => {
                                    col_def.default_value.clone().unwrap_or(Value::Null)
                                }
                                _ => unreachable!(),
                            };
                            if (col_def.not_null || col_def.primary_key) && nv.is_null() {
                                return Err(Error::InvalidData(format!(
                                    "NOT NULL constraint failed during ON UPDATE action on '{}.{}'",
                                    child_schema.name, col_def.name
                                )));
                            }
                            crow.values[ci] = nv;
                        }
                        child_pairs.push((orig, crow.clone()));
                        updated.push(crow);
                    }
                    self.check_row_constraints(&child_schema, &updated)?;
                    self.check_unique_constraints(
                        &child_schema.name,
                        &child_schema,
                        &updated,
                        Some(&old_ids),
                    )?;
                    self.mark_rowids_deleted_in_memory(&child_schema.name, &old_ids)?;
                    let ckey = child_schema.name.to_ascii_lowercase();
                    self.hot_buffers
                        .entry(ckey)
                        .or_default()
                        .extend(updated.clone());
                    self.record_wal_op(WalOp::UpdateRows {
                        table_name: child_schema.name.clone(),
                        rows: updated,
                    })?;
                    self.enforce_parent_update_cascades(&child_schema.name, &child_pairs)?;
                }
            }
        }
        Ok(())
    }

    fn check_unique_constraints(
        &mut self,
        table: &str,
        schema: &TableSchema,
        new_rows: &[Row],
        ignore_rowids: Option<&HashSet<i64>>,
    ) -> Result<()> {
        if new_rows.is_empty() {
            return Ok(());
        }
        let unique_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique || c.primary_key)
            .map(|(i, _)| i)
            .collect();

        let pk_col = schema.integer_pk_col();
        let t_idx = self.find_table_idx(table).unwrap();
        let current_next_rowid = self.catalog.tables[t_idx].schema.next_rowid;

        for &c_idx in &unique_cols {
            let col_name = &schema.columns[c_idx].name;
            let mut batch_hashes: HashMap<u64, Vec<&Value>> = HashMap::with_capacity(new_rows.len());
            let mut min_int_val = i64::MAX;
            let mut max_int_val = i64::MIN;
            let mut all_int = true;
            for r in new_rows {
                let v = &r.values[c_idx];
                if v.is_null() {
                    continue;
                }
                if let Value::Integer(iv) = v {
                    if *iv < min_int_val {
                        min_int_val = *iv;
                    }
                    if *iv > max_int_val {
                        max_int_val = *iv;
                    }
                } else {
                    all_int = false;
                }
                if let Some(h) = hash_value(v) {
                    let bucket = batch_hashes.entry(h).or_default();
                    if bucket.iter().any(|existing| existing.sql_cmp(v).is_eq()) {
                        return Err(Error::InvalidData(format!(
                            "UNIQUE constraint failed on '{}.{}': duplicate value {}",
                            schema.name, col_name, v
                        )));
                    }
                    bucket.push(v);
                }
            }
            if batch_hashes.is_empty() {
                continue;
            }

            if Some(c_idx) == pk_col
                && ignore_rowids.is_none()
                && all_int
                && min_int_val >= current_next_rowid
            {
                continue;
            }

            let key = schema.name.to_ascii_lowercase();
            if let Some(hot) = self.hot_buffers.get(&key) {
                for ex in hot {
                    if let Some(ign) = ignore_rowids {
                        if ign.contains(&ex.rowid) {
                            continue;
                        }
                    }
                    let ev = &ex.values[c_idx];
                    if let Some(h) = hash_value(ev) {
                        if let Some(bucket) = batch_hashes.get(&h) {
                            if bucket.iter().any(|nv| nv.sql_cmp(ev).is_eq()) {
                                return Err(Error::InvalidData(format!(
                                    "UNIQUE constraint failed on '{}.{}': value {} already exists",
                                    schema.name, col_name, ev
                                )));
                            }
                        }
                    }
                }
            }

            let pages = self.catalog.tables[t_idx].pages.clone();
            let mut needed = vec![false; schema.columns.len()];
            needed[c_idx] = true;
            for p in &pages {
                if p.live_rows() == 0 {
                    continue;
                }
                if all_int {
                    if Some(c_idx) == pk_col {
                        if max_int_val < p.min_rowid || min_int_val > p.max_rowid {
                            continue;
                        }
                    } else if let Some(zm) = p.zone_maps.get(c_idx) {
                        if let (Some(Value::Integer(lo)), Some(Value::Integer(hi))) =
                            (&zm.min_val, &zm.max_val)
                        {
                            if max_int_val < *lo || min_int_val > *hi {
                                continue;
                            }
                        }
                    }
                }
                if batch_hashes.len() <= 32 {
                    if let Some(Some(bf)) = p.blooms.get(c_idx) {
                        let any_hit = batch_hashes
                            .values()
                            .flat_map(|b| b.iter())
                            .any(|v| bf.might_contain(v));
                        if !any_hit {
                            continue;
                        }
                    }
                }
                let page_rows = self.storage.read_pax_page(&schema, p, Some(&needed))?;
                for (slot, ex) in page_rows.iter().enumerate() {
                    if p.is_deleted(slot as u32) {
                        continue;
                    }
                    if let Some(ign) = ignore_rowids {
                        if ign.contains(&ex.rowid) {
                            continue;
                        }
                    }
                    let ev = &ex.values[c_idx];
                    if let Some(h) = hash_value(ev) {
                        if let Some(bucket) = batch_hashes.get(&h) {
                            if bucket.iter().any(|nv| nv.sql_cmp(ev).is_eq()) {
                                return Err(Error::InvalidData(format!(
                                    "UNIQUE constraint failed on '{}.{}': value {} already exists",
                                    schema.name, col_name, ev
                                )));
                            }
                        }
                    }
                }
            }
        }

        // Also enforce CREATE UNIQUE INDEX (single-column or composite multi-column!)
        let unique_indexes: Vec<(String, Vec<usize>)> = self
            .catalog
            .indexes
            .iter()
            .filter(|ix| ix.unique && ix.table_name.eq_ignore_ascii_case(&schema.name))
            .filter_map(|ix| {
                let cis: Option<Vec<usize>> = ix
                    .columns
                    .iter()
                    .map(|c| schema.col_index(c))
                    .collect();
                cis.map(|v| (ix.name.clone(), v))
            })
            .collect();

        for (ix_name, cis) in unique_indexes {
            let mut batch_map: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
            for r in new_rows {
                let tup: Vec<Value> = cis.iter().map(|&ci| r.values[ci].clone()).collect();
                if let Some(h) = hash_key_tuple(&tup) {
                    let bucket = batch_map.entry(h).or_default();
                    if bucket
                        .iter()
                        .any(|ex| ex.iter().zip(&tup).all(|(a, b)| a.sql_cmp(b).is_eq()))
                    {
                        return Err(Error::InvalidData(format!(
                            "UNIQUE index '{}' failed on '{}': duplicate key {:?}",
                            ix_name, schema.name, tup
                        )));
                    }
                    bucket.push(tup);
                }
            }
            if batch_map.is_empty() {
                continue;
            }
            let key = schema.name.to_ascii_lowercase();
            if let Some(hot) = self.hot_buffers.get(&key) {
                for ex in hot {
                    if let Some(ign) = ignore_rowids {
                        if ign.contains(&ex.rowid) {
                            continue;
                        }
                    }
                    let tup: Vec<Value> = cis.iter().map(|&ci| ex.values[ci].clone()).collect();
                    if let Some(h) = hash_key_tuple(&tup) {
                        if let Some(bucket) = batch_map.get(&h) {
                            if bucket
                                .iter()
                                .any(|nv| nv.iter().zip(&tup).all(|(a, b)| a.sql_cmp(b).is_eq()))
                            {
                                return Err(Error::InvalidData(format!(
                                    "UNIQUE index '{}' failed on '{}': key {:?} already exists",
                                    ix_name, schema.name, tup
                                )));
                            }
                        }
                    }
                }
            }
            let pages = self.catalog.tables[t_idx].pages.clone();
            let mut needed = vec![false; schema.columns.len()];
            for &ci in &cis {
                needed[ci] = true;
            }
            for p in &pages {
                if p.live_rows() == 0 {
                    continue;
                }
                let page_rows = self.storage.read_pax_page(&schema, p, Some(&needed))?;
                for (slot, ex) in page_rows.iter().enumerate() {
                    if p.is_deleted(slot as u32) {
                        continue;
                    }
                    if let Some(ign) = ignore_rowids {
                        if ign.contains(&ex.rowid) {
                            continue;
                        }
                    }
                    let tup: Vec<Value> = cis.iter().map(|&ci| ex.values[ci].clone()).collect();
                    if let Some(h) = hash_key_tuple(&tup) {
                        if let Some(bucket) = batch_map.get(&h) {
                            if bucket
                                .iter()
                                .any(|nv| nv.iter().zip(&tup).all(|(a, b)| a.sql_cmp(b).is_eq()))
                            {
                                return Err(Error::InvalidData(format!(
                                    "UNIQUE index '{}' failed on '{}': key {:?} already exists",
                                    ix_name, schema.name, tup
                                )));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // ========================================================================
    // DML: Insert, Bulk Insert, Scan, Parallel Vectorized Analytics, Update, Delete
    // ========================================================================

    pub fn insert_row(&mut self, table: &str, values: Vec<Value>) -> Result<i64> {
        let ids = self.insert_batch_returning_rowids(table, vec![values])?;
        Ok(ids[0])
    }

    pub fn insert_batch(&mut self, table: &str, batch: Vec<Vec<Value>>) -> Result<usize> {
        Ok(self.insert_batch_returning_rowids(table, batch)?.len())
    }

    fn insert_batch_returning_rowids(
        &mut self,
        table: &str,
        batch: Vec<Vec<Value>>,
    ) -> Result<Vec<i64>> {
        self.require_writable()?;
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;

        let schema = self.catalog.tables[idx].schema.clone();
        let pk_col = schema.integer_pk_col();
        let mut next_rowid = schema.next_rowid;

        let mut rows = Vec::with_capacity(batch.len());
        let mut rowids = Vec::with_capacity(batch.len());
        for raw_vals in batch {
            if raw_vals.len() != schema.columns.len() {
                return Err(Error::InvalidData(format!(
                    "Expected {} values for table '{}', got {}",
                    schema.columns.len(),
                    schema.name,
                    raw_vals.len()
                )));
            }
            let mut coerced = Vec::with_capacity(schema.columns.len());
            for (c_idx, (v, col_def)) in raw_vals.iter().zip(&schema.columns).enumerate() {
                let cv = v.coerce_for_column(col_def.col_type)?;
                if (col_def.not_null || col_def.primary_key)
                    && cv.is_null()
                    && Some(c_idx) != pk_col
                {
                    return Err(Error::InvalidData(format!(
                        "NOT NULL constraint failed on '{}.{}'",
                        schema.name, col_def.name
                    )));
                }
                coerced.push(cv);
            }
            let rowid = if let Some(pk_i) = pk_col {
                match coerced[pk_i] {
                    Value::Integer(id) => {
                        if id >= next_rowid {
                            next_rowid = id.saturating_add(1);
                        }
                        id
                    }
                    Value::Null => {
                        let id = next_rowid;
                        next_rowid = next_rowid.saturating_add(1);
                        coerced[pk_i] = Value::Integer(id);
                        id
                    }
                    _ => unreachable!(),
                }
            } else {
                let id = next_rowid;
                next_rowid = next_rowid.saturating_add(1);
                id
            };
            rowids.push(rowid);
            rows.push(Row {
                rowid,
                values: coerced,
            });
        }

        // Enforce CHECK, UNIQUE/PK, and FOREIGN KEY constraints
        self.check_row_constraints(&schema, &rows)?;
        self.check_unique_constraints(table, &schema, &rows, None)?;
        self.check_foreign_keys_on_write(&schema, &rows)?;

        self.catalog.tables[idx].schema.next_rowid = next_rowid;
        let canonical = self.catalog.tables[idx].schema.name.clone();
        let key = canonical.to_ascii_lowercase();
        self.hot_buffers
            .entry(key)
            .or_default()
            .extend(rows.clone());

        self.record_wal_op(WalOp::InsertRows {
            table_name: canonical,
            rows,
        })?;
        Ok(rowids)
    }

    pub fn bulk_insert(&mut self, table: &str, batch: Vec<Vec<Value>>) -> Result<usize> {
        self.require_writable()?;
        if batch.is_empty() {
            return Ok(0);
        }
        if self.active_tx.is_some() {
            return self.insert_batch(table, batch);
        }
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;

        let schema = self.catalog.tables[idx].schema.clone();
        let pk_col = schema.integer_pk_col();
        let mut next_rowid = schema.next_rowid;
        let mut rows = Vec::with_capacity(batch.len());

        for raw_vals in batch {
            if raw_vals.len() != schema.columns.len() {
                return Err(Error::InvalidData("Column count mismatch".into()));
            }
            let mut coerced = Vec::with_capacity(schema.columns.len());
            for (c_idx, (v, col_def)) in raw_vals.iter().zip(&schema.columns).enumerate() {
                let cv = v.coerce_for_column(col_def.col_type)?;
                if (col_def.not_null || col_def.primary_key)
                    && cv.is_null()
                    && Some(c_idx) != pk_col
                {
                    return Err(Error::InvalidData(format!(
                        "NOT NULL constraint failed on '{}.{}'",
                        schema.name, col_def.name
                    )));
                }
                coerced.push(cv);
            }
            let rowid = if let Some(pk_i) = pk_col {
                match coerced[pk_i] {
                    Value::Integer(id) => {
                        if id >= next_rowid {
                            next_rowid = id.saturating_add(1);
                        }
                        id
                    }
                    Value::Null => {
                        let id = next_rowid;
                        next_rowid = next_rowid.saturating_add(1);
                        coerced[pk_i] = Value::Integer(id);
                        id
                    }
                    _ => unreachable!(),
                }
            } else {
                let id = next_rowid;
                next_rowid = next_rowid.saturating_add(1);
                id
            };
            rows.push(Row {
                rowid,
                values: coerced,
            });
        }

        if !schema.check_constraints.is_empty() {
            self.check_row_constraints(&schema, &rows)?;
        }
        self.check_unique_constraints(&schema.name, &schema, &rows, None)?;
        if self.foreign_keys_enabled && !schema.foreign_keys.is_empty() {
            self.check_foreign_keys_on_write(&schema, &rows)?;
        }

        let count = rows.len();
        self.catalog.tables[idx].schema.next_rowid = next_rowid;
        let key = schema.name.to_ascii_lowercase();
        let buf = self.hot_buffers.entry(key).or_default();
        buf.extend(rows);

        let flush_thresh = if schema.cluster_by.is_empty() {
            self.options.page_rows
        } else {
            self.options.page_rows * 16
        };
        let page_rows = self.options.page_rows;
        let profile = self.options.compression;
        if buf.len() >= flush_thresh {
            let mut to_flush = std::mem::take(buf);
            if !schema.cluster_by.is_empty() {
                sort_rows_by_cluster(&schema, &mut to_flush);
            }
            let full_len = (to_flush.len() / page_rows) * page_rows;
            let remainder = to_flush.split_off(full_len);
            *buf = remainder;

            let chunks: Vec<&[Row]> = to_flush.chunks(page_rows).collect();
            if chunks.len() >= 2 && self.pool.worker_count() > 1 {
                let encoded_pages: Vec<Result<_>> = self
                    .pool
                    .execute_indexed(&chunks, |_, chunk| encode_pax_page(&schema, chunk, profile));
                for (chunk, enc_res) in chunks.into_iter().zip(encoded_pages) {
                    let meta = self.storage.write_encoded_pax_page(enc_res?)?;
                    let pid = meta.page_id;
                    self.catalog.tables[idx].pages.push(meta);
                    self.update_indexes_for_new_page(&schema, pid, chunk);
                }
            } else {
                for chunk in chunks {
                    let meta = self.storage.write_pax_page(&schema, chunk, profile)?;
                    let pid = meta.page_id;
                    self.catalog.tables[idx].pages.push(meta);
                    self.update_indexes_for_new_page(&schema, pid, chunk);
                }
            }
        }
        self.dirty = true;
        Ok(count)
    }

    pub fn count_rows(&self, table: &str) -> Result<u64> {
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;
        let entry = &self.catalog.tables[idx];
        let mut total: u64 = entry.pages.iter().map(|p| p.live_rows() as u64).sum();
        let key = entry.schema.name.to_ascii_lowercase();
        if let Some(hot) = self.hot_buffers.get(&key) {
            total += hot.len() as u64;
        }
        Ok(total)
    }

    pub fn scan_all(&mut self, table: &str) -> Result<Vec<Row>> {
        self.scan_filtered(table, None, None)
    }

    pub fn scan_filtered(
        &mut self,
        table: &str,
        where_clause: Option<&Expr>,
        needed_cols: Option<&[bool]>,
    ) -> Result<Vec<Row>> {
        let (rows, _) = self.scan_filtered_with_metrics(table, where_clause, needed_cols)?;
        Ok(rows)
    }

    /// Parallel vectorized columnar streaming GROUP BY executor (PostgreSQL-style Gather +
    /// Partial HashAggregate across worker threads!).
    pub fn vectorized_group_by(
        &mut self,
        table: &str,
        group_col: Option<usize>,
        agg_kinds: &[VecAggKind],
        vec_filter: Option<&VecFilter>,
        where_expr: Option<&Expr>,
        needed_cols: &[bool],
    ) -> Result<(Vec<(Option<Value>, Vec<Value>)>, ScanMetrics)> {
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;
        let schema = self.catalog.tables[idx].schema.clone();
        let pages = self.catalog.tables[idx].pages.clone();
        let key = schema.name.to_ascii_lowercase();

        let mut metrics = ScanMetrics {
            total_pages: pages.len(),
            ..Default::default()
        };

        let mut candidate_indices: Vec<usize> = Vec::with_capacity(pages.len());
        for (p_i, page_meta) in pages.iter().enumerate() {
            if page_meta.live_rows() == 0 {
                continue;
            }
            if let Some(expr) = where_expr {
                let (zm_ok, bloom_ok) = page_might_match(
                    expr,
                    &schema,
                    page_meta.min_rowid,
                    page_meta.max_rowid,
                    page_meta.row_count,
                    &page_meta.zone_maps,
                    &page_meta.blooms,
                );
                if !zm_ok {
                    metrics.pruned_by_zonemap += 1;
                    continue;
                }
                if !bloom_ok {
                    metrics.pruned_by_bloom += 1;
                    continue;
                }
            }
            candidate_indices.push(p_i);
        }

        metrics.pages_scanned = candidate_indices.len();
        let mut group_table = VectorGroupTable::new(group_col, agg_kinds.to_vec());

        // Parallel vectorized scan + partial aggregation across worker pool!
        let num_workers = self.pool.worker_count();
        if candidate_indices.len() >= 4 && num_workers > 1 {
            let eff_workers = num_workers.min(candidate_indices.len());
            metrics.parallel_workers = eff_workers;
            let path = self.path.clone();
            let pages_ref = &pages;
            let cand_ref = &candidate_indices;
            let schema_ref = &schema;

            let partials: Vec<Result<VectorGroupTable>> =
                self.pool.execute_shards(eff_workers, |shard_id, total_shards| {
                    let mut local = VectorGroupTable::new(group_col, agg_kinds.to_vec());
                    let chunk = (cand_ref.len() + total_shards - 1) / total_shards;
                    let start = shard_id * chunk;
                    if start >= cand_ref.len() {
                        return Ok(local);
                    }
                    let end = (start + chunk).min(cand_ref.len());
                    let mut f = File::open(&path)?;
                    for &p_i in &cand_ref[start..end] {
                        let page_meta = &pages_ref[p_i];
                        let batch = read_pax_batch_from_file(
                            &mut f,
                            schema_ref,
                            page_meta,
                            needed_cols,
                            false,
                        )?;
                        local.ingest_batch(&batch, &page_meta.deleted_slots, vec_filter);
                    }
                    Ok(local)
                });
            for part_res in partials {
                group_table.merge_from(part_res?);
            }
        } else {
            for &p_i in &candidate_indices {
                let page_meta = &pages[p_i];
                let batch = self
                    .storage
                    .read_pax_batch(&schema, page_meta, needed_cols, false)?;
                group_table.ingest_batch(&batch, &page_meta.deleted_slots, vec_filter);
            }
        }

        // Also ingest any unflushed hot-buffer rows
        if let Some(hot) = self.hot_buffers.get(&key) {
            if !hot.is_empty() {
                let n = hot.len();
                let mut cols: Vec<Option<ColumnVector>> = vec![None; schema.columns.len()];
                for (c_idx, col_def) in schema.columns.iter().enumerate() {
                    if needed_cols[c_idx] {
                        let vals: Vec<Value> = hot.iter().map(|r| r.values[c_idx].clone()).collect();
                        cols[c_idx] = Some(ColumnVector::from_values(col_def.col_type, vals));
                    }
                }
                let hot_batch = ColumnBatch {
                    row_count: n,
                    rowids: hot.iter().map(|r| r.rowid).collect(),
                    columns: cols,
                };
                group_table.ingest_batch(&hot_batch, &[], vec_filter);
            }
        }

        Ok((group_table.into_groups(), metrics))
    }

    /// Ultra-fast zero-clone indexed point lookup (supports both Hash and B-Tree indexes!).
    pub fn fast_indexed_point_lookup(
        &mut self,
        table: &str,
        filter_col_idx: usize,
        target_val: &Value,
        proj_cols: &[Option<usize>],
    ) -> Result<Option<(Vec<Vec<Value>>, ScanMetrics)>> {
        let t_idx = match self.find_table_idx(table) {
            Some(i) => i,
            None => return Err(Error::Schema(format!("No such table: '{}'", table))),
        };
        let table_name_matches = self.catalog.tables[t_idx].schema.name.clone();
        let col_name = self.catalog.tables[t_idx].schema.columns[filter_col_idx]
            .name
            .clone();

        // Find any index where `col_name` is the sole column (Hash) or leading column (BTree)
        let ix_idx = match self.catalog.indexes.iter().position(|i| {
            i.table_name.eq_ignore_ascii_case(&table_name_matches)
                && i.columns
                    .first()
                    .map(|c| c.eq_ignore_ascii_case(&col_name))
                    .unwrap_or(false)
                && (i.kind == IndexKind::BTree || i.columns.len() == 1)
        }) {
            Some(i) => i,
            None => return Ok(None),
        };

        let total_pages = self.catalog.tables[t_idx].pages.len();
        let index_name = self.catalog.indexes[ix_idx].name.clone();
        let index_kind = self.catalog.indexes[ix_idx].kind;

        if target_val.is_null() {
            return Ok(Some((
                Vec::new(),
                ScanMetrics {
                    total_pages,
                    pruned_by_index: total_pages,
                    index_used: Some(index_name),
                    ..Default::default()
                },
            )));
        }

        let mut metrics = ScanMetrics {
            total_pages,
            index_used: Some(index_name),
            ..Default::default()
        };

        let mut matched_locs: Vec<(u64, u32)> = Vec::new();
        let needs_hash_recheck = match index_kind {
            IndexKind::Hash => {
                let (by_page, bread) =
                    self.lookup_hash_index(ix_idx, &[vec![target_val.clone()]])?;
                metrics.index_blocks_read += bread;
                for (pid, slots) in by_page {
                    for slot in slots {
                        matched_locs.push((pid, slot));
                    }
                }
                true
            }
            IndexKind::BTree => {
                let spec = BTreeScanSpec {
                    prefix_eq: vec![target_val.clone()],
                    range_lo: RangeBound::Unbounded,
                    range_hi: RangeBound::Unbounded,
                };
                let (by_page, bread) = self.lookup_btree_index(ix_idx, &[spec])?;
                metrics.index_blocks_read += bread;
                for (pid, slots) in by_page {
                    for slot in slots {
                        matched_locs.push((pid, slot));
                    }
                }
                false // B-tree stores exact keys: zero hash-collision recheck needed!
            }
        };
        matched_locs.sort_unstable();
        matched_locs.dedup();

        let mut unique_pids: Vec<u64> = matched_locs.iter().map(|&(p, _)| p).collect();
        unique_pids.dedup();
        metrics.pages_scanned = unique_pids.len();
        metrics.pruned_by_index = total_pages.saturating_sub(unique_pids.len());

        let int_pk_col = self.catalog.tables[t_idx].schema.integer_pk_col();
        let col_key_for = |opt_c: Option<usize>| -> u32 {
            match opt_c {
                None => u32::MAX,
                Some(c) if Some(c) == int_pk_col => u32::MAX,
                Some(c) => c as u32,
            }
        };

        let mut out_rows: Vec<Vec<Value>> = Vec::new();
        let filter_ck = col_key_for(Some(filter_col_idx));

        for (pid, slot) in matched_locs {
            let p_pos = match self.catalog.tables[t_idx]
                .pages
                .iter()
                .position(|p| p.page_id == pid)
            {
                Some(p) => p,
                None => continue,
            };
            if self.catalog.tables[t_idx].pages[p_pos].is_deleted(slot) {
                continue;
            }

            if needs_hash_recheck {
                let filter_hit = self.cache.get_col(pid, filter_ck).is_some();
                if filter_hit {
                    metrics.cache_hits += 1;
                } else {
                    let cv = self.storage.read_pax_column(
                        &self.catalog.tables[t_idx].schema,
                        &self.catalog.tables[t_idx].pages[p_pos],
                        Some(filter_col_idx),
                    )?;
                    self.cache.insert_col(pid, filter_ck, cv);
                }

                let matched = self
                    .cache
                    .get_col(pid, filter_ck)
                    .map(|cv| cv.eq_value_at(slot as usize, target_val))
                    .unwrap_or(false);
                if !matched {
                    continue;
                }
            }

            let mut row_vals = Vec::with_capacity(proj_cols.len());
            let mut counted_hit = false;
            for &pc in proj_cols {
                let ck = col_key_for(pc);
                if self.cache.get_col(pid, ck).is_some() {
                    if !needs_hash_recheck && !counted_hit {
                        metrics.cache_hits += 1;
                        counted_hit = true;
                    }
                } else {
                    let cv = self.storage.read_pax_column(
                        &self.catalog.tables[t_idx].schema,
                        &self.catalog.tables[t_idx].pages[p_pos],
                        pc,
                    )?;
                    self.cache.insert_col(pid, ck, cv);
                }
                let v = self
                    .cache
                    .get_col(pid, ck)
                    .unwrap()
                    .get_value(slot as usize);
                row_vals.push(v);
            }
            out_rows.push(row_vals);
        }

        let key = table_name_matches.to_ascii_lowercase();
        if let Some(hot) = self.hot_buffers.get(&key) {
            for r in hot {
                if r.values[filter_col_idx].sql_cmp(target_val).is_eq() {
                    let mut row_vals = Vec::with_capacity(proj_cols.len());
                    for &pc in proj_cols {
                        match pc {
                            None => row_vals.push(Value::Integer(r.rowid)),
                            Some(ci) => row_vals.push(r.values[ci].clone()),
                        }
                    }
                    out_rows.push(row_vals);
                }
            }
        }

        Ok(Some((out_rows, metrics)))
    }

    pub fn scan_filtered_with_metrics(
        &mut self,
        table: &str,
        where_clause: Option<&Expr>,
        needed_cols: Option<&[bool]>,
    ) -> Result<(Vec<Row>, ScanMetrics)> {
        self.scan_filtered_with_alias(table, None, where_clause, needed_cols)
    }

    pub fn scan_filtered_with_alias(
        &mut self,
        table: &str,
        alias: Option<&str>,
        where_clause: Option<&Expr>,
        needed_cols: Option<&[bool]>,
    ) -> Result<(Vec<Row>, ScanMetrics)> {
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;
        let schema = self.catalog.tables[idx].schema.clone();
        let pages = self.catalog.tables[idx].pages.clone();
        let key = schema.name.to_ascii_lowercase();

        let mut metrics = ScanMetrics {
            total_pages: pages.len(),
            ..Default::default()
        };

        let eff_alias = alias.unwrap_or(&schema.name);
        let eval_schema = EvalSchema::from_single(eff_alias, &schema);

        // Step 1: Composite & Range B-Tree / Hash Secondary Index lookup!
        let mut allowed_locs: Option<HashMap<u64, Vec<u32>>> = None;
        if let Some(expr) = where_clause {
            let preds = extract_column_predicates(expr);
            if let Some((ix_idx, plan)) = self.match_index_plan(&schema.name, &preds) {
                let ix_name = self.catalog.indexes[ix_idx].name.clone();
                let (matched_locs, b_read) = match plan {
                    IndexPlan::Hash(tuples) => self.lookup_hash_index(ix_idx, &tuples)?,
                    IndexPlan::BTree(specs) => self.lookup_btree_index(ix_idx, &specs)?,
                };
                metrics.index_blocks_read += b_read;
                metrics.index_used = Some(ix_name);
                allowed_locs = Some(matched_locs);
            }
        }

        let mut candidate_pages: Vec<(usize, Option<&[u32]>)> = Vec::new();
        for (p_i, page_meta) in pages.iter().enumerate() {
            if page_meta.live_rows() == 0 {
                continue;
            }
            let slot_filter: Option<&[u32]> = if let Some(ref locs) = allowed_locs {
                match locs.get(&page_meta.page_id) {
                    Some(slots) => Some(slots.as_slice()),
                    None => {
                        metrics.pruned_by_index += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            if let Some(expr) = where_clause {
                let (zm_ok, bloom_ok) = page_might_match(
                    expr,
                    &schema,
                    page_meta.min_rowid,
                    page_meta.max_rowid,
                    page_meta.row_count,
                    &page_meta.zone_maps,
                    &page_meta.blooms,
                );
                if !zm_ok {
                    metrics.pruned_by_zonemap += 1;
                    continue;
                }
                if !bloom_ok {
                    metrics.pruned_by_bloom += 1;
                    continue;
                }
            }
            candidate_pages.push((p_i, slot_filter));
        }

        let use_cache_fill = where_clause.is_some()
            && (allowed_locs.is_some()
                || candidate_pages.len() * 2 < pages.len().max(1)
                || pages.len() <= 2);

        let mut results = Vec::new();

        // Parallel Sequential Scan & Filter when scanning >= 4 pages without cache-fill!
        let num_workers = self.pool.worker_count();
        if candidate_pages.len() >= 4
            && num_workers > 1
            && !use_cache_fill
            && allowed_locs.is_none()
        {
            let eff_workers = num_workers.min(candidate_pages.len());
            metrics.parallel_workers = eff_workers;
            metrics.pages_scanned = candidate_pages.len();
            let path = self.path.clone();
            let pages_ref = &pages;
            let cand_ref = &candidate_pages;
            let schema_ref = &schema;
            let es_ref = &eval_schema;

            let shard_results: Vec<Result<Vec<Row>>> =
                self.pool.execute_shards(eff_workers, |shard_id, total_shards| {
                    let chunk = (cand_ref.len() + total_shards - 1) / total_shards;
                    let start = shard_id * chunk;
                    if start >= cand_ref.len() {
                        return Ok(Vec::new());
                    }
                    let end = (start + chunk).min(cand_ref.len());
                    let mut f = File::open(&path)?;
                    let mut local = Vec::new();
                    for &(p_i, _) in &cand_ref[start..end] {
                        let page_meta = &pages_ref[p_i];
                        let page_rows =
                            read_pax_page_from_file(&mut f, schema_ref, page_meta, needed_cols)?;
                        for (slot, row) in page_rows.into_iter().enumerate() {
                            if page_meta.is_deleted(slot as u32) {
                                continue;
                            }
                            if let Some(expr) = where_clause {
                                if eval_bool_fast(expr, es_ref, &row)? != Some(true) {
                                    continue;
                                }
                            }
                            local.push(row);
                        }
                    }
                    Ok(local)
                });
            for sr in shard_results {
                results.extend(sr?);
            }
        } else {
            for (p_i, slot_filter) in candidate_pages {
                let page_meta = &pages[p_i];
                metrics.pages_scanned += 1;

                if let Some(cached_rows) = self.cache.get(page_meta.page_id) {
                    metrics.cache_hits += 1;
                    if let Some(slots) = slot_filter {
                        for &slot in slots {
                            if page_meta.is_deleted(slot) {
                                continue;
                            }
                            if let Some(row) = cached_rows.get(slot as usize) {
                                if let Some(expr) = where_clause {
                                    if eval_bool_fast(expr, &eval_schema, row)? != Some(true) {
                                        continue;
                                    }
                                }
                                results.push(row.clone());
                            }
                        }
                    } else {
                        for (slot, row) in cached_rows.iter().enumerate() {
                            if page_meta.is_deleted(slot as u32) {
                                continue;
                            }
                            if let Some(expr) = where_clause {
                                if eval_bool_fast(expr, &eval_schema, row)? != Some(true) {
                                    continue;
                                }
                            }
                            results.push(row.clone());
                        }
                    }
                    continue;
                }

                let page_rows = if use_cache_fill {
                    let full = self.storage.read_pax_page(&schema, page_meta, None)?;
                    self.cache.insert(page_meta.page_id, full.clone());
                    full
                } else {
                    self.storage.read_pax_page(&schema, page_meta, needed_cols)?
                };

                if let Some(slots) = slot_filter {
                    for &slot in slots {
                        if page_meta.is_deleted(slot) {
                            continue;
                        }
                        if let Some(row) = page_rows.get(slot as usize) {
                            if let Some(expr) = where_clause {
                                if eval_bool_fast(expr, &eval_schema, row)? != Some(true) {
                                    continue;
                                }
                            }
                            results.push(row.clone());
                        }
                    }
                } else {
                    for (slot, row) in page_rows.into_iter().enumerate() {
                        if page_meta.is_deleted(slot as u32) {
                            continue;
                        }
                        if let Some(expr) = where_clause {
                            if eval_bool_fast(expr, &eval_schema, &row)? != Some(true) {
                                continue;
                            }
                        }
                        results.push(row);
                    }
                }
            }
        }

        if let Some(hot) = self.hot_buffers.get(&key) {
            for row in hot {
                if let Some(expr) = where_clause {
                    if eval_bool_fast(expr, &eval_schema, row)? != Some(true) {
                        continue;
                    }
                }
                results.push(row.clone());
            }
        }

        Ok((results, metrics))
    }

    fn mark_rowids_deleted_in_memory(
        &mut self,
        table: &str,
        target_rowids: &HashSet<i64>,
    ) -> Result<usize> {
        if target_rowids.is_empty() {
            return Ok(0);
        }
        let idx = match self.find_table_idx(table) {
            Some(i) => i,
            None => return Ok(0),
        };
        let schema = self.catalog.tables[idx].schema.clone();
        let key = schema.name.to_ascii_lowercase();
        let mut deleted_count = 0usize;

        if let Some(hot) = self.hot_buffers.get_mut(&key) {
            let before = hot.len();
            hot.retain(|r| !target_rowids.contains(&r.rowid));
            deleted_count += before - hot.len();
        }

        let no_cols = vec![false; schema.columns.len()];
        let num_pages = self.catalog.tables[idx].pages.len();
        for p_idx in 0..num_pages {
            let p_meta = self.catalog.tables[idx].pages[p_idx].clone();
            if p_meta.live_rows() == 0 {
                continue;
            }
            let any_in_range = target_rowids
                .iter()
                .any(|&id| id >= p_meta.min_rowid && id <= p_meta.max_rowid);
            if !any_in_range {
                continue;
            }
            let rows = self
                .storage
                .read_pax_page(&schema, &p_meta, Some(&no_cols))?;
            let pm = &mut self.catalog.tables[idx].pages[p_idx];
            for (slot, r) in rows.iter().enumerate() {
                if target_rowids.contains(&r.rowid) && pm.mark_deleted(slot as u32) {
                    deleted_count += 1;
                }
            }
        }
        Ok(deleted_count)
    }

    pub fn delete_where(&mut self, table: &str, where_clause: Option<&Expr>) -> Result<usize> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;
        let canonical = self.catalog.tables[idx].schema.name.clone();

        let matched = self.scan_filtered(table, where_clause, None)?;
        if matched.is_empty() {
            return Ok(0);
        }

        let has_refs = self.foreign_keys_enabled && !self.find_referencing_fks(&canonical).is_empty();
        if has_refs {
            // Use internal savepoint so any mid-cascade FK error rolls back atomically!
            self.savepoint("__fk_delete_guard")?;
            let res: Result<usize> = (|| {
                let mut visited: HashSet<(String, i64)> = matched
                    .iter()
                    .map(|r| (canonical.to_ascii_lowercase(), r.rowid))
                    .collect();
                let rowids: Vec<i64> = matched.iter().map(|r| r.rowid).collect();
                let set: HashSet<i64> = rowids.iter().copied().collect();
                let count = self.mark_rowids_deleted_in_memory(&canonical, &set)?;
                self.record_wal_op(WalOp::DeleteRowIds {
                    table_name: canonical.clone(),
                    rowids,
                })?;
                self.enforce_parent_delete_cascades(&canonical, &matched, &mut visited)?;
                Ok(count)
            })();
            match res {
                Ok(count) => {
                    self.release_savepoint("__fk_delete_guard")?;
                    return Ok(count);
                }
                Err(e) => {
                    let _ = self.rollback_to_savepoint("__fk_delete_guard");
                    let _ = self.release_savepoint("__fk_delete_guard");
                    return Err(e);
                }
            }
        }

        let rowids: Vec<i64> = matched.iter().map(|r| r.rowid).collect();
        let set: HashSet<i64> = rowids.iter().copied().collect();
        let count = self.mark_rowids_deleted_in_memory(&canonical, &set)?;

        self.record_wal_op(WalOp::DeleteRowIds {
            table_name: canonical,
            rowids,
        })?;
        Ok(count)
    }

    pub fn update_where(
        &mut self,
        table: &str,
        assignments: &[(String, Expr)],
        where_clause: Option<&Expr>,
    ) -> Result<usize> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(table)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?;
        let schema = self.catalog.tables[idx].schema.clone();
        let canonical = schema.name.clone();
        let key = canonical.to_ascii_lowercase();

        let mut assign_indices = Vec::with_capacity(assignments.len());
        for (col_name, expr) in assignments {
            let c_idx = schema
                .col_index(col_name)
                .ok_or_else(|| Error::Schema(format!("Unknown column '{}'", col_name)))?;
            assign_indices.push((c_idx, expr));
        }

        let matched = self.scan_filtered(table, where_clause, None)?;
        if matched.is_empty() {
            return Ok(0);
        }

        let pk_col = schema.integer_pk_col();
        let mut next_rowid = schema.next_rowid;
        let mut rowid_changed = false;
        let mut updated_rows = Vec::with_capacity(matched.len());
        let mut changed_pairs = Vec::with_capacity(matched.len());
        let mut old_ids = HashSet::with_capacity(matched.len());
        let mut old_ids_vec = Vec::with_capacity(matched.len());
        for mut row in matched {
            old_ids.insert(row.rowid);
            old_ids_vec.push(row.rowid);
            let orig = row.clone();
            for &(c_idx, expr) in &assign_indices {
                let new_val = eval_expr(expr, &schema, Some(&orig))?;
                let col_def = &schema.columns[c_idx];
                let coerced = new_val.coerce_for_column(col_def.col_type)?;
                if (col_def.not_null || col_def.primary_key) && coerced.is_null() {
                    return Err(Error::InvalidData(format!(
                        "NOT NULL constraint failed on '{}.{}'",
                        schema.name, col_def.name
                    )));
                }
                row.values[c_idx] = coerced;
            }
            if let Some(pk_i) = pk_col {
                if let Value::Integer(new_pk) = row.values[pk_i] {
                    if new_pk != row.rowid {
                        row.rowid = new_pk;
                        rowid_changed = true;
                    }
                    if new_pk >= next_rowid {
                        next_rowid = new_pk.saturating_add(1);
                    }
                }
            }
            changed_pairs.push((orig, row.clone()));
            updated_rows.push(row);
        }

        self.check_row_constraints(&schema, &updated_rows)?;
        self.check_unique_constraints(&canonical, &schema, &updated_rows, Some(&old_ids))?;
        self.check_foreign_keys_on_write(&schema, &updated_rows)?;

        let has_refs = self.foreign_keys_enabled && !self.find_referencing_fks(&canonical).is_empty();
        if has_refs {
            self.savepoint("__fk_update_guard")?;
            let res: Result<usize> = (|| {
                let count = updated_rows.len();
                self.catalog.tables[idx].schema.next_rowid = next_rowid;
                self.mark_rowids_deleted_in_memory(&canonical, &old_ids)?;
                self.hot_buffers
                    .entry(key.clone())
                    .or_default()
                    .extend(updated_rows.clone());
                if rowid_changed {
                    self.record_wal_ops(vec![
                        WalOp::DeleteRowIds {
                            table_name: canonical.clone(),
                            rowids: old_ids_vec.clone(),
                        },
                        WalOp::InsertRows {
                            table_name: canonical.clone(),
                            rows: updated_rows.clone(),
                        },
                    ])?;
                } else {
                    self.record_wal_op(WalOp::UpdateRows {
                        table_name: canonical.clone(),
                        rows: updated_rows.clone(),
                    })?;
                }
                self.enforce_parent_update_cascades(&canonical, &changed_pairs)?;
                Ok(count)
            })();
            match res {
                Ok(count) => {
                    self.release_savepoint("__fk_update_guard")?;
                    return Ok(count);
                }
                Err(e) => {
                    let _ = self.rollback_to_savepoint("__fk_update_guard");
                    let _ = self.release_savepoint("__fk_update_guard");
                    return Err(e);
                }
            }
        }

        let count = updated_rows.len();
        self.catalog.tables[idx].schema.next_rowid = next_rowid;
        self.mark_rowids_deleted_in_memory(&canonical, &old_ids)?;
        self.hot_buffers
            .entry(key)
            .or_default()
            .extend(updated_rows.clone());

        if rowid_changed {
            self.record_wal_ops(vec![
                WalOp::DeleteRowIds {
                    table_name: canonical.clone(),
                    rowids: old_ids_vec,
                },
                WalOp::InsertRows {
                    table_name: canonical,
                    rows: updated_rows,
                },
            ])?;
        } else {
            self.record_wal_op(WalOp::UpdateRows {
                table_name: canonical,
                rows: updated_rows,
            })?;
        }
        Ok(count)
    }

    pub fn backup_to<P: AsRef<Path>>(&mut self, dest: P) -> Result<()> {
        if !self.read_only && !self.mvcc_sandbox && self.active_tx.is_none() && self.dirty {
            self.checkpoint()?;
        }
        let dest_path = dest.as_ref();
        let tmp_path = {
            let mut os = dest_path.as_os_str().to_os_string();
            os.push(".backup_tmp");
            PathBuf::from(os)
        };
        if let Err(e) = std::fs::copy(&self.path, &tmp_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(Error::Io(e));
        }
        if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp_path) {
            let _ = f.sync_all();
        }
        if let Err(e) = atomic_replace_file(&tmp_path, dest_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(Error::Io(e));
        }
        Ok(())
    }

    // ========================================================================
    // Checkpoint & Vacuum (Parallel PAX Page Compression + Paged B-Tree/Hash Compaction)
    // ========================================================================

    pub fn checkpoint(&mut self) -> Result<()> {
        self.require_writable()?;
        if self.active_tx.is_some() {
            return Err(Error::Sql(
                "Cannot checkpoint while a transaction is active".into(),
            ));
        }
        let page_rows = self.options.page_rows;
        let profile = self.options.compression;

        for t_idx in 0..self.catalog.tables.len() {
            let schema = self.catalog.tables[t_idx].schema.clone();
            let key = schema.name.to_ascii_lowercase();
            let mut pending = self.hot_buffers.remove(&key).unwrap_or_default();
            let mut removed_pids = HashSet::new();

            if !pending.is_empty() {
                if let Some(last) = self.catalog.tables[t_idx].pages.last().cloned() {
                    if (last.live_rows() as usize) < page_rows / 2 {
                        let old_rows = self.storage.read_pax_page(&schema, &last, None)?;
                        let mut merged: Vec<Row> = old_rows
                            .into_iter()
                            .enumerate()
                            .filter(|(slot, _)| !last.is_deleted(*slot as u32))
                            .map(|(_, r)| r)
                            .collect();
                        merged.extend(pending);
                        pending = merged;
                        removed_pids.insert(last.page_id);
                        self.cache.invalidate(last.page_id);
                        self.catalog.tables[t_idx].pages.pop();
                    }
                }
            }

            self.catalog.tables[t_idx].pages.retain(|p| {
                if p.live_rows() == 0 {
                    removed_pids.insert(p.page_id);
                    self.cache.invalidate(p.page_id);
                    false
                } else {
                    true
                }
            });

            if !removed_pids.is_empty() {
                for ix in &mut self.catalog.indexes {
                    if ix.table_name.eq_ignore_ascii_case(&schema.name) {
                        ix.remove_pages(&removed_pids);
                    }
                }
            }

            if !pending.is_empty() {
                if !schema.cluster_by.is_empty() {
                    sort_rows_by_cluster(&schema, &mut pending);
                }
                let chunks: Vec<&[Row]> = pending.chunks(page_rows).collect();
                if chunks.len() >= 2 && self.pool.worker_count() > 1 {
                    let encoded: Vec<Result<_>> = self
                        .pool
                        .execute_indexed(&chunks, |_, chunk| {
                            encode_pax_page(&schema, chunk, profile)
                        });
                    for (chunk, enc_res) in chunks.into_iter().zip(encoded) {
                        let meta = self.storage.write_encoded_pax_page(enc_res?)?;
                        let pid = meta.page_id;
                        self.catalog.tables[t_idx].pages.push(meta);
                        self.update_indexes_for_new_page(&schema, pid, chunk);
                    }
                } else {
                    for chunk in chunks {
                        let meta = self.storage.write_pax_page(&schema, chunk, profile)?;
                        let pid = meta.page_id;
                        self.catalog.tables[t_idx].pages.push(meta);
                        self.update_indexes_for_new_page(&schema, pid, chunk);
                    }
                }
            }
        }

        self.flush_dirty_indexes()?;
        self.storage.commit_catalog(&self.catalog, profile)?;
        self.wal.truncate()?;
        self.dirty = false;
        Ok(())
    }

    pub fn vacuum(&mut self) -> Result<()> {
        self.require_writable()?;
        if self.active_tx.is_some() {
            return Err(Error::Sql(
                "Cannot VACUUM while a transaction is active".into(),
            ));
        }
        let tmp_path = {
            let mut os = self.path.as_os_str().to_os_string();
            os.push(".vacuum");
            PathBuf::from(os)
        };
        let _ = std::fs::remove_file(&tmp_path);

        let (mut new_storage, mut new_catalog) = StorageFile::open(&tmp_path)?;
        new_storage.superblock.next_table_id = self.storage.superblock.next_table_id;

        let page_rows = self.options.page_rows;
        let profile = self.options.compression;

        let mut new_indexes: Vec<IndexEntry> = self
            .catalog
            .indexes
            .iter()
            .map(|i| {
                IndexEntry::new_composite(
                    i.name.clone(),
                    i.table_name.clone(),
                    i.columns.clone(),
                    i.desc.clone(),
                    i.kind,
                    i.unique,
                )
            })
            .collect();

        for t_idx in 0..self.catalog.tables.len() {
            let schema = self.catalog.tables[t_idx].schema.clone();
            let mut live_rows = self.scan_all(&schema.name)?;
            sort_rows_by_cluster(&schema, &mut live_rows);

            let chunks: Vec<&[Row]> = live_rows.chunks(page_rows).collect();
            let mut new_pages = Vec::with_capacity(chunks.len());

            if chunks.len() >= 2 && self.pool.worker_count() > 1 {
                let encoded: Vec<Result<_>> = self
                    .pool
                    .execute_indexed(&chunks, |_, chunk| {
                        encode_pax_page(&schema, chunk, profile)
                    });
                for (chunk, enc_res) in chunks.into_iter().zip(encoded) {
                    let meta = new_storage.write_encoded_pax_page(enc_res?)?;
                    let pid = meta.page_id;
                    for ix in &mut new_indexes {
                        if ix.table_name.eq_ignore_ascii_case(&schema.name) {
                            let cis: Option<Vec<usize>> =
                                ix.columns.iter().map(|c| schema.col_index(c)).collect();
                            if let Some(cis) = cis {
                                let tuples: Vec<Vec<Value>> = chunk
                                    .iter()
                                    .map(|r| cis.iter().map(|&ci| r.values[ci].clone()).collect())
                                    .collect();
                                ix.index_page_tuples(pid, tuples);
                            }
                        }
                    }
                    new_pages.push(meta);
                }
            } else {
                for chunk in chunks {
                    let meta = new_storage.write_pax_page(&schema, chunk, profile)?;
                    let pid = meta.page_id;
                    for ix in &mut new_indexes {
                        if ix.table_name.eq_ignore_ascii_case(&schema.name) {
                            let cis: Option<Vec<usize>> =
                                ix.columns.iter().map(|c| schema.col_index(c)).collect();
                            if let Some(cis) = cis {
                                let tuples: Vec<Vec<Value>> = chunk
                                    .iter()
                                    .map(|r| cis.iter().map(|&ci| r.values[ci].clone()).collect())
                                    .collect();
                                ix.index_page_tuples(pid, tuples);
                            }
                        }
                    }
                    new_pages.push(meta);
                }
            }

            new_catalog.tables.push(TableEntry {
                schema,
                pages: new_pages,
            });
        }

        // Write clean sorted on-disk index leaf blocks into the fresh compacted file
        for ix in &mut new_indexes {
            match ix.kind {
                IndexKind::Hash => {
                    let entries = std::mem::take(&mut ix.pending_entries);
                    let mut blocks = Vec::new();
                    for chunk in entries.chunks(INDEX_BLOCK_ENTRIES) {
                        blocks.push(new_storage.write_index_block(chunk, profile)?);
                    }
                    ix.blocks = blocks;
                }
                IndexKind::BTree => {
                    let entries = std::mem::take(&mut ix.pending_btree);
                    let key_cols = ix.columns.len();
                    let mut blocks = Vec::new();
                    for chunk in entries.chunks(INDEX_BLOCK_ENTRIES) {
                        blocks.push(
                            new_storage.write_btree_index_block(chunk, key_cols, profile)?,
                        );
                    }
                    ix.btree_blocks = blocks;
                }
            }
        }
        new_catalog.indexes = new_indexes;

        new_storage.commit_catalog(&new_catalog, profile)?;
        drop(new_storage);

        let (tmp_storage, _) = StorageFile::open(&tmp_path)?;
        let old_storage = std::mem::replace(&mut self.storage, tmp_storage);
        drop(old_storage);

        std::fs::copy(&tmp_path, &self.path)?;
        let (final_storage, final_catalog) = StorageFile::open(&self.path)?;
        let tmp_handle = std::mem::replace(&mut self.storage, final_storage);
        drop(tmp_handle);
        let _ = std::fs::remove_file(&tmp_path);

        self.catalog = final_catalog;
        self.hot_buffers.clear();
        self.cache.clear();
        self.index_cache.clear();
        self.wal.truncate()?;
        self.dirty = false;
        Ok(())
    }

    pub fn stats(&self) -> Result<DbStats> {
        let mut tables = Vec::with_capacity(self.catalog.tables.len());
        for t in &self.catalog.tables {
            let key = t.schema.name.to_ascii_lowercase();
            let hot_rows = self
                .hot_buffers
                .get(&key)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            let live_page_rows: u64 = t.pages.iter().map(|p| p.live_rows() as u64).sum();
            let deleted_slots: u64 = t.pages.iter().map(|p| p.deleted_slots.len() as u64).sum();
            let raw_pax_bytes: u64 = t.pages.iter().map(|p| p.raw_pax_len as u64).sum();
            let stored_pax_bytes: u64 = t.pages.iter().map(|p| p.stored_len as u64).sum();
            tables.push(TableStats {
                name: t.schema.name.clone(),
                live_rows: live_page_rows + hot_rows,
                deleted_slots,
                hot_buffer_rows: hot_rows,
                page_count: t.pages.len(),
                raw_pax_bytes,
                stored_pax_bytes,
                cluster_by: t.schema.cluster_by.clone(),
            });
        }
        let index_disk_bytes: u64 = self
            .catalog
            .indexes
            .iter()
            .map(|i| i.total_disk_bytes())
            .sum();
        Ok(DbStats {
            db_file_bytes: self.storage.file_len()?,
            wal_file_bytes: self.wal.file_len(),
            catalog_stored_bytes: self.storage.superblock.catalog_stored_len as u64,
            catalog_raw_bytes: self.storage.superblock.catalog_raw_len as u64,
            generation: self.storage.superblock.generation,
            index_count: self.catalog.indexes.len(),
            index_disk_bytes,
            cache_hits: self.cache.hits,
            cache_misses: self.cache.misses,
            index_cache_hits: self.index_cache.hits,
            index_cache_misses: self.index_cache.misses,
            read_only: self.read_only,
            tables,
        })
    }
}

enum IndexPlan {
    Hash(Vec<Vec<Value>>),
    BTree(Vec<BTreeScanSpec>),
}

impl Drop for Database {
    fn drop(&mut self) {
        if !self.read_only && !self.mvcc_sandbox && self.active_tx.is_none() && self.dirty {
            let _ = self.checkpoint();
        }
        if !self.mvcc_sandbox {
            self.wal.remove_if_empty();
        }
    }
}

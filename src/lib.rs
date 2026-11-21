pub mod cache;
pub mod encoding;
pub mod index;
pub mod lock;
pub mod pax;
pub mod sql;
pub mod storage;
pub mod types;
pub mod vector;
pub mod wal;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::cache::PageCache;
use crate::index::{hash_value, IndexBlockCache, IndexEntry, INDEX_BLOCK_ENTRIES};
use crate::lock::{atomic_replace_file, OsFileLock};
use crate::pax::{CompressionProfile, DEFAULT_PAGE_ROWS};
use crate::sql::{
    eval_bool_fast, eval_expr, execute_statement, extract_index_candidates, page_might_match,
    parse_sql, EvalSchema, Expr,
};
use crate::storage::{Catalog, StorageFile, TableEntry};
use crate::types::{ColumnDef, Error, Result, Row, TableSchema, Value};
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
}

struct TxState {
    snapshot_catalog: Catalog,
    snapshot_hot: HashMap<String, Vec<Row>>,
    pending_wal_ops: Vec<WalOp>,
}

pub struct Database {
    path: PathBuf,
    _lock: OsFileLock,
    read_only: bool,
    storage: StorageFile,
    wal: WalFile,
    catalog: Catalog,
    hot_buffers: HashMap<String, Vec<Row>>,
    cache: PageCache,
    index_cache: IndexBlockCache,
    options: DbOptions,
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
    /// Multiple processes/handles can open read-only concurrently without blocking each other!
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let lock = OsFileLock::try_acquire(&path_buf, false)?;
        let (storage, catalog) = StorageFile::open_readonly(&path_buf)?;
        let mut wal = WalFile::open_readonly(&path_buf)?;
        let frames = wal.read_frames(storage.superblock.generation)?;
        let options = DbOptions::default();

        let mut db = Database {
            path: path_buf,
            _lock: lock,
            read_only: true,
            storage,
            wal,
            catalog,
            hot_buffers: HashMap::new(),
            cache: PageCache::new(options.cache_pages),
            index_cache: IndexBlockCache::new(options.index_cache_blocks),
            options,
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

        let mut db = Database {
            path: path_buf,
            _lock: lock,
            read_only: false,
            storage,
            wal,
            catalog,
            hot_buffers: HashMap::new(),
            cache: PageCache::new(cache_cap),
            index_cache: IndexBlockCache::new(idx_cap),
            options,
            active_tx: None,
            dirty: false,
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

    fn require_writable(&self) -> Result<()> {
        if self.read_only {
            Err(Error::Sql("Database is opened read-only".into()))
        } else {
            Ok(())
        }
    }

    fn apply_wal_op_in_memory(&mut self, op: WalOp) -> Result<()> {
        match op {
            WalOp::CreateTable(schema) => {
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
                    let _ = self.build_index_in_memory(&name, &table_name, &column_name);
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

    // ========================================================================
    // Transactions
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
        });
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let tx = self
            .active_tx
            .take()
            .ok_or_else(|| Error::Sql("No active transaction".into()))?;
        if !tx.pending_wal_ops.is_empty() {
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
        } else {
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
        if self.active_tx.is_none() && self.total_hot_rows() >= self.options.auto_checkpoint_rows {
            self.checkpoint()?;
        }
        Ok(())
    }

    // ========================================================================
    // DDL: Tables, Schema Evolution, Clustering, Paged Secondary Indexes
    // ========================================================================

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        self.create_table_clustered(name, columns, Vec::new())
    }

    pub fn create_table_clustered(
        &mut self,
        name: &str,
        columns: Vec<ColumnDef>,
        cluster_by: Vec<String>,
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
        let id = self.storage.superblock.next_table_id;
        self.storage.superblock.next_table_id += 1;
        let schema = TableSchema {
            id,
            name: name.to_string(),
            columns,
            next_rowid: 1,
            cluster_by,
        };
        self.catalog.tables.push(TableEntry {
            schema: schema.clone(),
            pages: Vec::new(),
        });
        self.record_wal_op(WalOp::CreateTable(schema))?;
        Ok(())
    }

    /// Instant O(1) schema evolution (`ALTER TABLE tbl ADD COLUMN ...`) without rewriting existing PAX pages!
    pub fn add_column(&mut self, table_name: &str, mut column: ColumnDef) -> Result<()> {
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
        let canonical = self.catalog.tables[idx].schema.name.clone();
        let fill = column.default_value.clone().unwrap_or(Value::Null);
        let key = canonical.to_ascii_lowercase();
        self.catalog.tables[idx].schema.columns.push(column.clone());
        if let Some(hot) = self.hot_buffers.get_mut(&key) {
            for r in hot.iter_mut() {
                r.values.push(fill.clone());
            }
        }
        // Invalidate cached full pages of this table so they re-decode with the new column
        for p in &self.catalog.tables[idx].pages {
            self.cache.invalidate(p.page_id);
        }
        self.record_wal_op(WalOp::AddColumn {
            table_name: canonical,
            column,
        })?;
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

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.require_writable()?;
        let idx = self
            .find_table_idx(name)
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
        self.record_wal_op(WalOp::DropTable(canonical))?;
        Ok(())
    }

    fn build_index_in_memory(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<()> {
        let t_idx = self
            .find_table_idx(table_name)
            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table_name)))?;
        let schema = self.catalog.tables[t_idx].schema.clone();
        let col_idx = schema.col_index(column_name).ok_or_else(|| {
            Error::Schema(format!(
                "Unknown column '{}' on table '{}'",
                column_name, schema.name
            ))
        })?;
        let pages = self.catalog.tables[t_idx].pages.clone();

        let mut needed = vec![false; schema.columns.len()];
        needed[col_idx] = true;

        let mut tuples: Vec<(u64, u64, u32)> = Vec::new();
        for p in &pages {
            if p.live_rows() == 0 {
                continue;
            }
            let rows = self.storage.read_pax_page(&schema, p, Some(&needed))?;
            for (slot, row) in rows.iter().enumerate() {
                if p.is_deleted(slot as u32) {
                    continue;
                }
                if let Some(h) = hash_value(&row.values[col_idx]) {
                    tuples.push((h, p.page_id, slot as u32));
                }
            }
        }
        tuples.sort_unstable();
        tuples.dedup();

        let mut entry = IndexEntry::new(
            index_name.to_string(),
            schema.name.clone(),
            schema.columns[col_idx].name.clone(),
        );
        entry.pending_entries = tuples;
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
        self.build_index_in_memory(index_name, table_name, column_name)?;
        let last = self.catalog.indexes.last().unwrap().clone();
        self.record_wal_op(WalOp::CreateIndex {
            name: last.name,
            table_name: last.table_name,
            column_name: last.column_name,
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
                if let Some(c_idx) = schema.col_index(&idx.column_name) {
                    let vals: Vec<Value> = rows.iter().map(|r| r.values[c_idx].clone()).collect();
                    idx.index_page_values(page_id, &vals);
                }
            }
        }
    }

    /// Demand-paged index lookup: binary-searches the sparse block directory, reads ONLY the
    /// matching leaf block(s) via LRU `index_cache`, plus checks any unflushed `pending_entries`.
    fn lookup_paged_index(
        &mut self,
        ix_idx: usize,
        vals: &[Value],
    ) -> Result<(HashMap<u64, Vec<u32>>, usize)> {
        let mut by_page: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut blocks_read = 0usize;

        let blocks_meta = self.catalog.indexes[ix_idx].blocks.clone();
        let pending = self.catalog.indexes[ix_idx].pending_entries.clone();
        let removed = self.catalog.indexes[ix_idx].removed_pages.clone();

        for v in vals {
            let target = match hash_value(v) {
                Some(h) => h,
                None => continue,
            };
            // 1. Check pending unflushed entries
            if !pending.is_empty() {
                IndexEntry::search_sorted_slice(&pending, target, &removed, &mut by_page);
            }
            // 2. Binary-search sparse directory -> read ONLY matching on-disk block(s)!
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

    /// Flushes all dirty secondary indexes into fresh compressed sorted on-disk leaf blocks.
    fn flush_dirty_indexes(&mut self) -> Result<()> {
        let profile = self.options.compression;
        let num_ix = self.catalog.indexes.len();
        for i in 0..num_ix {
            if !self.catalog.indexes[i].is_dirty() {
                continue;
            }
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
        Ok(())
    }

    // ========================================================================
    // Constraint checking (UNIQUE / PRIMARY KEY)
    // ========================================================================

    fn check_unique_constraints(
        &mut self,
        table: &str,
        schema: &TableSchema,
        new_rows: &[Row],
        ignore_rowids: Option<&HashSet<i64>>,
    ) -> Result<()> {
        let unique_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique || c.primary_key)
            .map(|(i, _)| i)
            .collect();
        if unique_cols.is_empty() || new_rows.is_empty() {
            return Ok(());
        }

        let pk_col = schema.integer_pk_col();
        let t_idx = self.find_table_idx(table).unwrap();
        let current_next_rowid = self.catalog.tables[t_idx].schema.next_rowid;

        for &c_idx in &unique_cols {
            let col_name = &schema.columns[c_idx].name;
            // 1. Check duplicates within the incoming batch itself in O(B)
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

            // O(1) fast path: sequential / auto-increment INTEGER PRIMARY KEY above `next_rowid`
            // is guaranteed disjoint from all existing rows!
            if Some(c_idx) == pk_col
                && ignore_rowids.is_none()
                && all_int
                && min_int_val >= current_next_rowid
            {
                continue;
            }

            // 2. Check against existing rows in O(1) hash lookup per row (with zone-map / Bloom pruning!)
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
                // Zone-map fast skip for integer PK / monotonic columns
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
                // Bloom filter skip for small batches
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
        Ok(())
    }

    // ========================================================================
    // DML: Insert, Bulk Insert, Scan, Vectorized Analytics, Update, Delete
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

        // Enforce UNIQUE / PRIMARY KEY constraints!
        self.check_unique_constraints(table, &schema, &rows, None)?;

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
        if buf.len() >= flush_thresh {
            let mut to_flush = std::mem::take(buf);
            if !schema.cluster_by.is_empty() {
                sort_rows_by_cluster(&schema, &mut to_flush);
            }
            let full_len = (to_flush.len() / page_rows) * page_rows;
            let remainder = to_flush.split_off(full_len);
            *buf = remainder;
            for chunk in to_flush.chunks(page_rows) {
                let meta =
                    self.storage
                        .write_pax_page(&schema, chunk, self.options.compression)?;
                let pid = meta.page_id;
                self.catalog.tables[idx].pages.push(meta);
                self.update_indexes_for_new_page(&schema, pid, chunk);
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

    /// Vectorized columnar streaming GROUP BY executor: decodes ONLY needed columns directly into
    /// typed `ColumnBatch` arrays with zero row-transposition or `Vec<Value>` allocations!
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
        let mut group_table = VectorGroupTable::new(group_col, agg_kinds.to_vec());

        for page_meta in &pages {
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
            metrics.pages_scanned += 1;
            let batch = self
                .storage
                .read_pax_batch(&schema, page_meta, needed_cols, false)?;
            group_table.ingest_batch(&batch, &page_meta.deleted_slots, vec_filter);
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

    /// Ultra-fast zero-clone indexed point lookup:
    /// - Zero `PageMeta` / `BloomFilter` / `IndexBlockMeta` cloning!
    /// - Demand-pages ONLY the matching SSTable index leaf block into `index_cache`!
    /// - Reads & caches ONLY the filter column + projected output columns as typed `ColumnVector`s!
    /// - Jumps directly in O(1) to `slot` without ever materializing `Vec<Row>`!
    pub fn fast_indexed_point_lookup(
        &mut self,
        table: &str,
        filter_col_idx: usize,
        target_val: &Value,
        proj_cols: &[Option<usize>], // None = rowid, Some(c_idx) = user column
    ) -> Result<Option<(Vec<Vec<Value>>, ScanMetrics)>> {
        let t_idx = match self.find_table_idx(table) {
            Some(i) => i,
            None => return Err(Error::Schema(format!("No such table: '{}'", table))),
        };
        let table_name_matches = self.catalog.tables[t_idx].schema.name.clone();
        let col_name = self.catalog.tables[t_idx].schema.columns[filter_col_idx]
            .name
            .clone();

        let ix_idx = match self.catalog.indexes.iter().position(|i| {
            i.table_name.eq_ignore_ascii_case(&table_name_matches)
                && i.column_name.eq_ignore_ascii_case(&col_name)
        }) {
            Some(i) => i,
            None => return Ok(None),
        };

        let target_hash = match hash_value(target_val) {
            Some(h) => h,
            None => {
                return Ok(Some((
                    Vec::new(),
                    ScanMetrics {
                        total_pages: self.catalog.tables[t_idx].pages.len(),
                        pruned_by_index: self.catalog.tables[t_idx].pages.len(),
                        index_used: Some(self.catalog.indexes[ix_idx].name.clone()),
                        ..Default::default()
                    },
                )));
            }
        };

        let total_pages = self.catalog.tables[t_idx].pages.len();
        let index_name = self.catalog.indexes[ix_idx].name.clone();
        let mut metrics = ScanMetrics {
            total_pages,
            index_used: Some(index_name),
            ..Default::default()
        };

        // 1. Zero-clone sparse directory binary search + demand-paged leaf block lookup
        let mut matched_locs: Vec<(u64, u32)> = Vec::new();
        {
            let ix = &self.catalog.indexes[ix_idx];
            if !ix.pending_entries.is_empty() {
                let start = ix.pending_entries.partition_point(|&(h, _, _)| h < target_hash);
                let mut i = start;
                while i < ix.pending_entries.len() && ix.pending_entries[i].0 == target_hash {
                    let (_, pid, slot) = ix.pending_entries[i];
                    if !ix.removed_pages.contains(&pid) {
                        matched_locs.push((pid, slot));
                    }
                    i += 1;
                }
            }
            let cand_blocks = ix.candidate_blocks(target_hash);
            for b_i in cand_blocks {
                let bmeta = &ix.blocks[b_i];
                let off = bmeta.offset;
                if let Some(cached) = self.index_cache.get(off) {
                    let start = cached.partition_point(|&(h, _, _)| h < target_hash);
                    let mut i = start;
                    while i < cached.len() && cached[i].0 == target_hash {
                        let (_, pid, slot) = cached[i];
                        if !ix.removed_pages.contains(&pid) {
                            matched_locs.push((pid, slot));
                        }
                        i += 1;
                    }
                } else {
                    metrics.index_blocks_read += 1;
                    let decoded = self.storage.read_index_block(bmeta)?;
                    let start = decoded.partition_point(|&(h, _, _)| h < target_hash);
                    let mut i = start;
                    while i < decoded.len() && decoded[i].0 == target_hash {
                        let (_, pid, slot) = decoded[i];
                        if !ix.removed_pages.contains(&pid) {
                            matched_locs.push((pid, slot));
                        }
                        i += 1;
                    }
                    self.index_cache.insert(off, decoded);
                }
            }
        }
        matched_locs.sort_unstable();
        matched_locs.dedup();

        // Count unique pages touched
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

            // Ensure filter column is in ColumnCache (reads ONLY that 1 column chunk on miss!)
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

            // Ensure every projected column is loaded (reads ONLY those columns on miss!)
            let mut row_vals = Vec::with_capacity(proj_cols.len());
            for &pc in proj_cols {
                let ck = col_key_for(pc);
                if self.cache.get_col(pid, ck).is_none() {
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

        // Also check hot buffer
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

        // Step 1: Demand-paged Secondary Index lookup!
        let mut allowed_locs: Option<HashMap<u64, Vec<u32>>> = None;
        if let Some(expr) = where_clause {
            let candidates = extract_index_candidates(expr);
            for (col_name, vals) in candidates {
                let ix_pos = self.catalog.indexes.iter().position(|i| {
                    i.table_name.eq_ignore_ascii_case(&schema.name)
                        && i.column_name.eq_ignore_ascii_case(&col_name)
                });
                if let Some(ix_idx) = ix_pos {
                    let ix_name = self.catalog.indexes[ix_idx].name.clone();
                    let (matched_locs, b_read) = self.lookup_paged_index(ix_idx, &vals)?;
                    metrics.index_blocks_read += b_read;
                    allowed_locs = Some(match allowed_locs {
                        None => matched_locs,
                        Some(prev) => {
                            let mut inter = HashMap::new();
                            for (pid, slots) in prev {
                                if let Some(other_slots) = matched_locs.get(&pid) {
                                    let common: Vec<u32> = slots
                                        .into_iter()
                                        .filter(|s| other_slots.binary_search(s).is_ok())
                                        .collect();
                                    if !common.is_empty() {
                                        inter.insert(pid, common);
                                    }
                                }
                            }
                            inter
                        }
                    });
                    metrics.index_used = Some(ix_name);
                }
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
            // Keep internal `rowid` synchronized if an `INTEGER PRIMARY KEY` column was updated!
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
            updated_rows.push(row);
        }

        self.check_unique_constraints(&canonical, &schema, &updated_rows, Some(&old_ids))?;

        let count = updated_rows.len();
        self.catalog.tables[idx].schema.next_rowid = next_rowid;
        self.mark_rowids_deleted_in_memory(&canonical, &old_ids)?;
        self.hot_buffers
            .entry(key)
            .or_default()
            .extend(updated_rows.clone());

        if rowid_changed {
            // Write DeleteRowIds + InsertRows in ONE atomic WAL frame BEFORE maybe_auto_checkpoint!
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

    /// Creates a consistent, self-contained single-file backup of the database at `dest`.
    /// Flushes pending WAL frames first (when writable), fsyncs the temp copy, and atomically
    /// replaces `dest` without ever deleting an existing backup first.
    pub fn backup_to<P: AsRef<Path>>(&mut self, dest: P) -> Result<()> {
        if !self.read_only && self.active_tx.is_none() && self.dirty {
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
    // Checkpoint & Vacuum (with Paged Index Compaction + Global Sort-Key Clustering)
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
                for chunk in pending.chunks(page_rows) {
                    let meta = self.storage.write_pax_page(&schema, chunk, profile)?;
                    let pid = meta.page_id;
                    self.catalog.tables[t_idx].pages.push(meta);
                    self.update_indexes_for_new_page(&schema, pid, chunk);
                }
            }
        }

        // Flush dirty secondary indexes into compressed paged SSTable blocks!
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
                IndexEntry::new(
                    i.name.clone(),
                    i.table_name.clone(),
                    i.column_name.clone(),
                )
            })
            .collect();

        for t_idx in 0..self.catalog.tables.len() {
            let schema = self.catalog.tables[t_idx].schema.clone();
            let mut live_rows = self.scan_all(&schema.name)?;
            sort_rows_by_cluster(&schema, &mut live_rows);

            let mut new_pages = Vec::new();
            for chunk in live_rows.chunks(page_rows) {
                let meta = new_storage.write_pax_page(&schema, chunk, profile)?;
                let pid = meta.page_id;
                for ix in &mut new_indexes {
                    if ix.table_name.eq_ignore_ascii_case(&schema.name) {
                        if let Some(c_idx) = schema.col_index(&ix.column_name) {
                            let vals: Vec<Value> =
                                chunk.iter().map(|r| r.values[c_idx].clone()).collect();
                            ix.index_page_values(pid, &vals);
                        }
                    }
                }
                new_pages.push(meta);
            }
            new_catalog.tables.push(TableEntry {
                schema,
                pages: new_pages,
            });
        }

        // Write clean sorted on-disk index leaf blocks into the fresh compacted file!
        for ix in &mut new_indexes {
            let entries = std::mem::take(&mut ix.pending_entries);
            let mut blocks = Vec::new();
            for chunk in entries.chunks(INDEX_BLOCK_ENTRIES) {
                blocks.push(new_storage.write_index_block(chunk, profile)?);
            }
            ix.blocks = blocks;
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

impl Drop for Database {
    fn drop(&mut self) {
        if !self.read_only && self.active_tx.is_none() && self.dirty {
            let _ = self.checkpoint();
        }
        self.wal.remove_if_empty();
    }
}

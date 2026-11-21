# SquishDB installation and usage guide

Version `v0.4.0-alpha.1` (`SQSHDB04` PAX-v4 format).

## Prerequisites and installation

SquishDB compiles as pure Rust with three crate dependencies (`brotli`, `flate2`, `crc32fast`) and no C toolchain. Install Rust 1.75 or newer from [rustup.rs](https://rustup.rs).

### Install the CLI with Cargo

```bash
cargo install --git https://github.com/F1nnD3v/squishdb
```

Run `squishdb` with no arguments to confirm the binary is on your `PATH`.

### Build from source

```bash
git clone https://github.com/F1nnD3v/squishdb.git
cd squishdb
cargo build --release
```

Cargo writes the release binary to `./target/release/squishdb` on Linux and macOS, or `.\target\release\squishdb.exe` on Windows.

### Use as a Rust dependency

Add this line to `Cargo.toml`:

```toml
[dependencies]
squishdb = { git = "https://github.com/F1nnD3v/squishdb" }
```

## CLI and REPL

```text
squishdb <file.sqdb>                                  Interactive SQL REPL
squishdb <file.sqdb> -c "<SQL>"                       Execute SQL statement(s)
squishdb stats <file.sqdb>                            Show storage, index & cache stats
squishdb import-bin <file.sqdb> [profile] [col1,col2] Bulk import (+ optional cluster keys)
squishdb export-bin <file.sqdb> <table>               Exact lossless binary export
squishdb bench-lookup <file.sqdb> < queries.sql       Measure query latency from stdin
```

Compression profiles are `fast`, `balanced` (default), `max`, and `none`.

### Interactive REPL

Open or create a database file:

```bash
squishdb demo.sqdb
```

The REPL accumulates multi-line input until it sees a top-level semicolon `;`. Semicolons inside single-quoted strings, double-quoted identifiers, or `--` comments do not trigger execution.

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    plan TEXT DEFAULT 'free',
    created_at INTEGER NOT NULL
) CLUSTER BY (plan, created_at);

INSERT INTO users (id, email, created_at) VALUES (1, 'alice@example.com', 1700000000);
INSERT INTO users VALUES (2, 'bob@example.com', 'pro', 1700000100);

SELECT id, email, plan FROM users ORDER BY id;
```

Dot commands inside the REPL:

| Command | Action |
|---|---|
| `.tables` | List tables, columns, row counts, and `CLUSTER BY` keys |
| `.indexes` | List secondary indexes and their on-disk sizes |
| `.stats` | Print storage, compression ratio, WAL, index, and cache stats |
| `.vacuum` | Rebuild pages to reclaim deleted slots and re-cluster rows |
| `.quit`, `.exit` | Checkpoint dirty changes outside an open transaction, then exit |

### One-shot SQL and diagnostics

Pass one or more semicolon-separated statements with `-c`:

```bash
squishdb demo.sqdb -c "SELECT plan, COUNT(*) AS n FROM users GROUP BY plan ORDER BY n DESC;"
```

Inspect file size, WAL size, per-table compression ratio, index size, and cache hit rate:

```bash
squishdb stats demo.sqdb
```

Benchmark newline-delimited queries in a single process without startup overhead:

```bash
squishdb bench-lookup demo.sqdb < queries.sql
```

## Using SquishDB in Rust

### Opening a database

```rust
use squishdb::{Database, DbOptions};
use squishdb::pax::CompressionProfile;

// Default: Balanced compression, 4,096 rows per page, buffered WAL
let mut db = Database::open("data.sqdb")?;

// Strict durability and higher compression
let opts = DbOptions {
    page_rows: 4096,
    compression: CompressionProfile::Max,
    sync_wal: true,
    auto_checkpoint_rows: 16384,
    cache_pages: 128,
    index_cache_blocks: 64,
};
let mut durable_db = Database::open_with_options("durable.sqdb", opts)?;

// Shared read-only connection (multiple readers can open the same file at once)
let mut ro_db = Database::open_read_only("data.sqdb")?;
```

### Running queries

```rust
use squishdb::{Database, QueryResult};
use squishdb::types::Value;

let mut db = Database::open("shop.sqdb")?;

db.execute(
    "CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer TEXT NOT NULL,
        total REAL NOT NULL
     );
     CREATE INDEX idx_customer ON orders(customer);
     INSERT INTO orders VALUES (1, 'alice', 29.50), (2, 'bob', 120.00), (3, 'alice', 15.25);",
)?;

let results = db.execute(
    "SELECT customer, COUNT(*) AS cnt, SUM(total) AS spent
     FROM orders
     GROUP BY customer
     ORDER BY spent DESC;",
)?;

for res in results {
    match res {
        QueryResult::Rows { columns, rows } => {
            println!("Columns: {:?}", columns);
            for row in rows {
                if let [Value::Text(cust), Value::Integer(cnt), Value::Real(spent)] = row.as_slice() {
                    println!("{cust}: {cnt} orders, ${spent:.2}");
                }
            }
        }
        QueryResult::Affected(n) => println!("{n} rows affected"),
        QueryResult::Message(msg) => println!("{msg}"),
    }
}
```

### Bulk inserts

Single-row `INSERT` statements with `sync_wal = true` hit disk on every call. For large loads, batch rows inside a transaction or call `bulk_insert` to stream straight into compressed PAX-v4 pages:

```rust
use squishdb::Database;
use squishdb::types::Value;

let mut db = Database::open("metrics.sqdb")?;
db.execute("CREATE TABLE metrics (ts INTEGER, host TEXT, cpu REAL) CLUSTER BY (host, ts);")?;

// Crash-safe batched WAL write
db.begin()?;
let batch: Vec<Vec<Value>> = (0..10_000)
    .map(|i| vec![Value::Integer(1700000000 + i), Value::Text("web-01".into()), Value::Real(0.42)])
    .collect();
db.insert_batch("metrics", batch)?;
db.commit()?;
db.checkpoint()?;

// Direct PAX-v4 bulk load (fastest for initial ingestion)
let bulk: Vec<Vec<Value>> = (10_000..50_000)
    .map(|i| vec![Value::Integer(1700000000 + i), Value::Text("web-02".into()), Value::Real(0.35)])
    .collect();
db.bulk_insert("metrics", bulk)?;
```

### Stats and backups in Rust

```rust
let stats = db.stats()?;
println!("{}", stats.format_report());

db.backup_to("metrics-backup.sqdb")?;
```

## SQL reference

### Data types

| SQL type names | Internal type | Notes |
|---|---|---|
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT`, `BOOLEAN`, `BOOL` | `i64` | `TRUE` is `1`, `FALSE` is `0` |
| `REAL`, `FLOAT`, `DOUBLE`, `NUMERIC`, `DECIMAL` | `f64` | Bit-exact round-trip |
| `TEXT`, `VARCHAR`, `CHAR`, `STRING`, `CLOB` | UTF-8 `String` | Escape single quotes as `''` |
| `BLOB`, `BYTES`, `BINARY`, `VARBINARY` | `Vec<u8>` | Hex literals like `X'DEADBEEF'` |

### DDL

```sql
CREATE TABLE [IF NOT EXISTS] name (
    id INTEGER PRIMARY KEY [AUTOINCREMENT],
    code TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'active',
    score REAL
) [CLUSTER BY (col1, col2)];

-- Metadata-only column add; older pages fill the default or NULL lazily on read
ALTER TABLE name ADD [COLUMN] extra_col INTEGER DEFAULT 0;

-- Change clustering keys for the next VACUUM
ALTER TABLE name CLUSTER BY (status, id);

CREATE INDEX [IF NOT EXISTS] idx_name ON table_name(column_name);
DROP INDEX [IF EXISTS] idx_name;
DROP TABLE [IF EXISTS] table_name;
```

### Queries

```sql
WITH active_orders AS (
    SELECT user_id, amount
    FROM orders
    WHERE status = 'paid'
)
SELECT
    u.country,
    u.name,
    o.amount,
    ROW_NUMBER() OVER (PARTITION BY u.country ORDER BY o.amount DESC) AS rnk,
    SUM(o.amount) OVER (PARTITION BY u.country ORDER BY o.amount DESC) AS running_total
FROM users AS u
LEFT JOIN active_orders AS o ON u.id = o.user_id
WHERE u.id IN (SELECT user_id FROM active_orders)
ORDER BY u.country ASC, o.amount DESC
LIMIT 50 OFFSET 0;
```

Supported query syntax:

- `SELECT [DISTINCT]`, column and table aliases, qualified wildcards (`t.*`), and quoted identifiers (`"a""b"`).
- `INNER JOIN` and `LEFT [OUTER] JOIN` via hash equi-join or nested-loop fallback. `RIGHT`, `FULL`, `CROSS`, and `NATURAL` joins return an explicit error.
- `WHERE` with 3-valued `NULL` logic: `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN (...)`, `[NOT] IN (SELECT ...)`, `[NOT] LIKE`, and `CASE WHEN ... THEN ... ELSE ... END`.
- `GROUP BY`, `HAVING`, `COUNT(*)`, `COUNT([DISTINCT] expr)`, `SUM`, `AVG`, `MIN`, and `MAX`.
- Window functions: `ROW_NUMBER()`, `RANK()`, `COUNT(*) OVER (...)`, `SUM(...) OVER (...)`, and `AVG(...) OVER (...)` with SQL `RANGE` peer-tie semantics when `ORDER BY` is present.
- `ORDER BY ... [ASC|DESC]`, `LIMIT`, and `OFFSET`.
- `EXPLAIN <SELECT>` to print the execution plan and pruning counts.
- `PRAGMA STATS;` to print the storage and cache report.

### Transactions and maintenance

```sql
BEGIN [TRANSACTION];
UPDATE users SET plan = 'pro' WHERE id IN (SELECT user_id FROM upgrades);
DELETE FROM logs WHERE ts < 1700000000;
COMMIT [TRANSACTION];   -- or ROLLBACK [TRANSACTION];

CHECKPOINT;                        -- flush WAL and hot buffers into PAX-v4 pages
VACUUM;                            -- reclaim deleted slots and re-cluster all tables
VACUUM readings SORT BY device, ts; -- update CLUSTER BY and vacuum in one step
BACKUP TO 'snapshot.sqdb';         -- atomic online backup
```

## Compression tips

SquishDB picks the smallest lossless codec per column per 4,096-row page, so default tables compress well already. Four choices push files from 6x smaller into the 10x to 20x range:

1. **Cluster on low-cardinality columns first, then timestamp.** For example, `CLUSTER BY (device_id, ts)` or `CLUSTER BY (service, level, ts)`. Grouping identical prefixes together improves Delta, RLE, front-coded dictionaries, and zone-map page pruning at the same time. Run `VACUUM;` after loading unsorted data.
2. **Use `INTEGER PRIMARY KEY`.** SquishDB maps it directly onto the page's internal delta-encoded `rowid` stream, so that column takes zero extra bytes on disk.
3. **Pick the compression profile for your workload.**
   - `CompressionProfile::Balanced` (default): adaptive Brotli, Zlib, or Raw per chunk. Best default.
   - `CompressionProfile::Max`: strongest compression for archival tables, roughly 5% to 15% smaller on text and JSON at higher write CPU cost.
   - `CompressionProfile::Fast`: Zlib fast mode for write-heavy workloads.
   - `CompressionProfile::None`: columnar encodings only (Delta, FOR, RLE, Dictionary) with no general-purpose compressor.
4. **Run `CHECKPOINT;` after bulk writes.** Uncheckpointed rows sit uncompressed in `.sqdb-wal` until `auto_checkpoint_rows` (default 16,384) or an explicit `CHECKPOINT;`.

## Files, locking, and durability

| File | Purpose |
|---|---|
| `mydb.sqdb` | Main database file: double-buffered A/B CRC32 superblocks, PAX-v4 pages, paged indexes, and catalog |
| `mydb.sqdb-wal` | Write-ahead log for uncheckpointed mutations, truncated to 0 bytes on `CHECKPOINT` |
| `mydb.sqdb.lock` | Cross-process OS lock sidecar (`LockFileEx` on Windows, `flock` on Unix) |

Multiple processes can open a database concurrently with `Database::open_read_only`. A read-write `Database::open` takes an exclusive OS lock, and any second writer gets `Error::Busy` right away instead of racing on the file.

Set `DbOptions { sync_wal: true, .. }` to fsync the WAL on every commit. Every superblock, PAX-v4 page, column chunk, and WAL frame carries a CRC32 checksum. After a crash, SquishDB replays every valid WAL frame and drops any torn tail frame.

## Backups and recovery

Run `BACKUP TO` while the database is open:

```sql
BACKUP TO '/path/to/backup-2026-11-21.sqdb';
```

Or from Rust:

```rust
db.backup_to("/path/to/backup-2026-11-21.sqdb")?;
```

SquishDB checkpoints the WAL, writes a temporary file next to the destination, calls `sync_all`, and replaces the target atomically (`MoveFileExW` on Windows, `rename` on Unix). If copying fails halfway through, any existing backup at the destination stays untouched.

For offline copies, run `CHECKPOINT;`, close the writer, and copy the single `.sqdb` file with `cp`, `rsync`, or `aws s3 cp`.

## Tests and benchmarks

```bash
cargo test --release

# Run a single test binary
cargo test --release --test integration
cargo test --release --test fuzz_and_crash
cargo test --release --test adversarial_bugs
cargo test --release --test audit_regressions
```

Benchmark scripts require Python 3, plus `pyarrow` if you want the Apache Parquet comparison:

```bash
python3 benchmarks/compare_sqlite.py balanced
python3 benchmarks/compare_industry.py
python3 benchmarks/bench_v2.py
```

## Troubleshooting

**`Error::Busy` on open.** Another process, or another `Database` handle in the same process, holds the exclusive write lock on that `.sqdb` file. Close the writer or open extra handles with `Database::open_read_only`.

**File size did not drop after `DELETE`.** Deletes write slot tombstones instead of rewriting compressed pages. Run `VACUUM;` to rebuild pages and reclaim space.

**Slow single-row `INSERT` loop.** With `sync_wal = true`, each auto-commit insert waits for a disk fsync (~973 rows/sec). Wrap loops in `BEGIN; ... COMMIT;`, or call `insert_batch` or `bulk_insert`.

**Verifying index or vectorized execution.** Prefix any `SELECT` with `EXPLAIN` to see whether SquishDB used the metadata `COUNT(*)` path, vectorized `GROUP BY`, paged secondary index lookup, zone-map pruning, or Bloom pruning.

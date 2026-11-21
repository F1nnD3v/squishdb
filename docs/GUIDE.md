# SquishDB Installation & Usage Guide

> **Version:** `v0.4.0-alpha.1` (`SQSHDB04` PAX-v4 format)

This guide covers installing SquishDB, using the standalone CLI and interactive REPL, embedding SquishDB in Rust applications, SQL syntax reference, compression tuning, concurrency, and backups.

---

## Table of Contents

1. [Prerequisites & Installation](#1-prerequisites--installation)
2. [CLI & Interactive REPL](#2-cli--interactive-repl)
3. [Using SquishDB in Rust](#3-using-squishdb-in-rust)
4. [SQL Reference](#4-sql-reference)
5. [Getting Maximum Compression](#5-getting-maximum-compression)
6. [Files, Concurrency & Durability](#6-files-concurrency--durability)
7. [Backups & Recovery](#7-backups--recovery)
8. [Running Tests & Benchmarks](#8-running-tests--benchmarks)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites & Installation

SquishDB is written in pure Rust with zero C/system library dependencies (`brotli`, `flate2`, `crc32fast`). You only need a Rust toolchain (`1.75+`).

If you don't have Rust installed yet, install it from [rustup.rs](https://rustup.rs).

### Option A: Install the CLI globally via Cargo

```bash
cargo install --git https://github.com/F1nnD3v/squishdb
```

Verify the installation:

```bash
squishdb
```

### Option B: Build from source

```bash
git clone https://github.com/F1nnD3v/squishdb.git
cd squishdb
cargo build --release
```

The optimized binary will be created at:
- **Linux / macOS:** `./target/release/squishdb`
- **Windows:** `.\target\release\squishdb.exe`

Optionally copy it onto your `PATH` (e.g., `/usr/local/bin/squishdb` on Unix or a folder in your Windows User `PATH`).

### Option C: Add as a Rust library dependency

In your project's `Cargo.toml`:

```toml
[dependencies]
squishdb = { git = "https://github.com/F1nnD3v/squishdb" }
```

---

## 2. CLI & Interactive REPL

### Command Overview

```text
squishdb <file.sqdb>                                  Interactive SQL REPL
squishdb <file.sqdb> -c "<SQL>"                       Execute SQL statement(s)
squishdb stats <file.sqdb>                            Show storage, index & cache stats
squishdb import-bin <file.sqdb> [profile] [col1,col2] Bulk import (+ optional cluster keys)
squishdb export-bin <file.sqdb> <table>               Exact lossless binary export
squishdb bench-lookup <file.sqdb> < queries.sql       Measure query latency from stdin
```

Compression profiles: `fast` | `balanced` (default) | `max` | `none`.

### Interactive SQL REPL

Start a session (creates `demo.sqdb` if it does not exist):

```bash
squishdb demo.sqdb
```

Multi-line SQL statements are buffered until a top-level terminating semicolon `;` (semicolons inside `'...'` strings, `"..."` identifiers, or `--` comments are handled safely):

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

Built-in REPL dot commands:

| Command | Action |
|---|---|
| `.tables` | List all tables, schemas, row counts, and `CLUSTER BY` keys |
| `.indexes` | List all secondary indexes and their on-disk block sizes |
| `.stats` | Print full storage, compression ratio, WAL, index & cache report |
| `.vacuum` | Rebuild all pages cleanly (reclaims deleted slots and re-clusters) |
| `.quit` / `.exit` | Checkpoint dirty changes (if not inside a transaction) and exit |

### One-Shot SQL (`-c`)

```bash
squishdb demo.sqdb -c "SELECT plan, COUNT(*) AS n FROM users GROUP BY plan ORDER BY n DESC;"
```

Multiple semicolon-separated statements can be passed in a single `-c` string.

### Storage & Compression Diagnostics (`stats`)

```bash
squishdb stats demo.sqdb
```

Prints file size, WAL size, catalog size, per-table live/deleted/hot rows, raw vs compressed PAX-v4 bytes, compression ratio, secondary index disk size, and LRU cache hit rate.

### Query Latency Benchmarking (`bench-lookup`)

Pipe newline-delimited SQL statements into `bench-lookup` to time each query in-process (avoiding process startup overhead):

```bash
squishdb bench-lookup demo.sqdb < queries.sql
```

---

## 3. Using SquishDB in Rust

### Opening a Database

```rust
use squishdb::{Database, DbOptions};
use squishdb::pax::CompressionProfile;

// Default options (Balanced Brotli/Zlib adaptive compression, 4,096 rows/page)
let mut db = Database::open("data.sqdb")?;

// Custom options (e.g., strict durability + high compression)
let opts = DbOptions {
    page_rows: 4096,
    compression: CompressionProfile::Max,
    sync_wal: true,              // fsync WAL on every commit
    auto_checkpoint_rows: 16384, // auto-flush hot buffer into PAX-v4 pages
    cache_pages: 128,            // decoded page/column LRU cache entries
    index_cache_blocks: 64,      // decoded SSTable index leaf block LRU entries
};
let mut durable_db = Database::open_with_options("durable.sqdb", opts)?;

// Read-only mode (acquires shared OS lock; multiple readers can run concurrently)
let mut ro_db = Database::open_read_only("data.sqdb")?;
```

### Executing SQL & Reading Results

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

### Fast Programmatic Inserts & Bulk Loading

For large ingests, prefer `insert_batch` inside a transaction or `bulk_insert` (which streams straight into compressed PAX-v4 pages at >2M rows/sec):

```rust
use squishdb::Database;
use squishdb::types::Value;

let mut db = Database::open("metrics.sqdb")?;
db.execute("CREATE TABLE metrics (ts INTEGER, host TEXT, cpu REAL) CLUSTER BY (host, ts);")?;

// Option 1: Batched WAL transaction (crash-safe & fast)
db.begin()?;
let batch: Vec<Vec<Value>> = (0..10_000)
    .map(|i| vec![Value::Integer(1700000000 + i), Value::Text("web-01".into()), Value::Real(0.42)])
    .collect();
db.insert_batch("metrics", batch)?;
db.commit()?;
db.checkpoint()?; // compact WAL into compressed PAX-v4 pages

// Option 2: Direct bulk load (fastest for initial table loads)
let bulk: Vec<Vec<Value>> = (10_000..50_000)
    .map(|i| vec![Value::Integer(1700000000 + i), Value::Text("web-02".into()), Value::Real(0.35)])
    .collect();
db.bulk_insert("metrics", bulk)?;
```

### Inspecting Statistics & Running Backups in Rust

```rust
let stats = db.stats()?;
println!("{}", stats.format_report());

// Atomic online backup (checkpoints WAL first, then writes temp file + fsync + atomic replace)
db.backup_to("metrics-backup.sqdb")?;
```

---

## 4. SQL Reference

### Data Types

| SQL Type Names | Storage Type | Notes |
|---|---|---|
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT`, `BOOLEAN`, `BOOL` | 64-bit signed integer (`i64`) | `TRUE` = `1`, `FALSE` = `0` |
| `REAL`, `FLOAT`, `DOUBLE`, `NUMERIC`, `DECIMAL` | IEEE-754 64-bit float (`f64`) | Bit-exact round-trip preserved |
| `TEXT`, `VARCHAR`, `CHAR`, `STRING`, `CLOB` | UTF-8 string | Single-quoted literals `'it''s'` |
| `BLOB`, `BYTES`, `BINARY`, `VARBINARY` | Raw byte vector | Hex literals `X'DEADBEEF'` |

### DDL

```sql
-- Create table with optional clustering & column constraints
CREATE TABLE [IF NOT EXISTS] name (
    id INTEGER PRIMARY KEY [AUTOINCREMENT],
    code TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'active',
    score REAL
) [CLUSTER BY (col1, col2)];

-- O(1) metadata-only column addition (existing pages lazily fill default/NULL on read)
ALTER TABLE name ADD [COLUMN] extra_col INTEGER DEFAULT 0;

-- Update clustering keys (takes effect on next VACUUM)
ALTER TABLE name CLUSTER BY (status, id);

-- Demand-paged SSTable secondary index
CREATE INDEX [IF NOT EXISTS] idx_name ON table_name(column_name);
DROP INDEX [IF EXISTS] idx_name;
DROP TABLE [IF EXISTS] table_name;
```

### Queries & Analytical Features

```sql
-- CTEs, Window Functions, JOINs, Subqueries, CASE, Aggregates
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

Supported SQL features:
- `SELECT [DISTINCT]`, aliases (`AS`), qualified wildcards (`t.*`), quoted identifiers (`"weird ""name"""`)
- `INNER JOIN` and `LEFT [OUTER] JOIN` (hash equi-join + nested-loop fallback)
- `WHERE` with full SQL 3-valued `NULL` logic (`AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN (...)`, `[NOT] IN (SELECT ...)`, `[NOT] LIKE`, `CASE WHEN ... THEN ... ELSE ... END`)
- `GROUP BY`, `HAVING`, `COUNT(*)`, `COUNT([DISTINCT] expr)`, `SUM`, `AVG`, `MIN`, `MAX`
- Window functions: `ROW_NUMBER()`, `RANK()`, `COUNT(*) OVER (...)`, `SUM(...) OVER (...)`, `AVG(...) OVER (...)` (standard `RANGE` peer-tie semantics with `ORDER BY`)
- `ORDER BY ... [ASC|DESC]`, `LIMIT`, `OFFSET`
- `EXPLAIN <SELECT>` — prints whether metadata `COUNT(*)`, vectorized columnar `GROUP BY`, paged secondary index lookup, zone-map pruning, or Bloom pruning fired
- `PRAGMA STATS;` — prints full storage & cache report

### Maintenance & Transactions

```sql
BEGIN [TRANSACTION];
UPDATE users SET plan = 'pro' WHERE id IN (SELECT user_id FROM upgrades);
DELETE FROM logs WHERE ts < 1700000000;
COMMIT [TRANSACTION];   -- or ROLLBACK [TRANSACTION];

CHECKPOINT;                        -- flush WAL & hot buffers into PAX-v4 pages
VACUUM;                            -- reclaim deleted slots & re-cluster all tables
VACUUM readings SORT BY device, ts; -- set CLUSTER BY and vacuum in one command
BACKUP TO 'snapshot.sqdb';         -- atomic online backup
```

---

## 5. Getting Maximum Compression

SquishDB automatically selects the smallest lossless codec per column per 4,096-row page, so it works well out of the box — but a few schema habits unlock **10×–20× compression**:

1. **Set `CLUSTER BY` on your low-cardinality + timestamp columns** (e.g., `CLUSTER BY (device_id, ts)` or `CLUSTER BY (service, level, ts)`). Sorting rows within/across pages dramatically boosts Delta, RLE, Front-Coded Dictionary, and Zone-Map pruning all at once. Run `VACUUM;` after bulk loading unsorted data.
2. **Use `INTEGER PRIMARY KEY`** for row IDs. SquishDB aliases `INTEGER PRIMARY KEY` directly onto the page's internal delta-encoded `rowid` stream, storing **0 duplicate bytes** for that column.
3. **Pick the right compression profile**:
   - `CompressionProfile::Balanced` *(default)* — best all-around size + fast reads (adaptive Brotli/Zlib/Raw).
   - `CompressionProfile::Max` — maximum archival compression (~5–15% smaller on text/JSON, slower writes).
   - `CompressionProfile::Fast` — Zlib-fast / lightweight codecs for write-heavy workloads.
   - `CompressionProfile::None` — type-aware columnar encodings only (Delta/FOR/RLE/Dict) without general-purpose compression.
4. **Checkpoint after bulk writes**: Uncheckpointed rows sit in the uncompressed WAL until `auto_checkpoint_rows` (default 16,384 rows) or an explicit `CHECKPOINT;`.

---

## 6. Files, Concurrency & Durability

A SquishDB database consists of:

| File | Purpose |
|---|---|
| `mydb.sqdb` | The main single-file database (double-buffered A/B CRC32 superblocks, PAX-v4 pages, paged indexes, catalog) |
| `mydb.sqdb-wal` | Write-ahead log for uncommitted/uncheckpointed mutations (truncated to 0 bytes on `CHECKPOINT`) |
| `mydb.sqdb.lock` | Cross-process OS lock sidecar (`LockFileEx` on Windows, `flock` on Unix) |

- **Concurrency model:** Multiple concurrent read-only connections (`Database::open_read_only`) can hold shared locks simultaneously. A read-write connection (`Database::open`) acquires an exclusive lock; attempting a second writer returns `Error::Busy` immediately instead of corrupting the file.
- **Durability:** Set `DbOptions { sync_wal: true, .. }` to `fsync` the WAL on every commit (equivalent to SQLite `PRAGMA synchronous = FULL`). Every PAX-v4 page, column chunk, superblock, and WAL frame is protected by CRC32 checksums. On crash recovery, any truncated or torn tail frame in `-wal` is cleanly discarded while all prior committed frames replay intact.

---

## 7. Backups & Recovery

### Online Atomic Backup (Recommended)

```sql
BACKUP TO '/path/to/backup-2026-11-21.sqdb';
```

Or in Rust:

```rust
db.backup_to("/path/to/backup-2026-11-21.sqdb")?;
```

`BACKUP TO` checkpoints the WAL first, copies the database to a temporary file alongside the destination, calls `sync_all()`, and atomically replaces the destination file (`MoveFileExW` on Windows, `rename` on Unix). Existing backups at the destination are never deleted if an error occurs mid-copy.

### Offline File Copy

After closing all writers (or running `CHECKPOINT;` while idle), simply copy `mydb.sqdb` with `cp`, `rsync`, `aws s3 cp`, etc.

---

## 8. Running Tests & Benchmarks

```bash
# Run all 44 release tests (integration, fuzz/crash, adversarial & audit regressions)
cargo test --release

# Run individual test suites
cargo test --release --test integration
cargo test --release --test fuzz_and_crash
cargo test --release --test adversarial_bugs
cargo test --release --test audit_regressions
```

Benchmarks (Python 3; install `pyarrow` if you also want the Apache Parquet comparison):

```bash
python3 benchmarks/compare_sqlite.py balanced
python3 benchmarks/compare_industry.py
python3 benchmarks/bench_v2.py
```

---

## 9. Troubleshooting

- **`Error::Busy` when opening a database:** Another process (or another open `Database` handle in the same process) holds an exclusive write lock on `mydb.sqdb`. Close the other writer or open secondary handles with `Database::open_read_only`.
- **Database file didn't shrink after `DELETE`:** Deletes mark lightweight slot tombstones for fast writes. Run `VACUUM;` to rebuild pages and reclaim deleted space.
- **Slow single-row `INSERT` loop:** Wrap bulk inserts in `BEGIN; ... COMMIT;` (or use `insert_batch` / `bulk_insert`). With `sync_wal = true`, individual auto-commit inserts fsync the disk on every single row (~1,000 rows/sec), whereas batched transactions reach >360,000 rows/sec.
- **Checking if an index or vectorized fast path was used:** Prefix any `SELECT` with `EXPLAIN` (e.g., `EXPLAIN SELECT ...;`) to inspect page pruning, Bloom pruning, index lookup, or vectorized `GROUP BY` plans.

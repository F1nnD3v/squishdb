# SquishDB

[![CI](https://github.com/F1nnD3v/squishdb/actions/workflows/ci.yml/badge.svg)](https://github.com/F1nnD3v/squishdb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Alpha / Experimental (`v0.4.0-alpha.1`)** — On-disk format v4 (`SQSHDB04`) may still change before `1.0`. Do not rely on it yet as a drop-in production replacement for SQLite without independent backups.

A lossless, single-file embedded relational database in Rust that **beats SQLite C on storage size (6×–19.4× smaller), analytical `GROUP BY` (4.5× faster), and indexed UUID point lookups (1.78× faster with 4.7× smaller indexes), while beating Apache Parquet Zstd-9 on disk size** — with **100% bit-exact lossless data fidelity**.

📖 **Full documentation:** See the **[Installation & Usage Guide](docs/GUIDE.md)** for complete CLI flags, Rust library API, SQL reference, compression tuning, concurrency, and backup workflows.

---

## Quick Start

### Install the CLI (requires Rust 1.75+)

```bash
cargo install --git https://github.com/F1nnD3v/squishdb
```

Or build from source:

```bash
git clone https://github.com/F1nnD3v/squishdb.git
cd squishdb
cargo build --release
# Binary is at ./target/release/squishdb (or target\release\squishdb.exe on Windows)
```

### Use from the CLI

```bash
# Run SQL directly
squishdb sensors.sqdb -c "
  CREATE TABLE readings (
    id INTEGER PRIMARY KEY,
    device TEXT NOT NULL,
    ts INTEGER NOT NULL,
    temp REAL
  ) CLUSTER BY (device, ts);
  INSERT INTO readings VALUES (1, 'dev-01', 1700000000, 21.5), (2, 'dev-01', 1700000060, 21.7);
  SELECT device, COUNT(*), AVG(temp) FROM readings GROUP BY device;
"

# Launch the interactive SQL REPL
squishdb sensors.sqdb

# Inspect compression ratio, page counts, indexes & cache stats
squishdb stats sensors.sqdb
```

### Use as a Rust Library

Add SquishDB to your `Cargo.toml`:

```toml
[dependencies]
squishdb = { git = "https://github.com/F1nnD3v/squishdb" }
```

```rust
use squishdb::{Database, QueryResult};

fn main() -> squishdb::types::Result<()> {
    let mut db = Database::open("app.sqdb")?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            amount REAL
         ) CLUSTER BY (kind);
         INSERT INTO events VALUES (1, 'signup', 0.0), (2, 'purchase', 49.99);",
    )?;

    for res in db.execute("SELECT kind, COUNT(*), SUM(amount) FROM events GROUP BY kind;")? {
        if let QueryResult::Rows { columns, rows } = res {
            println!("{:?} -> {:?}", columns, rows);
        }
    }

    db.backup_to("app-backup.sqdb")?;
    Ok(())
}
```

---

## Architecture (PAX-v4)

- **Single-file embedded database (`.sqdb`)**: Serverless library + standalone CLI binary, with double-buffered A/B CRC32 superblocks, CRC32-framed `.sqdb-wal`, and native OS-level shared/exclusive locking (`LockFileEx` on Windows, `flock` on Unix) via [src/lock.rs](src/lock.rs).
- **Per-Column Independently Compressed PAX-v4 Pages** ([src/pax.rs](src/pax.rs), [src/storage.rs](src/storage.rs)):
  - 4,096-row PAX pages with a CRC32-verified chunk offset table + independently compressed column chunks — giving **true disk-level columnar projection** (untouched columns have zero disk read, zero CRC32, and zero decompression).
- **Automatic Type-Aware Lossless Codecs** ([src/encoding.rs](src/encoding.rs)):
  - **INTEGER (`i64`):** `Constant`, `FOR + Bitpack`, `Delta + ZigZag + Bitpack`, `Delta + Varint`, `RLE`, `Dictionary + Bitpack`, `Raw`, plus zero-duplicate `INTEGER PRIMARY KEY` rowid aliasing.
  - **REAL (`f64`):** Bit-exact `Constant`, `ScaledDecimal` (IEEE-754 `.to_bits()` round-trip verified), 8-lane `ByteShuffle`, `Raw`.
  - **TEXT / BLOB:** `Constant`, lexicographically sorted **Front-Coded Dictionary**, split-length `Raw`.
- **Dictionary-Native Vectorized Execution** ([src/vector.rs](src/vector.rs)):
  - Decodes dictionary-encoded text columns directly into `{ dict, codes }` with zero per-row `String` allocations, filters via single integer code comparison, and aggregates `GROUP BY` into L1-cache-resident flat arrays indexed by dictionary code.
- **Demand-Paged SSTable Secondary Indexes & Per-Column Cache** ([src/index.rs](src/index.rs), [src/cache.rs](src/cache.rs)):
  - 4,096-entry delta-encoded compressed leaf blocks on disk + sparse Catalog directory + LRU `IndexBlockCache` + per-column `PageCache`, plus automatic 6-bit/key per-page Bloom filters.
- **Rich SQL Engine & Atomic Backups** ([src/sql.rs](src/sql.rs)):
  - CTEs (`WITH`), subqueries (`FROM`, `IN`), window functions (`ROW_NUMBER()`, `RANK()`, `SUM`/`AVG`/`COUNT` `OVER`), Hash `INNER`/`LEFT JOIN`, `GROUP BY`/`HAVING`, `UNIQUE`/`DEFAULT`, O(1) `ALTER TABLE ADD COLUMN`, `CLUSTER BY`, `VACUUM [SORT BY ...]`, `BACKUP TO 'path'`, and `EXPLAIN`.

## Measured Benchmark Summary (SquishDB v0.4 vs SQLite C & Apache Parquet)

| Benchmark | SQLite C (VACUUMed) | Apache Parquet (Zstd L9) | SquishDB v0.4 | Speedup / Savings |
|---|---:|---:|---:|---:|
| **GROUP BY + SUM/AVG/COUNT (100k rows)** | 17.31 ms (9.21 MB) | — | **3.85 ms (1.29 MB)** | **4.50× faster & 7.14× smaller** |
| **200 Random UUID Lookups (40k rows, indexed)** | 23.34 ms (4.64 MB) | — | **13.14 ms (1.89 MB)** | **1.78× faster & 2.46× smaller** |
| **UUID Index Disk Overhead (40k rows)** | +1,752 KB | — | **+375 KB** | **4.7× smaller index** |
| **Multi-Device IoT Clustered (150k rows)** | 7.82 MB | 1.50 MB | **0.40 MB** | **19.44× smaller vs SQLite (3.75× vs Parquet)** |
| **JSON Telemetry Logs (80k rows)** | 14.81 MB | 1.47 MB | **1.05 MB** | **14.10× smaller vs SQLite** |
| **Random 4 KB Blobs (Entropy Limit)** | 8.80 MB | 7.85 MB | **7.81 MB** | **Zero inflation (+0.57 B/row)** |

### Insert Throughput (Release Mode)
- Single-row auto-commit (`sync_wal = true`, `fsync` every row): **973 rows/sec**
- Single-row auto-commit (`sync_wal = false`, WAL buffered): **119,181 rows/sec**
- Batched WAL transaction (`1,000 rows/COMMIT`, `sync_wal = true`): **363,416 rows/sec**
- Bulk PAX-v4 load + Brotli checkpoint (`50,000 rows`): **2,088,886 rows/sec**

## Backups

```sql
-- Atomic online backup (checkpoints WAL first, writes via temp file + atomic rename)
BACKUP TO 'backup.sqdb';
```

Or run `CHECKPOINT;` (or close the DB) and copy the single `.sqdb` file directly.

## Current Limitations & Roadmap (Pre-1.0)

Not yet implemented (planned before `1.0`):
- **Foreign keys & `CHECK` constraints** (`REFERENCES ... ON DELETE CASCADE`)
- **Nested `SAVEPOINT`s** and multi-writer **MVCC** snapshot isolation (currently concurrent shared readers + single exclusive writer)
- **Direct `.sqlite` → `.sqdb` CLI migration tool** (`squishdb import-sqlite`)
- **Composite multi-column secondary indexes** and range-query B-tree indexes
- **Backward-compatible format migration guarantees** across major format versions

## Testing & Benchmarks

```bash
# Run the full test suite (integration, fuzz/crash, adversarial & audit regressions)
cargo test --release

# Run head-to-head SQLite & Parquet benchmarks (requires Python 3; optional pyarrow for Parquet)
python3 benchmarks/compare_sqlite.py balanced
python3 benchmarks/compare_industry.py
python3 benchmarks/bench_v2.py
```

## License

MIT — see [LICENSE](LICENSE).

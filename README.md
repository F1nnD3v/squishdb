# SquishDB

[![CI](https://github.com/F1nnD3v/squishdb/actions/workflows/ci.yml/badge.svg)](https://github.com/F1nnD3v/squishdb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

SquishDB is a single-file embedded SQL database written in Rust. It stores data in 4,096-row columnar pages and picks the smallest lossless codec for each column on each page. On our benchmark suite, database files come out 6x to 19.4x smaller than vacuumed SQLite files, and smaller than Parquet at Zstd level 9, with bit-exact round-trips for integers, IEEE-754 floats, text, and blobs.

The catch: this is `v0.4.0-alpha.1`. The on-disk format (`SQSHDB04`) can still change before 1.0, and foreign keys and multi-writer MVCC are not built yet. Keep separate backups if you put real data in it.

For full CLI flags, Rust API examples, SQL syntax, and tuning notes, read [docs/GUIDE.md](docs/GUIDE.md).

## Quick start

Install the CLI with Rust 1.75 or newer:

```bash
cargo install --git https://github.com/F1nnD3v/squishdb
```

Or build from source:

```bash
git clone https://github.com/F1nnD3v/squishdb.git
cd squishdb
cargo build --release
```

Run SQL from the command line:

```bash
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

# Open the interactive REPL
squishdb sensors.sqdb

# Print storage, compression ratio, index, and cache stats
squishdb stats sensors.sqdb
```

### Rust library

Add the crate to `Cargo.toml`:

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

## How it works

A database lives in one `.sqdb` file with double-buffered A/B CRC32 superblocks, a CRC32-framed `.sqdb-wal` write-ahead log, and OS file locking (`LockFileEx` on Windows, `flock` on Unix) in [src/lock.rs](src/lock.rs).

**PAX-v4 columnar pages.** Each 4,096-row page stores a CRC32-verified chunk offset table followed by independently compressed column chunks in [src/pax.rs](src/pax.rs) and [src/storage.rs](src/storage.rs). Queries only read, verify, and decompress the columns they touch.

**Per-column lossless codecs.** The encoder in [src/encoding.rs](src/encoding.rs) tries codecs per column and keeps the smallest output. Integers use Constant, FOR + Bitpack, Delta + ZigZag + Bitpack, Delta + Varint, RLE, Dictionary + Bitpack, or Raw, and `INTEGER PRIMARY KEY` aliases `rowid` for zero duplicate bytes. Floats use Constant, ScaledDecimal verified bit-for-bit via `.to_bits()`, 8-lane ByteShuffle, or Raw. Text and blobs use Constant, lexicographically sorted front-coded dictionaries, or split-length Raw.

**Dictionary-native vectorized `GROUP BY`.** For single-table aggregations, [src/vector.rs](src/vector.rs) decodes dictionary-encoded text straight into `{ dict, codes }` without allocating per-row `String` values, filters on integer codes, and aggregates into flat arrays indexed by code.

**Demand-paged secondary indexes.** Indexes in [src/index.rs](src/index.rs) write 4,096-entry delta-encoded compressed leaf blocks on disk with a sparse directory in the catalog, an LRU block cache in [src/cache.rs](src/cache.rs), and automatic 6-bit-per-key per-page Bloom filters.

**SQL engine.** [src/sql.rs](src/sql.rs) implements `WITH` CTEs, `FROM` and `IN` subqueries, window functions (`ROW_NUMBER()`, `RANK()`, `SUM`/`AVG`/`COUNT` `OVER`), hash `INNER` and `LEFT JOIN`, `GROUP BY`/`HAVING`, `UNIQUE`, `DEFAULT`, O(1) `ALTER TABLE ADD COLUMN`, `CLUSTER BY`, `VACUUM [SORT BY ...]`, `BACKUP TO`, and `EXPLAIN`.

## Benchmarks

Measured against vacuumed SQLite C and Apache Parquet with Zstd level 9 on the same synthetic relational datasets:

| Benchmark | SQLite C | Parquet (Zstd L9) | SquishDB v0.4 | Comparison |
|---|---:|---:|---:|---:|
| GROUP BY + SUM/AVG/COUNT (100k rows) | 17.31 ms, 9.21 MB | n/a | 3.85 ms, 1.29 MB | 4.50x faster, 7.14x smaller |
| 200 random UUID lookups (40k rows, indexed) | 23.34 ms, 4.64 MB | n/a | 13.14 ms, 1.89 MB | 1.78x faster, 2.46x smaller |
| UUID index disk overhead (40k rows) | +1,752 KB | n/a | +375 KB | 4.7x smaller index |
| Multi-device IoT clustered (150k rows) | 7.82 MB | 1.50 MB | 0.40 MB | 19.44x smaller than SQLite, 3.75x smaller than Parquet |
| JSON telemetry logs (80k rows) | 14.81 MB | 1.47 MB | 1.05 MB | 14.10x smaller than SQLite |
| Random 4 KB blobs (incompressible) | 8.80 MB | 7.85 MB | 7.81 MB | +0.57 B/row overhead |

### Insert throughput

Measured in release mode:

- Single-row auto-commit with `sync_wal = true` (fsync per row): 973 rows/sec.
- Single-row auto-commit with `sync_wal = false` (buffered WAL): 119,181 rows/sec.
- Batched WAL transaction at 1,000 rows per `COMMIT` with `sync_wal = true`: 363,416 rows/sec.
- Direct bulk PAX-v4 load plus Brotli checkpoint over 50,000 rows: 2,088,886 rows/sec.

Single-row fsynced writes are bound by disk fsync latency, so wrap bulk writes in `BEGIN` / `COMMIT` or call `insert_batch` / `bulk_insert`.

## Backups

Run an online atomic backup from SQL:

```sql
BACKUP TO 'backup.sqdb';
```

This checkpoints the WAL, writes a temporary copy, calls `sync_all`, and atomically replaces the target file. You can also run `CHECKPOINT;`, close the writer, and copy the `.sqdb` file directly.

## What is missing before 1.0

- Foreign keys and `CHECK` constraints.
- Nested `SAVEPOINT`s and multi-writer MVCC (today SquishDB allows concurrent shared readers or one exclusive writer).
- A direct `.sqlite` to `.sqdb` migration command.
- Composite multi-column indexes and range B-tree indexes.
- Guaranteed forward migration across on-disk format versions.

## Running tests and benchmarks

```bash
cargo test --release

# Head-to-head SQLite and Parquet benchmarks (Python 3; optional pyarrow for Parquet)
python3 benchmarks/compare_sqlite.py balanced
python3 benchmarks/compare_industry.py
python3 benchmarks/bench_v2.py
```

## License

MIT. See [LICENSE](LICENSE).

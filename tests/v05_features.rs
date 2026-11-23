use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;

use squishdb::index::IndexKind;
use squishdb::storage::{FORMAT_VERSION, FORMAT_VERSION_V4, MAGIC_V4, MAGIC_V5, SUPERBLOCK_SIZE};
use squishdb::types::{Error, Value};
use squishdb::{migrate_sqlite, Database, DbOptions, MigrateOptions, QueryResult, SharedDatabase};

fn temp_db(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_v05_{}_{}.sqdb", name, std::process::id()));
    cleanup(&p);
    p
}

fn cleanup(p: &PathBuf) {
    let _ = std::fs::remove_file(p);
    for suffix in ["-wal", ".lock", ".vacuum"] {
        let mut s = p.as_os_str().to_os_string();
        s.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(s));
    }
}

fn rows_of(res: &[QueryResult]) -> &[Vec<Value>] {
    match &res[0] {
        QueryResult::Rows { rows, .. } => rows.as_slice(),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

fn plan_of(res: &[QueryResult]) -> &str {
    match &res[0] {
        QueryResult::Message(m) => m.as_str(),
        other => panic!("Expected Message, got {:?}", other),
    }
}

// ============================================================================
// 1. CHECK Constraints & Foreign Keys (RESTRICT, CASCADE, SET NULL, SET DEFAULT)
// ============================================================================

#[test]
fn test_check_constraints_and_persistence() {
    let path = temp_db("check_constraints");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL CHECK (price > 0.0),
                discount REAL DEFAULT 0.0,
                stock INTEGER CHECK (stock >= 0),
                CONSTRAINT chk_discount CHECK (discount >= 0.0 AND discount <= price)
            );",
        )
        .unwrap();

        // Valid inserts (including NULL stock: SQL CHECK allows UNKNOWN/NULL)
        db.execute(
            "INSERT INTO products VALUES (1, 'Widget', 25.0, 5.0, 10), (2, 'Gadget', 100.0, 0.0, NULL);",
        )
        .unwrap();

        // Violates price > 0.0
        assert!(db
            .execute("INSERT INTO products VALUES (3, 'Free', 0.0, 0.0, 1);")
            .is_err());
        // Violates table-level CHECK (discount <= price)
        assert!(db
            .execute("INSERT INTO products VALUES (4, 'BadDisc', 10.0, 20.0, 1);")
            .is_err());
        // Violates stock >= 0 on UPDATE
        assert!(db
            .execute("UPDATE products SET stock = -5 WHERE id = 1;")
            .is_err());
        // Valid UPDATE
        db.execute("UPDATE products SET discount = 25.0 WHERE id = 1;")
            .unwrap();
        db.checkpoint().unwrap();
    }

    // Reopen and verify CHECK constraints persisted in v5 catalog!
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .execute("INSERT INTO products VALUES (5, 'Neg', -1.0, 0.0, 1);")
            .is_err());
        assert_eq!(db.count_rows("products").unwrap(), 2);
    }
    cleanup(&path);
}

#[test]
fn test_foreign_keys_restrict_cascade_set_null_and_atomicity() {
    let path = temp_db("foreign_keys");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                dept_id INTEGER REFERENCES departments(id) ON DELETE RESTRICT ON UPDATE CASCADE,
                manager_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY,
                emp_id INTEGER NOT NULL,
                title TEXT,
                FOREIGN KEY (emp_id) REFERENCES employees(id) ON DELETE CASCADE
            );",
        )
        .unwrap();

        db.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales');")
            .unwrap();
        // Self-referential insert + NULL FK (allowed by MATCH SIMPLE)
        db.execute(
            "INSERT INTO employees VALUES (10, 1, NULL, 'Alice'), (11, 1, 10, 'Bob'), (12, 2, NULL, 'Carol');",
        )
        .unwrap();
        db.execute("INSERT INTO tasks VALUES (100, 11, 'Build MVCC'), (101, 11, 'Write tests');")
            .unwrap();

        // Reject inserting child with non-existent parent
        assert!(db
            .execute("INSERT INTO employees VALUES (13, 999, NULL, 'Ghost');")
            .is_err());
        // Reject updating child to non-existent parent
        assert!(db
            .execute("UPDATE employees SET dept_id = 999 WHERE id = 10;")
            .is_err());

        // ON DELETE RESTRICT prevents deleting referenced department
        assert!(db.execute("DELETE FROM departments WHERE id = 1;").is_err());
        // Cannot DROP referenced parent table
        assert!(db.execute("DROP TABLE departments;").is_err());

        // ON UPDATE CASCADE updates child dept_id automatically
        db.execute("UPDATE departments SET id = 100 WHERE id = 1;")
            .unwrap();
        let emp_depts = db
            .execute("SELECT id, dept_id FROM employees WHERE id IN (10, 11) ORDER BY id;")
            .unwrap();
        assert_eq!(rows_of(&emp_depts).len(), 2);
        assert!(rows_of(&emp_depts)[0][1].exact_eq(&Value::Integer(100)));
        assert!(rows_of(&emp_depts)[1][1].exact_eq(&Value::Integer(100)));

        // ON DELETE SET NULL on manager_id when deleting Alice (id=10)
        db.execute("DELETE FROM employees WHERE id = 10;").unwrap();
        let bob = db
            .execute("SELECT manager_id FROM employees WHERE id = 11;")
            .unwrap();
        assert!(rows_of(&bob)[0][0].is_null());

        // ON DELETE CASCADE deletes tasks referencing Bob (id=11)
        db.execute("DELETE FROM employees WHERE id = 11;").unwrap();
        assert_eq!(db.count_rows("tasks").unwrap(), 0);

        // Now department 100 has no employees -> DELETE succeeds!
        db.execute("DELETE FROM departments WHERE id = 100;")
            .unwrap();
        assert_eq!(db.count_rows("departments").unwrap(), 1);
        db.checkpoint().unwrap();
    }

    // Reopen and verify foreign keys persisted across restart
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .execute("INSERT INTO employees VALUES (50, 999, NULL, 'Invalid');")
            .is_err());
    }
    cleanup(&path);
}

// ============================================================================
// 2. Nested SAVEPOINTs
// ============================================================================

#[test]
fn test_nested_savepoints_and_ddl_rollback() {
    let path = temp_db("savepoints");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note TEXT);")
            .unwrap();
        db.execute("INSERT INTO ledger VALUES (1, 'initial');")
            .unwrap();

        db.execute(
            "BEGIN;
             INSERT INTO ledger VALUES (2, 'tx_root');
             SAVEPOINT sp1;
             INSERT INTO ledger VALUES (3, 'in_sp1');
             SAVEPOINT sp2;
             INSERT INTO ledger VALUES (4, 'in_sp2');
             CREATE TABLE temp_ephemeral (x INTEGER);
             ROLLBACK TO SAVEPOINT sp2;
             INSERT INTO ledger VALUES (5, 'after_sp2_rollback');
             RELEASE SAVEPOINT sp2;",
        )
        .unwrap();

        // Row 4 and temp_ephemeral must be gone; rows 1, 2, 3, 5 remain
        assert!(db.table_schema("temp_ephemeral").is_none());
        let r = db
            .execute("SELECT id FROM ledger ORDER BY id ASC;")
            .unwrap();
        assert_eq!(rows_of(&r).len(), 4);

        // Rolling back to sp1 removes 3 & 5, while keeping sp1 alive for repeated rollback
        db.execute("ROLLBACK TO sp1;").unwrap();
        db.execute("INSERT INTO ledger VALUES (6, 'retry_1');")
            .unwrap();
        db.execute("ROLLBACK TO sp1;").unwrap();
        db.execute("INSERT INTO ledger VALUES (7, 'final_in_sp1');")
            .unwrap();
        db.execute("RELEASE sp1; COMMIT;").unwrap();

        let final_rows = db
            .execute("SELECT id, note FROM ledger ORDER BY id ASC;")
            .unwrap();
        let rows = rows_of(&final_rows);
        assert_eq!(rows.len(), 3);
        assert!(rows[0][0].exact_eq(&Value::Integer(1)));
        assert!(rows[1][0].exact_eq(&Value::Integer(2)));
        assert!(rows[2][0].exact_eq(&Value::Integer(7)));

        // Standalone SAVEPOINT outside explicit BEGIN (auto-starts & commits on final RELEASE)
        db.execute(
            "SAVEPOINT auto_sp;
             INSERT INTO ledger VALUES (8, 'auto_committed');
             RELEASE auto_sp;",
        )
        .unwrap();
        assert!(!db.in_transaction());
        assert_eq!(db.count_rows("ledger").unwrap(), 4);
    }

    // Verify persistence across reopen
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.count_rows("ledger").unwrap(), 4);
    }
    cleanup(&path);
}

// ============================================================================
// 3. Multi-Writer MVCC & Multi-Threaded Concurrency (PostgreSQL Snapshot Isolation)
// ============================================================================

#[test]
fn test_mvcc_snapshot_isolation_and_write_write_conflict() {
    let path = temp_db("mvcc_si");
    {
        let shared = SharedDatabase::open(&path).unwrap();
        let mut setup = shared.connect();
        setup
            .execute(
                "CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER);
                 INSERT INTO accounts VALUES (1, 'Alice', 100), (2, 'Bob', 200);",
            )
            .unwrap();

        // 1. Snapshot Isolation: reader sees old snapshot until writer commits
        let mut c1 = shared.connect();
        let mut c2 = shared.connect();

        c1.execute("BEGIN;").unwrap();
        c2.execute("BEGIN;").unwrap();

        c1.execute("UPDATE accounts SET balance = 150 WHERE id = 1;")
            .unwrap();

        // c2 still sees 100 before c1 commits
        let r_before = c2
            .execute("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        assert!(rows_of(&r_before)[0][0].exact_eq(&Value::Integer(100)));

        // c1 commits
        c1.execute("COMMIT;").unwrap();

        // c2 STILL sees 100 on its REPEATABLE READ snapshot!
        let r_after = c2
            .execute("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        assert!(rows_of(&r_after)[0][0].exact_eq(&Value::Integer(100)));

        // 2. Write-Write Conflict (First-Committer-Wins): c2 tries to update same row id=1
        c2.execute("UPDATE accounts SET balance = 50 WHERE id = 1;")
            .unwrap();
        let err = c2.execute("COMMIT;");
        assert!(
            matches!(err, Err(Error::Busy(_))),
            "Expected MVCC serialization conflict, got {:?}",
            err
        );

        // 3. Disjoint Multi-Writer Concurrent Transactions BOTH succeed!
        let mut w1 = shared.connect();
        let mut w2 = shared.connect();
        w1.execute("BEGIN;").unwrap();
        w2.execute("BEGIN;").unwrap();

        w1.execute("UPDATE accounts SET balance = 175 WHERE id = 1;")
            .unwrap();
        w2.execute("UPDATE accounts SET balance = 225 WHERE id = 2;")
            .unwrap();

        w1.execute("COMMIT;").unwrap();
        w2.execute("COMMIT;").unwrap();

        let mut verify = shared.connect_readonly();
        let rows = verify
            .execute("SELECT id, balance FROM accounts ORDER BY id ASC;")
            .unwrap();
        assert!(rows_of(&rows)[0][1].exact_eq(&Value::Integer(175)));
        assert!(rows_of(&rows)[1][1].exact_eq(&Value::Integer(225)));
    }
    cleanup(&path);
}

#[test]
fn test_multithreaded_mvcc_concurrent_writers_and_readers() {
    let path = temp_db("mvcc_threads");
    {
        let shared = SharedDatabase::open(&path).unwrap();
        let mut init = shared.connect();
        init.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, worker INTEGER, seq INTEGER, payload TEXT);",
        )
        .unwrap();

        let num_writers = 4usize;
        let num_readers = 4usize;
        let rows_per_writer = 25usize;
        let barrier = Arc::new(Barrier::new(num_writers + num_readers));
        let mut handles = Vec::new();

        for w in 0..num_writers {
            let db = shared.clone();
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut conn = db.connect();
                b.wait();
                for s in 0..rows_per_writer {
                    let id = (w * 1000 + s + 1) as i64;
                    conn.execute("BEGIN;").unwrap();
                    conn.execute(&format!(
                        "INSERT INTO events VALUES ({}, {}, {}, 'w{}_s{}');",
                        id, w, s, w, s
                    ))
                    .unwrap();
                    // Test nested savepoint inside concurrent MVCC writer
                    conn.execute("SAVEPOINT sp;").unwrap();
                    conn.execute(&format!(
                        "INSERT INTO events VALUES ({}, {}, {}, 'rolled_back');",
                        id + 500_000,
                        w,
                        s
                    ))
                    .unwrap();
                    conn.execute("ROLLBACK TO sp; RELEASE sp;").unwrap();
                    conn.execute("COMMIT;").unwrap();
                }
            }));
        }

        for _ in 0..num_readers {
            let db = shared.clone();
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut conn = db.connect_readonly();
                b.wait();
                for _ in 0..20 {
                    let res = conn.execute("SELECT COUNT(*) FROM events;").unwrap();
                    let cnt = match rows_of(&res)[0][0] {
                        Value::Integer(v) => v,
                        _ => panic!("expected int"),
                    };
                    assert!(cnt >= 0 && cnt <= (num_writers * rows_per_writer) as i64);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut check = shared.connect_readonly();
        let res = check
            .execute("SELECT worker, COUNT(*) FROM events GROUP BY worker ORDER BY worker;")
            .unwrap();
        let rows = rows_of(&res);
        assert_eq!(rows.len(), num_writers);
        for (w, row) in rows.iter().enumerate() {
            assert!(row[0].exact_eq(&Value::Integer(w as i64)));
            assert!(row[1].exact_eq(&Value::Integer(rows_per_writer as i64)));
        }
        shared.checkpoint().unwrap();
    }
    cleanup(&path);
}

// ============================================================================
// 4. Composite Multi-Column Indexes & Range B-Tree Indexes
// ============================================================================

#[test]
fn test_composite_and_range_btree_indexes() {
    let path = temp_db("btree_composite");
    {
        let mut opts = DbOptions::default();
        opts.page_rows = 128; // 16 pages for 2,048 rows
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute(
            "CREATE TABLE metrics (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL,
                ts INTEGER NOT NULL,
                score REAL NOT NULL
            );",
        )
        .unwrap();

        let mut batch = Vec::with_capacity(2048);
        for i in 0..2048i64 {
            let tenant = format!("t{:02}", i % 16);
            let ts = 1000 + (i / 16) * 10;
            let score = (i % 200) as f64 + 0.5;
            batch.push(vec![
                Value::Integer(i + 1),
                Value::Text(tenant),
                Value::Integer(ts),
                Value::Real(score),
            ]);
        }
        db.bulk_insert("metrics", batch).unwrap();
        db.checkpoint().unwrap();

        // Create composite B-tree index (tenant ASC, ts DESC) + single-column range B-tree on score
        db.execute("CREATE UNIQUE INDEX idx_tenant_ts ON metrics (tenant, ts DESC);")
            .unwrap();
        db.execute("CREATE INDEX idx_score ON metrics USING BTREE (score);")
            .unwrap();
        db.execute("CREATE INDEX idx_hash_pair ON metrics USING HASH (tenant, ts);")
            .unwrap();
        db.checkpoint().unwrap();

        // Unique composite index rejects duplicate (tenant, ts)
        assert!(db
            .execute("INSERT INTO metrics VALUES (9999, 't00', 1000, 1.0);")
            .is_err());

        // 1. Composite prefix equality + trailing range on (tenant, ts DESC)
        let exp1 = db
            .execute("EXPLAIN SELECT id, ts FROM metrics WHERE tenant = 't05' AND ts >= 1100 AND ts <= 1150;")
            .unwrap();
        assert!(
            plan_of(&exp1).contains("index='idx_tenant_ts'"),
            "Expected composite B-tree index use, got: {}",
            plan_of(&exp1)
        );
        let r1 = db
            .execute("SELECT ts FROM metrics WHERE tenant = 't05' AND ts >= 1100 AND ts <= 1150 ORDER BY ts ASC;")
            .unwrap();
        let expected_ts: Vec<i64> = (1100..=1150).step_by(10).collect();
        assert_eq!(rows_of(&r1).len(), expected_ts.len());
        for (row, &exp_v) in rows_of(&r1).iter().zip(&expected_ts) {
            assert!(row[0].exact_eq(&Value::Integer(exp_v)));
        }

        // 2. Single-column range B-tree BETWEEN
        let exp2 = db
            .execute("EXPLAIN SELECT id FROM metrics WHERE score BETWEEN 195.5 AND 199.5;")
            .unwrap();
        assert!(
            plan_of(&exp2).contains("index='idx_score'"),
            "Expected B-tree range index, got: {}",
            plan_of(&exp2)
        );
        let r2 = db
            .execute("SELECT COUNT(*) FROM metrics WHERE score BETWEEN 195.5 AND 199.5;")
            .unwrap();
        let expected_cnt = (0..2048i64).filter(|i| (i % 200) >= 195).count() as i64;
        assert!(rows_of(&r2)[0][0].exact_eq(&Value::Integer(expected_cnt)));

        // Reopen and verify composite & B-tree indexes persisted properly!
        drop(db);
        let mut db2 = Database::open(&path).unwrap();
        let r3 = db2
            .execute("SELECT ts FROM metrics WHERE tenant = 't05' AND ts = 1120;")
            .unwrap();
        assert_eq!(rows_of(&r3).len(), 1);
        assert!(rows_of(&r3)[0][0].exact_eq(&Value::Integer(1120)));

        // VACUUM preserves composite & B-tree indexes cleanly
        db2.vacuum().unwrap();
        let r4 = db2
            .execute("SELECT ts FROM metrics WHERE tenant = 't05' AND ts >= 1100 AND ts <= 1150 ORDER BY ts;")
            .unwrap();
        assert_eq!(rows_of(&r4).len(), expected_ts.len());
    }
    cleanup(&path);
}

// ============================================================================
// 5. PostgreSQL-Style Parallel Query Execution & Compression
// ============================================================================

#[test]
fn test_parallel_query_execution_matches_single_threaded() {
    let path = temp_db("parallel_exec");
    {
        let mut opts = DbOptions::default();
        opts.page_rows = 256; // 16 pages for 4,096 rows
        opts.parallel_workers = 4;
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute(
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL, qty INTEGER);",
        )
        .unwrap();

        let regions = ["us-east", "us-west", "eu-central", "ap-south"];
        let mut batch = Vec::with_capacity(4096);
        for i in 0..4096i64 {
            batch.push(vec![
                Value::Integer(i + 1),
                Value::Text(regions[(i % 4) as usize].into()),
                Value::Real(((i % 100) as f64) * 1.25),
                Value::Integer((i % 7) + 1),
            ]);
        }
        db.bulk_insert("sales", batch).unwrap();
        db.checkpoint().unwrap();

        let gb_sql = "SELECT region, COUNT(*) AS cnt, SUM(amount) AS rev, AVG(qty) AS avg_q
                      FROM sales WHERE qty >= 3 GROUP BY region ORDER BY region ASC;";

        // Parallel (4 workers)
        db.execute("SET max_parallel_workers = 4;").unwrap();
        let exp_par = db.execute(&format!("EXPLAIN {}", gb_sql)).unwrap();
        assert!(
            plan_of(&exp_par).contains("PARALLEL (workers="),
            "Expected PARALLEL plan, got: {}",
            plan_of(&exp_par)
        );
        let par_res = db.execute(gb_sql).unwrap();

        // Single-threaded (1 worker)
        db.execute("SET max_parallel_workers = 1;").unwrap();
        let seq_res = db.execute(gb_sql).unwrap();

        let pr = rows_of(&par_res);
        let sr = rows_of(&seq_res);
        assert_eq!(pr.len(), sr.len());
        for (a, b) in pr.iter().zip(sr.iter()) {
            for (va, vb) in a.iter().zip(b.iter()) {
                assert!(
                    va.sql_cmp(vb).is_eq(),
                    "Parallel vs sequential mismatch: {:?} vs {:?}",
                    va,
                    vb
                );
            }
        }

        // Parallel sequential scan with filter
        db.execute("PRAGMA threads = 4;").unwrap();
        let scan_sql = "SELECT id, amount FROM sales WHERE qty = 5 ORDER BY id ASC;";
        let exp_scan = db.execute(&format!("EXPLAIN {}", scan_sql)).unwrap();
        assert!(
            plan_of(&exp_scan).contains("PARALLEL (workers="),
            "Expected PARALLEL scan plan, got: {}",
            plan_of(&exp_scan)
        );
        let scan_par = db.execute(scan_sql).unwrap();
        db.execute("PRAGMA threads = 1;").unwrap();
        let scan_seq = db.execute(scan_sql).unwrap();
        assert_eq!(rows_of(&scan_par).len(), rows_of(&scan_seq).len());
    }
    cleanup(&path);
}

// ============================================================================
// 6. Guaranteed Forward Migration Across On-Disk Format Versions (v4 -> v5)
// ============================================================================

#[test]
fn test_guaranteed_forward_migration_v4_to_v5_and_future_version_guard() {
    let path = temp_db("format_migrate");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE legacy (id INTEGER PRIMARY KEY, code TEXT, val REAL, raw BLOB);
             INSERT INTO legacy VALUES
                (1, 'alpha', 3.14159, X'DEADBEEF'),
                (2, 'beta', -0.0, NULL),
                (3, 'gamma', 42.5, X'000102');
             CREATE INDEX idx_code ON legacy USING HASH (code);",
        )
        .unwrap();
        // Write a genuine byte-exact v4 (SQSHDB04) database file!
        db.write_v4_snapshot_for_test().unwrap();
    }

    // Verify raw file header starts with SQSHDB04
    {
        let mut f = std::fs::File::open(&path).unwrap();
        let mut hdr = [0u8; 12];
        f.read_exact(&mut hdr).unwrap();
        assert_eq!(&hdr[0..8], MAGIC_V4);
        assert_eq!(u32::from_le_bytes(hdr[8..12].try_into().unwrap()), FORMAT_VERSION_V4);
    }

    // 1. Read-only open of v4 file succeeds WITHOUT modifying the file on disk!
    {
        let mut ro = Database::open_readonly(&path).unwrap();
        assert_eq!(ro.format_version(), FORMAT_VERSION_V4);
        assert_eq!(ro.migrated_from_version(), Some(FORMAT_VERSION_V4));
        let rows = ro
            .execute("SELECT id, code, val, raw FROM legacy WHERE code = 'alpha';")
            .unwrap();
        assert_eq!(rows_of(&rows).len(), 1);
        assert!(rows_of(&rows)[0][0].exact_eq(&Value::Integer(1)));
        assert!(rows_of(&rows)[0][3].exact_eq(&Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])));
    }

    // Still SQSHDB04 on disk after read-only open
    {
        let mut f = std::fs::File::open(&path).unwrap();
        let mut hdr = [0u8; 8];
        f.read_exact(&mut hdr).unwrap();
        assert_eq!(&hdr, MAGIC_V4);
    }

    // 2. Read-write open + upgrade_format migrates atomically to v5 (SQSHDB05)
    {
        let mut rw = Database::open(&path).unwrap();
        assert_eq!(rw.migrated_from_version(), Some(FORMAT_VERSION_V4));
        assert!(rw.upgrade_format().unwrap());
        assert_eq!(rw.format_version(), FORMAT_VERSION);
        assert_eq!(rw.migrated_from_version(), None);

        // Now add v5 features (CHECK + FOREIGN KEY + composite B-tree index) on migrated DB!
        rw.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES legacy(id) ON DELETE CASCADE,
                qty INTEGER CHECK (qty > 0)
             );
             INSERT INTO child VALUES (10, 1, 5);
             CREATE INDEX idx_child_comp ON child (parent_id, qty);",
        )
        .unwrap();
        rw.checkpoint().unwrap();
    }

    // Verify upgraded file opens cleanly as v5 with all v4 + v5 data bit-exact
    {
        let mut f = std::fs::File::open(&path).unwrap();
        let mut sb_a = [0u8; 8];
        let mut sb_b = [0u8; 8];
        f.read_exact(&mut sb_a).unwrap();
        f.seek(SeekFrom::Start(SUPERBLOCK_SIZE)).unwrap();
        f.read_exact(&mut sb_b).unwrap();
        assert!(
            &sb_a == MAGIC_V5 || &sb_b == MAGIC_V5,
            "Expected SQSHDB05 superblock after forward migration"
        );

        let mut db = Database::open(&path).unwrap();
        assert_eq!(db.format_version(), FORMAT_VERSION);
        let r = db
            .execute("SELECT id, val FROM legacy ORDER BY id ASC;")
            .unwrap();
        assert_eq!(rows_of(&r).len(), 3);
        assert!(rows_of(&r)[1][1].exact_eq(&Value::Real(-0.0)));
        assert_eq!(db.count_rows("child").unwrap(), 1);
    }

    // 3. Future unsupported version (v99 with valid CRC32) returns clear Schema error
    {
        let future_path = temp_db("format_future_v99");
        let mut buf = [0u8; SUPERBLOCK_SIZE as usize];
        buf[0..8].copy_from_slice(b"SQSHDB99");
        buf[8..12].copy_from_slice(&99u32.to_le_bytes());
        let crc = crc32fast::hash(&buf[0..508]);
        buf[508..512].copy_from_slice(&crc.to_le_bytes());

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&future_path)
            .unwrap();
        f.write_all(&buf).unwrap();
        f.write_all(&buf).unwrap();
        drop(f);

        let err = Database::open(&future_path);
        assert!(
            matches!(err, Err(Error::Schema(ref m)) if m.contains("v99")),
            "Expected clear version upgrade error for v99, got {:?}",
            err.err()
        );
        cleanup(&future_path);
    }

    cleanup(&path);
}

// ============================================================================
// 7. Direct .sqlite to .sqdb Migration (Pure Rust, Zero C Dependencies!)
// ============================================================================

fn write_sqlite_varint(v: u64, out: &mut Vec<u8>) {
    let mut chunks = Vec::new();
    let mut rem = v;
    chunks.push((rem & 0x7F) as u8);
    rem >>= 7;
    while rem > 0 {
        chunks.push(((rem & 0x7F) as u8) | 0x80);
        rem >>= 7;
    }
    chunks.reverse();
    out.extend_from_slice(&chunks);
}

fn build_sqlite_record(vals: &[Value]) -> Vec<u8> {
    let mut serials = Vec::new();
    let mut body = Vec::new();
    for v in vals {
        match v {
            Value::Null => write_sqlite_varint(0, &mut serials),
            Value::Integer(0) => write_sqlite_varint(8, &mut serials),
            Value::Integer(1) => write_sqlite_varint(9, &mut serials),
            Value::Integer(i) => {
                write_sqlite_varint(6, &mut serials);
                body.extend_from_slice(&i.to_be_bytes());
            }
            Value::Real(f) => {
                write_sqlite_varint(7, &mut serials);
                body.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Value::Text(s) => {
                let st = (s.len() as u64) * 2 + 13;
                write_sqlite_varint(st, &mut serials);
                body.extend_from_slice(s.as_bytes());
            }
            Value::Blob(b) => {
                let st = (b.len() as u64) * 2 + 12;
                write_sqlite_varint(st, &mut serials);
                body.extend_from_slice(b);
            }
        }
    }
    let mut hdr_len_bytes = Vec::new();
    write_sqlite_varint((serials.len() + 1) as u64, &mut hdr_len_bytes);
    let mut rec = Vec::new();
    write_sqlite_varint((serials.len() + hdr_len_bytes.len()) as u64, &mut rec);
    rec.extend_from_slice(&serials);
    rec.extend_from_slice(&body);
    rec
}

fn build_sqlite_leaf_page(page_size: usize, is_page1: bool, cells: &[(i64, Vec<u8>)]) -> Vec<u8> {
    let mut page = vec![0u8; page_size];
    let hdr_off = if is_page1 {
        page[0..16].copy_from_slice(b"SQLite format 3\0");
        page[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        page[18] = 1;
        page[19] = 1;
        page[21] = 64;
        page[22] = 32;
        page[23] = 32;
        page[56..60].copy_from_slice(&1u32.to_be_bytes()); // UTF-8
        100usize
    } else {
        0usize
    };
    page[hdr_off] = 0x0D; // Leaf table B-tree page
    page[hdr_off + 3..hdr_off + 5].copy_from_slice(&(cells.len() as u16).to_be_bytes());

    let mut content_end = page_size;
    let mut ptrs = Vec::with_capacity(cells.len());
    for (rowid, payload) in cells {
        let mut cell = Vec::new();
        write_sqlite_varint(payload.len() as u64, &mut cell);
        write_sqlite_varint(*rowid as u64, &mut cell);
        cell.extend_from_slice(payload);
        content_end -= cell.len();
        page[content_end..content_end + cell.len()].copy_from_slice(&cell);
        ptrs.push(content_end as u16);
    }
    page[hdr_off + 5..hdr_off + 7].copy_from_slice(&(content_end as u16).to_be_bytes());
    let ptr_start = hdr_off + 8;
    for (i, ptr) in ptrs.iter().enumerate() {
        page[ptr_start + i * 2..ptr_start + i * 2 + 2].copy_from_slice(&ptr.to_be_bytes());
    }
    page
}

#[test]
fn test_direct_sqlite_to_sqdb_migration() {
    let sqlite_path = temp_db("source_sqlite").with_extension("sqlite");
    let sqdb_path = temp_db("migrated_from_sqlite");

    // Construct a self-contained 2-page SQLite 3 database file (zero external tools needed!)
    let page_size = 4096usize;
    let schema_cells = vec![
        (
            1i64,
            build_sqlite_record(&[
                Value::Text("table".into()),
                Value::Text("items".into()),
                Value::Text("items".into()),
                Value::Integer(2),
                Value::Text(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT NOT NULL, price REAL CHECK (price >= 0.0), raw BLOB)".into(),
                ),
            ]),
        ),
        (
            2i64,
            build_sqlite_record(&[
                Value::Text("index".into()),
                Value::Text("idx_sku".into()),
                Value::Text("items".into()),
                Value::Integer(0),
                Value::Text("CREATE INDEX idx_sku ON items (sku)".into()),
            ]),
        ),
    ];
    let data_cells = vec![
        (
            10i64,
            build_sqlite_record(&[
                Value::Null, // SQLite stores NULL in INTEGER PRIMARY KEY slot -> aliases rowid 10!
                Value::Text("SKU-001".into()),
                Value::Real(19.99),
                Value::Blob(vec![0xCA, 0xFE, 0xBA, 0xBE]),
            ]),
        ),
        (
            20i64,
            build_sqlite_record(&[
                Value::Null,
                Value::Text("SKU-002".into()),
                Value::Real(49.5),
                Value::Null,
            ]),
        ),
    ];

    let mut file_bytes = build_sqlite_leaf_page(page_size, true, &schema_cells);
    file_bytes.extend_from_slice(&build_sqlite_leaf_page(page_size, false, &data_cells));
    std::fs::write(&sqlite_path, &file_bytes).unwrap();

    let report = migrate_sqlite(&sqlite_path, &sqdb_path, MigrateOptions::default()).unwrap();
    assert_eq!(report.tables_migrated, 1);
    assert_eq!(report.indexes_migrated, 1);
    assert_eq!(report.total_rows, 2);

    let mut db = Database::open(&sqdb_path).unwrap();
    let res = db
        .execute("SELECT id, sku, price, raw FROM items ORDER BY id ASC;")
        .unwrap();
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 2);
    assert!(rows[0][0].exact_eq(&Value::Integer(10)));
    assert!(rows[0][1].exact_eq(&Value::Text("SKU-001".into())));
    assert!(rows[0][2].exact_eq(&Value::Real(19.99)));
    assert!(rows[0][3].exact_eq(&Value::Blob(vec![0xCA, 0xFE, 0xBA, 0xBE])));
    assert!(rows[1][0].exact_eq(&Value::Integer(20)));
    assert!(rows[1][3].is_null());

    // Migrated index & CHECK constraint work on the resulting .sqdb!
    let exp = db
        .execute("EXPLAIN SELECT price FROM items WHERE sku = 'SKU-002';")
        .unwrap();
    assert!(plan_of(&exp).contains("index='idx_sku'"));
    assert!(db
        .execute("INSERT INTO items VALUES (30, 'BAD', -5.0, NULL);")
        .is_err());

    drop(db);
    let _ = std::fs::remove_file(&sqlite_path);
    cleanup(&sqdb_path);
    let _ = HashMap::<String, Vec<String>>::new();
    let _ = IndexKind::BTree;
}

// ============================================================================
// 8. Regression Tests from Multi-Model Code Audits (Codex + Beam Free)
// ============================================================================

#[test]
fn test_audit_parallel_pool_panic_safety_leader_and_worker() {
    use squishdb::parallel::ParallelPool;
    let pool = ParallelPool::new(4);
    let items: Vec<usize> = (0..16).collect();

    // 1. Panic in worker shard (non-zero chunk) must propagate cleanly, never drop results silently.
    let res_worker = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.execute_indexed(&items, |i, &v| {
            if i == 10 {
                panic!("worker boom");
            }
            v * 2
        })
    }));
    assert!(res_worker.is_err(), "Worker panic must propagate to caller");

    // 2. Panic in caller shard 0 must wait for all dispatched workers before unwinding (no UB).
    let res_leader = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.execute_indexed(&items, |i, &v| {
            if i == 0 {
                panic!("leader boom");
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
            v + 1
        })
    }));
    assert!(res_leader.is_err(), "Leader panic must propagate cleanly");

    // 3. Pool remains usable after panics!
    let ok = pool.execute_indexed(&items, |_, &v| v * 3);
    assert_eq!(ok.len(), 16);
    assert_eq!(ok[5], 15);
}

#[test]
fn test_audit_alter_add_column_constraints_survive_uncheckpointed_wal_replay() {
    let path = temp_db("audit_alter_wal");
    let recovered = temp_db("audit_alter_wal_recovered");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY);
             INSERT INTO parents VALUES (1);
             CREATE TABLE items (id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        db.checkpoint().unwrap();

        // Add columns with CHECK and FOREIGN KEY WITHOUT checkpointing (stays in WAL!)
        db.execute("ALTER TABLE items ADD COLUMN qty INTEGER DEFAULT 1 CHECK (qty > 0);")
            .unwrap();
        db.execute("ALTER TABLE items ADD COLUMN parent_id INTEGER REFERENCES parents(id);")
            .unwrap();

        // Copy raw DB + uncheckpointed WAL snapshot to simulate abrupt crash before checkpoint
        std::fs::copy(&path, &recovered).unwrap();
        let mut src_wal = path.as_os_str().to_os_string();
        src_wal.push("-wal");
        let mut dst_wal = recovered.as_os_str().to_os_string();
        dst_wal.push("-wal");
        std::fs::copy(PathBuf::from(src_wal), PathBuf::from(dst_wal)).unwrap();
    }

    {
        let mut db = Database::open(&recovered).unwrap();
        // CHECK constraint must still be enforced after WAL replay
        assert!(db
            .execute("INSERT INTO items VALUES (1, -5, 1);")
            .is_err());
        // FOREIGN KEY constraint must still be enforced after WAL replay
        assert!(db
            .execute("INSERT INTO items VALUES (2, 5, 999);")
            .is_err());
        // Valid insert succeeds
        db.execute("INSERT INTO items VALUES (3, 5, 1);").unwrap();
        assert_eq!(db.count_rows("items").unwrap(), 1);
    }
    cleanup(&path);
    cleanup(&recovered);
}

#[test]
fn test_audit_bulk_insert_enforces_unique_and_pk_constraints() {
    let path = temp_db("audit_bulk_unique");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE);")
            .unwrap();
        db.bulk_insert(
            "u",
            vec![
                vec![Value::Integer(1), Value::Text("a@x.com".into())],
                vec![Value::Integer(2), Value::Text("b@x.com".into())],
            ],
        )
        .unwrap();

        // Intra-batch duplicate UNIQUE must fail
        assert!(db
            .bulk_insert(
                "u",
                vec![
                    vec![Value::Integer(3), Value::Text("dup@x.com".into())],
                    vec![Value::Integer(4), Value::Text("dup@x.com".into())],
                ],
            )
            .is_err());

        // Cross-batch duplicate UNIQUE against existing rows must fail
        assert!(db
            .bulk_insert(
                "u",
                vec![vec![Value::Integer(5), Value::Text("a@x.com".into())]],
            )
            .is_err());

        // Duplicate explicit PRIMARY KEY must fail
        assert!(db
            .bulk_insert(
                "u",
                vec![vec![Value::Integer(1), Value::Text("new@x.com".into())]],
            )
            .is_err());

        assert_eq!(db.count_rows("u").unwrap(), 2);
    }
    cleanup(&path);
}

#[test]
fn test_audit_mvcc_vacuum_blocks_while_snapshot_open_and_fk_write_skew() {
    let path = temp_db("audit_mvcc_vac");
    {
        let shared = SharedDatabase::open(&path).unwrap();
        let mut setup = shared.connect();
        setup
            .execute(
                "CREATE TABLE parents (id INTEGER PRIMARY KEY);
                 CREATE TABLE children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id) ON DELETE CASCADE);
                 INSERT INTO parents VALUES (1), (2);",
            )
            .unwrap();

        // Active snapshot blocks VACUUM
        let mut reader = shared.connect_readonly();
        reader.begin().unwrap();
        assert!(matches!(shared.vacuum(), Err(Error::Busy(_))));
        reader.rollback().unwrap();
        // Once snapshot closes, VACUUM succeeds
        shared.vacuum().unwrap();

        // Concurrent parent DELETE + child INSERT write-skew caught at commit time!
        let mut t_del = shared.connect();
        let mut t_ins = shared.connect();
        t_del.begin().unwrap();
        t_ins.begin().unwrap();

        t_ins
            .execute("INSERT INTO children VALUES (10, 1);")
            .unwrap();
        t_del.execute("DELETE FROM parents WHERE id = 1;").unwrap();

        t_ins.commit().unwrap();
        assert!(
            t_del.commit().is_err(),
            "Deleting parent referenced by concurrently committed child must fail!"
        );
    }
    cleanup(&path);
}

#[test]
fn test_audit_sqlite_corrupted_wal_frame_checksum_rejected() {
    let sqlite_path = temp_db("audit_sqlite_wal").with_extension("sqlite");
    let wal_path = {
        let mut os = sqlite_path.as_os_str().to_os_string();
        os.push("-wal");
        PathBuf::from(os)
    };
    let sqdb_path = temp_db("audit_sqlite_wal_out");

    let page_size = 4096usize;
    let schema_cells = vec![(
        1i64,
        build_sqlite_record(&[
            Value::Text("table".into()),
            Value::Text("t".into()),
            Value::Text("t".into()),
            Value::Integer(2),
            Value::Text("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)".into()),
        ]),
    )];
    let clean_cells = vec![(
        1i64,
        build_sqlite_record(&[Value::Null, Value::Text("clean".into())]),
    )];
    let mut base = build_sqlite_leaf_page(page_size, true, &schema_cells);
    base.extend_from_slice(&build_sqlite_leaf_page(page_size, false, &clean_cells));
    std::fs::write(&sqlite_path, &base).unwrap();

    // Craft a WAL with matching magic/salts/pgno/commit_pages, but BOGUS checksum + corrupted page!
    let mut bad_wal = vec![0u8; 32 + 24 + page_size];
    bad_wal[0..4].copy_from_slice(&0x377f_0682u32.to_be_bytes());
    bad_wal[8..12].copy_from_slice(&(page_size as u32).to_be_bytes());
    // Frame 1 targeting page 2 with bogus checksum
    bad_wal[32..36].copy_from_slice(&2u32.to_be_bytes());
    bad_wal[36..40].copy_from_slice(&2u32.to_be_bytes());
    bad_wal[56..56 + page_size].fill(0xAA);
    std::fs::write(&wal_path, &bad_wal).unwrap();

    // Migration must ignore the invalid-checksum WAL and read the clean base page!
    let rep = migrate_sqlite(&sqlite_path, &sqdb_path, MigrateOptions::default()).unwrap();
    assert_eq!(rep.total_rows, 1);
    let mut db = Database::open(&sqdb_path).unwrap();
    let res = db.execute("SELECT v FROM t WHERE id = 1;").unwrap();
    assert!(rows_of(&res)[0][0].exact_eq(&Value::Text("clean".into())));

    drop(db);
    let _ = std::fs::remove_file(&sqlite_path);
    let _ = std::fs::remove_file(&wal_path);
    cleanup(&sqdb_path);
}

use std::path::{Path, PathBuf};
use std::process::Command;

use squishdb::sqlite_import::{migrate_sqlite, MigrateOptions};
use squishdb::types::{Error, Value};
use squishdb::{Database, DbOptions, QueryResult, SharedDatabase};

fn temp_db(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "squishdb_v06_{}_{}.sqdb",
        name,
        std::process::id()
    ));
    cleanup(&p);
    p
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    for suffix in [
        "-wal",
        ".lock",
        "-mvcc.commit.lock",
        "-mvcc.snap.lock",
        "-mvcc.log",
        ".vacuum",
    ] {
        let mut os = path.as_os_str().to_os_string();
        os.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(os));
    }
}

fn rows_of(results: &[QueryResult]) -> &[Vec<Value>] {
    for r in results.iter().rev() {
        if let QueryResult::Rows { rows, .. } = r {
            return rows;
        }
    }
    panic!("No QueryResult::Rows in {:?}", results);
}

fn cols_of(results: &[QueryResult]) -> &[String] {
    for r in results.iter().rev() {
        if let QueryResult::Rows { columns, .. } = r {
            return columns;
        }
    }
    panic!("No QueryResult::Rows in {:?}", results);
}

fn explain_text(results: &[QueryResult]) -> String {
    for r in results {
        if let QueryResult::Message(s) = r {
            return s.clone();
        }
    }
    panic!("No EXPLAIN message in {:?}", results);
}

// ============================================================================
// 1. Cost-Based Join Reordering & Predicate Pushdown
// ============================================================================

#[test]
fn test_cost_based_join_reordering_select_star_and_left_join_safety() {
    let path = temp_db("join_reorder");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE regions (id INTEGER PRIMARY KEY, code TEXT UNIQUE);
             CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, tier TEXT);
             CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER);
             CREATE TABLE refunds (id INTEGER PRIMARY KEY, order_id INTEGER, reason TEXT);",
        )
        .unwrap();

        let region_rows: Vec<Vec<Value>> = ["US", "EU", "AP", "SA", "AF"]
            .iter()
            .enumerate()
            .map(|(i, code)| vec![Value::Integer((i + 1) as i64), Value::Text((*code).into())])
            .collect();
        db.bulk_insert("regions", region_rows).unwrap();

        let mut cust_rows = Vec::with_capacity(400);
        for i in 1..=400i64 {
            cust_rows.push(vec![
                Value::Integer(i),
                Value::Integer((i % 5) + 1),
                Value::Text(if i % 2 == 0 { "pro" } else { "free" }.into()),
            ]);
        }
        db.bulk_insert("customers", cust_rows).unwrap();

        let mut order_rows = Vec::with_capacity(3000);
        for i in 1..=3000i64 {
            order_rows.push(vec![
                Value::Integer(i),
                Value::Integer((i % 400) + 1),
                Value::Integer((i % 50) * 10),
            ]);
        }
        db.bulk_insert("orders", order_rows).unwrap();
        db.execute("INSERT INTO refunds VALUES (1, 1, 'damaged'), (2, 2, 'late');")
            .unwrap();
        db.checkpoint().unwrap();

        // Syntactic order starts with 3000-row `orders`, then 400-row `customers`, then 5-row `regions`
        // filtered down to 1 row (`r.code = 'EU'`). Cost-based DP must reorder to start with `r(1)`!
        let plan_res = db
            .execute(
                "EXPLAIN SELECT o.id, c.tier, r.code, o.amount
                 FROM orders o
                 INNER JOIN customers c ON o.cust_id = c.id
                 INNER JOIN regions r ON c.region_id = r.id
                 WHERE r.code = 'EU' AND o.amount >= 400;",
            )
            .unwrap();
        let plan = explain_text(&plan_res);
        assert!(
            plan.contains("COST-BASED JOIN ORDER [r(1) -> c("),
            "Expected cost-based join order starting with filtered r(1) -> c(...), got:\n{}",
            plan
        );

        // Verify query result matches manual filter count.
        let res = db
            .execute(
                "SELECT COUNT(*), SUM(o.amount)
                 FROM orders o
                 INNER JOIN customers c ON o.cust_id = c.id
                 INNER JOIN regions r ON c.region_id = r.id
                 WHERE r.code = 'EU' AND o.amount >= 400;",
            )
            .unwrap();
        let expected_cnt: i64 = (1..=3000i64)
            .filter(|&oid| {
                let cid = (oid % 400) + 1;
                let rid = (cid % 5) + 1;
                let amt = (oid % 50) * 10;
                rid == 2 && amt >= 400
            })
            .count() as i64;
        let expected_sum: i64 = (1..=3000i64)
            .filter_map(|oid| {
                let cid = (oid % 400) + 1;
                let rid = (cid % 5) + 1;
                let amt = (oid % 50) * 10;
                if rid == 2 && amt >= 400 {
                    Some(amt)
                } else {
                    None
                }
            })
            .sum();
        assert!(rows_of(&res)[0][0].exact_eq(&Value::Integer(expected_cnt)));
        assert!(rows_of(&res)[0][1].exact_eq(&Value::Integer(expected_sum)));

        // SELECT * must preserve syntactic FROM/JOIN column order even when physical join order flipped!
        let star = db
            .execute(
                "SELECT *
                 FROM orders o
                 INNER JOIN regions r ON o.id = r.id
                 WHERE r.code = 'US';",
            )
            .unwrap();
        assert_eq!(
            cols_of(&star),
            &["id", "cust_id", "amount", "id", "code"],
            "SELECT * must keep syntactic FROM/JOIN column order"
        );
        assert_eq!(rows_of(&star).len(), 1);
        assert!(rows_of(&star)[0][0].exact_eq(&Value::Integer(1)));
        assert!(rows_of(&star)[0][4].exact_eq(&Value::Text("US".into())));

        // INNER JOIN followed by LEFT JOIN + anti-join `WHERE rf.id IS NULL` must preserve LEFT JOIN semantics!
        let left_res = db
            .execute(
                "SELECT o.id, rf.reason
                 FROM orders o
                 INNER JOIN customers c ON o.cust_id = c.id
                 LEFT JOIN refunds rf ON o.id = rf.order_id
                 WHERE o.id <= 4 AND rf.id IS NULL
                 ORDER BY o.id ASC;",
            )
            .unwrap();
        let lrows = rows_of(&left_res);
        assert_eq!(lrows.len(), 2);
        assert!(lrows[0][0].exact_eq(&Value::Integer(3)));
        assert!(lrows[0][1].is_null());
        assert!(lrows[1][0].exact_eq(&Value::Integer(4)));
        assert!(lrows[1][1].is_null());

        // Composite multi-column equi-join + residual non-equi predicate
        db.execute(
            "CREATE TABLE m1 (k1 TEXT, k2 INTEGER, v INTEGER);
             CREATE TABLE m2 (k1 TEXT, k2 INTEGER, min_v INTEGER);
             INSERT INTO m1 VALUES ('a', 1, 10), ('a', 1, 50), ('a', 2, 100);
             INSERT INTO m2 VALUES ('a', 1, 20), ('a', 2, 200);",
        )
        .unwrap();
        let comp = db
            .execute(
                "SELECT m1.k1, m1.k2, m1.v
                 FROM m1
                 INNER JOIN m2 ON m1.k1 = m2.k1 AND m1.k2 = m2.k2 AND m1.v > m2.min_v;",
            )
            .unwrap();
        assert_eq!(rows_of(&comp).len(), 1);
        assert!(rows_of(&comp)[0][2].exact_eq(&Value::Integer(50)));
    }
    cleanup(&path);
}

// ============================================================================
// 2. Multi-Column Vectorized GROUP BY (DictText Flat-Array + General + Parallel)
// ============================================================================

#[test]
fn test_multi_column_vectorized_group_by_dict_mixed_nulls_and_parallel() {
    let path = temp_db("multi_vec_gb");
    {
        let mut opts = DbOptions::default();
        opts.parallel_workers = 4;
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute(
            "CREATE TABLE sales (
                id INTEGER PRIMARY KEY,
                region TEXT,
                tier TEXT,
                bucket INTEGER,
                amount REAL
             );",
        )
        .unwrap();

        // Insert 20,000 rows (5 PAX pages) so parallel worker pool triggers!
        let mut rows = Vec::with_capacity(20_000);
        let regions = ["us", "eu", "ap", "latam"];
        let tiers = ["free", "pro", "ent"];
        for i in 0..20_000i64 {
            let reg = if i % 100 == 0 {
                Value::Null
            } else {
                Value::Text(regions[(i % 4) as usize].into())
            };
            let tier = if i % 250 == 0 {
                Value::Null
            } else {
                Value::Text(tiers[(i % 3) as usize].into())
            };
            let bucket = if i % 500 == 0 {
                Value::Null
            } else {
                Value::Integer(i % 5)
            };
            rows.push(vec![
                Value::Integer(i + 1),
                reg,
                tier,
                bucket,
                Value::Real(((i % 100) + 1) as f64 * 1.5),
            ]);
        }
        db.bulk_insert("sales", rows).unwrap();
        db.checkpoint().unwrap();

        // 1. 2-column DictText GROUP BY (hits flat multi-dimensional code array + parallel gather)
        let plan_res = db
            .execute(
                "EXPLAIN SELECT region, tier, COUNT(*) AS cnt, SUM(amount) AS total
                 FROM sales
                 GROUP BY region, tier
                 ORDER BY region ASC, tier ASC;",
            )
            .unwrap();
        let plan = explain_text(&plan_res);
        assert!(
            plan.contains("VECTORIZED COLUMNAR SCAN + STREAMING HASH AGGREGATE")
                && plan.contains("GROUP BY (region, tier)")
                && plan.contains("PARALLEL"),
            "Expected parallel multi-column vectorized GROUP BY plan, got:\n{}",
            plan
        );

        let vec_res = db
            .execute(
                "SELECT region, tier, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount), MIN(bucket), MAX(bucket)
                 FROM sales
                 WHERE amount > 15.0
                 GROUP BY region, tier
                 HAVING cnt >= 50
                 ORDER BY region ASC, tier ASC;",
            )
            .unwrap();

        // Force row-based fallback (using `bucket + 0` expression in GROUP BY) for oracle verification!
        let ref_res = db
            .execute(
                "SELECT region, tier, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount), MIN(bucket), MAX(bucket)
                 FROM sales
                 WHERE amount > 15.0 AND id > -1
                 GROUP BY region, tier
                 HAVING cnt >= 50 AND 1 = 1
                 ORDER BY region ASC, tier ASC;",
            )
            .unwrap();
        assert_eq!(rows_of(&vec_res).len(), rows_of(&ref_res).len());
        assert!(!rows_of(&vec_res).is_empty());
        for (vr, rr) in rows_of(&vec_res).iter().zip(rows_of(&ref_res).iter()) {
            assert_eq!(vr.len(), rr.len());
            for (a, b) in vr.iter().zip(rr.iter()) {
                match (a, b) {
                    (Value::Real(fa), Value::Real(fb)) => {
                        assert!((fa - fb).abs() < 1e-6, "Float mismatch: {} vs {}", fa, fb);
                    }
                    _ => assert!(a.exact_eq(b), "Mismatch: {:?} vs {:?}", a, b),
                }
            }
        }

        // 2. 3-column mixed-type (Text, Text, Integer) GROUP BY (general zero-alloc probe path)
        let plan3 = explain_text(
            &db.execute(
                "EXPLAIN SELECT region, tier, bucket, COUNT(*) AS c, SUM(amount)
                 FROM sales
                 GROUP BY region, tier, bucket;",
            )
            .unwrap(),
        );
        assert!(
            plan3.contains("VECTORIZED COLUMNAR SCAN")
                && plan3.contains("GROUP BY (region, tier, bucket)"),
            "Expected 3-column vectorized plan, got:\n{}",
            plan3
        );
        let vec3 = db
            .execute(
                "SELECT region, tier, bucket, COUNT(*) AS c, SUM(amount)
                 FROM sales
                 GROUP BY region, tier, bucket
                 ORDER BY region ASC, tier ASC, bucket ASC;",
            )
            .unwrap();
        let ref3 = db
            .execute(
                "SELECT region, tier, bucket, COUNT(*) AS c, SUM(amount)
                 FROM sales
                 GROUP BY region, tier, bucket
                 HAVING 1 = 1
                 ORDER BY region ASC, tier ASC, bucket ASC;",
            )
            .unwrap();
        assert_eq!(rows_of(&vec3).len(), rows_of(&ref3).len());
        for (vr, rr) in rows_of(&vec3).iter().zip(rows_of(&ref3).iter()) {
            assert!(vr[0].exact_eq(&rr[0]));
            assert!(vr[1].exact_eq(&rr[1]));
            assert!(vr[2].exact_eq(&rr[2]));
            assert!(vr[3].exact_eq(&rr[3]));
            if let (Value::Real(fa), Value::Real(fb)) = (&vr[4], &rr[4]) {
                assert!((fa - fb).abs() < 1e-6);
            }
        }
    }
    cleanup(&path);
}

// ============================================================================
// 3. ALTER TABLE DROP COLUMN and RENAME COLUMN
// ============================================================================

#[test]
fn test_alter_table_drop_column_and_rename_column_full_lifecycle() {
    let path = temp_db("alter_drop_rename");
    let recovered = temp_db("alter_drop_rename_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE parents (
                temp_drop TEXT,
                pid INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                score INTEGER CHECK (score >= 0),
                notes TEXT
             ) CLUSTER BY (code, notes);
             CREATE INDEX idx_parents_code ON parents (code);
             CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(pid) ON DELETE RESTRICT,
                label TEXT
             );
             INSERT INTO parents VALUES ('x', 1, 'A', 10, 'n1'), ('y', 2, 'B', 20, 'n2');
             INSERT INTO children VALUES (100, 1, 'c1');",
        )
        .unwrap();
        db.checkpoint().unwrap();

        // Validation guards for DROP COLUMN
        assert!(db.execute("ALTER TABLE parents DROP COLUMN pid;").is_err()); // PK
        assert!(db.execute("ALTER TABLE parents DROP COLUMN code;").is_err()); // Indexed
        assert!(db.execute("ALTER TABLE parents DROP COLUMN score;").is_err()); // Referenced by CHECK
        assert!(db.execute("ALTER TABLE children DROP COLUMN parent_id;").is_err()); // Local FK
        assert!(db.execute("ALTER TABLE parents DROP COLUMN missing;").is_err()); // Missing
        // IF EXISTS on missing column is a clean no-op
        db.execute("ALTER TABLE parents DROP COLUMN IF EXISTS missing;")
            .unwrap();

        // Drop a column BEFORE the INTEGER PRIMARY KEY (tests rowid + schema re-indexing across PAX pages!)
        db.execute("INSERT INTO parents VALUES ('z', 3, 'C', 30, 'n3');")
            .unwrap(); // unflushed hot row mixed with checkpointed PAX rows!
        db.execute("ALTER TABLE parents DROP COLUMN temp_drop;")
            .unwrap();
        db.execute("ALTER TABLE parents DROP COLUMN notes;")
            .unwrap();

        // Verify schema, data, remaining index, and CLUSTER BY cleanup
        let res = db
            .execute("SELECT pid, code, score FROM parents ORDER BY pid ASC;")
            .unwrap();
        let rows = rows_of(&res);
        assert_eq!(rows.len(), 3);
        assert!(rows[0][0].exact_eq(&Value::Integer(1)));
        assert!(rows[0][1].exact_eq(&Value::Text("A".into())));
        assert!(rows[2][0].exact_eq(&Value::Integer(3)));
        assert!(rows[2][2].exact_eq(&Value::Integer(30)));
        assert_eq!(db.table_schema("parents").unwrap().cluster_by, vec!["code"]);

        // Test savepoint rollback of DROP COLUMN and RENAME COLUMN
        db.begin().unwrap();
        db.savepoint("sp_ddl").unwrap();
        db.execute("ALTER TABLE parents RENAME COLUMN score TO points;")
            .unwrap();
        db.execute("ALTER TABLE children DROP COLUMN label;")
            .unwrap();
        assert!(db.table_schema("parents").unwrap().col_index("points").is_some());
        assert!(db.table_schema("children").unwrap().col_index("label").is_none());
        db.rollback_to_savepoint("sp_ddl").unwrap();
        db.release_savepoint("sp_ddl").unwrap();
        db.commit().unwrap();
        assert!(db.table_schema("parents").unwrap().col_index("score").is_some());
        assert!(db.table_schema("children").unwrap().col_index("label").is_some());

        // Checkpoint after DROP COLUMN so PAX pages + remaining index rebuild cleanly on disk
        db.checkpoint().unwrap();

        // RENAME COLUMN across CHECK constraint, index, CLUSTER BY, and referenced FK PK column!
        db.execute("ALTER TABLE parents RENAME COLUMN score TO points;")
            .unwrap();
        db.execute("ALTER TABLE parents RENAME COLUMN code TO sku;")
            .unwrap();
        db.execute("ALTER TABLE parents RENAME COLUMN pid TO parent_pk;")
            .unwrap();
        db.execute("ALTER TABLE children RENAME COLUMN parent_id TO p_ref;")
            .unwrap();

        // CHECK constraint must now enforce `points >= 0`
        assert!(db
            .execute("INSERT INTO parents VALUES (4, 'D', -1);")
            .is_err());
        db.execute("INSERT INTO parents VALUES (4, 'D', 40);")
            .unwrap();

        // Foreign key must now enforce `children.p_ref -> parents.parent_pk`
        assert!(db
            .execute("INSERT INTO children VALUES (101, 999, 'bad');")
            .is_err());
        assert!(db
            .execute("DELETE FROM parents WHERE parent_pk = 1;")
            .is_err());
        db.execute("INSERT INTO children VALUES (101, 4, 'ok');")
            .unwrap();

        // Indexed lookup on renamed column `sku` must work
        let sku_res = db
            .execute("SELECT parent_pk, points FROM parents WHERE sku = 'B';")
            .unwrap();
        assert_eq!(rows_of(&sku_res).len(), 1);
        assert!(rows_of(&sku_res)[0][0].exact_eq(&Value::Integer(2)));

        // Copy raw DB + uncheckpointed WAL to verify WAL crash replay of RENAME + DROP
        std::fs::copy(&path, &recovered).unwrap();
        let mut src_wal = path.as_os_str().to_os_string();
        src_wal.push("-wal");
        let mut dst_wal = recovered.as_os_str().to_os_string();
        dst_wal.push("-wal");
        std::fs::copy(PathBuf::from(src_wal), PathBuf::from(dst_wal)).unwrap();
    }

    // Reopen recovered DB from uncheckpointed WAL and verify all renames, drops, CHECKs, and FKs!
    {
        let mut db = Database::open(&recovered).unwrap();
        let res = db
            .execute("SELECT parent_pk, sku, points FROM parents ORDER BY parent_pk ASC;")
            .unwrap();
        assert_eq!(rows_of(&res).len(), 4);
        assert!(rows_of(&res)[3][1].exact_eq(&Value::Text("D".into())));
        assert!(db
            .execute("INSERT INTO parents VALUES (5, 'E', -5);")
            .is_err());
        assert!(db
            .execute("INSERT INTO children VALUES (102, 777, 'bad');")
            .is_err());
        assert_eq!(db.table_schema("parents").unwrap().cluster_by, vec!["sku"]);
    }

    cleanup(&path);
    cleanup(&recovered);
}

// ============================================================================
// 4. WITHOUT ROWID Support in `squishdb migrate` (Real SQLite 3 Multi-Page + Overflow)
// ============================================================================

#[test]
fn test_sqlite_migrate_without_rowid_real_sqlite_multipage_and_overflow() {
    let sqlite_path = temp_db("without_rowid_src").with_extension("sqlite");
    let sqdb_path = temp_db("without_rowid_out");

    // Generate a genuine SQLite 3 database with multi-page (interior 0x02 + leaf 0x0A)
    // WITHOUT ROWID tables, out-of-order composite PRIMARY KEY, and >4KB overflow blobs!
    let py_script = r#"
import sqlite3, sys
db_path = sys.argv[1]
con = sqlite3.connect(db_path)
con.execute("PRAGMA page_size = 1024;")
con.execute("""
    CREATE TABLE kv_comp (
        payload TEXT,
        id INTEGER,
        score REAL,
        region TEXT,
        PRIMARY KEY (region, id)
    ) WITHOUT ROWID
""")
con.execute("""
    CREATE TABLE dict_text (
        word TEXT PRIMARY KEY,
        freq INTEGER NOT NULL
    ) WITHOUT ROWID
""")
con.execute("""
    CREATE TABLE big_blobs (
        tag TEXT,
        k INTEGER PRIMARY KEY,
        raw BLOB
    ) WITHOUT ROWID
""")
regions = ["ap", "eu", "us"]
rows = [(f"val-{r}-{i}", i, i * 0.25, r) for r in regions for i in range(1, 301)]
con.executemany("INSERT INTO kv_comp VALUES (?, ?, ?, ?)", rows)
con.executemany("INSERT INTO dict_text VALUES (?, ?)", [(f"word_{i:04d}", i * 10) for i in range(1, 201)])
# Insert >3KB blobs on a 1024-byte page database to force multi-page index overflow chains!
big_rows = [(f"t{i}", i * 100, bytes([(i + j) % 251 for j in range(3200)])) for i in range(1, 15)]
con.executemany("INSERT INTO big_blobs VALUES (?, ?, ?)", big_rows)
con.commit()
con.close()
"#;

    let py_status = Command::new("python")
        .arg("-c")
        .arg(py_script)
        .arg(&sqlite_path)
        .status()
        .or_else(|_| {
            Command::new("python3")
                .arg("-c")
                .arg(py_script)
                .arg(&sqlite_path)
                .status()
        })
        .expect("Python with sqlite3 is required for WITHOUT ROWID migration test");
    assert!(py_status.success(), "Failed to generate SQLite WITHOUT ROWID test DB");

    let report = migrate_sqlite(&sqlite_path, &sqdb_path, MigrateOptions::default()).unwrap();
    assert_eq!(report.tables_migrated, 3);
    assert_eq!(report.total_rows, 900 + 200 + 14);

    {
        let mut db = Database::open(&sqdb_path).unwrap();

        // 1. Verify composite PK WITHOUT ROWID column remapping (declared order: payload, id, score, region)
        let r1 = db
            .execute(
                "SELECT payload, id, score, region
                 FROM kv_comp
                 WHERE region = 'eu' AND id = 150;",
            )
            .unwrap();
        assert_eq!(rows_of(&r1).len(), 1);
        assert!(rows_of(&r1)[0][0].exact_eq(&Value::Text("val-eu-150".into())));
        assert!(rows_of(&r1)[0][1].exact_eq(&Value::Integer(150)));
        assert!(rows_of(&r1)[0][2].exact_eq(&Value::Real(37.5)));
        assert!(rows_of(&r1)[0][3].exact_eq(&Value::Text("eu".into())));

        // Verify exact row count across all interior + leaf index B-tree pages
        assert_eq!(db.count_rows("kv_comp").unwrap(), 900);

        // Verify composite PK uniqueness index was created and rejects duplicates
        assert!(
            db.execute("INSERT INTO kv_comp VALUES ('dup', 150, 1.0, 'eu');")
                .is_err(),
            "Composite PK uniqueness must be enforced after migrating WITHOUT ROWID table"
        );

        // 2. Verify single-column TEXT PRIMARY KEY WITHOUT ROWID
        assert_eq!(db.count_rows("dict_text").unwrap(), 200);
        let r2 = db
            .execute("SELECT word, freq FROM dict_text WHERE word = 'word_0042';")
            .unwrap();
        assert_eq!(rows_of(&r2).len(), 1);
        assert!(rows_of(&r2)[0][1].exact_eq(&Value::Integer(420)));

        // 3. Verify overflow blob payload integrity + column order (tag, k, raw)
        assert_eq!(db.count_rows("big_blobs").unwrap(), 14);
        let r3 = db
            .execute("SELECT tag, k, raw FROM big_blobs WHERE k = 700;")
            .unwrap();
        assert_eq!(rows_of(&r3).len(), 1);
        assert!(rows_of(&r3)[0][0].exact_eq(&Value::Text("t7".into())));
        let expected_blob: Vec<u8> = (0..3200).map(|j| ((7 + j) % 251) as u8).collect();
        assert!(rows_of(&r3)[0][2].exact_eq(&Value::Blob(expected_blob)));
    }

    let _ = std::fs::remove_file(&sqlite_path);
    cleanup(&sqdb_path);
}

// ============================================================================
// 5. Cross-Process Multi-Writer MVCC (Real OS Child Processes!)
// ============================================================================

/// Child process entrypoint invoked by `test_cross_process_mvcc_multi_writer_conflict_and_vacuum`.
#[test]
fn xproc_mvcc_worker_helper() {
    let Ok(mode) = std::env::var("SQUISHDB_XPROC_MODE") else {
        return; // No-op when run directly by `cargo test`
    };
    let db_path = PathBuf::from(std::env::var("SQUISHDB_XPROC_DB").unwrap());
    let shared = SharedDatabase::open(&db_path).expect("child SharedDatabase::open failed");

    match mode.as_str() {
        "disjoint_writer" => {
            let worker_id: i64 = std::env::var("SQUISHDB_XPROC_WORKER_ID")
                .unwrap()
                .parse()
                .unwrap();
            let mut conn = shared.connect();
            for i in 0..30i64 {
                let id = worker_id * 1000 + i;
                conn.begin().unwrap();
                conn.execute(&format!(
                    "INSERT INTO items VALUES ({}, {}, 'w{}');",
                    id,
                    i * 10,
                    worker_id
                ))
                .unwrap();
                conn.commit().unwrap();
            }
        }
        "conflict_writer" => {
            let mut conn = shared.connect();
            conn.begin().unwrap();
            conn.execute("UPDATE accounts SET balance = balance + 50 WHERE id = 1;")
                .unwrap();
            conn.commit().unwrap();
        }
        "vacuum_while_snap" => {
            let res = shared.vacuum();
            assert!(
                matches!(res, Err(Error::Busy(_))),
                "Expected Error::Busy when another OS process holds a snapshot, got {:?}",
                res
            );
        }
        "vacuum_after_snap" => {
            shared.vacuum().expect("Cross-process VACUUM should succeed once snapshots closed");
        }
        other => panic!("Unknown SQUISHDB_XPROC_MODE: {}", other),
    }
}

#[test]
fn test_cross_process_mvcc_multi_writer_conflict_and_vacuum() {
    let path = temp_db("xproc_mvcc");
    let exe = std::env::current_exe().unwrap();

    // 1. Setup schema via SharedDatabase in parent process
    let shared = SharedDatabase::open(&path).unwrap();
    {
        let mut setup = shared.connect();
        setup
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER, src TEXT);
                 CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);
                 INSERT INTO accounts VALUES (1, 100), (2, 200);",
            )
            .unwrap();
    }

    // 2. Spawn 3 real separate OS processes writing concurrently via SharedDatabase::open!
    let mut children = Vec::new();
    for wid in 1..=3 {
        let child = Command::new(&exe)
            .arg("--exact")
            .arg("xproc_mvcc_worker_helper")
            .env("SQUISHDB_XPROC_MODE", "disjoint_writer")
            .env("SQUISHDB_XPROC_DB", &path)
            .env("SQUISHDB_XPROC_WORKER_ID", wid.to_string())
            .spawn()
            .expect("failed to spawn child MVCC writer process");
        children.push(child);
    }
    // Parent ALSO writes concurrently while the 3 child OS processes are committing!
    {
        let mut pconn = shared.connect();
        for i in 0..30i64 {
            let id = 4000 + i;
            pconn.begin().unwrap();
            pconn
                .execute(&format!("INSERT INTO items VALUES ({}, {}, 'parent');", id, i))
                .unwrap();
            pconn.commit().unwrap();
        }
    }
    for mut ch in children {
        let st = ch.wait().unwrap();
        assert!(st.success(), "Child MVCC writer process failed");
    }

    // Parent must immediately see all 4 * 30 = 120 rows committed across all 4 OS processes!
    {
        let mut reader = shared.connect_readonly();
        let res = reader.execute("SELECT COUNT(*) FROM items;").unwrap();
        assert!(
            rows_of(&res)[0][0].exact_eq(&Value::Integer(120)),
            "Expected 120 cross-process committed rows, got {:?}",
            rows_of(&res)
        );
    }

    // 3. Cross-process Snapshot Isolation + First-Committer-Wins write-write conflict!
    {
        let mut parent_tx = shared.connect();
        parent_tx.begin().unwrap();
        // Parent reads balance=100 on its snapshot, then writes balance=110 locally
        let before = parent_tx
            .execute("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        assert!(rows_of(&before)[0][0].exact_eq(&Value::Integer(100)));
        parent_tx
            .execute("UPDATE accounts SET balance = 110 WHERE id = 1;")
            .unwrap();

        // Spawn child OS process that updates the SAME row (id=1 -> 150) and commits FIRST!
        let st = Command::new(&exe)
            .arg("--exact")
            .arg("xproc_mvcc_worker_helper")
            .env("SQUISHDB_XPROC_MODE", "conflict_writer")
            .env("SQUISHDB_XPROC_DB", &path)
            .status()
            .unwrap();
        assert!(st.success(), "Child conflict_writer should commit first");

        // Parent's commit MUST fail with Error::Busy (cross-process First-Committer-Wins)!
        let commit_res = parent_tx.commit();
        assert!(
            matches!(commit_res, Err(Error::Busy(_))),
            "Expected cross-process MVCC conflict Error::Busy, got {:?}",
            commit_res
        );

        // Fresh reader in parent sees child's committed value (150)!
        let mut verify = shared.connect_readonly();
        let after = verify
            .execute("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        assert!(rows_of(&after)[0][0].exact_eq(&Value::Integer(150)));
    }

    // 4. Cross-process VACUUM snapshot guard + post-vacuum reload!
    {
        let mut snap = shared.connect_readonly();
        snap.begin().unwrap();
        let st_busy = Command::new(&exe)
            .arg("--exact")
            .arg("xproc_mvcc_worker_helper")
            .env("SQUISHDB_XPROC_MODE", "vacuum_while_snap")
            .env("SQUISHDB_XPROC_DB", &path)
            .status()
            .unwrap();
        assert!(st_busy.success(), "Child vacuum_while_snap check failed");
        snap.rollback().unwrap();

        // Once parent closes its snapshot, child OS process VACUUM succeeds!
        let st_ok = Command::new(&exe)
            .arg("--exact")
            .arg("xproc_mvcc_worker_helper")
            .env("SQUISHDB_XPROC_MODE", "vacuum_after_snap")
            .env("SQUISHDB_XPROC_DB", &path)
            .status()
            .unwrap();
        assert!(st_ok.success(), "Child vacuum_after_snap failed");

        // Parent seamlessly reloads the vacuumed file and reads all committed rows!
        let mut verify = shared.connect_readonly();
        let cnt = verify.execute("SELECT COUNT(*) FROM items;").unwrap();
        assert!(rows_of(&cnt)[0][0].exact_eq(&Value::Integer(120)));
        let bal = verify
            .execute("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        assert!(rows_of(&bal)[0][0].exact_eq(&Value::Integer(150)));
    }

    drop(shared);
    cleanup(&path);
}

#[test]
fn test_audit3_mvcc_torn_log_tail_recovery_and_sqlite_comment_without_rowid() {
    use std::io::Write;

    // 1. Torn -mvcc.log tail must be truncated under the commit lock so later commits remain readable.
    let path = temp_db("audit3_mvcc_torn");
    {
        let s1 = SharedDatabase::open(&path).unwrap();
        let mut c1 = s1.connect();
        c1.execute("CREATE TABLE logt (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO logt VALUES (1, 'before');")
            .unwrap();

        // Append a torn/corrupt partial frame to `<db>-mvcc.log`
        let log_path = {
            let mut os = path.as_os_str().to_os_string();
            os.push("-mvcc.log");
            PathBuf::from(os)
        };
        let mut lf = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        lf.write_all(b"SQMV\xDE\xAD\xBE\xEF\x00\x01\x02\x03\x04")
            .unwrap();
        drop(lf);

        // Second SharedDatabase commits after the torn tail; must truncate torn bytes cleanly.
        let s2 = SharedDatabase::open(&path).unwrap();
        let mut c2 = s2.connect();
        c2.execute("INSERT INTO logt VALUES (2, 'after');").unwrap();

        // First handle (and a fresh third handle) must observe the post-crash commit!
        let mut r1 = s1.connect_readonly();
        let res1 = r1.execute("SELECT id, v FROM logt ORDER BY id ASC;").unwrap();
        assert_eq!(rows_of(&res1).len(), 2);
        assert!(rows_of(&res1)[1][1].exact_eq(&Value::Text("after".into())));

        let s3 = SharedDatabase::open(&path).unwrap();
        assert!(s3.committed_version() >= s2.committed_version());
        let mut r3 = s3.connect_readonly();
        let res3 = r3.execute("SELECT COUNT(*) FROM logt;").unwrap();
        assert!(rows_of(&res3)[0][0].exact_eq(&Value::Integer(2)));
    }
    cleanup(&path);

    // 2. SQLite DDL with trailing `-- WITHOUT ROWID` comment on a normal table, and quoted `(` in
    //    table name on a genuine WITHOUT ROWID table.
    let sqlite_path = temp_db("audit3_sqlite_comments").with_extension("sqlite");
    let sqdb_path = temp_db("audit3_sqlite_comments_out");
    let py_script = r#"
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
con.execute("CREATE TABLE normal_tbl (id INTEGER PRIMARY KEY, v TEXT) -- WITHOUT ROWID")
con.execute("INSERT INTO normal_tbl VALUES (10, 'from_rowid')")
con.execute('CREATE TABLE "kv(v1)" (v TEXT, k TEXT PRIMARY KEY) WITHOUT ROWID')
con.execute('INSERT INTO "kv(v1)" VALUES (\'val1\', \'key1\')')
con.commit()
con.close()
"#;
    let st = Command::new("python")
        .arg("-c")
        .arg(py_script)
        .arg(&sqlite_path)
        .status()
        .or_else(|_| {
            Command::new("python3")
                .arg("-c")
                .arg(py_script)
                .arg(&sqlite_path)
                .status()
        })
        .unwrap();
    assert!(st.success());

    let rep = migrate_sqlite(&sqlite_path, &sqdb_path, MigrateOptions::default()).unwrap();
    assert_eq!(rep.tables_migrated, 2);
    assert_eq!(rep.total_rows, 2);
    {
        let mut db = Database::open(&sqdb_path).unwrap();
        let r_norm = db.execute("SELECT id, v FROM normal_tbl;").unwrap();
        assert!(rows_of(&r_norm)[0][0].exact_eq(&Value::Integer(10)));
        assert!(rows_of(&r_norm)[0][1].exact_eq(&Value::Text("from_rowid".into())));

        let r_wr = db.execute("SELECT v, k FROM \"kv(v1)\";").unwrap();
        assert!(rows_of(&r_wr)[0][0].exact_eq(&Value::Text("val1".into())));
        assert!(rows_of(&r_wr)[0][1].exact_eq(&Value::Text("key1".into())));
    }
    let _ = std::fs::remove_file(&sqlite_path);
    cleanup(&sqdb_path);
}

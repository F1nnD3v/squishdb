use std::path::PathBuf;
use std::time::Instant;

use squishdb::types::{Error, Value};
use squishdb::{Database, DbOptions, QueryResult};

fn temp_db(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_adv_{}_{}.sqdb", name, std::process::id()));
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

#[test]
fn bug_hunt_update_integer_primary_key_persists_across_checkpoint() {
    let path = temp_db("update_pk");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob');")
            .unwrap();
        db.checkpoint().unwrap();

        // Update the INTEGER PRIMARY KEY column itself!
        db.execute("UPDATE t SET id = 999 WHERE id = 1;").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let res = db.execute("SELECT id, name FROM t ORDER BY id ASC;").unwrap();
        let rows = rows_of(&res);
        assert_eq!(rows.len(), 2);
        assert!(rows[0][0].exact_eq(&Value::Integer(2)), "got {:?}", rows);
        assert!(rows[1][0].exact_eq(&Value::Integer(999)), "PK update reverted! got {:?}", rows);
        assert!(rows[1][1].exact_eq(&Value::Text("alice".into())));

        // Next auto-assigned PK must be > 999!
        db.execute("INSERT INTO t (name) VALUES ('carol');").unwrap();
        let res2 = db.execute("SELECT id FROM t WHERE name = 'carol';").unwrap();
        let id = match rows_of(&res2)[0][0] {
            Value::Integer(v) => v,
            _ => panic!("expected int"),
        };
        assert!(id > 999, "Expected next_rowid > 999, got {}", id);
    }
    cleanup(&path);
}

#[test]
fn bug_hunt_update_delete_index_consistency_before_and_after_checkpoint() {
    let path = temp_db("idx_mut");
    {
        let mut opts = DbOptions::default();
        opts.page_rows = 64;
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, score REAL);")
            .unwrap();
        let mut batch = Vec::new();
        for i in 1..=200i64 {
            batch.push(vec![
                Value::Integer(i),
                Value::Text(format!("u{}@test.com", i)),
                Value::Real(i as f64),
            ]);
        }
        db.bulk_insert("users", batch).unwrap();
        db.checkpoint().unwrap();

        db.execute("CREATE INDEX idx_email ON users (email);").unwrap();
        db.checkpoint().unwrap();

        // Update persisted row BEFORE checkpoint
        db.execute("UPDATE users SET email = 'renamed@test.com' WHERE id = 42;")
            .unwrap();
        // Delete another persisted row BEFORE checkpoint
        db.execute("DELETE FROM users WHERE id = 100;").unwrap();
        // Insert new row into hot buffer BEFORE checkpoint
        db.execute("INSERT INTO users VALUES (201, 'fresh@test.com', 999.0);")
            .unwrap();

        // Query BEFORE checkpoint: old email must return 0, renamed & fresh must return 1, deleted 0!
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'u42@test.com';").unwrap()).len(), 0);
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'u100@test.com';").unwrap()).len(), 0);
        let r1 = db.execute("SELECT id FROM users WHERE email = 'renamed@test.com';").unwrap();
        assert_eq!(rows_of(&r1).len(), 1);
        assert!(rows_of(&r1)[0][0].exact_eq(&Value::Integer(42)));
        let r2 = db.execute("SELECT id FROM users WHERE email = 'fresh@test.com';").unwrap();
        assert_eq!(rows_of(&r2).len(), 1);
        assert!(rows_of(&r2)[0][0].exact_eq(&Value::Integer(201)));

        // Now CHECKPOINT, reopen, and verify again!
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'u42@test.com';").unwrap()).len(), 0);
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'u100@test.com';").unwrap()).len(), 0);
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'renamed@test.com';").unwrap()).len(), 1);
        assert_eq!(rows_of(&db.execute("SELECT id FROM users WHERE email = 'fresh@test.com';").unwrap()).len(), 1);
    }
    cleanup(&path);
}

#[test]
fn bug_hunt_sql_edge_cases_nulls_empty_tables_joins_escapes_and_extremes() {
    let path = temp_db("sql_edges");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, grp TEXT, x REAL);
             CREATE TABLE vals (id INTEGER PRIMARY KEY, u TEXT UNIQUE, s TEXT, n INTEGER, f REAL, b BLOB);",
        )
        .unwrap();

        // Empty table queries: SELECT, COUNT(*), GROUP BY, SUM/AVG, VACUUM
        assert_eq!(rows_of(&db.execute("SELECT * FROM empty_t;").unwrap()).len(), 0);
        let c0 = db.execute("SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x) FROM empty_t;").unwrap();
        assert!(rows_of(&c0)[0][0].exact_eq(&Value::Integer(0)));
        assert!(rows_of(&c0)[0][1].is_null());
        assert!(rows_of(&c0)[0][2].is_null());
        assert_eq!(rows_of(&db.execute("SELECT grp, COUNT(*) FROM empty_t GROUP BY grp;").unwrap()).len(), 0);
        db.vacuum().unwrap();

        // SQL standard: UNIQUE allows multiple NULLs!
        let big_str = "x".repeat(65_536);
        db.execute(&format!(
            "INSERT INTO vals VALUES
                (1, NULL, 'O''Reilly ''quoted''', -1, -0.0, X'00FF00AA'),
                (2, NULL, '', 9223372036854775807, 10.0, X''),
                (3, 'uniq1', '{}', 0, NULL, NULL);",
            big_str
        ))
        .unwrap();
        db.checkpoint().unwrap();

        // Verify escaped quotes, empty string, 64KB string, null-byte blob, i64::MAX
        let r1 = db.execute("SELECT s, b FROM vals WHERE id = 1;").unwrap();
        assert!(rows_of(&r1)[0][0].exact_eq(&Value::Text("O'Reilly 'quoted'".into())));
        assert!(rows_of(&r1)[0][1].exact_eq(&Value::Blob(vec![0x00, 0xFF, 0x00, 0xAA])));

        let r3 = db.execute("SELECT s FROM vals WHERE id = 3;").unwrap();
        assert!(rows_of(&r3)[0][0].exact_eq(&Value::Text(big_str)));

        // 3-valued NULL logic in WHERE
        assert_eq!(rows_of(&db.execute("SELECT id FROM vals WHERE f = NULL;").unwrap()).len(), 0);
        assert_eq!(rows_of(&db.execute("SELECT id FROM vals WHERE f != 10.0;").unwrap()).len(), 1); // only id=1 (-0.0), NOT id=3 (NULL)
        assert_eq!(rows_of(&db.execute("SELECT id FROM vals WHERE f IS NULL;").unwrap()).len(), 1);

        // Division by zero must return NULL, never panic!
        let dz = db.execute("SELECT 10 / 0, 10.0 / 0.0 FROM vals WHERE id = 1;").unwrap();
        assert!(rows_of(&dz)[0][0].is_null());
        assert!(rows_of(&dz)[0][1].is_null());

        // Cross-type indexed lookup: REAL column queried with INTEGER literal (`WHERE f = 10`)!
        db.execute("CREATE INDEX idx_f ON vals (f);").unwrap();
        db.checkpoint().unwrap();
        let rf = db.execute("SELECT id FROM vals WHERE f = 10;").unwrap();
        assert_eq!(rows_of(&rf).len(), 1);
        assert!(rows_of(&rf)[0][0].exact_eq(&Value::Integer(2)));

        // LIMIT 0 and OFFSET past end
        assert_eq!(rows_of(&db.execute("SELECT * FROM vals LIMIT 0;").unwrap()).len(), 0);
        assert_eq!(rows_of(&db.execute("SELECT * FROM vals OFFSET 999;").unwrap()).len(), 0);

        // Self-join!
        db.execute(
            "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT);
             INSERT INTO nodes VALUES (1, NULL, 'root'), (2, 1, 'child_a'), (3, 1, 'child_b'), (4, 2, 'leaf');",
        )
        .unwrap();
        let sj = db
            .execute(
                "SELECT c.label, p.label AS parent_label
                 FROM nodes c
                 LEFT JOIN nodes p ON c.parent_id = p.id
                 ORDER BY c.id ASC;",
            )
            .unwrap();
        let srows = rows_of(&sj);
        assert_eq!(srows.len(), 4);
        assert!(srows[0][1].is_null());
        assert!(srows[1][1].exact_eq(&Value::Text("root".into())));
        assert!(srows[3][1].exact_eq(&Value::Text("child_a".into())));

        // DELETE ALL rows -> CHECKPOINT -> re-INSERT!
        db.execute("DELETE FROM nodes;").unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.count_rows("nodes").unwrap(), 0);
        db.execute("INSERT INTO nodes (parent_id, label) VALUES (NULL, 'reborn');")
            .unwrap();
        let rb = db.execute("SELECT id, label FROM nodes;").unwrap();
        assert_eq!(rows_of(&rb).len(), 1);
        assert!(rows_of(&rb)[0][0].exact_eq(&Value::Integer(5)));
    }
    cleanup(&path);
}

#[test]
fn bug_hunt_alter_add_column_then_index_and_vectorized_group_by() {
    let path = temp_db("alter_idx");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, price REAL);")
            .unwrap();
        for i in 1..=200 {
            db.execute(&format!("INSERT INTO items VALUES ({}, {}.5);", i, i % 10))
                .unwrap();
        }
        db.checkpoint().unwrap();

        // Add column with DEFAULT to persisted table, then CREATE INDEX on the evolved column!
        db.execute("ALTER TABLE items ADD COLUMN region TEXT DEFAULT 'us-east';")
            .unwrap();
        db.execute("INSERT INTO items VALUES (201, 50.0, 'eu-west');")
            .unwrap();
        db.execute("CREATE INDEX idx_region ON items (region);")
            .unwrap();
        db.checkpoint().unwrap();

        // Query both old (synthesized default) and new rows through the secondary index!
        let us = db.execute("SELECT id FROM items WHERE region = 'us-east';").unwrap();
        assert_eq!(rows_of(&us).len(), 200);
        let eu = db.execute("SELECT id FROM items WHERE region = 'eu-west';").unwrap();
        assert_eq!(rows_of(&eu).len(), 1);

        // Vectorized GROUP BY on the evolved column!
        let gb = db
            .execute("SELECT region, COUNT(*) AS cnt, SUM(price) AS tot FROM items GROUP BY region ORDER BY cnt DESC;")
            .unwrap();
        let grows = rows_of(&gb);
        assert_eq!(grows.len(), 2);
        assert!(grows[0][0].exact_eq(&Value::Text("us-east".into())));
        assert!(grows[0][1].exact_eq(&Value::Integer(200)));
        assert!(grows[1][0].exact_eq(&Value::Text("eu-west".into())));
        assert!(grows[1][1].exact_eq(&Value::Integer(1)));
        assert!(grows[1][2].exact_eq(&Value::Real(50.0)));
    }
    cleanup(&path);
}

#[test]
fn bug_hunt_insert_rate_throughput_limits() {
    // 1. Single-row auto-commit (sync_wal = false, like SQLite synchronous=OFF / WAL async)
    let p1 = temp_db("rate_autocommit_nosync");
    {
        let mut opts = DbOptions::default();
        opts.sync_wal = false;
        let mut db = Database::open_with_options(&p1, opts).unwrap();
        db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, tag TEXT, val REAL);")
            .unwrap();
        let n = 2_000usize;
        let t0 = Instant::now();
        for i in 1..=n {
            db.insert_row(
                "ev",
                vec![
                    Value::Integer(i as i64),
                    Value::Integer(1_700_000_000 + i as i64),
                    Value::Text("click".into()),
                    Value::Real((i % 100) as f64 / 10.0),
                ],
            )
            .unwrap();
        }
        let secs = t0.elapsed().as_secs_f64();
        let rps = (n as f64) / secs;
        println!(
            "[RATE] Single-row auto-commit (sync_wal=false): {} rows in {:.3}s = {:.0} rows/sec",
            n, secs, rps
        );
        assert!(rps > 50.0, "Expected >> 50 rows/s, got {:.1}", rps);
    }
    cleanup(&p1);

    // 2. Single-row auto-commit WITH full fsync per row (sync_wal = true, worst-case ACID durability!)
    let p2 = temp_db("rate_autocommit_fsync");
    {
        let mut opts = DbOptions::default();
        opts.sync_wal = true;
        let mut db = Database::open_with_options(&p2, opts).unwrap();
        db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, tag TEXT, val REAL);")
            .unwrap();
        let n = 100usize;
        let t0 = Instant::now();
        for i in 1..=n {
            db.insert_row(
                "ev",
                vec![
                    Value::Integer(i as i64),
                    Value::Integer(1_700_000_000 + i as i64),
                    Value::Text("click".into()),
                    Value::Real((i % 100) as f64 / 10.0),
                ],
            )
            .unwrap();
        }
        let secs = t0.elapsed().as_secs_f64();
        let rps = (n as f64) / secs;
        println!(
            "[RATE] Single-row auto-commit (sync_wal=true, fsync every row): {} rows in {:.3}s = {:.0} rows/sec",
            n, secs, rps
        );
    }
    cleanup(&p2);

    // 3. Batched transactions (1,000 rows per COMMIT, sync_wal = true)
    let p3 = temp_db("rate_batched_tx");
    {
        let mut opts = DbOptions::default();
        opts.sync_wal = true;
        let mut db = Database::open_with_options(&p3, opts).unwrap();
        db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, tag TEXT, val REAL);")
            .unwrap();
        let batches = 20usize;
        let batch_sz = 1_000usize;
        let t0 = Instant::now();
        for b in 0..batches {
            let mut rows = Vec::with_capacity(batch_sz);
            for j in 0..batch_sz {
                let i = (b * batch_sz + j + 1) as i64;
                rows.push(vec![
                    Value::Integer(i),
                    Value::Integer(1_700_000_000 + i),
                    Value::Text("click".into()),
                    Value::Real((i % 100) as f64 / 10.0),
                ]);
            }
            db.begin().unwrap();
            db.insert_batch("ev", rows).unwrap();
            db.commit().unwrap();
        }
        let secs = t0.elapsed().as_secs_f64();
        let total = batches * batch_sz;
        let rps = (total as f64) / secs;
        println!(
            "[RATE] Batched WAL tx (1000 rows/COMMIT, sync_wal=true): {} rows in {:.3}s = {:.0} rows/sec",
            total, secs, rps
        );
    }
    cleanup(&p3);

    // 4. Bulk insert (50,000 rows directly into PAX-v4 compressed pages)
    let p4 = temp_db("rate_bulk");
    {
        let mut db = Database::open(&p4).unwrap();
        db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, tag TEXT, val REAL);")
            .unwrap();
        let total = 50_000usize;
        let mut rows = Vec::with_capacity(total);
        for i in 1..=(total as i64) {
            rows.push(vec![
                Value::Integer(i),
                Value::Integer(1_700_000_000 + i),
                Value::Text("click".into()),
                Value::Real((i % 100) as f64 / 10.0),
            ]);
        }
        let t0 = Instant::now();
        db.bulk_insert("ev", rows).unwrap();
        db.checkpoint().unwrap();
        let secs = t0.elapsed().as_secs_f64();
        let rps = (total as f64) / secs;
        println!(
            "[RATE] Bulk PAX-v4 insert + Brotli checkpoint: {} rows in {:.3}s = {:.0} rows/sec",
            total, secs, rps
        );
    }
    cleanup(&p4);
}

#[test]
fn bug_hunt_error_busy_is_clean() {
    let p = temp_db("busy_clean");
    let _db1 = Database::open(&p).unwrap();
    let err = Database::open(&p);
    assert!(matches!(err, Err(Error::Busy(_))));
    drop(_db1);
    cleanup(&p);
}

#[test]
fn bug_hunt_online_backup_to_produces_independent_exact_copy() {
    let src = temp_db("backup_src");
    let dst = temp_db("backup_dst");
    {
        let mut db = Database::open(&src).unwrap();
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, bal REAL);")
            .unwrap();
        db.execute("INSERT INTO accounts VALUES (1, 'Alice', 500.25), (2, 'Bob', 99.75);")
            .unwrap();
        // Backup via SQL command BEFORE manual checkpoint!
        let dst_str = dst.to_string_lossy().replace('\'', "''");
        db.execute(&format!("BACKUP TO '{}';", dst_str)).unwrap();

        // Mutate original AFTER backup -> backup must remain unaffected!
        db.execute("DELETE FROM accounts WHERE id = 1;").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut bdb = Database::open(&dst).unwrap();
        let res = bdb
            .execute("SELECT id, owner, bal FROM accounts ORDER BY id ASC;")
            .unwrap();
        let rows = rows_of(&res);
        assert_eq!(rows.len(), 2);
        assert!(rows[0][1].exact_eq(&Value::Text("Alice".into())));
        assert!(rows[0][2].exact_eq(&Value::Real(500.25)));
        assert!(rows[1][1].exact_eq(&Value::Text("Bob".into())));
    }
    cleanup(&src);
    cleanup(&dst);
}

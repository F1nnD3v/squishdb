use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use squishdb::encoding::{decode_column, encode_column};
use squishdb::types::{ColumnType, Value};
use squishdb::{Database, DbOptions, QueryResult};

fn temp_db_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_test_{}_{}.sqdb", name, std::process::id()));
    cleanup_db(&p);
    p
}

fn cleanup_db(p: &PathBuf) {
    let _ = std::fs::remove_file(p);
    for suffix in ["-wal", ".lock", ".vacuum"] {
        let mut s = p.as_os_str().to_os_string();
        s.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(s));
    }
}

#[test]
fn test_all_codecs_exact_lossless_roundtrip() {
    let int_cases: Vec<Vec<Value>> = vec![
        vec![Value::Integer(42); 100],
        (0..500)
            .map(|i| Value::Integer(1_700_000_000 + i * 10))
            .collect(),
        vec![
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(0),
            Value::Integer(-1),
            Value::Null,
            Value::Integer(i64::MIN + 1),
            Value::Integer(i64::MAX - 1),
        ],
        (0..200)
            .map(|i| Value::Integer(if i < 100 { 7 } else { 999_999_999_999 }))
            .collect(),
        (0..200)
            .map(|i| {
                let ids = [100_000_000_001i64, 500_000_000_005, -900_000_000_009, 42];
                if i % 13 == 0 {
                    Value::Null
                } else {
                    Value::Integer(ids[i % ids.len()])
                }
            })
            .collect(),
        vec![Value::Null; 64],
    ];

    for case in &int_cases {
        let mut buf = Vec::new();
        encode_column(ColumnType::Integer, case, &mut buf).unwrap();
        let mut pos = 0;
        let dec = decode_column(ColumnType::Integer, &buf, &mut pos, case.len()).unwrap();
        assert_eq!(dec.len(), case.len());
        for (a, b) in case.iter().zip(&dec) {
            assert!(a.exact_eq(b), "Int mismatch: {:?} != {:?}", a, b);
        }
    }

    let real_cases: Vec<Vec<Value>> = vec![
        vec![Value::Real(-0.0), Value::Real(0.0), Value::Real(-0.0)],
        vec![
            Value::Real(f64::NAN),
            Value::Real(f64::INFINITY),
            Value::Real(f64::NEG_INFINITY),
            Value::Real(f64::MIN_POSITIVE / 2.0),
            Value::Real(-0.0),
            Value::Null,
        ],
        (0..300)
            .map(|i| Value::Real(((i % 5000) as f64) / 100.0))
            .collect(),
        (0..200)
            .map(|i| Value::Real((i as f64).sin() * 12345.6789))
            .collect(),
    ];

    for case in &real_cases {
        let mut buf = Vec::new();
        encode_column(ColumnType::Real, case, &mut buf).unwrap();
        let mut pos = 0;
        let dec = decode_column(ColumnType::Real, &buf, &mut pos, case.len()).unwrap();
        assert_eq!(dec.len(), case.len());
        for (a, b) in case.iter().zip(&dec) {
            assert!(a.exact_eq(b), "Real bit mismatch: {:?} != {:?}", a, b);
        }
    }

    let text_case: Vec<Value> = (0..300)
        .map(|i| match i % 7 {
            0 => Value::Null,
            1 => Value::Text(String::new()),
            2 => Value::Text("https://api.example.com/v2/orders/status".into()),
            3 => Value::Text("https://api.example.com/v2/orders/shipping".into()),
            4 => Value::Text("日本語テスト 🚀 café".into()),
            _ => Value::Text(format!("user_{:04}@corp.example.org", i % 25)),
        })
        .collect();
    let mut buf = Vec::new();
    encode_column(ColumnType::Text, &text_case, &mut buf).unwrap();
    let mut pos = 0;
    let dec = decode_column(ColumnType::Text, &buf, &mut pos, text_case.len()).unwrap();
    for (a, b) in text_case.iter().zip(&dec) {
        assert!(a.exact_eq(b));
    }

    let blob_case: Vec<Value> = (0..100)
        .map(|i| {
            if i % 10 == 0 {
                Value::Null
            } else {
                Value::Blob(vec![(i % 256) as u8, 0, 255, (i * 7 % 256) as u8])
            }
        })
        .collect();
    let mut bbuf = Vec::new();
    encode_column(ColumnType::Blob, &blob_case, &mut bbuf).unwrap();
    let mut bpos = 0;
    let bdec = decode_column(ColumnType::Blob, &bbuf, &mut bpos, blob_case.len()).unwrap();
    for (a, b) in blob_case.iter().zip(&bdec) {
        assert!(a.exact_eq(b));
    }
}

#[test]
fn test_sql_crud_transactions_and_vacuum() {
    let path = temp_db_path("sql_crud");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                country TEXT,
                balance REAL,
                bio TEXT
            );",
        )
        .unwrap();

        db.execute(
            "INSERT INTO users (id, email, country, balance, bio) VALUES
                (1, 'alice@example.com', 'US', 120.50, 'Engineer'),
                (2, 'bob@example.com', 'UK', 85.25, NULL),
                (3, 'carol@example.com', 'US', 310.00, 'Designer'),
                (4, 'dave@example.com', 'DE', 0.0, 'Ops');",
        )
        .unwrap();

        db.execute("BEGIN;").unwrap();
        db.execute("DELETE FROM users WHERE country = 'US';")
            .unwrap();
        db.execute("INSERT INTO users VALUES (99, 'ghost@x.com', 'FR', 1.0, 'tmp');")
            .unwrap();
        db.execute("ROLLBACK;").unwrap();

        let res = db.execute("SELECT COUNT(*) FROM users;").unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => assert!(rows[0][0].exact_eq(&Value::Integer(4))),
            _ => panic!("Expected rows"),
        }

        db.execute("BEGIN;").unwrap();
        db.execute("UPDATE users SET balance = balance + 10.0 WHERE country = 'US';")
            .unwrap();
        db.execute("DELETE FROM users WHERE id = 4;").unwrap();
        db.execute("COMMIT;").unwrap();
        db.checkpoint().unwrap();

        drop(db);
        let mut db2 = Database::open(&path).unwrap();
        let res = db2
            .execute(
                "SELECT id, balance FROM users WHERE country IN ('US', 'UK') AND email LIKE '%@example.com' ORDER BY balance DESC LIMIT 2;",
            )
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert!(rows[0][0].exact_eq(&Value::Integer(3)));
                assert!(rows[0][1].exact_eq(&Value::Real(320.0)));
                assert!(rows[1][0].exact_eq(&Value::Integer(1)));
                assert!(rows[1][1].exact_eq(&Value::Real(130.5)));
            }
            _ => panic!("Expected rows"),
        }

        db2.vacuum().unwrap();
        let stats = db2.stats().unwrap();
        assert_eq!(stats.tables[0].live_rows, 3);
        assert_eq!(stats.tables[0].deleted_slots, 0);

        let res_null = db2
            .execute("SELECT email FROM users WHERE bio IS NULL;")
            .unwrap();
        match &res_null[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].exact_eq(&Value::Text("bob@example.com".into())));
            }
            _ => panic!("Expected rows"),
        }
    }
    cleanup_db(&path);
}

#[test]
fn test_wal_crash_recovery_with_truncated_tail() {
    let path = temp_db_path("wal_crash");
    let recovered_path = temp_db_path("wal_crash_recovered");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO items VALUES (1, 'persisted_in_wal');")
            .unwrap();
        // Copy raw DB + uncheckpointed WAL snapshot to simulate abrupt power loss on another process
        std::fs::copy(&path, &recovered_path).unwrap();
        let mut src_wal = path.as_os_str().to_os_string();
        src_wal.push("-wal");
        let mut dst_wal = recovered_path.as_os_str().to_os_string();
        dst_wal.push("-wal");
        std::fs::copy(PathBuf::from(src_wal), PathBuf::from(&dst_wal)).unwrap();

        // Append garbage partial frame to WAL tail to simulate power loss mid-write
        let mut f = OpenOptions::new()
            .append(true)
            .open(PathBuf::from(&dst_wal))
            .unwrap();
        f.write_all(b"SQWL\x01\x00\x00\x00\x00\x00\x00\x00\x99\x00\x00\x00DEADhalfwritten")
            .unwrap();
    }

    // Reopen must recover row 1 and ignore the corrupted tail!
    {
        let mut db = Database::open(&recovered_path).unwrap();
        let res = db.execute("SELECT id, name FROM items;").unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].exact_eq(&Value::Integer(1)));
                assert!(rows[0][1].exact_eq(&Value::Text("persisted_in_wal".into())));
            }
            _ => panic!("Expected rows"),
        }
    }
    cleanup_db(&path);
    cleanup_db(&recovered_path);
}

#[test]
fn test_bloom_filters_secondary_indexes_and_page_cache() {
    let path = temp_db_path("indexes_bloom");
    {
        let mut opts = DbOptions::default();
        opts.page_rows = 256; // 10 pages for 2,560 rows
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute("CREATE TABLE tokens (id INTEGER PRIMARY KEY, tok TEXT, grp INTEGER);")
            .unwrap();

        let mut batch = Vec::new();
        for i in 1..=2560i64 {
            // Unsorted hash-like token string
            let h = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
            let tok = format!("tok_{:016x}", h);
            batch.push(vec![
                Value::Integer(i),
                Value::Text(tok),
                Value::Integer(i % 50),
            ]);
        }
        db.bulk_insert("tokens", batch).unwrap();
        db.checkpoint().unwrap();

        let target_h = (1337u64)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let target_tok = format!("tok_{:016x}", target_h);

        // 1. BEFORE creating secondary index: Bloom filter should prune almost all 10 pages!
        let exp = db
            .execute(&format!(
                "EXPLAIN SELECT id FROM tokens WHERE tok = '{}';",
                target_tok
            ))
            .unwrap();
        let plan_str = match &exp[0] {
            QueryResult::Message(m) => m.clone(),
            _ => panic!("Expected plan message"),
        };
        assert!(
            plan_str.contains("bloom=") && !plan_str.contains("bloom=0"),
            "Expected Bloom filter pruning in plan: {}",
            plan_str
        );

        // 2. Create secondary index and verify O(log N) exact single-page index pruning!
        db.execute("CREATE INDEX idx_tok ON tokens (tok);").unwrap();
        db.checkpoint().unwrap();

        // Reopen DB to prove index persisted cleanly in catalog!
        drop(db);
        let mut db2 = Database::open(&path).unwrap();
        let exp2 = db2
            .execute(&format!(
                "EXPLAIN SELECT id FROM tokens WHERE tok = '{}';",
                target_tok
            ))
            .unwrap();
        let plan2 = match &exp2[0] {
            QueryResult::Message(m) => m.clone(),
            _ => panic!("Expected plan"),
        };
        assert!(
            plan2.contains("index='idx_tok'") && plan2.contains("pruned: index=9"),
            "Expected index to prune 9 of 10 pages: {}",
            plan2
        );

        // Repeat lookup -> must hit LRU Page Cache!
        let res = db2
            .execute(&format!(
                "SELECT id FROM tokens WHERE tok = '{}';",
                target_tok
            ))
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].exact_eq(&Value::Integer(1337)));
            }
            _ => panic!("Expected 1 row"),
        }
        assert!(db2.stats().unwrap().cache_hits >= 1);

        // Update + VACUUM must keep index 100% accurate!
        db2.execute("UPDATE tokens SET tok = 'tok_updated_key' WHERE id = 1337;")
            .unwrap();
        db2.vacuum().unwrap();

        let res_old = db2
            .execute(&format!(
                "SELECT id FROM tokens WHERE tok = '{}';",
                target_tok
            ))
            .unwrap();
        match &res_old[0] {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("Expected 0 rows for old key"),
        }
        let res_new = db2
            .execute("SELECT id FROM tokens WHERE tok = 'tok_updated_key';")
            .unwrap();
        match &res_new[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].exact_eq(&Value::Integer(1337)));
            }
            _ => panic!("Expected 1 row for updated key"),
        }
    }
    cleanup_db(&path);
}

#[test]
fn test_joins_group_by_having_aggregates_and_clustering() {
    let path = temp_db_path("sql_analytics");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT);
             CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount REAL, status TEXT) CLUSTER BY (status, cust_id);
             INSERT INTO customers VALUES (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (3, 'Carol', 'gold');
             INSERT INTO orders VALUES
               (10, 1, 100.0, 'paid'),
               (11, 1, 50.0, 'paid'),
               (12, 2, 200.0, 'paid'),
               (13, 2, 30.0, 'refunded');",
        )
        .unwrap();

        // INNER JOIN + GROUP BY + SUM/AVG/COUNT + HAVING + ORDER BY alias
        let res = db
            .execute(
                "SELECT c.name, COUNT(*) AS cnt, SUM(o.amount) AS total, AVG(o.amount) AS avg_amt
                 FROM customers c
                 INNER JOIN orders o ON c.id = o.cust_id
                 WHERE o.status = 'paid'
                 GROUP BY c.name
                 HAVING SUM(o.amount) >= 150.0
                 ORDER BY total DESC;",
            )
            .unwrap();
        match &res[0] {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, &["name", "cnt", "total", "avg_amt"]);
                assert_eq!(rows.len(), 2);
                assert!(rows[0][0].exact_eq(&Value::Text("Bob".into())));
                assert!(rows[0][1].exact_eq(&Value::Integer(1)));
                assert!(rows[0][2].exact_eq(&Value::Real(200.0)));
                assert!(rows[1][0].exact_eq(&Value::Text("Alice".into())));
                assert!(rows[1][1].exact_eq(&Value::Integer(2)));
                assert!(rows[1][2].exact_eq(&Value::Real(150.0)));
                assert!(rows[1][3].exact_eq(&Value::Real(75.0)));
            }
            _ => panic!("Expected rows"),
        }

        // LEFT JOIN includes Carol with NULL order amount + CASE WHEN + COUNT(col)
        let res_left = db
            .execute(
                "SELECT c.name, COUNT(o.id) AS order_cnt,
                        CASE WHEN COUNT(o.id) = 0 THEN 'none' ELSE 'active' END AS state
                 FROM customers c
                 LEFT JOIN orders o ON c.id = o.cust_id
                 GROUP BY c.name
                 ORDER BY c.name ASC;",
            )
            .unwrap();
        match &res_left[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                // Carol has 0 orders
                assert!(rows[2][0].exact_eq(&Value::Text("Carol".into())));
                assert!(rows[2][1].exact_eq(&Value::Integer(0)));
                assert!(rows[2][2].exact_eq(&Value::Text("none".into())));
            }
            _ => panic!("Expected rows"),
        }

        // DISTINCT + BETWEEN + COUNT(DISTINCT ...)
        let res_dist = db
            .execute(
                "SELECT COUNT(DISTINCT tier) AS tiers FROM customers WHERE id BETWEEN 1 AND 3;",
            )
            .unwrap();
        match &res_dist[0] {
            QueryResult::Rows { rows, .. } => {
                assert!(rows[0][0].exact_eq(&Value::Integer(2)));
            }
            _ => panic!("Expected rows"),
        }

        // VACUUM SORT BY re-clustering preserves every single row losslessly
        db.execute("VACUUM orders SORT BY status, id;").unwrap();
        let res_cnt = db.execute("SELECT COUNT(*) FROM orders;").unwrap();
        match &res_cnt[0] {
            QueryResult::Rows { rows, .. } => {
                assert!(rows[0][0].exact_eq(&Value::Integer(4)));
            }
            _ => panic!("Expected rows"),
        }
    }
    cleanup_db(&path);
}

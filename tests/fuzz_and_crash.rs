use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use squishdb::encoding::{decode_column, encode_column};
use squishdb::types::{ColumnType, Error, Value};
use squishdb::{Database, DbOptions, QueryResult};

struct XorShift64(u64);
impl XorShift64 {
    fn new(seed: u64) -> Self {
        XorShift64(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, max: usize) -> usize {
        (self.next_u64() as usize) % max
    }
}

fn temp_db(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_v3_{}_{}.sqdb", name, std::process::id()));
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

#[test]
fn test_paged_on_disk_index_blocks_and_demand_paging() {
    let path = temp_db("paged_idx");
    {
        let mut opts = DbOptions::default();
        opts.page_rows = 512;
        let mut db = Database::open_with_options(&path, opts).unwrap();
        db.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, k TEXT, v INTEGER);")
            .unwrap();

        let mut batch = Vec::with_capacity(10_000);
        for i in 1..=10_000i64 {
            let h = (i as u64).wrapping_mul(2862933555777941757).wrapping_add(3037000493);
            batch.push(vec![
                Value::Integer(i),
                Value::Text(format!("key_{:016x}", h)),
                Value::Integer(i * 10),
            ]);
        }
        db.bulk_insert("kv", batch).unwrap();
        db.checkpoint().unwrap();

        db.execute("CREATE INDEX idx_k ON kv (k);").unwrap();
        db.checkpoint().unwrap();

        let stats = db.stats().unwrap();
        // 10,000 entries -> 3 paged index leaf blocks on disk, while catalog stays tiny!
        assert!(
            stats.index_disk_bytes > 5_000,
            "Expected paged index blocks on disk, got {}",
            stats.index_disk_bytes
        );
        assert!(
            stats.catalog_stored_bytes < stats.index_disk_bytes,
            "Catalog ({}) should be much smaller than paged index blocks ({})!",
            stats.catalog_stored_bytes,
            stats.index_disk_bytes
        );
    }

    // Fresh open: demand-page ONLY 1 index leaf block!
    {
        let mut db = Database::open(&path).unwrap();
        let h = (7777u64)
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let target = format!("key_{:016x}", h);

        let exp1 = db
            .execute(&format!("EXPLAIN SELECT v FROM kv WHERE k = '{}';", target))
            .unwrap();
        let p1 = match &exp1[0] {
            QueryResult::Message(m) => m.clone(),
            _ => panic!("Expected plan"),
        };
        assert!(
            p1.contains("index='idx_k'") && p1.contains("idx_blocks=1"),
            "First lookup should demand-page exactly 1 index leaf block: {}",
            p1
        );

        // Second lookup on same key should read 0 index blocks from disk (LRU index cache hit!)
        let exp2 = db
            .execute(&format!("EXPLAIN SELECT v FROM kv WHERE k = '{}';", target))
            .unwrap();
        let p2 = match &exp2[0] {
            QueryResult::Message(m) => m.clone(),
            _ => panic!("Expected plan"),
        };
        assert!(
            p2.contains("idx_blocks=0") && p2.contains("cache_hits=1"),
            "Second lookup should hit both index LRU cache and page LRU cache: {}",
            p2
        );

        let res = db
            .execute(&format!("SELECT v FROM kv WHERE k = '{}';", target))
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].exact_eq(&Value::Integer(77770)));
            }
            _ => panic!("Expected row"),
        }
    }
    cleanup(&path);
}

#[test]
fn test_os_file_locking_shared_and_exclusive() {
    let path = temp_db("os_lock");
    {
        let mut rw = Database::open(&path).unwrap();
        rw.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT); INSERT INTO t VALUES (1, 'ok');")
            .unwrap();
        rw.checkpoint().unwrap();

        // While exclusive RW lock is held, another RW or RO open must fail with Error::Busy!
        assert!(matches!(Database::open(&path), Err(Error::Busy(_))));
        assert!(matches!(Database::open_readonly(&path), Err(Error::Busy(_))));
    }

    // Now open THREE concurrent read-only shared connections simultaneously!
    {
        let mut r1 = Database::open_readonly(&path).unwrap();
        let mut r2 = Database::open_readonly(&path).unwrap();
        let mut r3 = Database::open_readonly(&path).unwrap();

        // Writer must be blocked while shared readers exist!
        assert!(matches!(Database::open(&path), Err(Error::Busy(_))));

        // All 3 readers read concurrently; writes on RO must be rejected
        for r in [&mut r1, &mut r2, &mut r3] {
            let res = r.execute("SELECT x FROM t WHERE id = 1;").unwrap();
            match &res[0] {
                QueryResult::Rows { rows, .. } => {
                    assert!(rows[0][0].exact_eq(&Value::Text("ok".into())));
                }
                _ => panic!("Expected row"),
            }
            assert!(r.execute("INSERT INTO t VALUES (2, 'nope');").is_err());
        }
    }

    // Once all readers drop, RW lock succeeds again!
    {
        let mut rw = Database::open(&path).unwrap();
        rw.execute("INSERT INTO t VALUES (2, 'after_readers');")
            .unwrap();
        assert_eq!(rw.count_rows("t").unwrap(), 2);
    }
    cleanup(&path);
}

#[test]
fn test_advanced_sql_ctes_subqueries_windows_constraints_and_alter() {
    let path = temp_db("adv_sql");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE emp (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                dept TEXT DEFAULT 'eng',
                salary REAL
            );",
        )
        .unwrap();

        // DEFAULT value on omitted column
        db.execute(
            "INSERT INTO emp (id, email, salary) VALUES (1, 'a@co.com', 100.0);
             INSERT INTO emp (id, email, dept, salary) VALUES
               (2, 'b@co.com', 'eng', 150.0),
               (3, 'c@co.com', 'eng', 150.0),
               (4, 'd@co.com', 'sales', 120.0),
               (5, 'e@co.com', 'sales', 90.0);",
        )
        .unwrap();
        db.checkpoint().unwrap();

        // UNIQUE constraint must reject duplicate email on INSERT and UPDATE!
        assert!(db
            .execute("INSERT INTO emp VALUES (6, 'a@co.com', 'hr', 80.0);")
            .is_err());
        assert!(db
            .execute("UPDATE emp SET email = 'b@co.com' WHERE id = 1;")
            .is_err());

        // Instant O(1) ALTER TABLE ADD COLUMN on already-persisted PAX pages!
        db.execute("ALTER TABLE emp ADD COLUMN bonus REAL DEFAULT 10.0;")
            .unwrap();
        db.execute("INSERT INTO emp VALUES (6, 'f@co.com', 'sales', 130.0, 25.0);")
            .unwrap();
        let bonus_res = db
            .execute("SELECT id, bonus FROM emp WHERE id IN (1, 6) ORDER BY id ASC;")
            .unwrap();
        match &bonus_res[0] {
            QueryResult::Rows { rows, .. } => {
                assert!(rows[0][1].exact_eq(&Value::Real(10.0))); // backfilled from old PAX page!
                assert!(rows[1][1].exact_eq(&Value::Real(25.0))); // newly inserted!
            }
            _ => panic!("Expected rows"),
        }

        // CTE + Subquery in FROM + IN (SELECT ...)
        let cte_res = db
            .execute(
                "WITH high_depts AS (
                    SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept HAVING AVG(salary) >= 120.0
                 )
                 SELECT id, email FROM emp
                 WHERE dept IN (SELECT dept FROM high_depts)
                 ORDER BY id ASC;",
            )
            .unwrap();
        match &cte_res[0] {
            QueryResult::Rows { rows, .. } => {
                // eng avg = 133.33, sales avg = 113.33 -> only eng (ids 1, 2, 3)
                assert_eq!(rows.len(), 3);
                assert!(rows[0][0].exact_eq(&Value::Integer(1)));
                assert!(rows[2][0].exact_eq(&Value::Integer(3)));
            }
            _ => panic!("Expected rows"),
        }

        // Subquery in FROM
        let sub_res = db
            .execute(
                "SELECT sub.dept, sub.cnt FROM (SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept) sub ORDER BY sub.dept ASC;",
            )
            .unwrap();
        match &sub_res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert!(rows[0][0].exact_eq(&Value::Text("eng".into())));
                assert!(rows[0][1].exact_eq(&Value::Integer(3)));
                assert!(rows[1][0].exact_eq(&Value::Text("sales".into())));
                assert!(rows[1][1].exact_eq(&Value::Integer(3)));
            }
            _ => panic!("Expected rows"),
        }

        // Window Functions: ROW_NUMBER(), RANK(), running SUM() OVER (PARTITION BY ... ORDER BY ...)
        let win_res = db
            .execute(
                "SELECT id, dept, salary,
                        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id ASC) AS rn,
                        RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk,
                        SUM(salary) OVER (PARTITION BY dept ORDER BY salary DESC, id ASC) AS run_sum
                 FROM emp
                 WHERE dept = 'eng'
                 ORDER BY rn ASC;",
            )
            .unwrap();
        match &win_res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                // id=2 (150): rn=1, rnk=1, run_sum=150
                assert!(rows[0][0].exact_eq(&Value::Integer(2)));
                assert!(rows[0][3].exact_eq(&Value::Integer(1)));
                assert!(rows[0][4].exact_eq(&Value::Integer(1)));
                assert!(rows[0][5].exact_eq(&Value::Real(150.0)));
                // id=3 (150): rn=2, rnk=1 (tie!), run_sum=300
                assert!(rows[1][0].exact_eq(&Value::Integer(3)));
                assert!(rows[1][3].exact_eq(&Value::Integer(2)));
                assert!(rows[1][4].exact_eq(&Value::Integer(1)));
                assert!(rows[1][5].exact_eq(&Value::Real(300.0)));
                // id=1 (100): rn=3, rnk=3, run_sum=400
                assert!(rows[2][0].exact_eq(&Value::Integer(1)));
                assert!(rows[2][3].exact_eq(&Value::Integer(3)));
                assert!(rows[2][4].exact_eq(&Value::Integer(3)));
                assert!(rows[2][5].exact_eq(&Value::Real(400.0)));
            }
            _ => panic!("Expected window rows"),
        }

        // Verify EXPLAIN uses VECTORIZED COLUMNAR SCAN for GROUP BY!
        let exp = db
            .execute(
                "EXPLAIN SELECT dept, COUNT(*), SUM(salary) FROM emp WHERE dept = 'eng' GROUP BY dept;",
            )
            .unwrap();
        match &exp[0] {
            QueryResult::Message(m) => {
                assert!(
                    m.contains("VECTORIZED COLUMNAR SCAN"),
                    "Expected vectorized path: {}",
                    m
                );
            }
            _ => panic!("Expected plan"),
        }
    }
    cleanup(&path);
}

#[test]
fn test_codec_randomized_fuzz_2000_vectors() {
    let mut rng = XorShift64::new(0xDEAD_BEEF_CAFE_BABE);
    for iter in 0..2000usize {
        let len = 1 + rng.range(128);
        let null_prob = rng.range(4); // 0=never, 1=sparse, 2=half, 3=all
        match iter % 4 {
            0 => {
                // Integer fuzz across distributions (constant, delta, FOR, dict, full i64)
                let dist = rng.range(5);
                let mut vals = Vec::with_capacity(len);
                let mut cur = rng.next_u64() as i64;
                for _ in 0..len {
                    if (null_prob == 3) || (null_prob == 2 && rng.range(2) == 0) || (null_prob == 1 && rng.range(10) == 0) {
                        vals.push(Value::Null);
                        continue;
                    }
                    let v = match dist {
                        0 => 42,
                        1 => {
                            cur = cur.wrapping_add((rng.range(20) as i64) - 5);
                            cur
                        }
                        2 => 10_000 + (rng.range(64) as i64),
                        3 => [i64::MIN, i64::MAX, -1, 0, 1][rng.range(5)],
                        _ => rng.next_u64() as i64,
                    };
                    vals.push(Value::Integer(v));
                }
                let mut buf = Vec::new();
                encode_column(ColumnType::Integer, &vals, &mut buf).unwrap();
                let mut pos = 0;
                let dec = decode_column(ColumnType::Integer, &buf, &mut pos, vals.len()).unwrap();
                for (a, b) in vals.iter().zip(&dec) {
                    assert!(a.exact_eq(b), "Int fuzz fail iter {}", iter);
                }
            }
            1 => {
                // Real fuzz (exact decimals, -0.0, NaN, inf, subnormal, random bits)
                let dist = rng.range(4);
                let mut vals = Vec::with_capacity(len);
                for _ in 0..len {
                    if (null_prob == 3) || (null_prob == 2 && rng.range(2) == 0) {
                        vals.push(Value::Null);
                        continue;
                    }
                    let v = match dist {
                        0 => ((rng.range(10_000) as i64) - 5000) as f64 / 100.0,
                        1 => [-0.0, 0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MIN_POSITIVE / 4.0][rng.range(6)],
                        2 => 3.141592653589793,
                        _ => f64::from_bits(rng.next_u64()),
                    };
                    vals.push(Value::Real(v));
                }
                let mut buf = Vec::new();
                encode_column(ColumnType::Real, &vals, &mut buf).unwrap();
                let mut pos = 0;
                let dec = decode_column(ColumnType::Real, &buf, &mut pos, vals.len()).unwrap();
                for (a, b) in vals.iter().zip(&dec) {
                    assert!(a.exact_eq(b), "Real fuzz fail iter {}: {:?} != {:?}", iter, a, b);
                }
            }
            2 => {
                // Text fuzz
                let dist = rng.range(3);
                let mut vals = Vec::with_capacity(len);
                for _ in 0..len {
                    if (null_prob == 3) || (null_prob == 2 && rng.range(2) == 0) {
                        vals.push(Value::Null);
                        continue;
                    }
                    let s = match dist {
                        0 => format!("prefix/path/item_{}", rng.range(12)),
                        1 => ["", "αβγ", "🚀🔥", "hello'world"][rng.range(4)].to_string(),
                        _ => format!("rnd_{:016x}", rng.next_u64()),
                    };
                    vals.push(Value::Text(s));
                }
                let mut buf = Vec::new();
                encode_column(ColumnType::Text, &vals, &mut buf).unwrap();
                let mut pos = 0;
                let dec = decode_column(ColumnType::Text, &buf, &mut pos, vals.len()).unwrap();
                for (a, b) in vals.iter().zip(&dec) {
                    assert!(a.exact_eq(b), "Text fuzz fail iter {}", iter);
                }
            }
            _ => {
                // Blob fuzz
                let mut vals = Vec::with_capacity(len);
                for _ in 0..len {
                    if (null_prob == 3) || (null_prob == 2 && rng.range(2) == 0) {
                        vals.push(Value::Null);
                        continue;
                    }
                    let blen = rng.range(24);
                    let mut b = Vec::with_capacity(blen);
                    for _ in 0..blen {
                        b.push((rng.next_u64() & 0xFF) as u8);
                    }
                    vals.push(Value::Blob(b));
                }
                let mut buf = Vec::new();
                encode_column(ColumnType::Blob, &vals, &mut buf).unwrap();
                let mut pos = 0;
                let dec = decode_column(ColumnType::Blob, &buf, &mut pos, vals.len()).unwrap();
                for (a, b) in vals.iter().zip(&dec) {
                    assert!(a.exact_eq(b), "Blob fuzz fail iter {}", iter);
                }
            }
        }
    }
}

#[test]
fn test_randomized_oltp_property_fuzzer_against_oracle() {
    let path = temp_db("oltp_fuzz");
    let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0);
    let mut oracle: BTreeMap<i64, (String, i64)> = BTreeMap::new();
    let mut next_id = 1i64;
    let mut has_idx = false;

    let mut opts = DbOptions::default();
    opts.page_rows = 32; // Force frequent page creation, merges, and index block splits!
    opts.auto_checkpoint_rows = 80;
    let mut db = Some(Database::open_with_options(&path, opts.clone()).unwrap());
    db.as_mut()
        .unwrap()
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT, val INTEGER);")
        .unwrap();

    for step in 0..350usize {
        let d = db.as_mut().unwrap();
        match rng.range(10) {
            0..=3 => {
                // INSERT 1..5 rows
                let cnt = 1 + rng.range(5);
                for _ in 0..cnt {
                    let id = next_id;
                    next_id += 1;
                    let tag = format!("tag_{}", rng.range(30));
                    let val = (rng.next_u64() % 10_000) as i64 - 5000;
                    d.execute(&format!(
                        "INSERT INTO t VALUES ({}, '{}', {});",
                        id, tag, val
                    ))
                    .unwrap();
                    oracle.insert(id, (tag, val));
                }
            }
            4 => {
                // UPDATE random existing row
                if !oracle.is_empty() {
                    let keys: Vec<i64> = oracle.keys().copied().collect();
                    let k = keys[rng.range(keys.len())];
                    let new_tag = format!("tag_{}", rng.range(30));
                    let new_val = (rng.next_u64() % 10_000) as i64 - 5000;
                    d.execute(&format!(
                        "UPDATE t SET tag = '{}', val = {} WHERE id = {};",
                        new_tag, new_val, k
                    ))
                    .unwrap();
                    oracle.insert(k, (new_tag, new_val));
                }
            }
            5 => {
                // DELETE random existing row
                if !oracle.is_empty() {
                    let keys: Vec<i64> = oracle.keys().copied().collect();
                    let k = keys[rng.range(keys.len())];
                    d.execute(&format!("DELETE FROM t WHERE id = {};", k))
                        .unwrap();
                    oracle.remove(&k);
                }
            }
            6 => {
                // Aborted transaction ROLLBACK (must leave oracle unchanged!)
                d.begin().unwrap();
                let ghost = next_id + 100_000;
                d.execute(&format!("INSERT INTO t VALUES ({}, 'ghost', 999);", ghost))
                    .unwrap();
                d.execute("DELETE FROM t WHERE val > 4000;").unwrap();
                d.rollback().unwrap();
            }
            7 => {
                // Toggle Secondary Index
                if !has_idx {
                    d.execute("CREATE INDEX idx_tag ON t (tag);").unwrap();
                    has_idx = true;
                } else {
                    d.execute("DROP INDEX idx_tag;").unwrap();
                    has_idx = false;
                }
            }
            8 => {
                // CHECKPOINT or VACUUM (with optional CLUSTER BY!)
                if rng.range(2) == 0 {
                    d.checkpoint().unwrap();
                } else {
                    if rng.range(2) == 0 {
                        d.execute("VACUUM t SORT BY tag, id;").unwrap();
                    } else {
                        d.vacuum().unwrap();
                    }
                }
            }
            _ => {
                // Close & Reopen DB to verify persistence + WAL replay!
                drop(db.take());
                db = Some(Database::open_with_options(&path, opts.clone()).unwrap());
            }
        }

        // Periodically verify 100% EXACT state equality against oracle (plus indexed point query!)
        if step % 15 == 0 || step == 349 {
            let d = db.as_mut().unwrap();
            let res = d.execute("SELECT id, tag, val FROM t ORDER BY id ASC;").unwrap();
            let rows = match &res[0] {
                QueryResult::Rows { rows, .. } => rows,
                _ => panic!("Expected rows"),
            };
            assert_eq!(rows.len(), oracle.len(), "Row count mismatch at step {}", step);
            for (r, (&eid, (etag, eval))) in rows.iter().zip(oracle.iter()) {
                assert!(r[0].exact_eq(&Value::Integer(eid)), "step {} id mismatch", step);
                assert!(r[1].exact_eq(&Value::Text(etag.clone())), "step {} tag mismatch", step);
                assert!(r[2].exact_eq(&Value::Integer(*eval)), "step {} val mismatch", step);
            }
            // Also verify indexed/bloom equality query matches oracle!
            let probe_tag = format!("tag_{}", rng.range(30));
            let expected_cnt = oracle.values().filter(|(t, _)| t == &probe_tag).count();
            let qres = d
                .execute(&format!("SELECT id FROM t WHERE tag = '{}';", probe_tag))
                .unwrap();
            match &qres[0] {
                QueryResult::Rows { rows, .. } => {
                    assert_eq!(
                        rows.len(),
                        expected_cnt,
                        "Indexed/Bloom query mismatch at step {} for {}",
                        step,
                        probe_tag
                    );
                }
                _ => panic!("Expected rows"),
            }
        }
    }

    drop(db);
    cleanup(&path);
}

#[test]
fn test_crash_wal_truncation_every_byte_and_corruption_detection() {
    let base_path = temp_db("crash_base");
    let mut wal_bytes = Vec::new();
    let mut db_bytes = Vec::new();

    // 1. Create committed base DB (gen 2, so BOTH Superblock A and B have valid generations!)
    //    then write an uncheckpointed WAL transaction and simulate abrupt crash.
    {
        let mut db = Database::open(&base_path).unwrap();
        db.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT);")
            .unwrap();
        db.execute("INSERT INTO log VALUES (1, 'committed_v1');")
            .unwrap();
        db.checkpoint().unwrap(); // writes Slot B
        db.execute("INSERT INTO log VALUES (2, 'committed_v2');")
            .unwrap();
        db.checkpoint().unwrap(); // writes Slot A (now Slot A = gen 3, Slot B = gen 2)

        // Uncheckpointed transaction in WAL
        db.execute("BEGIN; INSERT INTO log VALUES (3, 'in_wal_a'), (4, 'in_wal_b'); COMMIT;")
            .unwrap();
        // Simulate abrupt power loss (prevent Drop checkpoint!)
        std::mem::forget(db);
    }

    // Read base DB bytes and WAL bytes
    {
        std::fs::File::open(&base_path)
            .unwrap()
            .read_to_end(&mut db_bytes)
            .unwrap();
        let mut wp = base_path.as_os_str().to_os_string();
        wp.push("-wal");
        std::fs::File::open(PathBuf::from(wp))
            .unwrap()
            .read_to_end(&mut wal_bytes)
            .unwrap();
    }
    assert!(wal_bytes.len() > 25);

    // 2. Power-loss fuzz: truncate WAL at EVERY SINGLE BYTE OFFSET 0..=wal_bytes.len()!
    //    Recovery must NEVER panic/corrupt and must be strictly atomic (either 2 rows OR 4 rows!).
    for cut in 0..=wal_bytes.len() {
        let p = temp_db(&format!("trunc_{}", cut));
        std::fs::write(&p, &db_bytes).unwrap();
        let mut wp = p.as_os_str().to_os_string();
        wp.push("-wal");
        std::fs::write(PathBuf::from(wp), &wal_bytes[..cut]).unwrap();

        let db = Database::open(&p).unwrap();
        let cnt = db.count_rows("log").unwrap();
        if cut == wal_bytes.len() {
            assert_eq!(cnt, 4, "Full WAL must recover all 4 rows");
        } else {
            assert_eq!(
                cnt, 2,
                "Partial WAL cut at byte {} must atomically roll back to 2 rows!",
                cut
            );
        }
        drop(db);
        cleanup(&p);
    }

    // 3. Superblock A corruption -> must seamlessly fail over to intact Superblock B!
    {
        let p = temp_db("sb_failover");
        let mut corrupted = db_bytes.clone();
        // Corrupt Superblock A (offset 0..512)
        for b in &mut corrupted[0..64] {
            *b ^= 0xFF;
        }
        std::fs::write(&p, &corrupted).unwrap();
        let db = Database::open(&p).unwrap();
        // Should cleanly recover the previous generation from Superblock B (1 or 2 rows, zero crash!)
        assert!(db.count_rows("log").unwrap() >= 1);
        drop(db);
        cleanup(&p);
    }

    // 4. Bit-flip inside a live persisted PAX page -> CRC32 must detect corruption loudly!
    {
        let p = temp_db("page_corrupt");
        std::fs::write(&p, &db_bytes).unwrap();
        // Vacuum first so the live PAX page sits cleanly at offset 1024
        {
            let mut db = Database::open(&p).unwrap();
            db.vacuum().unwrap();
        }
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start(1024)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            b[0] ^= 0x55;
            f.seek(SeekFrom::Start(1024)).unwrap();
            f.write_all(&b).unwrap();
        }
        let mut db = Database::open(&p).unwrap();
        let err = db.execute("SELECT * FROM log;");
        assert!(
            matches!(err, Err(Error::Corruption(_))),
            "Expected CRC32 Corruption error on bit-flipped PAX page, got {:?}",
            err
        );
        drop(db);
        cleanup(&p);
    }

    cleanup(&base_path);
}

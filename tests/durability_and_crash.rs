use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use squishdb::pax::CompressionProfile;
use squishdb::storage::SUPERBLOCK_SIZE;
use squishdb::types::{Error, Value};
use squishdb::{Database, DbOptions, QueryResult};

fn temp_db(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "squishdb_durability_{}_{}.sqdb",
        name,
        std::process::id()
    ));
    cleanup(&p);
    p
}

fn wal_path(db: &Path) -> PathBuf {
    let mut s = db.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}

fn cleanup(p: &Path) {
    let _ = std::fs::remove_file(p);
    for suffix in ["-wal", ".lock", ".vacuum", "-mvcc.log", "-mvcc.commit.lock", "-mvcc.snap.lock"] {
        let mut s = p.as_os_str().to_os_string();
        s.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(s));
    }
}

fn read_slot_generations(db_bytes: &[u8]) -> (u64, u64) {
    assert!(db_bytes.len() >= (SUPERBLOCK_SIZE as usize) * 2);
    let gen_a = u64::from_le_bytes(db_bytes[12..20].try_into().unwrap());
    let gen_b = u64::from_le_bytes(
        db_bytes[(SUPERBLOCK_SIZE as usize) + 12..(SUPERBLOCK_SIZE as usize) + 20]
            .try_into()
            .unwrap(),
    );
    (gen_a, gen_b)
}

/// Simulates a truncated or bit-corrupted WAL frame mid-transaction:
/// - Tx1 is checkpointed in `.sqdb`.
/// - Tx2 is committed in `.sqdb-wal`.
/// - Tx3 is appended to `.sqdb-wal`, then either truncated at every byte offset or bit-flipped
///   at every byte (including header `magic`, `base_generation`, `payload_len`, `crc32`, and payload).
/// Guarantees CRC32 rejects the incomplete/corrupt Tx3 without corrupting Tx1 or Tx2.
#[test]
fn test_wal_mid_tx_truncation_and_every_byte_bitflip_preserves_committed_txs() {
    let base = temp_db("wal_mid_tx");
    let mut db_bytes = Vec::new();
    let mut wal_tx2_bytes = Vec::new();
    let mut wal_tx2_tx3_bytes = Vec::new();

    {
        let mut db = Database::open(&base).unwrap();
        db.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note TEXT, amt INTEGER);")
            .unwrap();
        db.execute("INSERT INTO ledger VALUES (1, 'checkpointed_tx1', 100);")
            .unwrap();
        db.checkpoint().unwrap();

        // Tx2 committed in WAL
        db.execute("BEGIN; INSERT INTO ledger VALUES (2, 'committed_tx2_a', 200), (3, 'committed_tx2_b', 300); COMMIT;")
            .unwrap();
        std::fs::File::open(wal_path(&base))
            .unwrap()
            .read_to_end(&mut wal_tx2_bytes)
            .unwrap();

        // Tx3 committed in WAL (we will corrupt/truncate this second frame)
        db.execute("BEGIN; INSERT INTO ledger VALUES (4, 'uncommitted_tx3_a', 400), (5, 'uncommitted_tx3_b', 500); COMMIT;")
            .unwrap();
        std::mem::forget(db);
    }

    std::fs::File::open(&base)
        .unwrap()
        .read_to_end(&mut db_bytes)
        .unwrap();
    std::fs::File::open(wal_path(&base))
        .unwrap()
        .read_to_end(&mut wal_tx2_tx3_bytes)
        .unwrap();

    let tx2_len = wal_tx2_bytes.len();
    let total_len = wal_tx2_tx3_bytes.len();
    assert!(tx2_len > 20 && total_len > tx2_len + 20);

    // 1. Truncate inside Tx3 at every byte offset [tx2_len..total_len)
    for cut in tx2_len..total_len {
        let p = temp_db(&format!("wal_cut_{}", cut));
        std::fs::write(&p, &db_bytes).unwrap();
        std::fs::write(wal_path(&p), &wal_tx2_tx3_bytes[..cut]).unwrap();

        let mut db = Database::open(&p).unwrap();
        let res = db
            .execute("SELECT id, note, amt FROM ledger ORDER BY id ASC;")
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3, "Truncated Tx3 at cut={} must leave exactly 3 rows", cut);
                assert!(rows[0][0].exact_eq(&Value::Integer(1)));
                assert!(rows[1][0].exact_eq(&Value::Integer(2)));
                assert!(rows[2][0].exact_eq(&Value::Integer(3)));
                assert!(rows[1][1].exact_eq(&Value::Text("committed_tx2_a".into())));
                assert!(rows[2][2].exact_eq(&Value::Integer(300)));
            }
            _ => panic!("Expected rows"),
        }
        drop(db);
        cleanup(&p);
    }

    // 2. Flip bits at EVERY SINGLE BYTE offset inside Tx3 (header + payload)
    for byte_idx in tx2_len..total_len {
        let p = temp_db(&format!("wal_flip_{}", byte_idx));
        let mut corrupt_wal = wal_tx2_tx3_bytes.clone();
        corrupt_wal[byte_idx] ^= 0x01;
        std::fs::write(&p, &db_bytes).unwrap();
        std::fs::write(wal_path(&p), &corrupt_wal).unwrap();

        let mut db = Database::open(&p).unwrap();
        let res = db
            .execute("SELECT id, amt FROM ledger ORDER BY id ASC;")
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    3,
                    "Bit-flip at Tx3 WAL byte {} (rel {}) must be rejected by CRC32 while preserving Tx1+Tx2",
                    byte_idx,
                    byte_idx - tx2_len
                );
                assert!(rows[0][1].exact_eq(&Value::Integer(100)));
                assert!(rows[1][1].exact_eq(&Value::Integer(200)));
                assert!(rows[2][1].exact_eq(&Value::Integer(300)));
            }
            _ => panic!("Expected rows"),
        }
        drop(db);
        cleanup(&p);
    }

    cleanup(&base);
}

/// Regression test: after recovering from a truncated/corrupt WAL tail, `Database::open`
/// must truncate the torn tail so subsequent committed transactions survive another crash.
#[test]
fn test_wal_torn_tail_truncated_on_open_so_subsequent_commits_survive_second_crash() {
    let p1 = temp_db("wal_second_crash_1");
    let p2 = temp_db("wal_second_crash_2");
    let p3 = temp_db("wal_second_crash_3");

    // Process 1: commit base + WAL row 2, then crash abruptly (forget handle without Drop checkpoint).
    {
        let mut db = Database::open(&p1).unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'base');").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO items VALUES (2, 'wal_ok');").unwrap();
        std::mem::forget(db);
    }

    // Copy crashed DB + WAL to p2 (simulates OS releasing dead process lock) and append a torn frame.
    std::fs::copy(&p1, &p2).unwrap();
    std::fs::copy(wal_path(&p1), wal_path(&p2)).unwrap();
    {
        let mut f = OpenOptions::new().append(true).open(wal_path(&p2)).unwrap();
        f.write_all(b"SQWL\x02\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\xDE\xAD\xBE\xEFpartial_tx")
            .unwrap();
    }

    // Process 2: reopen (must recover rows 1 & 2 AND truncate the torn WAL tail), commit row 3, crash again!
    {
        let mut db = Database::open(&p2).unwrap();
        assert_eq!(db.count_rows("items").unwrap(), 2);
        db.execute("INSERT INTO items VALUES (3, 'after_recovery');")
            .unwrap();
        std::mem::forget(db);
    }

    // Process 3: reopen after second crash (copy files to p3 so lock is fresh) -> row 3 MUST survive!
    std::fs::copy(&p2, &p3).unwrap();
    std::fs::copy(wal_path(&p2), wal_path(&p3)).unwrap();
    {
        let mut db = Database::open(&p3).unwrap();
        let res = db
            .execute("SELECT id, name FROM items ORDER BY id ASC;")
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3, "Row committed after torn-WAL recovery must survive a second crash");
                assert!(rows[0][1].exact_eq(&Value::Text("base".into())));
                assert!(rows[1][1].exact_eq(&Value::Text("wal_ok".into())));
                assert!(rows[2][1].exact_eq(&Value::Text("after_recovery".into())));
            }
            _ => panic!("Expected rows"),
        }
    }

    cleanup(&p1);
    cleanup(&p2);
    cleanup(&p3);
}

/// Regression test for A/B superblock failover when the NEWER superblock is torn or corrupted
/// mid-checkpoint: the previous valid superblock's catalog and pages must remain intact on disk.
#[test]
fn test_ab_superblock_mid_checkpoint_torn_newer_slot_fails_over_cleanly() {
    let base = temp_db("ab_superblock_base");
    let mut db_bytes = Vec::new();
    let mut wal_after_gen3 = Vec::new();

    {
        let mut opts = DbOptions::default();
        opts.page_rows = 32;
        let mut db = Database::open_with_options(&base, opts).unwrap();
        db.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, tag TEXT, val INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_kv_tag ON kv (tag);").unwrap();

        // Checkpoint #1: 50 rows (gen 2)
        for i in 1..=50 {
            db.execute(&format!("INSERT INTO kv VALUES ({}, 'v1_{}', {});", i, i % 5, i * 10))
                .unwrap();
        }
        db.checkpoint().unwrap();

        // Checkpoint #2: 50 more rows (gen 3) -> updates the OTHER superblock slot
        for i in 51..=100 {
            db.execute(&format!("INSERT INTO kv VALUES ({}, 'v2_{}', {});", i, i % 5, i * 10))
                .unwrap();
        }
        db.checkpoint().unwrap();

        // Snapshot committed `.sqdb` with valid gen 2 in one slot and valid gen 3 in the other
        std::fs::File::open(&base)
            .unwrap()
            .read_to_end(&mut db_bytes)
            .unwrap();

        // Also create an uncheckpointed WAL transaction on top of gen 3
        db.execute("BEGIN; INSERT INTO kv VALUES (101, 'wal_row', 1010), (102, 'wal_row', 1020); COMMIT;")
            .unwrap();
        std::fs::File::open(wal_path(&base))
            .unwrap()
            .read_to_end(&mut wal_after_gen3)
            .unwrap();
        std::mem::forget(db);
    }

    let (gen_a, gen_b) = read_slot_generations(&db_bytes);
    assert_ne!(gen_a, gen_b, "Expected distinct generations across A/B slots");
    let newer_slot_offset = if gen_b > gen_a {
        SUPERBLOCK_SIZE as usize
    } else {
        0usize
    };
    let older_slot_offset = if gen_b > gen_a {
        0usize
    } else {
        SUPERBLOCK_SIZE as usize
    };
    let newer_gen = gen_a.max(gen_b);
    let older_gen = gen_a.min(gen_b);
    assert_eq!(newer_gen, older_gen + 1);

    // Case 1: Tear the NEWER superblock (mid-write during checkpoint #2: half new bytes, half zeros).
    // Must cleanly fall back to the OLDER superblock (gen 2, 50 rows) AND allow subsequent commits!
    {
        let p = temp_db("ab_torn_newer");
        let mut torn = db_bytes.clone();
        // Zero out second half of the newer superblock (simulating torn sector write)
        for b in &mut torn[newer_slot_offset + 128..newer_slot_offset + 512] {
            *b = 0;
        }
        std::fs::write(&p, &torn).unwrap();

        {
            let mut db = Database::open(&p).unwrap();
            assert_eq!(db.stats().unwrap().generation, older_gen);
            assert_eq!(db.count_rows("kv").unwrap(), 50);
            let res = db
                .execute("SELECT COUNT(*), SUM(val) FROM kv WHERE tag = 'v1_0';")
                .unwrap();
            match &res[0] {
                QueryResult::Rows { rows, .. } => {
                    assert!(rows[0][0].exact_eq(&Value::Integer(10)));
                }
                _ => panic!("Expected aggregate row"),
            }
            // Write new data and checkpoint to verify the DB heals the damaged slot cleanly
            db.execute("INSERT INTO kv VALUES (500, 'healed', 5000);")
                .unwrap();
            db.checkpoint().unwrap();
        }
        {
            let db = Database::open(&p).unwrap();
            assert_eq!(db.count_rows("kv").unwrap(), 51);
        }
        cleanup(&p);
    }

    // Case 2: Simulate crash mid-checkpoint #3:
    // - Newer slot (gen 3) is intact
    // - Uncommitted trailing page bytes were appended to `.sqdb` past gen 3's catalog
    // - Older slot was partially overwritten with a torn gen 4 superblock
    // - `.sqdb-wal` still holds committed rows 101 & 102 (checkpoint never reached wal.truncate)
    {
        let p = temp_db("ab_mid_ckpt3_with_wal");
        let mut crashed = db_bytes.clone();
        // Append uncommitted half-written page/catalog bytes from aborted checkpoint #3
        crashed.extend_from_slice(&[0xAB; 4096]);
        // Simulate torn write of gen 4 into the older slot
        crashed[older_slot_offset..older_slot_offset + 8].copy_from_slice(b"SQSHDB05");
        crashed[older_slot_offset + 12..older_slot_offset + 20]
            .copy_from_slice(&(newer_gen + 1).to_le_bytes());
        crashed[older_slot_offset + 200..older_slot_offset + 512].fill(0x00);

        std::fs::write(&p, &crashed).unwrap();
        std::fs::write(wal_path(&p), &wal_after_gen3).unwrap();

        let mut db = Database::open(&p).unwrap();
        assert_eq!(db.stats().unwrap().generation, newer_gen);
        assert_eq!(db.count_rows("kv").unwrap(), 102);
        let res = db
            .execute("SELECT id, val FROM kv WHERE tag = 'wal_row' ORDER BY id ASC;")
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert!(rows[0][0].exact_eq(&Value::Integer(101)));
                assert!(rows[1][0].exact_eq(&Value::Integer(102)));
            }
            _ => panic!("Expected WAL rows"),
        }
        db.checkpoint().unwrap();
        drop(db);

        let db2 = Database::open(&p).unwrap();
        assert_eq!(db2.count_rows("kv").unwrap(), 102);
        drop(db2);
        cleanup(&p);
    }

    // Case 3: File truncated before the newer slot's catalog landed (older slot's catalog still in bounds).
    {
        let p = temp_db("ab_truncated_before_new_cat");
        let newer_cat_off = u64::from_le_bytes(
            db_bytes[newer_slot_offset + 20..newer_slot_offset + 28]
                .try_into()
                .unwrap(),
        ) as usize;
        let truncated = &db_bytes[..newer_cat_off + 1]; // cuts off newer catalog, keeps older catalog intact
        std::fs::write(&p, truncated).unwrap();

        let db = Database::open(&p).unwrap();
        assert_eq!(db.stats().unwrap().generation, older_gen);
        assert_eq!(db.count_rows("kv").unwrap(), 50);
        drop(db);
        cleanup(&p);
    }

    // Case 4: Truncated non-empty file (< 1024 bytes) must return Corruption, never wipe silently.
    {
        let p = temp_db("truncated_header");
        std::fs::write(&p, &db_bytes[..300]).unwrap();
        assert!(matches!(Database::open(&p), Err(Error::Corruption(_))));
        cleanup(&p);
    }

    cleanup(&base);
}

/// Regression test: if power fails after `VACUUM` atomically replaces `.sqdb` but before
/// `.sqdb-wal` is truncated, reopening must ignore stale pre-vacuum WAL frames instead of
/// replaying them and duplicating rows.
#[test]
fn test_vacuum_crash_before_wal_truncate_does_not_replay_stale_wal() {
    let p = temp_db("vacuum_stale_wal");
    let mut stale_wal = Vec::new();

    {
        let mut db = Database::open(&p).unwrap();
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, msg TEXT);")
            .unwrap();
        // Advance generation past 5
        for round in 0..5 {
            db.execute(&format!("INSERT INTO events VALUES ({}, 'r{}');", round + 1, round))
                .unwrap();
            db.checkpoint().unwrap();
        }
        // Commit uncheckpointed rows into WAL
        db.execute("BEGIN; INSERT INTO events VALUES (10, 'wal_a'), (11, 'wal_b'); COMMIT;")
            .unwrap();
        std::fs::File::open(wal_path(&p))
            .unwrap()
            .read_to_end(&mut stale_wal)
            .unwrap();
        assert!(!stale_wal.is_empty());

        let gen_before = db.stats().unwrap().generation;
        db.vacuum().unwrap();
        let gen_after = db.stats().unwrap().generation;
        assert!(
            gen_after > gen_before,
            "VACUUM must strictly increment superblock generation ({} -> {})",
            gen_before,
            gen_after
        );
    }

    // Put the stale pre-vacuum WAL back on disk (simulating crash right before wal.truncate())
    std::fs::write(wal_path(&p), &stale_wal).unwrap();

    {
        let mut db = Database::open(&p).unwrap();
        assert_eq!(
            db.count_rows("events").unwrap(),
            7,
            "Stale pre-vacuum WAL must NOT be replayed onto the vacuumed database"
        );
        let res = db
            .execute("SELECT id FROM events ORDER BY id ASC;")
            .unwrap();
        match &res[0] {
            QueryResult::Rows { rows, .. } => {
                let ids: Vec<i64> = rows
                    .iter()
                    .map(|r| match r[0] {
                        Value::Integer(v) => v,
                        _ => panic!("expected int"),
                    })
                    .collect();
                assert_eq!(ids, vec![1, 2, 3, 4, 5, 10, 11]);
            }
            _ => panic!("Expected rows"),
        }
    }

    cleanup(&p);
}

/// Bit-exact round-trip test across all types (`INTEGER`, `REAL` including `-0.0`/subnormals/NaN/Inf,
/// `TEXT` UTF-8, `BLOB`, `NULL`) through `bulk_insert`, `CHECKPOINT`, `VACUUM`, `VACUUM SORT BY`,
/// and full DB close/reopen across every compression profile.
#[test]
fn test_bit_exact_round_trip_all_types_bulk_checkpoint_vacuum_reopen() {
    let profiles = [
        ("none", CompressionProfile::None),
        ("fast", CompressionProfile::Fast),
        ("balanced", CompressionProfile::Balanced),
        ("max", CompressionProfile::Max),
    ];

    let special_ints: Vec<Option<i64>> = vec![
        Some(i64::MIN),
        Some(i64::MIN + 1),
        Some(-1_000_000_000_000),
        Some(-1),
        Some(0),
        Some(1),
        Some(42),
        Some(1_000_000_000_000),
        Some(i64::MAX - 1),
        Some(i64::MAX),
        None,
    ];

    let special_reals: Vec<Option<f64>> = vec![
        Some(-0.0_f64),
        Some(0.0_f64),
        Some(f64::MIN),
        Some(f64::MAX),
        Some(f64::MIN_POSITIVE),
        Some(-f64::MIN_POSITIVE),
        Some(f64::from_bits(1)),                    // smallest positive subnormal
        Some(f64::from_bits(0x8000_0000_0000_0001)), // smallest negative subnormal
        Some(f64::EPSILON),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(f64::from_bits(0x7ff8_0000_0000_1234)), // quiet NaN with custom payload
        Some(0.01),
        Some(-123.45),
        Some(999_999.999_999),
        Some(std::f64::consts::PI),
        None,
    ];

    let special_texts: Vec<Option<String>> = vec![
        Some(String::new()),
        Some("ascii_plain".into()),
        Some("single'quote\"double\\slash\nnewline\ttab".into()),
        Some("café naïve résumé".into()),
        Some("日本語テスト Ελληνικά Кириллица العربية".into()),
        Some("🚀🔥🧪🦀👩🏽‍💻".into()),
        Some("long_string_".repeat(12)), // > 64 bytes (zone-map long string path)
        None,
    ];

    let all_bytes: Vec<u8> = (0u8..=255).collect();
    let large_blob: Vec<u8> = (0..4096usize).map(|i| ((i * 73 + 19) & 0xFF) as u8).collect();
    let special_blobs: Vec<Option<Vec<u8>>> = vec![
        Some(Vec::new()),
        Some(vec![0u8]),
        Some(vec![0u8, 0xFF, 0x00, 0x7F, 0x80]),
        Some(all_bytes),
        Some(large_blob),
        None,
    ];

    // Build 180 rows (spans 3 PAX pages when page_rows = 64) covering every edge value & combination
    let mut expected: Vec<Vec<Value>> = Vec::with_capacity(180);
    for i in 0..180usize {
        let id = (i as i64) + 1;
        let iv = match special_ints[i % special_ints.len()] {
            Some(v) => Value::Integer(v),
            None => Value::Null,
        };
        let rv = match special_reals[i % special_reals.len()] {
            Some(v) => Value::Real(v),
            None => Value::Null,
        };
        let tv = match &special_texts[i % special_texts.len()] {
            Some(v) => Value::Text(v.clone()),
            None => Value::Null,
        };
        let bv = match &special_blobs[i % special_blobs.len()] {
            Some(v) => Value::Blob(v.clone()),
            None => Value::Null,
        };
        expected.push(vec![Value::Integer(id), iv, rv, tv, bv]);
    }

    let verify_exact = |db: &mut Database, stage: &str| {
        let res = db
            .execute("SELECT id, i_col, r_col, t_col, b_col FROM exact_rt ORDER BY id ASC;")
            .unwrap();
        let rows = match &res[0] {
            QueryResult::Rows { rows, .. } => rows,
            _ => panic!("Expected rows at {}", stage),
        };
        assert_eq!(rows.len(), expected.len(), "Row count mismatch at {}", stage);
        for (r_idx, (got, want)) in rows.iter().zip(expected.iter()).enumerate() {
            assert_eq!(got.len(), want.len());
            for (c_idx, (g, w)) in got.iter().zip(want.iter()).enumerate() {
                assert!(
                    g.exact_eq(w),
                    "Bit-exact mismatch at {} row {} col {}: got {:?} (bits {:?}) vs want {:?} (bits {:?})",
                    stage,
                    r_idx,
                    c_idx,
                    g,
                    match g {
                        Value::Real(f) => Some(format!("{:#018x}", f.to_bits())),
                        _ => None,
                    },
                    w,
                    match w {
                        Value::Real(f) => Some(format!("{:#018x}", f.to_bits())),
                        _ => None,
                    }
                );
            }
        }
    };

    for (pname, profile) in profiles {
        let p = temp_db(&format!("bit_exact_{}", pname));
        let mut opts = DbOptions::default();
        opts.page_rows = 64;
        opts.compression = profile;

        // Stage 1: bulk_insert -> verify -> checkpoint -> close
        {
            let mut db = Database::open_with_options(&p, opts.clone()).unwrap();
            db.execute(
                "CREATE TABLE exact_rt (
                    id INTEGER PRIMARY KEY,
                    i_col INTEGER,
                    r_col REAL,
                    t_col TEXT,
                    b_col BLOB
                 );",
            )
            .unwrap();
            db.bulk_insert("exact_rt", expected.clone()).unwrap();
            verify_exact(&mut db, &format!("{}/after_bulk_insert", pname));
            db.checkpoint().unwrap();
            verify_exact(&mut db, &format!("{}/after_checkpoint", pname));
        }

        // Stage 2: Reopen -> verify -> VACUUM -> close
        {
            let mut db = Database::open_with_options(&p, opts.clone()).unwrap();
            verify_exact(&mut db, &format!("{}/reopen_after_checkpoint", pname));
            db.vacuum().unwrap();
            verify_exact(&mut db, &format!("{}/after_vacuum", pname));
        }

        // Stage 3: Reopen -> verify -> VACUUM SORT BY (exercises re-clustering with NULLs, -0.0, NaN) -> close
        {
            let mut db = Database::open_with_options(&p, opts.clone()).unwrap();
            verify_exact(&mut db, &format!("{}/reopen_after_vacuum", pname));
            db.execute("VACUUM exact_rt SORT BY r_col, id;").unwrap();
            verify_exact(&mut db, &format!("{}/after_vacuum_sort_by", pname));
        }

        // Stage 4: Final reopen after VACUUM SORT BY
        {
            let mut db = Database::open_with_options(&p, opts.clone()).unwrap();
            verify_exact(&mut db, &format!("{}/reopen_after_vacuum_sort_by", pname));
        }

        cleanup(&p);
    }
}

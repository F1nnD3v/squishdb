use squishdb::types::{ColumnDef, ColumnType, Value};
use squishdb::{Database, QueryResult};
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "squishdb_audit_{}_{}.sqsh",
        name,
        std::process::id()
    ));
    cleanup(&p);
    p
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-wal", ".lock", ".vacuum"] {
        let mut s = path.as_os_str().to_os_string();
        s.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(s));
    }
}

fn query_rows(db: &mut Database, sql: &str) -> Vec<Vec<Value>> {
    let mut res = db.execute(sql).unwrap();
    match res.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("Expected rows for `{}`, got {:?}", sql, other),
    }
}

fn val_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Real(x), Value::Real(y)) => (x - y).abs() < 1e-12 || x.to_bits() == y.to_bits(),
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Blob(x), Value::Blob(y)) => x == y,
        _ => false,
    }
}

macro_rules! assert_rows {
    ($a:expr, $b:expr $(,)?) => {
        assert_rows_eq(&$a, &$b);
    };
}

fn assert_rows_eq(actual: &[Vec<Value>], expected: &[Vec<Value>]) {
    let ok = actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(ar, er)| ar.len() == er.len() && ar.iter().zip(er.iter()).all(|(a, b)| val_eq(a, b)));
    assert!(ok, "Row mismatch!\n  actual:   {:?}\n  expected: {:?}", actual, expected);
}

#[test]
fn test_codex_backup_to_atomic_overwrite() {
    let src = temp_db_path("backup_src");
    let dst = temp_db_path("backup_dst");

    // Pre-populate destination with an older backup
    std::fs::write(&dst, b"OLD_BACKUP_SENTINEL").unwrap();

    let mut db = Database::open(&src).unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
        .unwrap();

    // Backup over existing destination must atomically replace it
    db.backup_to(&dst).unwrap();

    // Verify backup is a valid, readable database with all rows
    let mut bk = Database::open(&dst).unwrap();
    let rows = query_rows(&mut bk, "SELECT id, name FROM items ORDER BY id");
    assert_rows!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Text("alpha".into())],
            vec![Value::Integer(2), Value::Text("beta".into())],
        ]
    );

    // Overwrite a second time after more writes
    db.execute("INSERT INTO items VALUES (3, 'gamma')")
        .unwrap();
    drop(bk);
    db.backup_to(&dst).unwrap();

    let mut bk2 = Database::open(&dst).unwrap();
    let rows2 = query_rows(&mut bk2, "SELECT COUNT(*) FROM items");
    assert_rows!(rows2, vec![vec![Value::Integer(3)]]);

    drop(bk2);
    drop(db);
    cleanup(&src);
    cleanup(&dst);
}

#[test]
fn test_bug1_i64_min_div_neg_one_no_panic() {
    let path = temp_db_path("bug1_div");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(i64::MIN), Value::Integer(-1)])
        .unwrap();

    let res = db.execute("SELECT a / b FROM t");
    assert!(res.is_err(), "Expected overflow error for i64::MIN / -1");

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug2_empty_table_wildcard_aggregate_order_by() {
    let path = temp_db_path("bug2_empty_agg");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();

    let rows = query_rows(&mut db, "SELECT *, COUNT(*) AS c FROM t ORDER BY c");
    assert_rows!(
        rows,
        vec![vec![Value::Null, Value::Null, Value::Integer(0)]]
    );

    let rows2 = query_rows(&mut db, "SELECT x.*, COUNT(*) AS c FROM t AS x ORDER BY a");
    assert_rows!(
        rows2,
        vec![vec![Value::Null, Value::Null, Value::Integer(0)]]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug3_non_ascii_hex_blob_no_panic() {
    let path = temp_db_path("bug3_hex");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (b BLOB)").unwrap();

    let err = db.execute("INSERT INTO t VALUES (X'🔥')");
    assert!(err.is_err());
    let err2 = db.execute("INSERT INTO t VALUES (X'éé')");
    assert!(err2.is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug4_single_table_aliases_and_wrong_qualifiers() {
    let path = temp_db_path("bug4_alias");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
        .unwrap();
    db.checkpoint().unwrap();

    // Valid alias in WHERE, SELECT, GROUP BY, and indexed lookup
    let r1 = query_rows(&mut db, "SELECT x.id, x.val FROM t AS x WHERE x.cat = 'a' ORDER BY x.id");
    assert_rows!(
        r1,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );

    let r2 = query_rows(
        &mut db,
        "SELECT x.cat, SUM(x.val) FROM t AS x WHERE x.val > 5 GROUP BY x.cat ORDER BY x.cat",
    );
    assert_rows!(
        r2,
        vec![
            vec![Value::Text("a".into()), Value::Integer(30)],
            vec![Value::Text("b".into()), Value::Integer(30)],
        ]
    );

    // Wrong qualifiers MUST error even on indexed / pruned / vectorized paths
    assert!(db
        .execute("SELECT x.id FROM t AS x WHERE wrong.cat = 'a'")
        .is_err());
    assert!(db
        .execute("SELECT wrong.id FROM t AS x WHERE x.cat = 'a'")
        .is_err());
    assert!(db
        .execute("SELECT wrong.* FROM t AS x WHERE x.cat = 'a'")
        .is_err());
    assert!(db
        .execute("SELECT x.cat, SUM(wrong.val) FROM t AS x GROUP BY x.cat")
        .is_err());
    // Original table name should NOT be visible once aliased to `x`
    assert!(db
        .execute("SELECT t.id FROM t AS x WHERE t.cat = 'a'")
        .is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug5_numeric_cmp_and_hash_consistency() {
    let path = temp_db_path("bug5_numcmp");
    let mut db = Database::open(&path).unwrap();

    // Direct Value::sql_cmp transitivity & exactness
    let big_i = Value::Integer(9007199254740993); // 2^53 + 1
    let big_f = Value::Real(9007199254740992.0); // 2^53
    assert!(big_i.sql_cmp(&big_f).is_gt());
    assert!(big_f.sql_cmp(&big_i).is_lt());

    let neg_zero = Value::Real(-0.0);
    let pos_zero = Value::Real(0.0);
    let int_zero = Value::Integer(0);
    assert!(neg_zero.sql_cmp(&pos_zero).is_eq());
    assert!(neg_zero.sql_cmp(&int_zero).is_eq());

    // End-to-end SQL with index lookup on REAL column
    db.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, x REAL)")
        .unwrap();
    db.execute("CREATE INDEX idx_rx ON r(x)").unwrap();
    db.insert_row("r", vec![Value::Integer(1), Value::Real(-0.0)])
        .unwrap();
    db.insert_row("r", vec![Value::Integer(2), Value::Real(9007199254740992.0)])
        .unwrap();
    db.checkpoint().unwrap();

    let r0 = query_rows(&mut db, "SELECT id FROM r WHERE x = 0.0");
    assert_rows!(r0, vec![vec![Value::Integer(1)]]);

    let r_int0 = query_rows(&mut db, "SELECT id FROM r WHERE x = 0");
    assert_rows!(r_int0, vec![vec![Value::Integer(1)]]);

    // 2^53 + 1 must NOT match 2^53.0
    let r_no = query_rows(&mut db, "SELECT id FROM r WHERE x = 9007199254740993");
    assert!(r_no.is_empty());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug6_i64_max_rowid_no_overflow() {
    let path = temp_db_path("bug6_max_rowid");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (9223372036854775807, 'max')")
        .unwrap();
    db.execute("UPDATE t SET v = 'max2' WHERE id = 9223372036854775807")
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let mut db2 = Database::open(&path).unwrap();
    let rows = query_rows(&mut db2, "SELECT id, v FROM t");
    assert_rows!(
        rows,
        vec![vec![Value::Integer(i64::MAX), Value::Text("max2".into())]]
    );

    drop(db2);
    cleanup(&path);
}

#[test]
fn test_bug7_fast_path_c_does_not_skip_having() {
    let path = temp_db_path("bug7_having");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_k ON t(k)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'a')")
        .unwrap();
    db.checkpoint().unwrap();

    let rows = query_rows(&mut db, "SELECT k FROM t WHERE k = 'a' HAVING COUNT(*) > 5");
    assert!(rows.is_empty());

    let rows2 = query_rows(&mut db, "SELECT k FROM t WHERE k = 'a' HAVING COUNT(*) = 2");
    assert_rows!(rows2, vec![vec![Value::Text("a".into())]]);

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug8_explicit_null_vs_default() {
    let path = temp_db_path("bug8_default");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER DEFAULT 42)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO t (id, x) VALUES (2, NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();

    let rows = query_rows(&mut db, "SELECT id, x FROM t ORDER BY id");
    assert_rows!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(42)],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Null],
        ]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug9_non_integer_primary_key_rejects_null() {
    let path = temp_db_path("bug9_text_pk");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    assert!(db.execute("INSERT INTO t VALUES (NULL, 1)").is_err());
    db.execute("INSERT INTO t VALUES ('ok', 1)").unwrap();
    assert!(db
        .execute("UPDATE t SET k = NULL WHERE k = 'ok'")
        .is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug10_fast_count_star_limit_offset() {
    let path = temp_db_path("bug10_count_limit");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    assert!(query_rows(&mut db, "SELECT COUNT(*) FROM t LIMIT 0").is_empty());
    assert!(query_rows(&mut db, "SELECT COUNT(*) FROM t OFFSET 1").is_empty());
    assert_rows!(
        query_rows(&mut db, "SELECT COUNT(*) FROM t LIMIT 1 OFFSET 0"),
        vec![vec![Value::Integer(3)]]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug11_vectorized_group_by_non_numeric_and_having_ops() {
    let path = temp_db_path("bug11_vec_gb");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (g TEXT, s TEXT, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('a', 'hello', 10), ('a', 'world', 20)")
        .unwrap();
    db.checkpoint().unwrap();

    // SUM/AVG on TEXT must return an error, not silently return 0/NULL
    assert!(db.execute("SELECT g, SUM(s) FROM t GROUP BY g").is_err());
    assert!(db.execute("SELECT g, AVG(s) FROM t GROUP BY g").is_err());

    // HAVING with LIKE must fall back and evaluate LIKE properly
    let rows = query_rows(
        &mut db,
        "SELECT g, MIN(s) AS ms FROM t GROUP BY g HAVING ms LIKE 'hel%'",
    );
    assert_rows!(
        rows,
        vec![vec![Value::Text("a".into()), Value::Text("hello".into())]]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug12_unsupported_joins_error_explicitly() {
    let path = temp_db_path("bug12_joins");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE a (id INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").unwrap();

    assert!(db
        .execute("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .is_err());
    assert!(db
        .execute("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
        .is_err());
    assert!(db.execute("SELECT * FROM a CROSS JOIN b").is_err());
    assert!(db.execute("SELECT * FROM a NATURAL JOIN b").is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug13_three_valued_logic_between_and_empty_in() {
    let path = temp_db_path("bug13_3vl");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    db.execute("CREATE TABLE empty_t (y INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (5), (NULL)").unwrap();

    // 5 BETWEEN 10 AND NULL => (5 >= 10) AND (5 <= NULL) => FALSE AND NULL => FALSE (0)
    // 5 NOT BETWEEN 10 AND NULL => TRUE (1)
    let r = query_rows(
        &mut db,
        "SELECT x, x BETWEEN 10 AND NULL, x NOT BETWEEN 10 AND NULL, x BETWEEN 1 AND NULL FROM t ORDER BY x",
    );
    assert_rows!(
        r,
        vec![
            vec![Value::Null, Value::Null, Value::Null, Value::Null],
            vec![
                Value::Integer(5),
                Value::Integer(0),
                Value::Integer(1),
                Value::Null
            ],
        ]
    );

    // NULL IN (empty subquery) => 0; NULL NOT IN (empty subquery) => 1
    let r2 = query_rows(
        &mut db,
        "SELECT x, x IN (SELECT y FROM empty_t), x NOT IN (SELECT y FROM empty_t) FROM t WHERE x IS NULL",
    );
    assert_rows!(
        r2,
        vec![vec![Value::Null, Value::Integer(0), Value::Integer(1)]]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug14_subqueries_in_dml_case_and_having() {
    let path = temp_db_path("bug14_subq");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE keep (id INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0), (2, 0), (3, 0)")
        .unwrap();
    db.execute("INSERT INTO keep VALUES (1), (2)").unwrap();

    // CASE WHEN x IN (SELECT ...)
    let r_case = query_rows(
        &mut db,
        "SELECT id, CASE WHEN id IN (SELECT id FROM keep) THEN 'yes' ELSE 'no' END FROM t ORDER BY id",
    );
    assert_rows!(
        r_case,
        vec![
            vec![Value::Integer(1), Value::Text("yes".into())],
            vec![Value::Integer(2), Value::Text("yes".into())],
            vec![Value::Integer(3), Value::Text("no".into())],
        ]
    );

    // UPDATE with IN (SELECT ...) in SET and WHERE
    db.execute(
        "UPDATE t SET flag = CASE WHEN id IN (SELECT id FROM keep) THEN 1 ELSE 2 END WHERE id IN (SELECT id FROM keep)",
    )
    .unwrap();
    // DELETE with NOT IN (SELECT ...)
    db.execute("DELETE FROM t WHERE id NOT IN (SELECT id FROM keep)")
        .unwrap();

    let rows = query_rows(&mut db, "SELECT id, flag FROM t ORDER BY id");
    assert_rows!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(1)],
        ]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug15_window_peer_ties_and_non_numeric_error() {
    let path = temp_db_path("bug15_window");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (k INTEGER, v INTEGER, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c')")
        .unwrap();

    // SQL RANGE default: peers with k=1 share running sum 30, count 2, avg 15.0
    let rows = query_rows(
        &mut db,
        "SELECT k, SUM(v) OVER (ORDER BY k), COUNT(*) OVER (ORDER BY k), AVG(v) OVER (ORDER BY k) FROM t ORDER BY k, v",
    );
    assert_rows!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(30),
                Value::Integer(2),
                Value::Real(15.0)
            ],
            vec![
                Value::Integer(1),
                Value::Integer(30),
                Value::Integer(2),
                Value::Real(15.0)
            ],
            vec![
                Value::Integer(2),
                Value::Integer(35),
                Value::Integer(3),
                Value::Real(35.0 / 3.0)
            ],
        ]
    );

    // Non-numeric SUM/AVG OVER (ORDER BY ...) must error
    assert!(db
        .execute("SELECT SUM(s) OVER (ORDER BY k) FROM t")
        .is_err());
    assert!(db
        .execute("SELECT AVG(s) OVER (ORDER BY k) FROM t")
        .is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug16_ambiguous_columns_and_duplicate_join_alias() {
    let path = temp_db_path("bug16_ambig");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE a (id INTEGER, x INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER, x INTEGER)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 20)").unwrap();

    // Ambiguous unqualified `x`
    assert!(db
        .execute("SELECT x FROM a JOIN b ON a.id = b.id")
        .is_err());
    // Duplicate alias in self-join
    assert!(db
        .execute("SELECT a.id FROM a JOIN a ON a.id = a.id")
        .is_err());
    // Properly aliased self-join works
    let ok = query_rows(
        &mut db,
        "SELECT a1.x, a2.x FROM a a1 JOIN a a2 ON a1.id = a2.id",
    );
    assert_rows!(ok, vec![vec![Value::Integer(10), Value::Integer(10)]]);

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug17_i64_min_sql_literal() {
    let path = temp_db_path("bug17_i64_min");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (x INTEGER DEFAULT -9223372036854775808)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (-9223372036854775808)")
        .unwrap();

    let rows = query_rows(&mut db, "SELECT x FROM t WHERE x = -9223372036854775808");
    assert_rows!(rows, vec![vec![Value::Integer(i64::MIN)]]);

    // Positive 2^63 must error as out of range
    assert!(db
        .execute("INSERT INTO t VALUES (9223372036854775808)")
        .is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug18_missing_semicolon_syntax_error() {
    let path = temp_db_path("bug18_semi");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();

    assert!(db
        .execute("SELECT * FROM t SELECT * FROM t")
        .is_err());
    assert!(db.execute("SELECT * FROM t AS a GARBAGE").is_err());
    assert!(db.execute("DELETE FROM t GARBAGE").is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug19_truncated_value_deserialization_no_overflow() {
    // Tag 3 (Text) with length u32::MAX (0xFFFFFFFF) in only 5 bytes
    let bad = vec![3u8, 0xFF, 0xFF, 0xFF, 0xFF];
    let mut pos = 0usize;
    assert!(Value::read_binary(&bad, &mut pos).is_err());
}

#[test]
fn test_bug20_pk_update_wal_atomic_replay() {
    let path = temp_db_path("bug20_pk_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
            .unwrap();
        db.checkpoint().unwrap();

        // Update PK without checkpointing (stays in WAL)
        db.execute("UPDATE t SET id = 100, v = 'hundred' WHERE id = 1")
            .unwrap();
    }

    // Reopen and verify WAL replayed cleanly without resurrecting id=1
    let mut db2 = Database::open(&path).unwrap();
    let rows = query_rows(&mut db2, "SELECT id, v FROM t ORDER BY id");
    assert_rows!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Text("two".into())],
            vec![Value::Integer(100), Value::Text("hundred".into())],
        ]
    );

    db2.checkpoint().unwrap();
    let rows_after = query_rows(&mut db2, "SELECT id, v FROM t ORDER BY id");
    assert_rows!(rows, rows_after);

    drop(db2);
    cleanup(&path);
}

#[test]
fn test_bug21_quoted_identifiers_and_escaped_quotes() {
    let path = temp_db_path("bug21_quoted");
    let mut db = Database::open(&path).unwrap();
    db.execute(r#"CREATE TABLE "from" ("null" INTEGER, "true" TEXT, "a""b" INTEGER)"#)
        .unwrap();
    db.execute(r#"INSERT INTO "from" VALUES (7, 'hi', 99)"#)
        .unwrap();

    let rows = query_rows(
        &mut db,
        r#"SELECT "null", "true", "a""b" FROM "from" WHERE "null" = 7"#,
    );
    assert_rows!(
        rows,
        vec![vec![
            Value::Integer(7),
            Value::Text("hi".into()),
            Value::Integer(99)
        ]]
    );

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug22_unknown_qualified_wildcard_errors() {
    let path = temp_db_path("bug22_wildcard");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    assert!(db.execute("SELECT nonexistent.* FROM t").is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug23_add_column_constraints_and_insert_row_rowid() {
    let path = temp_db_path("bug23_ddl");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // insert_row must return the actual explicit PK rowid!
    let r1 = db
        .insert_row("t", vec![Value::Integer(500), Value::Text("a".into())])
        .unwrap();
    assert_eq!(r1, 500);
    let r2 = db
        .insert_row("t", vec![Value::Integer(10), Value::Text("b".into())])
        .unwrap();
    assert_eq!(r2, 10);

    // Adding a PRIMARY KEY column to a non-empty table must fail
    assert!(db
        .add_column(
            "t",
            ColumnDef {
                name: "pk2".into(),
                col_type: ColumnType::Text,
                primary_key: true,
                not_null: true,
                unique: true,
                default_value: Some(Value::Text("x".into())),
            },
        )
        .is_err());

    // Adding a UNIQUE column with non-NULL default to a table with >1 rows must fail
    assert!(db
        .add_column(
            "t",
            ColumnDef {
                name: "u".into(),
                col_type: ColumnType::Integer,
                primary_key: false,
                not_null: false,
                unique: true,
                default_value: Some(Value::Integer(1)),
            },
        )
        .is_err());

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug24_count_distinct_hash_buckets() {
    let path = temp_db_path("bug24_distinct");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (k INTEGER)").unwrap();
    let batch: Vec<Vec<Value>> = (0..10_000)
        .map(|i| {
            if i % 100 == 0 {
                vec![Value::Null]
            } else {
                vec![Value::Integer(i % 1337)]
            }
        })
        .collect();
    db.insert_batch("t", batch).unwrap();

    let rows = query_rows(&mut db, "SELECT COUNT(DISTINCT k) FROM t");
    assert_rows!(rows, vec![vec![Value::Integer(1337)]]);

    drop(db);
    cleanup(&path);
}

#[test]
fn test_bug25_dirty_and_transaction_state() {
    let path = temp_db_path("bug25_state");
    let mut db = Database::open(&path).unwrap();
    assert!(!db.is_dirty());
    assert!(!db.in_transaction());

    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    assert!(db.is_dirty());
    db.checkpoint().unwrap();
    assert!(!db.is_dirty());

    db.execute("BEGIN").unwrap();
    assert!(db.in_transaction());
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();
    assert!(!db.in_transaction());

    drop(db);
    cleanup(&path);
}

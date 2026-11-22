use std::ffi::{CStr, CString};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::ptr;

use squishdb::ffi::*;
use squishdb::types::Value;
use squishdb::{Database, QueryResult};

fn temp_db(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_test_{}_{}.sqdb", name, std::process::id()));
    let _ = fs::remove_file(&p);
    let _ = fs::remove_file(format!("{}-wal", p.display()));
    let _ = fs::remove_file(format!("{}.lock", p.display()));
    p
}

fn cleanup_db(p: &std::path::Path) {
    let _ = fs::remove_file(p);
    let _ = fs::remove_file(format!("{}-wal", p.display()));
    let _ = fs::remove_file(format!("{}.lock", p.display()));
}

#[test]
fn test_execute_params_positional_and_numbered() {
    let path = temp_db("params");
    let mut db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, data BLOB);")
        .unwrap();

    // Positional ? parameters including SQL injection payload, blob, and NULL
    let evil = "'; DROP TABLE items; --";
    db.execute_params(
        "INSERT INTO items VALUES (?, ?, ?, ?);",
        &[
            Value::Integer(1),
            Value::Text(evil.to_string()),
            Value::Real(19.99),
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ],
    )
    .unwrap();

    db.execute_params(
        "INSERT INTO items VALUES ($1, $2, $3, $4);",
        &[
            Value::Integer(2),
            Value::Text("gadget".into()),
            Value::Null,
            Value::Blob(vec![]),
        ],
    )
    .unwrap();

    // Reused numbered parameter + parameterized LIMIT/OFFSET
    let res = db
        .execute_params(
            "SELECT id, name, price, data FROM items WHERE name = $1 OR name = $1 LIMIT $2 OFFSET $3;",
            &[
                Value::Text(evil.to_string()),
                Value::Integer(10),
                Value::Integer(0),
            ],
        )
        .unwrap();

    match &res[0] {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0][0].exact_eq(&Value::Integer(1)));
            assert!(rows[0][1].exact_eq(&Value::Text(evil.to_string())));
            assert!(rows[0][2].exact_eq(&Value::Real(19.99)));
            assert!(rows[0][3].exact_eq(&Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])));
        }
        other => panic!("Expected Rows, got {:?}", other),
    }

    // Missing, unused, and $0 params must fail cleanly
    assert!(db
        .execute_params("SELECT * FROM items WHERE id = ?;", &[])
        .is_err());
    assert!(db
        .execute_params(
            "SELECT * FROM items WHERE id = ?;",
            &[Value::Integer(1), Value::Integer(2)]
        )
        .is_err());
    assert!(db
        .execute_params("SELECT * FROM items WHERE id = $0;", &[Value::Integer(1)])
        .is_err());
    assert!(db
        .execute_params("SELECT * FROM items LIMIT ?;", &[Value::Integer(-1)])
        .is_err());
    assert!(db
        .execute_params("SELECT * FROM items LIMIT ?;", &[Value::Text("abc".into())])
        .is_err());

    drop(db);
    cleanup_db(&path);
}

#[test]
fn test_execute_params_dml_clauses_and_multistatement() {
    let path = temp_db("params_dml");
    let mut db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, price REAL);
         CREATE INDEX idx_sku ON products(sku);",
    )
    .unwrap();

    // Multi-statement positional parameters
    let res = db
        .execute_params(
            "INSERT INTO products VALUES (?, ?, ?);
             INSERT INTO products VALUES (?, ?, ?);
             INSERT INTO products VALUES (?, ?, ?);",
            &[
                Value::Integer(1),
                Value::Text("SKU-A".into()),
                Value::Real(10.0),
                Value::Integer(2),
                Value::Text("SKU-B".into()),
                Value::Real(25.0),
                Value::Integer(3),
                Value::Text("SKU-C".into()),
                Value::Real(50.0),
            ],
        )
        .unwrap();
    assert_eq!(res.len(), 3);

    // Parameterized UPDATE, IN, BETWEEN, LIKE, CASE, unary negation
    db.execute_params(
        "UPDATE products SET price = price + ? WHERE sku = ?;",
        &[Value::Real(5.0), Value::Text("SKU-B".into())],
    )
    .unwrap();

    let rows_res = db
        .execute_params(
            "SELECT id, CASE WHEN price >= ? THEN ? ELSE ? END AS tier
             FROM products
             WHERE id IN (?, ?, ?) AND price BETWEEN ? AND ? AND sku LIKE ?
             ORDER BY id;",
            &[
                Value::Real(25.0),
                Value::Text("high".into()),
                Value::Text("low".into()),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Real(10.0),
                Value::Real(35.0),
                Value::Text("SKU-%".into()),
            ],
        )
        .unwrap();
    match &rows_res[0] {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert!(rows[0][0].exact_eq(&Value::Integer(1)));
            assert!(rows[0][1].exact_eq(&Value::Text("low".into())));
            assert!(rows[1][0].exact_eq(&Value::Integer(2)));
            assert!(rows[1][1].exact_eq(&Value::Text("high".into())));
        }
        other => panic!("Expected Rows, got {:?}", other),
    }

    // Parameterized DELETE + unary minus
    db.execute_params(
        "DELETE FROM products WHERE id = -?;",
        &[Value::Integer(-3)],
    )
    .unwrap();
    assert_eq!(db.count_rows("products").unwrap(), 2);

    // Indexed lookup with parameter after checkpoint
    db.checkpoint().unwrap();
    let idx_res = db
        .execute_params("SELECT id FROM products WHERE sku = ?;", &[Value::Text("SKU-B".into())])
        .unwrap();
    match &idx_res[0] {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0][0].exact_eq(&Value::Integer(2)));
        }
        other => panic!("Expected Rows, got {:?}", other),
    }

    drop(db);
    cleanup_db(&path);
}

#[test]
fn test_c_ffi_roundtrip() {
    let path = temp_db("c_ffi");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();

    unsafe {
        let ver = CStr::from_ptr(squishdb_version()).to_str().unwrap();
        assert!(!ver.is_empty());

        let db = squishdb_open(cpath.as_ptr());
        assert!(
            !db.is_null(),
            "open error: {:?}",
            CStr::from_ptr(squishdb_last_error())
        );

        let ddl = CString::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT, score REAL, raw BLOB);",
        )
        .unwrap();
        let r = squishdb_execute(db, ddl.as_ptr());
        assert!(!r.is_null());
        squishdb_results_free(r);

        // Parameterized insert via SquishParams (including embedded NUL & multi-byte UTF-8)
        let params = squishdb_params_new();
        squishdb_params_push_int(params, 42);
        let txt = b"hello\0world";
        assert_eq!(
            squishdb_params_push_text(params, txt.as_ptr() as *const _, txt.len()),
            SQUISHDB_OK
        );
        squishdb_params_push_real(params, 3.14159);
        let blob = [1u8, 2, 3, 255];
        assert_eq!(
            squishdb_params_push_blob(params, blob.as_ptr(), blob.len()),
            SQUISHDB_OK
        );

        let ins = CString::new("INSERT INTO t VALUES (?, ?, ?, ?);").unwrap();
        let r2 = squishdb_execute_params(db, ins.as_ptr(), params);
        assert!(!r2.is_null());
        assert_eq!(squishdb_result_kind(r2, 0), SQUISHDB_RESULT_AFFECTED);
        assert_eq!(squishdb_result_affected(r2, 0), 1);
        squishdb_results_free(r2);

        // Reuse params after squishdb_params_clear with i64::MIN and UTF-8 emoji
        squishdb_params_clear(params);
        squishdb_params_push_int(params, i64::MIN);
        let uni = "日本語 🚀";
        assert_eq!(
            squishdb_params_push_text(params, uni.as_ptr() as *const _, uni.len()),
            SQUISHDB_OK
        );
        squishdb_params_push_real(params, f64::INFINITY);
        assert_eq!(squishdb_params_push_blob(params, ptr::null(), 0), SQUISHDB_OK);
        let r3 = squishdb_execute_params(db, ins.as_ptr(), params);
        assert!(!r3.is_null());
        squishdb_results_free(r3);
        squishdb_params_free(params);

        // Bulk insert via SquishBatch
        let batch = squishdb_batch_new(4);
        squishdb_batch_push_int(batch, i64::MAX);
        let lbl = CString::new("bulk_row").unwrap();
        assert_eq!(squishdb_batch_push_cstr(batch, lbl.as_ptr()), SQUISHDB_OK);
        squishdb_batch_push_null(batch);
        assert_eq!(squishdb_batch_push_blob(batch, ptr::null(), 0), SQUISHDB_OK);

        let tbl = CString::new("t").unwrap();
        let inserted = squishdb_bulk_insert(db, tbl.as_ptr(), batch);
        assert_eq!(inserted, 1);
        squishdb_batch_free(batch);

        // Query back and inspect typed values
        let q = CString::new("SELECT id, label, score, raw FROM t ORDER BY id;").unwrap();
        let qr = squishdb_execute(db, q.as_ptr());
        assert!(!qr.is_null());
        assert_eq!(squishdb_results_count(qr), 1);
        assert_eq!(squishdb_result_kind(qr, 0), SQUISHDB_RESULT_ROWS);
        assert_eq!(squishdb_result_column_count(qr, 0), 4);
        assert_eq!(squishdb_result_row_count(qr, 0), 3);

        // Row 0: i64::MIN, "日本語 🚀", +inf, empty blob
        assert_eq!(squishdb_result_value_int(qr, 0, 0, 0), i64::MIN);
        let mut ulen = 0usize;
        let uptr = squishdb_result_value_text(qr, 0, 0, 1, &mut ulen);
        assert_eq!(
            std::str::from_utf8(std::slice::from_raw_parts(uptr as *const u8, ulen)).unwrap(),
            uni
        );
        assert!(squishdb_result_value_real(qr, 0, 0, 2).is_infinite());
        let mut zlen = 99usize;
        let _zptr = squishdb_result_value_blob(qr, 0, 0, 3, &mut zlen);
        assert_eq!(zlen, 0);

        // Row 1: 42, "hello\0world", 3.14159, [1,2,3,255]
        assert_eq!(squishdb_result_value_type(qr, 0, 1, 0), SQUISHDB_TYPE_INTEGER);
        assert_eq!(squishdb_result_value_int(qr, 0, 1, 0), 42);
        let mut tlen = 0usize;
        let tptr = squishdb_result_value_text(qr, 0, 1, 1, &mut tlen);
        assert_eq!(tlen, txt.len());
        assert_eq!(std::slice::from_raw_parts(tptr as *const u8, tlen), txt);
        assert_eq!(squishdb_result_value_real(qr, 0, 1, 2), 3.14159);
        let mut blen = 0usize;
        let bptr = squishdb_result_value_blob(qr, 0, 1, 3, &mut blen);
        assert_eq!(blen, 4);
        assert_eq!(std::slice::from_raw_parts(bptr, blen), &blob);

        // Row 2: i64::MAX, NULL score
        assert_eq!(squishdb_result_value_int(qr, 0, 2, 0), i64::MAX);
        assert_eq!(squishdb_result_value_type(qr, 0, 2, 2), SQUISHDB_TYPE_NULL);

        // Out-of-bounds accessors must return safe defaults
        assert_eq!(squishdb_result_kind(qr, 99), -1);
        assert_eq!(squishdb_result_value_type(qr, 0, 99, 0), -1);
        assert_eq!(squishdb_result_value_type(qr, 0, 0, 99), -1);
        squishdb_results_free(qr);

        // Stats report, JSON & table names JSON
        let report = squishdb_stats_report(db);
        assert!(!report.is_null());
        assert!(CStr::from_ptr(report).to_str().unwrap().contains("Table 't'"));
        squishdb_string_free(report);

        let stats_json = squishdb_stats_json(db);
        assert!(!stats_json.is_null());
        let stats_str = CStr::from_ptr(stats_json).to_str().unwrap();
        assert!(stats_str.contains("\"tables\""));
        squishdb_string_free(stats_json);

        let tables_json = squishdb_table_names_json(db);
        assert!(!tables_json.is_null());
        assert_eq!(CStr::from_ptr(tables_json).to_str().unwrap(), "[\"t\"]");
        squishdb_string_free(tables_json);

        assert_eq!(squishdb_checkpoint(db), SQUISHDB_OK);
        assert_eq!(squishdb_vacuum(db), SQUISHDB_OK);
        squishdb_close(db);
    }

    cleanup_db(&path);
}

#[test]
fn test_c_ffi_error_paths_nulls_transactions_and_readonly() {
    let path = temp_db("c_ffi_errors");
    let backup = temp_db("c_ffi_backup");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();
    let cbackup = CString::new(backup.to_str().unwrap()).unwrap();

    unsafe {
        // Null pointers across all APIs must not crash
        assert!(squishdb_open(ptr::null()).is_null());
        assert!(!CStr::from_ptr(squishdb_last_error()).to_str().unwrap().is_empty());
        assert!(squishdb_open_readonly(ptr::null()).is_null());
        assert!(squishdb_open_with_options(cpath.as_ptr(), 999, 0, 0, 0, 0, 0).is_null());
        assert!(squishdb_execute(ptr::null_mut(), ptr::null()).is_null());
        assert_eq!(squishdb_results_count(ptr::null()), 0);
        assert_eq!(squishdb_result_kind(ptr::null(), 0), -1);
        assert_eq!(squishdb_begin(ptr::null_mut()), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_commit(ptr::null_mut()), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_rollback(ptr::null_mut()), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_in_transaction(ptr::null()), 0);
        assert_eq!(squishdb_checkpoint(ptr::null_mut()), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_vacuum(ptr::null_mut()), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_backup_to(ptr::null_mut(), cbackup.as_ptr()), SQUISHDB_ERR_NULL);
        assert!(squishdb_stats_json(ptr::null()).is_null());
        assert!(squishdb_table_names_json(ptr::null()).is_null());

        // Invalid UTF-8 & null-with-nonzero-len parameter errors
        let p = squishdb_params_new();
        let bad_utf8 = [0xFFu8, 0xFEu8];
        assert_eq!(
            squishdb_params_push_text(p, bad_utf8.as_ptr() as *const _, bad_utf8.len()),
            SQUISHDB_ERR_INVALID
        );
        assert_eq!(squishdb_params_push_text(p, ptr::null(), 3), SQUISHDB_ERR_NULL);
        assert_eq!(squishdb_params_push_blob(p, ptr::null(), 3), SQUISHDB_ERR_NULL);
        squishdb_params_free(p);

        // Open with explicit options
        let db = squishdb_open_with_options(
            cpath.as_ptr(),
            SQUISHDB_PROFILE_FAST,
            1,
            1024,
            4096,
            32,
            16,
        );
        assert!(!db.is_null());

        let ddl = CString::new("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);").unwrap();
        let r = squishdb_execute(db, ddl.as_ptr());
        assert!(!r.is_null());
        squishdb_results_free(r);

        // Incomplete batch must fail cleanly
        let bad_batch = squishdb_batch_new(2);
        squishdb_batch_push_int(bad_batch, 1);
        let tbl = CString::new("kv").unwrap();
        assert_eq!(squishdb_insert_batch(db, tbl.as_ptr(), bad_batch), -1);
        assert!(CStr::from_ptr(squishdb_last_error())
            .to_str()
            .unwrap()
            .contains("Incomplete"));
        squishdb_batch_free(bad_batch);

        // Transaction commit, double-begin error, and rollback
        assert_eq!(squishdb_in_transaction(db), 0);
        assert_eq!(squishdb_commit(db), SQUISHDB_ERR_SQL);
        assert_eq!(squishdb_begin(db), SQUISHDB_OK);
        assert_eq!(squishdb_in_transaction(db), 1);
        assert_eq!(squishdb_begin(db), SQUISHDB_ERR_SQL);

        let ins1 = CString::new("INSERT INTO kv VALUES (1, 'committed');").unwrap();
        let r = squishdb_execute(db, ins1.as_ptr());
        assert!(!r.is_null());
        squishdb_results_free(r);
        assert_eq!(squishdb_commit(db), SQUISHDB_OK);
        assert_eq!(squishdb_in_transaction(db), 0);

        assert_eq!(squishdb_begin(db), SQUISHDB_OK);
        let ins2 = CString::new("INSERT INTO kv VALUES (2, 'rolled_back');").unwrap();
        let r = squishdb_execute(db, ins2.as_ptr());
        assert!(!r.is_null());
        squishdb_results_free(r);
        assert_eq!(squishdb_rollback(db), SQUISHDB_OK);

        // Backup works and creates a readable copy
        assert_eq!(squishdb_backup_to(db, cbackup.as_ptr()), SQUISHDB_OK);
        squishdb_close(db);

        // Read-only rejects writes and allows concurrent readers
        let ro1 = squishdb_open_readonly(cbackup.as_ptr());
        let ro2 = squishdb_open_readonly(cbackup.as_ptr());
        assert!(!ro1.is_null());
        assert!(!ro2.is_null());

        let write_attempt = CString::new("INSERT INTO kv VALUES (3, 'nope');").unwrap();
        assert!(squishdb_execute(ro1, write_attempt.as_ptr()).is_null());

        let sel = CString::new("SELECT COUNT(*) FROM kv;").unwrap();
        let rr = squishdb_execute(ro2, sel.as_ptr());
        assert!(!rr.is_null());
        assert_eq!(squishdb_result_value_int(rr, 0, 0, 0), 1);
        squishdb_results_free(rr);

        squishdb_close(ro1);
        squishdb_close(ro2);
    }

    cleanup_db(&path);
    cleanup_db(&backup);
}

#[test]
fn test_rpc_stdio_protocol_edge_cases() {
    let bin = env!("CARGO_BIN_EXE_squishdb");
    let path = temp_db("rpc_proto");
    let backup = temp_db("rpc_backup");

    let mut child = Command::new(bin)
        .args(["rpc", path.to_str().unwrap(), "--profile", "fast"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn squishdb rpc");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    let mut send = |req: &str| -> String {
        writeln!(stdin, "{}", req).unwrap();
        stdin.flush().unwrap();
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        line
    };

    // 1. Ping
    let resp = send(r#"{"id":1,"method":"ping"}"#);
    assert!(resp.contains(r#""ok":true"#) && resp.contains(r#""read_only":false"#));

    // 2. Malformed JSON, trailing garbage, non-object JSON, unknown method
    assert!(send("not-json").contains(r#""ok":false"#));
    assert!(send("[1,2,3]").contains(r#""ok":false"#));
    assert!(send(r#"{"id":2,"method":"ping"} trailing"#).contains(r#""ok":false"#));
    assert!(send(r#"{"id":3,"method":"does_not_exist"}"#).contains(r#""ok":false"#));

    // 3. Create table & test UTF-16 surrogate pair emoji + extreme i64 + NaN/Inf + blob
    let resp = send(
        r#"{"id":4,"method":"execute","sql":"CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, f REAL, b BLOB);"}"#,
    );
    assert!(resp.contains(r#""ok":true"#), "create failed: {}", resp);

    // Malformed base64 padding must fail cleanly
    let bad_b64_1 = send(
        r#"{"id":5,"method":"execute","sql":"INSERT INTO t VALUES (1, 'a', 1.0, ?);","params":[{"$blob":"AA=A"}]}"#,
    );
    assert!(bad_b64_1.contains(r#""ok":false"#), "expected failure: {}", bad_b64_1);
    let bad_b64_2 = send(
        r#"{"id":6,"method":"execute","sql":"INSERT INTO t VALUES (1, 'a', 1.0, ?);","params":[{"$blob":"AA==AAAA"}]}"#,
    );
    assert!(bad_b64_2.contains(r#""ok":false"#), "expected failure: {}", bad_b64_2);

    // Valid extreme values + UTF-16 surrogate pair \uD83D\uDE80 (🚀) + control chars
    let ins_ok = send(
        r#"{"id":7,"method":"execute","sql":"INSERT INTO t VALUES (?, ?, ?, ?);","params":[{"$int":"-9223372036854775808"},"hi\n\t\"\\\uD83D\uDE80",{"$real":"NaN"},{"$blob":"3q2+7w=="}]}"#,
    );
    assert!(ins_ok.contains(r#""ok":true"#), "insert failed: {}", ins_ok);

    let ins_ok2 = send(
        r#"{"id":8,"method":"insert_batch","table":"t","rows":[[{"$int":"9223372036854775807"},"inf",{"$real":"-Infinity"},{"$blob":""}]]}"#,
    );
    assert!(ins_ok2.contains(r#""result":1"#), "insert_batch failed: {}", ins_ok2);

    // Query back and verify exact lossless encoding
    let sel = send(r#"{"id":9,"method":"execute","sql":"SELECT id, s, f, b FROM t ORDER BY id;"}"#);
    assert!(sel.contains(r#"{"$int":"-9223372036854775808"}"#), "sel={}", sel);
    assert!(sel.contains("🚀"), "missing emoji in sel={}", sel);
    assert!(sel.contains(r#"{"$real":"NaN"}"#), "missing NaN in sel={}", sel);
    assert!(sel.contains(r#"{"$blob":"3q2+7w=="}"#), "missing blob in sel={}", sel);
    assert!(sel.contains(r#"{"$int":"9223372036854775807"}"#), "sel={}", sel);
    assert!(sel.contains(r#"{"$real":"-Infinity"}"#), "sel={}", sel);

    // Transaction rollback + backup + vacuum + stats + tables + close
    assert!(send(r#"{"id":10,"method":"begin"}"#).contains(r#""ok":true"#));
    assert!(send(r#"{"id":11,"method":"in_transaction"}"#).contains(r#""result":true"#));
    assert!(send(r#"{"id":12,"method":"rollback"}"#).contains(r#""ok":true"#));
    assert!(send(r#"{"id":13,"method":"in_transaction"}"#).contains(r#""result":false"#));
    assert!(send(r#"{"id":14,"method":"vacuum"}"#).contains(r#""ok":true"#));

    let backup_escaped = backup.to_str().unwrap().replace('\\', "\\\\");
    let bk = send(&format!(
        r#"{{"id":15,"method":"backup_to","path":"{}"}}"#,
        backup_escaped
    ));
    assert!(bk.contains(r#""ok":true"#), "backup failed: {}", bk);
    assert!(send(r#"{"id":16,"method":"tables"}"#).contains(r#"["t"]"#));
    assert!(send(r#"{"id":17,"method":"stats"}"#).contains(r#""live_rows":2"#));
    assert!(send(r#"{"id":18,"method":"close"}"#).contains(r#""ok":true"#));

    let status = child.wait().unwrap();
    assert!(status.success());

    cleanup_db(&path);
    cleanup_db(&backup);
}

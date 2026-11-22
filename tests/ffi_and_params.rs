use std::ffi::{CStr, CString};
use std::fs;
use std::ptr;

use squishdb::ffi::*;
use squishdb::types::Value;
use squishdb::{Database, QueryResult};

fn temp_db(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("squishdb_test_{}_{}.sqdb", name, std::process::id()));
    let _ = fs::remove_file(&p);
    let _ = fs::remove_file(format!("{}-wal", p.display()));
    p
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

    // Missing and unused params must fail cleanly
    assert!(db
        .execute_params("SELECT * FROM items WHERE id = ?;", &[])
        .is_err());
    assert!(db
        .execute_params(
            "SELECT * FROM items WHERE id = ?;",
            &[Value::Integer(1), Value::Integer(2)]
        )
        .is_err());

    drop(db);
    let _ = fs::remove_file(&path);
}

#[test]
fn test_c_ffi_roundtrip() {
    let path = temp_db("c_ffi");
    let cpath = CString::new(path.to_str().unwrap()).unwrap();

    unsafe {
        let ver = CStr::from_ptr(squishdb_version()).to_str().unwrap();
        assert!(!ver.is_empty());

        let db = squishdb_open(cpath.as_ptr());
        assert!(!db.is_null(), "open error: {:?}", CStr::from_ptr(squishdb_last_error()));

        let ddl = CString::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT, score REAL, raw BLOB);",
        )
        .unwrap();
        let r = squishdb_execute(db, ddl.as_ptr());
        assert!(!r.is_null());
        squishdb_results_free(r);

        // Parameterized insert via SquishParams
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
        squishdb_params_free(params);

        // Bulk insert via SquishBatch
        let batch = squishdb_batch_new(4);
        squishdb_batch_push_int(batch, 43);
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
        assert_eq!(squishdb_result_row_count(qr, 0), 2);

        assert_eq!(squishdb_result_value_type(qr, 0, 0, 0), SQUISHDB_TYPE_INTEGER);
        assert_eq!(squishdb_result_value_int(qr, 0, 0, 0), 42);

        let mut tlen = 0usize;
        let tptr = squishdb_result_value_text(qr, 0, 0, 1, &mut tlen);
        assert_eq!(tlen, txt.len());
        assert_eq!(std::slice::from_raw_parts(tptr as *const u8, tlen), txt);

        assert_eq!(squishdb_result_value_real(qr, 0, 0, 2), 3.14159);

        let mut blen = 0usize;
        let bptr = squishdb_result_value_blob(qr, 0, 0, 3, &mut blen);
        assert_eq!(blen, 4);
        assert_eq!(std::slice::from_raw_parts(bptr, blen), &blob);

        assert_eq!(squishdb_result_value_type(qr, 0, 1, 2), SQUISHDB_TYPE_NULL);
        squishdb_results_free(qr);

        // Stats JSON & table names JSON
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
        squishdb_close(db);
    }

    let _ = fs::remove_file(&path);
}

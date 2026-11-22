//! Stable C ABI for SquishDB.
//!
//! Exposed in the `cdylib` (`squishdb.dll` / `libsquishdb.so` / `libsquishdb.dylib`)
//! and `staticlib` (`squishdb.lib` / `libsquishdb.a`) build targets so C, C++,
//! Python, Java, C#, Go, TypeScript/JavaScript, Swift, Ruby, PHP, and other
//! languages can embed SquishDB in-process.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

use crate::pax::CompressionProfile;
use crate::types::{Error, Value};
use crate::{Database, DbOptions, QueryResult};

// ============================================================================
// Status / Error Codes
// ============================================================================

pub const SQUISHDB_OK: c_int = 0;
pub const SQUISHDB_ERR_IO: c_int = 1;
pub const SQUISHDB_ERR_CORRUPTION: c_int = 2;
pub const SQUISHDB_ERR_SQL: c_int = 3;
pub const SQUISHDB_ERR_SCHEMA: c_int = 4;
pub const SQUISHDB_ERR_INVALID: c_int = 5;
pub const SQUISHDB_ERR_BUSY: c_int = 6;
pub const SQUISHDB_ERR_PANIC: c_int = 7;
pub const SQUISHDB_ERR_NULL: c_int = 8;

// ============================================================================
// Compression Profile Constants
// ============================================================================

pub const SQUISHDB_PROFILE_FAST: c_int = 0;
pub const SQUISHDB_PROFILE_BALANCED: c_int = 1;
pub const SQUISHDB_PROFILE_MAX: c_int = 2;
pub const SQUISHDB_PROFILE_NONE: c_int = 3;

// ============================================================================
// Result Kind & Value Type Constants
// ============================================================================

pub const SQUISHDB_RESULT_ROWS: c_int = 0;
pub const SQUISHDB_RESULT_AFFECTED: c_int = 1;
pub const SQUISHDB_RESULT_MESSAGE: c_int = 2;

pub const SQUISHDB_TYPE_NULL: c_int = 0;
pub const SQUISHDB_TYPE_INTEGER: c_int = 1;
pub const SQUISHDB_TYPE_REAL: c_int = 2;
pub const SQUISHDB_TYPE_TEXT: c_int = 3;
pub const SQUISHDB_TYPE_BLOB: c_int = 4;

static VERSION_CSTR: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
static EMPTY_CSTR: &[u8] = b"\0";
static EMPTY_BYTE: u8 = 0;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").unwrap());
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new("").unwrap();
    });
}

fn set_last_error(msg: &str) {
    let clean = msg.replace('\0', " ");
    let cs = CString::new(clean).unwrap_or_else(|_| CString::new("Unknown error").unwrap());
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = cs;
    });
}

fn error_to_code(err: &Error) -> c_int {
    set_last_error(&err.to_string());
    match err {
        Error::Io(_) => SQUISHDB_ERR_IO,
        Error::Corruption(_) => SQUISHDB_ERR_CORRUPTION,
        Error::Sql(_) => SQUISHDB_ERR_SQL,
        Error::Schema(_) => SQUISHDB_ERR_SCHEMA,
        Error::InvalidData(_) => SQUISHDB_ERR_INVALID,
        Error::Busy(_) => SQUISHDB_ERR_BUSY,
    }
}

unsafe fn cstr_to_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, c_int> {
    if ptr.is_null() {
        set_last_error(&format!("Null pointer passed for {}", name));
        return Err(SQUISHDB_ERR_NULL);
    }
    CStr::from_ptr(ptr).to_str().map_err(|e| {
        set_last_error(&format!("Invalid UTF-8 in {}: {}", name, e));
        SQUISHDB_ERR_INVALID
    })
}

fn string_to_c_alloc(s: String) -> *mut c_char {
    let clean = s.replace('\0', " ");
    match CString::new(clean) {
        Ok(cs) => cs.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

fn profile_from_int(v: c_int) -> Result<CompressionProfile, c_int> {
    match v {
        SQUISHDB_PROFILE_FAST => Ok(CompressionProfile::Fast),
        SQUISHDB_PROFILE_BALANCED => Ok(CompressionProfile::Balanced),
        SQUISHDB_PROFILE_MAX => Ok(CompressionProfile::Max),
        SQUISHDB_PROFILE_NONE => Ok(CompressionProfile::None),
        _ => {
            set_last_error(&format!("Invalid compression profile: {}", v));
            Err(SQUISHDB_ERR_INVALID)
        }
    }
}

// ============================================================================
// Opaque Handle Types
// ============================================================================

pub struct SquishDb {
    inner: Database,
}

pub struct SquishParams {
    values: Vec<Value>,
}

pub struct SquishBatch {
    num_cols: usize,
    rows: Vec<Vec<Value>>,
    current: Vec<Value>,
}

enum FfiValue {
    Null,
    Integer(i64),
    Real(f64),
    /// Stored with a trailing NUL byte so C callers can read it directly,
    /// while `len` holds the exact UTF-8 byte length (excluding the trailing NUL).
    Text { buf: Vec<u8>, len: usize },
    Blob(Vec<u8>),
}

enum FfiResultItem {
    Rows {
        columns: Vec<Vec<u8>>, // NUL-terminated UTF-8 names
        rows: Vec<Vec<FfiValue>>,
    },
    Affected(u64),
    Message(Vec<u8>), // NUL-terminated UTF-8 message
}

pub struct SquishResults {
    items: Vec<FfiResultItem>,
}

fn to_nul_terminated(s: String) -> Vec<u8> {
    let mut b = s.into_bytes();
    b.push(0);
    b
}

fn convert_results(results: Vec<QueryResult>) -> SquishResults {
    let items = results
        .into_iter()
        .map(|r| match r {
            QueryResult::Rows { columns, rows } => {
                let cols = columns.into_iter().map(to_nul_terminated).collect();
                let ffi_rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|v| match v {
                                Value::Null => FfiValue::Null,
                                Value::Integer(i) => FfiValue::Integer(i),
                                Value::Real(f) => FfiValue::Real(f),
                                Value::Text(s) => {
                                    let len = s.len();
                                    let mut buf = s.into_bytes();
                                    buf.push(0);
                                    FfiValue::Text { buf, len }
                                }
                                Value::Blob(b) => FfiValue::Blob(b),
                            })
                            .collect()
                    })
                    .collect();
                FfiResultItem::Rows {
                    columns: cols,
                    rows: ffi_rows,
                }
            }
            QueryResult::Affected(n) => FfiResultItem::Affected(n as u64),
            QueryResult::Message(m) => FfiResultItem::Message(to_nul_terminated(m)),
        })
        .collect();
    SquishResults { items }
}

// ============================================================================
// Version & Error Inspection
// ============================================================================

#[no_mangle]
pub extern "C" fn squishdb_version() -> *const c_char {
    VERSION_CSTR.as_ptr() as *const c_char
}

#[no_mangle]
pub extern "C" fn squishdb_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_string_free(s: *mut c_char) {
    if !s.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(CString::from_raw(s));
        }));
    }
}

// ============================================================================
// Database Lifecycle
// ============================================================================

#[no_mangle]
pub unsafe extern "C" fn squishdb_open(path: *const c_char) -> *mut SquishDb {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        let p = match cstr_to_str(path, "path") {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };
        match Database::open(p) {
            Ok(db) => Box::into_raw(Box::new(SquishDb { inner: db })),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(ptr) => ptr,
        Err(_) => {
            set_last_error("Internal panic in squishdb_open");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_open_readonly(path: *const c_char) -> *mut SquishDb {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        let p = match cstr_to_str(path, "path") {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };
        match Database::open_readonly(p) {
            Ok(db) => Box::into_raw(Box::new(SquishDb { inner: db })),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(ptr) => ptr,
        Err(_) => {
            set_last_error("Internal panic in squishdb_open_readonly");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_open_with_options(
    path: *const c_char,
    compression_profile: c_int,
    sync_wal: c_int,
    page_rows: usize,
    auto_checkpoint_rows: usize,
    cache_pages: usize,
    index_cache_blocks: usize,
) -> *mut SquishDb {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        let p = match cstr_to_str(path, "path") {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };
        let profile = match profile_from_int(compression_profile) {
            Ok(p) => p,
            Err(_) => return ptr::null_mut(),
        };
        let mut opts = DbOptions::default();
        opts.compression = profile;
        opts.sync_wal = sync_wal != 0;
        if page_rows > 0 {
            opts.page_rows = page_rows;
        }
        if auto_checkpoint_rows > 0 {
            opts.auto_checkpoint_rows = auto_checkpoint_rows;
        }
        if cache_pages > 0 {
            opts.cache_pages = cache_pages;
        }
        if index_cache_blocks > 0 {
            opts.index_cache_blocks = index_cache_blocks;
        }
        match Database::open_with_options(p, opts) {
            Ok(db) => Box::into_raw(Box::new(SquishDb { inner: db })),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(ptr) => ptr,
        Err(_) => {
            set_last_error("Internal panic in squishdb_open_with_options");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_close(db: *mut SquishDb) {
    if !db.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(Box::from_raw(db));
        }));
    }
}

// ============================================================================
// Parameter Builder
// ============================================================================

#[no_mangle]
pub extern "C" fn squishdb_params_new() -> *mut SquishParams {
    Box::into_raw(Box::new(SquishParams { values: Vec::new() }))
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_clear(params: *mut SquishParams) {
    if !params.is_null() {
        (*params).values.clear();
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_free(params: *mut SquishParams) {
    if !params.is_null() {
        drop(Box::from_raw(params));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_null(params: *mut SquishParams) {
    if !params.is_null() {
        (*params).values.push(Value::Null);
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_int(params: *mut SquishParams, v: i64) {
    if !params.is_null() {
        (*params).values.push(Value::Integer(v));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_real(params: *mut SquishParams, v: c_double) {
    if !params.is_null() {
        (*params).values.push(Value::Real(v));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_text(
    params: *mut SquishParams,
    ptr: *const c_char,
    len: usize,
) -> c_int {
    if params.is_null() {
        set_last_error("Null SquishParams pointer");
        return SQUISHDB_ERR_NULL;
    }
    let bytes = if len == 0 {
        &[]
    } else if ptr.is_null() {
        set_last_error("Null text pointer with non-zero length");
        return SQUISHDB_ERR_NULL;
    } else {
        slice::from_raw_parts(ptr as *const u8, len)
    };
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            (*params).values.push(Value::Text(s.to_string()));
            SQUISHDB_OK
        }
        Err(e) => {
            set_last_error(&format!("Invalid UTF-8 in text parameter: {}", e));
            SQUISHDB_ERR_INVALID
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_cstr(
    params: *mut SquishParams,
    s: *const c_char,
) -> c_int {
    if params.is_null() {
        set_last_error("Null SquishParams pointer");
        return SQUISHDB_ERR_NULL;
    }
    match cstr_to_str(s, "text") {
        Ok(text) => {
            (*params).values.push(Value::Text(text.to_string()));
            SQUISHDB_OK
        }
        Err(code) => code,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_params_push_blob(
    params: *mut SquishParams,
    data: *const u8,
    len: usize,
) -> c_int {
    if params.is_null() {
        set_last_error("Null SquishParams pointer");
        return SQUISHDB_ERR_NULL;
    }
    let bytes = if len == 0 {
        Vec::new()
    } else if data.is_null() {
        set_last_error("Null blob pointer with non-zero length");
        return SQUISHDB_ERR_NULL;
    } else {
        slice::from_raw_parts(data, len).to_vec()
    };
    (*params).values.push(Value::Blob(bytes));
    SQUISHDB_OK
}

// ============================================================================
// SQL Execution
// ============================================================================

#[no_mangle]
pub unsafe extern "C" fn squishdb_execute(
    db: *mut SquishDb,
    sql: *const c_char,
) -> *mut SquishResults {
    squishdb_execute_params(db, sql, ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_execute_params(
    db: *mut SquishDb,
    sql: *const c_char,
    params: *const SquishParams,
) -> *mut SquishResults {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return ptr::null_mut();
        }
        let sql_str = match cstr_to_str(sql, "sql") {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };
        let bound: &[Value] = if params.is_null() {
            &[]
        } else {
            &(*params).values
        };
        let out = if bound.is_empty() {
            (*db).inner.execute(sql_str)
        } else {
            (*db).inner.execute_params(sql_str, bound)
        };
        match out {
            Ok(r) => Box::into_raw(Box::new(convert_results(r))),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(ptr) => ptr,
        Err(_) => {
            set_last_error("Internal panic in squishdb_execute_params");
            ptr::null_mut()
        }
    }
}

// ============================================================================
// Result Inspection
// ============================================================================

#[no_mangle]
pub unsafe extern "C" fn squishdb_results_free(res: *mut SquishResults) {
    if !res.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(Box::from_raw(res));
        }));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_results_count(res: *const SquishResults) -> usize {
    if res.is_null() {
        return 0;
    }
    (&*res).items.len()
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_kind(
    res: *const SquishResults,
    stmt_idx: usize,
) -> c_int {
    if res.is_null() {
        return -1;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Rows { .. }) => SQUISHDB_RESULT_ROWS,
        Some(FfiResultItem::Affected(_)) => SQUISHDB_RESULT_AFFECTED,
        Some(FfiResultItem::Message(_)) => SQUISHDB_RESULT_MESSAGE,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_affected(
    res: *const SquishResults,
    stmt_idx: usize,
) -> u64 {
    if res.is_null() {
        return 0;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Affected(n)) => *n,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_message(
    res: *const SquishResults,
    stmt_idx: usize,
) -> *const c_char {
    if res.is_null() {
        return EMPTY_CSTR.as_ptr() as *const c_char;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Message(m)) => m.as_ptr() as *const c_char,
        _ => EMPTY_CSTR.as_ptr() as *const c_char,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_column_count(
    res: *const SquishResults,
    stmt_idx: usize,
) -> usize {
    if res.is_null() {
        return 0;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Rows { columns, .. }) => columns.len(),
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_column_name(
    res: *const SquishResults,
    stmt_idx: usize,
    col_idx: usize,
) -> *const c_char {
    if res.is_null() {
        return EMPTY_CSTR.as_ptr() as *const c_char;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Rows { columns, .. }) => columns
            .get(col_idx)
            .map(|c| c.as_ptr() as *const c_char)
            .unwrap_or(EMPTY_CSTR.as_ptr() as *const c_char),
        _ => EMPTY_CSTR.as_ptr() as *const c_char,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_row_count(
    res: *const SquishResults,
    stmt_idx: usize,
) -> usize {
    if res.is_null() {
        return 0;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Rows { rows, .. }) => rows.len(),
        _ => 0,
    }
}

#[inline]
unsafe fn get_value<'a>(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
) -> Option<&'a FfiValue> {
    if res.is_null() {
        return None;
    }
    match (&*res).items.get(stmt_idx) {
        Some(FfiResultItem::Rows { rows, .. }) => rows.get(row_idx).and_then(|r| r.get(col_idx)),
        _ => None,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_value_type(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
) -> c_int {
    match get_value(res, stmt_idx, row_idx, col_idx) {
        Some(FfiValue::Null) => SQUISHDB_TYPE_NULL,
        Some(FfiValue::Integer(_)) => SQUISHDB_TYPE_INTEGER,
        Some(FfiValue::Real(_)) => SQUISHDB_TYPE_REAL,
        Some(FfiValue::Text { .. }) => SQUISHDB_TYPE_TEXT,
        Some(FfiValue::Blob(_)) => SQUISHDB_TYPE_BLOB,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_value_int(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
) -> i64 {
    match get_value(res, stmt_idx, row_idx, col_idx) {
        Some(FfiValue::Integer(i)) => *i,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_value_real(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
) -> c_double {
    match get_value(res, stmt_idx, row_idx, col_idx) {
        Some(FfiValue::Real(f)) => *f,
        Some(FfiValue::Integer(i)) => *i as f64,
        _ => 0.0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_value_text(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
    out_len: *mut usize,
) -> *const c_char {
    match get_value(res, stmt_idx, row_idx, col_idx) {
        Some(FfiValue::Text { buf, len }) => {
            if !out_len.is_null() {
                *out_len = *len;
            }
            buf.as_ptr() as *const c_char
        }
        _ => {
            if !out_len.is_null() {
                *out_len = 0;
            }
            EMPTY_CSTR.as_ptr() as *const c_char
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_result_value_blob(
    res: *const SquishResults,
    stmt_idx: usize,
    row_idx: usize,
    col_idx: usize,
    out_len: *mut usize,
) -> *const u8 {
    match get_value(res, stmt_idx, row_idx, col_idx) {
        Some(FfiValue::Blob(b)) => {
            if !out_len.is_null() {
                *out_len = b.len();
            }
            if b.is_empty() {
                &EMPTY_BYTE as *const u8
            } else {
                b.as_ptr()
            }
        }
        _ => {
            if !out_len.is_null() {
                *out_len = 0;
            }
            &EMPTY_BYTE as *const u8
        }
    }
}

// ============================================================================
// High-Throughput Batch Insert Builder
// ============================================================================

#[no_mangle]
pub extern "C" fn squishdb_batch_new(num_cols: usize) -> *mut SquishBatch {
    Box::into_raw(Box::new(SquishBatch {
        num_cols: num_cols.max(1),
        rows: Vec::new(),
        current: Vec::with_capacity(num_cols.max(1)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_free(batch: *mut SquishBatch) {
    if !batch.is_null() {
        drop(Box::from_raw(batch));
    }
}

#[inline]
unsafe fn batch_push_val(batch: *mut SquishBatch, val: Value) {
    let b = &mut *batch;
    b.current.push(val);
    if b.current.len() >= b.num_cols {
        let cap = b.num_cols;
        let row = std::mem::replace(&mut b.current, Vec::with_capacity(cap));
        b.rows.push(row);
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_null(batch: *mut SquishBatch) {
    if !batch.is_null() {
        batch_push_val(batch, Value::Null);
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_int(batch: *mut SquishBatch, v: i64) {
    if !batch.is_null() {
        batch_push_val(batch, Value::Integer(v));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_real(batch: *mut SquishBatch, v: c_double) {
    if !batch.is_null() {
        batch_push_val(batch, Value::Real(v));
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_text(
    batch: *mut SquishBatch,
    ptr: *const c_char,
    len: usize,
) -> c_int {
    if batch.is_null() {
        set_last_error("Null SquishBatch pointer");
        return SQUISHDB_ERR_NULL;
    }
    let bytes = if len == 0 {
        &[]
    } else if ptr.is_null() {
        set_last_error("Null text pointer with non-zero length");
        return SQUISHDB_ERR_NULL;
    } else {
        slice::from_raw_parts(ptr as *const u8, len)
    };
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            batch_push_val(batch, Value::Text(s.to_string()));
            SQUISHDB_OK
        }
        Err(e) => {
            set_last_error(&format!("Invalid UTF-8 in batch text: {}", e));
            SQUISHDB_ERR_INVALID
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_cstr(
    batch: *mut SquishBatch,
    s: *const c_char,
) -> c_int {
    if batch.is_null() {
        set_last_error("Null SquishBatch pointer");
        return SQUISHDB_ERR_NULL;
    }
    match cstr_to_str(s, "text") {
        Ok(text) => {
            batch_push_val(batch, Value::Text(text.to_string()));
            SQUISHDB_OK
        }
        Err(code) => code,
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_batch_push_blob(
    batch: *mut SquishBatch,
    data: *const u8,
    len: usize,
) -> c_int {
    if batch.is_null() {
        set_last_error("Null SquishBatch pointer");
        return SQUISHDB_ERR_NULL;
    }
    let bytes = if len == 0 {
        Vec::new()
    } else if data.is_null() {
        set_last_error("Null blob pointer with non-zero length");
        return SQUISHDB_ERR_NULL;
    } else {
        slice::from_raw_parts(data, len).to_vec()
    };
    batch_push_val(batch, Value::Blob(bytes));
    SQUISHDB_OK
}

unsafe fn run_batch_insert(
    db: *mut SquishDb,
    table: *const c_char,
    batch: *mut SquishBatch,
    bulk: bool,
) -> i64 {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() || batch.is_null() {
            set_last_error("Null pointer passed to batch insert");
            return -1;
        }
        let tbl = match cstr_to_str(table, "table") {
            Ok(s) => s,
            Err(_) => return -1,
        };
        let b = &mut *batch;
        if !b.current.is_empty() {
            set_last_error("Incomplete row in SquishBatch (column count mismatch)");
            return -1;
        }
        let rows = std::mem::take(&mut b.rows);
        let r = if bulk {
            (*db).inner.bulk_insert(tbl, rows)
        } else {
            (*db).inner.insert_batch(tbl, rows)
        };
        match r {
            Ok(n) => n as i64,
            Err(e) => {
                error_to_code(&e);
                -1
            }
        }
    }));
    match res {
        Ok(n) => n,
        Err(_) => {
            set_last_error("Internal panic in batch insert");
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_insert_batch(
    db: *mut SquishDb,
    table: *const c_char,
    batch: *mut SquishBatch,
) -> i64 {
    run_batch_insert(db, table, batch, false)
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_bulk_insert(
    db: *mut SquishDb,
    table: *const c_char,
    batch: *mut SquishBatch,
) -> i64 {
    run_batch_insert(db, table, batch, true)
}

// ============================================================================
// Transactions, Maintenance & Introspection
// ============================================================================

unsafe fn run_db_op<F>(db: *mut SquishDb, op_name: &str, f: F) -> c_int
where
    F: FnOnce(&mut Database) -> crate::types::Result<()>,
{
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return SQUISHDB_ERR_NULL;
        }
        match f(&mut (*db).inner) {
            Ok(()) => SQUISHDB_OK,
            Err(e) => error_to_code(&e),
        }
    }));
    match res {
        Ok(code) => code,
        Err(_) => {
            set_last_error(&format!("Internal panic in {}", op_name));
            SQUISHDB_ERR_PANIC
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_begin(db: *mut SquishDb) -> c_int {
    run_db_op(db, "squishdb_begin", |d| d.begin())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_commit(db: *mut SquishDb) -> c_int {
    run_db_op(db, "squishdb_commit", |d| d.commit())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_rollback(db: *mut SquishDb) -> c_int {
    run_db_op(db, "squishdb_rollback", |d| d.rollback())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_in_transaction(db: *const SquishDb) -> c_int {
    if db.is_null() {
        return 0;
    }
    if (*db).inner.in_transaction() {
        1
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_checkpoint(db: *mut SquishDb) -> c_int {
    run_db_op(db, "squishdb_checkpoint", |d| d.checkpoint())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_vacuum(db: *mut SquishDb) -> c_int {
    run_db_op(db, "squishdb_vacuum", |d| d.vacuum())
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_backup_to(
    db: *mut SquishDb,
    dest_path: *const c_char,
) -> c_int {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return SQUISHDB_ERR_NULL;
        }
        let dest = match cstr_to_str(dest_path, "dest_path") {
            Ok(s) => s,
            Err(code) => return code,
        };
        match (*db).inner.backup_to(dest) {
            Ok(()) => SQUISHDB_OK,
            Err(e) => error_to_code(&e),
        }
    }));
    match res {
        Ok(code) => code,
        Err(_) => {
            set_last_error("Internal panic in squishdb_backup_to");
            SQUISHDB_ERR_PANIC
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_stats_report(db: *const SquishDb) -> *mut c_char {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return ptr::null_mut();
        }
        match (*db).inner.stats() {
            Ok(s) => string_to_c_alloc(s.format_report()),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(p) => p,
        Err(_) => {
            set_last_error("Internal panic in squishdb_stats_report");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_stats_json(db: *const SquishDb) -> *mut c_char {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return ptr::null_mut();
        }
        match (*db).inner.stats() {
            Ok(s) => string_to_c_alloc(s.to_json()),
            Err(e) => {
                error_to_code(&e);
                ptr::null_mut()
            }
        }
    }));
    match res {
        Ok(p) => p,
        Err(_) => {
            set_last_error("Internal panic in squishdb_stats_json");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn squishdb_table_names_json(db: *const SquishDb) -> *mut c_char {
    clear_last_error();
    let res = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            set_last_error("Null SquishDb pointer");
            return ptr::null_mut();
        }
        let names = (*db).inner.table_names();
        let mut out = String::from("[");
        for (i, n) in names.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('"');
            for ch in n.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        out.push(']');
        string_to_c_alloc(out)
    }));
    match res {
        Ok(p) => p,
        Err(_) => {
            set_last_error("Internal panic in squishdb_table_names_json");
            ptr::null_mut()
        }
    }
}

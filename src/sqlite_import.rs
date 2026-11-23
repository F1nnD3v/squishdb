use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::index::IndexKind;
use crate::pax::CompressionProfile;
use crate::types::{
    CheckConstraint, ColumnDef, ColumnType, Error, FkAction, ForeignKeyDef, Result, Value,
};
use crate::{Database, DbOptions};

#[derive(Debug, Clone)]
pub struct MigrateOptions {
    pub compression: CompressionProfile,
    pub cluster_by: HashMap<String, Vec<String>>,
    pub parallel_workers: usize,
}

impl Default for MigrateOptions {
    fn default() -> Self {
        MigrateOptions {
            compression: CompressionProfile::Balanced,
            cluster_by: HashMap::new(),
            parallel_workers: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationReport {
    pub tables_migrated: usize,
    pub indexes_migrated: usize,
    pub total_rows: u64,
    pub sqlite_bytes: u64,
    pub sqdb_bytes: u64,
}

// ============================================================================
// Pure-Rust SQLite 3 Binary File Reader (B-trees, Overflow Pages, Records, WAL)
// ============================================================================

struct SqliteReader {
    file: File,
    page_size: usize,
    usable_size: usize,
    text_encoding: u32,
    wal_pages: HashMap<u32, Vec<u8>>,
}

impl SqliteReader {
    fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut hdr = [0u8; 100];
        file.read_exact(&mut hdr).map_err(|e| {
            Error::InvalidData(format!("Cannot read SQLite header from '{}': {}", path.display(), e))
        })?;
        if &hdr[0..16] != b"SQLite format 3\0" {
            return Err(Error::InvalidData(format!(
                "'{}' is not a valid SQLite 3 database file",
                path.display()
            )));
        }
        let raw_ps = u16::from_be_bytes([hdr[16], hdr[17]]) as usize;
        let page_size = if raw_ps == 1 { 65536 } else { raw_ps };
        if page_size < 512 || !page_size.is_power_of_two() {
            return Err(Error::Corruption(format!(
                "Invalid SQLite page size: {}",
                page_size
            )));
        }
        let reserved = hdr[20] as usize;
        let usable_size = page_size.saturating_sub(reserved);
        if usable_size < 480 {
            return Err(Error::Corruption("Invalid SQLite usable page size".into()));
        }
        let text_encoding = u32::from_be_bytes([hdr[56], hdr[57], hdr[58], hdr[59]]);

        let wal_pages = Self::load_sqlite_wal(path, page_size).unwrap_or_default();

        Ok(SqliteReader {
            file,
            page_size,
            usable_size,
            text_encoding,
            wal_pages,
        })
    }

    /// SQLite WAL cumulative checksum (Section 2.2 of SQLite WAL file format specification).
    fn sqlite_wal_checksum(bytes: &[u8], init: (u32, u32), big_endian: bool) -> (u32, u32) {
        let (mut s0, mut s1) = init;
        for chunk in bytes.chunks_exact(8) {
            let x0 = if big_endian {
                u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            } else {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            };
            let x1 = if big_endian {
                u32::from_be_bytes([chunk[4], chunk[5], chunk[6], chunk[7]])
            } else {
                u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]])
            };
            s0 = s0.wrapping_add(x0).wrapping_add(s1);
            s1 = s1.wrapping_add(x1).wrapping_add(s0);
        }
        (s0, s1)
    }

    /// Reads committed, checksum-verified frames from a companion `<file>-wal` if SQLite was left in WAL mode.
    fn load_sqlite_wal(db_path: &Path, expected_page_size: usize) -> Option<HashMap<u32, Vec<u8>>> {
        let mut wal_os = db_path.as_os_str().to_os_string();
        wal_os.push("-wal");
        let mut f = File::open(Path::new(&wal_os)).ok()?;
        let mut raw = Vec::new();
        f.read_to_end(&mut raw).ok()?;
        if raw.len() < 32 {
            return None;
        }
        let magic = u32::from_be_bytes(raw[0..4].try_into().ok()?);
        let big_endian = match magic {
            0x377f_0682 => false,
            0x377f_0683 => true,
            _ => return None,
        };
        let wal_ps = u32::from_be_bytes(raw[8..12].try_into().ok()?) as usize;
        if wal_ps != expected_page_size {
            return None;
        }
        let salt1 = u32::from_be_bytes(raw[16..20].try_into().ok()?);
        let salt2 = u32::from_be_bytes(raw[20..24].try_into().ok()?);
        let hdr_c0 = u32::from_be_bytes(raw[24..28].try_into().ok()?);
        let hdr_c1 = u32::from_be_bytes(raw[28..32].try_into().ok()?);
        let calc_hdr = Self::sqlite_wal_checksum(&raw[0..24], (0, 0), big_endian);
        if calc_hdr != (hdr_c0, hdr_c1) {
            return None;
        }

        let frame_size = 24 + wal_ps;
        let mut committed = HashMap::new();
        let mut pending: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut running_csum = calc_hdr;
        let mut pos = 32usize;
        while pos + frame_size <= raw.len() {
            let pgno = u32::from_be_bytes(raw[pos..pos + 4].try_into().ok()?);
            let commit_pages = u32::from_be_bytes(raw[pos + 4..pos + 8].try_into().ok()?);
            let f_salt1 = u32::from_be_bytes(raw[pos + 8..pos + 12].try_into().ok()?);
            let f_salt2 = u32::from_be_bytes(raw[pos + 12..pos + 16].try_into().ok()?);
            let f_c0 = u32::from_be_bytes(raw[pos + 16..pos + 20].try_into().ok()?);
            let f_c1 = u32::from_be_bytes(raw[pos + 20..pos + 24].try_into().ok()?);
            if f_salt1 != salt1 || f_salt2 != salt2 || pgno == 0 {
                break;
            }
            let page_slice = &raw[pos + 24..pos + frame_size];
            let step1 = Self::sqlite_wal_checksum(&raw[pos..pos + 8], running_csum, big_endian);
            let step2 = Self::sqlite_wal_checksum(page_slice, step1, big_endian);
            if step2 != (f_c0, f_c1) {
                break;
            }
            running_csum = step2;
            pending.push((pgno, page_slice.to_vec()));
            if commit_pages > 0 {
                for (p, data) in pending.drain(..) {
                    committed.insert(p, data);
                }
            }
            pos += frame_size;
        }
        Some(committed)
    }

    fn read_page(&mut self, pgno: u32) -> Result<Vec<u8>> {
        if pgno == 0 {
            return Err(Error::Corruption("SQLite page number 0 is invalid".into()));
        }
        if let Some(p) = self.wal_pages.get(&pgno) {
            return Ok(p.clone());
        }
        let offset = ((pgno as u64) - 1) * (self.page_size as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; self.page_size];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_table_btree(&mut self, root_page: u32) -> Result<Vec<(i64, Vec<Value>)>> {
        let mut rows = Vec::new();
        let mut visited = HashSet::new();
        self.walk_table_page(root_page, &mut rows, &mut visited)?;
        Ok(rows)
    }

    fn walk_table_page(
        &mut self,
        pgno: u32,
        out: &mut Vec<(i64, Vec<Value>)>,
        visited: &mut HashSet<u32>,
    ) -> Result<()> {
        if !visited.insert(pgno) {
            return Err(Error::Corruption(format!(
                "Cycle detected in SQLite B-tree at page {}",
                pgno
            )));
        }
        let page = self.read_page(pgno)?;
        let hdr_off = if pgno == 1 { 100usize } else { 0usize };
        if hdr_off + 8 > page.len() {
            return Err(Error::Corruption("Truncated SQLite B-tree page".into()));
        }
        let page_type = page[hdr_off];
        let cell_count = u16::from_be_bytes([page[hdr_off + 3], page[hdr_off + 4]]) as usize;

        match page_type {
            0x05 => {
                // Interior table B-tree page
                if hdr_off + 12 > page.len() {
                    return Err(Error::Corruption("Truncated interior B-tree header".into()));
                }
                let right_most = u32::from_be_bytes(
                    page[hdr_off + 8..hdr_off + 12].try_into().unwrap(),
                );
                let ptr_start = hdr_off + 12;
                let mut child_pages = Vec::with_capacity(cell_count + 1);
                for i in 0..cell_count {
                    let p_off = ptr_start + i * 2;
                    let cell_off = u16::from_be_bytes([page[p_off], page[p_off + 1]]) as usize;
                    if cell_off + 4 > page.len() {
                        return Err(Error::Corruption("Invalid SQLite interior cell offset".into()));
                    }
                    let left_child = u32::from_be_bytes(
                        page[cell_off..cell_off + 4].try_into().unwrap(),
                    );
                    child_pages.push(left_child);
                }
                child_pages.push(right_most);
                for child in child_pages {
                    self.walk_table_page(child, out, visited)?;
                }
            }
            0x0D => {
                // Leaf table B-tree page
                let ptr_start = hdr_off + 8;
                let u = self.usable_size;
                let max_local = u - 35;
                let min_local = ((u - 12) * 32 / 255) - 23;

                for i in 0..cell_count {
                    let p_off = ptr_start + i * 2;
                    if p_off + 2 > page.len() {
                        return Err(Error::Corruption("Invalid SQLite cell pointer array".into()));
                    }
                    let mut pos = u16::from_be_bytes([page[p_off], page[p_off + 1]]) as usize;
                    let (payload_len_u64, n1) = read_sqlite_varint(&page, pos)?;
                    pos += n1;
                    let (rowid_u64, n2) = read_sqlite_varint(&page, pos)?;
                    pos += n2;
                    let payload_len = payload_len_u64 as usize;
                    let rowid = rowid_u64 as i64;

                    let payload = if payload_len <= max_local {
                        if pos + payload_len > page.len() {
                            return Err(Error::Corruption("SQLite leaf cell exceeds page".into()));
                        }
                        page[pos..pos + payload_len].to_vec()
                    } else {
                        let k = min_local + ((payload_len - min_local) % (u - 4));
                        let local_len = if k <= max_local { k } else { min_local };
                        if pos + local_len + 4 > page.len() {
                            return Err(Error::Corruption(
                                "SQLite overflow cell exceeds page".into(),
                            ));
                        }
                        let mut buf = Vec::with_capacity(payload_len);
                        buf.extend_from_slice(&page[pos..pos + local_len]);
                        let mut ov_pg = u32::from_be_bytes(
                            page[pos + local_len..pos + local_len + 4]
                                .try_into()
                                .unwrap(),
                        );
                        let mut ov_guard = 0usize;
                        while buf.len() < payload_len && ov_pg != 0 {
                            ov_guard += 1;
                            if ov_guard > 1_000_000 {
                                return Err(Error::Corruption(
                                    "SQLite overflow chain loop detected".into(),
                                ));
                            }
                            let ov_page = self.read_page(ov_pg)?;
                            let next_pg = u32::from_be_bytes(ov_page[0..4].try_into().unwrap());
                            let take = (payload_len - buf.len()).min(u - 4);
                            buf.extend_from_slice(&ov_page[4..4 + take]);
                            ov_pg = next_pg;
                        }
                        buf
                    };

                    let values = decode_sqlite_record(&payload, self.text_encoding)?;
                    out.push((rowid, values));
                }
            }
            0x02 | 0x0A => {
                // Index B-tree page (skip; we only walk table B-trees)
            }
            other => {
                return Err(Error::InvalidData(format!(
                    "Unsupported SQLite B-tree page type 0x{:02x} (WITHOUT ROWID tables are not supported yet)",
                    other
                )));
            }
        }
        Ok(())
    }
}

fn read_sqlite_varint(buf: &[u8], start: usize) -> Result<(u64, usize)> {
    let mut val: u64 = 0;
    for i in 0..9 {
        let idx = start + i;
        if idx >= buf.len() {
            return Err(Error::Corruption("Truncated SQLite varint".into()));
        }
        let b = buf[idx];
        if i == 8 {
            val = (val << 8) | (b as u64);
            return Ok((val, 9));
        }
        val = (val << 7) | ((b & 0x7F) as u64);
        if (b & 0x80) == 0 {
            return Ok((val, i + 1));
        }
    }
    unreachable!()
}

fn decode_sqlite_record(payload: &[u8], text_encoding: u32) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let (hdr_len_u64, mut hpos) = read_sqlite_varint(payload, 0)?;
    let hdr_len = hdr_len_u64 as usize;
    if hdr_len > payload.len() {
        return Err(Error::Corruption("SQLite record header exceeds payload".into()));
    }
    let mut serial_types = Vec::new();
    while hpos < hdr_len {
        let (st, n) = read_sqlite_varint(payload, hpos)?;
        hpos += n;
        serial_types.push(st);
    }

    let mut dpos = hdr_len;
    let mut vals = Vec::with_capacity(serial_types.len());
    for st in serial_types {
        match st {
            0 => vals.push(Value::Null),
            1 => {
                ensure_len(payload, dpos, 1)?;
                vals.push(Value::Integer(payload[dpos] as i8 as i64));
                dpos += 1;
            }
            2 => {
                ensure_len(payload, dpos, 2)?;
                let v = i16::from_be_bytes([payload[dpos], payload[dpos + 1]]) as i64;
                vals.push(Value::Integer(v));
                dpos += 2;
            }
            3 => {
                ensure_len(payload, dpos, 3)?;
                let sign = if (payload[dpos] & 0x80) != 0 { 0xFF } else { 0x00 };
                let v = i32::from_be_bytes([sign, payload[dpos], payload[dpos + 1], payload[dpos + 2]])
                    as i64;
                vals.push(Value::Integer(v));
                dpos += 3;
            }
            4 => {
                ensure_len(payload, dpos, 4)?;
                let v = i32::from_be_bytes(payload[dpos..dpos + 4].try_into().unwrap()) as i64;
                vals.push(Value::Integer(v));
                dpos += 4;
            }
            5 => {
                ensure_len(payload, dpos, 6)?;
                let sign = if (payload[dpos] & 0x80) != 0 { 0xFF } else { 0x00 };
                let v = i64::from_be_bytes([
                    sign,
                    sign,
                    payload[dpos],
                    payload[dpos + 1],
                    payload[dpos + 2],
                    payload[dpos + 3],
                    payload[dpos + 4],
                    payload[dpos + 5],
                ]);
                vals.push(Value::Integer(v));
                dpos += 6;
            }
            6 => {
                ensure_len(payload, dpos, 8)?;
                let v = i64::from_be_bytes(payload[dpos..dpos + 8].try_into().unwrap());
                vals.push(Value::Integer(v));
                dpos += 8;
            }
            7 => {
                ensure_len(payload, dpos, 8)?;
                let v = f64::from_bits(u64::from_be_bytes(
                    payload[dpos..dpos + 8].try_into().unwrap(),
                ));
                vals.push(Value::Real(v));
                dpos += 8;
            }
            8 => vals.push(Value::Integer(0)),
            9 => vals.push(Value::Integer(1)),
            10 | 11 => vals.push(Value::Null),
            n if n >= 12 && n % 2 == 0 => {
                let len = ((n - 12) / 2) as usize;
                ensure_len(payload, dpos, len)?;
                vals.push(Value::Blob(payload[dpos..dpos + len].to_vec()));
                dpos += len;
            }
            n if n >= 13 && n % 2 == 1 => {
                let len = ((n - 13) / 2) as usize;
                ensure_len(payload, dpos, len)?;
                let bytes = &payload[dpos..dpos + len];
                let s = decode_sqlite_text(bytes, text_encoding)?;
                vals.push(Value::Text(s));
                dpos += len;
            }
            _ => unreachable!(),
        }
    }
    Ok(vals)
}

fn ensure_len(buf: &[u8], pos: usize, need: usize) -> Result<()> {
    if buf.len().saturating_sub(pos) < need {
        Err(Error::Corruption("Truncated SQLite record value".into()))
    } else {
        Ok(())
    }
}

fn decode_sqlite_text(bytes: &[u8], encoding: u32) -> Result<String> {
    match encoding {
        2 => {
            let units: Vec<u16> = bytes
                .chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]))
                .collect();
            Ok(String::from_utf16_lossy(&units))
        }
        3 => {
            let units: Vec<u16> = bytes
                .chunks_exact(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect();
            Ok(String::from_utf16_lossy(&units))
        }
        _ => Ok(String::from_utf8_lossy(bytes).into_owned()),
    }
}

// ============================================================================
// SQLite DDL Parser (normalizes SQLite syntax -> SquishDB TableSchema / Index)
// ============================================================================

struct ParsedSqliteTable {
    name: String,
    root_page: u32,
    columns: Vec<ColumnDef>,
    checks: Vec<CheckConstraint>,
    foreign_keys: Vec<ForeignKeyDef>,
    rowid_alias_col: Option<usize>,
}

struct ParsedSqliteIndex {
    name: String,
    table_name: String,
    columns: Vec<String>,
    desc: Vec<bool>,
    unique: bool,
}

fn normalize_sqlite_ident_quotes(sql: &str) -> String {
    // Convert SQLite [bracketed] identifiers outside string literals into "double-quoted" ones.
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while i < bytes.len() {
        let ch = sql[i..].chars().next().unwrap();
        if in_single {
            out.push(ch);
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += ch.len_utf8();
        } else if in_double {
            out.push(ch);
            if ch == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += ch.len_utf8();
        } else if ch == '\'' {
            in_single = true;
            out.push(ch);
            i += 1;
        } else if ch == '"' {
            in_double = true;
            out.push(ch);
            i += 1;
        } else if ch == '[' {
            out.push('"');
            i += 1;
            while i < bytes.len() && bytes[i] != b']' {
                let c2 = sql[i..].chars().next().unwrap();
                if c2 == '"' {
                    out.push_str("\"\"");
                } else {
                    out.push(c2);
                }
                i += c2.len_utf8();
            }
            if i < bytes.len() && bytes[i] == b']' {
                i += 1;
            }
            out.push('"');
        } else {
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Maps SQLite type affinity rules (Section 3.1 of SQLite docs) to SquishDB ColumnType.
fn sqlite_type_affinity(type_name: &str) -> ColumnType {
    let up = type_name.to_ascii_uppercase();
    if up.contains("INT") || up.contains("BOOL") {
        ColumnType::Integer
    } else if up.contains("CHAR") || up.contains("CLOB") || up.contains("TEXT") || up.contains("STR")
    {
        ColumnType::Text
    } else if up.contains("BLOB") || up.contains("BIN") || up.contains("BYTE") {
        ColumnType::Blob
    } else if up.contains("REAL")
        || up.contains("FLOA")
        || up.contains("DOUB")
        || up.contains("NUM")
        || up.contains("DEC")
    {
        ColumnType::Real
    } else {
        // SQLite untyped or date/time columns store text cleanly
        ColumnType::Text
    }
}

fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = s[i..].chars().next().unwrap();
        if in_single {
            cur.push(ch);
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    cur.push('\'');
                    i += 2;
                    continue;
                }
                in_single = false;
            }
        } else if in_double {
            cur.push(ch);
            if ch == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    cur.push('"');
                    i += 2;
                    continue;
                }
                in_double = false;
            }
        } else {
            match ch {
                '\'' => {
                    in_single = true;
                    cur.push(ch);
                }
                '"' => {
                    in_double = true;
                    cur.push(ch);
                }
                '(' => {
                    depth += 1;
                    cur.push(ch);
                }
                ')' => {
                    depth -= 1;
                    cur.push(ch);
                }
                ',' if depth == 0 => {
                    parts.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(ch),
            }
        }
        i += ch.len_utf8();
    }
    let tail = cur.trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

fn unquote_ident(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 {
        let first = t.as_bytes()[0];
        let last = t.as_bytes()[t.len() - 1];
        if (first == b'"' && last == b'"')
            || (first == b'`' && last == b'`')
            || (first == b'\'' && last == b'\'')
        {
            let inner = &t[1..t.len() - 1];
            let q = first as char;
            return inner.replace(&format!("{}{}", q, q), &q.to_string());
        }
    }
    t.to_string()
}

fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut i = open_idx;
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
        } else if in_double {
            if b == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
        } else {
            match b {
                b'\'' => in_single = true,
                b'"' => in_double = true,
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Returns the exact SQLite rowid-alias column name (if any) for a CREATE TABLE DDL.
/// Per SQLite rules, only a single-column PRIMARY KEY whose declared type is exact `INTEGER`
/// (case-insensitive, no length modifier, and not `DESC`) aliases the rowid.
fn find_sqlite_rowid_alias_col(sql: &str) -> Option<String> {
    let open = sql.find('(')?;
    let close = find_matching_paren(sql, open)?;
    let body = &sql[open + 1..close];
    let items = split_top_level_commas(body);

    let mut exact_integer_cols: Vec<String> = Vec::new();
    let mut alias_candidate: Option<String> = None;

    for item in items {
        let trimmed = item.trim();
        let up = trimmed.to_ascii_uppercase();
        let rest = if up.starts_with("CONSTRAINT ") {
            let after = trimmed[11..].trim();
            let split_at = after
                .find(|c: char| c.is_ascii_whitespace())
                .unwrap_or(after.len());
            after[split_at..].trim()
        } else {
            trimmed
        };
        let rest_up = rest.to_ascii_uppercase();

        if rest_up.starts_with("PRIMARY KEY") {
            if let Some(p0) = rest.find('(') {
                if let Some(p1) = find_matching_paren(rest, p0) {
                    let pk_items = split_top_level_commas(&rest[p0 + 1..p1]);
                    if pk_items.len() == 1 {
                        let pk_up = pk_items[0].to_ascii_uppercase();
                        let pk_words: Vec<&str> = pk_up.split_whitespace().collect();
                        let has_desc = pk_words.iter().any(|&w| w == "DESC");
                        let col = unquote_ident(
                            pk_items[0]
                                .split_whitespace()
                                .next()
                                .unwrap_or(pk_items[0].as_str()),
                        );
                        if !has_desc
                            && exact_integer_cols
                                .iter()
                                .any(|c| c.eq_ignore_ascii_case(&col))
                        {
                            alias_candidate = Some(col);
                        }
                    }
                }
            }
            continue;
        }
        if rest_up.starts_with("UNIQUE")
            || rest_up.starts_with("CHECK")
            || rest_up.starts_with("FOREIGN KEY")
        {
            continue;
        }

        let (col_name, after_name) = split_first_ident(rest);
        if col_name.is_empty() {
            continue;
        }
        let mut remainder = after_name.trim();
        let mut type_tokens = String::new();
        let mut had_type_parens = false;
        while !remainder.is_empty() {
            let (tok, next_rem) = split_first_word(remainder);
            let tok_up = tok.to_ascii_uppercase();
            if matches!(
                tok_up.as_str(),
                "PRIMARY"
                    | "NOT"
                    | "NULL"
                    | "UNIQUE"
                    | "DEFAULT"
                    | "CHECK"
                    | "REFERENCES"
                    | "CONSTRAINT"
                    | "COLLATE"
                    | "GENERATED"
                    | "AS"
            ) {
                break;
            }
            if !type_tokens.is_empty() {
                type_tokens.push(' ');
            }
            type_tokens.push_str(tok);
            remainder = next_rem.trim();
            if remainder.starts_with('(') {
                had_type_parens = true;
                if let Some(p1) = find_matching_paren(remainder, 0) {
                    remainder = remainder[p1 + 1..].trim();
                }
            }
        }

        let is_exact_integer = !had_type_parens && type_tokens.eq_ignore_ascii_case("INTEGER");
        if is_exact_integer {
            exact_integer_cols.push(col_name.clone());
        }

        let rem_up = remainder.to_ascii_uppercase();
        if let Some(pk_pos) = rem_up.find("PRIMARY KEY") {
            let after_pk = rem_up[pk_pos + "PRIMARY KEY".len()..].trim();
            let first_after = after_pk.split_whitespace().next().unwrap_or("");
            let is_desc = first_after == "DESC";
            if is_exact_integer && !is_desc {
                alias_candidate = Some(col_name);
            }
        }
    }

    alias_candidate
}

/// Ensures only a genuine SQLite `INTEGER PRIMARY KEY` rowid alias keeps `primary_key = true`
/// on an integer column; non-alias integer PKs (such as `INT PRIMARY KEY` or `INTEGER PRIMARY KEY DESC`)
/// are stored as `unique = true` so SquishDB does not alias them to internal rowid or replace NULLs.
fn reconcile_sqlite_rowid_alias(
    columns: &mut [ColumnDef],
    alias_name: Option<&str>,
) -> Option<usize> {
    let rowid_alias_col = alias_name.and_then(|an| {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(an) && c.col_type == ColumnType::Integer)
    });
    for (i, c) in columns.iter_mut().enumerate() {
        if c.col_type == ColumnType::Integer && c.primary_key && Some(i) != rowid_alias_col {
            c.primary_key = false;
            c.unique = true;
        }
    }
    if let Some(i) = rowid_alias_col {
        columns[i].primary_key = true;
    }
    rowid_alias_col
}

fn parse_sqlite_create_table(table_name: &str, root_page: u32, raw_sql: &str) -> Result<ParsedSqliteTable> {
    let sql = normalize_sqlite_ident_quotes(raw_sql);
    let alias_name = find_sqlite_rowid_alias_col(&sql);

    // Try SquishDB's own SQL parser first for 100% fidelity on standard DDL!
    if let Ok(stmts) = crate::sql::parse_sql(&sql) {
        if let Some(crate::sql::Statement::CreateTable {
            mut columns,
            check_constraints,
            foreign_keys,
            ..
        }) = stmts.into_iter().next()
        {
            let rowid_alias_col = reconcile_sqlite_rowid_alias(&mut columns, alias_name.as_deref());
            return Ok(ParsedSqliteTable {
                name: table_name.to_string(),
                root_page,
                columns,
                checks: check_constraints,
                foreign_keys,
                rowid_alias_col,
            });
        }
    }

    // Fallback lenient SQLite DDL parser (handles COLLATE, ON CONFLICT, untyped cols, etc.)
    let open = sql
        .find('(')
        .ok_or_else(|| Error::Sql(format!("Cannot parse SQLite DDL for '{}': {}", table_name, raw_sql)))?;
    let close = find_matching_paren(&sql, open)
        .ok_or_else(|| Error::Sql(format!("Unmatched '(' in SQLite DDL for '{}'", table_name)))?;
    let body = &sql[open + 1..close];
    let items = split_top_level_commas(body);

    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut checks: Vec<CheckConstraint> = Vec::new();
    let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();

    for item in items {
        let trimmed = item.trim();
        let up = trimmed.to_ascii_uppercase();

        // Strip optional CONSTRAINT <name>
        let (cname, rest) = if up.starts_with("CONSTRAINT ") {
            let after = trimmed[11..].trim();
            let split_at = after
                .find(|c: char| c.is_ascii_whitespace())
                .unwrap_or(after.len());
            (
                Some(unquote_ident(&after[..split_at])),
                after[split_at..].trim(),
            )
        } else {
            (None, trimmed)
        };
        let rest_up = rest.to_ascii_uppercase();

        if rest_up.starts_with("PRIMARY KEY") {
            if let Some(p0) = rest.find('(') {
                if let Some(p1) = find_matching_paren(rest, p0) {
                    let pk_cols: Vec<String> = split_top_level_commas(&rest[p0 + 1..p1])
                        .into_iter()
                        .map(|c| {
                            unquote_ident(
                                c.split_whitespace().next().unwrap_or(c.as_str()),
                            )
                        })
                        .collect();
                    if pk_cols.len() == 1 {
                        if let Some(col) = columns
                            .iter_mut()
                            .find(|c| c.name.eq_ignore_ascii_case(&pk_cols[0]))
                        {
                            col.primary_key = true;
                        }
                    }
                }
            }
            continue;
        }
        if rest_up.starts_with("UNIQUE") {
            if let Some(p0) = rest.find('(') {
                if let Some(p1) = find_matching_paren(rest, p0) {
                    let ucols: Vec<String> = split_top_level_commas(&rest[p0 + 1..p1])
                        .into_iter()
                        .map(|c| unquote_ident(c.split_whitespace().next().unwrap_or(c.as_str())))
                        .collect();
                    if ucols.len() == 1 {
                        if let Some(col) = columns
                            .iter_mut()
                            .find(|c| c.name.eq_ignore_ascii_case(&ucols[0]))
                        {
                            col.unique = true;
                        }
                    }
                }
            }
            continue;
        }
        if rest_up.starts_with("CHECK") {
            if let Some(p0) = rest.find('(') {
                if let Some(p1) = find_matching_paren(rest, p0) {
                    let expr_sql = rest[p0 + 1..p1].trim().to_string();
                    if crate::sql::parse_expr_str(&expr_sql).is_ok() {
                        checks.push(CheckConstraint {
                            name: cname,
                            expr_sql,
                        });
                    }
                }
            }
            continue;
        }
        if rest_up.starts_with("FOREIGN KEY") {
            if let Some(fk) = parse_lenient_fk(cname, rest) {
                foreign_keys.push(fk);
            }
            continue;
        }

        // Column definition: <name> [type [(len)]] [constraints...]
        let (col_name, after_name) = split_first_ident(rest);
        if col_name.is_empty() {
            continue;
        }
        let mut remainder = after_name.trim();
        let mut type_tokens = String::new();
        while !remainder.is_empty() {
            let (tok, next_rem) = split_first_word(remainder);
            let tok_up = tok.to_ascii_uppercase();
            if matches!(
                tok_up.as_str(),
                "PRIMARY"
                    | "NOT"
                    | "NULL"
                    | "UNIQUE"
                    | "DEFAULT"
                    | "CHECK"
                    | "REFERENCES"
                    | "CONSTRAINT"
                    | "COLLATE"
                    | "GENERATED"
                    | "AS"
            ) {
                break;
            }
            if !type_tokens.is_empty() {
                type_tokens.push(' ');
            }
            type_tokens.push_str(tok);
            remainder = next_rem.trim();
            if remainder.starts_with('(') {
                if let Some(p1) = find_matching_paren(remainder, 0) {
                    remainder = remainder[p1 + 1..].trim();
                }
            }
        }

        let col_type = sqlite_type_affinity(&type_tokens);
        let rem_up = remainder.to_ascii_uppercase();
        let primary_key = rem_up.contains("PRIMARY KEY");
        let not_null = rem_up.contains("NOT NULL");
        let unique = rem_up.contains("UNIQUE");

        // Extract column-level CHECK (...) if present
        if let Some(chk_pos) = rem_up.find("CHECK") {
            let sub = &remainder[chk_pos..];
            if let Some(p0) = sub.find('(') {
                if let Some(p1) = find_matching_paren(sub, p0) {
                    let expr_sql = sub[p0 + 1..p1].trim().to_string();
                    if crate::sql::parse_expr_str(&expr_sql).is_ok() {
                        checks.push(CheckConstraint {
                            name: None,
                            expr_sql,
                        });
                    }
                }
            }
        }

        // Extract column-level REFERENCES tbl(col) if present
        if let Some(ref_pos) = rem_up.find("REFERENCES") {
            let sub = &remainder[ref_pos..];
            if let Some(fk) = parse_lenient_col_fk(&col_name, sub) {
                foreign_keys.push(fk);
            }
        }

        columns.push(ColumnDef {
            name: col_name,
            col_type,
            primary_key,
            not_null,
            unique,
            default_value: None,
        });
    }

    if columns.is_empty() {
        return Err(Error::Schema(format!(
            "No columns parsed from SQLite table '{}'",
            table_name
        )));
    }

    let rowid_alias_col = reconcile_sqlite_rowid_alias(&mut columns, alias_name.as_deref());

    Ok(ParsedSqliteTable {
        name: table_name.to_string(),
        root_page,
        columns,
        checks,
        foreign_keys,
        rowid_alias_col,
    })
}

fn split_first_ident(s: &str) -> (String, &str) {
    let t = s.trim();
    if t.starts_with('"') || t.starts_with('`') {
        let q = t.as_bytes()[0];
        let bytes = t.as_bytes();
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == q {
                if i + 1 < bytes.len() && bytes[i + 1] == q {
                    i += 2;
                    continue;
                }
                return (unquote_ident(&t[..=i]), &t[i + 1..]);
            }
            i += 1;
        }
        return (unquote_ident(t), "");
    }
    let end = t
        .find(|c: char| c.is_ascii_whitespace() || c == '(' || c == ',')
        .unwrap_or(t.len());
    (t[..end].to_string(), &t[end..])
}

fn split_first_word(s: &str) -> (&str, &str) {
    let t = s.trim();
    let end = t
        .find(|c: char| c.is_ascii_whitespace() || c == '(')
        .unwrap_or(t.len());
    (&t[..end], &t[end..])
}

fn parse_fk_actions(tail: &str) -> (FkAction, FkAction) {
    let up = tail.to_ascii_uppercase();
    let parse_one = |kw: &str| -> FkAction {
        if let Some(idx) = up.find(kw) {
            let after = up[idx + kw.len()..].trim();
            if after.starts_with("CASCADE") {
                FkAction::Cascade
            } else if after.starts_with("SET NULL") {
                FkAction::SetNull
            } else if after.starts_with("SET DEFAULT") {
                FkAction::SetDefault
            } else if after.starts_with("RESTRICT") {
                FkAction::Restrict
            } else {
                FkAction::NoAction
            }
        } else {
            FkAction::NoAction
        }
    };
    (parse_one("ON DELETE"), parse_one("ON UPDATE"))
}

fn parse_lenient_fk(name: Option<String>, rest: &str) -> Option<ForeignKeyDef> {
    let p0 = rest.find('(')?;
    let p1 = find_matching_paren(rest, p0)?;
    let cols: Vec<String> = split_top_level_commas(&rest[p0 + 1..p1])
        .into_iter()
        .map(|c| unquote_ident(&c))
        .collect();
    let after_cols = &rest[p1 + 1..];
    let up_after = after_cols.to_ascii_uppercase();
    let ref_pos = up_after.find("REFERENCES")?;
    let after_ref = after_cols[ref_pos + 10..].trim();
    let (ref_table, after_tbl) = split_first_ident(after_ref);
    let after_tbl = after_tbl.trim();
    let (ref_columns, actions_tail) = if after_tbl.starts_with('(') {
        let rp1 = find_matching_paren(after_tbl, 0)?;
        let rcols: Vec<String> = split_top_level_commas(&after_tbl[1..rp1])
            .into_iter()
            .map(|c| unquote_ident(&c))
            .collect();
        (rcols, &after_tbl[rp1 + 1..])
    } else {
        (Vec::new(), after_tbl)
    };
    let (on_delete, on_update) = parse_fk_actions(actions_tail);
    Some(ForeignKeyDef {
        name,
        columns: cols,
        ref_table,
        ref_columns,
        on_delete,
        on_update,
    })
}

fn parse_lenient_col_fk(col_name: &str, ref_clause: &str) -> Option<ForeignKeyDef> {
    let after_ref = ref_clause[10..].trim();
    let (ref_table, after_tbl) = split_first_ident(after_ref);
    let after_tbl = after_tbl.trim();
    let (ref_columns, actions_tail) = if after_tbl.starts_with('(') {
        let rp1 = find_matching_paren(after_tbl, 0)?;
        let rcols: Vec<String> = split_top_level_commas(&after_tbl[1..rp1])
            .into_iter()
            .map(|c| unquote_ident(&c))
            .collect();
        (rcols, &after_tbl[rp1 + 1..])
    } else {
        (Vec::new(), after_tbl)
    };
    let (on_delete, on_update) = parse_fk_actions(actions_tail);
    Some(ForeignKeyDef {
        name: None,
        columns: vec![col_name.to_string()],
        ref_table,
        ref_columns,
        on_delete,
        on_update,
    })
}

fn parse_sqlite_create_index(name: &str, table_name: &str, raw_sql: &str) -> Option<ParsedSqliteIndex> {
    let sql = normalize_sqlite_ident_quotes(raw_sql);
    let unique = sql.to_ascii_uppercase().contains("CREATE UNIQUE INDEX");
    let p0 = sql.find('(')?;
    let p1 = find_matching_paren(&sql, p0)?;
    let parts = split_top_level_commas(&sql[p0 + 1..p1]);
    let mut columns = Vec::new();
    let mut desc = Vec::new();
    for p in parts {
        let (col, rem) = split_first_ident(&p);
        if col.is_empty() {
            return None;
        }
        let is_desc = rem.to_ascii_uppercase().contains("DESC");
        columns.push(col);
        desc.push(is_desc);
    }
    if columns.is_empty() {
        return None;
    }
    Some(ParsedSqliteIndex {
        name: name.to_string(),
        table_name: table_name.to_string(),
        columns,
        desc,
        unique,
    })
}

/// Topologically orders tables iteratively (Kahn's algorithm) so parent tables referenced
/// by FOREIGN KEYs are created and populated before their child tables without recursion limits.
fn order_tables_for_fk(tables: Vec<ParsedSqliteTable>) -> Vec<ParsedSqliteTable> {
    let n = tables.len();
    let mut by_name: HashMap<String, usize> = HashMap::with_capacity(n);
    for (i, t) in tables.iter().enumerate() {
        by_name.insert(t.name.to_ascii_lowercase(), i);
    }

    let mut indegree = vec![0usize; n];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (child_idx, t) in tables.iter().enumerate() {
        let mut parents_seen: HashSet<usize> = HashSet::new();
        for fk in &t.foreign_keys {
            let ref_l = fk.ref_table.to_ascii_lowercase();
            if let Some(&parent_idx) = by_name.get(&ref_l) {
                if parent_idx != child_idx && parents_seen.insert(parent_idx) {
                    children[parent_idx].push(child_idx);
                    indegree[child_idx] += 1;
                }
            }
        }
    }

    let mut ready: std::collections::VecDeque<usize> = (0..n)
        .filter(|&i| indegree[i] == 0)
        .collect();
    let mut order = Vec::with_capacity(n);
    while let Some(u) = ready.pop_front() {
        order.push(u);
        for &v in &children[u] {
            indegree[v] -= 1;
            if indegree[v] == 0 {
                ready.push_back(v);
            }
        }
    }
    // Append any remaining cyclic tables in original order
    if order.len() < n {
        let placed: HashSet<usize> = order.iter().copied().collect();
        for i in 0..n {
            if !placed.contains(&i) {
                order.push(i);
            }
        }
    }

    let mut opts: Vec<Option<ParsedSqliteTable>> = tables.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| opts[i].take()).collect()
}

fn coerce_sqlite_value(v: Value, target: ColumnType) -> Value {
    match (v, target) {
        (Value::Null, _) => Value::Null,
        (Value::Integer(i), ColumnType::Integer) => Value::Integer(i),
        (Value::Real(f), ColumnType::Real) => Value::Real(f),
        (Value::Integer(i), ColumnType::Real) => Value::Real(i as f64),
        (Value::Text(s), ColumnType::Text) => Value::Text(s),
        (Value::Blob(b), ColumnType::Blob) => Value::Blob(b),
        (Value::Text(s), ColumnType::Blob) => Value::Blob(s.into_bytes()),
        (Value::Integer(i), ColumnType::Text) => Value::Text(i.to_string()),
        (Value::Real(f), ColumnType::Text) => Value::Text(f.to_string()),
        (Value::Real(f), ColumnType::Integer) => Value::Integer(f as i64),
        (Value::Text(s), ColumnType::Integer) => {
            s.trim().parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
        }
        (Value::Text(s), ColumnType::Real) => {
            s.trim().parse::<f64>().map(Value::Real).unwrap_or(Value::Null)
        }
        (Value::Blob(b), ColumnType::Text) => {
            Value::Text(String::from_utf8_lossy(&b).into_owned())
        }
        (other, _) => other,
    }
}

/// Directly migrates a SQLite 3 database file (`.sqlite` / `.db`) into a compressed SquishDB
/// `.sqdb` file in pure Rust without linking `libsqlite3`.
pub fn migrate_sqlite<P1: AsRef<Path>, P2: AsRef<Path>>(
    sqlite_path: P1,
    sqdb_path: P2,
    migrate_opts: MigrateOptions,
) -> Result<MigrationReport> {
    let sqlite_path = sqlite_path.as_ref();
    let sqdb_path = sqdb_path.as_ref();
    let sqlite_bytes = std::fs::metadata(sqlite_path)?.len();

    let mut reader = SqliteReader::open(sqlite_path)?;
    // Page 1 is always sqlite_schema: (type, name, tbl_name, rootpage, sql)
    let master_rows = reader.read_table_btree(1)?;

    let mut parsed_tables = Vec::new();
    let mut parsed_indexes = Vec::new();

    for (_, vals) in master_rows {
        if vals.len() < 5 {
            continue;
        }
        let obj_type = match &vals[0] {
            Value::Text(s) => s.as_str(),
            _ => continue,
        };
        let obj_name = match &vals[1] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let tbl_name = match &vals[2] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let root_page = match &vals[3] {
            Value::Integer(p) if *p > 0 => *p as u32,
            _ => 0,
        };
        let ddl_sql = match &vals[4] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };

        if obj_name.starts_with("sqlite_") || tbl_name.starts_with("sqlite_") {
            continue;
        }

        if obj_type.eq_ignore_ascii_case("table") && root_page > 0 {
            parsed_tables.push(parse_sqlite_create_table(&tbl_name, root_page, &ddl_sql)?);
        } else if obj_type.eq_ignore_ascii_case("index") {
            if let Some(idx) = parse_sqlite_create_index(&obj_name, &tbl_name, &ddl_sql) {
                parsed_indexes.push(idx);
            }
        }
    }

    let ordered_tables = order_tables_for_fk(parsed_tables);

    // Overwrite destination cleanly if it already exists
    let _ = std::fs::remove_file(sqdb_path);
    let mut db_opts = DbOptions::default();
    db_opts.compression = migrate_opts.compression;
    db_opts.parallel_workers = migrate_opts.parallel_workers;
    let mut db = Database::open_with_options(sqdb_path, db_opts)?;
    // Disable FK enforcement during bulk load if source SQLite had orphaned rows,
    // while still preserving the FK metadata in the schema!
    db.set_foreign_keys_enabled(false);

    let mut total_rows = 0u64;
    let has_any_cluster = !migrate_opts.cluster_by.is_empty();

    for tbl in &ordered_tables {
        let cluster_cols = migrate_opts
            .cluster_by
            .get(&tbl.name.to_ascii_lowercase())
            .or_else(|| migrate_opts.cluster_by.get(&tbl.name))
            .cloned()
            .unwrap_or_default();

        db.create_table_full(
            &tbl.name,
            tbl.columns.clone(),
            cluster_cols,
            tbl.checks.clone(),
            tbl.foreign_keys.clone(),
        )?;

        let int_pk_col = tbl.rowid_alias_col;

        let raw_rows = reader.read_table_btree(tbl.root_page)?;
        let num_cols = tbl.columns.len();
        let mut batch: Vec<Vec<Value>> = Vec::with_capacity(4096);

        for (rowid, mut vals) in raw_rows {
            if vals.len() < num_cols {
                vals.resize(num_cols, Value::Null);
            } else if vals.len() > num_cols {
                vals.truncate(num_cols);
            }
            if let Some(pk_i) = int_pk_col {
                if vals[pk_i].is_null() {
                    vals[pk_i] = Value::Integer(rowid);
                }
            }
            for (c_i, col_def) in tbl.columns.iter().enumerate() {
                let v = std::mem::replace(&mut vals[c_i], Value::Null);
                vals[c_i] = coerce_sqlite_value(v, col_def.col_type);
            }
            batch.push(vals);
            total_rows += 1;
            if batch.len() >= 4096 {
                db.bulk_insert(&tbl.name, std::mem::take(&mut batch))?;
            }
        }
        if !batch.is_empty() {
            db.bulk_insert(&tbl.name, batch)?;
        }
    }

    if has_any_cluster {
        db.vacuum()?;
    } else {
        db.checkpoint()?;
    }

    let mut indexes_migrated = 0usize;
    for idx in &parsed_indexes {
        if db
            .create_index_full(
                &idx.name,
                &idx.table_name,
                idx.columns.clone(),
                idx.desc.clone(),
                IndexKind::BTree,
                idx.unique,
                true,
            )
            .is_ok()
        {
            indexes_migrated += 1;
        }
    }

    db.checkpoint()?;
    let sqdb_bytes = db.stats()?.db_file_bytes;
    drop(db);

    Ok(MigrationReport {
        tables_migrated: ordered_tables.len(),
        indexes_migrated,
        total_rows,
        sqlite_bytes,
        sqdb_bytes,
    })
}

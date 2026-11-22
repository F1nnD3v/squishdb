//! Persistent newline-delimited JSON-RPC over stdin/stdout (`squishdb rpc`).
//!
//! Zero external crate dependencies. Allows any runtime (Node.js without native
//! addons, PHP, Ruby, Swift, Dart, shell pipelines, etc.) to keep an open
//! `Database` session with parameterized queries, transactions, blobs, and bulk
//! inserts at sub-millisecond per-query overhead.

use std::collections::HashMap;
use std::io::{self, BufRead, BufWriter, Write};

use crate::types::{Error, Result, Value};
use crate::{Database, QueryResult};

const JS_MAX_SAFE_INT: i64 = 9_007_199_254_740_991;

// ============================================================================
// Minimal zero-dependency JSON parser & encoder + Base64
// ============================================================================

#[derive(Debug, Clone)]
enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(HashMap<String, Json>),
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(s: &'a str) -> Self {
        JsonParser {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn parse_value(&mut self) -> Result<Json> {
        self.skip_ws();
        let b = self
            .peek()
            .ok_or_else(|| Error::InvalidData("Unexpected EOF in JSON".into()))?;
        match b {
            b'n' => {
                self.expect_bytes(b"null")?;
                Ok(Json::Null)
            }
            b't' => {
                self.expect_bytes(b"true")?;
                Ok(Json::Bool(true))
            }
            b'f' => {
                self.expect_bytes(b"false")?;
                Ok(Json::Bool(false))
            }
            b'"' => Ok(Json::Str(self.parse_string()?)),
            b'[' => self.parse_array(),
            b'{' => self.parse_object(),
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => Err(Error::InvalidData(format!(
                "Unexpected JSON byte '{}' at {}",
                b as char, self.pos
            ))),
        }
    }

    fn expect_bytes(&mut self, lit: &[u8]) -> Result<()> {
        if self.bytes.len().saturating_sub(self.pos) < lit.len()
            || &self.bytes[self.pos..self.pos + lit.len()] != lit
        {
            return Err(Error::InvalidData("Invalid JSON literal".into()));
        }
        self.pos += lit.len();
        Ok(())
    }

    fn parse_string(&mut self) -> Result<String> {
        if self.peek() != Some(b'"') {
            return Err(Error::InvalidData("Expected '\"'".into()));
        }
        self.pos += 1;
        let mut out = String::new();
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            self.pos += 1;
            if b == b'"' {
                return Ok(out);
            }
            if b == b'\\' {
                if self.pos >= self.bytes.len() {
                    return Err(Error::InvalidData("Unterminated escape in JSON string".into()));
                }
                let esc = self.bytes[self.pos];
                self.pos += 1;
                match esc {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let code = self.parse_hex4()?;
                        if (0xD800..=0xDBFF).contains(&code) {
                            // UTF-16 surrogate pair
                            if self.pos + 2 <= self.bytes.len()
                                && self.bytes[self.pos] == b'\\'
                                && self.bytes[self.pos + 1] == b'u'
                            {
                                self.pos += 2;
                                let low = self.parse_hex4()?;
                                if (0xDC00..=0xDFFF).contains(&low) {
                                    let cp = 0x10000
                                        + (((code as u32 - 0xD800) << 10) | (low as u32 - 0xDC00));
                                    if let Some(ch) = char::from_u32(cp) {
                                        out.push(ch);
                                        continue;
                                    }
                                }
                            }
                            return Err(Error::InvalidData("Invalid Unicode surrogate pair".into()));
                        }
                        let ch = char::from_u32(code as u32)
                            .ok_or_else(|| Error::InvalidData("Invalid Unicode scalar".into()))?;
                        out.push(ch);
                    }
                    _ => return Err(Error::InvalidData("Invalid JSON escape".into())),
                }
            } else {
                // Step back 1 to decode full UTF-8 char
                self.pos -= 1;
                let s = std::str::from_utf8(&self.bytes[self.pos..])
                    .map_err(|_| Error::InvalidData("Invalid UTF-8 in JSON string".into()))?;
                let ch = s.chars().next().unwrap();
                out.push(ch);
                self.pos += ch.len_utf8();
            }
        }
        Err(Error::InvalidData("Unterminated JSON string".into()))
    }

    fn parse_hex4(&mut self) -> Result<u16> {
        if self.bytes.len().saturating_sub(self.pos) < 4 {
            return Err(Error::InvalidData("Incomplete \\uXXXX escape".into()));
        }
        let hex = std::str::from_utf8(&self.bytes[self.pos..self.pos + 4])
            .map_err(|_| Error::InvalidData("Invalid hex escape".into()))?;
        self.pos += 4;
        u16::from_str_radix(hex, 16)
            .map_err(|_| Error::InvalidData("Invalid hex digits in \\uXXXX".into()))
    }

    fn parse_number(&mut self) -> Result<Json> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(b'0'..=b'9') = self.peek() {
            self.pos += 1;
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
        }
        let raw = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| Error::InvalidData("Invalid JSON number".into()))?;
        if is_float {
            let f: f64 = raw
                .parse()
                .map_err(|_| Error::InvalidData(format!("Invalid JSON float: {}", raw)))?;
            Ok(Json::Float(f))
        } else if let Ok(i) = raw.parse::<i64>() {
            Ok(Json::Int(i))
        } else {
            let f: f64 = raw
                .parse()
                .map_err(|_| Error::InvalidData(format!("Invalid JSON number: {}", raw)))?;
            Ok(Json::Float(f))
        }
    }

    fn parse_array(&mut self) -> Result<Json> {
        self.pos += 1; // '['
        let mut arr = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Arr(arr));
        }
        loop {
            arr.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Json::Arr(arr));
                }
                _ => return Err(Error::InvalidData("Expected ',' or ']' in JSON array".into())),
            }
        }
    }

    fn parse_object(&mut self) -> Result<Json> {
        self.pos += 1; // '{'
        let mut map = HashMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Obj(map));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err(Error::InvalidData("Expected ':' in JSON object".into()));
            }
            self.pos += 1;
            let val = self.parse_value()?;
            map.insert(key, val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Json::Obj(map));
                }
                _ => return Err(Error::InvalidData("Expected ',' or '}' in JSON object".into())),
            }
        }
    }
}

fn json_escape(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

const B64_TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(((input.len() + 2) / 3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_TABLE[((n >> 18) & 63) as usize] as char);
        out.push(B64_TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_TABLE[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_TABLE[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn b64_val(b: u8) -> Option<u32> {
    match b {
        b'A'..=b'Z' => Some((b - b'A') as u32),
        b'a'..=b'z' => Some((b - b'a' + 26) as u32),
        b'0'..=b'9' => Some((b - b'0' + 52) as u32),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if bytes.len() % 4 != 0 {
        return Err(Error::InvalidData("Invalid base64 length".into()));
    }
    let total_chunks = bytes.len() / 4;
    let mut out = Vec::with_capacity(total_chunks * 3);
    for (idx, chunk) in bytes.chunks(4).enumerate() {
        let is_last = idx + 1 == total_chunks;
        let v0 = b64_val(chunk[0]).ok_or_else(|| Error::InvalidData("Invalid base64".into()))?;
        let v1 = b64_val(chunk[1]).ok_or_else(|| Error::InvalidData("Invalid base64".into()))?;
        out.push(((v0 << 2) | (v1 >> 4)) as u8);
        if chunk[2] == b'=' {
            if !is_last || chunk[3] != b'=' {
                return Err(Error::InvalidData("Invalid base64 padding".into()));
            }
        } else {
            let v2 =
                b64_val(chunk[2]).ok_or_else(|| Error::InvalidData("Invalid base64".into()))?;
            out.push((((v1 & 0xF) << 4) | (v2 >> 2)) as u8);
            if chunk[3] == b'=' {
                if !is_last {
                    return Err(Error::InvalidData("Invalid base64 padding".into()));
                }
            } else {
                let v3 = b64_val(chunk[3])
                    .ok_or_else(|| Error::InvalidData("Invalid base64".into()))?;
                out.push((((v2 & 0x3) << 6) | v3) as u8);
            }
        }
    }
    Ok(out)
}

fn json_to_value(j: &Json) -> Result<Value> {
    match j {
        Json::Null => Ok(Value::Null),
        Json::Bool(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
        Json::Int(i) => Ok(Value::Integer(*i)),
        Json::Float(f) => Ok(Value::Real(*f)),
        Json::Str(s) => Ok(Value::Text(s.clone())),
        Json::Obj(map) => {
            if let Some(Json::Str(b64)) = map.get("$blob") {
                return Ok(Value::Blob(base64_decode(b64)?));
            }
            if let Some(Json::Str(int_s)) = map.get("$int") {
                let i = int_s
                    .parse::<i64>()
                    .map_err(|_| Error::InvalidData(format!("Invalid $int: {}", int_s)))?;
                return Ok(Value::Integer(i));
            }
            if let Some(v) = map.get("$real") {
                match v {
                    Json::Str(s) => match s.as_str() {
                        "NaN" => return Ok(Value::Real(f64::NAN)),
                        "Infinity" => return Ok(Value::Real(f64::INFINITY)),
                        "-Infinity" => return Ok(Value::Real(f64::NEG_INFINITY)),
                        other => {
                            let f: f64 = other.parse().map_err(|_| {
                                Error::InvalidData(format!("Invalid $real: {}", other))
                            })?;
                            return Ok(Value::Real(f));
                        }
                    },
                    Json::Float(f) => return Ok(Value::Real(*f)),
                    Json::Int(i) => return Ok(Value::Real(*i as f64)),
                    _ => {}
                }
            }
            Err(Error::InvalidData(
                "Unsupported JSON object as SQL value (expected $blob, $int, or $real)".into(),
            ))
        }
        Json::Arr(_) => Err(Error::InvalidData(
            "JSON array cannot be bound directly as a single SQL value".into(),
        )),
    }
}

fn write_value_json(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Integer(i) => {
            if i.unsigned_abs() <= (JS_MAX_SAFE_INT as u64) {
                out.push_str(&i.to_string());
            } else {
                out.push_str("{\"$int\":\"");
                out.push_str(&i.to_string());
                out.push_str("\"}");
            }
        }
        Value::Real(f) => {
            if f.is_nan() {
                out.push_str("{\"$real\":\"NaN\"}");
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    out.push_str("{\"$real\":\"Infinity\"}");
                } else {
                    out.push_str("{\"$real\":\"-Infinity\"}");
                }
            } else {
                // Format cleanly so floats always preserve decimal/IEEE-754 representation
                let s = format!("{:?}", f);
                out.push_str(&s);
            }
        }
        Value::Text(s) => json_escape(s, out),
        Value::Blob(b) => {
            out.push_str("{\"$blob\":\"");
            out.push_str(&base64_encode(b));
            out.push_str("\"}");
        }
    }
}

fn write_results_json(results: &[QueryResult], out: &mut String) {
    out.push('[');
    for (i, res) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match res {
            QueryResult::Rows { columns, rows } => {
                out.push_str("{\"kind\":\"rows\",\"columns\":[");
                for (ci, c) in columns.iter().enumerate() {
                    if ci > 0 {
                        out.push(',');
                    }
                    json_escape(c, out);
                }
                out.push_str("],\"rows\":[");
                for (ri, r) in rows.iter().enumerate() {
                    if ri > 0 {
                        out.push(',');
                    }
                    out.push('[');
                    for (vi, v) in r.iter().enumerate() {
                        if vi > 0 {
                            out.push(',');
                        }
                        write_value_json(v, out);
                    }
                    out.push(']');
                }
                out.push_str("]}");
            }
            QueryResult::Affected(n) => {
                out.push_str(&format!("{{\"kind\":\"affected\",\"affected\":{}}}", n));
            }
            QueryResult::Message(m) => {
                out.push_str("{\"kind\":\"message\",\"message\":");
                json_escape(m, out);
                out.push('}');
            }
        }
    }
    out.push(']');
}

fn error_code_str(err: &Error) -> &'static str {
    match err {
        Error::Io(_) => "Io",
        Error::Corruption(_) => "Corruption",
        Error::Sql(_) => "Sql",
        Error::Schema(_) => "Schema",
        Error::InvalidData(_) => "InvalidData",
        Error::Busy(_) => "Busy",
    }
}

fn format_id(id: Option<&Json>) -> String {
    match id {
        Some(Json::Int(i)) => i.to_string(),
        Some(Json::Str(s)) => {
            let mut o = String::new();
            json_escape(s, &mut o);
            o
        }
        _ => "null".to_string(),
    }
}

fn handle_request(db: &mut Database, map: &HashMap<String, Json>) -> (bool, Result<String>) {
    let method = match map.get("method") {
        Some(Json::Str(s)) => s.as_str(),
        _ => return (false, Err(Error::InvalidData("Missing 'method' field".into()))),
    };

    match method {
        "ping" => {
            let stats_ro = db.stats().map(|s| s.read_only).unwrap_or(false);
            (
                false,
                Ok(format!(
                    "{{\"version\":\"{}\",\"read_only\":{}}}",
                    env!("CARGO_PKG_VERSION"),
                    stats_ro
                )),
            )
        }
        "execute" => {
            let sql = match map.get("sql") {
                Some(Json::Str(s)) => s.as_str(),
                _ => return (false, Err(Error::InvalidData("Missing 'sql' string".into()))),
            };
            let params: Vec<Value> = match map.get("params") {
                None | Some(Json::Null) => Vec::new(),
                Some(Json::Arr(arr)) => {
                    let mut v = Vec::with_capacity(arr.len());
                    for item in arr {
                        match json_to_value(item) {
                            Ok(val) => v.push(val),
                            Err(e) => return (false, Err(e)),
                        }
                    }
                    v
                }
                _ => {
                    return (
                        false,
                        Err(Error::InvalidData("'params' must be an array".into())),
                    )
                }
            };
            let res = if params.is_empty() {
                db.execute(sql)
            } else {
                db.execute_params(sql, &params)
            };
            (
                false,
                res.map(|r| {
                    let mut out = String::new();
                    write_results_json(&r, &mut out);
                    out
                }),
            )
        }
        "begin" => (false, db.begin().map(|_| "true".to_string())),
        "commit" => (false, db.commit().map(|_| "true".to_string())),
        "rollback" => (false, db.rollback().map(|_| "true".to_string())),
        "in_transaction" => (
            false,
            Ok(if db.in_transaction() { "true" } else { "false" }.to_string()),
        ),
        "checkpoint" => (false, db.checkpoint().map(|_| "true".to_string())),
        "vacuum" => (false, db.vacuum().map(|_| "true".to_string())),
        "backup_to" => {
            let path = match map.get("path") {
                Some(Json::Str(s)) => s.as_str(),
                _ => return (false, Err(Error::InvalidData("Missing 'path' string".into()))),
            };
            (false, db.backup_to(path).map(|_| "true".to_string()))
        }
        "stats" => (false, db.stats().map(|s| s.to_json())),
        "tables" => {
            let names = db.table_names();
            let mut out = String::from("[");
            for (i, n) in names.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_escape(n, &mut out);
            }
            out.push(']');
            (false, Ok(out))
        }
        "insert_batch" | "bulk_insert" => {
            let table = match map.get("table") {
                Some(Json::Str(s)) => s.as_str(),
                _ => return (false, Err(Error::InvalidData("Missing 'table' string".into()))),
            };
            let rows_json = match map.get("rows") {
                Some(Json::Arr(a)) => a,
                _ => return (false, Err(Error::InvalidData("Missing 'rows' array".into()))),
            };
            let mut rows = Vec::with_capacity(rows_json.len());
            for r in rows_json {
                match r {
                    Json::Arr(cols) => {
                        let mut row = Vec::with_capacity(cols.len());
                        for c in cols {
                            match json_to_value(c) {
                                Ok(v) => row.push(v),
                                Err(e) => return (false, Err(e)),
                            }
                        }
                        rows.push(row);
                    }
                    _ => {
                        return (
                            false,
                            Err(Error::InvalidData("Each item in 'rows' must be an array".into())),
                        )
                    }
                }
            }
            let r = if method == "bulk_insert" {
                db.bulk_insert(table, rows)
            } else {
                db.insert_batch(table, rows)
            };
            (false, r.map(|n| n.to_string()))
        }
        "close" => (true, Ok("true".to_string())),
        other => (
            false,
            Err(Error::InvalidData(format!("Unknown RPC method: '{}'", other))),
        ),
    }
}

pub fn run_rpc_loop(mut db: Database) -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    for line_res in stdin.lock().lines() {
        let line = line_res?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parser = JsonParser::new(trimmed);
        let req = match parser.parse_value().and_then(|val| {
            parser.skip_ws();
            if parser.pos != parser.bytes.len() {
                Err(Error::InvalidData("Trailing characters after JSON object".into()))
            } else {
                Ok(val)
            }
        }) {
            Ok(Json::Obj(m)) => m,
            Ok(_) => {
                writeln!(
                    writer,
                    "{{\"id\":null,\"ok\":false,\"error\":{{\"code\":\"InvalidData\",\"message\":\"Request must be a JSON object\"}}}}"
                )?;
                writer.flush()?;
                continue;
            }
            Err(e) => {
                let mut msg = String::new();
                json_escape(&e.to_string(), &mut msg);
                writeln!(
                    writer,
                    "{{\"id\":null,\"ok\":false,\"error\":{{\"code\":\"InvalidData\",\"message\":{}}}}}",
                    msg
                )?;
                writer.flush()?;
                continue;
            }
        };

        let id_str = format_id(req.get("id"));
        let (should_close, outcome) = handle_request(&mut db, &req);
        match outcome {
            Ok(result_json) => {
                writeln!(
                    writer,
                    "{{\"id\":{},\"ok\":true,\"result\":{}}}",
                    id_str, result_json
                )?;
            }
            Err(e) => {
                let mut msg = String::new();
                json_escape(&e.to_string(), &mut msg);
                writeln!(
                    writer,
                    "{{\"id\":{},\"ok\":false,\"error\":{{\"code\":\"{}\",\"message\":{}}}}}",
                    id_str,
                    error_code_str(&e),
                    msg
                )?;
            }
        }
        writer.flush()?;
        if should_close {
            break;
        }
    }
    Ok(())
}

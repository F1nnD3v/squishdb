use std::cmp::Ordering;
use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Corruption(String),
    Sql(String),
    Schema(String),
    InvalidData(String),
    Busy(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Corruption(m) => write!(f, "Database corruption: {}", m),
            Error::Sql(m) => write!(f, "SQL error: {}", m),
            Error::Schema(m) => write!(f, "Schema error: {}", m),
            Error::InvalidData(m) => write!(f, "Invalid data: {}", m),
            Error::Busy(m) => write!(f, "Database busy: {}", m),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
}

impl ColumnType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(ColumnType::Integer),
            2 => Ok(ColumnType::Real),
            3 => Ok(ColumnType::Text),
            4 => Ok(ColumnType::Blob),
            _ => Err(Error::Corruption(format!("Unknown column type tag: {}", v))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "REAL",
            ColumnType::Text => "TEXT",
            ColumnType::Blob => "BLOB",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub primary_key: bool,
    pub not_null: bool,
    pub unique: bool,
    pub default_value: Option<Value>,
}

impl PartialEq for ColumnDef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.col_type == other.col_type
            && self.primary_key == other.primary_key
            && self.not_null == other.not_null
            && self.unique == other.unique
            && match (&self.default_value, &other.default_value) {
                (None, None) => true,
                (Some(a), Some(b)) => a.exact_eq(b),
                _ => false,
            }
    }
}

impl Eq for ColumnDef {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FkAction {
    NoAction = 0,
    Restrict = 1,
    Cascade = 2,
    SetNull = 3,
    SetDefault = 4,
}

impl FkAction {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(FkAction::NoAction),
            1 => Ok(FkAction::Restrict),
            2 => Ok(FkAction::Cascade),
            3 => Ok(FkAction::SetNull),
            4 => Ok(FkAction::SetDefault),
            _ => Err(Error::Corruption(format!("Unknown FK action tag: {}", v))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expr_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub next_rowid: i64,
    /// Optional sort-key columns (`CLUSTER BY (col1, col2)`) used to order rows inside
    /// PAX pages and during VACUUM for maximum RLE/Delta/Dictionary compression & zone-map pruning.
    pub cluster_by: Vec<String>,
    /// Table-level and column-level CHECK constraints stored as canonical SQL expressions.
    pub check_constraints: Vec<CheckConstraint>,
    /// Foreign key constraints referencing parent tables.
    pub foreign_keys: Vec<ForeignKeyDef>,
}

impl TableSchema {
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Returns index of an `INTEGER PRIMARY KEY` column (SQLite rowid alias), if any.
    pub fn integer_pk_col(&self) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.primary_key && c.col_type == ColumnType::Integer)
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Exact lossless equality (uses bit-level IEEE-754 comparison for Real).
    pub fn exact_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            _ => false,
        }
    }

    /// Exact lossless `i64` vs `f64` comparison without precision-losing `i64 as f64` casts.
    fn cmp_i64_f64(i: i64, f: f64) -> Ordering {
        if f.is_nan() {
            return Ordering::Less; // canonical: NaN sorts after finite numbers (matches total_cmp)
        }
        if f.is_infinite() {
            return if f.is_sign_positive() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        // 2^63 = 9223372036854775808.0 is exact in f64; every i64 is strictly < 2^63
        if f >= 9223372036854775808.0 {
            return Ordering::Less;
        }
        // -2^63 = -9223372036854775808.0 is exact in f64
        if f < -9223372036854775808.0 {
            return Ordering::Greater;
        }
        let trunc = f.trunc();
        let fi = trunc as i64;
        match i.cmp(&fi) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let frac = f - trunc;
                if frac > 0.0 {
                    Ordering::Less
                } else if frac < 0.0 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }

    #[inline]
    fn canonical_f64(f: f64) -> f64 {
        if f == 0.0 {
            0.0 // canonicalize -0.0 -> +0.0 for SQL comparison & grouping
        } else if f.is_nan() {
            f64::NAN
        } else {
            f
        }
    }

    /// Transitive SQL comparison ordering (NULLs sort first; exact lossless numeric cross-comparison;
    /// `-0.0` compares equal to `0.0` and `0`).
    pub fn sql_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => {
                Self::canonical_f64(*a).total_cmp(&Self::canonical_f64(*b))
            }
            (Value::Integer(a), Value::Real(b)) => Self::cmp_i64_f64(*a, *b),
            (Value::Real(a), Value::Integer(b)) => Self::cmp_i64_f64(*b, *a).reverse(),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (a, b) => a.type_rank().cmp(&b.type_rank()),
        }
    }

    fn type_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 1,
            Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }

    pub fn coerce_for_column(&self, col_type: ColumnType) -> Result<Value> {
        match (self, col_type) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Integer(v), ColumnType::Integer) => Ok(Value::Integer(*v)),
            (Value::Real(v), ColumnType::Real) => Ok(Value::Real(*v)),
            (Value::Integer(v), ColumnType::Real) => Ok(Value::Real(*v as f64)),
            (Value::Text(s), ColumnType::Text) => Ok(Value::Text(s.clone())),
            (Value::Blob(b), ColumnType::Blob) => Ok(Value::Blob(b.clone())),
            (Value::Text(s), ColumnType::Blob) => Ok(Value::Blob(s.as_bytes().to_vec())),
            (v, expected) => Err(Error::InvalidData(format!(
                "Type mismatch: cannot store {:?} into {} column",
                v,
                expected.as_str()
            ))),
        }
    }

    pub fn write_binary(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(0),
            Value::Integer(v) => {
                out.push(1);
                write_zigzag_varint(*v, out);
            }
            Value::Real(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            Value::Text(s) => {
                out.push(3);
                write_uvarint(s.len() as u64, out);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Blob(b) => {
                out.push(4);
                write_uvarint(b.len() as u64, out);
                out.extend_from_slice(b);
            }
        }
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Value> {
        let tag = read_u8(buf, pos)?;
        match tag {
            0 => Ok(Value::Null),
            1 => Ok(Value::Integer(read_zigzag_varint(buf, pos)?)),
            2 => {
                let bytes = read_exact::<8>(buf, pos)?;
                Ok(Value::Real(f64::from_bits(u64::from_le_bytes(bytes))))
            }
            3 => {
                let len = read_uvarint(buf, pos)? as usize;
                let bytes = read_slice(buf, pos, len)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| Error::Corruption(format!("Invalid UTF-8: {}", e)))?;
                Ok(Value::Text(s.to_string()))
            }
            4 => {
                let len = read_uvarint(buf, pos)? as usize;
                let bytes = read_slice(buf, pos, len)?;
                Ok(Value::Blob(bytes.to_vec()))
            }
            _ => Err(Error::Corruption(format!("Invalid value tag: {}", tag))),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Real(v) => write!(f, "{}", v),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{:02X}", byte)?;
                }
                write!(f, "'")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub rowid: i64,
    pub values: Vec<Value>,
}

// ============================================================================
// Low-level varint, zigzag, and slice reader utilities
// ============================================================================

#[inline]
pub fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
pub fn zigzag_decode(z: u64) -> i64 {
    ((z >> 1) as i64) ^ (-((z & 1) as i64))
}

#[inline]
pub fn write_uvarint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
pub fn write_zigzag_varint(v: i64, out: &mut Vec<u8>) {
    write_uvarint(zigzag_encode(v), out);
}

#[inline]
pub fn read_uvarint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut res: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= buf.len() {
            return Err(Error::Corruption("Unexpected EOF reading varint".into()));
        }
        let b = buf[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err(Error::Corruption("Varint overflow".into()));
        }
        res |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok(res);
        }
        shift += 7;
    }
}

#[inline]
pub fn read_zigzag_varint(buf: &[u8], pos: &mut usize) -> Result<i64> {
    Ok(zigzag_decode(read_uvarint(buf, pos)?))
}

#[inline]
pub fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8> {
    if *pos >= buf.len() {
        return Err(Error::Corruption("Unexpected EOF reading u8".into()));
    }
    let b = buf[*pos];
    *pos += 1;
    Ok(b)
}

#[inline]
pub fn read_exact<const N: usize>(buf: &[u8], pos: &mut usize) -> Result<[u8; N]> {
    if buf.len().saturating_sub(*pos) < N {
        return Err(Error::Corruption(format!(
            "Unexpected EOF reading {} bytes",
            N
        )));
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&buf[*pos..*pos + N]);
    *pos += N;
    Ok(arr)
}

#[inline]
pub fn read_slice<'a>(buf: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    if buf.len().saturating_sub(*pos) < len {
        return Err(Error::Corruption(format!(
            "Unexpected EOF reading slice of {} bytes",
            len
        )));
    }
    let s = &buf[*pos..*pos + len];
    *pos += len;
    Ok(s)
}

pub fn write_str(s: &str, out: &mut Vec<u8>) {
    write_uvarint(s.len() as u64, out);
    out.extend_from_slice(s.as_bytes());
}

pub fn read_str(buf: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_uvarint(buf, pos)? as usize;
    let bytes = read_slice(buf, pos, len)?;
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| Error::Corruption(format!("Invalid UTF-8 string: {}", e)))
}

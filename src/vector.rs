use std::collections::HashMap;

use crate::index::hash_value;
use crate::types::{ColumnType, Value};

/// Typed columnar array for a single column inside a decoded PAX page.
/// Supports dictionary-native `DictText` for zero-String-allocation filtering & flat-array GROUP BY!
#[derive(Debug, Clone)]
pub enum ColumnVector {
    Int64 {
        data: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Float64 {
        data: Vec<f64>,
        valid: Option<Vec<bool>>,
    },
    /// Native dictionary-encoded text vector: ZERO per-row String heap allocations!
    DictText {
        dict: Vec<String>,
        codes: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Text {
        data: Vec<String>,
        valid: Option<Vec<bool>>,
    },
    Blob {
        data: Vec<Vec<u8>>,
        valid: Option<Vec<bool>>,
    },
}

impl ColumnVector {
    pub fn from_values(col_type: ColumnType, values: Vec<Value>) -> Self {
        let n = values.len();
        let has_null = values.iter().any(|v| v.is_null());
        match col_type {
            ColumnType::Integer => {
                let mut data = Vec::with_capacity(n);
                let mut valid = if has_null {
                    Some(Vec::with_capacity(n))
                } else {
                    None
                };
                for v in values {
                    match v {
                        Value::Integer(i) => {
                            data.push(i);
                            if let Some(ref mut bm) = valid {
                                bm.push(true);
                            }
                        }
                        _ => {
                            data.push(0);
                            if let Some(ref mut bm) = valid {
                                bm.push(false);
                            }
                        }
                    }
                }
                ColumnVector::Int64 { data, valid }
            }
            ColumnType::Real => {
                let mut data = Vec::with_capacity(n);
                let mut valid = if has_null {
                    Some(Vec::with_capacity(n))
                } else {
                    None
                };
                for v in values {
                    match v {
                        Value::Real(f) => {
                            data.push(f);
                            if let Some(ref mut bm) = valid {
                                bm.push(true);
                            }
                        }
                        Value::Integer(i) => {
                            data.push(i as f64);
                            if let Some(ref mut bm) = valid {
                                bm.push(true);
                            }
                        }
                        _ => {
                            data.push(0.0);
                            if let Some(ref mut bm) = valid {
                                bm.push(false);
                            }
                        }
                    }
                }
                ColumnVector::Float64 { data, valid }
            }
            ColumnType::Text => {
                let mut data = Vec::with_capacity(n);
                let mut valid = if has_null {
                    Some(Vec::with_capacity(n))
                } else {
                    None
                };
                for v in values {
                    match v {
                        Value::Text(s) => {
                            data.push(s);
                            if let Some(ref mut bm) = valid {
                                bm.push(true);
                            }
                        }
                        _ => {
                            data.push(String::new());
                            if let Some(ref mut bm) = valid {
                                bm.push(false);
                            }
                        }
                    }
                }
                ColumnVector::Text { data, valid }
            }
            ColumnType::Blob => {
                let mut data = Vec::with_capacity(n);
                let mut valid = if has_null {
                    Some(Vec::with_capacity(n))
                } else {
                    None
                };
                for v in values {
                    match v {
                        Value::Blob(b) => {
                            data.push(b);
                            if let Some(ref mut bm) = valid {
                                bm.push(true);
                            }
                        }
                        _ => {
                            data.push(Vec::new());
                            if let Some(ref mut bm) = valid {
                                bm.push(false);
                            }
                        }
                    }
                }
                ColumnVector::Blob { data, valid }
            }
        }
    }

    pub fn filled(col_type: ColumnType, default_val: Option<&Value>, row_count: usize) -> Self {
        match (col_type, default_val) {
            (ColumnType::Text, Some(Value::Text(s))) => ColumnVector::DictText {
                dict: vec![s.clone()],
                codes: vec![0; row_count],
                valid: None,
            },
            (col_t, dv) => {
                let v = dv.cloned().unwrap_or(Value::Null);
                Self::from_values(col_t, vec![v; row_count])
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Int64 { data, .. } => data.len(),
            ColumnVector::Float64 { data, .. } => data.len(),
            ColumnVector::DictText { codes, .. } => codes.len(),
            ColumnVector::Text { data, .. } => data.len(),
            ColumnVector::Blob { data, .. } => data.len(),
        }
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        match self {
            ColumnVector::Int64 { valid, .. }
            | ColumnVector::Float64 { valid, .. }
            | ColumnVector::DictText { valid, .. }
            | ColumnVector::Text { valid, .. }
            | ColumnVector::Blob { valid, .. } => match valid {
                None => true,
                Some(bm) => bm[row],
            },
        }
    }

    #[inline]
    pub fn get_value(&self, row: usize) -> Value {
        if !self.is_valid(row) {
            return Value::Null;
        }
        match self {
            ColumnVector::Int64 { data, .. } => Value::Integer(data[row]),
            ColumnVector::Float64 { data, .. } => Value::Real(data[row]),
            ColumnVector::DictText { dict, codes, .. } => {
                Value::Text(dict[codes[row] as usize].clone())
            }
            ColumnVector::Text { data, .. } => Value::Text(data[row].clone()),
            ColumnVector::Blob { data, .. } => Value::Blob(data[row].clone()),
        }
    }

    /// Zero-allocation exact equality check consistent with `Value::sql_cmp`.
    #[inline]
    pub fn eq_value_at(&self, row: usize, target: &Value) -> bool {
        if !self.is_valid(row) {
            return false;
        }
        match (self, target) {
            (ColumnVector::Int64 { data, .. }, Value::Integer(v)) => data[row] == *v,
            (ColumnVector::Float64 { data, .. }, Value::Real(v)) => {
                Value::Real(data[row]).sql_cmp(&Value::Real(*v)).is_eq()
            }
            (ColumnVector::Float64 { data, .. }, Value::Integer(v)) => {
                Value::Real(data[row]).sql_cmp(&Value::Integer(*v)).is_eq()
            }
            (ColumnVector::DictText { dict, codes, .. }, Value::Text(s)) => {
                dict[codes[row] as usize].as_str() == s.as_str()
            }
            (ColumnVector::Text { data, .. }, Value::Text(s)) => data[row].as_str() == s.as_str(),
            (ColumnVector::Blob { data, .. }, Value::Blob(b)) => {
                data[row].as_slice() == b.as_slice()
            }
            _ => self.get_value(row).sql_cmp(target).is_eq(),
        }
    }

    /// Zero-allocation equality check that treats NULL == NULL (for GROUP BY key matching).
    #[inline]
    pub fn eq_or_null_at(&self, row: usize, target: &Value) -> bool {
        if !self.is_valid(row) {
            return target.is_null();
        }
        if target.is_null() {
            return false;
        }
        self.eq_value_at(row, target)
    }

    /// Zero-allocation deterministic hash matching `hash_value` (NULL hashes to a fixed sentinel).
    #[inline]
    pub fn hash_or_null_at(&self, row: usize) -> u64 {
        const NULL_HASH: u64 = 0x9e37_79b9_7f4a_7c15;
        if !self.is_valid(row) {
            return NULL_HASH;
        }
        match self {
            ColumnVector::Int64 { data, .. } => {
                hash_value(&Value::Integer(data[row])).unwrap_or(NULL_HASH)
            }
            ColumnVector::Float64 { data, .. } => {
                hash_value(&Value::Real(data[row])).unwrap_or(NULL_HASH)
            }
            ColumnVector::DictText { dict, codes, .. } => dict
                .get(codes[row] as usize)
                .map(|s| fnv1a_str_hash(s.as_bytes()))
                .unwrap_or(NULL_HASH),
            ColumnVector::Text { data, .. } => fnv1a_str_hash(data[row].as_bytes()),
            ColumnVector::Blob { data, .. } => fnv1a_blob_hash(&data[row]),
        }
    }
}

#[inline]
fn splitmix64_local(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[inline]
fn fnv1a_str_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    splitmix64_local(h) ^ 0xAAAA_BBBB_CCCC_DDDD
}

#[inline]
fn fnv1a_blob_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    splitmix64_local(h) ^ 0xDDDD_EEEE_FFFF_1111
}

#[inline]
fn combine_key_hash(acc: u64, h: u64) -> u64 {
    splitmix64_local(acc ^ h.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

#[inline]
fn hash_value_tuple(vals: &[Value]) -> u64 {
    const NULL_HASH: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut acc = 0x517c_c1b7_2722_0a95u64;
    for v in vals {
        let h = hash_value(v).unwrap_or(NULL_HASH);
        acc = combine_key_hash(acc, h);
    }
    acc
}

/// Decoded columnar batch for a single PAX page (zero row transposition!).
#[derive(Debug)]
pub struct ColumnBatch {
    pub row_count: usize,
    pub rowids: Vec<i64>,
    pub columns: Vec<Option<ColumnVector>>,
}

// ============================================================================
// Vectorized Streaming Hash Aggregator (with Dictionary-Code Flat-Array Fast Path!)
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub enum VecAggKind {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

#[derive(Debug, Clone)]
pub enum VecAggAcc {
    Count(i64),
    Sum {
        has_val: bool,
        is_real: bool,
        sum_i: i64,
        sum_f: f64,
    },
    Avg {
        cnt: u64,
        sum: f64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl VecAggAcc {
    #[inline]
    pub fn new(kind: VecAggKind) -> Self {
        match kind {
            VecAggKind::CountStar | VecAggKind::Count(_) => VecAggAcc::Count(0),
            VecAggKind::Sum(_) => VecAggAcc::Sum {
                has_val: false,
                is_real: false,
                sum_i: 0,
                sum_f: 0.0,
            },
            VecAggKind::Avg(_) => VecAggAcc::Avg { cnt: 0, sum: 0.0 },
            VecAggKind::Min(_) => VecAggAcc::Min(None),
            VecAggKind::Max(_) => VecAggAcc::Max(None),
        }
    }

    #[inline]
    pub fn merge_from(&mut self, other: &VecAggAcc) {
        match (self, other) {
            (VecAggAcc::Count(a), VecAggAcc::Count(b)) => *a += *b,
            (
                VecAggAcc::Sum {
                    has_val: ha,
                    is_real: ra,
                    sum_i: ia,
                    sum_f: fa,
                },
                VecAggAcc::Sum {
                    has_val: hb,
                    is_real: rb,
                    sum_i: ib,
                    sum_f: fb,
                },
            ) => {
                if *hb {
                    *ha = true;
                    *ra |= *rb;
                    *ia = ia.wrapping_add(*ib);
                    *fa += *fb;
                }
            }
            (VecAggAcc::Avg { cnt: ca, sum: sa }, VecAggAcc::Avg { cnt: cb, sum: sb }) => {
                *ca += *cb;
                *sa += *sb;
            }
            (VecAggAcc::Min(a), VecAggAcc::Min(Some(b))) => match a {
                None => *a = Some(b.clone()),
                Some(cur) if b.sql_cmp(cur).is_lt() => *a = Some(b.clone()),
                _ => {}
            },
            (VecAggAcc::Max(a), VecAggAcc::Max(Some(b))) => match a {
                None => *a = Some(b.clone()),
                Some(cur) if b.sql_cmp(cur).is_gt() => *a = Some(b.clone()),
                _ => {}
            },
            _ => {}
        }
    }

    #[inline]
    pub fn update_batch(&mut self, kind: VecAggKind, batch: &ColumnBatch, row: usize) {
        match (self, kind) {
            (VecAggAcc::Count(c), VecAggKind::CountStar) => *c += 1,
            (VecAggAcc::Count(c), VecAggKind::Count(ci)) => {
                if let Some(ref cv) = batch.columns[ci] {
                    if cv.is_valid(row) {
                        *c += 1;
                    }
                }
            }
            (
                VecAggAcc::Sum {
                    has_val,
                    is_real,
                    sum_i,
                    sum_f,
                },
                VecAggKind::Sum(ci),
            ) => {
                if let Some(ref cv) = batch.columns[ci] {
                    match cv {
                        ColumnVector::Float64 { data, valid: None } => {
                            *has_val = true;
                            *is_real = true;
                            *sum_f += data[row];
                        }
                        ColumnVector::Int64 { data, valid: None } => {
                            *has_val = true;
                            *sum_i = sum_i.wrapping_add(data[row]);
                            *sum_f += data[row] as f64;
                        }
                        ColumnVector::Float64 {
                            data,
                            valid: Some(bm),
                        } => {
                            if bm[row] {
                                *has_val = true;
                                *is_real = true;
                                *sum_f += data[row];
                            }
                        }
                        ColumnVector::Int64 {
                            data,
                            valid: Some(bm),
                        } => {
                            if bm[row] {
                                *has_val = true;
                                *sum_i = sum_i.wrapping_add(data[row]);
                                *sum_f += data[row] as f64;
                            }
                        }
                        _ => {}
                    }
                }
            }
            (VecAggAcc::Avg { cnt, sum }, VecAggKind::Avg(ci)) => {
                if let Some(ref cv) = batch.columns[ci] {
                    match cv {
                        ColumnVector::Float64 { data, valid: None } => {
                            *cnt += 1;
                            *sum += data[row];
                        }
                        ColumnVector::Int64 { data, valid: None } => {
                            *cnt += 1;
                            *sum += data[row] as f64;
                        }
                        ColumnVector::Float64 {
                            data,
                            valid: Some(bm),
                        } => {
                            if bm[row] {
                                *cnt += 1;
                                *sum += data[row];
                            }
                        }
                        ColumnVector::Int64 {
                            data,
                            valid: Some(bm),
                        } => {
                            if bm[row] {
                                *cnt += 1;
                                *sum += data[row] as f64;
                            }
                        }
                        _ => {}
                    }
                }
            }
            (VecAggAcc::Min(best), VecAggKind::Min(ci)) => {
                if let Some(ref cv) = batch.columns[ci] {
                    if !cv.is_valid(row) {
                        return;
                    }
                    let v = cv.get_value(row);
                    match best {
                        None => *best = Some(v),
                        Some(cur) if v.sql_cmp(cur).is_lt() => *best = Some(v),
                        _ => {}
                    }
                }
            }
            (VecAggAcc::Max(best), VecAggKind::Max(ci)) => {
                if let Some(ref cv) = batch.columns[ci] {
                    if !cv.is_valid(row) {
                        return;
                    }
                    let v = cv.get_value(row);
                    match best {
                        None => *best = Some(v),
                        Some(cur) if v.sql_cmp(cur).is_gt() => *best = Some(v),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    pub fn finalize(&self) -> Value {
        match self {
            VecAggAcc::Count(c) => Value::Integer(*c),
            VecAggAcc::Sum {
                has_val,
                is_real,
                sum_i,
                sum_f,
            } => {
                if !*has_val {
                    Value::Null
                } else if *is_real {
                    Value::Real(*sum_f)
                } else {
                    Value::Integer(*sum_i)
                }
            }
            VecAggAcc::Avg { cnt, sum } => {
                if *cnt == 0 {
                    Value::Null
                } else {
                    Value::Real(*sum / (*cnt as f64))
                }
            }
            VecAggAcc::Min(v) | VecAggAcc::Max(v) => v.clone().unwrap_or(Value::Null),
        }
    }
}

/// Vectorized filter predicate (automatically resolves dictionary codes ONCE per batch!).
#[derive(Debug, Clone)]
pub enum VecFilter {
    EqStr(usize, String),
    EqInt(usize, i64),
    GtInt(usize, i64),
    GeInt(usize, i64),
    LtInt(usize, i64),
    LeInt(usize, i64),
    GtReal(usize, f64),
    LtReal(usize, f64),
    And(Box<VecFilter>, Box<VecFilter>),
}

/// Pre-bound filter for a specific `ColumnBatch`: converts `EqStr` on `DictText` into a single `u32` code!
enum BoundFilter<'a> {
    AlwaysFalse,
    EqDictCode(usize, u32),
    EqStrRaw(usize, &'a str),
    EqInt(usize, i64),
    GtInt(usize, i64),
    GeInt(usize, i64),
    LtInt(usize, i64),
    LeInt(usize, i64),
    GtReal(usize, f64),
    LtReal(usize, f64),
    And(Box<BoundFilter<'a>>, Box<BoundFilter<'a>>),
}

impl VecFilter {
    fn bind<'a>(&'a self, batch: &ColumnBatch) -> BoundFilter<'a> {
        match self {
            VecFilter::EqStr(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::DictText { dict, .. }) => {
                    match dict.iter().position(|s| s == target) {
                        Some(code) => BoundFilter::EqDictCode(*ci, code as u32),
                        None => BoundFilter::AlwaysFalse, // Entire page skipped in O(1)!
                    }
                }
                _ => BoundFilter::EqStrRaw(*ci, target.as_str()),
            },
            VecFilter::EqInt(ci, v) => BoundFilter::EqInt(*ci, *v),
            VecFilter::GtInt(ci, v) => BoundFilter::GtInt(*ci, *v),
            VecFilter::GeInt(ci, v) => BoundFilter::GeInt(*ci, *v),
            VecFilter::LtInt(ci, v) => BoundFilter::LtInt(*ci, *v),
            VecFilter::LeInt(ci, v) => BoundFilter::LeInt(*ci, *v),
            VecFilter::GtReal(ci, v) => BoundFilter::GtReal(*ci, *v),
            VecFilter::LtReal(ci, v) => BoundFilter::LtReal(*ci, *v),
            VecFilter::And(a, b) => {
                let ba = a.bind(batch);
                let bb = b.bind(batch);
                if matches!(ba, BoundFilter::AlwaysFalse) || matches!(bb, BoundFilter::AlwaysFalse)
                {
                    BoundFilter::AlwaysFalse
                } else {
                    BoundFilter::And(Box::new(ba), Box::new(bb))
                }
            }
        }
    }
}

impl<'a> BoundFilter<'a> {
    #[inline]
    fn eval_row(&self, batch: &ColumnBatch, row: usize) -> bool {
        match self {
            BoundFilter::AlwaysFalse => false,
            BoundFilter::EqDictCode(ci, target_code) => match &batch.columns[*ci] {
                Some(ColumnVector::DictText { codes, valid: None, .. }) => {
                    codes[row] == *target_code
                }
                Some(ColumnVector::DictText {
                    codes,
                    valid: Some(bm),
                    ..
                }) => bm[row] && codes[row] == *target_code,
                _ => false,
            },
            BoundFilter::EqStrRaw(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Text { data, valid }) => {
                    valid.as_ref().map(|v| v[row]).unwrap_or(true) && data[row].as_str() == *target
                }
                _ => false,
            },
            BoundFilter::EqInt(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Int64 { data, valid: None }) => data[row] == *target,
                Some(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && data[row] == *target,
                _ => false,
            },
            BoundFilter::GtInt(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Int64 { data, valid: None }) => data[row] > *target,
                Some(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && data[row] > *target,
                _ => false,
            },
            BoundFilter::GeInt(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Int64 { data, valid: None }) => data[row] >= *target,
                Some(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && data[row] >= *target,
                _ => false,
            },
            BoundFilter::LtInt(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Int64 { data, valid: None }) => data[row] < *target,
                Some(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && data[row] < *target,
                _ => false,
            },
            BoundFilter::LeInt(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Int64 { data, valid: None }) => data[row] <= *target,
                Some(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && data[row] <= *target,
                _ => false,
            },
            BoundFilter::GtReal(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Float64 { data, valid: None }) => {
                    Value::Real(data[row]).sql_cmp(&Value::Real(*target)).is_gt()
                }
                Some(ColumnVector::Float64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && Value::Real(data[row]).sql_cmp(&Value::Real(*target)).is_gt(),
                _ => false,
            },
            BoundFilter::LtReal(ci, target) => match &batch.columns[*ci] {
                Some(ColumnVector::Float64 { data, valid: None }) => {
                    Value::Real(data[row]).sql_cmp(&Value::Real(*target)).is_lt()
                }
                Some(ColumnVector::Float64 {
                    data,
                    valid: Some(bm),
                }) => bm[row] && Value::Real(data[row]).sql_cmp(&Value::Real(*target)).is_lt(),
                _ => false,
            },
            BoundFilter::And(a, b) => a.eval_row(batch, row) && b.eval_row(batch, row),
        }
    }
}

/// Streaming vectorized GROUP BY accumulator (single-column AND multi-column!).
/// - Single `DictText` column: accumulates in an L1-cache-resident flat array indexed directly by `dict_code`.
/// - Multiple `DictText` columns with small combined cardinality (<= 4096): accumulates in a flat
///   multi-dimensional code array with zero per-row hashing or String allocations.
/// - General single or multi-column keys: hashes directly from `ColumnVector` slices without
///   transposing rows or cloning Strings on hash hits.
pub struct VectorGroupTable {
    pub group_cols: Vec<usize>,
    pub agg_kinds: Vec<VecAggKind>,
    pub str_groups: HashMap<String, Vec<VecAggAcc>>,
    pub int_groups: HashMap<i64, Vec<VecAggAcc>>,
    pub null_group: Option<Vec<VecAggAcc>>,
    pub global_group: Option<Vec<VecAggAcc>>,
    pub multi_groups: HashMap<u64, Vec<(Vec<Value>, Vec<VecAggAcc>)>>,
}

impl VectorGroupTable {
    pub fn new(group_cols: Vec<usize>, agg_kinds: Vec<VecAggKind>) -> Self {
        let global_group = if group_cols.is_empty() {
            Some(agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect())
        } else {
            None
        };
        VectorGroupTable {
            group_cols,
            agg_kinds,
            str_groups: HashMap::new(),
            int_groups: HashMap::new(),
            null_group: None,
            global_group,
            multi_groups: HashMap::new(),
        }
    }

    fn merge_multi_entry(&mut self, key: Vec<Value>, accs: Vec<VecAggAcc>) {
        let h = hash_value_tuple(&key);
        let bucket = self.multi_groups.entry(h).or_default();
        for (ek, eaccs) in bucket.iter_mut() {
            if ek.len() == key.len()
                && ek
                    .iter()
                    .zip(&key)
                    .all(|(a, b)| a.sql_cmp(b).is_eq())
            {
                for (ga, la) in eaccs.iter_mut().zip(&accs) {
                    ga.merge_from(la);
                }
                return;
            }
        }
        bucket.push((key, accs));
    }

    pub fn ingest_batch(
        &mut self,
        batch: &ColumnBatch,
        deleted_slots: &[u32],
        filter: Option<&VecFilter>,
    ) {
        let bound_filter = filter.map(|f| f.bind(batch));
        if matches!(bound_filter, Some(BoundFilter::AlwaysFalse)) {
            return;
        }

        // Build tight SelectionVector of surviving row indices!
        let mut sel: Vec<usize> = Vec::with_capacity(batch.row_count);
        let mut del_idx = 0usize;
        match &bound_filter {
            None if deleted_slots.is_empty() => {
                sel.extend(0..batch.row_count);
            }
            Some(bf) => {
                for row in 0..batch.row_count {
                    if del_idx < deleted_slots.len() && deleted_slots[del_idx] == row as u32 {
                        del_idx += 1;
                        continue;
                    }
                    if bf.eval_row(batch, row) {
                        sel.push(row);
                    }
                }
            }
            None => {
                for row in 0..batch.row_count {
                    if del_idx < deleted_slots.len() && deleted_slots[del_idx] == row as u32 {
                        del_idx += 1;
                        continue;
                    }
                    sel.push(row);
                }
            }
        }
        if sel.is_empty() {
            return;
        }

        // GLOBAL GROUPING (0 group columns)
        if self.group_cols.is_empty() {
            let accs = self.global_group.as_mut().unwrap();
            for &row in &sel {
                for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                    acc.update_batch(kind, batch, row);
                }
            }
            return;
        }

        // SINGLE-COLUMN GROUPING (preserves L1 flat-array DictText fast path!)
        if self.group_cols.len() == 1 {
            let gc = self.group_cols[0];
            if let Some(ColumnVector::DictText { dict, codes, valid }) = &batch.columns[gc] {
                let dlen = dict.len();
                let mut local: Vec<Vec<VecAggAcc>> = (0..dlen)
                    .map(|_| self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect())
                    .collect();
                let mut touched = vec![false; dlen];

                if let Some(bm) = valid {
                    for &row in &sel {
                        if !bm[row] {
                            let accs = self.null_group.get_or_insert_with(|| {
                                self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect()
                            });
                            for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                                acc.update_batch(kind, batch, row);
                            }
                        } else {
                            let code = codes[row] as usize;
                            touched[code] = true;
                            let accs = &mut local[code];
                            for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                                acc.update_batch(kind, batch, row);
                            }
                        }
                    }
                } else {
                    for &row in &sel {
                        let code = codes[row] as usize;
                        touched[code] = true;
                        let accs = &mut local[code];
                        for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                            acc.update_batch(kind, batch, row);
                        }
                    }
                }

                for code in 0..dlen {
                    if !touched[code] {
                        continue;
                    }
                    let key = &dict[code];
                    if let Some(existing) = self.str_groups.get_mut(key.as_str()) {
                        for (ga, la) in existing.iter_mut().zip(&local[code]) {
                            ga.merge_from(la);
                        }
                    } else {
                        self.str_groups.insert(key.clone(), local[code].clone());
                    }
                }
                return;
            }

            if matches!(
                batch.columns[gc],
                Some(ColumnVector::Text { .. }) | Some(ColumnVector::Int64 { .. })
            ) {
                for &row in &sel {
                    let accs: &mut Vec<VecAggAcc> = match &batch.columns[gc] {
                        Some(ColumnVector::Text { data, valid }) => {
                            if !valid.as_ref().map(|v| v[row]).unwrap_or(true) {
                                self.null_group.get_or_insert_with(|| {
                                    self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect()
                                })
                            } else {
                                let key = &data[row];
                                if let Some(existing) = self.str_groups.get_mut(key.as_str()) {
                                    existing
                                } else {
                                    let fresh: Vec<VecAggAcc> = self
                                        .agg_kinds
                                        .iter()
                                        .map(|&k| VecAggAcc::new(k))
                                        .collect();
                                    self.str_groups.entry(key.clone()).or_insert(fresh)
                                }
                            }
                        }
                        Some(ColumnVector::Int64 { data, valid }) => {
                            if !valid.as_ref().map(|v| v[row]).unwrap_or(true) {
                                self.null_group.get_or_insert_with(|| {
                                    self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect()
                                })
                            } else {
                                let k = data[row];
                                self.int_groups.entry(k).or_insert_with(|| {
                                    self.agg_kinds.iter().map(|&ak| VecAggAcc::new(ak)).collect()
                                })
                            }
                        }
                        _ => unreachable!(),
                    };
                    for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                        acc.update_batch(kind, batch, row);
                    }
                }
                return;
            }
        }

        // MULTI-COLUMN (or Float64/Blob single-column) VECTORIZED GROUPING
        // Fast path: all group columns are DictText with small combined cardinality <= 4096
        let mut dict_dims: Option<Vec<(&[String], &[u32], Option<&[bool]>, usize)>> =
            Some(Vec::with_capacity(self.group_cols.len()));
        let mut total_slots = 1usize;
        for &gc in &self.group_cols {
            if let Some(ColumnVector::DictText { dict, codes, valid }) = &batch.columns[gc] {
                let stride = dict.len() + 1; // last slot reserved for NULL
                match total_slots.checked_mul(stride) {
                    Some(next) if next <= 4096 => {
                        total_slots = next;
                        dict_dims.as_mut().unwrap().push((
                            dict.as_slice(),
                            codes.as_slice(),
                            valid.as_deref(),
                            stride,
                        ));
                    }
                    _ => {
                        dict_dims = None;
                        break;
                    }
                }
            } else {
                dict_dims = None;
                break;
            }
        }

        if let Some(dims) = dict_dims {
            let mut local: Vec<Option<Vec<VecAggAcc>>> = vec![None; total_slots];
            let mut touched: Vec<usize> = Vec::new();
            for &row in &sel {
                let mut flat_idx = 0usize;
                for &(dict, codes, valid, stride) in &dims {
                    let raw_c = codes[row] as usize;
                    let c = if valid.map(|bm| !bm[row]).unwrap_or(false) || raw_c >= dict.len() {
                        stride - 1
                    } else {
                        raw_c
                    };
                    flat_idx = flat_idx * stride + c;
                }
                if flat_idx >= total_slots {
                    continue;
                }
                let slot = &mut local[flat_idx];
                if slot.is_none() {
                    touched.push(flat_idx);
                    *slot = Some(self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect());
                }
                let accs = slot.as_mut().unwrap();
                for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                    acc.update_batch(kind, batch, row);
                }
            }
            for flat_idx in touched {
                let accs = local[flat_idx].take().unwrap();
                let mut rem = flat_idx;
                let mut key = vec![Value::Null; dims.len()];
                for d_i in (0..dims.len()).rev() {
                    let (dict, _, _, stride) = dims[d_i];
                    let c = rem % stride;
                    rem /= stride;
                    if c < dict.len() {
                        key[d_i] = Value::Text(dict[c].clone());
                    }
                }
                self.merge_multi_entry(key, accs);
            }
            return;
        }

        // General multi-column path: zero-allocation direct ColumnVector hash + equality probe!
        let gcols = self.group_cols.clone();
        for &row in &sel {
            let mut h = 0x517c_c1b7_2722_0a95u64;
            for &gc in &gcols {
                let ch = batch.columns[gc]
                    .as_ref()
                    .map(|cv| cv.hash_or_null_at(row))
                    .unwrap_or(0x9e37_79b9_7f4a_7c15);
                h = combine_key_hash(h, ch);
            }
            let bucket = self.multi_groups.entry(h).or_default();
            let mut found_idx: Option<usize> = None;
            for (bi, (ek, _)) in bucket.iter().enumerate() {
                let mut eq = true;
                for (k_pos, &gc) in gcols.iter().enumerate() {
                    let matched = match &batch.columns[gc] {
                        Some(cv) => cv.eq_or_null_at(row, &ek[k_pos]),
                        None => ek[k_pos].is_null(),
                    };
                    if !matched {
                        eq = false;
                        break;
                    }
                }
                if eq {
                    found_idx = Some(bi);
                    break;
                }
            }
            let accs = if let Some(bi) = found_idx {
                &mut bucket[bi].1
            } else {
                let mut key = Vec::with_capacity(gcols.len());
                for &gc in &gcols {
                    let v = batch.columns[gc]
                        .as_ref()
                        .map(|cv| cv.get_value(row))
                        .unwrap_or(Value::Null);
                    key.push(v);
                }
                let fresh: Vec<VecAggAcc> =
                    self.agg_kinds.iter().map(|&k| VecAggAcc::new(k)).collect();
                bucket.push((key, fresh));
                &mut bucket.last_mut().unwrap().1
            };
            for (acc, &kind) in accs.iter_mut().zip(&self.agg_kinds) {
                acc.update_batch(kind, batch, row);
            }
        }
    }

    pub fn merge_from(&mut self, other: VectorGroupTable) {
        if let (Some(ref mut g_self), Some(ref g_other)) =
            (&mut self.global_group, &other.global_group)
        {
            for (a, b) in g_self.iter_mut().zip(g_other) {
                a.merge_from(b);
            }
        }
        for (k, accs) in other.str_groups {
            if let Some(existing) = self.str_groups.get_mut(&k) {
                for (a, b) in existing.iter_mut().zip(&accs) {
                    a.merge_from(b);
                }
            } else {
                self.str_groups.insert(k, accs);
            }
        }
        for (k, accs) in other.int_groups {
            if let Some(existing) = self.int_groups.get_mut(&k) {
                for (a, b) in existing.iter_mut().zip(&accs) {
                    a.merge_from(b);
                }
            } else {
                self.int_groups.insert(k, accs);
            }
        }
        if let Some(o_null) = other.null_group {
            if let Some(ref mut s_null) = self.null_group {
                for (a, b) in s_null.iter_mut().zip(&o_null) {
                    a.merge_from(b);
                }
            } else {
                self.null_group = Some(o_null);
            }
        }
        for (_, entries) in other.multi_groups {
            for (key, accs) in entries {
                self.merge_multi_entry(key, accs);
            }
        }
    }

    pub fn into_groups(self) -> Vec<(Vec<Value>, Vec<Value>)> {
        let mut out = Vec::new();
        if let Some(g) = self.global_group {
            out.push((Vec::new(), g.iter().map(|a| a.finalize()).collect()));
            return out;
        }
        for (k, accs) in self.str_groups {
            out.push((
                vec![Value::Text(k)],
                accs.iter().map(|a| a.finalize()).collect(),
            ));
        }
        for (k, accs) in self.int_groups {
            out.push((
                vec![Value::Integer(k)],
                accs.iter().map(|a| a.finalize()).collect(),
            ));
        }
        if let Some(accs) = self.null_group {
            out.push((vec![Value::Null], accs.iter().map(|a| a.finalize()).collect()));
        }
        for (_, entries) in self.multi_groups {
            for (key, accs) in entries {
                out.push((key, accs.iter().map(|a| a.finalize()).collect()));
            }
        }
        out
    }
}

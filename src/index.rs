use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::pax::{compress_bytes, decompress_bytes, CompressionAlgo, CompressionProfile};
use crate::types::{
    read_exact, read_slice, read_str, read_u8, read_uvarint, read_zigzag_varint, write_str,
    write_uvarint, write_zigzag_varint, Error, Result, Value,
};

pub const INDEX_BLOCK_ENTRIES: usize = 4096;

// ============================================================================
// Fast deterministic 64-bit value hashing (SplitMix64 + FNV-1a)
// ============================================================================

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[inline]
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    splitmix64(h)
}

/// Deterministic 64-bit hash for any non-NULL SQL Value.
/// Numeric values that compare equal in SQL (e.g. Integer(5) and Real(5.0)) hash identically.
pub fn hash_value(val: &Value) -> Option<u64> {
    match val {
        Value::Null => None,
        Value::Integer(i) => Some(splitmix64((*i as u64) ^ 0x1111_2222_3333_4444)),
        Value::Real(f) => {
            if f.is_nan() {
                return Some(splitmix64(f64::NAN.to_bits() ^ 0x5555_6666_7777_8888));
            }
            if f.is_finite()
                && f.fract() == 0.0
                && *f >= -9223372036854775808.0
                && *f < 9223372036854775808.0
            {
                let iv = *f as i64;
                if Value::Integer(iv).sql_cmp(&Value::Real(*f)).is_eq() {
                    return Some(splitmix64((iv as u64) ^ 0x1111_2222_3333_4444));
                }
            }
            let bits = if *f == 0.0 { 0u64 } else { f.to_bits() };
            Some(splitmix64(bits ^ 0x5555_6666_7777_8888))
        }
        Value::Text(s) => Some(fnv1a_64(s.as_bytes()) ^ 0xAAAA_BBBB_CCCC_DDDD),
        Value::Blob(b) => Some(fnv1a_64(b) ^ 0xDDDD_EEEE_FFFF_1111),
    }
}

/// Deterministic 64-bit hash for a composite key tuple (returns None if any column is NULL).
pub fn hash_key_tuple(vals: &[Value]) -> Option<u64> {
    if vals.len() == 1 {
        return hash_value(&vals[0]);
    }
    let mut acc = 0x517c_c1b7_2722_0a95u64;
    for v in vals {
        let h = hash_value(v)?;
        acc = splitmix64(acc ^ h.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    }
    Some(acc)
}

// ============================================================================
// Composite Key Ordering & Range Bounds (for B-Tree Indexes)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexKind {
    Hash = 1,
    BTree = 2,
}

impl IndexKind {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(IndexKind::Hash),
            2 => Ok(IndexKind::BTree),
            _ => Err(Error::Corruption(format!("Unknown index kind tag: {}", v))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RangeBound {
    Unbounded,
    Included(Value),
    Excluded(Value),
}

/// Range/prefix scan specification for a composite B-tree index:
/// matches keys where `key[0..prefix_eq.len()] == prefix_eq` and
/// `key[prefix_eq.len()]` falls within `[range_lo, range_hi]`.
#[derive(Debug, Clone)]
pub struct BTreeScanSpec {
    pub prefix_eq: Vec<Value>,
    pub range_lo: RangeBound,
    pub range_hi: RangeBound,
}

#[inline]
pub fn cmp_key_tuple(a: &[Value], b: &[Value], desc: &[bool]) -> Ordering {
    let n = a.len().min(b.len());
    for i in 0..n {
        let o = a[i].sql_cmp(&b[i]);
        if o != Ordering::Equal {
            return if desc.get(i).copied().unwrap_or(false) {
                o.reverse()
            } else {
                o
            };
        }
    }
    a.len().cmp(&b.len())
}

/// Compares a stored key against the lower bound of a `BTreeScanSpec`.
/// Returns `Less` if `key` is strictly before the scan range, `Equal` or `Greater` if at/past start.
pub fn cmp_key_vs_spec_lo(key: &[Value], spec: &BTreeScanSpec, desc: &[bool]) -> Ordering {
    let plen = spec.prefix_eq.len();
    for i in 0..plen {
        let mut o = key[i].sql_cmp(&spec.prefix_eq[i]);
        if desc.get(i).copied().unwrap_or(false) {
            o = o.reverse();
        }
        if o != Ordering::Equal {
            return o;
        }
    }
    if plen >= key.len() {
        return Ordering::Equal;
    }
    let col_desc = desc.get(plen).copied().unwrap_or(false);
    // If the range column is stored DESC, the logical upper bound is the physical lower bound!
    let eff_lo = if col_desc {
        &spec.range_hi
    } else {
        &spec.range_lo
    };
    match eff_lo {
        RangeBound::Unbounded => Ordering::Equal,
        RangeBound::Included(bound) => {
            let mut o = key[plen].sql_cmp(bound);
            if col_desc {
                o = o.reverse();
            }
            if o == Ordering::Less {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
        RangeBound::Excluded(bound) => {
            let mut o = key[plen].sql_cmp(bound);
            if col_desc {
                o = o.reverse();
            }
            if o != Ordering::Greater {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
    }
}

/// Compares a stored key against the upper bound of a `BTreeScanSpec`.
/// Returns `Greater` if `key` is strictly past the scan range.
pub fn cmp_key_vs_spec_hi(key: &[Value], spec: &BTreeScanSpec, desc: &[bool]) -> Ordering {
    let plen = spec.prefix_eq.len();
    for i in 0..plen {
        let mut o = key[i].sql_cmp(&spec.prefix_eq[i]);
        if desc.get(i).copied().unwrap_or(false) {
            o = o.reverse();
        }
        if o != Ordering::Equal {
            return o;
        }
    }
    if plen >= key.len() {
        return Ordering::Equal;
    }
    let col_desc = desc.get(plen).copied().unwrap_or(false);
    let eff_hi = if col_desc {
        &spec.range_lo
    } else {
        &spec.range_hi
    };
    match eff_hi {
        RangeBound::Unbounded => Ordering::Equal,
        RangeBound::Included(bound) => {
            let mut o = key[plen].sql_cmp(bound);
            if col_desc {
                o = o.reverse();
            }
            if o == Ordering::Greater {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        RangeBound::Excluded(bound) => {
            let mut o = key[plen].sql_cmp(bound);
            if col_desc {
                o = o.reverse();
            }
            if o != Ordering::Less {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
    }
}

pub fn key_matches_spec(key: &[Value], spec: &BTreeScanSpec, desc: &[bool]) -> bool {
    // SQL NULLs never match equality or range predicates (=, <, <=, >, >=, BETWEEN)
    for (i, pv) in spec.prefix_eq.iter().enumerate() {
        if key[i].is_null() || pv.is_null() {
            return false;
        }
    }
    let plen = spec.prefix_eq.len();
    if plen < key.len()
        && (!matches!(spec.range_lo, RangeBound::Unbounded)
            || !matches!(spec.range_hi, RangeBound::Unbounded))
        && key[plen].is_null()
    {
        return false;
    }
    cmp_key_vs_spec_lo(key, spec, desc) != Ordering::Less
        && cmp_key_vs_spec_hi(key, spec, desc) != Ordering::Greater
}

// ============================================================================
// Per-Page Compact Bloom Filter (~6 bits/unique value -> ~5.6% FP rate)
// ============================================================================

#[derive(Debug, Clone)]
pub struct BloomFilter {
    pub num_hashes: u8,
    pub bits: Vec<u8>,
}

impl BloomFilter {
    pub fn build_for_column(values: &[Value]) -> Option<Self> {
        let mut hashes: Vec<u64> = Vec::with_capacity(values.len());
        let mut uniq: HashSet<u64> = HashSet::new();
        for v in values {
            if let Value::Blob(b) = v {
                if b.len() > 256 {
                    return None;
                }
            }
            if let Value::Text(s) = v {
                if s.len() > 256 {
                    return None;
                }
            }
            if let Some(h) = hash_value(v) {
                if uniq.insert(h) {
                    hashes.push(h);
                }
            }
        }
        if hashes.len() < 16 {
            return None;
        }

        let num_bits = (hashes.len() * 6).max(64);
        let num_bytes = (num_bits + 7) / 8;
        let total_bits = (num_bytes * 8) as u64;
        let num_hashes: u8 = 4;
        let mut bits = vec![0u8; num_bytes];

        for h in hashes {
            let h1 = h;
            let h2 = splitmix64(h.rotate_left(17));
            for i in 0..(num_hashes as u64) {
                let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % total_bits) as usize;
                bits[bit_idx >> 3] |= 1 << (bit_idx & 7);
            }
        }

        Some(BloomFilter { num_hashes, bits })
    }

    #[inline]
    pub fn might_contain(&self, val: &Value) -> bool {
        let h = match hash_value(val) {
            Some(h) => h,
            None => return false,
        };
        if self.bits.is_empty() {
            return false;
        }
        let total_bits = (self.bits.len() * 8) as u64;
        let h1 = h;
        let h2 = splitmix64(h.rotate_left(17));
        for i in 0..(self.num_hashes as u64) {
            let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % total_bits) as usize;
            if (self.bits[bit_idx >> 3] & (1 << (bit_idx & 7))) == 0 {
                return false;
            }
        }
        true
    }

    pub fn write_binary(&self, out: &mut Vec<u8>) {
        out.push(self.num_hashes);
        write_uvarint(self.bits.len() as u64, out);
        out.extend_from_slice(&self.bits);
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let num_hashes = read_u8(buf, pos)?;
        let len = read_uvarint(buf, pos)? as usize;
        let bytes = read_slice(buf, pos, len)?;
        Ok(BloomFilter {
            num_hashes,
            bits: bytes.to_vec(),
        })
    }
}

// ============================================================================
// Paged On-Disk Hash SSTable Secondary Index
// ============================================================================

#[derive(Debug, Clone)]
pub struct IndexBlockMeta {
    pub min_hash: u64,
    pub max_hash: u64,
    pub offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub compression: CompressionAlgo,
    pub crc32: u32,
    pub entry_count: u32,
}

impl IndexBlockMeta {
    pub fn write_binary(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.min_hash.to_le_bytes());
        out.extend_from_slice(&self.max_hash.to_le_bytes());
        write_uvarint(self.offset, out);
        write_uvarint(self.stored_len as u64, out);
        write_uvarint(self.raw_len as u64, out);
        out.push(self.compression as u8);
        out.extend_from_slice(&self.crc32.to_le_bytes());
        write_uvarint(self.entry_count as u64, out);
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let min_hash = u64::from_le_bytes(read_exact::<8>(buf, pos)?);
        let max_hash = u64::from_le_bytes(read_exact::<8>(buf, pos)?);
        let offset = read_uvarint(buf, pos)?;
        let stored_len = read_uvarint(buf, pos)? as u32;
        let raw_len = read_uvarint(buf, pos)? as u32;
        let compression = CompressionAlgo::from_u8(read_u8(buf, pos)?)?;
        let crc32 = u32::from_le_bytes(read_exact::<4>(buf, pos)?);
        let entry_count = read_uvarint(buf, pos)? as u32;
        Ok(IndexBlockMeta {
            min_hash,
            max_hash,
            offset,
            stored_len,
            raw_len,
            compression,
            crc32,
            entry_count,
        })
    }
}

pub fn encode_index_block(
    entries: &[(u64, u64, u32)],
    profile: CompressionProfile,
) -> Result<(Vec<u8>, u32, CompressionAlgo, u32)> {
    let mut raw = Vec::with_capacity(entries.len() * 10);
    write_uvarint(entries.len() as u64, &mut raw);
    let mut prev_h = 0u64;
    for &(h, pid, slot) in entries {
        write_uvarint(h.wrapping_sub(prev_h), &mut raw);
        write_uvarint(pid, &mut raw);
        write_uvarint(slot as u64, &mut raw);
        prev_h = h;
    }
    let raw_len = raw.len() as u32;
    let (algo, stored) = compress_bytes(&raw, profile)?;
    let crc = crc32fast::hash(&stored);
    Ok((stored, raw_len, algo, crc))
}

pub fn decode_index_block(
    stored: &[u8],
    algo: CompressionAlgo,
    raw_len: u32,
    expected_crc32: u32,
) -> Result<Vec<(u64, u64, u32)>> {
    let actual_crc = crc32fast::hash(stored);
    if actual_crc != expected_crc32 {
        return Err(Error::Corruption(format!(
            "Index block CRC32 mismatch: expected {:08x}, got {:08x}",
            expected_crc32, actual_crc
        )));
    }
    let raw = decompress_bytes(stored, algo, raw_len as usize)?;
    let mut pos = 0usize;
    let len = read_uvarint(&raw, &mut pos)? as usize;
    let mut entries = Vec::with_capacity(len);
    let mut prev_h = 0u64;
    for _ in 0..len {
        let dh = read_uvarint(&raw, &mut pos)?;
        prev_h = prev_h.wrapping_add(dh);
        let pid = read_uvarint(&raw, &mut pos)?;
        let slot = read_uvarint(&raw, &mut pos)? as u32;
        entries.push((prev_h, pid, slot));
    }
    Ok(entries)
}

// ============================================================================
// Paged On-Disk Range B-Tree Secondary Index (Single & Composite Multi-Column)
// ============================================================================

pub type BTreeLeafEntry = (Vec<Value>, u64, u32);

#[derive(Debug, Clone)]
pub struct BTreeBlockMeta {
    pub min_key: Vec<Value>,
    pub max_key: Vec<Value>,
    pub offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub compression: CompressionAlgo,
    pub crc32: u32,
    pub entry_count: u32,
}

impl BTreeBlockMeta {
    pub fn write_binary(&self, out: &mut Vec<u8>) {
        write_uvarint(self.min_key.len() as u64, out);
        for v in &self.min_key {
            v.write_binary(out);
        }
        write_uvarint(self.max_key.len() as u64, out);
        for v in &self.max_key {
            v.write_binary(out);
        }
        write_uvarint(self.offset, out);
        write_uvarint(self.stored_len as u64, out);
        write_uvarint(self.raw_len as u64, out);
        out.push(self.compression as u8);
        out.extend_from_slice(&self.crc32.to_le_bytes());
        write_uvarint(self.entry_count as u64, out);
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let min_len = read_uvarint(buf, pos)? as usize;
        let mut min_key = Vec::with_capacity(min_len);
        for _ in 0..min_len {
            min_key.push(Value::read_binary(buf, pos)?);
        }
        let max_len = read_uvarint(buf, pos)? as usize;
        let mut max_key = Vec::with_capacity(max_len);
        for _ in 0..max_len {
            max_key.push(Value::read_binary(buf, pos)?);
        }
        let offset = read_uvarint(buf, pos)?;
        let stored_len = read_uvarint(buf, pos)? as u32;
        let raw_len = read_uvarint(buf, pos)? as u32;
        let compression = CompressionAlgo::from_u8(read_u8(buf, pos)?)?;
        let crc32 = u32::from_le_bytes(read_exact::<4>(buf, pos)?);
        let entry_count = read_uvarint(buf, pos)? as u32;
        Ok(BTreeBlockMeta {
            min_key,
            max_key,
            offset,
            stored_len,
            raw_len,
            compression,
            crc32,
            entry_count,
        })
    }
}

/// Front-coded + delta-encoded compressed B-tree leaf block encoder.
pub fn encode_btree_block(
    entries: &[BTreeLeafEntry],
    key_cols: usize,
    profile: CompressionProfile,
) -> Result<(Vec<u8>, u32, CompressionAlgo, u32)> {
    let mut raw = Vec::with_capacity(entries.len() * (8 + key_cols * 8));
    write_uvarint(entries.len() as u64, &mut raw);
    write_uvarint(key_cols as u64, &mut raw);

    let mut prev_vals: Vec<Option<Value>> = vec![None; key_cols];

    for (key, pid, slot) in entries {
        for c in 0..key_cols {
            let v = &key[c];
            match (&prev_vals[c], v) {
                (Some(Value::Integer(prev_i)), Value::Integer(cur_i)) => {
                    // Tag 10 = delta from previous integer
                    raw.push(10);
                    write_zigzag_varint(cur_i.wrapping_sub(*prev_i), &mut raw);
                }
                (Some(Value::Text(prev_s)), Value::Text(cur_s)) => {
                    // Tag 11 = front-coded UTF-8 prefix share
                    let pb = prev_s.as_bytes();
                    let cb = cur_s.as_bytes();
                    let shared = pb
                        .iter()
                        .zip(cb.iter())
                        .take_while(|(a, b)| a == b)
                        .count();
                    raw.push(11);
                    write_uvarint(shared as u64, &mut raw);
                    let suffix = &cb[shared..];
                    write_uvarint(suffix.len() as u64, &mut raw);
                    raw.extend_from_slice(suffix);
                }
                _ => {
                    v.write_binary(&mut raw);
                }
            }
            prev_vals[c] = Some(v.clone());
        }
        write_uvarint(*pid, &mut raw);
        write_uvarint(*slot as u64, &mut raw);
    }

    let raw_len = raw.len() as u32;
    let (algo, stored) = compress_bytes(&raw, profile)?;
    let crc = crc32fast::hash(&stored);
    Ok((stored, raw_len, algo, crc))
}

pub fn decode_btree_block(
    stored: &[u8],
    algo: CompressionAlgo,
    raw_len: u32,
    expected_crc32: u32,
) -> Result<Vec<BTreeLeafEntry>> {
    let actual_crc = crc32fast::hash(stored);
    if actual_crc != expected_crc32 {
        return Err(Error::Corruption(format!(
            "B-tree index block CRC32 mismatch: expected {:08x}, got {:08x}",
            expected_crc32, actual_crc
        )));
    }
    let raw = decompress_bytes(stored, algo, raw_len as usize)?;
    let mut pos = 0usize;
    let len = read_uvarint(&raw, &mut pos)? as usize;
    let key_cols = read_uvarint(&raw, &mut pos)? as usize;
    let mut entries = Vec::with_capacity(len);
    let mut prev_vals: Vec<Option<Value>> = vec![None; key_cols];

    for _ in 0..len {
        let mut key = Vec::with_capacity(key_cols);
        for c in 0..key_cols {
            if pos >= raw.len() {
                return Err(Error::Corruption("Unexpected EOF in B-tree block".into()));
            }
            let tag = raw[pos];
            let val = match tag {
                10 => {
                    pos += 1;
                    let delta = read_zigzag_varint(&raw, &mut pos)?;
                    match prev_vals[c] {
                        Some(Value::Integer(prev_i)) => Value::Integer(prev_i.wrapping_add(delta)),
                        _ => {
                            return Err(Error::Corruption(
                                "Invalid delta integer in B-tree block".into(),
                            ))
                        }
                    }
                }
                11 => {
                    pos += 1;
                    let shared = read_uvarint(&raw, &mut pos)? as usize;
                    let suff_len = read_uvarint(&raw, &mut pos)? as usize;
                    let suff = read_slice(&raw, &mut pos, suff_len)?;
                    match &prev_vals[c] {
                        Some(Value::Text(prev_s)) if shared <= prev_s.len() => {
                            let mut bytes = Vec::with_capacity(shared + suff_len);
                            bytes.extend_from_slice(&prev_s.as_bytes()[..shared]);
                            bytes.extend_from_slice(suff);
                            let s = String::from_utf8(bytes).map_err(|e| {
                                Error::Corruption(format!("Invalid UTF-8 in B-tree block: {}", e))
                            })?;
                            Value::Text(s)
                        }
                        _ => {
                            return Err(Error::Corruption(
                                "Invalid front-coded string in B-tree block".into(),
                            ))
                        }
                    }
                }
                _ => Value::read_binary(&raw, &mut pos)?,
            };
            prev_vals[c] = Some(val.clone());
            key.push(val);
        }
        let pid = read_uvarint(&raw, &mut pos)?;
        let slot = read_uvarint(&raw, &mut pos)? as u32;
        entries.push((key, pid, slot));
    }
    Ok(entries)
}

/// Bounded LRU cache for demand-paged on-disk index leaf blocks (both Hash and B-Tree).
pub struct IndexBlockCache {
    max_blocks: usize,
    entries: HashMap<u64, Vec<(u64, u64, u32)>>,
    order: VecDeque<u64>,
    btree_entries: HashMap<u64, Vec<BTreeLeafEntry>>,
    btree_order: VecDeque<u64>,
    pub hits: u64,
    pub misses: u64,
}

impl IndexBlockCache {
    pub fn new(max_blocks: usize) -> Self {
        IndexBlockCache {
            max_blocks: max_blocks.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
            btree_entries: HashMap::new(),
            btree_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, offset: u64) -> Option<&[(u64, u64, u32)]> {
        if self.entries.contains_key(&offset) {
            self.hits += 1;
            if let Some(pos) = self.order.iter().position(|&id| id == offset) {
                self.order.remove(pos);
            }
            self.order.push_back(offset);
            return self.entries.get(&offset).map(|v| v.as_slice());
        }
        self.misses += 1;
        None
    }

    pub fn insert(&mut self, offset: u64, block: Vec<(u64, u64, u32)>) {
        if self.entries.contains_key(&offset) {
            if let Some(pos) = self.order.iter().position(|&id| id == offset) {
                self.order.remove(pos);
            }
        } else if self.entries.len() >= self.max_blocks {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        self.entries.insert(offset, block);
        self.order.push_back(offset);
    }

    pub fn get_btree(&mut self, offset: u64) -> Option<&[BTreeLeafEntry]> {
        if self.btree_entries.contains_key(&offset) {
            self.hits += 1;
            if let Some(pos) = self.btree_order.iter().position(|&id| id == offset) {
                self.btree_order.remove(pos);
            }
            self.btree_order.push_back(offset);
            return self.btree_entries.get(&offset).map(|v| v.as_slice());
        }
        self.misses += 1;
        None
    }

    pub fn insert_btree(&mut self, offset: u64, block: Vec<BTreeLeafEntry>) {
        if self.btree_entries.contains_key(&offset) {
            if let Some(pos) = self.btree_order.iter().position(|&id| id == offset) {
                self.btree_order.remove(pos);
            }
        } else if self.btree_entries.len() >= self.max_blocks {
            if let Some(evicted) = self.btree_order.pop_front() {
                self.btree_entries.remove(&evicted);
            }
        }
        self.btree_entries.insert(offset, block);
        self.btree_order.push_back(offset);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.btree_entries.clear();
        self.btree_order.clear();
    }
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub name: String,
    pub table_name: String,
    /// First indexed column (kept for backward compatibility with single-column callers).
    pub column_name: String,
    /// Ordered list of indexed columns (1 for single-column, >=2 for composite indexes).
    pub columns: Vec<String>,
    /// Per-column descending flags (`false` = ASC, `true` = DESC).
    pub desc: Vec<bool>,
    pub kind: IndexKind,
    pub unique: bool,
    /// Sparse directory of sorted compressed on-disk hash leaf blocks.
    pub blocks: Vec<IndexBlockMeta>,
    /// Unflushed in-memory hash delta entries.
    pub pending_entries: Vec<(u64, u64, u32)>,
    /// Sparse directory of sorted compressed on-disk B-tree leaf blocks.
    pub btree_blocks: Vec<BTreeBlockMeta>,
    /// Unflushed in-memory sorted B-tree entries.
    pub pending_btree: Vec<BTreeLeafEntry>,
    /// Page IDs removed since last index block compaction.
    pub removed_pages: HashSet<u64>,
}

impl IndexEntry {
    pub fn new(name: String, table_name: String, column_name: String) -> Self {
        Self::new_composite(
            name,
            table_name,
            vec![column_name],
            vec![false],
            IndexKind::BTree,
            false,
        )
    }

    pub fn new_composite(
        name: String,
        table_name: String,
        columns: Vec<String>,
        desc: Vec<bool>,
        kind: IndexKind,
        unique: bool,
    ) -> Self {
        let first_col = columns.first().cloned().unwrap_or_default();
        let norm_desc = if desc.len() == columns.len() {
            desc
        } else {
            vec![false; columns.len()]
        };
        IndexEntry {
            name,
            table_name,
            column_name: first_col,
            columns,
            desc: norm_desc,
            kind,
            unique,
            blocks: Vec::new(),
            pending_entries: Vec::new(),
            btree_blocks: Vec::new(),
            pending_btree: Vec::new(),
            removed_pages: HashSet::new(),
        }
    }

    pub fn is_dirty(&self) -> bool {
        !self.pending_entries.is_empty()
            || !self.pending_btree.is_empty()
            || !self.removed_pages.is_empty()
    }

    pub fn index_page_tuples(&mut self, page_id: u64, key_tuples: Vec<Vec<Value>>) {
        match self.kind {
            IndexKind::Hash => {
                for (slot, vals) in key_tuples.iter().enumerate() {
                    if let Some(h) = hash_key_tuple(vals) {
                        self.pending_entries.push((h, page_id, slot as u32));
                    }
                }
                self.pending_entries.sort_unstable();
                self.pending_entries.dedup();
            }
            IndexKind::BTree => {
                for (slot, vals) in key_tuples.into_iter().enumerate() {
                    self.pending_btree.push((vals, page_id, slot as u32));
                }
                let desc = self.desc.clone();
                self.pending_btree.sort_by(|a, b| {
                    cmp_key_tuple(&a.0, &b.0, &desc)
                        .then_with(|| a.1.cmp(&b.1))
                        .then_with(|| a.2.cmp(&b.2))
                });
            }
        }
    }

    pub fn index_page_values(&mut self, page_id: u64, values: &[Value]) {
        let tuples: Vec<Vec<Value>> = values.iter().map(|v| vec![v.clone()]).collect();
        self.index_page_tuples(page_id, tuples);
    }

    pub fn remove_pages(&mut self, removed: &HashSet<u64>) {
        for &pid in removed {
            self.removed_pages.insert(pid);
        }
        if !self.pending_entries.is_empty() {
            self.pending_entries
                .retain(|(_, pid, _)| !removed.contains(pid));
        }
        if !self.pending_btree.is_empty() {
            self.pending_btree
                .retain(|(_, pid, _)| !removed.contains(pid));
        }
    }

    pub fn total_disk_bytes(&self) -> u64 {
        let hash_bytes: u64 = self.blocks.iter().map(|b| b.stored_len as u64).sum();
        let btree_bytes: u64 = self.btree_blocks.iter().map(|b| b.stored_len as u64).sum();
        hash_bytes + btree_bytes
    }

    /// Finds which on-disk hash leaf block indices could contain `target_hash`.
    pub fn candidate_blocks(&self, target_hash: u64) -> Vec<usize> {
        if self.blocks.is_empty() {
            return Vec::new();
        }
        let start = self.blocks.partition_point(|b| b.max_hash < target_hash);
        let mut out = Vec::new();
        let mut i = start;
        while i < self.blocks.len() && self.blocks[i].min_hash <= target_hash {
            out.push(i);
            i += 1;
        }
        out
    }

    /// Finds which on-disk B-tree leaf blocks overlap `spec` via O(log B) binary search.
    pub fn candidate_btree_blocks(&self, spec: &BTreeScanSpec) -> Vec<usize> {
        if self.btree_blocks.is_empty() {
            return Vec::new();
        }
        // First block whose max_key is not strictly less than spec lower bound
        let start = self
            .btree_blocks
            .partition_point(|b| cmp_key_vs_spec_lo(&b.max_key, spec, &self.desc) == Ordering::Less);
        let mut out = Vec::new();
        let mut i = start;
        while i < self.btree_blocks.len() {
            if cmp_key_vs_spec_hi(&self.btree_blocks[i].min_key, spec, &self.desc)
                == Ordering::Greater
            {
                break;
            }
            out.push(i);
            i += 1;
        }
        out
    }

    pub fn search_sorted_slice(
        slice: &[(u64, u64, u32)],
        target_hash: u64,
        removed: &HashSet<u64>,
        by_page: &mut HashMap<u64, Vec<u32>>,
    ) {
        let start = slice.partition_point(|&(h, _, _)| h < target_hash);
        let mut i = start;
        while i < slice.len() && slice[i].0 == target_hash {
            let (_, pid, slot) = slice[i];
            if !removed.contains(&pid) {
                by_page.entry(pid).or_default().push(slot);
            }
            i += 1;
        }
    }

    pub fn search_btree_slice(
        slice: &[BTreeLeafEntry],
        spec: &BTreeScanSpec,
        desc: &[bool],
        removed: &HashSet<u64>,
        by_page: &mut HashMap<u64, Vec<u32>>,
    ) {
        let start =
            slice.partition_point(|(k, _, _)| cmp_key_vs_spec_lo(k, spec, desc) == Ordering::Less);
        let mut i = start;
        while i < slice.len() {
            let (ref k, pid, slot) = slice[i];
            if cmp_key_vs_spec_hi(k, spec, desc) == Ordering::Greater {
                break;
            }
            if !removed.contains(&pid) && key_matches_spec(k, spec, desc) {
                by_page.entry(pid).or_default().push(slot);
            }
            i += 1;
        }
    }

    /// v4 legacy binary writer (kept for migration tests).
    pub fn write_binary_v4(&self, out: &mut Vec<u8>) {
        write_str(&self.name, out);
        write_str(&self.table_name, out);
        write_str(&self.column_name, out);
        write_uvarint(self.blocks.len() as u64, out);
        for b in &self.blocks {
            b.write_binary(out);
        }
    }

    /// v4 legacy binary reader (upgrades v4 single-column hash indexes cleanly).
    pub fn read_binary_v4(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let name = read_str(buf, pos)?;
        let table_name = read_str(buf, pos)?;
        let column_name = read_str(buf, pos)?;
        let blen = read_uvarint(buf, pos)? as usize;
        let mut blocks = Vec::with_capacity(blen);
        for _ in 0..blen {
            blocks.push(IndexBlockMeta::read_binary(buf, pos)?);
        }
        Ok(IndexEntry {
            name,
            table_name,
            column_name: column_name.clone(),
            columns: vec![column_name],
            desc: vec![false],
            kind: IndexKind::Hash,
            unique: false,
            blocks,
            pending_entries: Vec::new(),
            btree_blocks: Vec::new(),
            pending_btree: Vec::new(),
            removed_pages: HashSet::new(),
        })
    }

    /// v5 binary writer supporting composite multi-column + B-Tree & Hash indexes.
    pub fn write_binary(&self, out: &mut Vec<u8>) {
        write_str(&self.name, out);
        write_str(&self.table_name, out);
        out.push(self.kind as u8);
        out.push(self.unique as u8);
        write_uvarint(self.columns.len() as u64, out);
        for (i, c) in self.columns.iter().enumerate() {
            write_str(c, out);
            out.push(self.desc.get(i).copied().unwrap_or(false) as u8);
        }
        write_uvarint(self.blocks.len() as u64, out);
        for b in &self.blocks {
            b.write_binary(out);
        }
        write_uvarint(self.btree_blocks.len() as u64, out);
        for b in &self.btree_blocks {
            b.write_binary(out);
        }
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let name = read_str(buf, pos)?;
        let table_name = read_str(buf, pos)?;
        let kind = IndexKind::from_u8(read_u8(buf, pos)?)?;
        let unique = read_u8(buf, pos)? != 0;
        let clen = read_uvarint(buf, pos)? as usize;
        let mut columns = Vec::with_capacity(clen);
        let mut desc = Vec::with_capacity(clen);
        for _ in 0..clen {
            columns.push(read_str(buf, pos)?);
            desc.push(read_u8(buf, pos)? != 0);
        }
        let blen = read_uvarint(buf, pos)? as usize;
        let mut blocks = Vec::with_capacity(blen);
        for _ in 0..blen {
            blocks.push(IndexBlockMeta::read_binary(buf, pos)?);
        }
        let btlen = read_uvarint(buf, pos)? as usize;
        let mut btree_blocks = Vec::with_capacity(btlen);
        for _ in 0..btlen {
            btree_blocks.push(BTreeBlockMeta::read_binary(buf, pos)?);
        }
        let column_name = columns.first().cloned().unwrap_or_default();
        Ok(IndexEntry {
            name,
            table_name,
            column_name,
            columns,
            desc,
            kind,
            unique,
            blocks,
            pending_entries: Vec::new(),
            btree_blocks,
            pending_btree: Vec::new(),
            removed_pages: HashSet::new(),
        })
    }
}

use std::collections::{HashMap, HashSet, VecDeque};

use crate::pax::{compress_bytes, decompress_bytes, CompressionAlgo, CompressionProfile};
use crate::types::{
    read_exact, read_slice, read_str, read_u8, read_uvarint, write_str, write_uvarint, Error,
    Result, Value,
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
// Paged On-Disk SSTable Secondary Index
// Sparse block directory lives in Catalog (~32B per 4,096 keys!).
// Sorted leaf blocks live compressed on disk and are demand-paged into an LRU cache!
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

/// Bounded LRU cache for demand-paged on-disk index leaf blocks (keyed by file offset).
pub struct IndexBlockCache {
    max_blocks: usize,
    entries: HashMap<u64, Vec<(u64, u64, u32)>>,
    order: VecDeque<u64>,
    pub hits: u64,
    pub misses: u64,
}

impl IndexBlockCache {
    pub fn new(max_blocks: usize) -> Self {
        IndexBlockCache {
            max_blocks: max_blocks.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
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

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    /// Sparse directory of sorted compressed on-disk leaf blocks (tiny footprint in Catalog!).
    pub blocks: Vec<IndexBlockMeta>,
    /// Unflushed in-memory delta entries (merged into sorted on-disk blocks on checkpoint/vacuum).
    pub pending_entries: Vec<(u64, u64, u32)>,
    /// Page IDs removed since last index block compaction.
    pub removed_pages: HashSet<u64>,
}

impl IndexEntry {
    pub fn new(name: String, table_name: String, column_name: String) -> Self {
        IndexEntry {
            name,
            table_name,
            column_name,
            blocks: Vec::new(),
            pending_entries: Vec::new(),
            removed_pages: HashSet::new(),
        }
    }

    pub fn is_dirty(&self) -> bool {
        !self.pending_entries.is_empty() || !self.removed_pages.is_empty()
    }

    pub fn index_page_values(&mut self, page_id: u64, values: &[Value]) {
        for (slot, v) in values.iter().enumerate() {
            if let Some(h) = hash_value(v) {
                self.pending_entries.push((h, page_id, slot as u32));
            }
        }
        self.pending_entries.sort_unstable();
        self.pending_entries.dedup();
    }

    pub fn remove_pages(&mut self, removed: &HashSet<u64>) {
        for &pid in removed {
            self.removed_pages.insert(pid);
        }
        if !self.pending_entries.is_empty() {
            self.pending_entries
                .retain(|(_, pid, _)| !removed.contains(pid));
        }
    }

    pub fn total_disk_bytes(&self) -> u64 {
        self.blocks.iter().map(|b| b.stored_len as u64).sum()
    }

    /// Finds which on-disk leaf block indices could contain `target_hash`.
    /// O(log B) binary search over the sparse directory!
    pub fn candidate_blocks(&self, target_hash: u64) -> Vec<usize> {
        if self.blocks.is_empty() {
            return Vec::new();
        }
        // Blocks are sorted by min_hash; find first block with max_hash >= target_hash
        let start = self.blocks.partition_point(|b| b.max_hash < target_hash);
        let mut out = Vec::new();
        let mut i = start;
        while i < self.blocks.len() && self.blocks[i].min_hash <= target_hash {
            out.push(i);
            i += 1;
        }
        out
    }

    /// Searches a decoded leaf block + pending_entries for `target_hash`.
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

    pub fn write_binary(&self, out: &mut Vec<u8>) {
        write_str(&self.name, out);
        write_str(&self.table_name, out);
        write_str(&self.column_name, out);
        write_uvarint(self.blocks.len() as u64, out);
        for b in &self.blocks {
            b.write_binary(out);
        }
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
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
            column_name,
            blocks,
            pending_entries: Vec::new(),
            removed_pages: HashSet::new(),
        })
    }
}

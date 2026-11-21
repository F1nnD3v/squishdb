use std::io::{Read, Write};

use brotli::enc::BrotliEncoderParams;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression as FlateLevel;

use crate::encoding::{
    decode_column, decode_column_vector, decode_i64_stream, encode_column, encode_i64_stream,
};
use crate::index::BloomFilter;
use crate::types::{
    read_exact, read_u8, read_uvarint, write_uvarint, ColumnType, Error, Result, Row, TableSchema,
    Value,
};
use crate::vector::ColumnVector;

/// Default rows per PAX page: balances strong intra-column compression with fast random access.
pub const DEFAULT_PAGE_ROWS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgo {
    None = 0,
    Deflate = 1,
    Brotli = 2,
}

impl CompressionAlgo {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(CompressionAlgo::None),
            1 => Ok(CompressionAlgo::Deflate),
            2 => Ok(CompressionAlgo::Brotli),
            _ => Err(Error::Corruption(format!(
                "Unknown compression algorithm tag: {}",
                v
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionProfile {
    /// Fastest writes/reads (Deflate level 4)
    Fast,
    /// Default: Brotli quality 4 — strong compression, fast decompression
    Balanced,
    /// Maximum space savings: Brotli quality 6
    Max,
    /// Disable secondary block compression (column codecs only)
    None,
}

impl Default for CompressionProfile {
    fn default() -> Self {
        CompressionProfile::Balanced
    }
}

#[derive(Debug, Clone)]
pub struct ColumnZoneMap {
    pub null_count: u32,
    pub min_val: Option<Value>,
    pub max_val: Option<Value>,
}

impl ColumnZoneMap {
    pub fn from_values(col_type: ColumnType, values: &[Value]) -> Self {
        let mut null_count = 0u32;
        let mut min_v: Option<Value> = None;
        let mut max_v: Option<Value> = None;

        let store_bounds = matches!(
            col_type,
            ColumnType::Integer | ColumnType::Real | ColumnType::Text
        );

        for v in values {
            if v.is_null() {
                null_count += 1;
                continue;
            }
            if !store_bounds {
                continue;
            }
            if let Value::Text(s) = v {
                if s.len() > 64 {
                    return ColumnZoneMap {
                        null_count: values.iter().filter(|x| x.is_null()).count() as u32,
                        min_val: None,
                        max_val: None,
                    };
                }
            }
            if let Value::Real(r) = v {
                if r.is_nan() {
                    return ColumnZoneMap {
                        null_count: values.iter().filter(|x| x.is_null()).count() as u32,
                        min_val: None,
                        max_val: None,
                    };
                }
            }
            match &min_v {
                None => min_v = Some(v.clone()),
                Some(cur) if v.sql_cmp(cur).is_lt() => min_v = Some(v.clone()),
                _ => {}
            }
            match &max_v {
                None => max_v = Some(v.clone()),
                Some(cur) if v.sql_cmp(cur).is_gt() => max_v = Some(v.clone()),
                _ => {}
            }
        }

        ColumnZoneMap {
            null_count,
            min_val: min_v,
            max_val: max_v,
        }
    }

    pub fn write_binary(&self, out: &mut Vec<u8>) {
        write_uvarint(self.null_count as u64, out);
        match (&self.min_val, &self.max_val) {
            (Some(lo), Some(hi)) => {
                out.push(1);
                lo.write_binary(out);
                hi.write_binary(out);
            }
            _ => out.push(0),
        }
    }

    pub fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let null_count = read_uvarint(buf, pos)? as u32;
        let has_bounds = read_u8(buf, pos)?;
        let (min_val, max_val) = if has_bounds == 1 {
            (
                Some(Value::read_binary(buf, pos)?),
                Some(Value::read_binary(buf, pos)?),
            )
        } else {
            (None, None)
        };
        Ok(ColumnZoneMap {
            null_count,
            min_val,
            max_val,
        })
    }
}

/// Metadata for a single independently-compressed column chunk inside a PAX-v4 page.
#[derive(Debug, Clone, Copy)]
pub struct ColumnChunkMeta {
    pub algo: CompressionAlgo,
    pub raw_len: u32,
    pub stored_len: u32,
    pub crc32: u32,
    /// Relative byte offset from the start of the chunk payload section.
    pub rel_offset: u32,
}

#[derive(Debug, Clone)]
pub struct PaxHeader {
    pub row_count: usize,
    pub stored_col_count: usize,
    pub rowids_chunk: ColumnChunkMeta,
    pub col_chunks: Vec<ColumnChunkMeta>,
}

impl PaxHeader {
    pub fn write_bytes(&self, out: &mut Vec<u8>) {
        write_uvarint(self.row_count as u64, out);
        write_uvarint(self.stored_col_count as u64, out);
        write_chunk_meta(&self.rowids_chunk, out);
        for cm in &self.col_chunks {
            write_chunk_meta(cm, out);
        }
    }

    pub fn parse_bytes(buf: &[u8]) -> Result<Self> {
        let mut pos = 0usize;
        let row_count = read_uvarint(buf, &mut pos)? as usize;
        let stored_col_count = read_uvarint(buf, &mut pos)? as usize;
        let mut rel = 0u32;
        let rowids_chunk = read_chunk_meta(buf, &mut pos, &mut rel)?;
        let mut col_chunks = Vec::with_capacity(stored_col_count);
        for _ in 0..stored_col_count {
            col_chunks.push(read_chunk_meta(buf, &mut pos, &mut rel)?);
        }
        Ok(PaxHeader {
            row_count,
            stored_col_count,
            rowids_chunk,
            col_chunks,
        })
    }
}

fn write_chunk_meta(cm: &ColumnChunkMeta, out: &mut Vec<u8>) {
    out.push(cm.algo as u8);
    write_uvarint(cm.raw_len as u64, out);
    write_uvarint(cm.stored_len as u64, out);
    out.extend_from_slice(&cm.crc32.to_le_bytes());
}

fn read_chunk_meta(buf: &[u8], pos: &mut usize, rel_acc: &mut u32) -> Result<ColumnChunkMeta> {
    let algo = CompressionAlgo::from_u8(read_u8(buf, pos)?)?;
    let raw_len = read_uvarint(buf, pos)? as u32;
    let stored_len = read_uvarint(buf, pos)? as u32;
    let crc32 = u32::from_le_bytes(read_exact::<4>(buf, pos)?);
    let rel_offset = *rel_acc;
    *rel_acc = rel_acc.saturating_add(stored_len);
    Ok(ColumnChunkMeta {
        algo,
        raw_len,
        stored_len,
        crc32,
        rel_offset,
    })
}

pub struct EncodedPaxPage {
    pub stored_bytes: Vec<u8>,
    pub raw_pax_len: u32,
    pub compression: CompressionAlgo,
    pub crc32: u32,
    pub row_count: u32,
    pub min_rowid: i64,
    pub max_rowid: i64,
    pub zone_maps: Vec<ColumnZoneMap>,
    pub blooms: Vec<Option<BloomFilter>>,
}

fn is_monotonic(values: &[Value]) -> bool {
    let mut non_dec = true;
    let mut non_inc = true;
    let mut prev: Option<&Value> = None;
    for v in values {
        if v.is_null() {
            continue;
        }
        if let Some(p) = prev {
            let ord = v.sql_cmp(p);
            if ord.is_lt() {
                non_dec = false;
            }
            if ord.is_gt() {
                non_inc = false;
            }
            if !non_dec && !non_inc {
                return false;
            }
        }
        prev = Some(v);
    }
    true
}

pub fn compress_bytes(raw: &[u8], profile: CompressionProfile) -> Result<(CompressionAlgo, Vec<u8>)> {
    if raw.len() < 32 || matches!(profile, CompressionProfile::None) {
        return Ok((CompressionAlgo::None, raw.to_vec()));
    }

    let (algo, compressed) = match profile {
        CompressionProfile::None => return Ok((CompressionAlgo::None, raw.to_vec())),
        CompressionProfile::Fast => {
            let mut enc = DeflateEncoder::new(Vec::with_capacity(raw.len() / 2), FlateLevel::new(4));
            enc.write_all(raw)?;
            (CompressionAlgo::Deflate, enc.finish()?)
        }
        CompressionProfile::Balanced => {
            let mut out = Vec::with_capacity(raw.len() / 2);
            let mut params = BrotliEncoderParams::default();
            params.quality = 4;
            params.lgwin = 20;
            brotli::BrotliCompress(&mut std::io::Cursor::new(raw), &mut out, &params)?;
            (CompressionAlgo::Brotli, out)
        }
        CompressionProfile::Max => {
            let mut out = Vec::with_capacity(raw.len() / 2);
            let mut params = BrotliEncoderParams::default();
            params.quality = 6;
            params.lgwin = 22;
            brotli::BrotliCompress(&mut std::io::Cursor::new(raw), &mut out, &params)?;
            (CompressionAlgo::Brotli, out)
        }
    };

    if compressed.len() < raw.len() {
        Ok((algo, compressed))
    } else {
        Ok((CompressionAlgo::None, raw.to_vec()))
    }
}

pub fn decompress_bytes(
    data: &[u8],
    algo: CompressionAlgo,
    expected_len: usize,
) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Deflate => {
            let mut dec = DeflateDecoder::new(data);
            let mut out = Vec::with_capacity(expected_len);
            dec.read_to_end(&mut out)?;
            Ok(out)
        }
        CompressionAlgo::Brotli => {
            let mut out = Vec::with_capacity(expected_len);
            brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut out)?;
            Ok(out)
        }
    }
}

/// Encodes rows into a PAX-v4 page with **independently compressed column chunks**,
/// enabling true disk-level columnar projection (skip untouched columns with zero decompression!).
pub fn encode_pax_page(
    schema: &TableSchema,
    rows: &[Row],
    profile: CompressionProfile,
) -> Result<EncodedPaxPage> {
    if rows.is_empty() {
        return Err(Error::InvalidData("Cannot encode empty PAX page".into()));
    }
    let row_count = rows.len();
    let num_cols = schema.columns.len();

    // 1. Encode & compress rowids chunk
    let mut rowids = Vec::with_capacity(row_count);
    let mut min_rowid = i64::MAX;
    let mut max_rowid = i64::MIN;
    for r in rows {
        rowids.push(r.rowid);
        if r.rowid < min_rowid {
            min_rowid = r.rowid;
        }
        if r.rowid > max_rowid {
            max_rowid = r.rowid;
        }
    }
    let mut raw_rowids = Vec::new();
    encode_i64_stream(&rowids, &mut raw_rowids);
    let (r_algo, stored_rowids) = compress_bytes(&raw_rowids, profile)?;
    let rowids_chunk = ColumnChunkMeta {
        algo: r_algo,
        raw_len: raw_rowids.len() as u32,
        stored_len: stored_rowids.len() as u32,
        crc32: crc32fast::hash(&stored_rowids),
        rel_offset: 0,
    };

    let mut raw_pax_total: u32 = raw_rowids.len() as u32;
    let mut rel_off = stored_rowids.len() as u32;
    let mut payload_chunks: Vec<Vec<u8>> = Vec::with_capacity(num_cols + 1);
    payload_chunks.push(stored_rowids);

    // 2. Encode & independently compress each column chunk
    let mut zone_maps = Vec::with_capacity(num_cols);
    let mut blooms = Vec::with_capacity(num_cols);
    let mut col_chunks = Vec::with_capacity(num_cols);
    let mut col_buf: Vec<Value> = Vec::with_capacity(row_count);
    let int_pk_col = schema.integer_pk_col();

    for (c_idx, col_def) in schema.columns.iter().enumerate() {
        col_buf.clear();
        for r in rows {
            col_buf.push(r.values[c_idx].clone());
        }
        zone_maps.push(ColumnZoneMap::from_values(col_def.col_type, &col_buf));

        let bloom = if !col_def.primary_key
            && matches!(col_def.col_type, ColumnType::Integer | ColumnType::Text)
            && !is_monotonic(&col_buf)
        {
            BloomFilter::build_for_column(&col_buf)
        } else {
            None
        };
        blooms.push(bloom);

        if Some(c_idx) == int_pk_col {
            col_chunks.push(ColumnChunkMeta {
                algo: CompressionAlgo::None,
                raw_len: 0,
                stored_len: 0,
                crc32: 0,
                rel_offset: rel_off,
            });
            payload_chunks.push(Vec::new());
            continue;
        }

        let mut raw_col = Vec::new();
        encode_column(col_def.col_type, &col_buf, &mut raw_col)?;
        raw_pax_total = raw_pax_total.saturating_add(raw_col.len() as u32);
        let (c_algo, stored_col) = compress_bytes(&raw_col, profile)?;
        let cm = ColumnChunkMeta {
            algo: c_algo,
            raw_len: raw_col.len() as u32,
            stored_len: stored_col.len() as u32,
            crc32: crc32fast::hash(&stored_col),
            rel_offset: rel_off,
        };
        rel_off = rel_off.saturating_add(cm.stored_len);
        col_chunks.push(cm);
        payload_chunks.push(stored_col);
    }

    // 3. Assemble PAX-v4 frame: [header_len: u32][header_crc32: u32][header_bytes][chunks...]
    let header = PaxHeader {
        row_count,
        stored_col_count: num_cols,
        rowids_chunk,
        col_chunks,
    };
    let mut header_bytes = Vec::new();
    header.write_bytes(&mut header_bytes);
    let header_crc = crc32fast::hash(&header_bytes);

    let total_payload: usize = payload_chunks.iter().map(|c| c.len()).sum();
    let mut stored_bytes = Vec::with_capacity(8 + header_bytes.len() + total_payload);
    stored_bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    stored_bytes.extend_from_slice(&header_crc.to_le_bytes());
    stored_bytes.extend_from_slice(&header_bytes);
    for ch in payload_chunks {
        stored_bytes.extend_from_slice(&ch);
    }

    let page_crc = crc32fast::hash(&stored_bytes);

    Ok(EncodedPaxPage {
        stored_bytes,
        raw_pax_len: raw_pax_total,
        compression: CompressionAlgo::None, // Per-column compression is recorded in PaxHeader
        crc32: page_crc,
        row_count: row_count as u32,
        min_rowid,
        max_rowid,
        zone_maps,
        blooms,
    })
}

/// Decodes a single compressed `rowids` chunk after CRC32 verification.
pub fn decode_rowids_chunk(chunk_bytes: &[u8], meta: &ColumnChunkMeta, row_count: usize) -> Result<Vec<i64>> {
    let actual_crc = crc32fast::hash(chunk_bytes);
    if actual_crc != meta.crc32 {
        return Err(Error::Corruption(format!(
            "PAX rowids chunk CRC32 mismatch: expected {:08x}, got {:08x}",
            meta.crc32, actual_crc
        )));
    }
    let raw = decompress_bytes(chunk_bytes, meta.algo, meta.raw_len as usize)?;
    let mut pos = 0usize;
    decode_i64_stream(&raw, &mut pos, row_count)
}

/// Decodes a single compressed user column chunk directly into a typed `ColumnVector`.
pub fn decode_column_chunk_vector(
    col_type: ColumnType,
    chunk_bytes: &[u8],
    meta: &ColumnChunkMeta,
    row_count: usize,
) -> Result<ColumnVector> {
    let actual_crc = crc32fast::hash(chunk_bytes);
    if actual_crc != meta.crc32 {
        return Err(Error::Corruption(format!(
            "PAX column chunk CRC32 mismatch: expected {:08x}, got {:08x}",
            meta.crc32, actual_crc
        )));
    }
    let raw = decompress_bytes(chunk_bytes, meta.algo, meta.raw_len as usize)?;
    let mut pos = 0usize;
    decode_column_vector(col_type, &raw, &mut pos, row_count)
}

/// Decodes a full PAX-v4 page buffer into rows (still only decompressing `needed_cols`!).
pub fn decode_pax_page(
    schema: &TableSchema,
    stored_bytes: &[u8],
    _algo: CompressionAlgo,
    _raw_pax_len: u32,
    expected_crc32: u32,
    needed_cols: Option<&[bool]>,
) -> Result<Vec<Row>> {
    let actual_crc = crc32fast::hash(stored_bytes);
    if actual_crc != expected_crc32 {
        return Err(Error::Corruption(format!(
            "PAX page CRC32 mismatch: expected {:08x}, got {:08x}",
            expected_crc32, actual_crc
        )));
    }
    if stored_bytes.len() < 8 {
        return Err(Error::Corruption("Truncated PAX-v4 page header".into()));
    }
    let hlen = u32::from_le_bytes(stored_bytes[0..4].try_into().unwrap()) as usize;
    let hcrc = u32::from_le_bytes(stored_bytes[4..8].try_into().unwrap());
    if 8 + hlen > stored_bytes.len() {
        return Err(Error::Corruption("PAX-v4 header length out of bounds".into()));
    }
    let hbytes = &stored_bytes[8..8 + hlen];
    if crc32fast::hash(hbytes) != hcrc {
        return Err(Error::Corruption("PAX-v4 header CRC32 mismatch".into()));
    }
    let header = PaxHeader::parse_bytes(hbytes)?;
    let data = &stored_bytes[8 + hlen..];

    let rc = &header.rowids_chunk;
    let r_end = (rc.rel_offset + rc.stored_len) as usize;
    if r_end > data.len() {
        return Err(Error::Corruption("PAX-v4 rowids chunk out of bounds".into()));
    }
    let rowids = decode_rowids_chunk(&data[rc.rel_offset as usize..r_end], rc, header.row_count)?;

    let num_cols = schema.columns.len();
    let int_pk_col = schema.integer_pk_col();
    let mut decoded_cols: Vec<Option<Vec<Value>>> = vec![None; num_cols];

    for (c_idx, col_def) in schema.columns.iter().enumerate() {
        let want = needed_cols.map(|m| m[c_idx]).unwrap_or(true);
        if !want {
            continue;
        }
        if c_idx >= header.stored_col_count {
            let fill = col_def.default_value.clone().unwrap_or(Value::Null);
            decoded_cols[c_idx] = Some(vec![fill; header.row_count]);
            continue;
        }
        let cm = &header.col_chunks[c_idx];
        if cm.stored_len == 0 && Some(c_idx) == int_pk_col {
            decoded_cols[c_idx] = Some(rowids.iter().map(|&id| Value::Integer(id)).collect());
            continue;
        }
        let start = cm.rel_offset as usize;
        let end = start + cm.stored_len as usize;
        if end > data.len() {
            return Err(Error::Corruption("PAX-v4 column chunk out of bounds".into()));
        }
        let chunk = &data[start..end];
        if crc32fast::hash(chunk) != cm.crc32 {
            return Err(Error::Corruption("PAX-v4 column chunk CRC32 mismatch".into()));
        }
        let raw = decompress_bytes(chunk, cm.algo, cm.raw_len as usize)?;
        let mut cpos = 0usize;
        let vals = decode_column(col_def.col_type, &raw, &mut cpos, header.row_count)?;
        decoded_cols[c_idx] = Some(vals);
    }

    let mut rows = Vec::with_capacity(header.row_count);
    for r_idx in 0..header.row_count {
        let mut values = Vec::with_capacity(num_cols);
        for c_idx in 0..num_cols {
            if let Some(col_vec) = &mut decoded_cols[c_idx] {
                values.push(std::mem::replace(&mut col_vec[r_idx], Value::Null));
            } else {
                values.push(Value::Null);
            }
        }
        rows.push(Row {
            rowid: rowids[r_idx],
            values,
        });
    }

    Ok(rows)
}

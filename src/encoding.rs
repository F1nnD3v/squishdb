use std::collections::HashMap;

use crate::types::{
    read_exact, read_slice, read_u8, read_uvarint, read_zigzag_varint, write_uvarint,
    write_zigzag_varint, zigzag_decode, zigzag_encode, ColumnType, Error, Result, Value,
};
use crate::vector::ColumnVector;

// ============================================================================
// Validity (NULL) bitmap encoding
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ValidityMode {
    AllValid = 0,
    AllNull = 1,
    Bitmap = 2,
}

fn encode_validity(values: &[Value], out: &mut Vec<u8>) -> (usize, bool) {
    let null_count = values.iter().filter(|v| v.is_null()).count();
    if null_count == 0 {
        out.push(ValidityMode::AllValid as u8);
        return (0, false);
    }
    if null_count == values.len() {
        out.push(ValidityMode::AllNull as u8);
        return (null_count, true);
    }
    out.push(ValidityMode::Bitmap as u8);
    let byte_len = (values.len() + 7) / 8;
    let start = out.len();
    out.resize(start + byte_len, 0);
    for (i, v) in values.iter().enumerate() {
        if !v.is_null() {
            out[start + (i >> 3)] |= 1 << (i & 7);
        }
    }
    (null_count, false)
}

fn decode_validity(buf: &[u8], pos: &mut usize, row_count: usize) -> Result<(Vec<bool>, usize)> {
    let mode = read_u8(buf, pos)?;
    match mode {
        0 => Ok((vec![true; row_count], row_count)),
        1 => Ok((vec![false; row_count], 0)),
        2 => {
            let byte_len = (row_count + 7) / 8;
            let bytes = read_slice(buf, pos, byte_len)?;
            let mut valid = Vec::with_capacity(row_count);
            let mut non_null = 0usize;
            for i in 0..row_count {
                let is_valid = (bytes[i >> 3] & (1 << (i & 7))) != 0;
                if is_valid {
                    non_null += 1;
                }
                valid.push(is_valid);
            }
            Ok((valid, non_null))
        }
        _ => Err(Error::Corruption(format!("Invalid validity mode: {}", mode))),
    }
}

// ============================================================================
// Tight bit-packing helpers (LSB-first, arbitrary width 0..=64)
// ============================================================================

#[inline]
pub fn bits_needed(max_val: u64) -> u8 {
    if max_val == 0 {
        0
    } else {
        (64 - max_val.leading_zeros()) as u8
    }
}

pub fn bitpack_encode(values: &[u64], bit_width: u8, out: &mut Vec<u8>) {
    if bit_width == 0 || values.is_empty() {
        return;
    }
    if bit_width == 64 {
        for &v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        return;
    }
    let total_bits = values.len() * (bit_width as usize);
    let total_bytes = (total_bits + 7) / 8;
    out.reserve(total_bytes);

    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let bw = bit_width as u32;
    let mask: u128 = (1u128 << bw) - 1;

    for &v in values {
        acc |= ((v as u128) & mask) << acc_bits;
        acc_bits += bw;
        while acc_bits >= 8 {
            out.push((acc & 0xFF) as u8);
            acc >>= 8;
            acc_bits -= 8;
        }
    }
    if acc_bits > 0 {
        out.push((acc & 0xFF) as u8);
    }
}

pub fn bitpack_decode(buf: &[u8], pos: &mut usize, count: usize, bit_width: u8) -> Result<Vec<u64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if bit_width == 0 {
        return Ok(vec![0u64; count]);
    }
    if bit_width == 64 {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let b = read_exact::<8>(buf, pos)?;
            out.push(u64::from_le_bytes(b));
        }
        return Ok(out);
    }
    let bw = bit_width as u32;
    let total_bits = count * (bit_width as usize);
    let total_bytes = (total_bits + 7) / 8;
    let raw = read_slice(buf, pos, total_bytes)?;

    let mut out = Vec::with_capacity(count);
    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let mut byte_idx = 0usize;
    let mask: u128 = (1u128 << bw) - 1;

    for _ in 0..count {
        while acc_bits < bw && byte_idx < raw.len() {
            acc |= (raw[byte_idx] as u128) << acc_bits;
            acc_bits += 8;
            byte_idx += 1;
        }
        out.push((acc & mask) as u64);
        acc >>= bw;
        acc_bits -= bw;
    }
    Ok(out)
}

// ============================================================================
// Integer column codecs (100% lossless across full i64::MIN..=i64::MAX)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IntCodec {
    Constant = 0,
    ForBitpack = 1,
    DeltaBitpack = 2,
    DeltaVarint = 3,
    Rle = 4,
    Dictionary = 5,
    Raw = 6,
}

pub fn encode_i64_stream(vals: &[i64], out: &mut Vec<u8>) -> IntCodec {
    if vals.is_empty() {
        out.push(IntCodec::Raw as u8);
        return IntCodec::Raw;
    }

    // 1. Constant check
    let first = vals[0];
    if vals.iter().all(|&v| v == first) {
        out.push(IntCodec::Constant as u8);
        write_zigzag_varint(first, out);
        return IntCodec::Constant;
    }

    let mut best_codec = IntCodec::Raw;
    let mut best_buf: Vec<u8> = Vec::with_capacity(vals.len() * 8);
    for &v in vals {
        best_buf.extend_from_slice(&v.to_le_bytes());
    }

    // 2. FOR (Frame-of-Reference) + Bitpack
    {
        let mut min_v = i64::MAX;
        let mut max_v = i64::MIN;
        for &v in vals {
            if v < min_v {
                min_v = v;
            }
            if v > max_v {
                max_v = v;
            }
        }
        let span = (max_v as u64).wrapping_sub(min_v as u64);
        let bw = bits_needed(span);
        let est = 12 + (vals.len() * bw as usize + 7) / 8;
        if est < best_buf.len() {
            let mut cand = Vec::with_capacity(est);
            write_zigzag_varint(min_v, &mut cand);
            cand.push(bw);
            let offsets: Vec<u64> = vals
                .iter()
                .map(|&v| (v as u64).wrapping_sub(min_v as u64))
                .collect();
            bitpack_encode(&offsets, bw, &mut cand);
            if cand.len() < best_buf.len() {
                best_buf = cand;
                best_codec = IntCodec::ForBitpack;
            }
        }
    }

    // 3. Delta + ZigZag + Bitpack  AND  4. Delta + ZigZag + Varint
    if vals.len() >= 2 {
        let mut zdeltas = Vec::with_capacity(vals.len() - 1);
        let mut max_z = 0u64;
        for w in vals.windows(2) {
            let d = (w[1] as u64).wrapping_sub(w[0] as u64) as i64;
            let z = zigzag_encode(d);
            if z > max_z {
                max_z = z;
            }
            zdeltas.push(z);
        }

        // 3a. Delta + Bitpack
        let bw = bits_needed(max_z);
        let est = 12 + (zdeltas.len() * bw as usize + 7) / 8;
        if est < best_buf.len() {
            let mut cand = Vec::with_capacity(est);
            write_zigzag_varint(first, &mut cand);
            cand.push(bw);
            bitpack_encode(&zdeltas, bw, &mut cand);
            if cand.len() < best_buf.len() {
                best_buf = cand;
                best_codec = IntCodec::DeltaBitpack;
            }
        }

        // 3b. Delta + Varint (wins when occasional outliers inflate bit width)
        {
            let mut cand = Vec::with_capacity(vals.len() * 2);
            write_zigzag_varint(first, &mut cand);
            for &z in &zdeltas {
                write_uvarint(z, &mut cand);
                if cand.len() >= best_buf.len() {
                    break;
                }
            }
            if cand.len() < best_buf.len() {
                best_buf = cand;
                best_codec = IntCodec::DeltaVarint;
            }
        }
    }

    // 5. RLE (Run-Length Encoding)
    {
        let mut cand = Vec::new();
        let mut runs: Vec<(i64, u64)> = Vec::new();
        let mut cur = vals[0];
        let mut len = 1u64;
        for &v in &vals[1..] {
            if v == cur {
                len += 1;
            } else {
                runs.push((cur, len));
                cur = v;
                len = 1;
            }
        }
        runs.push((cur, len));
        if runs.len() * 2 < vals.len() {
            write_uvarint(runs.len() as u64, &mut cand);
            for &(v, r) in &runs {
                write_zigzag_varint(v, &mut cand);
                write_uvarint(r, &mut cand);
                if cand.len() >= best_buf.len() {
                    break;
                }
            }
            if cand.len() < best_buf.len() {
                best_buf = cand;
                best_codec = IntCodec::Rle;
            }
        }
    }

    // 6. Dictionary + Bitpack (great for scattered 64-bit IDs / foreign keys)
    if vals.len() >= 8 {
        let max_dict = (vals.len() / 2).min(4096);
        let mut map: HashMap<i64, u64> = HashMap::new();
        let mut dict: Vec<i64> = Vec::new();
        let mut indices: Vec<u64> = Vec::with_capacity(vals.len());
        let mut ok = true;
        for &v in vals {
            if let Some(&idx) = map.get(&v) {
                indices.push(idx);
            } else {
                if dict.len() >= max_dict {
                    ok = false;
                    break;
                }
                let idx = dict.len() as u64;
                map.insert(v, idx);
                dict.push(v);
                indices.push(idx);
            }
        }
        if ok && !dict.is_empty() {
            let bw = bits_needed((dict.len() - 1) as u64);
            let mut cand = Vec::new();
            write_uvarint(dict.len() as u64, &mut cand);
            for &d in &dict {
                write_zigzag_varint(d, &mut cand);
            }
            cand.push(bw);
            bitpack_encode(&indices, bw, &mut cand);
            if cand.len() < best_buf.len() {
                best_buf = cand;
                best_codec = IntCodec::Dictionary;
            }
        }
    }

    out.push(best_codec as u8);
    out.extend_from_slice(&best_buf);
    best_codec
}

pub fn decode_i64_stream(buf: &[u8], pos: &mut usize, count: usize) -> Result<Vec<i64>> {
    let tag = read_u8(buf, pos)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    match tag {
        0 => {
            // Constant
            let v = read_zigzag_varint(buf, pos)?;
            Ok(vec![v; count])
        }
        1 => {
            // ForBitpack
            let min_v = read_zigzag_varint(buf, pos)?;
            let bw = read_u8(buf, pos)?;
            let offsets = bitpack_decode(buf, pos, count, bw)?;
            Ok(offsets
                .into_iter()
                .map(|o| (min_v as u64).wrapping_add(o) as i64)
                .collect())
        }
        2 => {
            // DeltaBitpack
            let first = read_zigzag_varint(buf, pos)?;
            let bw = read_u8(buf, pos)?;
            let zdeltas = bitpack_decode(buf, pos, count - 1, bw)?;
            let mut out = Vec::with_capacity(count);
            out.push(first);
            let mut cur = first as u64;
            for z in zdeltas {
                let d = zigzag_decode(z);
                cur = cur.wrapping_add(d as u64);
                out.push(cur as i64);
            }
            Ok(out)
        }
        3 => {
            // DeltaVarint
            let first = read_zigzag_varint(buf, pos)?;
            let mut out = Vec::with_capacity(count);
            out.push(first);
            let mut cur = first as u64;
            for _ in 1..count {
                let z = read_uvarint(buf, pos)?;
                let d = zigzag_decode(z);
                cur = cur.wrapping_add(d as u64);
                out.push(cur as i64);
            }
            Ok(out)
        }
        4 => {
            // RLE
            let num_runs = read_uvarint(buf, pos)? as usize;
            let mut out = Vec::with_capacity(count);
            for _ in 0..num_runs {
                let v = read_zigzag_varint(buf, pos)?;
                let r = read_uvarint(buf, pos)? as usize;
                for _ in 0..r {
                    out.push(v);
                }
            }
            if out.len() != count {
                return Err(Error::Corruption(format!(
                    "RLE int decode count mismatch: expected {}, got {}",
                    count,
                    out.len()
                )));
            }
            Ok(out)
        }
        5 => {
            // Dictionary
            let dict_len = read_uvarint(buf, pos)? as usize;
            let mut dict = Vec::with_capacity(dict_len);
            for _ in 0..dict_len {
                dict.push(read_zigzag_varint(buf, pos)?);
            }
            let bw = read_u8(buf, pos)?;
            let indices = bitpack_decode(buf, pos, count, bw)?;
            let mut out = Vec::with_capacity(count);
            for idx in indices {
                let v = *dict.get(idx as usize).ok_or_else(|| {
                    Error::Corruption(format!("Int dictionary index out of bounds: {}", idx))
                })?;
                out.push(v);
            }
            Ok(out)
        }
        6 => {
            // Raw
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let b = read_exact::<8>(buf, pos)?;
                out.push(i64::from_le_bytes(b));
            }
            Ok(out)
        }
        _ => Err(Error::Corruption(format!("Unknown int codec tag: {}", tag))),
    }
}

// ============================================================================
// Real (f64) column codecs (100% IEEE-754 bit-exact lossless)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RealCodec {
    Constant = 0,
    ScaledDecimal = 1,
    ByteShuffle = 2,
    Raw = 3,
}

fn try_scaled_decimal(vals: &[f64]) -> Option<(u8, Vec<i64>)> {
    // Check powers of 10 from 10^0 to 10^6, verifying EXACT IEEE-754 bit round-trip!
    const SCALES: [(u8, f64); 7] = [
        (0, 1.0),
        (1, 10.0),
        (2, 100.0),
        (3, 1_000.0),
        (4, 10_000.0),
        (5, 100_000.0),
        (6, 1_000_000.0),
    ];
    for &(pow, scale) in &SCALES {
        let mut ints = Vec::with_capacity(vals.len());
        let mut all_exact = true;
        for &v in vals {
            if !v.is_finite() {
                all_exact = false;
                break;
            }
            let scaled_f = (v * scale).round();
            if scaled_f < (i64::MIN as f64) || scaled_f >= (i64::MAX as f64) {
                all_exact = false;
                break;
            }
            let iv = scaled_f as i64;
            let roundtrip = (iv as f64) / scale;
            if roundtrip.to_bits() != v.to_bits() {
                all_exact = false;
                break;
            }
            ints.push(iv);
        }
        if all_exact {
            return Some((pow, ints));
        }
    }
    None
}

fn encode_f64_stream(vals: &[f64], out: &mut Vec<u8>) {
    if vals.is_empty() {
        out.push(RealCodec::Raw as u8);
        return;
    }

    let first_bits = vals[0].to_bits();
    if vals.iter().all(|v| v.to_bits() == first_bits) {
        out.push(RealCodec::Constant as u8);
        out.extend_from_slice(&first_bits.to_le_bytes());
        return;
    }

    // Try exact decimal-scaled integer encoding (huge win for currency, percentages, sensor readings)
    if let Some((pow, ints)) = try_scaled_decimal(vals) {
        let mut cand = Vec::new();
        cand.push(pow);
        encode_i64_stream(&ints, &mut cand);
        if cand.len() < vals.len() * 8 {
            out.push(RealCodec::ScaledDecimal as u8);
            out.extend_from_slice(&cand);
            return;
        }
    }

    // Byte-shuffle (8-lane transpose): groups sign+exponent bytes together so Brotli/Deflate
    // crushes IEEE-754 doubles by 2x-4x even when mantissas vary!
    if vals.len() >= 4 {
        out.push(RealCodec::ByteShuffle as u8);
        let n = vals.len();
        let start = out.len();
        out.resize(start + n * 8, 0);
        for (i, v) in vals.iter().enumerate() {
            let b = v.to_bits().to_le_bytes();
            for lane in 0..8 {
                out[start + lane * n + i] = b[lane];
            }
        }
    } else {
        out.push(RealCodec::Raw as u8);
        for v in vals {
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
    }
}

fn decode_f64_stream(buf: &[u8], pos: &mut usize, count: usize) -> Result<Vec<f64>> {
    let tag = read_u8(buf, pos)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    match tag {
        0 => {
            let bits = u64::from_le_bytes(read_exact::<8>(buf, pos)?);
            Ok(vec![f64::from_bits(bits); count])
        }
        1 => {
            let pow = read_u8(buf, pos)?;
            let scale = 10f64.powi(pow as i32);
            let ints = decode_i64_stream(buf, pos, count)?;
            Ok(ints.into_iter().map(|iv| (iv as f64) / scale).collect())
        }
        2 => {
            let raw = read_slice(buf, pos, count * 8)?;
            let mut out = Vec::with_capacity(count);
            for i in 0..count {
                let mut b = [0u8; 8];
                for lane in 0..8 {
                    b[lane] = raw[lane * count + i];
                }
                out.push(f64::from_bits(u64::from_le_bytes(b)));
            }
            Ok(out)
        }
        3 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let bits = u64::from_le_bytes(read_exact::<8>(buf, pos)?);
                out.push(f64::from_bits(bits));
            }
            Ok(out)
        }
        _ => Err(Error::Corruption(format!("Unknown real codec tag: {}", tag))),
    }
}

// ============================================================================
// Text & Blob column codecs (Constant, Front-Coded Dictionary, Split-Length Raw)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BytesCodec {
    Constant = 0,
    DictionaryFrontCoded = 1,
    RawSplit = 2,
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

fn encode_bytes_stream<'a>(vals: &[&'a [u8]], out: &mut Vec<u8>) {
    if vals.is_empty() {
        out.push(BytesCodec::RawSplit as u8);
        return;
    }

    // 1. Constant
    let first = vals[0];
    if vals.iter().all(|&v| v == first) {
        out.push(BytesCodec::Constant as u8);
        write_uvarint(first.len() as u64, out);
        out.extend_from_slice(first);
        return;
    }

    // Build RawSplit candidate (separate length stream + concatenated payload)
    let mut raw_cand = Vec::new();
    let lengths: Vec<i64> = vals.iter().map(|v| v.len() as i64).collect();
    encode_i64_stream(&lengths, &mut raw_cand);
    for &v in vals {
        raw_cand.extend_from_slice(v);
    }

    // 2. Try sorted front-coded Dictionary + bitpacked (or int-encoded) indices
    // Works great when there are duplicates (low/medium cardinality columns like status,
    // country, category, service, domain, user-agent, repeated tags).
    if vals.len() >= 4 {
        let max_dict = (vals.len() * 3 / 4).min(4096);
        let mut uniq_map: HashMap<&'a [u8], usize> = HashMap::new();
        let mut ok = true;
        for &v in vals {
            if !uniq_map.contains_key(v) {
                if uniq_map.len() >= max_dict {
                    ok = false;
                    break;
                }
                let idx = uniq_map.len();
                uniq_map.insert(v, idx);
            }
        }
        if ok && uniq_map.len() < vals.len() {
            // Sort dictionary lexicographically so front-coding maximizes shared prefixes!
            let mut sorted_dict: Vec<&'a [u8]> = uniq_map.keys().copied().collect();
            sorted_dict.sort_unstable();
            let mut sorted_map: HashMap<&'a [u8], i64> = HashMap::with_capacity(sorted_dict.len());
            for (i, &s) in sorted_dict.iter().enumerate() {
                sorted_map.insert(s, i as i64);
            }

            let mut dict_cand = Vec::new();
            write_uvarint(sorted_dict.len() as u64, &mut dict_cand);
            let mut prev: &[u8] = &[];
            for &entry in &sorted_dict {
                let pfx = common_prefix_len(prev, entry);
                let suffix = &entry[pfx..];
                write_uvarint(pfx as u64, &mut dict_cand);
                write_uvarint(suffix.len() as u64, &mut dict_cand);
                dict_cand.extend_from_slice(suffix);
                prev = entry;
            }

            let indices: Vec<i64> = vals.iter().map(|&v| sorted_map[v]).collect();
            // Encode dictionary indices with our smart int encoder (RLE/FOR/Bitpack!)
            encode_i64_stream(&indices, &mut dict_cand);

            if dict_cand.len() < raw_cand.len() {
                out.push(BytesCodec::DictionaryFrontCoded as u8);
                out.extend_from_slice(&dict_cand);
                return;
            }
        }
    }

    out.push(BytesCodec::RawSplit as u8);
    out.extend_from_slice(&raw_cand);
}

fn decode_bytes_stream(buf: &[u8], pos: &mut usize, count: usize) -> Result<Vec<Vec<u8>>> {
    let tag = read_u8(buf, pos)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    match tag {
        0 => {
            let len = read_uvarint(buf, pos)? as usize;
            let bytes = read_slice(buf, pos, len)?;
            Ok(vec![bytes.to_vec(); count])
        }
        1 => {
            let dict_len = read_uvarint(buf, pos)? as usize;
            let mut dict: Vec<Vec<u8>> = Vec::with_capacity(dict_len);
            let mut prev: Vec<u8> = Vec::new();
            for _ in 0..dict_len {
                let pfx = read_uvarint(buf, pos)? as usize;
                let sfx_len = read_uvarint(buf, pos)? as usize;
                let sfx = read_slice(buf, pos, sfx_len)?;
                if pfx > prev.len() {
                    return Err(Error::Corruption("Front-coded prefix exceeds prev len".into()));
                }
                let mut entry = Vec::with_capacity(pfx + sfx_len);
                entry.extend_from_slice(&prev[..pfx]);
                entry.extend_from_slice(sfx);
                prev = entry.clone();
                dict.push(entry);
            }
            let indices = decode_i64_stream(buf, pos, count)?;
            let mut out = Vec::with_capacity(count);
            for idx in indices {
                let item = dict.get(idx as usize).ok_or_else(|| {
                    Error::Corruption(format!("Bytes dict index out of bounds: {}", idx))
                })?;
                out.push(item.clone());
            }
            Ok(out)
        }
        2 => {
            let lengths = decode_i64_stream(buf, pos, count)?;
            let mut out = Vec::with_capacity(count);
            for l in lengths {
                let len = l as usize;
                let bytes = read_slice(buf, pos, len)?;
                out.push(bytes.to_vec());
            }
            Ok(out)
        }
        _ => Err(Error::Corruption(format!(
            "Unknown bytes codec tag: {}",
            tag
        ))),
    }
}

// ============================================================================
// Unified Column Encoder / Decoder
// ============================================================================

pub fn encode_column(col_type: ColumnType, values: &[Value], out: &mut Vec<u8>) -> Result<()> {
    let (_nulls, all_null) = encode_validity(values, out);
    if all_null || values.is_empty() {
        return Ok(());
    }

    match col_type {
        ColumnType::Integer => {
            let mut ints = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => {}
                    Value::Integer(i) => ints.push(*i),
                    other => {
                        return Err(Error::InvalidData(format!(
                            "Expected Integer, found {:?}",
                            other
                        )))
                    }
                }
            }
            encode_i64_stream(&ints, out);
        }
        ColumnType::Real => {
            let mut reals = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => {}
                    Value::Real(r) => reals.push(*r),
                    Value::Integer(i) => reals.push(*i as f64),
                    other => {
                        return Err(Error::InvalidData(format!(
                            "Expected Real, found {:?}",
                            other
                        )))
                    }
                }
            }
            encode_f64_stream(&reals, out);
        }
        ColumnType::Text => {
            let mut slices: Vec<&[u8]> = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => {}
                    Value::Text(s) => slices.push(s.as_bytes()),
                    other => {
                        return Err(Error::InvalidData(format!(
                            "Expected Text, found {:?}",
                            other
                        )))
                    }
                }
            }
            encode_bytes_stream(&slices, out);
        }
        ColumnType::Blob => {
            let mut slices: Vec<&[u8]> = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Null => {}
                    Value::Blob(b) => slices.push(b.as_slice()),
                    other => {
                        return Err(Error::InvalidData(format!(
                            "Expected Blob, found {:?}",
                            other
                        )))
                    }
                }
            }
            encode_bytes_stream(&slices, out);
        }
    }
    Ok(())
}

pub fn decode_column(
    col_type: ColumnType,
    buf: &[u8],
    pos: &mut usize,
    row_count: usize,
) -> Result<Vec<Value>> {
    let (valid, non_null) = decode_validity(buf, pos, row_count)?;
    if non_null == 0 {
        return Ok(vec![Value::Null; row_count]);
    }

    let mut result = Vec::with_capacity(row_count);
    match col_type {
        ColumnType::Integer => {
            let ints = decode_i64_stream(buf, pos, non_null)?;
            let mut it = ints.into_iter();
            for is_valid in valid {
                if is_valid {
                    result.push(Value::Integer(it.next().unwrap()));
                } else {
                    result.push(Value::Null);
                }
            }
        }
        ColumnType::Real => {
            let reals = decode_f64_stream(buf, pos, non_null)?;
            let mut it = reals.into_iter();
            for is_valid in valid {
                if is_valid {
                    result.push(Value::Real(it.next().unwrap()));
                } else {
                    result.push(Value::Null);
                }
            }
        }
        ColumnType::Text => {
            let items = decode_bytes_stream(buf, pos, non_null)?;
            let mut it = items.into_iter();
            for is_valid in valid {
                if is_valid {
                    let b = it.next().unwrap();
                    let s = String::from_utf8(b)
                        .map_err(|e| Error::Corruption(format!("Invalid UTF-8 in text col: {}", e)))?;
                    result.push(Value::Text(s));
                } else {
                    result.push(Value::Null);
                }
            }
        }
        ColumnType::Blob => {
            let items = decode_bytes_stream(buf, pos, non_null)?;
            let mut it = items.into_iter();
            for is_valid in valid {
                if is_valid {
                    result.push(Value::Blob(it.next().unwrap()));
                } else {
                    result.push(Value::Null);
                }
            }
        }
    }
    Ok(result)
}

/// Fast zero-boxing typed columnar decoder directly into `ColumnVector`!
/// - Preserves dictionary encoding (`ColumnVector::DictText`) with ZERO per-row String cloning!
/// - Decodes `Int64` and `Float64` directly into tight contiguous Vecs without `Value` boxing!
pub fn decode_column_vector(
    col_type: ColumnType,
    buf: &[u8],
    pos: &mut usize,
    row_count: usize,
) -> Result<ColumnVector> {
    let mode = read_u8(buf, pos)?;
    let (valid_opt, non_null): (Option<Vec<bool>>, usize) = match mode {
        0 => (None, row_count),
        1 => (Some(vec![false; row_count]), 0),
        2 => {
            let byte_len = (row_count + 7) / 8;
            let bytes = read_slice(buf, pos, byte_len)?;
            let mut bm = Vec::with_capacity(row_count);
            let mut nn = 0usize;
            for i in 0..row_count {
                let ok = (bytes[i >> 3] & (1 << (i & 7))) != 0;
                if ok {
                    nn += 1;
                }
                bm.push(ok);
            }
            (Some(bm), nn)
        }
        _ => return Err(Error::Corruption(format!("Invalid validity mode: {}", mode))),
    };

    if non_null == 0 {
        return Ok(match col_type {
            ColumnType::Integer => ColumnVector::Int64 {
                data: vec![0; row_count],
                valid: Some(vec![false; row_count]),
            },
            ColumnType::Real => ColumnVector::Float64 {
                data: vec![0.0; row_count],
                valid: Some(vec![false; row_count]),
            },
            ColumnType::Text => ColumnVector::DictText {
                dict: Vec::new(),
                codes: vec![0; row_count],
                valid: Some(vec![false; row_count]),
            },
            ColumnType::Blob => ColumnVector::Blob {
                data: vec![Vec::new(); row_count],
                valid: Some(vec![false; row_count]),
            },
        });
    }

    match col_type {
        ColumnType::Integer => {
            let ints = decode_i64_stream(buf, pos, non_null)?;
            if valid_opt.is_none() {
                Ok(ColumnVector::Int64 {
                    data: ints,
                    valid: None,
                })
            } else {
                let bm = valid_opt.unwrap();
                let mut data = vec![0i64; row_count];
                let mut it = ints.into_iter();
                for i in 0..row_count {
                    if bm[i] {
                        data[i] = it.next().unwrap();
                    }
                }
                Ok(ColumnVector::Int64 {
                    data,
                    valid: Some(bm),
                })
            }
        }
        ColumnType::Real => {
            let reals = decode_f64_stream(buf, pos, non_null)?;
            if valid_opt.is_none() {
                Ok(ColumnVector::Float64 {
                    data: reals,
                    valid: None,
                })
            } else {
                let bm = valid_opt.unwrap();
                let mut data = vec![0.0f64; row_count];
                let mut it = reals.into_iter();
                for i in 0..row_count {
                    if bm[i] {
                        data[i] = it.next().unwrap();
                    }
                }
                Ok(ColumnVector::Float64 {
                    data,
                    valid: Some(bm),
                })
            }
        }
        ColumnType::Text => {
            let tag = read_u8(buf, pos)?;
            match tag {
                0 => {
                    // Constant -> 1-entry DictText (ZERO per-row String allocations!)
                    let len = read_uvarint(buf, pos)? as usize;
                    let bytes = read_slice(buf, pos, len)?;
                    let s = String::from_utf8(bytes.to_vec())
                        .map_err(|e| Error::Corruption(format!("Invalid UTF-8: {}", e)))?;
                    Ok(ColumnVector::DictText {
                        dict: vec![s],
                        codes: vec![0u32; row_count],
                        valid: valid_opt,
                    })
                }
                1 => {
                    // DictionaryFrontCoded -> native DictText (ZERO per-row String cloning!)
                    let dict_len = read_uvarint(buf, pos)? as usize;
                    let mut dict = Vec::with_capacity(dict_len);
                    let mut prev: Vec<u8> = Vec::new();
                    for _ in 0..dict_len {
                        let pfx = read_uvarint(buf, pos)? as usize;
                        let sfx_len = read_uvarint(buf, pos)? as usize;
                        let sfx = read_slice(buf, pos, sfx_len)?;
                        if pfx > prev.len() {
                            return Err(Error::Corruption(
                                "Front-coded prefix exceeds prev len".into(),
                            ));
                        }
                        let mut entry = Vec::with_capacity(pfx + sfx_len);
                        entry.extend_from_slice(&prev[..pfx]);
                        entry.extend_from_slice(sfx);
                        prev = entry.clone();
                        let s = String::from_utf8(entry)
                            .map_err(|e| Error::Corruption(format!("Invalid UTF-8: {}", e)))?;
                        dict.push(s);
                    }
                    let indices = decode_i64_stream(buf, pos, non_null)?;
                    let mut codes = vec![0u32; row_count];
                    if let Some(ref bm) = valid_opt {
                        let mut it = indices.into_iter();
                        for i in 0..row_count {
                            if bm[i] {
                                let idx = it.next().unwrap() as usize;
                                if idx >= dict.len() {
                                    return Err(Error::Corruption("Dict idx out of bounds".into()));
                                }
                                codes[i] = idx as u32;
                            }
                        }
                    } else {
                        for (i, idx) in indices.into_iter().enumerate() {
                            let u = idx as usize;
                            if u >= dict.len() {
                                return Err(Error::Corruption("Dict idx out of bounds".into()));
                            }
                            codes[i] = u as u32;
                        }
                    }
                    Ok(ColumnVector::DictText {
                        dict,
                        codes,
                        valid: valid_opt,
                    })
                }
                2 => {
                    // RawSplit
                    let lengths = decode_i64_stream(buf, pos, non_null)?;
                    let mut data = Vec::with_capacity(row_count);
                    if let Some(ref bm) = valid_opt {
                        let mut it = lengths.into_iter();
                        for i in 0..row_count {
                            if bm[i] {
                                let len = it.next().unwrap() as usize;
                                let bytes = read_slice(buf, pos, len)?;
                                let s = String::from_utf8(bytes.to_vec()).map_err(|e| {
                                    Error::Corruption(format!("Invalid UTF-8: {}", e))
                                })?;
                                data.push(s);
                            } else {
                                data.push(String::new());
                            }
                        }
                    } else {
                        for l in lengths {
                            let bytes = read_slice(buf, pos, l as usize)?;
                            let s = String::from_utf8(bytes.to_vec())
                                .map_err(|e| Error::Corruption(format!("Invalid UTF-8: {}", e)))?;
                            data.push(s);
                        }
                    }
                    Ok(ColumnVector::Text {
                        data,
                        valid: valid_opt,
                    })
                }
                _ => Err(Error::Corruption(format!(
                    "Unknown bytes codec tag: {}",
                    tag
                ))),
            }
        }
        ColumnType::Blob => {
            let items = decode_bytes_stream(buf, pos, non_null)?;
            if valid_opt.is_none() {
                Ok(ColumnVector::Blob {
                    data: items,
                    valid: None,
                })
            } else {
                let bm = valid_opt.unwrap();
                let mut data = Vec::with_capacity(row_count);
                let mut it = items.into_iter();
                for i in 0..row_count {
                    if bm[i] {
                        data.push(it.next().unwrap());
                    } else {
                        data.push(Vec::new());
                    }
                }
                Ok(ColumnVector::Blob {
                    data,
                    valid: Some(bm),
                })
            }
        }
    }
}

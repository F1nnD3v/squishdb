use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::index::{
    decode_index_block, encode_index_block, BloomFilter, IndexBlockMeta, IndexEntry,
};
use crate::pax::{
    compress_bytes, decode_column_chunk_vector, decode_pax_page, decode_rowids_chunk,
    decompress_bytes, encode_pax_page, ColumnZoneMap, CompressionAlgo, CompressionProfile,
    PaxHeader,
};
use crate::types::{
    read_exact, read_str, read_u8, read_uvarint, read_zigzag_varint, write_str, write_uvarint,
    write_zigzag_varint, ColumnDef, ColumnType, Error, Result, Row, TableSchema, Value,
};
use crate::vector::{ColumnBatch, ColumnVector};

pub const MAGIC: &[u8; 8] = b"SQSHDB04";
pub const FORMAT_VERSION: u32 = 4;
pub const SUPERBLOCK_SIZE: u64 = 512;
pub const DATA_START_OFFSET: u64 = SUPERBLOCK_SIZE * 2; // 1024 bytes (Slot A + Slot B)

#[derive(Debug, Clone)]
pub struct Superblock {
    pub version: u32,
    pub generation: u64,
    pub catalog_offset: u64,
    pub catalog_stored_len: u32,
    pub catalog_raw_len: u32,
    pub catalog_algo: CompressionAlgo,
    pub catalog_crc32: u32,
    pub next_table_id: u32,
    pub next_page_id: u64,
    pub data_end_offset: u64,
}

impl Superblock {
    pub fn empty() -> Self {
        Superblock {
            version: FORMAT_VERSION,
            generation: 0,
            catalog_offset: 0,
            catalog_stored_len: 0,
            catalog_raw_len: 0,
            catalog_algo: CompressionAlgo::None,
            catalog_crc32: 0,
            next_table_id: 1,
            next_page_id: 1,
            data_end_offset: DATA_START_OFFSET,
        }
    }

    pub fn serialize(&self) -> [u8; SUPERBLOCK_SIZE as usize] {
        let mut buf = [0u8; SUPERBLOCK_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..20].copy_from_slice(&self.generation.to_le_bytes());
        buf[20..28].copy_from_slice(&self.catalog_offset.to_le_bytes());
        buf[28..32].copy_from_slice(&self.catalog_stored_len.to_le_bytes());
        buf[32..36].copy_from_slice(&self.catalog_raw_len.to_le_bytes());
        buf[36] = self.catalog_algo as u8;
        buf[40..44].copy_from_slice(&self.catalog_crc32.to_le_bytes());
        buf[44..48].copy_from_slice(&self.next_table_id.to_le_bytes());
        buf[48..56].copy_from_slice(&self.next_page_id.to_le_bytes());
        buf[56..64].copy_from_slice(&self.data_end_offset.to_le_bytes());

        let crc = crc32fast::hash(&buf[0..508]);
        buf[508..512].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8; SUPERBLOCK_SIZE as usize]) -> Option<Self> {
        if &buf[0..8] != MAGIC {
            return None;
        }
        let expected_crc = u32::from_le_bytes(buf[508..512].try_into().ok()?);
        let actual_crc = crc32fast::hash(&buf[0..508]);
        if expected_crc != actual_crc {
            return None;
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().ok()?);
        if version != FORMAT_VERSION {
            return None;
        }
        let generation = u64::from_le_bytes(buf[12..20].try_into().ok()?);
        let catalog_offset = u64::from_le_bytes(buf[20..28].try_into().ok()?);
        let catalog_stored_len = u32::from_le_bytes(buf[28..32].try_into().ok()?);
        let catalog_raw_len = u32::from_le_bytes(buf[32..36].try_into().ok()?);
        let catalog_algo = CompressionAlgo::from_u8(buf[36]).ok()?;
        let catalog_crc32 = u32::from_le_bytes(buf[40..44].try_into().ok()?);
        let next_table_id = u32::from_le_bytes(buf[44..48].try_into().ok()?);
        let next_page_id = u64::from_le_bytes(buf[48..56].try_into().ok()?);
        let data_end_offset = u64::from_le_bytes(buf[56..64].try_into().ok()?);

        Some(Superblock {
            version,
            generation,
            catalog_offset,
            catalog_stored_len,
            catalog_raw_len,
            catalog_algo,
            catalog_crc32,
            next_table_id,
            next_page_id,
            data_end_offset,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PageMeta {
    pub page_id: u64,
    pub offset: u64,
    pub stored_len: u32,
    pub raw_pax_len: u32,
    pub compression: CompressionAlgo,
    pub crc32: u32,
    pub row_count: u32,
    pub min_rowid: i64,
    pub max_rowid: i64,
    pub zone_maps: Vec<ColumnZoneMap>,
    pub blooms: Vec<Option<BloomFilter>>,
    pub deleted_slots: Vec<u32>,
}

impl PageMeta {
    pub fn live_rows(&self) -> u32 {
        self.row_count.saturating_sub(self.deleted_slots.len() as u32)
    }

    pub fn is_deleted(&self, slot: u32) -> bool {
        self.deleted_slots.binary_search(&slot).is_ok()
    }

    pub fn mark_deleted(&mut self, slot: u32) -> bool {
        match self.deleted_slots.binary_search(&slot) {
            Ok(_) => false,
            Err(idx) => {
                self.deleted_slots.insert(idx, slot);
                true
            }
        }
    }

    fn write_binary(&self, out: &mut Vec<u8>) {
        write_uvarint(self.page_id, out);
        write_uvarint(self.offset, out);
        write_uvarint(self.stored_len as u64, out);
        write_uvarint(self.raw_pax_len as u64, out);
        out.push(self.compression as u8);
        out.extend_from_slice(&self.crc32.to_le_bytes());
        write_uvarint(self.row_count as u64, out);
        write_zigzag_varint(self.min_rowid, out);
        write_zigzag_varint(self.max_rowid, out);

        write_uvarint(self.zone_maps.len() as u64, out);
        for zm in &self.zone_maps {
            zm.write_binary(out);
        }

        write_uvarint(self.blooms.len() as u64, out);
        for b in &self.blooms {
            match b {
                Some(bf) => {
                    out.push(1);
                    bf.write_binary(out);
                }
                None => out.push(0),
            }
        }

        write_uvarint(self.deleted_slots.len() as u64, out);
        let mut prev = 0u64;
        for &slot in &self.deleted_slots {
            let s = slot as u64;
            write_uvarint(s - prev, out);
            prev = s;
        }
    }

    fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let page_id = read_uvarint(buf, pos)?;
        let offset = read_uvarint(buf, pos)?;
        let stored_len = read_uvarint(buf, pos)? as u32;
        let raw_pax_len = read_uvarint(buf, pos)? as u32;
        let compression = CompressionAlgo::from_u8(read_u8(buf, pos)?)?;
        let crc32 = u32::from_le_bytes(read_exact::<4>(buf, pos)?);
        let row_count = read_uvarint(buf, pos)? as u32;
        let min_rowid = read_zigzag_varint(buf, pos)?;
        let max_rowid = read_zigzag_varint(buf, pos)?;

        let zm_len = read_uvarint(buf, pos)? as usize;
        let mut zone_maps = Vec::with_capacity(zm_len);
        for _ in 0..zm_len {
            zone_maps.push(ColumnZoneMap::read_binary(buf, pos)?);
        }

        let bl_len = read_uvarint(buf, pos)? as usize;
        let mut blooms = Vec::with_capacity(bl_len);
        for _ in 0..bl_len {
            let has = read_u8(buf, pos)?;
            if has == 1 {
                blooms.push(Some(BloomFilter::read_binary(buf, pos)?));
            } else {
                blooms.push(None);
            }
        }

        let del_len = read_uvarint(buf, pos)? as usize;
        let mut deleted_slots = Vec::with_capacity(del_len);
        let mut prev = 0u64;
        for _ in 0..del_len {
            let delta = read_uvarint(buf, pos)?;
            prev += delta;
            deleted_slots.push(prev as u32);
        }
        Ok(PageMeta {
            page_id,
            offset,
            stored_len,
            raw_pax_len,
            compression,
            crc32,
            row_count,
            min_rowid,
            max_rowid,
            zone_maps,
            blooms,
            deleted_slots,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    pub schema: TableSchema,
    pub pages: Vec<PageMeta>,
}

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub tables: Vec<TableEntry>,
    pub indexes: Vec<IndexEntry>,
}

pub fn write_col_def(c: &ColumnDef, out: &mut Vec<u8>) {
    write_str(&c.name, out);
    out.push(c.col_type as u8);
    let flags = (c.primary_key as u8)
        | ((c.not_null as u8) << 1)
        | ((c.unique as u8) << 2)
        | ((c.default_value.is_some() as u8) << 3);
    out.push(flags);
    if let Some(ref dv) = c.default_value {
        dv.write_binary(out);
    }
}

pub fn read_col_def(buf: &[u8], pos: &mut usize) -> Result<ColumnDef> {
    let name = read_str(buf, pos)?;
    let col_type = ColumnType::from_u8(read_u8(buf, pos)?)?;
    let flags = read_u8(buf, pos)?;
    let default_value = if (flags & 8) != 0 {
        Some(Value::read_binary(buf, pos)?)
    } else {
        None
    };
    Ok(ColumnDef {
        name,
        col_type,
        primary_key: (flags & 1) != 0,
        not_null: (flags & 2) != 0,
        unique: (flags & 4) != 0,
        default_value,
    })
}

impl Catalog {
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_uvarint(self.tables.len() as u64, &mut out);
        for t in &self.tables {
            write_uvarint(t.schema.id as u64, &mut out);
            write_str(&t.schema.name, &mut out);
            write_zigzag_varint(t.schema.next_rowid, &mut out);
            write_uvarint(t.schema.cluster_by.len() as u64, &mut out);
            for cb in &t.schema.cluster_by {
                write_str(cb, &mut out);
            }
            write_uvarint(t.schema.columns.len() as u64, &mut out);
            for c in &t.schema.columns {
                write_col_def(c, &mut out);
            }
            write_uvarint(t.pages.len() as u64, &mut out);
            for p in &t.pages {
                p.write_binary(&mut out);
            }
        }
        write_uvarint(self.indexes.len() as u64, &mut out);
        for idx in &self.indexes {
            idx.write_binary(&mut out);
        }
        out
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        let mut pos = 0usize;
        let num_tables = read_uvarint(buf, &mut pos)? as usize;
        let mut tables = Vec::with_capacity(num_tables);
        for _ in 0..num_tables {
            let id = read_uvarint(buf, &mut pos)? as u32;
            let name = read_str(buf, &mut pos)?;
            let next_rowid = read_zigzag_varint(buf, &mut pos)?;
            let cb_len = read_uvarint(buf, &mut pos)? as usize;
            let mut cluster_by = Vec::with_capacity(cb_len);
            for _ in 0..cb_len {
                cluster_by.push(read_str(buf, &mut pos)?);
            }
            let num_cols = read_uvarint(buf, &mut pos)? as usize;
            let mut columns = Vec::with_capacity(num_cols);
            for _ in 0..num_cols {
                columns.push(read_col_def(buf, &mut pos)?);
            }
            let num_pages = read_uvarint(buf, &mut pos)? as usize;
            let mut pages = Vec::with_capacity(num_pages);
            for _ in 0..num_pages {
                pages.push(PageMeta::read_binary(buf, &mut pos)?);
            }
            tables.push(TableEntry {
                schema: TableSchema {
                    id,
                    name,
                    columns,
                    next_rowid,
                    cluster_by,
                },
                pages,
            });
        }
        let num_indexes = read_uvarint(buf, &mut pos)? as usize;
        let mut indexes = Vec::with_capacity(num_indexes);
        for _ in 0..num_indexes {
            indexes.push(IndexEntry::read_binary(buf, &mut pos)?);
        }
        Ok(Catalog { tables, indexes })
    }
}

pub struct StorageFile {
    file: File,
    pub superblock: Superblock,
    active_slot: u8,
    read_only: bool,
}

impl StorageFile {
    pub fn open(path: &Path) -> Result<(Self, Catalog)> {
        Self::open_mode(path, false)
    }

    pub fn open_readonly(path: &Path) -> Result<(Self, Catalog)> {
        Self::open_mode(path, true)
    }

    fn open_mode(path: &Path, read_only: bool) -> Result<(Self, Catalog)> {
        let file = if read_only {
            OpenOptions::new().read(true).open(path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?
        };

        let file_len = file.metadata()?.len();
        if file_len < DATA_START_OFFSET {
            if read_only {
                return Err(Error::Corruption(
                    "Database file is empty or too small to open read-only".into(),
                ));
            }
            let mut sf = StorageFile {
                file,
                superblock: Superblock::empty(),
                active_slot: 0,
                read_only: false,
            };
            let cat = Catalog::default();
            sf.commit_catalog(&cat, CompressionProfile::Balanced)?;
            return Ok((sf, cat));
        }

        let mut sf = StorageFile {
            file,
            superblock: Superblock::empty(),
            active_slot: 0,
            read_only,
        };

        let mut buf_a = [0u8; SUPERBLOCK_SIZE as usize];
        let mut buf_b = [0u8; SUPERBLOCK_SIZE as usize];
        sf.file.seek(SeekFrom::Start(0))?;
        sf.file.read_exact(&mut buf_a)?;
        sf.file.read_exact(&mut buf_b)?;

        let sb_a = Superblock::deserialize(&buf_a);
        let sb_b = Superblock::deserialize(&buf_b);

        let (sb, slot) = match (sb_a, sb_b) {
            (Some(a), Some(b)) => {
                if b.generation > a.generation {
                    (b, 1u8)
                } else {
                    (a, 0u8)
                }
            }
            (Some(a), None) => (a, 0u8),
            (None, Some(b)) => (b, 1u8),
            (None, None) => {
                return Err(Error::Corruption(
                    "Both A/B superblocks are missing or corrupted (or incompatible format version)"
                        .into(),
                ));
            }
        };

        sf.superblock = sb;
        sf.active_slot = slot;

        let catalog = if sf.superblock.catalog_stored_len == 0 {
            Catalog::default()
        } else {
            sf.read_catalog()?
        };

        Ok((sf, catalog))
    }

    fn read_catalog(&mut self) -> Result<Catalog> {
        let sb = &self.superblock;
        self.file.seek(SeekFrom::Start(sb.catalog_offset))?;
        let mut stored = vec![0u8; sb.catalog_stored_len as usize];
        self.file.read_exact(&mut stored)?;
        let actual_crc = crc32fast::hash(&stored);
        if actual_crc != sb.catalog_crc32 {
            return Err(Error::Corruption(format!(
                "Catalog CRC32 mismatch: expected {:08x}, got {:08x}",
                sb.catalog_crc32, actual_crc
            )));
        }
        let raw = decompress_bytes(&stored, sb.catalog_algo, sb.catalog_raw_len as usize)?;
        Catalog::deserialize(&raw)
    }

    pub fn write_pax_page(
        &mut self,
        schema: &TableSchema,
        rows: &[Row],
        profile: CompressionProfile,
    ) -> Result<PageMeta> {
        if self.read_only {
            return Err(Error::Sql("Database is opened read-only".into()));
        }
        let encoded = encode_pax_page(schema, rows, profile)?;
        let offset = self.superblock.data_end_offset.max(DATA_START_OFFSET);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&encoded.stored_bytes)?;

        let page_id = self.superblock.next_page_id;
        self.superblock.next_page_id += 1;
        let stored_len = encoded.stored_bytes.len() as u32;
        self.superblock.data_end_offset = offset + stored_len as u64;

        Ok(PageMeta {
            page_id,
            offset,
            stored_len,
            raw_pax_len: encoded.raw_pax_len,
            compression: encoded.compression,
            crc32: encoded.crc32,
            row_count: encoded.row_count,
            min_rowid: encoded.min_rowid,
            max_rowid: encoded.max_rowid,
            zone_maps: encoded.zone_maps,
            blooms: encoded.blooms,
            deleted_slots: Vec::new(),
        })
    }

    pub fn read_pax_page(
        &mut self,
        schema: &TableSchema,
        meta: &PageMeta,
        needed_cols: Option<&[bool]>,
    ) -> Result<Vec<Row>> {
        self.file.seek(SeekFrom::Start(meta.offset))?;
        let mut stored = vec![0u8; meta.stored_len as usize];
        self.file.read_exact(&mut stored)?;
        decode_pax_page(
            schema,
            &stored,
            meta.compression,
            meta.raw_pax_len,
            meta.crc32,
            needed_cols,
        )
    }

    /// Reads ONLY the tiny PAX-v4 page header (~60-120 bytes) to locate individual column chunks!
    pub fn read_pax_header(&mut self, meta: &PageMeta) -> Result<(u64, PaxHeader)> {
        if meta.stored_len < 8 {
            return Err(Error::Corruption("PAX-v4 page smaller than 8-byte header".into()));
        }
        self.file.seek(SeekFrom::Start(meta.offset))?;
        let mut prefix = [0u8; 8];
        self.file.read_exact(&mut prefix)?;
        let hlen = u32::from_le_bytes(prefix[0..4].try_into().unwrap()) as usize;
        let hcrc = u32::from_le_bytes(prefix[4..8].try_into().unwrap());
        if 8 + hlen > meta.stored_len as usize {
            return Err(Error::Corruption("PAX-v4 header length exceeds page bounds".into()));
        }
        let mut hbytes = vec![0u8; hlen];
        self.file.read_exact(&mut hbytes)?;
        if crc32fast::hash(&hbytes) != hcrc {
            return Err(Error::Corruption("PAX-v4 header CRC32 mismatch".into()));
        }
        let header = PaxHeader::parse_bytes(&hbytes)?;
        Ok((meta.offset + 8 + hlen as u64, header))
    }

    /// Reads ONLY a single column chunk from disk directly into a typed `ColumnVector`!
    /// `col_idx == None` reads the `rowids` chunk as `ColumnVector::Int64`.
    pub fn read_pax_column(
        &mut self,
        schema: &TableSchema,
        meta: &PageMeta,
        col_idx: Option<usize>,
    ) -> Result<ColumnVector> {
        let (data_base, header) = self.read_pax_header(meta)?;
        let int_pk_col = schema.integer_pk_col();

        // Rowids or INTEGER PRIMARY KEY alias
        if col_idx.is_none() || col_idx == int_pk_col {
            let rc = &header.rowids_chunk;
            self.file.seek(SeekFrom::Start(data_base + rc.rel_offset as u64))?;
            let mut buf = vec![0u8; rc.stored_len as usize];
            self.file.read_exact(&mut buf)?;
            let rowids = decode_rowids_chunk(&buf, rc, header.row_count)?;
            return Ok(ColumnVector::Int64 {
                data: rowids,
                valid: None,
            });
        }

        let ci = col_idx.unwrap();
        let col_def = &schema.columns[ci];
        if ci >= header.stored_col_count {
            return Ok(ColumnVector::filled(
                col_def.col_type,
                col_def.default_value.as_ref(),
                header.row_count,
            ));
        }
        let cm = &header.col_chunks[ci];
        self.file.seek(SeekFrom::Start(data_base + cm.rel_offset as u64))?;
        let mut buf = vec![0u8; cm.stored_len as usize];
        self.file.read_exact(&mut buf)?;
        decode_column_chunk_vector(col_def.col_type, &buf, cm, header.row_count)
    }

    /// True disk-level columnar projection: reads and decompresses ONLY `needed_cols` chunks
    /// directly into typed `ColumnVector`s (zero disk I/O & zero decompression on skipped columns!).
    pub fn read_pax_batch(
        &mut self,
        schema: &TableSchema,
        meta: &PageMeta,
        needed_cols: &[bool],
        need_rowids: bool,
    ) -> Result<ColumnBatch> {
        let (data_base, header) = self.read_pax_header(meta)?;
        let int_pk_col = schema.integer_pk_col();
        let need_pk_rowids = need_rowids
            || int_pk_col
                .map(|i| needed_cols.get(i).copied().unwrap_or(false))
                .unwrap_or(false);

        let rowids = if need_pk_rowids {
            let rc = &header.rowids_chunk;
            self.file.seek(SeekFrom::Start(data_base + rc.rel_offset as u64))?;
            let mut buf = vec![0u8; rc.stored_len as usize];
            self.file.read_exact(&mut buf)?;
            decode_rowids_chunk(&buf, rc, header.row_count)?
        } else {
            Vec::new()
        };

        let num_cols = schema.columns.len();
        let mut columns: Vec<Option<ColumnVector>> = vec![None; num_cols];

        for (c_idx, col_def) in schema.columns.iter().enumerate() {
            if !needed_cols.get(c_idx).copied().unwrap_or(false) {
                continue;
            }
            if Some(c_idx) == int_pk_col {
                columns[c_idx] = Some(ColumnVector::Int64 {
                    data: rowids.clone(),
                    valid: None,
                });
                continue;
            }
            if c_idx >= header.stored_col_count {
                columns[c_idx] = Some(ColumnVector::filled(
                    col_def.col_type,
                    col_def.default_value.as_ref(),
                    header.row_count,
                ));
                continue;
            }
            let cm = &header.col_chunks[c_idx];
            self.file.seek(SeekFrom::Start(data_base + cm.rel_offset as u64))?;
            let mut buf = vec![0u8; cm.stored_len as usize];
            self.file.read_exact(&mut buf)?;
            columns[c_idx] = Some(decode_column_chunk_vector(
                col_def.col_type,
                &buf,
                cm,
                header.row_count,
            )?);
        }

        Ok(ColumnBatch {
            row_count: header.row_count,
            rowids,
            columns,
        })
    }

    /// Appends a sorted, compressed on-disk secondary index leaf block.
    pub fn write_index_block(
        &mut self,
        entries: &[(u64, u64, u32)],
        profile: CompressionProfile,
    ) -> Result<IndexBlockMeta> {
        if self.read_only {
            return Err(Error::Sql("Database is opened read-only".into()));
        }
        let min_hash = entries.first().map(|e| e.0).unwrap_or(0);
        let max_hash = entries.last().map(|e| e.0).unwrap_or(0);
        let (stored, raw_len, compression, crc32) = encode_index_block(entries, profile)?;
        let offset = self.superblock.data_end_offset.max(DATA_START_OFFSET);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&stored)?;
        let stored_len = stored.len() as u32;
        self.superblock.data_end_offset = offset + stored_len as u64;

        Ok(IndexBlockMeta {
            min_hash,
            max_hash,
            offset,
            stored_len,
            raw_len,
            compression,
            crc32,
            entry_count: entries.len() as u32,
        })
    }

    /// Reads and decodes a single on-disk secondary index leaf block.
    pub fn read_index_block(&mut self, meta: &IndexBlockMeta) -> Result<Vec<(u64, u64, u32)>> {
        self.file.seek(SeekFrom::Start(meta.offset))?;
        let mut stored = vec![0u8; meta.stored_len as usize];
        self.file.read_exact(&mut stored)?;
        decode_index_block(&stored, meta.compression, meta.raw_len, meta.crc32)
    }

    pub fn commit_catalog(
        &mut self,
        catalog: &Catalog,
        profile: CompressionProfile,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::Sql("Database is opened read-only".into()));
        }
        let raw_cat = catalog.serialize();
        let (algo, stored_cat) = compress_bytes(&raw_cat, profile)?;
        let crc = crc32fast::hash(&stored_cat);

        let cat_offset = self.superblock.data_end_offset.max(DATA_START_OFFSET);
        self.file.seek(SeekFrom::Start(cat_offset))?;
        self.file.write_all(&stored_cat)?;
        let new_end = cat_offset + stored_cat.len() as u64;
        self.file.set_len(new_end)?;
        self.file.sync_data()?;

        self.superblock.generation += 1;
        self.superblock.catalog_offset = cat_offset;
        self.superblock.catalog_stored_len = stored_cat.len() as u32;
        self.superblock.catalog_raw_len = raw_cat.len() as u32;
        self.superblock.catalog_algo = algo;
        self.superblock.catalog_crc32 = crc;

        let next_slot = if self.active_slot == 0 { 1 } else { 0 };
        let sb_bytes = self.superblock.serialize();
        self.file
            .seek(SeekFrom::Start((next_slot as u64) * SUPERBLOCK_SIZE))?;
        self.file.write_all(&sb_bytes)?;
        self.file.sync_data()?;
        self.active_slot = next_slot;
        Ok(())
    }

    pub fn file_len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

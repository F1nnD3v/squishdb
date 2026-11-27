use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::index::IndexKind;
use crate::storage::{
    read_check_constraint, read_col_def, read_foreign_key, write_check_constraint, write_col_def,
    write_foreign_key,
};
use crate::types::{
    read_str, read_u8, read_uvarint, read_zigzag_varint, write_str, write_uvarint,
    write_zigzag_varint, CheckConstraint, ColumnDef, Error, ForeignKeyDef, Result, Row,
    TableSchema, Value,
};

const WAL_MAGIC: &[u8; 4] = b"SQWL";

#[derive(Debug, Clone)]
pub enum WalOp {
    CreateTable(TableSchema),
    DropTable(String),
    InsertRows {
        table_name: String,
        rows: Vec<Row>,
    },
    DeleteRowIds {
        table_name: String,
        rowids: Vec<i64>,
    },
    UpdateRows {
        table_name: String,
        rows: Vec<Row>,
    },
    CreateIndex {
        name: String,
        table_name: String,
        column_name: String,
    },
    DropIndex(String),
    SetClusterBy {
        table_name: String,
        cluster_by: Vec<String>,
    },
    AddColumn {
        table_name: String,
        column: ColumnDef,
    },
    /// v5: CREATE TABLE with CHECK constraints and FOREIGN KEY definitions.
    CreateTableV5(TableSchema),
    /// v5: CREATE [UNIQUE] INDEX with composite columns, ASC/DESC, and B-Tree or Hash kind.
    CreateIndexV5 {
        name: String,
        table_name: String,
        columns: Vec<String>,
        desc: Vec<bool>,
        kind: IndexKind,
        unique: bool,
    },
    /// v5: ALTER TABLE ADD COLUMN carrying any column-level CHECK and FOREIGN KEY constraints.
    AddColumnV5 {
        table_name: String,
        column: ColumnDef,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    },
    /// v5: ALTER TABLE DROP COLUMN.
    DropColumn {
        table_name: String,
        column_name: String,
    },
    /// v5: ALTER TABLE RENAME COLUMN.
    RenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
}

impl WalOp {
    fn write_binary(&self, out: &mut Vec<u8>) {
        match self {
            WalOp::CreateTable(schema) => {
                if !schema.check_constraints.is_empty() || !schema.foreign_keys.is_empty() {
                    WalOp::CreateTableV5(schema.clone()).write_binary(out);
                    return;
                }
                out.push(1);
                write_uvarint(schema.id as u64, out);
                write_str(&schema.name, out);
                write_zigzag_varint(schema.next_rowid, out);
                write_uvarint(schema.cluster_by.len() as u64, out);
                for cb in &schema.cluster_by {
                    write_str(cb, out);
                }
                write_uvarint(schema.columns.len() as u64, out);
                for c in &schema.columns {
                    write_col_def(c, out);
                }
            }
            WalOp::CreateTableV5(schema) => {
                out.push(10);
                write_uvarint(schema.id as u64, out);
                write_str(&schema.name, out);
                write_zigzag_varint(schema.next_rowid, out);
                write_uvarint(schema.cluster_by.len() as u64, out);
                for cb in &schema.cluster_by {
                    write_str(cb, out);
                }
                write_uvarint(schema.columns.len() as u64, out);
                for c in &schema.columns {
                    write_col_def(c, out);
                }
                write_uvarint(schema.check_constraints.len() as u64, out);
                for chk in &schema.check_constraints {
                    write_check_constraint(chk, out);
                }
                write_uvarint(schema.foreign_keys.len() as u64, out);
                for fk in &schema.foreign_keys {
                    write_foreign_key(fk, out);
                }
            }
            WalOp::DropTable(name) => {
                out.push(2);
                write_str(name, out);
            }
            WalOp::InsertRows { table_name, rows } => {
                out.push(3);
                write_str(table_name, out);
                write_uvarint(rows.len() as u64, out);
                for r in rows {
                    write_zigzag_varint(r.rowid, out);
                    write_uvarint(r.values.len() as u64, out);
                    for v in &r.values {
                        v.write_binary(out);
                    }
                }
            }
            WalOp::DeleteRowIds { table_name, rowids } => {
                out.push(4);
                write_str(table_name, out);
                write_uvarint(rowids.len() as u64, out);
                for &id in rowids {
                    write_zigzag_varint(id, out);
                }
            }
            WalOp::UpdateRows { table_name, rows } => {
                out.push(5);
                write_str(table_name, out);
                write_uvarint(rows.len() as u64, out);
                for r in rows {
                    write_zigzag_varint(r.rowid, out);
                    write_uvarint(r.values.len() as u64, out);
                    for v in &r.values {
                        v.write_binary(out);
                    }
                }
            }
            WalOp::CreateIndex {
                name,
                table_name,
                column_name,
            } => {
                out.push(6);
                write_str(name, out);
                write_str(table_name, out);
                write_str(column_name, out);
            }
            WalOp::CreateIndexV5 {
                name,
                table_name,
                columns,
                desc,
                kind,
                unique,
            } => {
                out.push(11);
                write_str(name, out);
                write_str(table_name, out);
                out.push(*kind as u8);
                out.push(*unique as u8);
                write_uvarint(columns.len() as u64, out);
                for (i, c) in columns.iter().enumerate() {
                    write_str(c, out);
                    out.push(desc.get(i).copied().unwrap_or(false) as u8);
                }
            }
            WalOp::DropIndex(name) => {
                out.push(7);
                write_str(name, out);
            }
            WalOp::SetClusterBy {
                table_name,
                cluster_by,
            } => {
                out.push(8);
                write_str(table_name, out);
                write_uvarint(cluster_by.len() as u64, out);
                for c in cluster_by {
                    write_str(c, out);
                }
            }
            WalOp::AddColumn { table_name, column } => {
                out.push(9);
                write_str(table_name, out);
                write_col_def(column, out);
            }
            WalOp::AddColumnV5 {
                table_name,
                column,
                check_constraints,
                foreign_keys,
            } => {
                out.push(12);
                write_str(table_name, out);
                write_col_def(column, out);
                write_uvarint(check_constraints.len() as u64, out);
                for chk in check_constraints {
                    write_check_constraint(chk, out);
                }
                write_uvarint(foreign_keys.len() as u64, out);
                for fk in foreign_keys {
                    write_foreign_key(fk, out);
                }
            }
            WalOp::DropColumn {
                table_name,
                column_name,
            } => {
                out.push(13);
                write_str(table_name, out);
                write_str(column_name, out);
            }
            WalOp::RenameColumn {
                table_name,
                old_name,
                new_name,
            } => {
                out.push(14);
                write_str(table_name, out);
                write_str(old_name, out);
                write_str(new_name, out);
            }
        }
    }

    fn read_binary(buf: &[u8], pos: &mut usize) -> Result<Self> {
        let tag = read_u8(buf, pos)?;
        match tag {
            1 => {
                let id = read_uvarint(buf, pos)? as u32;
                let name = read_str(buf, pos)?;
                let next_rowid = read_zigzag_varint(buf, pos)?;
                let cb_len = read_uvarint(buf, pos)? as usize;
                let mut cluster_by = Vec::with_capacity(cb_len);
                for _ in 0..cb_len {
                    cluster_by.push(read_str(buf, pos)?);
                }
                let ncols = read_uvarint(buf, pos)? as usize;
                let mut columns = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    columns.push(read_col_def(buf, pos)?);
                }
                Ok(WalOp::CreateTable(TableSchema {
                    id,
                    name,
                    columns,
                    next_rowid,
                    cluster_by,
                    check_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                }))
            }
            10 => {
                let id = read_uvarint(buf, pos)? as u32;
                let name = read_str(buf, pos)?;
                let next_rowid = read_zigzag_varint(buf, pos)?;
                let cb_len = read_uvarint(buf, pos)? as usize;
                let mut cluster_by = Vec::with_capacity(cb_len);
                for _ in 0..cb_len {
                    cluster_by.push(read_str(buf, pos)?);
                }
                let ncols = read_uvarint(buf, pos)? as usize;
                let mut columns = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    columns.push(read_col_def(buf, pos)?);
                }
                let nchk = read_uvarint(buf, pos)? as usize;
                let mut check_constraints = Vec::with_capacity(nchk);
                for _ in 0..nchk {
                    check_constraints.push(read_check_constraint(buf, pos)?);
                }
                let nfk = read_uvarint(buf, pos)? as usize;
                let mut foreign_keys = Vec::with_capacity(nfk);
                for _ in 0..nfk {
                    foreign_keys.push(read_foreign_key(buf, pos)?);
                }
                Ok(WalOp::CreateTableV5(TableSchema {
                    id,
                    name,
                    columns,
                    next_rowid,
                    cluster_by,
                    check_constraints,
                    foreign_keys,
                }))
            }
            2 => Ok(WalOp::DropTable(read_str(buf, pos)?)),
            3 | 5 => {
                let table_name = read_str(buf, pos)?;
                let nrows = read_uvarint(buf, pos)? as usize;
                let mut rows = Vec::with_capacity(nrows);
                for _ in 0..nrows {
                    let rowid = read_zigzag_varint(buf, pos)?;
                    let nvals = read_uvarint(buf, pos)? as usize;
                    let mut values = Vec::with_capacity(nvals);
                    for _ in 0..nvals {
                        values.push(Value::read_binary(buf, pos)?);
                    }
                    rows.push(Row { rowid, values });
                }
                if tag == 3 {
                    Ok(WalOp::InsertRows { table_name, rows })
                } else {
                    Ok(WalOp::UpdateRows { table_name, rows })
                }
            }
            4 => {
                let table_name = read_str(buf, pos)?;
                let n = read_uvarint(buf, pos)? as usize;
                let mut rowids = Vec::with_capacity(n);
                for _ in 0..n {
                    rowids.push(read_zigzag_varint(buf, pos)?);
                }
                Ok(WalOp::DeleteRowIds { table_name, rowids })
            }
            6 => Ok(WalOp::CreateIndex {
                name: read_str(buf, pos)?,
                table_name: read_str(buf, pos)?,
                column_name: read_str(buf, pos)?,
            }),
            11 => {
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
                Ok(WalOp::CreateIndexV5 {
                    name,
                    table_name,
                    columns,
                    desc,
                    kind,
                    unique,
                })
            }
            7 => Ok(WalOp::DropIndex(read_str(buf, pos)?)),
            8 => {
                let table_name = read_str(buf, pos)?;
                let n = read_uvarint(buf, pos)? as usize;
                let mut cluster_by = Vec::with_capacity(n);
                for _ in 0..n {
                    cluster_by.push(read_str(buf, pos)?);
                }
                Ok(WalOp::SetClusterBy {
                    table_name,
                    cluster_by,
                })
            }
            9 => {
                let table_name = read_str(buf, pos)?;
                let column = read_col_def(buf, pos)?;
                Ok(WalOp::AddColumn { table_name, column })
            }
            12 => {
                let table_name = read_str(buf, pos)?;
                let column = read_col_def(buf, pos)?;
                let nchk = read_uvarint(buf, pos)? as usize;
                let mut check_constraints = Vec::with_capacity(nchk);
                for _ in 0..nchk {
                    check_constraints.push(read_check_constraint(buf, pos)?);
                }
                let nfk = read_uvarint(buf, pos)? as usize;
                let mut foreign_keys = Vec::with_capacity(nfk);
                for _ in 0..nfk {
                    foreign_keys.push(read_foreign_key(buf, pos)?);
                }
                Ok(WalOp::AddColumnV5 {
                    table_name,
                    column,
                    check_constraints,
                    foreign_keys,
                })
            }
            13 => Ok(WalOp::DropColumn {
                table_name: read_str(buf, pos)?,
                column_name: read_str(buf, pos)?,
            }),
            14 => Ok(WalOp::RenameColumn {
                table_name: read_str(buf, pos)?,
                old_name: read_str(buf, pos)?,
                new_name: read_str(buf, pos)?,
            }),
            _ => Err(Error::Corruption(format!("Unknown WAL op tag: {}", tag))),
        }
    }
}

pub struct WalFrame {
    pub base_generation: u64,
    pub ops: Vec<WalOp>,
}

pub struct WalFile {
    path: PathBuf,
    file: Option<File>,
    read_only: bool,
}

impl WalFile {
    pub fn wal_path_for(db_path: &Path) -> PathBuf {
        let mut os = db_path.as_os_str().to_os_string();
        os.push("-wal");
        PathBuf::from(os)
    }

    pub fn open(db_path: &Path) -> Result<Self> {
        let path = Self::wal_path_for(db_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(WalFile {
            path,
            file: Some(file),
            read_only: false,
        })
    }

    pub fn open_readonly(db_path: &Path) -> Result<Self> {
        let path = Self::wal_path_for(db_path);
        let file = OpenOptions::new().read(true).open(&path).ok();
        Ok(WalFile {
            path,
            file,
            read_only: true,
        })
    }

    pub fn read_frames(&mut self, current_gen: u64) -> Result<Vec<WalFrame>> {
        let (frames, _) = self.read_frames_from(current_gen, 0)?;
        Ok(frames)
    }

    /// Reads valid WAL frames starting at byte offset `start_offset`, returning `(frames, new_offset)`.
    pub fn read_frames_from(
        &mut self,
        current_gen: u64,
        start_offset: u64,
    ) -> Result<(Vec<WalFrame>, u64)> {
        let f = match self.file.as_mut() {
            Some(f) => f,
            None => return Ok((Vec::new(), start_offset)),
        };
        let len = f.metadata()?.len();
        if start_offset >= len {
            return Ok((Vec::new(), start_offset));
        }
        f.seek(SeekFrom::Start(start_offset))?;
        let mut raw = Vec::with_capacity((len - start_offset) as usize);
        f.read_to_end(&mut raw)?;

        let mut frames = Vec::new();
        let mut pos = 0usize;
        while pos + 20 <= raw.len() {
            if &raw[pos..pos + 4] != WAL_MAGIC {
                break;
            }
            let base_gen = u64::from_le_bytes(raw[pos + 4..pos + 12].try_into().unwrap());
            let payload_len =
                u32::from_le_bytes(raw[pos + 12..pos + 16].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(raw[pos + 16..pos + 20].try_into().unwrap());
            let payload_start = pos + 20;
            let payload_end = payload_start.saturating_add(payload_len);
            if payload_end > raw.len() {
                break;
            }
            let payload = &raw[payload_start..payload_end];
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&raw[pos..pos + 16]);
            hasher.update(payload);
            if hasher.finalize() != expected_crc {
                break;
            }
            pos = payload_end;

            if base_gen >= current_gen {
                let mut p = 0usize;
                let num_ops = read_uvarint(payload, &mut p)? as usize;
                let mut ops = Vec::with_capacity(num_ops);
                for _ in 0..num_ops {
                    ops.push(WalOp::read_binary(payload, &mut p)?);
                }
                frames.push(WalFrame {
                    base_generation: base_gen,
                    ops,
                });
            }
        }
        Ok((frames, start_offset + pos as u64))
    }

    pub fn append_frame(&mut self, base_generation: u64, ops: &[WalOp], sync: bool) -> Result<()> {
        if self.read_only {
            return Err(Error::Sql("Cannot write to read-only database".into()));
        }
        if ops.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::new();
        write_uvarint(ops.len() as u64, &mut payload);
        for op in ops {
            op.write_binary(&mut payload);
        }
        let mut header = [0u8; 20];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..12].copy_from_slice(&base_generation.to_le_bytes());
        header[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[0..16]);
        hasher.update(&payload);
        let crc = hasher.finalize();
        header[16..20].copy_from_slice(&crc.to_le_bytes());

        let f = self.file.as_mut().unwrap();
        f.seek(SeekFrom::End(0))?;
        f.write_all(&header)?;
        f.write_all(&payload)?;
        if sync {
            f.sync_data()?;
        }
        Ok(())
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.truncate_to(0)
    }

    pub fn truncate_to(&mut self, offset: u64) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        if let Some(f) = self.file.as_mut() {
            let cur = f.metadata()?.len();
            if cur > offset || offset == 0 {
                f.set_len(offset)?;
                f.seek(SeekFrom::Start(offset))?;
                f.sync_data()?;
            }
        }
        Ok(())
    }

    pub fn remove_if_empty(&self) {
        if self.read_only {
            return;
        }
        if let Some(f) = self.file.as_ref() {
            if let Ok(meta) = f.metadata() {
                if meta.len() == 0 {
                    let _ = std::fs::remove_file(&self.path);
                }
            }
        }
    }

    pub fn file_len(&self) -> u64 {
        self.file
            .as_ref()
            .and_then(|f| f.metadata().ok())
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

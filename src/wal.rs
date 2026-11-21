use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::{read_col_def, write_col_def};
use crate::types::{
    read_str, read_u8, read_uvarint, read_zigzag_varint, write_str, write_uvarint,
    write_zigzag_varint, ColumnDef, Error, Result, Row, TableSchema, Value,
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
}

impl WalOp {
    fn write_binary(&self, out: &mut Vec<u8>) {
        match self {
            WalOp::CreateTable(schema) => {
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
        let f = match self.file.as_mut() {
            Some(f) => f,
            None => return Ok(Vec::new()),
        };
        f.seek(SeekFrom::Start(0))?;
        let mut raw = Vec::new();
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
            if crc32fast::hash(payload) != expected_crc {
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
        Ok(frames)
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
        let crc = crc32fast::hash(&payload);

        let mut header = [0u8; 20];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..12].copy_from_slice(&base_generation.to_le_bytes());
        header[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
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
        if self.read_only {
            return Ok(());
        }
        if let Some(f) = self.file.as_mut() {
            f.set_len(0)?;
            f.seek(SeekFrom::Start(0))?;
            f.sync_data()?;
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

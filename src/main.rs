use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

use squishdb::pax::CompressionProfile;
use squishdb::types::{ColumnDef, ColumnType, Error, Result, Value};
use squishdb::{migrate_sqlite, Database, DbOptions, MigrateOptions, QueryResult};

const MAX_STREAM_ITEM_BYTES: usize = 256 * 1024 * 1024; // 256 MiB safety cap

fn print_usage() {
    eprintln!("SquishDB v0.5.0-alpha.1, a compressed single-file SQL database.");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  squishdb <file.sqdb>                                  Interactive SQL REPL");
    eprintln!("  squishdb <file.sqdb> -c \"<SQL>\"                       Execute SQL statement(s)");
    eprintln!("  squishdb stats <file.sqdb>                            Show storage, index & cache stats");
    eprintln!("  squishdb migrate <src.sqlite> <dst.sqdb> [--profile P] [--cluster tbl:c1,c2] [--threads N]");
    eprintln!("                                                        Migrate SQLite 3 file directly to .sqdb");
    eprintln!("  squishdb import-bin <file.sqdb> [profile] [col1,col2] Bulk import (+ optional cluster keys)");
    eprintln!("  squishdb export-bin <file.sqdb> <table>               Exact lossless binary export");
    eprintln!("  squishdb bench-lookup <file.sqdb> < queries.sql       Measure query latency from stdin");
    eprintln!("  squishdb rpc <file.sqdb> [--readonly] [--profile P] [--sync-wal]");
    eprintln!("                                                        Persistent JSON-RPC over stdio");
    eprintln!();
    eprintln!("Profiles: fast | balanced (default) | max | none");
}

/// Returns true if `sql` has a top-level terminating `;` outside single/double quotes and `--` comments.
fn has_complete_statement(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut last_non_ws = b'\0';
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'\'' {
            in_single = true;
            last_non_ws = b;
        } else if b == b'"' {
            in_double = true;
            last_non_ws = b;
        } else if !b.is_ascii_whitespace() {
            last_non_ws = b;
        }
        i += 1;
    }
    !in_single && !in_double && last_non_ws == b';'
}

fn parse_profile(s: &str) -> CompressionProfile {
    match s.to_ascii_lowercase().as_str() {
        "fast" => CompressionProfile::Fast,
        "max" => CompressionProfile::Max,
        "none" => CompressionProfile::None,
        _ => CompressionProfile::Balanced,
    }
}

fn print_query_results(results: &[QueryResult]) {
    for res in results {
        match res {
            QueryResult::Rows { columns, rows } => {
                println!("{}", columns.join(" | "));
                println!("{}", "-".repeat((columns.len() * 14).max(12)));
                for r in rows {
                    let line: Vec<String> = r.iter().map(|v| v.to_string()).collect();
                    println!("{}", line.join(" | "));
                }
                println!("({} rows)", rows.len());
            }
            QueryResult::Affected(n) => println!("OK ({} rows affected)", n),
            QueryResult::Message(m) => println!("{}", m),
        }
    }
}

fn run_repl(db_path: &Path) -> Result<()> {
    let mut db = Database::open(db_path)?;
    println!("SquishDB opened: {}", db_path.display());
    println!("Type SQL ending with ';', or '.stats', '.vacuum', '.tables', '.indexes', '.quit'");

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut buffer = String::new();

    for line_res in stdin.lock().lines() {
        let line = line_res?;
        let trimmed = line.trim();
        if buffer.is_empty() {
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.eq_ignore_ascii_case(".quit") || trimmed.eq_ignore_ascii_case(".exit") {
                break;
            }
            if trimmed.eq_ignore_ascii_case(".stats") {
                println!("{}", db.stats()?.format_report());
                continue;
            }
            if trimmed.eq_ignore_ascii_case(".vacuum") {
                db.vacuum()?;
                println!("VACUUM complete.");
                continue;
            }
            if trimmed.eq_ignore_ascii_case(".tables") {
                println!("{}", db.table_names().join(", "));
                continue;
            }
            if trimmed.eq_ignore_ascii_case(".indexes") {
                println!("{}", db.index_names().join(", "));
                continue;
            }
        }
        buffer.push_str(&line);
        buffer.push('\n');
        if has_complete_statement(&buffer) {
            match db.execute(&buffer) {
                Ok(res) => print_query_results(&res),
                Err(e) => eprintln!("Error: {}", e),
            }
            buffer.clear();
            let _ = stdout.flush();
        }
    }
    if db.is_dirty() && !db.in_transaction() {
        db.checkpoint()?;
    }
    Ok(())
}

fn read_u32_le<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_le<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_u8_one<R: Read>(r: &mut R) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn run_import_bin(
    db_path: &Path,
    profile: CompressionProfile,
    cluster_by: Vec<String>,
) -> Result<()> {
    let mut opts = DbOptions::default();
    opts.compression = profile;
    let mut db = Database::open_with_options(db_path, opts)?;

    let stdin = io::stdin();
    let mut reader = BufReader::with_capacity(1 << 20, stdin.lock());

    let name_len = read_u32_le(&mut reader)? as usize;
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf)?;
    let table_name = String::from_utf8(name_buf)
        .map_err(|e| Error::InvalidData(format!("Invalid table name UTF-8: {}", e)))?;

    let num_cols = read_u32_le(&mut reader)? as usize;
    let mut cols = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        let clen = read_u32_le(&mut reader)? as usize;
        let mut cbuf = vec![0u8; clen];
        reader.read_exact(&mut cbuf)?;
        let cname = String::from_utf8(cbuf)
            .map_err(|e| Error::InvalidData(format!("Invalid col name UTF-8: {}", e)))?;
        let ctype = ColumnType::from_u8(read_u8_one(&mut reader)?)?;
        let flags = read_u8_one(&mut reader)?;
        cols.push(ColumnDef {
            name: cname,
            col_type: ctype,
            primary_key: (flags & 1) != 0,
            not_null: (flags & 2) != 0,
            unique: (flags & 4) != 0,
            default_value: None,
        });
    }

    let do_cluster = !cluster_by.is_empty();
    if db.table_schema(&table_name).is_none() {
        db.create_table_clustered(&table_name, cols, cluster_by)?;
    }

    let num_rows = read_u64_le(&mut reader)?;
    let chunk_size = 4096usize;
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(chunk_size);

    for _ in 0..num_rows {
        let mut row = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let tag = read_u8_one(&mut reader)?;
            let val = match tag {
                0 => Value::Null,
                1 => Value::Integer(read_u64_le(&mut reader)? as i64),
                2 => Value::Real(f64::from_bits(read_u64_le(&mut reader)?)),
                3 => {
                    let len = read_u32_le(&mut reader)? as usize;
                    if len > MAX_STREAM_ITEM_BYTES {
                        return Err(Error::InvalidData(format!(
                            "Stream text length {} exceeds safety cap",
                            len
                        )));
                    }
                    let mut buf = vec![0u8; len];
                    reader.read_exact(&mut buf)?;
                    let s = String::from_utf8(buf)
                        .map_err(|e| Error::InvalidData(format!("Invalid UTF-8: {}", e)))?;
                    Value::Text(s)
                }
                4 => {
                    let len = read_u32_le(&mut reader)? as usize;
                    if len > MAX_STREAM_ITEM_BYTES {
                        return Err(Error::InvalidData(format!(
                            "Stream blob length {} exceeds safety cap",
                            len
                        )));
                    }
                    let mut buf = vec![0u8; len];
                    reader.read_exact(&mut buf)?;
                    Value::Blob(buf)
                }
                _ => return Err(Error::InvalidData(format!("Unknown stream tag: {}", tag))),
            };
            row.push(val);
        }
        batch.push(row);
        if batch.len() >= chunk_size {
            db.bulk_insert(&table_name, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.bulk_insert(&table_name, batch)?;
    }

    if do_cluster {
        db.vacuum()?;
    } else {
        db.checkpoint()?;
    }
    Ok(())
}

fn run_export_bin(db_path: &Path, table_name: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;
    let rows = db.scan_all(table_name)?;

    let stdout = io::stdout();
    let mut w = BufWriter::with_capacity(1 << 20, stdout.lock());
    w.write_all(&(rows.len() as u64).to_le_bytes())?;
    for r in &rows {
        for v in &r.values {
            match v {
                Value::Null => w.write_all(&[0u8])?,
                Value::Integer(i) => {
                    w.write_all(&[1u8])?;
                    w.write_all(&(*i as u64).to_le_bytes())?;
                }
                Value::Real(f) => {
                    w.write_all(&[2u8])?;
                    w.write_all(&f.to_bits().to_le_bytes())?;
                }
                Value::Text(s) => {
                    w.write_all(&[3u8])?;
                    w.write_all(&(s.len() as u32).to_le_bytes())?;
                    w.write_all(s.as_bytes())?;
                }
                Value::Blob(b) => {
                    w.write_all(&[4u8])?;
                    w.write_all(&(b.len() as u32).to_le_bytes())?;
                    w.write_all(b)?;
                }
            }
        }
    }
    w.flush()?;
    Ok(())
}

fn run_bench_lookup(db_path: &Path) -> Result<()> {
    let mut db = Database::open(db_path)?;
    let stdin = io::stdin();
    let queries: Vec<String> = stdin
        .lock()
        .lines()
        .filter_map(|l| l.ok())
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let t0 = Instant::now();
    let mut total_rows = 0usize;
    for q in &queries {
        let res = db.execute(q)?;
        for r in res {
            if let QueryResult::Rows { rows, .. } = r {
                total_rows += rows.len();
            }
        }
    }
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let stats = db.stats()?;
    println!(
        "{{\"queries\":{},\"matched_rows\":{},\"elapsed_ms\":{:.3},\"avg_ms\":{:.4},\"cache_hits\":{},\"cache_misses\":{}}}",
        queries.len(),
        total_rows,
        elapsed_ms,
        if queries.is_empty() {
            0.0
        } else {
            elapsed_ms / (queries.len() as f64)
        },
        stats.cache_hits,
        stats.cache_misses
    );
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let res = match args[1].as_str() {
        "stats" => {
            if args.len() < 3 {
                print_usage();
                std::process::exit(1);
            }
            Database::open(&args[2]).and_then(|db| {
                println!("{}", db.stats()?.format_report());
                Ok(())
            })
        }
        "migrate" | "import-sqlite" => {
            if args.len() < 4 {
                print_usage();
                std::process::exit(1);
            }
            let src = Path::new(&args[2]);
            let dst = Path::new(&args[3]);
            let mut mopts = MigrateOptions::default();
            let mut cluster_map: HashMap<String, Vec<String>> = HashMap::new();
            let mut i = 4;
            while i < args.len() {
                match args[i].as_str() {
                    "--profile" if i + 1 < args.len() => {
                        mopts.compression = parse_profile(&args[i + 1]);
                        i += 1;
                    }
                    "--threads" if i + 1 < args.len() => {
                        mopts.parallel_workers = args[i + 1].parse().unwrap_or(0);
                        i += 1;
                    }
                    "--cluster" if i + 1 < args.len() => {
                        if let Some((tbl, cols)) = args[i + 1].split_once(':') {
                            let list: Vec<String> = cols
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                            cluster_map.insert(tbl.trim().to_ascii_lowercase(), list);
                        }
                        i += 1;
                    }
                    _ => {}
                }
                i += 1;
            }
            mopts.cluster_by = cluster_map;
            let t0 = Instant::now();
            migrate_sqlite(src, dst, mopts).map(|rep| {
                let ratio = if rep.sqdb_bytes > 0 {
                    (rep.sqlite_bytes as f64) / (rep.sqdb_bytes as f64)
                } else {
                    1.0
                };
                println!(
                    "Migrated {} tables, {} indexes, {} rows in {:.2}s: {} bytes -> {} bytes ({:.2}x smaller)",
                    rep.tables_migrated,
                    rep.indexes_migrated,
                    rep.total_rows,
                    t0.elapsed().as_secs_f64(),
                    rep.sqlite_bytes,
                    rep.sqdb_bytes,
                    ratio
                );
            })
        }
        "import-bin" => {
            if args.len() < 3 {
                print_usage();
                std::process::exit(1);
            }
            let profile = args
                .get(3)
                .map(|s| parse_profile(s))
                .unwrap_or(CompressionProfile::Balanced);
            let cluster_by = args
                .get(4)
                .map(|s| {
                    s.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            run_import_bin(Path::new(&args[2]), profile, cluster_by)
        }
        "export-bin" => {
            if args.len() < 4 {
                print_usage();
                std::process::exit(1);
            }
            run_export_bin(Path::new(&args[2]), &args[3])
        }
        "bench-lookup" => {
            if args.len() < 3 {
                print_usage();
                std::process::exit(1);
            }
            run_bench_lookup(Path::new(&args[2]))
        }
        "rpc" => {
            if args.len() < 3 {
                print_usage();
                std::process::exit(1);
            }
            let db_path = &args[2];
            let mut readonly = false;
            let mut opts = DbOptions::default();
            let mut i = 3;
            while i < args.len() {
                match args[i].as_str() {
                    "--readonly" => readonly = true,
                    "--sync-wal" => opts.sync_wal = true,
                    "--profile" if i + 1 < args.len() => {
                        opts.compression = parse_profile(&args[i + 1]);
                        i += 1;
                    }
                    _ => {}
                }
                i += 1;
            }
            let db_res = if readonly {
                Database::open_readonly(db_path)
            } else {
                Database::open_with_options(db_path, opts)
            };
            db_res.and_then(squishdb::rpc::run_rpc_loop)
        }
        "-h" | "--help" | "help" => {
            print_usage();
            Ok(())
        }
        db_file => {
            if args.get(2).map(|s| s.as_str()) == Some("-c") {
                if args.len() < 4 {
                    print_usage();
                    std::process::exit(1);
                }
                Database::open(db_file).and_then(|mut db| {
                    let out = db.execute(&args[3])?;
                    print_query_results(&out);
                    if db.is_dirty() && !db.in_transaction() {
                        db.checkpoint()?;
                    }
                    Ok(())
                })
            } else {
                run_repl(Path::new(db_file))
            }
        }
    };

    if let Err(e) = res {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

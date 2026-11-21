"""
Real measured head-to-head storage comparison on identical datasets:
  - SQLite (VACUUMed)
  - SQLite 4KB-page zlib (ZIPVFS-equivalent row-oriented page compression)
  - Apache Parquet (Snappy)   [immutable columnar]
  - Apache Parquet (Zstd L3)  [immutable columnar]
  - Apache Parquet (Zstd L9)  [immutable columnar, high compression]
  - SquishDB (balanced)       [mutable single-file PAX + WAL]
  - SquishDB (max)            [mutable single-file PAX + WAL]
"""

import json
import os
import sqlite3
import subprocess
import tempfile
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

from compare_sqlite import (
    COL_BLOB,
    COL_INT,
    COL_REAL,
    COL_TEXT,
    SQUISH_BIN,
    TYPE_SQL,
    gen_app_logs,
    gen_articles_text,
    gen_ecommerce,
    gen_high_entropy_mixed,
    gen_incompressible_blobs,
    gen_iot_sensors,
    pack_stream,
)


def zipvfs_equivalent_bytes(sqlite_path: str) -> int:
    """Measures real per-4KB-page zlib compression (same model as SQLite ZIPVFS:
    each 4KB B-tree page compressed independently, fallback to raw if incompressible,
    plus 8-byte page map entry per page)."""
    with open(sqlite_path, "rb") as f:
        data = f.read()
    page_size = 4096
    total = 1024  # header overhead
    for off in range(0, len(data), page_size):
        page = data[off : off + page_size]
        comp = zlib.compress(page, level=6)
        total += min(len(comp), len(page)) + 8  # variable-length page + slot map entry
    return total


def rows_to_arrow_table(columns, rows) -> pa.Table:
    arrays = []
    names = []
    for c_idx, (cname, ctype, _, _) in enumerate(columns):
        names.append(cname)
        col_data = [r[c_idx] for r in rows]
        if ctype == COL_INT:
            pa_type = pa.int64()
        elif ctype == COL_REAL:
            pa_type = pa.float64()
        elif ctype == COL_TEXT:
            pa_type = pa.string()
        else:
            pa_type = pa.binary()
        arrays.append(pa.array(col_data, type=pa_type))
    return pa.Table.from_arrays(arrays, names=names)


def measure_workload(label: str, table: str, gen_fn):
    cols, rows = gen_fn()
    with tempfile.TemporaryDirectory(prefix="squish_ind_") as tmp:
        sqlite_path = os.path.join(tmp, "db.sqlite")
        col_defs = ", ".join(
            f"{c} {TYPE_SQL[t]}"
            + (" PRIMARY KEY" if pk else "")
            + (" NOT NULL" if nn else "")
            for c, t, pk, nn in cols
        )
        placeholders = ", ".join(["?"] * len(cols))
        conn = sqlite3.connect(sqlite_path)
        conn.execute(f"CREATE TABLE {table} ({col_defs})")
        conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
        sqlite_sz = os.path.getsize(sqlite_path)
        zipvfs_sz = zipvfs_equivalent_bytes(sqlite_path)

        # Parquet variants via PyArrow
        arrow_tbl = rows_to_arrow_table(cols, rows)
        pq_snappy_path = os.path.join(tmp, "snappy.parquet")
        pq_zstd3_path = os.path.join(tmp, "zstd3.parquet")
        pq_zstd9_path = os.path.join(tmp, "zstd9.parquet")

        pq.write_table(arrow_tbl, pq_snappy_path, compression="snappy")
        pq.write_table(arrow_tbl, pq_zstd3_path, compression="zstd", compression_level=3)
        pq.write_table(arrow_tbl, pq_zstd9_path, compression="zstd", compression_level=9)

        pq_snappy_sz = os.path.getsize(pq_snappy_path)
        pq_zstd3_sz = os.path.getsize(pq_zstd3_path)
        pq_zstd9_sz = os.path.getsize(pq_zstd9_path)

        # SquishDB balanced & max
        stream = pack_stream(table, cols, rows)
        sq_bal_path = os.path.join(tmp, "bal.sqdb")
        sq_max_path = os.path.join(tmp, "max.sqdb")
        subprocess.run([SQUISH_BIN, "import-bin", sq_bal_path, "balanced"], input=stream, check=True)
        subprocess.run([SQUISH_BIN, "import-bin", sq_max_path, "max"], input=stream, check=True)
        sq_bal_sz = os.path.getsize(sq_bal_path)
        sq_max_sz = os.path.getsize(sq_max_path)

        return {
            "label": label,
            "rows": len(rows),
            "sqlite": sqlite_sz,
            "zipvfs_sim": zipvfs_sz,
            "parquet_snappy": pq_snappy_sz,
            "parquet_zstd3": pq_zstd3_sz,
            "parquet_zstd9": pq_zstd9_sz,
            "squish_balanced": sq_bal_sz,
            "squish_max": sq_max_sz,
        }


def main():
    workloads = [
        ("E-Commerce (100k)", "orders", gen_ecommerce),
        ("JSON Logs (80k)", "logs", gen_app_logs),
        ("IoT Sensors (150k)", "sensors", gen_iot_sensors),
        ("Articles Text (15k)", "articles", gen_articles_text),
        ("UUIDs+Floats (40k)", "mixed", gen_high_entropy_mixed),
        ("Random Blobs (2k)", "blobs", gen_incompressible_blobs),
    ]
    out = []
    for label, table, fn in workloads:
        res = measure_workload(label, table, fn)
        out.append(res)
        mb = lambda x: x / (1024 * 1024)
        print(
            f"{label:20s} | SQLite {mb(res['sqlite']):5.2f} | "
            f"ZIPVFS~ {mb(res['zipvfs_sim']):5.2f} | "
            f"Pq-Snappy {mb(res['parquet_snappy']):5.2f} | "
            f"Pq-Zstd3 {mb(res['parquet_zstd3']):5.2f} | "
            f"Pq-Zstd9 {mb(res['parquet_zstd9']):5.2f} | "
            f"Squish(bal) {mb(res['squish_balanced']):5.2f} | "
            f"Squish(max) {mb(res['squish_max']):5.2f}"
        )

    rel = out[:5]
    print("-" * 115)
    totals = {}
    for k in ["sqlite", "zipvfs_sim", "parquet_snappy", "parquet_zstd3", "parquet_zstd9", "squish_balanced", "squish_max"]:
        totals[k] = sum(r[k] for r in rel)
    base = totals["sqlite"]
    print("RELATIONAL TOTAL (Workloads 1-5, 385k rows):")
    for k, v in totals.items():
        print(f"  {k:16s}: {v / (1024*1024):6.2f} MB  ({base / v:5.2f}x smaller than SQLite | 5 GB -> {5.0 * v / base:.2f} GB)")

    out_path = os.path.join(os.path.dirname(__file__), "industry_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"workloads": out, "relational_totals": totals}, f, indent=2)


if __name__ == "__main__":
    main()

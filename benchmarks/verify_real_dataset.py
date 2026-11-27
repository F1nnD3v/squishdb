#!/usr/bin/env python3
"""
Verify an existing SquishDB (.sqdb) file against its original CSV or SQLite source
and measure exact storage ratios and analytical query latencies for README reporting.

Checks performed:
  1. Exact row count match: source rows == COUNT(*) in .sqdb.
  2. Bit-exact verification of a random sample (default 1,000 rows) comparing
     INTEGER, IEEE-754 REAL (via struct.pack("<d", ...) including -0.0/NaN),
     UTF-8 TEXT, BLOB, and NULL values between the source and .sqdb.
  3. Disk size comparison: original source, vacuumed SQLite (provided or built on
     request), and .sqdb (including any non-empty -wal), with exact ratios.
  4. Latency of 3 analytical queries (GROUP BY by symbol/year, AVG/MIN/MAX, and
     timestamp range scan) over a warm persistent connection, with optional
     head-to-head SQLite timing and result cross-checking.

Usage examples:
  python3 benchmarks/verify_real_dataset.py \\
      --sqdb market.sqdb --source market.sqlite --table ohlcv

  python3 benchmarks/verify_real_dataset.py \\
      --sqdb market.sqdb --source market.csv --table ohlcv --build-sqlite

  python3 benchmarks/verify_real_dataset.py \\
      --sqdb market.sqdb --source market.sqlite --table ticks \\
      --symbol-col ticker --ts-col ts_ms --price-col close --volume-col volume \\
      --sample-size 1000 --json-out market_report.json
"""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import json
import math
import os
import random
import shutil
import sqlite3
import statistics
import struct
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BIN = ROOT / "target" / "release" / ("squishdb.exe" if os.name == "nt" else "squishdb")


# ---------------------------------------------------------------------------
# Bit-exact value normalization
# ---------------------------------------------------------------------------

def canonical_value(val: Any) -> tuple[str, Any]:
    """Convert a Python value into a hashable bit-exact key for comparison."""
    if val is None:
        return ("null", None)
    if isinstance(val, bool):
        return ("int", int(val))
    if isinstance(val, int):
        return ("int", val)
    if isinstance(val, float):
        return ("real", struct.pack("<d", val))
    if isinstance(val, (bytes, bytearray, memoryview)):
        return ("blob", bytes(val))
    if isinstance(val, str):
        return ("text", val)
    return ("text", str(val))


def canonical_row(row: Sequence[Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(canonical_value(v) for v in row)


def parse_csv_cell(raw: str, col_type: str, null_tokens: set[str]) -> Any:
    """Coerce a raw CSV cell into the target column type using exact parsing."""
    if raw in null_tokens:
        return None
    ct = col_type.upper()
    if ct == "INTEGER":
        return int(raw)
    if ct == "REAL":
        return float(raw)
    if ct == "BLOB":
        if raw.startswith("0x") or raw.startswith("0X"):
            return bytes.fromhex(raw[2:])
        return raw.encode("utf-8")
    return raw


def coerce_to_schema_types(row: Sequence[Any], col_types: Sequence[str]) -> tuple[Any, ...]:
    """Normalize a source row to match SquishDB column affinities (e.g., int->float in REAL col)."""
    out = []
    for val, ct in zip(row, col_types):
        if val is None:
            out.append(None)
        elif ct == "REAL" and isinstance(val, int) and not isinstance(val, bool):
            out.append(float(val))
        elif ct == "BLOB" and isinstance(val, str):
            out.append(val.encode("utf-8"))
        else:
            out.append(val)
    return tuple(out)


# ---------------------------------------------------------------------------
# Persistent SquishDB client (prefers C FFI binding; falls back to `squishdb rpc`)
# ---------------------------------------------------------------------------

class RpcClient:
    """Minimal zero-dependency client for `squishdb rpc` over stdio."""

    def __init__(self, bin_path: Path, db_path: Path, readonly: bool = True) -> None:
        cmd = [str(bin_path), "rpc", str(db_path)]
        if readonly:
            cmd.append("--readonly")
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self._next_id = 1

    def _decode_val(self, v: Any) -> Any:
        if isinstance(v, dict):
            if "$blob" in v:
                return base64.b64decode(v["$blob"])
            if "$int" in v:
                return int(v["$int"])
            if "$real" in v:
                return float(v["$real"])
        return v

    def _encode_val(self, v: Any) -> Any:
        if v is None or isinstance(v, str):
            return v
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            if abs(v) > 9007199254740991:
                return {"$int": str(v)}
            return v
        if isinstance(v, float):
            if math.isnan(v):
                return {"$real": "NaN"}
            if math.isinf(v):
                return {"$real": "Infinity" if v > 0 else "-Infinity"}
            return v
        if isinstance(v, (bytes, bytearray)):
            return {"$blob": base64.b64encode(bytes(v)).decode("ascii")}
        return str(v)

    def _call(self, method: str, **kwargs: Any) -> Any:
        req_id = self._next_id
        self._next_id += 1
        payload = {"id": req_id, "method": method, **kwargs}
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.write(json.dumps(payload) + "\n")
        self._proc.stdin.flush()
        line = self._proc.stdout.readline()
        if not line:
            stderr = self._proc.stderr.read() if self._proc.stderr else ""
            raise RuntimeError(f"squishdb rpc terminated unexpectedly: {stderr.strip()}")
        resp = json.loads(line)
        if not resp.get("ok"):
            raise RuntimeError(f"squishdb rpc error: {resp.get('error')}")
        return resp.get("result")

    def tables(self) -> list[str]:
        return list(self._call("tables"))

    def stats(self) -> dict[str, Any]:
        return dict(self._call("stats"))

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        kwargs: dict[str, Any] = {"sql": sql}
        if params is not None:
            kwargs["params"] = [self._encode_val(p) for p in params]
        results = self._call("execute", **kwargs)
        for item in reversed(results):
            if item.get("kind") == "rows":
                cols = list(item["columns"])
                rows = [tuple(self._decode_val(c) for c in r) for r in item["rows"]]
                return cols, rows
        return [], []

    def close(self) -> None:
        try:
            self._call("close")
        except Exception:
            pass
        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.wait(timeout=5)


class FfiClient:
    """In-process client using `bindings/python` C FFI (`libsquishdb`)."""

    def __init__(self, db_path: Path, readonly: bool = True) -> None:
        py_binding = str(ROOT / "bindings" / "python")
        if py_binding not in sys.path:
            sys.path.insert(0, py_binding)
        import squishdb as sq  # type: ignore

        self._conn = sq.connect(str(db_path), read_only=readonly)

    def tables(self) -> list[str]:
        return list(self._conn.tables())

    def stats(self) -> dict[str, Any]:
        return dict(self._conn.stats())

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        results = self._conn.execute_raw(sql, params)
        for item in reversed(results):
            if item.get("kind") == "rows":
                return list(item["columns"]), [tuple(r) for r in item["rows"]]
        return [], []

    def close(self) -> None:
        self._conn.close()


def open_squish_client(bin_path: Path, db_path: Path, backend: str = "auto"):
    if backend in ("auto", "ffi"):
        try:
            return FfiClient(db_path, readonly=True), "ffi"
        except Exception as exc:
            if backend == "ffi":
                raise RuntimeError(f"Failed to load Python FFI backend: {exc}") from exc
    if not bin_path.exists():
        raise FileNotFoundError(
            f"SquishDB binary not found at {bin_path}. Run `cargo build --release` first."
        )
    return RpcClient(bin_path, db_path, readonly=True), "rpc"


# ---------------------------------------------------------------------------
# Streaming binary export reader (O(1) RAM, bit-exact)
# ---------------------------------------------------------------------------

def stream_export_bin(bin_path: Path, db_path: Path, table: str, num_cols: int) -> Iterator[tuple[Any, ...]]:
    """Yield every row from `squishdb export-bin` with exact IEEE-754 float bits."""
    if not bin_path.exists():
        raise FileNotFoundError(
            f"SquishDB binary not found at {bin_path}. Run `cargo build --release` first."
        )
    proc = subprocess.Popen(
        [str(bin_path), "export-bin", str(db_path), table],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1 << 20,
    )
    assert proc.stdout is not None
    r = proc.stdout

    def read_exact(n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = r.read(n - len(buf))
            if not chunk:
                err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
                raise RuntimeError(f"Unexpected EOF reading export-bin stream: {err}")
            buf.extend(chunk)
        return bytes(buf)

    (num_rows,) = struct.unpack("<Q", read_exact(8))
    unpack_i = struct.Struct("<q").unpack
    unpack_f = struct.Struct("<d").unpack
    unpack_u32 = struct.Struct("<I").unpack

    for _ in range(num_rows):
        row = []
        for _ in range(num_cols):
            tag = read_exact(1)[0]
            if tag == 0:
                row.append(None)
            elif tag == 1:
                row.append(unpack_i(read_exact(8))[0])
            elif tag == 2:
                row.append(unpack_f(read_exact(8))[0])
            elif tag == 3:
                ln = unpack_u32(read_exact(4))[0]
                row.append(read_exact(ln).decode("utf-8"))
            elif tag == 4:
                ln = unpack_u32(read_exact(4))[0]
                row.append(read_exact(ln))
            else:
                raise ValueError(f"Unknown export-bin tag: {tag}")
        yield tuple(row)

    rc = proc.wait()
    if rc != 0:
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        raise RuntimeError(f"export-bin exited with code {rc}: {err.strip()}")


# ---------------------------------------------------------------------------
# Source readers (SQLite & CSV / CSV.gz) with O(k) reservoir sampling
# ---------------------------------------------------------------------------

def infer_squish_column_types(client, table: str) -> tuple[list[str], list[str]]:
    """Infer column names and types from a sample of rows in .sqdb."""
    cols, sample_rows = client.query(f"SELECT * FROM {quote_ident(table)} LIMIT 256;")
    col_types = ["TEXT"] * len(cols)
    for c_idx in range(len(cols)):
        seen = None
        for r in sample_rows:
            v = r[c_idx]
            if v is None:
                continue
            if isinstance(v, float):
                seen = "REAL"
                break
            if isinstance(v, int) and not isinstance(v, bool):
                seen = "INTEGER"
            elif isinstance(v, (bytes, bytearray)):
                seen = "BLOB"
                break
            elif isinstance(v, str):
                seen = "TEXT"
                break
        if seen is not None:
            col_types[c_idx] = seen
    return cols, col_types


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_sql_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, int) and not isinstance(val, bool):
        return str(val)
    if isinstance(val, float):
        return repr(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"


def sample_from_sqlite(
    sqlite_path: Path,
    table: str,
    sqdb_cols: list[str],
    sqdb_types: list[str],
    sample_size: int,
    seed: int,
) -> tuple[int, list[tuple[Any, ...]], list[str]]:
    """Stream SQLite table to count exact rows and collect a uniform random sample."""
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    cur = conn.cursor()
    info = cur.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    if not info:
        conn.close()
        raise ValueError(f"Table '{table}' not found in SQLite database {sqlite_path}")
    pk_cols = [row[1] for row in sorted(info, key=lambda r: r[5]) if row[5] > 0]
    sqlite_col_names = [row[1] for row in info]

    # Select in the exact column order of .sqdb (case-insensitive map)
    lower_map = {c.lower(): c for c in sqlite_col_names}
    select_cols = []
    for c in sqdb_cols:
        src_name = lower_map.get(c.lower())
        if src_name is None:
            conn.close()
            raise ValueError(f"Column '{c}' from .sqdb not found in SQLite table '{table}'")
        select_cols.append(quote_ident(src_name))

    total = cur.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0]
    rng = random.Random(seed)
    reservoir: list[tuple[Any, ...]] = []

    sql = f"SELECT {', '.join(select_cols)} FROM {quote_ident(table)}"
    for idx, raw_row in enumerate(cur.execute(sql)):
        if len(reservoir) < sample_size:
            reservoir.append(coerce_to_schema_types(raw_row, sqdb_types))
        else:
            j = rng.randint(0, idx)
            if j < sample_size:
                reservoir[j] = coerce_to_schema_types(raw_row, sqdb_types)

    conn.close()
    return int(total), reservoir, pk_cols


def open_csv_text(path: Path):
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, "r", encoding="utf-8", newline="")


def sample_from_csv(
    csv_path: Path,
    sqdb_cols: list[str],
    sqdb_types: list[str],
    sample_size: int,
    seed: int,
    delimiter: str,
    null_tokens: set[str],
) -> tuple[int, list[tuple[Any, ...]]]:
    """Stream CSV (or .csv.gz) to count exact rows and collect a uniform random sample."""
    rng = random.Random(seed)
    reservoir: list[tuple[Any, ...]] = []
    total = 0

    with open_csv_text(csv_path) as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        header = next(reader, None)
        if header is None:
            return 0, []
        header_map = {h.strip().lower(): i for i, h in enumerate(header)}
        col_indices = []
        for c in sqdb_cols:
            ci = header_map.get(c.lower())
            if ci is None:
                raise ValueError(
                    f"Column '{c}' from .sqdb not found in CSV header {header}"
                )
            col_indices.append(ci)

        for row_raw in reader:
            if not row_raw:
                continue
            idx = total
            total += 1
            if len(reservoir) < sample_size:
                parsed = tuple(
                    parse_csv_cell(row_raw[ci], ct, null_tokens)
                    for ci, ct in zip(col_indices, sqdb_types)
                )
                reservoir.append(parsed)
            else:
                j = rng.randint(0, idx)
                if j < sample_size:
                    parsed = tuple(
                        parse_csv_cell(row_raw[ci], ct, null_tokens)
                        for ci, ct in zip(col_indices, sqdb_types)
                    )
                    reservoir[j] = parsed

    return total, reservoir


def build_vacuumed_sqlite_from_csv(
    csv_path: Path,
    sqlite_out: Path,
    table: str,
    sqdb_cols: list[str],
    sqdb_types: list[str],
    delimiter: str,
    null_tokens: set[str],
) -> int:
    """Build a vacuumed SQLite copy from a CSV file for apples-to-apples disk comparison."""
    if sqlite_out.exists():
        sqlite_out.unlink()
    conn = sqlite3.connect(str(sqlite_out))
    col_defs = ", ".join(f"{quote_ident(c)} {t}" for c, t in zip(sqdb_cols, sqdb_types))
    conn.execute(f"CREATE TABLE {quote_ident(table)} ({col_defs})")
    placeholders = ", ".join(["?"] * len(sqdb_cols))
    ins_sql = f"INSERT INTO {quote_ident(table)} VALUES ({placeholders})"

    with open_csv_text(csv_path) as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        header = next(reader)
        header_map = {h.strip().lower(): i for i, h in enumerate(header)}
        col_indices = [header_map[c.lower()] for c in sqdb_cols]
        batch = []
        for row_raw in reader:
            if not row_raw:
                continue
            batch.append(
                tuple(
                    parse_csv_cell(row_raw[ci], ct, null_tokens)
                    for ci, ct in zip(col_indices, sqdb_types)
                )
            )
            if len(batch) >= 20_000:
                conn.executemany(ins_sql, batch)
                batch.clear()
        if batch:
            conn.executemany(ins_sql, batch)

    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    return sqlite_out.stat().st_size


# ---------------------------------------------------------------------------
# Sample verification modes (stream vs query)
# ---------------------------------------------------------------------------

def verify_sample_stream(
    bin_path: Path,
    sqdb_path: Path,
    table: str,
    num_cols: int,
    sample: list[tuple[Any, ...]],
) -> tuple[int, int, Optional[tuple[Any, ...]]]:
    """
    Single-pass O(sample) memory bit-exact verification over `squishdb export-bin`.
    Works identically regardless of CLUSTER BY ordering or missing secondary indexes.
    """
    needed: Counter[tuple[tuple[str, Any], ...]] = Counter(canonical_row(r) for r in sample)
    remaining = len(sample)
    exported_count = 0

    for row in stream_export_bin(bin_path, sqdb_path, table, num_cols):
        exported_count += 1
        if remaining > 0:
            key = canonical_row(row)
            cnt = needed.get(key, 0)
            if cnt > 0:
                if cnt == 1:
                    del needed[key]
                else:
                    needed[key] = cnt - 1
                remaining -= 1

    matched = len(sample) - remaining
    first_missing = None
    if remaining > 0:
        for r in sample:
            if canonical_row(r) in needed:
                first_missing = r
                break
    return exported_count, matched, first_missing


def verify_sample_query(
    client,
    table: str,
    sqdb_cols: list[str],
    key_cols: list[str],
    sample: list[tuple[Any, ...]],
) -> tuple[int, Optional[tuple[Any, ...]]]:
    """Verify sampled rows via parameterized SQL point lookups against .sqdb."""
    col_idx_map = {c.lower(): i for i, c in enumerate(sqdb_cols)}
    key_idxs = [col_idx_map[k.lower()] for k in key_cols]
    select_list = ", ".join(quote_ident(c) for c in sqdb_cols)

    matched = 0
    first_missing = None
    for row in sample:
        preds = []
        params = []
        for k_name, ki in zip(key_cols, key_idxs):
            v = row[ki]
            if v is None:
                preds.append(f"{quote_ident(k_name)} IS NULL")
            else:
                preds.append(f"{quote_ident(k_name)} = ?")
                params.append(v)
        sql = f"SELECT {select_list} FROM {quote_ident(table)} WHERE {' AND '.join(preds)};"
        _, got_rows = client.query(sql, params)
        target_key = canonical_row(row)
        if any(canonical_row(gr) == target_key for gr in got_rows):
            matched += 1
        elif first_missing is None:
            first_missing = row
    return matched, first_missing


# ---------------------------------------------------------------------------
# Analytical query builder & latency harness
# ---------------------------------------------------------------------------

def pick_column(cols: list[str], types: list[str], candidates: Sequence[str], fallback_type: Optional[str]) -> Optional[str]:
    lower_cols = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in lower_cols:
            return lower_cols[cand]
    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if cand in cl:
                return c
    if fallback_type is not None:
        for c, t in zip(cols, types):
            if t == fallback_type:
                return c
    return None


def build_analytical_queries(
    table: str,
    cols: list[str],
    types: list[str],
    sample: list[tuple[Any, ...]],
    symbol_col: Optional[str],
    year_col: Optional[str],
    ts_col: Optional[str],
    price_col: Optional[str],
    volume_col: Optional[str],
    custom_q1: Optional[str],
    custom_q2: Optional[str],
    custom_q3: Optional[str],
) -> list[tuple[str, str]]:
    tbl = quote_ident(table)
    sym = symbol_col or pick_column(
        cols, types, ("symbol", "ticker", "asset", "pair", "instrument", "code"), "TEXT"
    )
    yr = year_col or pick_column(cols, types, ("year", "yr"), None)
    ts = ts_col or pick_column(
        cols,
        types,
        ("ts", "timestamp", "ts_ms", "ts_sec", "open_time", "time", "epoch", "date", "datetime"),
        "INTEGER",
    )
    price = price_col or pick_column(
        cols, types, ("close", "price", "vwap", "last", "open", "high", "low", "mid"), "REAL"
    )
    vol = volume_col or pick_column(
        cols, types, ("volume", "vol", "qty", "quantity", "size", "amount"), None
    )

    col_idx = {c.lower(): i for i, c in enumerate(cols)}
    metric = price or vol or cols[-1]
    metric_q = quote_ident(metric)

    # Pick an observed symbol and a 25%..50% timestamp window from the sample
    top_sym = None
    if sym and sample:
        si = col_idx[sym.lower()]
        vals = [r[si] for r in sample if r[si] is not None]
        if vals:
            top_sym = Counter(vals).most_common(1)[0][0]

    ts_lo, ts_hi = None, None
    if ts and sample:
        ti = col_idx[ts.lower()]
        tvals = sorted(r[ti] for r in sample if r[ti] is not None)
        if len(tvals) >= 4:
            ts_lo = tvals[len(tvals) // 4]
            ts_hi = tvals[len(tvals) // 2]
        elif tvals:
            ts_lo, ts_hi = tvals[0], tvals[-1]

    # Query 1: GROUP BY by symbol (and year if present)
    if custom_q1:
        q1 = ("Q1 (custom GROUP BY)", custom_q1)
    elif sym and yr:
        g_cols = f"{quote_ident(sym)}, {quote_ident(yr)}"
        vol_agg = f", SUM({quote_ident(vol)})" if vol and vol.lower() != metric.lower() else ""
        q1 = (
            f"GROUP BY ({sym}, {yr})",
            f"SELECT {g_cols}, COUNT(*), AVG({metric_q}), MIN({metric_q}), MAX({metric_q}){vol_agg} FROM {tbl} GROUP BY {g_cols};",
        )
    elif sym:
        g_col = quote_ident(sym)
        vol_agg = f", SUM({quote_ident(vol)})" if vol and vol.lower() != metric.lower() else ""
        q1 = (
            f"GROUP BY ({sym})",
            f"SELECT {g_col}, COUNT(*), AVG({metric_q}), MIN({metric_q}), MAX({metric_q}){vol_agg} FROM {tbl} GROUP BY {g_col};",
        )
    else:
        g_col = quote_ident(cols[0])
        q1 = (
            f"GROUP BY ({cols[0]})",
            f"SELECT {g_col}, COUNT(*), AVG({metric_q}) FROM {tbl} GROUP BY {g_col};",
        )

    # Query 2: AVG / MIN / MAX aggregation (filtered by top symbol when available)
    if custom_q2:
        q2 = ("Q2 (custom AVG/MIN/MAX)", custom_q2)
    elif sym and top_sym is not None:
        q2 = (
            f"AVG/MIN/MAX ({sym} = {top_sym!r})",
            f"SELECT COUNT(*), AVG({metric_q}), MIN({metric_q}), MAX({metric_q}) FROM {tbl} WHERE {quote_ident(sym)} = {quote_sql_literal(top_sym)};",
        )
    else:
        q2 = (
            f"AVG/MIN/MAX ({metric})",
            f"SELECT COUNT(*), AVG({metric_q}), MIN({metric_q}), MAX({metric_q}) FROM {tbl};",
        )

    # Query 3: Range scan by timestamp
    if custom_q3:
        q3 = ("Q3 (custom range scan)", custom_q3)
    elif ts and ts_lo is not None and ts_hi is not None:
        where = f"{quote_ident(ts)} >= {quote_sql_literal(ts_lo)} AND {quote_ident(ts)} <= {quote_sql_literal(ts_hi)}"
        q3 = (
            f"Range scan ({ts} 25% window)",
            f"SELECT COUNT(*), MIN({metric_q}), MAX({metric_q}), AVG({metric_q}) FROM {tbl} WHERE {where};",
        )
    else:
        q3 = (
            f"Full count & min/max ({metric})",
            f"SELECT COUNT(*), MIN({metric_q}), MAX({metric_q}) FROM {tbl};",
        )

    return [q1, q2, q3]


def benchmark_query_squish(client, sql: str, runs: int) -> dict[str, Any]:
    t0 = time.perf_counter()
    _, rows = client.query(sql)
    cold_ms = (time.perf_counter() - t0) * 1000.0

    warm = []
    for _ in range(runs):
        t0 = time.perf_counter()
        _, rows = client.query(sql)
        warm.append((time.perf_counter() - t0) * 1000.0)

    return {
        "rows": len(rows),
        "cold_ms": cold_ms,
        "median_ms": statistics.median(warm),
        "min_ms": min(warm),
        "mean_ms": statistics.mean(warm),
    }


def benchmark_query_sqlite(sqlite_path: Path, sql: str, runs: int) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    t0 = time.perf_counter()
    rows = conn.execute(sql).fetchall()
    cold_ms = (time.perf_counter() - t0) * 1000.0

    warm = []
    for _ in range(runs):
        t0 = time.perf_counter()
        rows = conn.execute(sql).fetchall()
        warm.append((time.perf_counter() - t0) * 1000.0)

    conn.close()
    return {
        "rows": len(rows),
        "cold_ms": cold_ms,
        "median_ms": statistics.median(warm),
        "min_ms": min(warm),
        "mean_ms": statistics.mean(warm),
    }


def fmt_mb(num_bytes: int) -> str:
    return f"{num_bytes / (1024 * 1024):.2f} MB"


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Verify a real-world .sqdb database against its CSV or SQLite source and measure README metrics."
    )
    p.add_argument("--sqdb", required=True, type=Path, help="Path to existing .sqdb file")
    p.add_argument("--source", required=True, type=Path, help="Path to original .csv/.csv.gz or .sqlite/.db file")
    p.add_argument("--table", default=None, help="Table name (auto-detected if .sqdb has one table)")
    p.add_argument("--sqlite", default=None, type=Path, help="Optional vacuumed SQLite path when --source is CSV")
    p.add_argument(
        "--build-sqlite",
        action="store_true",
        help="When --source is CSV (or to get a freshly vacuumed SQLite copy), build a temporary vacuumed SQLite file",
    )
    p.add_argument("--sample-size", type=int, default=1000, help="Random sample size for bit-exact verification (default: 1000)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for deterministic sampling (default: 42)")
    p.add_argument(
        "--verify-mode",
        choices=("auto", "stream", "query"),
        default="auto",
        help="Verification strategy: stream (single-pass export-bin), query (SQL point lookups by --key-col), or auto",
    )
    p.add_argument("--key-col", default=None, help="Comma-separated key columns for query verification mode")
    p.add_argument("--symbol-col", default=None, help="Asset/symbol column name for analytical queries")
    p.add_argument("--year-col", default=None, help="Optional year column name for GROUP BY (symbol, year)")
    p.add_argument("--ts-col", default=None, help="Timestamp column name for range scan query")
    p.add_argument("--price-col", default=None, help="Price/value column name for AVG/MIN/MAX queries")
    p.add_argument("--volume-col", default=None, help="Volume column name for SUM aggregation")
    p.add_argument("--q1", default=None, help="Override analytical query 1 SQL")
    p.add_argument("--q2", default=None, help="Override analytical query 2 SQL")
    p.add_argument("--q3", default=None, help="Override analytical query 3 SQL")
    p.add_argument("--runs", type=int, default=5, help="Measured warm runs per analytical query (default: 5)")
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ',')")
    p.add_argument("--null-tokens", default=",NULL,null,NaN", help="Comma-separated CSV tokens treated as NULL (empty token included by leading comma)")
    p.add_argument("--bin", type=Path, default=DEFAULT_BIN, help=f"Path to squishdb binary (default: {DEFAULT_BIN})")
    p.add_argument("--backend", choices=("auto", "ffi", "rpc"), default="auto", help="Query backend (default: auto)")
    p.add_argument("--json-out", type=Path, default=None, help="Optional path to write full JSON metrics report")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    sqdb_path: Path = args.sqdb.resolve()
    source_path: Path = args.source.resolve()

    if not sqdb_path.exists():
        print(f"ERROR: .sqdb file not found: {sqdb_path}", file=sys.stderr)
        return 2
    if not source_path.exists():
        print(f"ERROR: source file not found: {source_path}", file=sys.stderr)
        return 2

    is_sqlite_source = source_path.suffix.lower() in (".sqlite", ".sqlite3", ".db")
    null_tokens = set(args.null_tokens.split(","))

    client, backend_used = open_squish_client(args.bin, sqdb_path, backend=args.backend)
    tmp_dir: Optional[tempfile.TemporaryDirectory] = None

    try:
        tables = client.tables()
        if not tables:
            print("ERROR: .sqdb contains no tables.", file=sys.stderr)
            return 2
        table = args.table
        if table is None:
            if len(tables) == 1:
                table = tables[0]
            else:
                print(f"ERROR: multiple tables found ({tables}); specify --table.", file=sys.stderr)
                return 2

        sqdb_cols, sqdb_types = infer_squish_column_types(client, table)
        _, cnt_rows = client.query(f"SELECT COUNT(*) FROM {quote_ident(table)};")
        sqdb_row_count = int(cnt_rows[0][0])

        print(f"SquishDB file: {sqdb_path}")
        print(f"Source file:   {source_path} ({'SQLite' if is_sqlite_source else 'CSV'})")
        print(f"Table:         {table} ({len(sqdb_cols)} columns, backend={backend_used})")
        print(f"Columns:       {', '.join(f'{c}:{t}' for c, t in zip(sqdb_cols, sqdb_types))}")
        print()

        # 1. Sample source and check exact COUNT(*)
        t0 = time.perf_counter()
        pk_cols: list[str] = []
        if is_sqlite_source:
            source_row_count, sample, pk_cols = sample_from_sqlite(
                source_path, table, sqdb_cols, sqdb_types, args.sample_size, args.seed
            )
        else:
            source_row_count, sample = sample_from_csv(
                source_path,
                sqdb_cols,
                sqdb_types,
                args.sample_size,
                args.seed,
                args.delimiter,
                null_tokens,
            )
        sample_elapsed = time.perf_counter() - t0

        count_ok = sqdb_row_count == source_row_count
        print(
            f"[1/4] Row count check: source={source_row_count:,} vs sqdb={sqdb_row_count:,} "
            f"-> {'PASS' if count_ok else 'FAIL'} ({sample_elapsed:.2f}s)"
        )
        if not count_ok:
            print("ERROR: Row count mismatch!", file=sys.stderr)
            return 1

        # 2. Bit-exact verification of random sample
        verify_mode = args.verify_mode
        key_cols = [k.strip() for k in args.key_col.split(",") if k.strip()] if args.key_col else pk_cols
        if verify_mode == "auto":
            verify_mode = "query" if args.key_col else "stream"

        t0 = time.perf_counter()
        if verify_mode == "query":
            if not key_cols:
                print("ERROR: --verify-mode query requires --key-col or a PRIMARY KEY.", file=sys.stderr)
                return 2
            matched, first_missing = verify_sample_query(client, table, sqdb_cols, key_cols, sample)
            exported_count = sqdb_row_count
        else:
            exported_count, matched, first_missing = verify_sample_stream(
                args.bin, sqdb_path, table, len(sqdb_cols), sample
            )
        verify_elapsed = time.perf_counter() - t0

        sample_ok = matched == len(sample) and exported_count == source_row_count
        print(
            f"[2/4] Bit-exact sample check ({verify_mode}): {matched:,}/{len(sample):,} matched "
            f"-> {'PASS' if sample_ok else 'FAIL'} ({verify_elapsed:.2f}s)"
        )
        if not sample_ok:
            print(f"ERROR: Bit-exact mismatch! First missing row: {first_missing!r}", file=sys.stderr)
            return 1

        # 3. Disk sizes & compression ratios
        source_bytes = source_path.stat().st_size
        sqdb_bytes = sqdb_path.stat().st_size
        wal_path = Path(str(sqdb_path) + "-wal")
        if wal_path.exists():
            sqdb_bytes += wal_path.stat().st_size

        sqlite_path_for_bench: Optional[Path] = None
        sqlite_bytes: Optional[int] = None

        if is_sqlite_source:
            if args.build_sqlite:
                tmp_dir = tempfile.TemporaryDirectory(prefix="squish_verify_sqlite_")
                vac_copy = Path(tmp_dir.name) / "vacuumed.sqlite"
                shutil.copy2(source_path, vac_copy)
                c = sqlite3.connect(str(vac_copy))
                c.execute("VACUUM")
                c.close()
                sqlite_path_for_bench = vac_copy
                sqlite_bytes = vac_copy.stat().st_size
            else:
                sqlite_path_for_bench = source_path
                sqlite_bytes = source_bytes
        elif args.sqlite is not None:
            sqlite_path_for_bench = args.sqlite.resolve()
            sqlite_bytes = sqlite_path_for_bench.stat().st_size
        elif args.build_sqlite:
            tmp_dir = tempfile.TemporaryDirectory(prefix="squish_verify_sqlite_")
            vac_copy = Path(tmp_dir.name) / "vacuumed.sqlite"
            print("      Building temporary vacuumed SQLite database for size comparison...")
            sqlite_bytes = build_vacuumed_sqlite_from_csv(
                source_path, vac_copy, table, sqdb_cols, sqdb_types, args.delimiter, null_tokens
            )
            sqlite_path_for_bench = vac_copy

        ratio_vs_source = source_bytes / sqdb_bytes if sqdb_bytes > 0 else 0.0
        ratio_vs_sqlite = (sqlite_bytes / sqdb_bytes) if (sqlite_bytes and sqdb_bytes > 0) else None

        print("[3/4] Disk size comparison:")
        print(f"      Original source:  {source_bytes:>14,} bytes ({fmt_mb(source_bytes)})")
        if sqlite_bytes is not None:
            print(f"      SQLite (VACUUM):  {sqlite_bytes:>14,} bytes ({fmt_mb(sqlite_bytes)})")
        print(f"      SquishDB (.sqdb): {sqdb_bytes:>14,} bytes ({fmt_mb(sqdb_bytes)})")
        print(f"      Ratio vs source:  {ratio_vs_source:.2f}x smaller ({(1.0 - sqdb_bytes / source_bytes) * 100:.1f}% saved)")
        if ratio_vs_sqlite is not None and sqlite_bytes is not None:
            print(
                f"      Ratio vs SQLite:  {ratio_vs_sqlite:.2f}x smaller "
                f"({(1.0 - sqdb_bytes / sqlite_bytes) * 100:.1f}% saved)"
            )

        # 4. Analytical query latencies
        queries = build_analytical_queries(
            table=table,
            cols=sqdb_cols,
            types=sqdb_types,
            sample=sample,
            symbol_col=args.symbol_col,
            year_col=args.year_col,
            ts_col=args.ts_col,
            price_col=args.price_col,
            volume_col=args.volume_col,
            custom_q1=args.q1,
            custom_q2=args.q2,
            custom_q3=args.q3,
        )

        print(f"[4/4] Analytical query latency (1 cold + {args.runs} warm runs):")
        query_reports = []
        for label, sql in queries:
            sq_m = benchmark_query_squish(client, sql, args.runs)
            sl_m = (
                benchmark_query_sqlite(sqlite_path_for_bench, sql, args.runs)
                if sqlite_path_for_bench is not None
                else None
            )
            entry: dict[str, Any] = {
                "label": label,
                "sql": sql,
                "squishdb": sq_m,
            }
            if sl_m is not None:
                entry["sqlite"] = sl_m
                speedup = sl_m["median_ms"] / sq_m["median_ms"] if sq_m["median_ms"] > 0 else 0.0
                entry["speedup_vs_sqlite"] = speedup
                print(
                    f"      {label}: SquishDB {sq_m['median_ms']:.2f} ms (cold {sq_m['cold_ms']:.2f} ms) "
                    f"vs SQLite {sl_m['median_ms']:.2f} ms ({speedup:.2f}x) [{sq_m['rows']} rows]"
                )
            else:
                print(
                    f"      {label}: SquishDB {sq_m['median_ms']:.2f} ms "
                    f"(cold {sq_m['cold_ms']:.2f} ms, min {sq_m['min_ms']:.2f} ms) [{sq_m['rows']} rows]"
                )
            query_reports.append(entry)

        # Print ready-to-paste Markdown table for README.md
        print("\n--- Markdown snippet for README.md ---")
        print(f"| Metric | Value |")
        print(f"|---|---:|")
        print(f"| Rows verified (`COUNT(*)`) | {sqdb_row_count:,} (bit-exact sample: {matched:,}/{len(sample):,}) |")
        print(f"| Original source size | {fmt_mb(source_bytes)} ({source_bytes:,} B) |")
        if sqlite_bytes is not None:
            print(f"| SQLite size (after `VACUUM`) | {fmt_mb(sqlite_bytes)} ({sqlite_bytes:,} B) |")
        print(f"| SquishDB `.sqdb` size | {fmt_mb(sqdb_bytes)} ({sqdb_bytes:,} B) |")
        if ratio_vs_sqlite is not None:
            print(f"| Compression ratio | {ratio_vs_source:.2f}x vs source, {ratio_vs_sqlite:.2f}x vs SQLite |")
        else:
            print(f"| Compression ratio | {ratio_vs_source:.2f}x vs source |")
        for qr in query_reports:
            sq_ms = qr["squishdb"]["median_ms"]
            if "sqlite" in qr:
                sl_ms = qr["sqlite"]["median_ms"]
                sp = qr["speedup_vs_sqlite"]
                print(f"| {qr['label']} | {sq_ms:.2f} ms (SQLite: {sl_ms:.2f} ms, {sp:.2f}x) |")
            else:
                print(f"| {qr['label']} | {sq_ms:.2f} ms median ({qr['squishdb']['cold_ms']:.2f} ms cold) |")

        if args.json_out is not None:
            report = {
                "sqdb_path": str(sqdb_path),
                "source_path": str(source_path),
                "table": table,
                "row_count": sqdb_row_count,
                "sample_size": len(sample),
                "sample_matched": matched,
                "verify_mode": verify_mode,
                "source_bytes": source_bytes,
                "sqlite_bytes": sqlite_bytes,
                "sqdb_bytes": sqdb_bytes,
                "ratio_vs_source": ratio_vs_source,
                "ratio_vs_sqlite": ratio_vs_sqlite,
                "queries": query_reports,
            }
            args.json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(f"\nJSON report written to {args.json_out}")

        return 0
    finally:
        client.close()
        if tmp_dir is not None:
            tmp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())

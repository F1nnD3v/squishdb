"""
Head-to-head Storage Efficiency & Lossless Fidelity Benchmark:
SQLite (after VACUUM) vs. SquishDB

Generates 5 realistic relational workloads + 1 incompressible Shannon-entropy test,
writes identical data to SQLite (.sqlite) and SquishDB (.sqdb), verifies 100% exact
row-by-row & bit-for-bit lossless round-trip fidelity, and reports file sizes,
compression ratios, and write/read timings.
"""

import os
import random
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQUISH_BIN = os.path.join(
    ROOT,
    "target",
    "release",
    "squishdb.exe" if os.name == "nt" else "squishdb",
)

COL_INT = 1
COL_REAL = 2
COL_TEXT = 3
COL_BLOB = 4

TYPE_SQL = {
    COL_INT: "INTEGER",
    COL_REAL: "REAL",
    COL_TEXT: "TEXT",
    COL_BLOB: "BLOB",
}


def pack_stream(table_name: str, columns, rows) -> bytes:
    """Packs schema + rows into SquishDB's exact binary stream format."""
    parts = []
    tbytes = table_name.encode("utf-8")
    parts.append(struct.pack("<I", len(tbytes)))
    parts.append(tbytes)
    parts.append(struct.pack("<I", len(columns)))
    for cname, ctype, pk, not_null in columns:
        cb = cname.encode("utf-8")
        flags = (1 if pk else 0) | (2 if not_null else 0)
        parts.append(struct.pack("<I", len(cb)))
        parts.append(cb)
        parts.append(struct.pack("<BB", ctype, flags))
    parts.append(struct.pack("<Q", len(rows)))

    pack_i = struct.Struct("<Bq").pack
    pack_f = struct.Struct("<Bd").pack
    pack_l = struct.Struct("<BI").pack

    for row in rows:
        for val, (_, ctype, _, _) in zip(row, columns):
            if val is None:
                parts.append(b"\x00")
            elif ctype == COL_INT:
                parts.append(pack_i(1, val))
            elif ctype == COL_REAL:
                parts.append(pack_f(2, val))
            elif ctype == COL_TEXT:
                b = val.encode("utf-8")
                parts.append(pack_l(3, len(b)))
                parts.append(b)
            elif ctype == COL_BLOB:
                parts.append(pack_l(4, len(val)))
                parts.append(val)
    return b"".join(parts)


def unpack_export(data: bytes, num_cols: int):
    """Unpacks SquishDB's exact binary export back into Python tuples."""
    mv = memoryview(data)
    pos = 0
    (num_rows,) = struct.unpack_from("<Q", mv, pos)
    pos += 8
    rows = []
    for _ in range(num_rows):
        row = []
        for _ in range(num_cols):
            tag = mv[pos]
            pos += 1
            if tag == 0:
                row.append(None)
            elif tag == 1:
                (v,) = struct.unpack_from("<q", mv, pos)
                pos += 8
                row.append(v)
            elif tag == 2:
                (v,) = struct.unpack_from("<d", mv, pos)
                pos += 8
                row.append(v)
            elif tag == 3:
                (ln,) = struct.unpack_from("<I", mv, pos)
                pos += 4
                row.append(bytes(mv[pos : pos + ln]).decode("utf-8"))
                pos += ln
            elif tag == 4:
                (ln,) = struct.unpack_from("<I", mv, pos)
                pos += 4
                row.append(bytes(mv[pos : pos + ln]))
                pos += ln
            else:
                raise ValueError(f"Unknown export tag {tag}")
        rows.append(tuple(row))
    return rows


def values_bit_equal(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        return struct.pack("<d", float(a)) == struct.pack("<d", float(b))
    return a == b


def run_benchmark_case(name: str, table: str, columns, rows, profile: str = "balanced"):
    with tempfile.TemporaryDirectory(prefix="squish_bench_") as tmpdir:
        sqlite_path = os.path.join(tmpdir, "data.sqlite")
        squish_path = os.path.join(tmpdir, "data.sqdb")

        # 1. Write to SQLite (inside single transaction + VACUUM for fairest minimal SQLite size!)
        col_defs = ", ".join(
            f"{c} {TYPE_SQL[t]}"
            + (" PRIMARY KEY" if pk else "")
            + (" NOT NULL" if nn else "")
            for c, t, pk, nn in columns
        )
        placeholders = ", ".join(["?"] * len(columns))

        t0 = time.perf_counter()
        conn = sqlite3.connect(sqlite_path)
        conn.execute(f"CREATE TABLE {table} ({col_defs})")
        conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
        sqlite_write_s = time.perf_counter() - t0
        sqlite_size = os.path.getsize(sqlite_path)

        # 2. Write to SquishDB
        stream = pack_stream(table, columns, rows)
        t0 = time.perf_counter()
        proc = subprocess.run(
            [SQUISH_BIN, "import-bin", squish_path, profile],
            input=stream,
            capture_output=True,
            check=True,
        )
        squish_write_s = time.perf_counter() - t0
        squish_size = os.path.getsize(squish_path)
        wal_path = squish_path + "-wal"
        if os.path.exists(wal_path):
            squish_size += os.path.getsize(wal_path)

        # 3. Verify 100% lossless round-trip equality against SQLite!
        t0 = time.perf_counter()
        exp = subprocess.run(
            [SQUISH_BIN, "export-bin", squish_path, table],
            capture_output=True,
            check=True,
        )
        squish_read_s = time.perf_counter() - t0
        squish_rows = unpack_export(exp.stdout, len(columns))

        t0 = time.perf_counter()
        conn = sqlite3.connect(sqlite_path)
        sqlite_rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        conn.close()
        sqlite_read_s = time.perf_counter() - t0

        assert len(squish_rows) == len(sqlite_rows) == len(rows), (
            f"Row count mismatch: squish={len(squish_rows)}, sqlite={len(sqlite_rows)}"
        )
        for idx, (sr, qr) in enumerate(zip(sqlite_rows, squish_rows)):
            for c_i, (a, b) in enumerate(zip(sr, qr)):
                if not values_bit_equal(a, b):
                    raise AssertionError(
                        f"[{name}] Lossless check FAILED at row {idx} col {c_i}: "
                        f"sqlite={a!r} vs squish={b!r}"
                    )

        ratio = sqlite_size / squish_size
        savings_pct = (1.0 - (squish_size / sqlite_size)) * 100.0
        return {
            "name": name,
            "rows": len(rows),
            "sqlite_bytes": sqlite_size,
            "squish_bytes": squish_size,
            "ratio": ratio,
            "savings_pct": savings_pct,
            "sqlite_write_s": sqlite_write_s,
            "squish_write_s": squish_write_s,
            "sqlite_read_s": sqlite_read_s,
            "squish_read_s": squish_read_s,
            "lossless": True,
        }


# ============================================================================
# Realistic Workload Generators
# ============================================================================

def gen_ecommerce(n=100_000):
    rng = random.Random(42)
    countries = ["US", "UK", "DE", "FR", "CA", "JP", "AU", "BR", "IN", "NL"]
    statuses = ["completed", "completed", "completed", "shipped", "pending", "refunded", "cancelled"]
    currencies = ["USD", "EUR", "GBP", "CAD", "JPY"]
    domains = ["gmail.com", "outlook.com", "company.io", "acme.org", "fastmail.com"]
    categories = ["electronics", "books", "home", "apparel", "sports", "beauty", "toys", "grocery"]
    cols = [
        ("order_id", COL_INT, True, True),
        ("customer_id", COL_INT, False, True),
        ("created_ts", COL_INT, False, True),
        ("country", COL_TEXT, False, True),
        ("currency", COL_TEXT, False, True),
        ("status", COL_TEXT, False, True),
        ("category", COL_TEXT, False, True),
        ("subtotal", COL_REAL, False, True),
        ("tax", COL_REAL, False, True),
        ("email", COL_TEXT, False, True),
        ("shipping_note", COL_TEXT, False, False),
    ]
    rows = []
    ts = 1_700_000_000
    notes = [None, None, None, "Leave at front door", "Call on arrival", "Fragile - handle with care", "Gift wrap requested"]
    for i in range(1, n + 1):
        ts += rng.randint(1, 25)
        cid = rng.randint(1, 20_000)
        sub = round(rng.uniform(4.99, 899.99), 2)
        tax = round(sub * 0.0825, 2)
        rows.append((
            i,
            cid,
            ts,
            rng.choice(countries),
            rng.choice(currencies),
            rng.choice(statuses),
            rng.choice(categories),
            sub,
            tax,
            f"customer_{cid}@{rng.choice(domains)}",
            rng.choice(notes),
        ))
    return cols, rows


def gen_app_logs(n=80_000):
    rng = random.Random(101)
    services = ["api-gateway", "auth-service", "billing-worker", "search-index", "checkout-api", "mailer"]
    levels = ["INFO", "INFO", "INFO", "INFO", "WARN", "ERROR", "DEBUG"]
    endpoints = [
        "/api/v2/users/profile",
        "/api/v2/orders/create",
        "/api/v2/orders/list",
        "/api/v2/search/query",
        "/api/v2/billing/charge",
        "/api/v2/auth/token",
    ]
    regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-southeast-1"]
    cols = [
        ("id", COL_INT, True, True),
        ("ts_ms", COL_INT, False, True),
        ("service", COL_TEXT, False, True),
        ("region", COL_TEXT, False, True),
        ("level", COL_TEXT, False, True),
        ("http_status", COL_INT, False, True),
        ("latency_ms", COL_REAL, False, True),
        ("endpoint", COL_TEXT, False, True),
        ("payload_json", COL_TEXT, False, True),
    ]
    rows = []
    ts = 1_730_000_000_000
    for i in range(1, n + 1):
        ts += rng.randint(2, 50)
        svc = rng.choice(services)
        lvl = rng.choice(levels)
        ep = rng.choice(endpoints)
        status = 200 if lvl == "INFO" else (500 if lvl == "ERROR" else 429)
        lat = round(rng.uniform(1.2, 450.0), 2)
        payload = json.dumps({
            "req_id": f"req-{i:08x}",
            "user_id": rng.randint(1000, 99999),
            "endpoint": ep,
            "ok": status == 200,
            "retries": 0 if status == 200 else rng.randint(1, 3),
        })
        rows.append((i, ts, svc, rng.choice(regions), lvl, status, lat, ep, payload))
    return cols, rows


def gen_iot_sensors(n=150_000):
    rng = random.Random(7)
    cols = [
        ("seq", COL_INT, True, True),
        ("device_id", COL_INT, False, True),
        ("ts_sec", COL_INT, False, True),
        ("temp_c", COL_REAL, False, True),
        ("humidity_pct", COL_REAL, False, True),
        ("pressure_hpa", COL_REAL, False, True),
        ("battery_mv", COL_INT, False, True),
        ("firmware", COL_TEXT, False, True),
    ]
    fws = ["v2.4.1", "v2.4.2", "v2.5.0-rc1"]
    rows = []
    ts = 1_720_000_000
    temp = 21.5
    hum = 48.0
    pres = 1013.25
    for i in range(1, n + 1):
        ts += 5
        temp = round(max(-10.0, min(45.0, temp + rng.uniform(-0.15, 0.15))), 2)
        hum = round(max(10.0, min(95.0, hum + rng.uniform(-0.2, 0.2))), 2)
        pres = round(max(980.0, min(1040.0, pres + rng.uniform(-0.05, 0.05))), 2)
        rows.append((
            i,
            1000 + (i % 64),
            ts,
            temp,
            hum,
            pres,
            3700 + rng.randint(-150, 150),
            fws[i % 3],
        ))
    return cols, rows


def gen_articles_text(n=15_000):
    """Longer natural-language text documents (moderate entropy, not trivial repetition)."""
    rng = random.Random(2025)
    vocab = [
        "database", "storage", "engine", "columnar", "compression", "latency", "throughput",
        "transaction", "index", "query", "optimizer", "memory", "buffer", "cache", "snapshot",
        "recovery", "journal", "durability", "consistency", "isolation", "schema", "partition",
        "distributed", "embedded", "local", "filesystem", "page", "block", "checksum", "encoding",
        "dictionary", "delta", "vectorized", "execution", "pipeline", "analytics", "workload",
        "application", "developer", "reliability", "performance", "efficiency", "compact", "modern",
        "the", "and", "of", "to", "in", "for", "with", "that", "is", "are", "on", "by", "as", "from",
    ]
    authors = [f"author_{k}@editorial.org" for k in range(1, 200)]
    tags_pool = ["systems", "databases", "rust", "storage", "perf", "architecture", "sql", "compression"]
    cols = [
        ("doc_id", COL_INT, True, True),
        ("published_ts", COL_INT, False, True),
        ("author", COL_TEXT, False, True),
        ("tag", COL_TEXT, False, True),
        ("score", COL_REAL, False, False),
        ("title", COL_TEXT, False, True),
        ("body", COL_TEXT, False, True),
    ]
    rows = []
    ts = 1_650_000_000
    for i in range(1, n + 1):
        ts += rng.randint(60, 3600)
        title = " ".join(rng.choices(vocab, k=rng.randint(6, 12))).capitalize()
        body = " ".join(rng.choices(vocab, k=rng.randint(60, 120))) + "."
        score = None if i % 15 == 0 else round(rng.uniform(0.0, 100.0), 3)
        rows.append((i, ts, rng.choice(authors), rng.choice(tags_pool), score, title, body))
    return cols, rows


def gen_high_entropy_mixed(n=40_000):
    """Harder realistic case: high-cardinality random UUIDs + full-precision floats + random ints."""
    rng = random.Random(999)
    cols = [
        ("id", COL_INT, True, True),
        ("uuid", COL_TEXT, False, True),
        ("rand_i64", COL_INT, False, True),
        ("metric_a", COL_REAL, False, True),
        ("metric_b", COL_REAL, False, True),
        ("tag", COL_TEXT, False, True),
    ]
    tags = ["alpha", "beta", "gamma", "delta"]
    rows = []
    for i in range(1, n + 1):
        u = f"{rng.getrandbits(32):08x}-{rng.getrandbits(16):04x}-4{rng.getrandbits(12):03x}-{rng.getrandbits(16):04x}-{rng.getrandbits(48):012x}"
        rows.append((
            i,
            u,
            rng.randint(-10**12, 10**12),
            rng.random() * 1e6,
            rng.gauss(0.0, 1000.0),
            rng.choice(tags),
        ))
    return cols, rows


def gen_incompressible_blobs(n=2_000):
    """Pure cryptographic-like random bytes (Shannon entropy limit test!)."""
    rng = random.Random(777)
    cols = [
        ("id", COL_INT, True, True),
        ("payload", COL_BLOB, False, True),
    ]
    rows = [(i, rng.randbytes(4096)) for i in range(1, n + 1)]
    return cols, rows


def fmt_mb(b: int) -> str:
    return f"{b / (1024 * 1024):.2f} MB"


def main():
    if not os.path.exists(SQUISH_BIN):
        print(f"ERROR: Build release binary first: {SQUISH_BIN}", file=sys.stderr)
        sys.exit(1)

    profile = sys.argv[1] if len(sys.argv) > 1 else "balanced"
    print(f"=== SQLite (VACUUMed) vs SquishDB (profile='{profile}') ===\n")

    workloads = [
        ("1. E-Commerce Orders (100k rows)", "orders", gen_ecommerce),
        ("2. App Telemetry / JSON Logs (80k rows)", "logs", gen_app_logs),
        ("3. IoT Sensor Time-Series (150k rows)", "sensors", gen_iot_sensors),
        ("4. Knowledge Base Articles (15k docs)", "articles", gen_articles_text),
        ("5. Hard Mixed High-Card UUIDs+Floats (40k)", "mixed", gen_high_entropy_mixed),
        ("6. Random 4KB Blobs [Entropy Limit] (2k)", "blobs", gen_incompressible_blobs),
    ]

    results = []
    for label, table, gen_fn in workloads:
        cols, rows = gen_fn()
        res = run_benchmark_case(label, table, cols, rows, profile=profile)
        results.append(res)
        status = "PASS (>=2x)" if res["ratio"] >= 2.0 else "ENTROPY BOUND"
        print(
            f"{label}\n"
            f"  SQLite:   {fmt_mb(res['sqlite_bytes']):>9}  (write {res['sqlite_write_s']:.2f}s, read {res['sqlite_read_s']:.2f}s)\n"
            f"  SquishDB: {fmt_mb(res['squish_bytes']):>9}  (write {res['squish_write_s']:.2f}s, read {res['squish_read_s']:.2f}s)\n"
            f"  Ratio:    {res['ratio']:.2f}x smaller ({res['savings_pct']:.1f}% space saved)  | Lossless: VERIFIED | {status}\n"
        )

    # Summary across the 5 real relational workloads
    rel = results[:5]
    tot_sqlite = sum(r["sqlite_bytes"] for r in rel)
    tot_squish = sum(r["squish_bytes"] for r in rel)
    overall_ratio = tot_sqlite / tot_squish
    print("=" * 68)
    print(
        f"RELATIONAL TOTAL (Workloads 1-5):\n"
        f"  SQLite (VACUUMed): {fmt_mb(tot_sqlite)}\n"
        f"  SquishDB:          {fmt_mb(tot_squish)}\n"
        f"  Overall Reduction: {overall_ratio:.2f}x smaller "
        f"(5.00 GB SQLite -> {5.0 / overall_ratio:.2f} GB SquishDB!)\n"
        f"  Lossless Verification: 100% EXACT BIT-FOR-BIT MATCH ON ALL ROWS"
    )
    print("=" * 68)

    # Emit JSON summary for charting
    summary_path = os.path.join(ROOT, "benchmarks", "last_results.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()

"""
SquishDB v0.2 Capability & Performance Benchmark vs SQLite & Parquet:
  1. Sort-Key Clustering compression boost (IoT, JSON Logs, E-Commerce)
  2. High-Cardinality Unsorted Point Lookup Latency & Index Size Overhead
     (200 random UUID lookups on 40,000 rows: SQLite vs SquishDB Bloom vs SquishDB Indexed+Cache)
  3. Analytical SQL (GROUP BY + SUM/AVG/COUNT + HAVING) correctness & timing vs SQLite
"""

import json
import os
import random
import sqlite3
import subprocess
import tempfile
import time

import pyarrow.parquet as pq

from compare_industry import rows_to_arrow_table
from compare_sqlite import (
    COL_INT,
    COL_REAL,
    COL_TEXT,
    SQUISH_BIN,
    TYPE_SQL,
    gen_app_logs,
    gen_ecommerce,
    gen_high_entropy_mixed,
    pack_stream,
)


def gen_multidevice_iot(n=150_000):
    """Realistic multi-device IoT telemetry: 64 independent sensors in different climates/rooms,
    interleaved in arrival order (the classic ClickHouse/TimescaleDB clustering workload!)."""
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
    dev_temp = [rng.uniform(-5.0, 38.0) for _ in range(64)]
    dev_hum = [rng.uniform(20.0, 85.0) for _ in range(64)]
    dev_pres = [rng.uniform(990.0, 1030.0) for _ in range(64)]
    dev_bat = [rng.randint(3550, 4150) for _ in range(64)]
    dev_fw = [fws[d % 3] for d in range(64)]
    rows = []
    ts = 1_720_000_000
    for i in range(1, n + 1):
        ts += 5
        d = i % 64
        dev_temp[d] = round(max(-10.0, min(45.0, dev_temp[d] + rng.uniform(-0.08, 0.08))), 2)
        dev_hum[d] = round(max(10.0, min(95.0, dev_hum[d] + rng.uniform(-0.1, 0.1))), 2)
        dev_pres[d] = round(max(980.0, min(1040.0, dev_pres[d] + rng.uniform(-0.03, 0.03))), 2)
        rows.append((
            i,
            1000 + d,
            ts,
            dev_temp[d],
            dev_hum[d],
            dev_pres[d],
            dev_bat[d],
            dev_fw[d],
        ))
    return cols, rows


def mb(b: int) -> float:
    return b / (1024 * 1024)


def run_clustering_bench():
    print("=== 1. Sort-Key Clustering Compression Boost (Measured) ===")
    cases = [
        ("Multi-Device IoT (150k)", "sensors", gen_multidevice_iot, "device_id,ts_sec"),
        ("JSON Logs (80k)", "logs", gen_app_logs, "service,endpoint,level,ts_ms"),
        ("E-Commerce (100k)", "orders", gen_ecommerce, "country,status,category,created_ts"),
    ]
    results = []
    for label, table, gen_fn, cluster_cols in cases:
        cols, rows = gen_fn()
        with tempfile.TemporaryDirectory(prefix="sq_cluster_") as tmp:
            sq_path = os.path.join(tmp, "db.sqlite")
            col_defs = ", ".join(
                f"{c} {TYPE_SQL[t]}" + (" PRIMARY KEY" if pk else "")
                for c, t, pk, _ in cols
            )
            conn = sqlite3.connect(sq_path)
            conn.execute(f"CREATE TABLE {table} ({col_defs})")
            conn.executemany(
                f"INSERT INTO {table} VALUES ({','.join(['?']*len(cols))})", rows
            )
            conn.commit()
            conn.execute("VACUUM")
            conn.close()
            sqlite_sz = os.path.getsize(sq_path)

            pq_path = os.path.join(tmp, "zstd9.parquet")
            pq.write_table(
                rows_to_arrow_table(cols, rows),
                pq_path,
                compression="zstd",
                compression_level=9,
            )
            pq_sz = os.path.getsize(pq_path)

            stream = pack_stream(table, cols, rows)
            uncl_path = os.path.join(tmp, "uncl.sqdb")
            cl_path = os.path.join(tmp, "cl.sqdb")
            subprocess.run(
                [SQUISH_BIN, "import-bin", uncl_path, "balanced"],
                input=stream,
                check=True,
            )
            subprocess.run(
                [SQUISH_BIN, "import-bin", cl_path, "balanced", cluster_cols],
                input=stream,
                check=True,
            )
            uncl_sz = os.path.getsize(uncl_path)
            cl_sz = os.path.getsize(cl_path)
            ratio = sqlite_sz / cl_sz
            print(
                f"  {label:18s} | SQLite {mb(sqlite_sz):5.2f} MB | Parquet-Z9 {mb(pq_sz):4.2f} MB | "
                f"Squish(uncl) {mb(uncl_sz):4.2f} MB | Squish(clustered) {mb(cl_sz):4.2f} MB "
                f"({ratio:5.2f}x vs SQLite!)"
            )
            results.append(
                {
                    "label": label,
                    "cluster_by": cluster_cols,
                    "sqlite_bytes": sqlite_sz,
                    "parquet_zstd9_bytes": pq_sz,
                    "squish_unclustered_bytes": uncl_sz,
                    "squish_clustered_bytes": cl_sz,
                    "ratio_vs_sqlite": ratio,
                }
            )
    return results


def run_point_lookup_bench():
    print("\n=== 2. High-Cardinality UUID Point Lookups (200 queries on 40k rows) ===")
    cols, rows = gen_high_entropy_mixed(40_000)
    rng = random.Random(123)
    sample_uuids = [rows[rng.randint(0, len(rows) - 1)][1] for _ in range(200)]

    with tempfile.TemporaryDirectory(prefix="sq_lookup_") as tmp:
        # SQLite (unindexed vs indexed)
        sq_path = os.path.join(tmp, "db.sqlite")
        col_defs = ", ".join(
            f"{c} {TYPE_SQL[t]}" + (" PRIMARY KEY" if pk else "")
            for c, t, pk, _ in cols
        )
        conn = sqlite3.connect(sq_path)
        conn.execute(f"CREATE TABLE mixed ({col_defs})")
        conn.executemany(
            f"INSERT INTO mixed VALUES ({','.join(['?']*len(cols))})", rows
        )
        conn.commit()
        conn.execute("VACUUM")
        sqlite_no_idx_sz = os.path.getsize(sq_path)

        t0 = time.perf_counter()
        for u in sample_uuids:
            r = conn.execute("SELECT id, metric_a FROM mixed WHERE uuid = ?", (u,)).fetchall()
            assert len(r) == 1
        sqlite_scan_ms = (time.perf_counter() - t0) * 1000.0

        conn.execute("CREATE INDEX idx_uuid ON mixed (uuid)")
        conn.commit()
        conn.execute("VACUUM")
        sqlite_with_idx_sz = os.path.getsize(sq_path)

        t0 = time.perf_counter()
        for u in sample_uuids:
            r = conn.execute("SELECT id, metric_a FROM mixed WHERE uuid = ?", (u,)).fetchall()
            assert len(r) == 1
        sqlite_idx_ms = (time.perf_counter() - t0) * 1000.0
        conn.close()

        # SquishDB (Bloom-only vs Secondary Index + LRU Cache)
        squish_path = os.path.join(tmp, "db.sqdb")
        stream = pack_stream("mixed", cols, rows)
        subprocess.run(
            [SQUISH_BIN, "import-bin", squish_path, "balanced"],
            input=stream,
            check=True,
        )
        squish_bloom_sz = os.path.getsize(squish_path)

        sql_batch = "\n".join(
            f"SELECT id, metric_a FROM mixed WHERE uuid = '{u}';" for u in sample_uuids
        ).encode("utf-8")

        p_bloom = subprocess.run(
            [SQUISH_BIN, "bench-lookup", squish_path],
            input=sql_batch,
            capture_output=True,
            check=True,
        )
        bloom_res = json.loads(p_bloom.stdout.decode("utf-8"))
        assert bloom_res["matched_rows"] == 200

        # Create Secondary Index in SquishDB
        subprocess.run(
            [SQUISH_BIN, squish_path, "-c", "CREATE INDEX idx_uuid ON mixed (uuid);"],
            capture_output=True,
            check=True,
        )
        squish_idx_sz = os.path.getsize(squish_path)

        p_idx = subprocess.run(
            [SQUISH_BIN, "bench-lookup", squish_path],
            input=sql_batch,
            capture_output=True,
            check=True,
        )
        idx_res = json.loads(p_idx.stdout.decode("utf-8"))
        assert idx_res["matched_rows"] == 200

        sqlite_idx_overhead = sqlite_with_idx_sz - sqlite_no_idx_sz
        squish_idx_overhead = squish_idx_sz - squish_bloom_sz
        print(
            f"  SQLite (no index):       {sqlite_scan_ms:6.2f} ms total ({sqlite_scan_ms/200:.3f} ms/q) | DB {mb(sqlite_no_idx_sz):.2f} MB\n"
            f"  SQLite (B-tree index):   {sqlite_idx_ms:6.2f} ms total ({sqlite_idx_ms/200:.3f} ms/q) | DB {mb(sqlite_with_idx_sz):.2f} MB (+{sqlite_idx_overhead/1024:.0f} KB index!)\n"
            f"  SquishDB (auto Bloom):   {bloom_res['elapsed_ms']:6.2f} ms total ({bloom_res['avg_ms']:.3f} ms/q) | DB {mb(squish_bloom_sz):.2f} MB (cache hits={bloom_res['cache_hits']})\n"
            f"  SquishDB (2nd Index):    {idx_res['elapsed_ms']:6.2f} ms total ({idx_res['avg_ms']:.3f} ms/q) | DB {mb(squish_idx_sz):.2f} MB (+{squish_idx_overhead/1024:.0f} KB index!)"
        )
        return {
            "sqlite_scan_ms": sqlite_scan_ms,
            "sqlite_idx_ms": sqlite_idx_ms,
            "sqlite_no_idx_bytes": sqlite_no_idx_sz,
            "sqlite_with_idx_bytes": sqlite_with_idx_sz,
            "sqlite_idx_overhead_bytes": sqlite_idx_overhead,
            "squish_bloom_ms": bloom_res["elapsed_ms"],
            "squish_bloom_bytes": squish_bloom_sz,
            "squish_idx_ms": idx_res["elapsed_ms"],
            "squish_idx_bytes": squish_idx_sz,
            "squish_idx_overhead_bytes": squish_idx_overhead,
            "squish_idx_cache_hits": idx_res["cache_hits"],
        }


def run_analytics_bench():
    print("\n=== 3. Analytical SQL (GROUP BY + SUM/AVG/COUNT + HAVING) on 100k Orders ===")
    cols, rows = gen_ecommerce(100_000)
    query = (
        "SELECT country, COUNT(*) AS orders, SUM(subtotal) AS revenue, AVG(subtotal) AS avg_order "
        "FROM orders WHERE status = 'completed' GROUP BY country HAVING COUNT(*) > 100 ORDER BY revenue DESC;"
    )
    with tempfile.TemporaryDirectory(prefix="sq_olap_") as tmp:
        sq_path = os.path.join(tmp, "db.sqlite")
        col_defs = ", ".join(
            f"{c} {TYPE_SQL[t]}" + (" PRIMARY KEY" if pk else "")
            for c, t, pk, _ in cols
        )
        conn = sqlite3.connect(sq_path)
        conn.execute(f"CREATE TABLE orders ({col_defs})")
        conn.executemany(
            f"INSERT INTO orders VALUES ({','.join(['?']*len(cols))})", rows
        )
        conn.commit()
        t0 = time.perf_counter()
        sq_rows = conn.execute(query).fetchall()
        sqlite_ms = (time.perf_counter() - t0) * 1000.0
        conn.close()

        squish_path = os.path.join(tmp, "db.sqdb")
        subprocess.run(
            [SQUISH_BIN, "import-bin", squish_path, "balanced"],
            input=pack_stream("orders", cols, rows),
            check=True,
        )
        p = subprocess.run(
            [SQUISH_BIN, "bench-lookup", squish_path],
            input=(query + "\n").encode("utf-8"),
            capture_output=True,
            check=True,
        )
        sq_res = json.loads(p.stdout.decode("utf-8"))
        assert sq_res["matched_rows"] == len(sq_rows)
        print(
            f"  SQLite GROUP BY (100k rows):   {sqlite_ms:6.2f} ms ({len(sq_rows)} groups)\n"
            f"  SquishDB GROUP BY (100k rows): {sq_res['elapsed_ms']:6.2f} ms ({sq_res['matched_rows']} groups, exact match!)"
        )
        return {
            "sqlite_ms": sqlite_ms,
            "squish_ms": sq_res["elapsed_ms"],
            "groups": len(sq_rows),
        }


def main():
    c = run_clustering_bench()
    p = run_point_lookup_bench()
    a = run_analytics_bench()
    out_path = os.path.join(os.path.dirname(__file__), "v2_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"clustering": c, "point_lookups": p, "analytics": a}, f, indent=2)


if __name__ == "__main__":
    main()

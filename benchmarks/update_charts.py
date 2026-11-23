"""
Regenerates docs/charts/storage-comparison.svg and docs/charts/speed-comparison.svg
directly from measured benchmark JSON files:
  - benchmarks/v2_results.json
  - benchmarks/industry_results.json
  - benchmarks/throughput_results.json
"""

import json
import math
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH_DIR = os.path.join(ROOT, "benchmarks")
CHARTS_DIR = os.path.join(ROOT, "docs", "charts")


def mb(b: int) -> float:
    return b / (1024.0 * 1024.0)


def measure_throughput():
    """Runs release throughput benchmark test and parses measured rows/sec."""
    cmd = [
        "cargo",
        "test",
        "--release",
        "--test",
        "adversarial_bugs",
        "bug_hunt_insert_rate_throughput_limits",
        "--",
        "--nocapture",
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
    out = proc.stdout + "\n" + proc.stderr

    def grab(pattern: str) -> int:
        m = re.search(pattern, out)
        if not m:
            raise RuntimeError(f"Could not find '{pattern}' in cargo test output:\n{out}")
        return int(m.group(1))

    data = {
        "single_row_fsync_rps": grab(r"sync_wal=true, fsync every row\): .* = (\d+) rows/sec"),
        "single_row_buffered_rps": grab(r"sync_wal=false\): .* = (\d+) rows/sec"),
        "batched_1k_fsync_rps": grab(r"Batched WAL tx .* = (\d+) rows/sec"),
        "bulk_pax_checkpoint_rps": grab(r"Bulk PAX-v4 insert .* = (\d+) rows/sec"),
    }
    with open(os.path.join(BENCH_DIR, "throughput_results.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return data


def generate_storage_svg(v2, industry):
    iot = next(x for x in v2["clustering"] if "IoT" in x["label"])
    logs = next(x for x in industry["workloads"] if "JSON Logs" in x["label"])
    ecom = next(x for x in industry["workloads"] if "E-Commerce" in x["label"])
    blobs = next(x for x in industry["workloads"] if "Random Blobs" in x["label"])
    pt = v2["point_lookups"]

    iot_sq, iot_pq, iot_sh = mb(iot["sqlite_bytes"]), mb(iot["parquet_zstd9_bytes"]), mb(iot["squish_clustered_bytes"])
    log_sq, log_pq, log_sh = mb(logs["sqlite"]), mb(logs["parquet_zstd9"]), mb(logs["squish_balanced"])
    eco_sq, eco_sh = mb(ecom["sqlite"]), mb(ecom["squish_balanced"])
    uid_sq, uid_sh = mb(pt["sqlite_with_idx_bytes"]), mb(pt["squish_idx_bytes"])
    blb_sq, blb_pq, blb_sh = mb(blobs["sqlite"]), mb(blobs["parquet_zstd9"]), mb(blobs["squish_balanced"])

    blob_overhead_per_row = (blobs["squish_balanced"] - 2000 * 4096) / 2000.0

    # Scale: x=195 (0 MB) to x=595 (15 MB) -> 400 / 15 = 26.6667 px/MB
    px_per_mb = 400.0 / 15.0

    def w(val_mb: float) -> float:
        return round(val_mb * px_per_mb, 1)

    def x_lbl(val_mb: float) -> float:
        return round(195.0 + val_mb * px_per_mb + 6.5, 1)

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 760 400" width="760" height="400" role="img" aria-label="Disk size comparison between SQLite, Parquet, and SquishDB">
  <defs>
    <style>
      .bg {{ fill: #0d1117; stroke: #30363d; stroke-width: 1; }}
      .title {{ font: 600 15px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #e6edf3; }}
      .legend {{ font: 500 12px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #9198a1; }}
      .label {{ font: 500 12px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #c9d1d9; }}
      .sub {{ font: 400 11px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #8b949e; }}
      .val {{ font: 500 11px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #c9d1d9; }}
      .val-sq {{ font: 600 11px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #3fb950; }}
      .axis {{ font: 400 10px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #6e7681; }}
      .grid {{ stroke: #21262d; stroke-width: 1; }}
      .bar-sqlite {{ fill: #6e7681; }}
      .bar-parquet {{ fill: #d29922; }}
      .bar-squish {{ fill: #2ea043; }}
    </style>
  </defs>

  <rect class="bg" x="0.5" y="0.5" width="759" height="399" rx="10" />

  <text class="title" x="24" y="34">Disk size on identical rows (MB, lower is better)</text>

  <!-- Legend -->
  <rect class="bar-sqlite" x="24" y="48" width="10" height="10" rx="2" />
  <text class="legend" x="40" y="57">SQLite C (vacuumed)</text>
  <rect class="bar-parquet" x="184" y="48" width="10" height="10" rx="2" />
  <text class="legend" x="200" y="57">Parquet (Zstd L9)</text>
  <rect class="bar-squish" x="328" y="48" width="10" height="10" rx="2" />
  <text class="legend" x="344" y="57">SquishDB v0.5</text>

  <!-- Gridlines: x=195 (0 MB) to x=595 (15 MB), 26.67 px/MB -->
  <line class="grid" x1="195" y1="74" x2="195" y2="364" />
  <line class="grid" x1="328.3" y1="74" x2="328.3" y2="364" />
  <line class="grid" x1="461.7" y1="74" x2="461.7" y2="364" />
  <line class="grid" x1="595" y1="74" x2="595" y2="364" />

  <text class="axis" x="195" y="380" text-anchor="middle">0 MB</text>
  <text class="axis" x="328.3" y="380" text-anchor="middle">5 MB</text>
  <text class="axis" x="461.7" y="380" text-anchor="middle">10 MB</text>
  <text class="axis" x="595" y="380" text-anchor="middle">15 MB</text>

  <!-- Row 1: IoT clustered (150k rows) -->
  <text class="label" x="24" y="97">IoT clustered</text>
  <text class="sub" x="24" y="112">150k rows</text>
  <rect class="bar-sqlite" x="195" y="82" width="{w(iot_sq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(iot_sq)}" y="91">{iot_sq:.2f} MB</text>
  <rect class="bar-parquet" x="195" y="96" width="{w(iot_pq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(iot_pq)}" y="105">{iot_pq:.2f} MB</text>
  <rect class="bar-squish" x="195" y="110" width="{w(iot_sh)}" height="11" rx="2" />
  <text class="val-sq" x="{x_lbl(iot_sh)}" y="119">{iot_sh:.2f} MB ({iot_sq/iot_sh:.1f}x smaller)</text>

  <!-- Row 2: JSON telemetry logs (80k rows) -->
  <text class="label" x="24" y="157">JSON telemetry logs</text>
  <text class="sub" x="24" y="172">80k rows</text>
  <rect class="bar-sqlite" x="195" y="142" width="{w(log_sq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(log_sq)}" y="151">{log_sq:.2f} MB</text>
  <rect class="bar-parquet" x="195" y="156" width="{w(log_pq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(log_pq)}" y="165">{log_pq:.2f} MB</text>
  <rect class="bar-squish" x="195" y="170" width="{w(log_sh)}" height="11" rx="2" />
  <text class="val-sq" x="{x_lbl(log_sh)}" y="179">{log_sh:.2f} MB ({log_sq/log_sh:.1f}x smaller)</text>

  <!-- Row 3: Analytical dataset (100k rows) -->
  <text class="label" x="24" y="216">Analytical table</text>
  <text class="sub" x="24" y="231">100k rows</text>
  <rect class="bar-sqlite" x="195" y="204" width="{w(eco_sq)}" height="12" rx="2" />
  <text class="val" x="{x_lbl(eco_sq)}" y="214">{eco_sq:.2f} MB</text>
  <rect class="bar-squish" x="195" y="220" width="{w(eco_sh)}" height="12" rx="2" />
  <text class="val-sq" x="{x_lbl(eco_sh)}" y="230">{eco_sh:.2f} MB ({eco_sq/eco_sh:.1f}x smaller)</text>

  <!-- Row 4: UUID B-tree indexed table (40k rows) -->
  <text class="label" x="24" y="268">UUID indexed table</text>
  <text class="sub" x="24" y="283">40k rows + B-tree</text>
  <rect class="bar-sqlite" x="195" y="256" width="{w(uid_sq)}" height="12" rx="2" />
  <text class="val" x="{x_lbl(uid_sq)}" y="266">{uid_sq:.2f} MB</text>
  <rect class="bar-squish" x="195" y="272" width="{w(uid_sh)}" height="12" rx="2" />
  <text class="val-sq" x="{x_lbl(uid_sh)}" y="282">{uid_sh:.2f} MB ({uid_sq/uid_sh:.1f}x smaller)</text>

  <!-- Row 5: Random 4 KB blobs (entropy limit) -->
  <text class="label" x="24" y="325">Random 4 KB blobs</text>
  <text class="sub" x="24" y="340">Incompressible</text>
  <rect class="bar-sqlite" x="195" y="310" width="{w(blb_sq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(blb_sq)}" y="319">{blb_sq:.2f} MB</text>
  <rect class="bar-parquet" x="195" y="324" width="{w(blb_pq)}" height="11" rx="2" />
  <text class="val" x="{x_lbl(blb_pq)}" y="333">{blb_pq:.2f} MB</text>
  <rect class="bar-squish" x="195" y="338" width="{w(blb_sh)}" height="11" rx="2" />
  <text class="val-sq" x="{x_lbl(blb_sh)}" y="347">{blb_sh:.2f} MB (+{blob_overhead_per_row:.2f} B/row)</text>
</svg>
"""
    os.makedirs(CHARTS_DIR, exist_ok=True)
    with open(os.path.join(CHARTS_DIR, "storage-comparison.svg"), "w", encoding="utf-8", newline="\n") as f:
        f.write(svg)


def generate_speed_svg(v2, tp):
    gb_sq = v2["analytics"]["sqlite_ms"]
    gb_sh = v2["analytics"]["squish_ms"]
    pt_sq = v2["point_lookups"]["sqlite_idx_ms"]
    pt_sh = v2["point_lookups"]["squish_idx_ms"]

    max_ms = max(gb_sq, gb_sh, pt_sq, pt_sh, 25.0)
    axis_max = math.ceil(max_ms / 5.0) * 5.0
    px_per_ms = 420.0 / axis_max

    def wm(ms: float) -> float:
        return round(ms * px_per_ms, 1)

    def xm(ms: float) -> float:
        return round(205.0 + ms * px_per_ms + 6.5, 1)

    # Log10 scale for throughput: 10^2 to 10^7 across 420 px -> 84 px/decade
    def wt(rps: int) -> float:
        decades = max(0.05, math.log10(max(rps, 100)) - 2.0)
        return round(min(430.0, decades * 84.0), 1)

    def xt(rps: int) -> float:
        return round(205.0 + wt(rps) + 6.5, 1)

    r1 = tp["single_row_fsync_rps"]
    r2 = tp["single_row_buffered_rps"]
    r3 = tp["batched_1k_fsync_rps"]
    r4 = tp["bulk_pax_checkpoint_rps"]

    t1 = round(axis_max * 0.4)
    t2 = round(axis_max * 0.8)
    t3 = round(axis_max)
    x_t1 = round(205.0 + t1 * px_per_ms, 1)
    x_t2 = round(205.0 + t2 * px_per_ms, 1)
    x_t3 = round(205.0 + t3 * px_per_ms, 1)

    gb_ratio = f"{gb_sq/gb_sh:.2f}x faster" if gb_sh > 0 else "n/a"
    pt_ratio = f"{pt_sq/pt_sh:.2f}x faster" if pt_sh > 0 else "n/a"

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 760 355" width="760" height="355" role="img" aria-label="Query latency and insert throughput comparison for SquishDB and SQLite">
  <defs>
    <style>
      .bg {{ fill: #0d1117; stroke: #30363d; stroke-width: 1; }}
      .title {{ font: 600 15px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #e6edf3; }}
      .legend {{ font: 500 12px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #9198a1; }}
      .label {{ font: 500 12px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #c9d1d9; }}
      .sub {{ font: 400 11px system-ui, -apple-system, 'Segoe UI', sans-serif; fill: #8b949e; }}
      .val {{ font: 500 11px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #c9d1d9; }}
      .val-sq {{ font: 600 11px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #3fb950; }}
      .val-blue {{ font: 600 11px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #58a6ff; }}
      .axis {{ font: 400 10px ui-monospace, SFMono-Regular, Menlo, monospace; fill: #6e7681; }}
      .grid {{ stroke: #21262d; stroke-width: 1; }}
      .sep {{ stroke: #30363d; stroke-width: 1; }}
      .bar-sqlite {{ fill: #6e7681; }}
      .bar-squish {{ fill: #2ea043; }}
      .bar-rate {{ fill: #1f6feb; }}
    </style>
  </defs>

  <rect class="bg" x="0.5" y="0.5" width="759" height="354" rx="10" />

  <!-- Top section: Query latency -->
  <text class="title" x="24" y="34">Query latency (ms, lower is better)</text>
  <rect class="bar-sqlite" x="310" y="24" width="10" height="10" rx="2" />
  <text class="legend" x="326" y="33">SQLite C</text>
  <rect class="bar-squish" x="402" y="24" width="10" height="10" rx="2" />
  <text class="legend" x="418" y="33">SquishDB v0.5</text>

  <line class="grid" x1="205" y1="50" x2="205" y2="148" />
  <line class="grid" x1="{x_t1}" y1="50" x2="{x_t1}" y2="148" />
  <line class="grid" x1="{x_t2}" y1="50" x2="{x_t2}" y2="148" />
  <line class="grid" x1="{x_t3}" y1="50" x2="{x_t3}" y2="148" />

  <text class="axis" x="205" y="162" text-anchor="middle">0 ms</text>
  <text class="axis" x="{x_t1}" y="162" text-anchor="middle">{t1} ms</text>
  <text class="axis" x="{x_t2}" y="162" text-anchor="middle">{t2} ms</text>
  <text class="axis" x="{x_t3}" y="162" text-anchor="middle">{t3} ms</text>

  <!-- GROUP BY -->
  <text class="label" x="24" y="70">GROUP BY + aggregates</text>
  <text class="sub" x="24" y="85">100k rows (parallel)</text>
  <rect class="bar-sqlite" x="205" y="57" width="{wm(gb_sq)}" height="13" rx="2" />
  <text class="val" x="{xm(gb_sq)}" y="68">{gb_sq:.2f} ms</text>
  <rect class="bar-squish" x="205" y="74" width="{wm(gb_sh)}" height="13" rx="2" />
  <text class="val-sq" x="{xm(gb_sh)}" y="85">{gb_sh:.2f} ms ({gb_ratio})</text>

  <!-- UUID lookups -->
  <text class="label" x="24" y="121">200 random UUID lookups</text>
  <text class="sub" x="24" y="136">40k rows, B-tree indexed</text>
  <rect class="bar-sqlite" x="205" y="108" width="{wm(pt_sq)}" height="13" rx="2" />
  <text class="val" x="{xm(pt_sq)}" y="119">{pt_sq:.2f} ms</text>
  <rect class="bar-squish" x="205" y="125" width="{wm(pt_sh)}" height="13" rx="2" />
  <text class="val-sq" x="{xm(pt_sh)}" y="136">{pt_sh:.2f} ms ({pt_ratio})</text>

  <line class="sep" x1="24" y1="178" x2="736" y2="178" />

  <!-- Bottom section: Insert throughput (log10 scale: 10^2 to 10^7 across 420 px -> 84 px/decade) -->
  <text class="title" x="24" y="206">SquishDB insert throughput (rows/sec, log scale, higher is better)</text>

  <line class="grid" x1="205" y1="220" x2="205" y2="326" />
  <line class="grid" x1="289" y1="220" x2="289" y2="326" />
  <line class="grid" x1="373" y1="220" x2="373" y2="326" />
  <line class="grid" x1="457" y1="220" x2="457" y2="326" />
  <line class="grid" x1="541" y1="220" x2="541" y2="326" />
  <line class="grid" x1="625" y1="220" x2="625" y2="326" />

  <text class="axis" x="205" y="342" text-anchor="middle">100</text>
  <text class="axis" x="289" y="342" text-anchor="middle">1k</text>
  <text class="axis" x="373" y="342" text-anchor="middle">10k</text>
  <text class="axis" x="457" y="342" text-anchor="middle">100k</text>
  <text class="axis" x="541" y="342" text-anchor="middle">1M</text>
  <text class="axis" x="625" y="342" text-anchor="middle">10M</text>

  <text class="label" x="24" y="238">Single-row fsync</text>
  <rect class="bar-rate" x="205" y="226" width="{wt(r1)}" height="14" rx="2" />
  <text class="val-blue" x="{xt(r1)}" y="237">{r1:,} rows/s</text>

  <text class="label" x="24" y="264">Single-row buffered</text>
  <rect class="bar-rate" x="205" y="252" width="{wt(r2)}" height="14" rx="2" />
  <text class="val-blue" x="{xt(r2)}" y="263">{r2:,} rows/s</text>

  <text class="label" x="24" y="290">Batched 1k/tx (fsync)</text>
  <rect class="bar-rate" x="205" y="278" width="{wt(r3)}" height="14" rx="2" />
  <text class="val-blue" x="{xt(r3)}" y="289">{r3:,} rows/s</text>

  <text class="label" x="24" y="316">Bulk PAX-v4 + parallel cp</text>
  <rect class="bar-squish" x="205" y="304" width="{wt(r4)}" height="14" rx="2" />
  <text class="val-sq" x="{xt(r4)}" y="315">{r4:,} rows/s</text>
</svg>
"""
    os.makedirs(CHARTS_DIR, exist_ok=True)
    with open(os.path.join(CHARTS_DIR, "speed-comparison.svg"), "w", encoding="utf-8", newline="\n") as f:
        f.write(svg)


def main():
    v2_path = os.path.join(BENCH_DIR, "v2_results.json")
    ind_path = os.path.join(BENCH_DIR, "industry_results.json")
    tp_path = os.path.join(BENCH_DIR, "throughput_results.json")

    with open(v2_path, "r", encoding="utf-8") as f:
        v2 = json.load(f)
    with open(ind_path, "r", encoding="utf-8") as f:
        industry = json.load(f)

    if "--measure-throughput" in sys.argv or not os.path.exists(tp_path):
        tp = measure_throughput()
    else:
        with open(tp_path, "r", encoding="utf-8") as f:
            tp = json.load(f)

    generate_storage_svg(v2, industry)
    generate_speed_svg(v2, tp)
    print("Updated docs/charts/storage-comparison.svg and docs/charts/speed-comparison.svg")


if __name__ == "__main__":
    main()

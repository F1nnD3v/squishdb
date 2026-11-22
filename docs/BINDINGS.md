# Using SquishDB from Other Languages

SquishDB builds both an in-process **C ABI shared/static library** (`squishdb.dll`, `libsquishdb.so`, `libsquishdb.dylib`, `squishdb.lib`, `libsquishdb.a`) and a persistent **JSON-RPC stdio** mode (`squishdb rpc <file.sqdb>`).

All bindings support:
- Parameterized SQL (`?` positional and `$1`, `$2`, ... numbered placeholders)
- Full lossless types (`NULL`, 64-bit `INTEGER`, IEEE-754 `REAL`, UTF-8 `TEXT`, raw `BLOB`)
- Transactions (`begin` / `commit` / `rollback`)
- High-throughput batch ingestion (`insert_batch` and direct PAX-v4 `bulk_insert`)
- Shared read-only concurrent connections (`open_readonly`)
- `checkpoint`, `vacuum`, `backup_to`, and structured JSON `stats`

## Build the native library once

```bash
cargo build --release
```

This writes the artifacts to `target/release/`:
- Windows: `squishdb.dll`, `squishdb.dll.lib`, `squishdb.lib`, `squishdb.exe`
- Linux: `libsquishdb.so`, `libsquishdb.a`, `squishdb`
- macOS: `libsquishdb.dylib`, `libsquishdb.a`, `squishdb`

Set `SQUISHDB_LIB_PATH` (for FFI bindings) or `SQUISHDB_BIN` (for RPC bindings) if you move the built binary outside `target/release/`.

---

## Language matrix

| Language | Binding Path | Mechanism | External Dependencies |
|---|---|---|---|
| **Python** | `bindings/python/` | In-process C FFI (`ctypes`) + PEP 249 DB-API 2.0 | None (stdlib only) |
| **TypeScript / JavaScript** (Node.js, Bun, Deno) | `bindings/typescript/` | Persistent `squishdb rpc` over stdio | None (Node >= 18) |
| **Java / Kotlin (JVM)** | `bindings/java/` | In-process Java 22+ Foreign Function & Memory API | None (JDK 22+) |
| **C# / .NET** | `bindings/csharp/` | In-process P/Invoke (`DllImport`) | None (.NET 8+) |
| **Go** | `bindings/go/` | In-process `cgo` against `include/squishdb.h` | None (stdlib + cgo) |
| **C** | `include/squishdb.h` | Direct C99 header + shared/static lib | None |
| **C++17** | `include/squishdb.hpp` | Header-only RAII wrapper over `squishdb.h` | None |
| **Rust** | `src/lib.rs` | Native crate | None |
| **PHP / Ruby / Swift / Dart / Zig / etc.** | `include/squishdb.h` or `squishdb rpc` | C FFI or newline-delimited JSON-RPC over stdio | None |

---

## Python

Zero-dependency package implementing PEP 249 (DB-API 2.0) plus `query()`, `insert_batch()`, and `bulk_insert()`.

```bash
pip install -e bindings/python
# Or run tests directly:
PYTHONPATH=bindings/python python -m unittest discover -s bindings/python/tests
```

```python
import squishdb

with squishdb.connect("sensors.sqdb", compression="balanced") as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY,
            device TEXT NOT NULL,
            temp REAL,
            payload BLOB
        ) CLUSTER BY (device);
    """)
    conn.execute("INSERT INTO readings VALUES (?, ?, ?, ?);", (1, "dev-01", 21.5, b"\xde\xad"))
    conn.bulk_insert("readings", [(2, "dev-01", 22.1, None), (3, "dev-02", 19.4, None)])

    for row in conn.query("SELECT device, AVG(temp) AS avg_temp FROM readings GROUP BY device;"):
        print(row)
```

---

## TypeScript & JavaScript (Node.js, Bun, Deno)

Full TypeScript types in `bindings/typescript/src/index.d.ts`, zero npm dependencies required.

```bash
cd bindings/typescript
npm test
```

```ts
import { Database } from "./bindings/typescript/src/index.mjs";

const db = await Database.open("events.sqdb", { compression: "balanced" });
await db.execute(`
  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    amount REAL,
    payload BLOB
  ) CLUSTER BY (kind);
`);

await db.execute("INSERT INTO events VALUES (?, ?, ?, ?);", [
  1,
  "purchase",
  49.99,
  new Uint8Array([1, 2, 3]),
]);

await db.bulkInsert("events", [
  [2, "purchase", 19.5, null],
  [3, "signup", 0.0, null],
]);

const rows = await db.query(
  "SELECT kind, COUNT(*) AS n, SUM(amount) AS total FROM events GROUP BY kind;",
);
console.log(rows);
await db.close();
```

---

## Java & Kotlin (JVM)

Uses the standard Java 22+ Foreign Function & Memory API (`java.lang.foreign`) with zero Maven/Gradle dependencies.

```bash
javac -d bindings/java/out bindings/java/src/com/squishdb/SquishDb.java bindings/java/test/SquishDbTest.java
java --enable-native-access=ALL-UNNAMED -cp bindings/java/out SquishDbTest
```

```java
import com.squishdb.SquishDb;

try (SquishDb db = SquishDb.open("metrics.sqdb")) {
    db.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, host TEXT, cpu REAL) CLUSTER BY (host);");
    db.execute("INSERT INTO metrics VALUES (?, ?, ?);", 1L, "web-01", 0.42);
    System.out.println(db.query("SELECT host, AVG(cpu) AS avg_cpu FROM metrics GROUP BY host;"));
}
```

Kotlin works directly against `com.squishdb.SquishDb`:

```kotlin
import com.squishdb.SquishDb

SquishDb.open("metrics.sqdb").use { db ->
    db.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, host TEXT, cpu REAL);")
    db.execute("INSERT INTO metrics VALUES (?, ?, ?);", 1L, "web-01", 0.42)
    println(db.query("SELECT * FROM metrics;"))
}
```

---

## C# / .NET

P/Invoke class library (`net8.0+`) with automatic native library resolution and `IDisposable`.

```bash
dotnet run --project bindings/csharp/SquishDb.Tests
```

```csharp
using SquishDb;

using var db = SquishDatabase.Open("orders.sqdb");
db.Execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total REAL);");
db.Execute("INSERT INTO orders VALUES (?, ?, ?);", 1L, "alice", 29.50);
var rows = db.Query("SELECT customer, SUM(total) AS spent FROM orders GROUP BY customer;");
```

---

## Go

Standard `cgo` package linking directly against `include/squishdb.h` and `libsquishdb`.

```bash
cd bindings/go
go test -v ./...
```

```go
db, err := squishdb.Open("sensors.sqdb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

_, _ = db.Execute("CREATE TABLE sensors (id INTEGER PRIMARY KEY, device TEXT, temp REAL);")
_, _ = db.Execute("INSERT INTO sensors VALUES (?, ?, ?);", 1, "dev-01", 21.5)
rows, _ := db.Query("SELECT device, AVG(temp) AS avg_temp FROM sensors GROUP BY device;")
```

---

## C and C++17

Include `include/squishdb.h` (C99) or `include/squishdb.hpp` (header-only C++17 RAII wrapper) and link against `libsquishdb` / `squishdb.dll.lib`.

See runnable examples in:
- `bindings/c/example.c`
- `bindings/cpp/example.cpp`

---

## Persistent JSON-RPC Protocol (`squishdb rpc`)

Any other language (PHP, Ruby, Swift, Dart, Elixir, shell scripts, etc.) can either load `libsquishdb` through its native FFI or spawn a single long-lived process:

```bash
squishdb rpc app.sqdb [--readonly] [--profile fast|balanced|max|none] [--sync-wal]
```

Send one JSON object per line on `stdin` and read one JSON response per line on `stdout`:

```json
{"id": 1, "method": "execute", "sql": "SELECT * FROM users WHERE id = ?;", "params": [42]}
```

Response:

```json
{"id": 1, "ok": true, "result": [{"kind": "rows", "columns": ["id", "email"], "rows": [[42, "alice@example.com"]]}]}
```

Supported methods: `ping`, `execute`, `begin`, `commit`, `rollback`, `in_transaction`, `insert_batch`, `bulk_insert`, `checkpoint`, `vacuum`, `backup_to`, `stats`, `tables`, and `close`.

Special value encodings for lossless round-trips over JSON:
- `BLOB`: `{"$blob": "<base64>"}`
- 64-bit integers outside JS `Number.MAX_SAFE_INTEGER`: `{"$int": "9007199254740999"}`
- Non-finite IEEE-754 floats: `{"$real": "NaN" | "Infinity" | "-Infinity"}`

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { Database, SquishError } from "../src/index.mjs";

test("SquishDB TypeScript/Node.js end-to-end roundtrip", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "squishdb-node-"));
  const dbPath = path.join(dir, "app.sqdb");
  const backupPath = path.join(dir, "backup.sqdb");

  try {
    const db = await Database.open(dbPath, { compression: "balanced" });
    const ver = await db.version();
    assert.ok(ver.length > 0);

    await db.execute(`
      CREATE TABLE events (
        id INTEGER PRIMARY KEY,
        kind TEXT NOT NULL,
        amount REAL,
        payload BLOB
      ) CLUSTER BY (kind);
    `);

    const blob = new Uint8Array([0, 1, 2, 254, 255]);
    const evil = "signup'; DROP TABLE events; --";
    await db.execute("INSERT INTO events VALUES (?, ?, ?, ?);", [1, evil, 12.5, blob]);

    // Extreme 64-bit integers + Unicode + non-finite floats + empty blob
    const i64Min = -9223372036854775808n;
    const i64Max = 9223372036854775807n;
    await db.execute("INSERT INTO events VALUES ($1, $2, $3, $4);", [
      i64Min,
      "日本語 🚀 \"quotes\"\nline2",
      Number.NaN,
      new Uint8Array(0),
    ]);
    await db.execute("INSERT INTO events VALUES ($1, $2, $3, $4);", [
      i64Max,
      "inf",
      Infinity,
      null,
    ]);

    // Bulk PAX insert & WAL insertBatch
    const inserted = await db.bulkInsert("events", [
      [2, "purchase", 49.99, null],
      [3, "purchase", 19.5, new Uint8Array([9, 8, 7])],
    ]);
    assert.equal(inserted, 2);

    const batched = await db.insertBatch("events", [[4, "refund", -Infinity, null]]);
    assert.equal(batched, 1);

    // Transaction rollback
    await assert.rejects(
      db.transaction(async () => {
        assert.equal(await db.inTransaction(), true);
        await db.execute("INSERT INTO events VALUES (?, ?, ?, ?);", [99, "rolled", 0.0, null]);
        throw new Error("abort");
      }),
      /abort/,
    );
    assert.equal(await db.inTransaction(), false);

    // Transaction commit
    const committed = await db.transaction(async () => {
      await db.execute("INSERT INTO events VALUES (?, ?, ?, ?);", [5, "commit_ok", 1.25, null]);
      return "done";
    });
    assert.equal(committed, "done");

    const row = await db.queryOne("SELECT id, kind, amount, payload FROM events WHERE id = ?;", [1]);
    assert.equal(row.id, 1);
    assert.equal(row.kind, evil);
    assert.equal(row.amount, 12.5);
    assert.deepEqual(row.payload, blob);

    const minRow = await db.queryOne("SELECT id, kind, amount, payload FROM events WHERE id = ?;", [
      i64Min,
    ]);
    assert.equal(minRow.id, i64Min);
    assert.equal(minRow.kind, "日本語 🚀 \"quotes\"\nline2");
    assert.ok(Number.isNaN(minRow.amount));
    assert.deepEqual(minRow.payload, new Uint8Array(0));

    const maxRow = await db.queryOne("SELECT id, amount FROM events WHERE id = ?;", [i64Max]);
    assert.equal(maxRow.id, i64Max);
    assert.equal(maxRow.amount, Infinity);

    const refRow = await db.queryOne("SELECT amount FROM events WHERE id = ?;", [4]);
    assert.equal(refRow.amount, -Infinity);

    const summary = await db.query(
      "SELECT kind, COUNT(*) AS n, SUM(amount) AS total FROM events WHERE kind = ? GROUP BY kind;",
      ["purchase"],
    );
    assert.equal(summary.length, 1);
    assert.equal(summary[0].n, 2);
    assert.ok(Math.abs(summary[0].total - 69.49) < 1e-6);

    assert.deepEqual(await db.tables(), ["events"]);
    const stats = await db.stats();
    assert.equal(stats.tables[0].live_rows, 7);

    await db.vacuum();
    await db.checkpoint();
    await db.backupTo(backupPath);
    assert.ok(fs.existsSync(backupPath));

    await assert.rejects(db.execute("SELECT * FROM does_not_exist;"), SquishError);
    await db.close();

    // Closed connection must reject cleanly
    await assert.rejects(db.execute("SELECT 1;"), SquishError);

    // Concurrent read-only readers
    const ro1 = await Database.openReadOnly(dbPath);
    const ro2 = await Database.openReadOnly(dbPath);
    const c1 = await ro1.queryOne("SELECT COUNT(*) AS c FROM events;");
    const c2 = await ro2.queryOne("SELECT COUNT(*) AS c FROM events;");
    assert.equal(c1.c, 7);
    assert.equal(c2.c, 7);
    await assert.rejects(
      ro1.execute("INSERT INTO events VALUES (999, 'ro', 0.0, NULL);"),
      SquishError,
    );
    await ro1.close();
    await ro2.close();

    // Opening a non-existent DB in read-only mode must reject cleanly without EPIPE crash
    await assert.rejects(
      Database.openReadOnly(path.join(dir, "does_not_exist.sqdb")),
      SquishError,
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

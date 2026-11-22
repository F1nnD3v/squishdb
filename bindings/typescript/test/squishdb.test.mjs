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

    // Large 64-bit integer roundtrip via BigInt
    const big = 9007199254740999n;
    await db.execute("INSERT INTO events VALUES ($1, $2, $3, $4);", [big, "bigint", null, null]);

    // Bulk PAX insert
    const inserted = await db.bulkInsert("events", [
      [2, "purchase", 49.99, null],
      [3, "purchase", 19.5, new Uint8Array([9, 8, 7])],
    ]);
    assert.equal(inserted, 2);

    // Transaction rollback
    await assert.rejects(
      db.transaction(async () => {
        await db.execute("INSERT INTO events VALUES (?, ?, ?, ?);", [99, "rolled", 0.0, null]);
        throw new Error("abort");
      }),
      /abort/,
    );

    const row = await db.queryOne("SELECT id, kind, amount, payload FROM events WHERE id = ?;", [1]);
    assert.equal(row.id, 1);
    assert.equal(row.kind, evil);
    assert.equal(row.amount, 12.5);
    assert.deepEqual(row.payload, blob);

    const bigRow = await db.queryOne("SELECT id FROM events WHERE kind = ?;", ["bigint"]);
    assert.equal(bigRow.id, big);

    const summary = await db.query(
      "SELECT kind, COUNT(*) AS n, SUM(amount) AS total FROM events WHERE kind = ? GROUP BY kind;",
      ["purchase"],
    );
    assert.equal(summary.length, 1);
    assert.equal(summary[0].n, 2);
    assert.ok(Math.abs(summary[0].total - 69.49) < 1e-6);

    assert.deepEqual(await db.tables(), ["events"]);
    const stats = await db.stats();
    assert.equal(stats.tables[0].live_rows, 4);

    await db.backupTo(backupPath);
    assert.ok(fs.existsSync(backupPath));

    await assert.rejects(db.execute("SELECT * FROM does_not_exist;"), SquishError);
    await db.close();

    // Concurrent read-only readers
    const ro1 = await Database.openReadOnly(dbPath);
    const ro2 = await Database.openReadOnly(dbPath);
    const c1 = await ro1.queryOne("SELECT COUNT(*) AS c FROM events;");
    const c2 = await ro2.queryOne("SELECT COUNT(*) AS c FROM events;");
    assert.equal(c1.c, 4);
    assert.equal(c2.c, 4);
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

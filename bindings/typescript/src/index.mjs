import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

export class SquishError extends Error {
  constructor(message, code = "Error") {
    super(message);
    this.name = "SquishError";
    this.code = code;
  }
}

function findBinary(explicitPath) {
  if (explicitPath) {
    return explicitPath;
  }
  if (process.env.SQUISHDB_BIN) {
    return process.env.SQUISHDB_BIN;
  }
  const exe = process.platform === "win32" ? "squishdb.exe" : "squishdb";
  const repoRoot = path.resolve(__dirname, "..", "..", "..");
  const candidates = [
    path.join(repoRoot, "target", "release", exe),
    path.join(repoRoot, "target", "debug", exe),
    path.join(process.cwd(), "target", "release", exe),
    path.join(process.cwd(), "target", "debug", exe),
  ];
  for (const cand of candidates) {
    if (fs.existsSync(cand)) {
      return cand;
    }
  }
  return exe;
}

function encodeValue(v) {
  if (v === null || v === undefined) {
    return null;
  }
  if (typeof v === "boolean") {
    return v ? 1 : 0;
  }
  if (typeof v === "bigint") {
    if (v >= MIN_SAFE && v <= MAX_SAFE) {
      return Number(v);
    }
    return { $int: v.toString() };
  }
  if (typeof v === "number") {
    if (Number.isNaN(v)) return { $real: "NaN" };
    if (v === Infinity) return { $real: "Infinity" };
    if (v === -Infinity) return { $real: "-Infinity" };
    return v;
  }
  if (typeof v === "string") {
    return v;
  }
  if (v instanceof Uint8Array || Buffer.isBuffer(v)) {
    return { $blob: Buffer.from(v).toString("base64") };
  }
  throw new TypeError(`Unsupported SQL value type: ${typeof v}`);
}

function decodeValue(v) {
  if (v === null || typeof v !== "object") {
    return v;
  }
  if (typeof v.$blob === "string") {
    return new Uint8Array(Buffer.from(v.$blob, "base64"));
  }
  if (typeof v.$int === "string") {
    return BigInt(v.$int);
  }
  if (v.$real !== undefined) {
    if (v.$real === "NaN") return Number.NaN;
    if (v.$real === "Infinity") return Infinity;
    if (v.$real === "-Infinity") return -Infinity;
    return Number(v.$real);
  }
  return v;
}

function decodeResults(raw) {
  return raw.map((item) => {
    if (item.kind === "rows") {
      return {
        kind: "rows",
        columns: item.columns,
        rows: item.rows.map((row) => row.map(decodeValue)),
      };
    }
    return item;
  });
}

export class Database {
  #proc;
  #rl;
  #nextId = 1;
  #pending = new Map();
  #closed = false;
  #stderr = "";

  constructor(proc) {
    this.#proc = proc;
    this.#rl = readline.createInterface({ input: proc.stdout });

    proc.stderr.on("data", (chunk) => {
      this.#stderr += chunk.toString("utf8");
    });

    // Prevent unhandled EPIPE error events if the child exits while writing
    proc.stdin.on("error", () => {});

    this.#rl.on("line", (line) => {
      if (!line.trim()) return;
      let msg;
      try {
        msg = JSON.parse(line);
      } catch (err) {
        return;
      }
      const entry = this.#pending.get(msg.id);
      if (!entry) return;
      this.#pending.delete(msg.id);
      if (msg.ok) {
        entry.resolve(msg.result);
      } else {
        const code = msg.error?.code ?? "Error";
        const message = msg.error?.message ?? "Unknown SquishDB error";
        entry.reject(new SquishError(message, code));
      }
    });

    const failAll = (reason) => {
      this.#closed = true;
      this.#rl.close();
      for (const [, entry] of this.#pending) {
        entry.reject(reason);
      }
      this.#pending.clear();
    };

    proc.on("error", (err) => {
      failAll(new SquishError(`Failed to launch squishdb binary: ${err.message}`, "Io"));
    });

    proc.on("close", (code) => {
      const detail = this.#stderr.trim() || `squishdb process exited with code ${code}`;
      failAll(new SquishError(detail, "Io"));
    });
  }

  static async open(dbPath, options = {}) {
    const bin = findBinary(options.binaryPath);
    const args = ["rpc", String(dbPath)];
    if (options.readOnly) {
      args.push("--readonly");
    }
    if (options.compression) {
      args.push("--profile", options.compression);
    }
    if (options.syncWal) {
      args.push("--sync-wal");
    }

    const proc = spawn(bin, args, {
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
    const db = new Database(proc);
    // Verify the database opened cleanly
    await db.#call("ping");
    return db;
  }

  static openReadOnly(dbPath, options = {}) {
    return Database.open(dbPath, { ...options, readOnly: true });
  }

  #call(method, payload = {}) {
    if (this.#closed) {
      return Promise.reject(new SquishError("Database connection is closed", "InvalidData"));
    }
    const id = this.#nextId++;
    const line = JSON.stringify({ id, method, ...payload }) + "\n";
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#proc.stdin.write(line, "utf8", (err) => {
        if (err) {
          this.#pending.delete(id);
          reject(new SquishError(err.message, "Io"));
        }
      });
    });
  }

  async version() {
    const info = await this.#call("ping");
    return info.version;
  }

  async execute(sql, params = []) {
    const encoded = params && params.length > 0 ? params.map(encodeValue) : undefined;
    const raw = await this.#call("execute", encoded ? { sql, params: encoded } : { sql });
    return decodeResults(raw);
  }

  async query(sql, params = []) {
    const results = await this.execute(sql, params);
    for (let i = results.length - 1; i >= 0; i--) {
      if (results[i].kind === "rows") {
        const { columns, rows } = results[i];
        return rows.map((row) => {
          const obj = {};
          for (let c = 0; c < columns.length; c++) {
            obj[columns[c]] = row[c];
          }
          return obj;
        });
      }
    }
    return [];
  }

  async queryOne(sql, params = []) {
    const rows = await this.query(sql, params);
    return rows.length > 0 ? rows[0] : null;
  }

  async begin() {
    await this.#call("begin");
  }

  async commit() {
    await this.#call("commit");
  }

  async rollback() {
    await this.#call("rollback");
  }

  async inTransaction() {
    return Boolean(await this.#call("in_transaction"));
  }

  async transaction(fn) {
    await this.begin();
    try {
      const value = await fn();
      await this.commit();
      return value;
    } catch (err) {
      try {
        await this.rollback();
      } catch {
        // ignore rollback error after primary failure
      }
      throw err;
    }
  }

  async insertBatch(table, rows) {
    const encoded = rows.map((r) => r.map(encodeValue));
    return Number(await this.#call("insert_batch", { table, rows: encoded }));
  }

  async bulkInsert(table, rows) {
    const encoded = rows.map((r) => r.map(encodeValue));
    return Number(await this.#call("bulk_insert", { table, rows: encoded }));
  }

  async checkpoint() {
    await this.#call("checkpoint");
  }

  async vacuum() {
    await this.#call("vacuum");
  }

  async backupTo(destPath) {
    await this.#call("backup_to", { path: String(destPath) });
  }

  async stats() {
    return await this.#call("stats");
  }

  async tables() {
    return await this.#call("tables");
  }

  async close() {
    if (this.#closed) return;
    try {
      await this.#call("close");
    } catch {
      // ignore if process already exited
    }
    this.#closed = true;
    this.#rl.close();
    this.#proc.stdin.end();
    await new Promise((resolve) => {
      if (this.#proc.exitCode !== null) {
        resolve();
      } else {
        this.#proc.once("close", () => resolve());
      }
    });
  }
}

export default Database;

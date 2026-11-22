export type CompressionProfile = "fast" | "balanced" | "max" | "none";

export type SqlValue = null | number | bigint | boolean | string | Uint8Array;

export interface OpenOptions {
  /** Open in shared read-only mode (multiple concurrent readers allowed). */
  readOnly?: boolean;
  /** Compression profile for new PAX pages. Default: "balanced". */
  compression?: CompressionProfile;
  /** Fsync the WAL on every commit. Default: false. */
  syncWal?: boolean;
  /** Explicit path to the `squishdb` CLI executable (used by the zero-dependency RPC transport). */
  binaryPath?: string;
}

export interface RowsResult {
  kind: "rows";
  columns: string[];
  rows: SqlValue[][];
}

export interface AffectedResult {
  kind: "affected";
  affected: number;
}

export interface MessageResult {
  kind: "message";
  message: string;
}

export type StatementResult = RowsResult | AffectedResult | MessageResult;

export interface TableStats {
  name: string;
  live_rows: number;
  deleted_slots: number;
  hot_buffer_rows: number;
  page_count: number;
  raw_pax_bytes: number;
  stored_pax_bytes: number;
  compression_ratio: number;
  cluster_by: string[];
}

export interface DbStats {
  db_file_bytes: number;
  wal_file_bytes: number;
  catalog_stored_bytes: number;
  catalog_raw_bytes: number;
  generation: number;
  index_count: number;
  index_disk_bytes: number;
  cache_hits: number;
  cache_misses: number;
  index_cache_hits: number;
  index_cache_misses: number;
  read_only: boolean;
  tables: TableStats[];
}

export class SquishError extends Error {
  readonly code: string;
  constructor(message: string, code?: string);
}

export class Database {
  /** Open or create a SquishDB database file. */
  static open(path: string, options?: OpenOptions): Promise<Database>;

  /** Open a SquishDB database file in shared read-only mode. */
  static openReadOnly(path: string, options?: Omit<OpenOptions, "readOnly">): Promise<Database>;

  /** Execute one or more SQL statements with optional positional (`?`) or numbered (`$1`) parameters. */
  execute(sql: string, params?: readonly SqlValue[]): Promise<StatementResult[]>;

  /** Run a SELECT statement and return rows as objects keyed by column name. */
  query<T extends Record<string, SqlValue> = Record<string, SqlValue>>(
    sql: string,
    params?: readonly SqlValue[],
  ): Promise<T[]>;

  /** Run a SELECT statement and return the first row object, or `null` if empty. */
  queryOne<T extends Record<string, SqlValue> = Record<string, SqlValue>>(
    sql: string,
    params?: readonly SqlValue[],
  ): Promise<T | null>;

  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  inTransaction(): Promise<boolean>;

  /** Run `fn` inside a transaction; commits on resolve and rolls back on throw. */
  transaction<T>(fn: () => Promise<T>): Promise<T>;

  /** Crash-safe WAL-backed batch insert. */
  insertBatch(table: string, rows: readonly (readonly SqlValue[])[]): Promise<number>;

  /** Direct PAX-v4 bulk load (fastest initial ingestion). */
  bulkInsert(table: string, rows: readonly (readonly SqlValue[])[]): Promise<number>;

  checkpoint(): Promise<void>;
  vacuum(): Promise<void>;
  backupTo(destPath: string): Promise<void>;
  stats(): Promise<DbStats>;
  tables(): Promise<string[]>;
  version(): Promise<string>;
  close(): Promise<void>;
}

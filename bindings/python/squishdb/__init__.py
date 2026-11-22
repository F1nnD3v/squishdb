"""SquishDB Python bindings (PEP 249 DB-API 2.0 + high-throughput bulk API)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

from ._ffi import (
    PROFILE_BALANCED,
    PROFILE_MAP,
    SQUISHDB_OK,
    extract_results,
    last_error,
    load_library,
    push_value_to_batch,
    push_value_to_params,
    read_owned_cstr,
)

# PEP 249 module globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


class Error(Exception):
    """Base SquishDB exception (PEP 249)."""


class DatabaseError(Error):
    """Error related to the database engine or storage."""


class OperationalError(DatabaseError):
    """I/O, lock busy, or operational error."""


class ProgrammingError(DatabaseError):
    """SQL syntax, schema, or usage error."""


def version(lib_path: Optional[str] = None) -> str:
    """Return the underlying libsquishdb version string."""
    lib = load_library(lib_path)
    return lib.squishdb_version().decode("utf-8")


class Cursor:
    """PEP 249 DB-API 2.0 cursor over a SquishDB Connection."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self.description: Optional[list[tuple[Any, ...]]] = None
        self.rowcount: int = -1
        self.arraysize: int = 1
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cursor is closed")

    def execute(
        self,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
    ) -> Cursor:
        self._ensure_open()
        results = self._conn.execute_raw(sql, parameters)
        self._rows = []
        self._pos = 0
        self.description = None
        self.rowcount = -1

        if results:
            last = results[-1]
            if last["kind"] == "rows":
                self.description = [
                    (col, None, None, None, None, None, None) for col in last["columns"]
                ]
                self._rows = last["rows"]
                self.rowcount = len(self._rows)
            elif last["kind"] == "affected":
                self.rowcount = last["affected"]
        return self

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> Cursor:
        self._ensure_open()
        total = 0
        for params in seq_of_parameters:
            self.execute(sql, params)
            if self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total
        return self

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        self._ensure_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> list[tuple[Any, ...]]:
        self._ensure_open()
        n = self.arraysize if size is None else size
        if n <= 0:
            return []
        chunk = self._rows[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._ensure_open()
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class Connection:
    """Embedded SquishDB database connection (PEP 249 + SquishDB extensions)."""

    def __init__(
        self,
        path: Union[str, Path],
        *,
        read_only: bool = False,
        compression: str = "balanced",
        sync_wal: bool = False,
        page_rows: int = 0,
        auto_checkpoint_rows: int = 0,
        cache_pages: int = 0,
        index_cache_blocks: int = 0,
        lib_path: Optional[str] = None,
    ) -> None:
        self._lib = load_library(lib_path)
        self._handle: Optional[int] = None
        path_bytes = str(path).encode("utf-8")

        if read_only:
            handle = self._lib.squishdb_open_readonly(path_bytes)
        else:
            key = compression.lower()
            if key not in PROFILE_MAP:
                raise ProgrammingError(f"Unknown compression profile: {compression!r}")
            handle = self._lib.squishdb_open_with_options(
                path_bytes,
                PROFILE_MAP[key],
                1 if sync_wal else 0,
                page_rows,
                auto_checkpoint_rows,
                cache_pages,
                index_cache_blocks,
            )

        if not handle:
            raise OperationalError(last_error(self._lib) or f"Failed to open {path}")
        self._handle = handle

    def _ensure_open(self) -> int:
        if not self._handle:
            raise ProgrammingError("Database connection is closed")
        return self._handle

    def cursor(self) -> Cursor:
        self._ensure_open()
        return Cursor(self)

    def execute_raw(
        self,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        handle = self._ensure_open()
        sql_bytes = sql.encode("utf-8")
        if parameters:
            p = self._lib.squishdb_params_new()
            try:
                for v in parameters:
                    push_value_to_params(self._lib, p, v)
                res_ptr = self._lib.squishdb_execute_params(handle, sql_bytes, p)
            finally:
                self._lib.squishdb_params_free(p)
        else:
            res_ptr = self._lib.squishdb_execute(handle, sql_bytes)

        if not res_ptr:
            raise DatabaseError(last_error(self._lib) or "SQL execution failed")
        return extract_results(self._lib, res_ptr)

    def execute(
        self,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
    ) -> Cursor:
        """Convenience shortcut matching sqlite3.Connection.execute."""
        cur = self.cursor()
        return cur.execute(sql, parameters)

    def query(
        self,
        sql: str,
        parameters: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT and return rows as a list of dicts keyed by column name."""
        results = self.execute_raw(sql, parameters)
        for item in reversed(results):
            if item["kind"] == "rows":
                cols = item["columns"]
                return [dict(zip(cols, row)) for row in item["rows"]]
        return []

    def begin(self) -> None:
        handle = self._ensure_open()
        rc = self._lib.squishdb_begin(handle)
        if rc != SQUISHDB_OK:
            raise DatabaseError(last_error(self._lib) or "BEGIN failed")

    def commit(self) -> None:
        handle = self._ensure_open()
        if self._lib.squishdb_in_transaction(handle):
            rc = self._lib.squishdb_commit(handle)
            if rc != SQUISHDB_OK:
                raise DatabaseError(last_error(self._lib) or "COMMIT failed")

    def rollback(self) -> None:
        handle = self._ensure_open()
        if self._lib.squishdb_in_transaction(handle):
            rc = self._lib.squishdb_rollback(handle)
            if rc != SQUISHDB_OK:
                raise DatabaseError(last_error(self._lib) or "ROLLBACK failed")

    @property
    def in_transaction(self) -> bool:
        return bool(self._handle and self._lib.squishdb_in_transaction(self._handle))

    def _run_batch(self, table: str, rows: Iterable[Sequence[Any]], bulk: bool) -> int:
        handle = self._ensure_open()
        rows_list = list(rows)
        if not rows_list:
            return 0
        ncols = len(rows_list[0])
        batch = self._lib.squishdb_batch_new(ncols)
        try:
            for row in rows_list:
                if len(row) != ncols:
                    raise ProgrammingError(
                        f"Inconsistent column count in batch: expected {ncols}, got {len(row)}"
                    )
                for val in row:
                    push_value_to_batch(self._lib, batch, val)
            table_bytes = table.encode("utf-8")
            n = (
                self._lib.squishdb_bulk_insert(handle, table_bytes, batch)
                if bulk
                else self._lib.squishdb_insert_batch(handle, table_bytes, batch)
            )
            if n < 0:
                raise DatabaseError(last_error(self._lib) or "Batch insert failed")
            return int(n)
        finally:
            self._lib.squishdb_batch_free(batch)

    def insert_batch(self, table: str, rows: Iterable[Sequence[Any]]) -> int:
        """Crash-safe WAL-backed batch insert."""
        return self._run_batch(table, rows, bulk=False)

    def bulk_insert(self, table: str, rows: Iterable[Sequence[Any]]) -> int:
        """Direct PAX-v4 columnar bulk load (fastest initial ingestion)."""
        return self._run_batch(table, rows, bulk=True)

    def checkpoint(self) -> None:
        handle = self._ensure_open()
        rc = self._lib.squishdb_checkpoint(handle)
        if rc != SQUISHDB_OK:
            raise OperationalError(last_error(self._lib) or "CHECKPOINT failed")

    def vacuum(self) -> None:
        handle = self._ensure_open()
        rc = self._lib.squishdb_vacuum(handle)
        if rc != SQUISHDB_OK:
            raise OperationalError(last_error(self._lib) or "VACUUM failed")

    def backup_to(self, dest_path: Union[str, Path]) -> None:
        handle = self._ensure_open()
        rc = self._lib.squishdb_backup_to(handle, str(dest_path).encode("utf-8"))
        if rc != SQUISHDB_OK:
            raise OperationalError(last_error(self._lib) or "BACKUP failed")

    def stats(self) -> dict[str, Any]:
        handle = self._ensure_open()
        ptr = self._lib.squishdb_stats_json(handle)
        if not ptr:
            raise OperationalError(last_error(self._lib) or "stats() failed")
        return json.loads(read_owned_cstr(self._lib, ptr))

    def stats_report(self) -> str:
        handle = self._ensure_open()
        ptr = self._lib.squishdb_stats_report(handle)
        if not ptr:
            raise OperationalError(last_error(self._lib) or "stats_report() failed")
        return read_owned_cstr(self._lib, ptr)

    def tables(self) -> list[str]:
        handle = self._ensure_open()
        ptr = self._lib.squishdb_table_names_json(handle)
        if not ptr:
            raise OperationalError(last_error(self._lib) or "tables() failed")
        return json.loads(read_owned_cstr(self._lib, ptr))

    def close(self) -> None:
        if self._handle:
            self._lib.squishdb_close(self._handle)
            self._handle = None

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.in_transaction:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


def connect(path: Union[str, Path], **kwargs: Any) -> Connection:
    """Open a SquishDB database connection (PEP 249 entry point)."""
    return Connection(path, **kwargs)


__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Error",
    "DatabaseError",
    "OperationalError",
    "ProgrammingError",
    "Connection",
    "Cursor",
    "connect",
    "version",
]

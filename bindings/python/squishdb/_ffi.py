"""Low-level ctypes loader and C ABI bindings for libsquishdb."""

from __future__ import annotations

import ctypes
from ctypes import (
    POINTER,
    byref,
    c_char,
    c_char_p,
    c_double,
    c_int,
    c_int64,
    c_size_t,
    c_uint8,
    c_uint64,
    c_void_p,
)
import os
from pathlib import Path
import platform
import sys
from typing import Optional

# Error codes
SQUISHDB_OK = 0
SQUISHDB_ERR_IO = 1
SQUISHDB_ERR_CORRUPTION = 2
SQUISHDB_ERR_SQL = 3
SQUISHDB_ERR_SCHEMA = 4
SQUISHDB_ERR_INVALID = 5
SQUISHDB_ERR_BUSY = 6
SQUISHDB_ERR_PANIC = 7
SQUISHDB_ERR_NULL = 8

# Compression profiles
PROFILE_FAST = 0
PROFILE_BALANCED = 1
PROFILE_MAX = 2
PROFILE_NONE = 3

PROFILE_MAP = {
    "fast": PROFILE_FAST,
    "balanced": PROFILE_BALANCED,
    "max": PROFILE_MAX,
    "none": PROFILE_NONE,
}

# Result kinds
RESULT_ROWS = 0
RESULT_AFFECTED = 1
RESULT_MESSAGE = 2

# Value types
TYPE_NULL = 0
TYPE_INTEGER = 1
TYPE_REAL = 2
TYPE_TEXT = 3
TYPE_BLOB = 4

_LIB: Optional[ctypes.CDLL] = None


def _platform_lib_names() -> list[str]:
    system = platform.system().lower()
    if system == "windows":
        return ["squishdb.dll"]
    if system == "darwin":
        return ["libsquishdb.dylib"]
    return ["libsquishdb.so"]


def _candidate_paths() -> list[Path]:
    names = _platform_lib_names()
    candidates: list[Path] = []

    env_path = os.environ.get("SQUISHDB_LIB_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            for n in names:
                candidates.append(p / n)
        else:
            candidates.append(p)

    here = Path(__file__).resolve().parent
    repo_root = here.parent.parent.parent
    search_dirs = [
        here,
        here.parent,
        repo_root / "target" / "release",
        repo_root / "target" / "debug",
        Path.cwd() / "target" / "release",
        Path.cwd() / "target" / "debug",
    ]
    for d in search_dirs:
        for n in names:
            candidates.append(d / n)
    return candidates


def _configure_signatures(lib: ctypes.CDLL) -> ctypes.CDLL:
    lib.squishdb_version.argtypes = []
    lib.squishdb_version.restype = c_char_p

    lib.squishdb_last_error.argtypes = []
    lib.squishdb_last_error.restype = c_char_p

    lib.squishdb_string_free.argtypes = [c_void_p]
    lib.squishdb_string_free.restype = None

    lib.squishdb_open.argtypes = [c_char_p]
    lib.squishdb_open.restype = c_void_p

    lib.squishdb_open_readonly.argtypes = [c_char_p]
    lib.squishdb_open_readonly.restype = c_void_p

    lib.squishdb_open_with_options.argtypes = [
        c_char_p,
        c_int,
        c_int,
        c_size_t,
        c_size_t,
        c_size_t,
        c_size_t,
    ]
    lib.squishdb_open_with_options.restype = c_void_p

    lib.squishdb_close.argtypes = [c_void_p]
    lib.squishdb_close.restype = None

    lib.squishdb_params_new.argtypes = []
    lib.squishdb_params_new.restype = c_void_p

    lib.squishdb_params_clear.argtypes = [c_void_p]
    lib.squishdb_params_clear.restype = None

    lib.squishdb_params_free.argtypes = [c_void_p]
    lib.squishdb_params_free.restype = None

    lib.squishdb_params_push_null.argtypes = [c_void_p]
    lib.squishdb_params_push_null.restype = None

    lib.squishdb_params_push_int.argtypes = [c_void_p, c_int64]
    lib.squishdb_params_push_int.restype = None

    lib.squishdb_params_push_real.argtypes = [c_void_p, c_double]
    lib.squishdb_params_push_real.restype = None

    lib.squishdb_params_push_text.argtypes = [c_void_p, POINTER(c_char), c_size_t]
    lib.squishdb_params_push_text.restype = c_int

    lib.squishdb_params_push_blob.argtypes = [c_void_p, POINTER(c_uint8), c_size_t]
    lib.squishdb_params_push_blob.restype = c_int

    lib.squishdb_execute.argtypes = [c_void_p, c_char_p]
    lib.squishdb_execute.restype = c_void_p

    lib.squishdb_execute_params.argtypes = [c_void_p, c_char_p, c_void_p]
    lib.squishdb_execute_params.restype = c_void_p

    lib.squishdb_results_free.argtypes = [c_void_p]
    lib.squishdb_results_free.restype = None

    lib.squishdb_results_count.argtypes = [c_void_p]
    lib.squishdb_results_count.restype = c_size_t

    lib.squishdb_result_kind.argtypes = [c_void_p, c_size_t]
    lib.squishdb_result_kind.restype = c_int

    lib.squishdb_result_affected.argtypes = [c_void_p, c_size_t]
    lib.squishdb_result_affected.restype = c_uint64

    lib.squishdb_result_message.argtypes = [c_void_p, c_size_t]
    lib.squishdb_result_message.restype = c_char_p

    lib.squishdb_result_column_count.argtypes = [c_void_p, c_size_t]
    lib.squishdb_result_column_count.restype = c_size_t

    lib.squishdb_result_column_name.argtypes = [c_void_p, c_size_t, c_size_t]
    lib.squishdb_result_column_name.restype = c_char_p

    lib.squishdb_result_row_count.argtypes = [c_void_p, c_size_t]
    lib.squishdb_result_row_count.restype = c_size_t

    lib.squishdb_result_value_type.argtypes = [c_void_p, c_size_t, c_size_t, c_size_t]
    lib.squishdb_result_value_type.restype = c_int

    lib.squishdb_result_value_int.argtypes = [c_void_p, c_size_t, c_size_t, c_size_t]
    lib.squishdb_result_value_int.restype = c_int64

    lib.squishdb_result_value_real.argtypes = [c_void_p, c_size_t, c_size_t, c_size_t]
    lib.squishdb_result_value_real.restype = c_double

    lib.squishdb_result_value_text.argtypes = [
        c_void_p,
        c_size_t,
        c_size_t,
        c_size_t,
        POINTER(c_size_t),
    ]
    lib.squishdb_result_value_text.restype = c_void_p

    lib.squishdb_result_value_blob.argtypes = [
        c_void_p,
        c_size_t,
        c_size_t,
        c_size_t,
        POINTER(c_size_t),
    ]
    lib.squishdb_result_value_blob.restype = c_void_p

    lib.squishdb_batch_new.argtypes = [c_size_t]
    lib.squishdb_batch_new.restype = c_void_p

    lib.squishdb_batch_free.argtypes = [c_void_p]
    lib.squishdb_batch_free.restype = None

    lib.squishdb_batch_push_null.argtypes = [c_void_p]
    lib.squishdb_batch_push_null.restype = None

    lib.squishdb_batch_push_int.argtypes = [c_void_p, c_int64]
    lib.squishdb_batch_push_int.restype = None

    lib.squishdb_batch_push_real.argtypes = [c_void_p, c_double]
    lib.squishdb_batch_push_real.restype = None

    lib.squishdb_batch_push_text.argtypes = [c_void_p, POINTER(c_char), c_size_t]
    lib.squishdb_batch_push_text.restype = c_int

    lib.squishdb_batch_push_blob.argtypes = [c_void_p, POINTER(c_uint8), c_size_t]
    lib.squishdb_batch_push_blob.restype = c_int

    lib.squishdb_insert_batch.argtypes = [c_void_p, c_char_p, c_void_p]
    lib.squishdb_insert_batch.restype = c_int64

    lib.squishdb_bulk_insert.argtypes = [c_void_p, c_char_p, c_void_p]
    lib.squishdb_bulk_insert.restype = c_int64

    lib.squishdb_begin.argtypes = [c_void_p]
    lib.squishdb_begin.restype = c_int

    lib.squishdb_commit.argtypes = [c_void_p]
    lib.squishdb_commit.restype = c_int

    lib.squishdb_rollback.argtypes = [c_void_p]
    lib.squishdb_rollback.restype = c_int

    lib.squishdb_in_transaction.argtypes = [c_void_p]
    lib.squishdb_in_transaction.restype = c_int

    lib.squishdb_checkpoint.argtypes = [c_void_p]
    lib.squishdb_checkpoint.restype = c_int

    lib.squishdb_vacuum.argtypes = [c_void_p]
    lib.squishdb_vacuum.restype = c_int

    lib.squishdb_backup_to.argtypes = [c_void_p, c_char_p]
    lib.squishdb_backup_to.restype = c_int

    lib.squishdb_stats_report.argtypes = [c_void_p]
    lib.squishdb_stats_report.restype = c_void_p

    lib.squishdb_stats_json.argtypes = [c_void_p]
    lib.squishdb_stats_json.restype = c_void_p

    lib.squishdb_table_names_json.argtypes = [c_void_p]
    lib.squishdb_table_names_json.restype = c_void_p

    return lib


def load_library(lib_path: Optional[str] = None) -> ctypes.CDLL:
    global _LIB
    if _LIB is not None and lib_path is None:
        return _LIB

    if lib_path is not None:
        lib = _configure_signatures(ctypes.CDLL(str(lib_path)))
        if _LIB is None:
            _LIB = lib
        return lib

    for cand in _candidate_paths():
        if cand.is_file():
            _LIB = _configure_signatures(ctypes.CDLL(str(cand)))
            return _LIB

    # Fall back to system loader search path
    for name in _platform_lib_names():
        try:
            _LIB = _configure_signatures(ctypes.CDLL(name))
            return _LIB
        except OSError:
            pass

    raise OSError(
        "Could not locate SquishDB shared library (squishdb.dll / libsquishdb.so / libsquishdb.dylib). "
        "Build with `cargo build --release` or set SQUISHDB_LIB_PATH."
    )


def read_owned_cstr(lib: ctypes.CDLL, ptr: Optional[int]) -> str:
    if not ptr:
        return ""
    try:
        return ctypes.string_at(ptr).decode("utf-8")
    finally:
        lib.squishdb_string_free(ptr)


def last_error(lib: ctypes.CDLL) -> str:
    err = lib.squishdb_last_error()
    return err.decode("utf-8", errors="replace") if err else ""


_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


def _check_i64(value: int) -> int:
    if value < _I64_MIN or value > _I64_MAX:
        raise OverflowError(f"Integer {value} out of signed 64-bit range")
    return value


def push_value_to_params(lib: ctypes.CDLL, params_ptr: int, value: object) -> None:
    if value is None:
        lib.squishdb_params_push_null(params_ptr)
    elif isinstance(value, bool):
        lib.squishdb_params_push_int(params_ptr, 1 if value else 0)
    elif isinstance(value, int):
        lib.squishdb_params_push_int(params_ptr, _check_i64(value))
    elif isinstance(value, float):
        lib.squishdb_params_push_real(params_ptr, value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        buf = (c_char * len(raw)).from_buffer_copy(raw) if raw else None
        rc = lib.squishdb_params_push_text(params_ptr, buf, len(raw))
        if rc != SQUISHDB_OK:
            raise ValueError(last_error(lib))
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        buf = (c_uint8 * len(raw)).from_buffer_copy(raw) if raw else None
        rc = lib.squishdb_params_push_blob(params_ptr, buf, len(raw))
        if rc != SQUISHDB_OK:
            raise ValueError(last_error(lib))
    else:
        raise TypeError(f"Unsupported SQL parameter type: {type(value).__name__}")


def push_value_to_batch(lib: ctypes.CDLL, batch_ptr: int, value: object) -> None:
    if value is None:
        lib.squishdb_batch_push_null(batch_ptr)
    elif isinstance(value, bool):
        lib.squishdb_batch_push_int(batch_ptr, 1 if value else 0)
    elif isinstance(value, int):
        lib.squishdb_batch_push_int(batch_ptr, _check_i64(value))
    elif isinstance(value, float):
        lib.squishdb_batch_push_real(batch_ptr, value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        buf = (c_char * len(raw)).from_buffer_copy(raw) if raw else None
        rc = lib.squishdb_batch_push_text(batch_ptr, buf, len(raw))
        if rc != SQUISHDB_OK:
            raise ValueError(last_error(lib))
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        buf = (c_uint8 * len(raw)).from_buffer_copy(raw) if raw else None
        rc = lib.squishdb_batch_push_blob(batch_ptr, buf, len(raw))
        if rc != SQUISHDB_OK:
            raise ValueError(last_error(lib))
    else:
        raise TypeError(f"Unsupported SQL batch value type: {type(value).__name__}")


def extract_results(lib: ctypes.CDLL, res_ptr: int) -> list[dict]:
    out: list[dict] = []
    try:
        count = lib.squishdb_results_count(res_ptr)
        for s in range(count):
            kind = lib.squishdb_result_kind(res_ptr, s)
            if kind == RESULT_ROWS:
                ncols = lib.squishdb_result_column_count(res_ptr, s)
                nrows = lib.squishdb_result_row_count(res_ptr, s)
                cols = [
                    lib.squishdb_result_column_name(res_ptr, s, c).decode("utf-8")
                    for c in range(ncols)
                ]
                rows = []
                length = c_size_t(0)
                for r in range(nrows):
                    row = []
                    for c in range(ncols):
                        t = lib.squishdb_result_value_type(res_ptr, s, r, c)
                        if t == TYPE_NULL:
                            row.append(None)
                        elif t == TYPE_INTEGER:
                            row.append(int(lib.squishdb_result_value_int(res_ptr, s, r, c)))
                        elif t == TYPE_REAL:
                            row.append(float(lib.squishdb_result_value_real(res_ptr, s, r, c)))
                        elif t == TYPE_TEXT:
                            p = lib.squishdb_result_value_text(res_ptr, s, r, c, byref(length))
                            raw = ctypes.string_at(p, length.value) if length.value > 0 else b""
                            row.append(raw.decode("utf-8"))
                        elif t == TYPE_BLOB:
                            p = lib.squishdb_result_value_blob(res_ptr, s, r, c, byref(length))
                            row.append(
                                ctypes.string_at(p, length.value) if length.value > 0 else b""
                            )
                        else:
                            row.append(None)
                    rows.append(tuple(row))
                out.append({"kind": "rows", "columns": cols, "rows": rows})
            elif kind == RESULT_AFFECTED:
                out.append(
                    {"kind": "affected", "affected": int(lib.squishdb_result_affected(res_ptr, s))}
                )
            else:
                msg = lib.squishdb_result_message(res_ptr, s)
                out.append(
                    {
                        "kind": "message",
                        "message": msg.decode("utf-8") if msg else "",
                    }
                )
    finally:
        lib.squishdb_results_free(res_ptr)
    return out

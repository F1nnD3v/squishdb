package com.squishdb;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Embedded Java (and Kotlin/JVM) binding for SquishDB using the standard
 * Java 22+ Foreign Function & Memory (FFM) API — zero third-party dependencies.
 */
public final class SquishDb implements AutoCloseable {

    public enum CompressionProfile {
        FAST(0),
        BALANCED(1),
        MAX(2),
        NONE(3);

        final int code;
        CompressionProfile(int code) { this.code = code; }
    }

    public static final class SquishException extends RuntimeException {
        private final int code;

        public SquishException(String message) {
            this(message, -1);
        }

        public SquishException(String message, int code) {
            super(message);
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public record StatementResult(
            String kind,
            List<String> columns,
            List<List<Object>> rows,
            long affected,
            String message) {

        public List<Map<String, Object>> toMaps() {
            List<Map<String, Object>> out = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    map.put(columns.get(i), row.get(i));
                }
                out.add(map);
            }
            return out;
        }
    }

    // ========================================================================
    // Native FFM Loader & MethodHandles
    // ========================================================================

    private static final int SQUISHDB_OK = 0;
    private static final int RESULT_ROWS = 0;
    private static final int RESULT_AFFECTED = 1;
    private static final int TYPE_NULL = 0;
    private static final int TYPE_INTEGER = 1;
    private static final int TYPE_REAL = 2;
    private static final int TYPE_TEXT = 3;
    private static final int TYPE_BLOB = 4;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIB = loadNativeLibrary();

    private static final MethodHandle MH_VERSION = downcall("squishdb_version",
            FunctionDescriptor.of(ValueLayout.ADDRESS));
    private static final MethodHandle MH_LAST_ERROR = downcall("squishdb_last_error",
            FunctionDescriptor.of(ValueLayout.ADDRESS));
    private static final MethodHandle MH_STRING_FREE = downcall("squishdb_string_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

    private static final MethodHandle MH_OPEN = downcall("squishdb_open",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    private static final MethodHandle MH_OPEN_RO = downcall("squishdb_open_readonly",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    private static final MethodHandle MH_OPEN_OPTS = downcall("squishdb_open_with_options",
            FunctionDescriptor.of(
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_CLOSE = downcall("squishdb_close",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

    private static final MethodHandle MH_PARAMS_NEW = downcall("squishdb_params_new",
            FunctionDescriptor.of(ValueLayout.ADDRESS));
    private static final MethodHandle MH_PARAMS_FREE = downcall("squishdb_params_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    private static final MethodHandle MH_PARAMS_PUSH_NULL = downcall("squishdb_params_push_null",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    private static final MethodHandle MH_PARAMS_PUSH_INT = downcall("squishdb_params_push_int",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_PARAMS_PUSH_REAL = downcall("squishdb_params_push_real",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));
    private static final MethodHandle MH_PARAMS_PUSH_TEXT = downcall("squishdb_params_push_text",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_PARAMS_PUSH_BLOB = downcall("squishdb_params_push_blob",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

    private static final MethodHandle MH_EXECUTE = downcall("squishdb_execute",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    private static final MethodHandle MH_EXECUTE_PARAMS = downcall("squishdb_execute_params",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    private static final MethodHandle MH_RESULTS_FREE = downcall("squishdb_results_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    private static final MethodHandle MH_RESULTS_COUNT = downcall("squishdb_results_count",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    private static final MethodHandle MH_RESULT_KIND = downcall("squishdb_result_kind",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_AFFECTED = downcall("squishdb_result_affected",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_MESSAGE = downcall("squishdb_result_message",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_COL_COUNT = downcall("squishdb_result_column_count",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_COL_NAME = downcall("squishdb_result_column_name",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_ROW_COUNT = downcall("squishdb_result_row_count",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_VAL_TYPE = downcall("squishdb_result_value_type",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_VAL_INT = downcall("squishdb_result_value_int",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_VAL_REAL = downcall("squishdb_result_value_real",
            FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_RESULT_VAL_TEXT = downcall("squishdb_result_value_text",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    private static final MethodHandle MH_RESULT_VAL_BLOB = downcall("squishdb_result_value_blob",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

    private static final MethodHandle MH_BATCH_NEW = downcall("squishdb_batch_new",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_BATCH_FREE = downcall("squishdb_batch_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    private static final MethodHandle MH_BATCH_PUSH_NULL = downcall("squishdb_batch_push_null",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    private static final MethodHandle MH_BATCH_PUSH_INT = downcall("squishdb_batch_push_int",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_BATCH_PUSH_REAL = downcall("squishdb_batch_push_real",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));
    private static final MethodHandle MH_BATCH_PUSH_TEXT = downcall("squishdb_batch_push_text",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_BATCH_PUSH_BLOB = downcall("squishdb_batch_push_blob",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    private static final MethodHandle MH_INSERT_BATCH = downcall("squishdb_insert_batch",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    private static final MethodHandle MH_BULK_INSERT = downcall("squishdb_bulk_insert",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    private static final MethodHandle MH_BEGIN = downcall("squishdb_begin",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    private static final MethodHandle MH_COMMIT = downcall("squishdb_commit",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    private static final MethodHandle MH_ROLLBACK = downcall("squishdb_rollback",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    private static final MethodHandle MH_CHECKPOINT = downcall("squishdb_checkpoint",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    private static final MethodHandle MH_VACUUM = downcall("squishdb_vacuum",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    private static final MethodHandle MH_BACKUP_TO = downcall("squishdb_backup_to",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    private static final MethodHandle MH_STATS_JSON = downcall("squishdb_stats_json",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    private static SymbolLookup loadNativeLibrary() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String libName = os.contains("win") ? "squishdb.dll"
                : os.contains("mac") ? "libsquishdb.dylib" : "libsquishdb.so";

        List<Path> candidates = new ArrayList<>();
        String env = System.getenv("SQUISHDB_LIB_PATH");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env);
            candidates.add(Files.isDirectory(p) ? p.resolve(libName) : p);
        }
        String prop = System.getProperty("squishdb.lib.path");
        if (prop != null && !prop.isBlank()) {
            Path p = Path.of(prop);
            candidates.add(Files.isDirectory(p) ? p.resolve(libName) : p);
        }
        Path cwd = Path.of("").toAbsolutePath();
        candidates.add(cwd.resolve("target/release").resolve(libName));
        candidates.add(cwd.resolve("target/debug").resolve(libName));
        candidates.add(cwd.resolve("../../target/release").resolve(libName).normalize());
        candidates.add(cwd.resolve("../../target/debug").resolve(libName).normalize());

        for (Path c : candidates) {
            if (Files.isRegularFile(c)) {
                return SymbolLookup.libraryLookup(c, Arena.global());
            }
        }
        return SymbolLookup.libraryLookup(libName, Arena.global());
    }

    private static MethodHandle downcall(String symbol, FunctionDescriptor desc) {
        MemorySegment addr = LIB.find(symbol)
                .orElseThrow(() -> new UnsatisfiedLinkError("Missing SquishDB symbol: " + symbol));
        return LINKER.downcallHandle(addr, desc);
    }

    private static String readCStr(MemorySegment ptr) {
        if (ptr == null || ptr.equals(MemorySegment.NULL)) {
            return "";
        }
        return ptr.reinterpret(Long.MAX_VALUE).getString(0, StandardCharsets.UTF_8);
    }

    private static String lastError() {
        try {
            MemorySegment p = (MemorySegment) MH_LAST_ERROR.invokeExact();
            return readCStr(p);
        } catch (Throwable t) {
            return "Unknown native error";
        }
    }

    public static String version() {
        try {
            return readCStr((MemorySegment) MH_VERSION.invokeExact());
        } catch (Throwable t) {
            throw new SquishException("version() failed: " + t.getMessage());
        }
    }

    // ========================================================================
    // Instance State & Lifecycle
    // ========================================================================

    private MemorySegment handle;

    private SquishDb(MemorySegment handle) {
        this.handle = handle;
    }

    public static SquishDb open(String path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cpath = arena.allocateFrom(path, StandardCharsets.UTF_8);
            MemorySegment h = (MemorySegment) MH_OPEN.invokeExact(cpath);
            if (h.equals(MemorySegment.NULL)) {
                throw new SquishException(lastError());
            }
            return new SquishDb(h);
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("open failed: " + t.getMessage());
        }
    }

    public static SquishDb openReadOnly(String path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cpath = arena.allocateFrom(path, StandardCharsets.UTF_8);
            MemorySegment h = (MemorySegment) MH_OPEN_RO.invokeExact(cpath);
            if (h.equals(MemorySegment.NULL)) {
                throw new SquishException(lastError());
            }
            return new SquishDb(h);
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("openReadOnly failed: " + t.getMessage());
        }
    }

    public static SquishDb openWithOptions(String path, CompressionProfile profile, boolean syncWal) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cpath = arena.allocateFrom(path, StandardCharsets.UTF_8);
            MemorySegment h = (MemorySegment) MH_OPEN_OPTS.invokeExact(
                    cpath, profile.code, syncWal ? 1 : 0, 0L, 0L, 0L, 0L);
            if (h.equals(MemorySegment.NULL)) {
                throw new SquishException(lastError());
            }
            return new SquishDb(h);
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("openWithOptions failed: " + t.getMessage());
        }
    }

    private void ensureOpen() {
        if (handle == null || handle.equals(MemorySegment.NULL)) {
            throw new SquishException("Database is closed");
        }
    }

    public List<StatementResult> execute(String sql, Object... params) {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment csql = arena.allocateFrom(sql, StandardCharsets.UTF_8);
            MemorySegment resPtr;
            if (params == null || params.length == 0) {
                resPtr = (MemorySegment) MH_EXECUTE.invokeExact(handle, csql);
            } else {
                MemorySegment p = (MemorySegment) MH_PARAMS_NEW.invokeExact();
                try {
                    for (Object v : params) {
                        pushParam(arena, p, v);
                    }
                    resPtr = (MemorySegment) MH_EXECUTE_PARAMS.invokeExact(handle, csql, p);
                } finally {
                    MH_PARAMS_FREE.invokeExact(p);
                }
            }
            if (resPtr.equals(MemorySegment.NULL)) {
                throw new SquishException(lastError());
            }
            return extractResults(arena, resPtr);
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("execute failed: " + t.getMessage());
        }
    }

    public List<Map<String, Object>> query(String sql, Object... params) {
        List<StatementResult> results = execute(sql, params);
        for (int i = results.size() - 1; i >= 0; i--) {
            if ("rows".equals(results.get(i).kind())) {
                return results.get(i).toMaps();
            }
        }
        return List.of();
    }

    public long insertBatch(String table, List<List<Object>> rows) {
        return runBatch(table, rows, false);
    }

    public long bulkInsert(String table, List<List<Object>> rows) {
        return runBatch(table, rows, true);
    }

    private long runBatch(String table, List<List<Object>> rows, boolean bulk) {
        ensureOpen();
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        int ncols = rows.get(0).size();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batch = (MemorySegment) MH_BATCH_NEW.invokeExact((long) ncols);
            try {
                for (List<Object> row : rows) {
                    if (row.size() != ncols) {
                        throw new IllegalArgumentException(
                                "Inconsistent column count in batch: expected " + ncols + ", got " + row.size());
                    }
                    for (Object v : row) {
                        pushBatchValue(arena, batch, v);
                    }
                }
                MemorySegment ctable = arena.allocateFrom(table, StandardCharsets.UTF_8);
                long inserted = bulk
                        ? (long) MH_BULK_INSERT.invokeExact(handle, ctable, batch)
                        : (long) MH_INSERT_BATCH.invokeExact(handle, ctable, batch);
                if (inserted < 0) {
                    throw new SquishException(lastError());
                }
                return inserted;
            } finally {
                MH_BATCH_FREE.invokeExact(batch);
            }
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("batch insert failed: " + t.getMessage());
        }
    }

    public void begin() { runOp(MH_BEGIN, "begin"); }
    public void commit() { runOp(MH_COMMIT, "commit"); }
    public void rollback() { runOp(MH_ROLLBACK, "rollback"); }
    public void checkpoint() { runOp(MH_CHECKPOINT, "checkpoint"); }
    public void vacuum() { runOp(MH_VACUUM, "vacuum"); }

    public void backupTo(String destPath) {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cdest = arena.allocateFrom(destPath, StandardCharsets.UTF_8);
            int rc = (int) MH_BACKUP_TO.invokeExact(handle, cdest);
            if (rc != SQUISHDB_OK) {
                throw new SquishException(lastError(), rc);
            }
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("backupTo failed: " + t.getMessage());
        }
    }

    public String statsJson() {
        ensureOpen();
        try {
            MemorySegment ptr = (MemorySegment) MH_STATS_JSON.invokeExact(handle);
            if (ptr.equals(MemorySegment.NULL)) {
                throw new SquishException(lastError());
            }
            try {
                return readCStr(ptr);
            } finally {
                MH_STRING_FREE.invokeExact(ptr);
            }
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException("statsJson failed: " + t.getMessage());
        }
    }

    @Override
    public void close() {
        if (handle != null && !handle.equals(MemorySegment.NULL)) {
            try {
                MH_CLOSE.invokeExact(handle);
            } catch (Throwable ignored) {
            }
            handle = MemorySegment.NULL;
        }
    }

    private void runOp(MethodHandle mh, String name) {
        ensureOpen();
        try {
            int rc = (int) mh.invokeExact(handle);
            if (rc != SQUISHDB_OK) {
                throw new SquishException(lastError(), rc);
            }
        } catch (SquishException e) {
            throw e;
        } catch (Throwable t) {
            throw new SquishException(name + " failed: " + t.getMessage());
        }
    }

    private static void pushParam(Arena arena, MemorySegment p, Object v) throws Throwable {
        if (v == null) {
            MH_PARAMS_PUSH_NULL.invokeExact(p);
        } else if (v instanceof Boolean b) {
            MH_PARAMS_PUSH_INT.invokeExact(p, b ? 1L : 0L);
        } else if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            MH_PARAMS_PUSH_INT.invokeExact(p, ((Number) v).longValue());
        } else if (v instanceof Float || v instanceof Double) {
            MH_PARAMS_PUSH_REAL.invokeExact(p, ((Number) v).doubleValue());
        } else if (v instanceof String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            MemorySegment seg = bytes.length == 0
                    ? MemorySegment.NULL
                    : arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            int rc = (int) MH_PARAMS_PUSH_TEXT.invokeExact(p, seg, (long) bytes.length);
            if (rc != SQUISHDB_OK) throw new SquishException(lastError(), rc);
        } else if (v instanceof byte[] bytes) {
            MemorySegment seg = bytes.length == 0
                    ? MemorySegment.NULL
                    : arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            int rc = (int) MH_PARAMS_PUSH_BLOB.invokeExact(p, seg, (long) bytes.length);
            if (rc != SQUISHDB_OK) throw new SquishException(lastError(), rc);
        } else {
            throw new IllegalArgumentException("Unsupported SQL parameter type: " + v.getClass().getName());
        }
    }

    private static void pushBatchValue(Arena arena, MemorySegment b, Object v) throws Throwable {
        if (v == null) {
            MH_BATCH_PUSH_NULL.invokeExact(b);
        } else if (v instanceof Boolean boolVal) {
            MH_BATCH_PUSH_INT.invokeExact(b, boolVal ? 1L : 0L);
        } else if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            MH_BATCH_PUSH_INT.invokeExact(b, ((Number) v).longValue());
        } else if (v instanceof Float || v instanceof Double) {
            MH_BATCH_PUSH_REAL.invokeExact(b, ((Number) v).doubleValue());
        } else if (v instanceof String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            MemorySegment seg = bytes.length == 0
                    ? MemorySegment.NULL
                    : arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            int rc = (int) MH_BATCH_PUSH_TEXT.invokeExact(b, seg, (long) bytes.length);
            if (rc != SQUISHDB_OK) throw new SquishException(lastError(), rc);
        } else if (v instanceof byte[] bytes) {
            MemorySegment seg = bytes.length == 0
                    ? MemorySegment.NULL
                    : arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            int rc = (int) MH_BATCH_PUSH_BLOB.invokeExact(b, seg, (long) bytes.length);
            if (rc != SQUISHDB_OK) throw new SquishException(lastError(), rc);
        } else {
            throw new IllegalArgumentException("Unsupported batch value type: " + v.getClass().getName());
        }
    }

    private static List<StatementResult> extractResults(Arena arena, MemorySegment resPtr) throws Throwable {
        try {
            long count = (long) MH_RESULTS_COUNT.invokeExact(resPtr);
            List<StatementResult> out = new ArrayList<>((int) count);
            MemorySegment lenOut = arena.allocate(ValueLayout.JAVA_LONG);

            for (long s = 0; s < count; s++) {
                int kind = (int) MH_RESULT_KIND.invokeExact(resPtr, s);
                if (kind == RESULT_ROWS) {
                    long ncols = (long) MH_RESULT_COL_COUNT.invokeExact(resPtr, s);
                    long nrows = (long) MH_RESULT_ROW_COUNT.invokeExact(resPtr, s);
                    List<String> cols = new ArrayList<>((int) ncols);
                    for (long c = 0; c < ncols; c++) {
                        cols.add(readCStr((MemorySegment) MH_RESULT_COL_NAME.invokeExact(resPtr, s, c)));
                    }
                    List<List<Object>> rows = new ArrayList<>((int) nrows);
                    for (long r = 0; r < nrows; r++) {
                        List<Object> row = new ArrayList<>((int) ncols);
                        for (long c = 0; c < ncols; c++) {
                            int t = (int) MH_RESULT_VAL_TYPE.invokeExact(resPtr, s, r, c);
                            switch (t) {
                                case TYPE_NULL -> row.add(null);
                                case TYPE_INTEGER -> row.add((long) MH_RESULT_VAL_INT.invokeExact(resPtr, s, r, c));
                                case TYPE_REAL -> row.add((double) MH_RESULT_VAL_REAL.invokeExact(resPtr, s, r, c));
                                case TYPE_TEXT -> {
                                    MemorySegment tp = (MemorySegment) MH_RESULT_VAL_TEXT.invokeExact(resPtr, s, r, c, lenOut);
                                    long len = lenOut.get(ValueLayout.JAVA_LONG, 0);
                                    if (len == 0) {
                                        row.add("");
                                    } else {
                                        byte[] bytes = tp.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
                                        row.add(new String(bytes, StandardCharsets.UTF_8));
                                    }
                                }
                                case TYPE_BLOB -> {
                                    MemorySegment bp = (MemorySegment) MH_RESULT_VAL_BLOB.invokeExact(resPtr, s, r, c, lenOut);
                                    long len = lenOut.get(ValueLayout.JAVA_LONG, 0);
                                    if (len == 0) {
                                        row.add(new byte[0]);
                                    } else {
                                        row.add(bp.reinterpret(len).toArray(ValueLayout.JAVA_BYTE));
                                    }
                                }
                                default -> row.add(null);
                            }
                        }
                        rows.add(row);
                    }
                    out.add(new StatementResult("rows", cols, rows, 0, ""));
                } else if (kind == RESULT_AFFECTED) {
                    long aff = (long) MH_RESULT_AFFECTED.invokeExact(resPtr, s);
                    out.add(new StatementResult("affected", List.of(), List.of(), aff, ""));
                } else {
                    String msg = readCStr((MemorySegment) MH_RESULT_MESSAGE.invokeExact(resPtr, s));
                    out.add(new StatementResult("message", List.of(), List.of(), 0, msg));
                }
            }
            return out;
        } finally {
            MH_RESULTS_FREE.invokeExact(resPtr);
        }
    }
}

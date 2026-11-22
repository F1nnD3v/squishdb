using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace SquishDb;

public enum CompressionProfile
{
    Fast = 0,
    Balanced = 1,
    Max = 2,
    None = 3,
}

public sealed class SquishException : Exception
{
    public int Code { get; }

    public SquishException(string message, int code = -1) : base(message)
    {
        Code = code;
    }
}

public sealed class StatementResult
{
    public string Kind { get; init; } = "";
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<IReadOnlyList<object?>> Rows { get; init; } = Array.Empty<IReadOnlyList<object?>>();
    public ulong Affected { get; init; }
    public string Message { get; init; } = "";

    public List<Dictionary<string, object?>> ToDictionaries()
    {
        var list = new List<Dictionary<string, object?>>(Rows.Count);
        foreach (var row in Rows)
        {
            var dict = new Dictionary<string, object?>(Columns.Count);
            for (int i = 0; i < Columns.Count; i++)
            {
                dict[Columns[i]] = row[i];
            }
            list.Add(dict);
        }
        return list;
    }
}

internal static class NativeMethods
{
    private const string LibName = "squishdb";

    internal const int Ok = 0;
    internal const int ResultRows = 0;
    internal const int ResultAffected = 1;
    internal const int TypeNull = 0;
    internal const int TypeInteger = 1;
    internal const int TypeReal = 2;
    internal const int TypeText = 3;
    internal const int TypeBlob = 4;

    static NativeMethods()
    {
        NativeLibrary.SetDllImportResolver(typeof(NativeMethods).Assembly, ResolveLibrary);
    }

    private static IntPtr ResolveLibrary(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibName)
            return IntPtr.Zero;

        string fileName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "squishdb.dll"
            : RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? "libsquishdb.dylib"
                : "libsquishdb.so";

        var candidates = new List<string>();
        string? envPath = Environment.GetEnvironmentVariable("SQUISHDB_LIB_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
        {
            candidates.Add(Directory.Exists(envPath) ? Path.Combine(envPath, fileName) : envPath);
        }

        string baseDir = AppContext.BaseDirectory;
        candidates.Add(Path.Combine(baseDir, fileName));

        // Walk upward to locate repo target/release or target/debug
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
        for (int i = 0; i < 6 && dir != null; i++, dir = dir.Parent)
        {
            candidates.Add(Path.Combine(dir.FullName, "target", "release", fileName));
            candidates.Add(Path.Combine(dir.FullName, "target", "debug", fileName));
        }

        foreach (var cand in candidates)
        {
            if (File.Exists(cand) && NativeLibrary.TryLoad(cand, out IntPtr handle))
                return handle;
        }

        return NativeLibrary.TryLoad(fileName, assembly, searchPath, out IntPtr sys) ? sys : IntPtr.Zero;
    }

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_version();

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_last_error();

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_string_free(IntPtr ptr);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_open([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_open_readonly([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_open_with_options(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int compressionProfile,
        int syncWal,
        nuint pageRows,
        nuint autoCheckpointRows,
        nuint cachePages,
        nuint indexCacheBlocks);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_close(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_params_new();

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_params_free(IntPtr p);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_params_push_null(IntPtr p);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_params_push_int(IntPtr p, long value);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_params_push_real(IntPtr p, double value);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_params_push_text(IntPtr p, byte[]? utf8, nuint len);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_params_push_blob(IntPtr p, byte[]? data, nuint len);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_execute(IntPtr db, [MarshalAs(UnmanagedType.LPUTF8Str)] string sql);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_execute_params(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        IntPtr p);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_results_free(IntPtr res);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern nuint squishdb_results_count(IntPtr res);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_result_kind(IntPtr res, nuint stmtIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern ulong squishdb_result_affected(IntPtr res, nuint stmtIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_result_message(IntPtr res, nuint stmtIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern nuint squishdb_result_column_count(IntPtr res, nuint stmtIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_result_column_name(IntPtr res, nuint stmtIdx, nuint colIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern nuint squishdb_result_row_count(IntPtr res, nuint stmtIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_result_value_type(IntPtr res, nuint stmtIdx, nuint rowIdx, nuint colIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long squishdb_result_value_int(IntPtr res, nuint stmtIdx, nuint rowIdx, nuint colIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern double squishdb_result_value_real(IntPtr res, nuint stmtIdx, nuint rowIdx, nuint colIdx);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_result_value_text(
        IntPtr res, nuint stmtIdx, nuint rowIdx, nuint colIdx, out nuint outLen);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_result_value_blob(
        IntPtr res, nuint stmtIdx, nuint rowIdx, nuint colIdx, out nuint outLen);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_batch_new(nuint numCols);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_batch_free(IntPtr batch);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_batch_push_null(IntPtr batch);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_batch_push_int(IntPtr batch, long value);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void squishdb_batch_push_real(IntPtr batch, double value);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_batch_push_text(IntPtr batch, byte[]? utf8, nuint len);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_batch_push_blob(IntPtr batch, byte[]? data, nuint len);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long squishdb_insert_batch(
        IntPtr db, [MarshalAs(UnmanagedType.LPUTF8Str)] string table, IntPtr batch);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long squishdb_bulk_insert(
        IntPtr db, [MarshalAs(UnmanagedType.LPUTF8Str)] string table, IntPtr batch);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_begin(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_commit(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_rollback(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_checkpoint(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_vacuum(IntPtr db);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int squishdb_backup_to(IntPtr db, [MarshalAs(UnmanagedType.LPUTF8Str)] string dest);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr squishdb_stats_json(IntPtr db);
}

public sealed class SquishDatabase : IDisposable
{
    private IntPtr _handle;

    private SquishDatabase(IntPtr handle) => _handle = handle;

    public static string Version =>
        Marshal.PtrToStringUTF8(NativeMethods.squishdb_version()) ?? "";

    private static string LastError =>
        Marshal.PtrToStringUTF8(NativeMethods.squishdb_last_error()) ?? "Unknown SquishDB error";

    public static SquishDatabase Open(string path)
    {
        IntPtr h = NativeMethods.squishdb_open(path);
        if (h == IntPtr.Zero) throw new SquishException(LastError);
        return new SquishDatabase(h);
    }

    public static SquishDatabase OpenReadOnly(string path)
    {
        IntPtr h = NativeMethods.squishdb_open_readonly(path);
        if (h == IntPtr.Zero) throw new SquishException(LastError);
        return new SquishDatabase(h);
    }

    public static SquishDatabase OpenWithOptions(
        string path,
        CompressionProfile profile = CompressionProfile.Balanced,
        bool syncWal = false)
    {
        IntPtr h = NativeMethods.squishdb_open_with_options(
            path, (int)profile, syncWal ? 1 : 0, 0, 0, 0, 0);
        if (h == IntPtr.Zero) throw new SquishException(LastError);
        return new SquishDatabase(h);
    }

    private void EnsureOpen()
    {
        if (_handle == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(SquishDatabase));
    }

    public List<StatementResult> Execute(string sql, params object?[] parameters)
    {
        EnsureOpen();
        IntPtr resPtr;
        if (parameters.Length == 0)
        {
            resPtr = NativeMethods.squishdb_execute(_handle, sql);
        }
        else
        {
            IntPtr p = NativeMethods.squishdb_params_new();
            try
            {
                foreach (var val in parameters)
                {
                    PushParam(p, val);
                }
                resPtr = NativeMethods.squishdb_execute_params(_handle, sql, p);
            }
            finally
            {
                NativeMethods.squishdb_params_free(p);
            }
        }

        if (resPtr == IntPtr.Zero)
            throw new SquishException(LastError);

        return ExtractResults(resPtr);
    }

    public List<Dictionary<string, object?>> Query(string sql, params object?[] parameters)
    {
        var results = Execute(sql, parameters);
        for (int i = results.Count - 1; i >= 0; i--)
        {
            if (results[i].Kind == "rows")
                return results[i].ToDictionaries();
        }
        return new List<Dictionary<string, object?>>();
    }

    public long InsertBatch(string table, IReadOnlyList<IReadOnlyList<object?>> rows) =>
        RunBatch(table, rows, bulk: false);

    public long BulkInsert(string table, IReadOnlyList<IReadOnlyList<object?>> rows) =>
        RunBatch(table, rows, bulk: true);

    private long RunBatch(string table, IReadOnlyList<IReadOnlyList<object?>> rows, bool bulk)
    {
        EnsureOpen();
        if (rows.Count == 0) return 0;
        int ncols = rows[0].Count;
        IntPtr batch = NativeMethods.squishdb_batch_new((nuint)ncols);
        try
        {
            foreach (var row in rows)
            {
                if (row.Count != ncols)
                    throw new ArgumentException($"Inconsistent column count in batch: expected {ncols}, got {row.Count}");
                foreach (var val in row)
                {
                    PushBatchValue(batch, val);
                }
            }
            long n = bulk
                ? NativeMethods.squishdb_bulk_insert(_handle, table, batch)
                : NativeMethods.squishdb_insert_batch(_handle, table, batch);
            if (n < 0) throw new SquishException(LastError);
            return n;
        }
        finally
        {
            NativeMethods.squishdb_batch_free(batch);
        }
    }

    public void Begin() { EnsureOpen(); Check(NativeMethods.squishdb_begin(_handle)); }
    public void Commit() { EnsureOpen(); Check(NativeMethods.squishdb_commit(_handle)); }
    public void Rollback() { EnsureOpen(); Check(NativeMethods.squishdb_rollback(_handle)); }
    public void Checkpoint() { EnsureOpen(); Check(NativeMethods.squishdb_checkpoint(_handle)); }
    public void Vacuum() { EnsureOpen(); Check(NativeMethods.squishdb_vacuum(_handle)); }
    public void BackupTo(string destPath) { EnsureOpen(); Check(NativeMethods.squishdb_backup_to(_handle, destPath)); }

    public string StatsJson()
    {
        EnsureOpen();
        IntPtr ptr = NativeMethods.squishdb_stats_json(_handle);
        if (ptr == IntPtr.Zero) throw new SquishException(LastError);
        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? "{}";
        }
        finally
        {
            NativeMethods.squishdb_string_free(ptr);
        }
    }

    public void Dispose()
    {
        if (_handle != IntPtr.Zero)
        {
            NativeMethods.squishdb_close(_handle);
            _handle = IntPtr.Zero;
        }
        GC.SuppressFinalize(this);
    }

    ~SquishDatabase() => Dispose();

    private void Check(int rc)
    {
        EnsureOpen();
        if (rc != NativeMethods.Ok) throw new SquishException(LastError, rc);
    }

    private static void PushParam(IntPtr p, object? val)
    {
        switch (val)
        {
            case null:
                NativeMethods.squishdb_params_push_null(p);
                break;
            case bool b:
                NativeMethods.squishdb_params_push_int(p, b ? 1 : 0);
                break;
            case sbyte or byte or short or ushort or int or uint or long:
                NativeMethods.squishdb_params_push_int(p, Convert.ToInt64(val));
                break;
            case float or double or decimal:
                NativeMethods.squishdb_params_push_real(p, Convert.ToDouble(val));
                break;
            case string s:
                byte[] utf8 = Encoding.UTF8.GetBytes(s);
                int trc = NativeMethods.squishdb_params_push_text(p, utf8.Length == 0 ? null : utf8, (nuint)utf8.Length);
                if (trc != NativeMethods.Ok) throw new SquishException(LastError, trc);
                break;
            case byte[] bytes:
                int brc = NativeMethods.squishdb_params_push_blob(p, bytes.Length == 0 ? null : bytes, (nuint)bytes.Length);
                if (brc != NativeMethods.Ok) throw new SquishException(LastError, brc);
                break;
            default:
                throw new ArgumentException($"Unsupported SQL parameter type: {val.GetType().Name}");
        }
    }

    private static void PushBatchValue(IntPtr b, object? val)
    {
        switch (val)
        {
            case null:
                NativeMethods.squishdb_batch_push_null(b);
                break;
            case bool flag:
                NativeMethods.squishdb_batch_push_int(b, flag ? 1 : 0);
                break;
            case sbyte or byte or short or ushort or int or uint or long:
                NativeMethods.squishdb_batch_push_int(b, Convert.ToInt64(val));
                break;
            case float or double or decimal:
                NativeMethods.squishdb_batch_push_real(b, Convert.ToDouble(val));
                break;
            case string s:
                byte[] utf8 = Encoding.UTF8.GetBytes(s);
                int trc = NativeMethods.squishdb_batch_push_text(b, utf8.Length == 0 ? null : utf8, (nuint)utf8.Length);
                if (trc != NativeMethods.Ok) throw new SquishException(LastError, trc);
                break;
            case byte[] bytes:
                int brc = NativeMethods.squishdb_batch_push_blob(b, bytes.Length == 0 ? null : bytes, (nuint)bytes.Length);
                if (brc != NativeMethods.Ok) throw new SquishException(LastError, brc);
                break;
            default:
                throw new ArgumentException($"Unsupported batch value type: {val.GetType().Name}");
        }
    }

    private static List<StatementResult> ExtractResults(IntPtr resPtr)
    {
        try
        {
            nuint count = NativeMethods.squishdb_results_count(resPtr);
            var list = new List<StatementResult>((int)count);
            for (nuint s = 0; s < count; s++)
            {
                int kind = NativeMethods.squishdb_result_kind(resPtr, s);
                if (kind == NativeMethods.ResultRows)
                {
                    nuint ncols = NativeMethods.squishdb_result_column_count(resPtr, s);
                    nuint nrows = NativeMethods.squishdb_result_row_count(resPtr, s);
                    var cols = new string[(int)ncols];
                    for (nuint c = 0; c < ncols; c++)
                    {
                        cols[(int)c] = Marshal.PtrToStringUTF8(
                            NativeMethods.squishdb_result_column_name(resPtr, s, c)) ?? "";
                    }
                    var rows = new List<IReadOnlyList<object?>>((int)nrows);
                    for (nuint r = 0; r < nrows; r++)
                    {
                        var row = new object?[(int)ncols];
                        for (nuint c = 0; c < ncols; c++)
                        {
                            int t = NativeMethods.squishdb_result_value_type(resPtr, s, r, c);
                            row[(int)c] = t switch
                            {
                                NativeMethods.TypeNull => null,
                                NativeMethods.TypeInteger => NativeMethods.squishdb_result_value_int(resPtr, s, r, c),
                                NativeMethods.TypeReal => NativeMethods.squishdb_result_value_real(resPtr, s, r, c),
                                NativeMethods.TypeText => ReadText(resPtr, s, r, c),
                                NativeMethods.TypeBlob => ReadBlob(resPtr, s, r, c),
                                _ => null,
                            };
                        }
                        rows.Add(row);
                    }
                    list.Add(new StatementResult { Kind = "rows", Columns = cols, Rows = rows });
                }
                else if (kind == NativeMethods.ResultAffected)
                {
                    list.Add(new StatementResult
                    {
                        Kind = "affected",
                        Affected = NativeMethods.squishdb_result_affected(resPtr, s),
                    });
                }
                else
                {
                    list.Add(new StatementResult
                    {
                        Kind = "message",
                        Message = Marshal.PtrToStringUTF8(
                            NativeMethods.squishdb_result_message(resPtr, s)) ?? "",
                    });
                }
            }
            return list;
        }
        finally
        {
            NativeMethods.squishdb_results_free(resPtr);
        }
    }

    private static string ReadText(IntPtr resPtr, nuint s, nuint r, nuint c)
    {
        IntPtr ptr = NativeMethods.squishdb_result_value_text(resPtr, s, r, c, out nuint len);
        if (len == 0) return "";
        byte[] buf = new byte[(int)len];
        Marshal.Copy(ptr, buf, 0, (int)len);
        return Encoding.UTF8.GetString(buf);
    }

    private static byte[] ReadBlob(IntPtr resPtr, nuint s, nuint r, nuint c)
    {
        IntPtr ptr = NativeMethods.squishdb_result_value_blob(resPtr, s, r, c, out nuint len);
        if (len == 0) return Array.Empty<byte>();
        byte[] buf = new byte[(int)len];
        Marshal.Copy(ptr, buf, 0, (int)len);
        return buf;
    }
}

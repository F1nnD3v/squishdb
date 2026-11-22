using SquishDb;

static void AssertThrows<TException>(Action action) where TException : Exception
{
    try
    {
        action();
    }
    catch (TException)
    {
        return;
    }
    throw new Exception($"Expected {typeof(TException).Name} to be thrown");
}

string dir = Path.Combine(Path.GetTempPath(), $"squishdb-csharp-{Guid.NewGuid():N}");
Directory.CreateDirectory(dir);
string dbPath = Path.Combine(dir, "test.sqdb");
string backupPath = Path.Combine(dir, "backup.sqdb");

try
{
    Console.WriteLine($"SquishDB C#/.NET test (version {SquishDatabase.Version})");

    SquishDatabase disposedRef;
    using (var db = SquishDatabase.OpenWithOptions(dbPath, CompressionProfile.Balanced, syncWal: false))
    {
        disposedRef = db;
        db.Execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                total REAL,
                receipt BLOB
            ) CLUSTER BY (customer);
            """);

        byte[] receipt = [0xDE, 0xAD, 0xBE, 0xEF];
        db.Execute("INSERT INTO orders VALUES (?, ?, ?, ?);", 1L, "alice", 29.50, receipt);

        long inserted = db.BulkInsert("orders", [
            [2L, "alice", 15.25, null],
            [3L, "bob", 120.00, new byte[] { 1, 2, 3 }],
        ]);
        if (inserted != 2) throw new Exception($"Expected 2 inserted, got {inserted}");

        long batched = db.InsertBatch("orders", [
            [4L, "日本語 🚀", 5.00, Array.Empty<byte>()],
        ]);
        if (batched != 1) throw new Exception($"Expected 1 batched, got {batched}");

        // Transaction rollback & commit
        db.Begin();
        db.Execute("INSERT INTO orders VALUES (?, ?, ?, ?);", 99L, "rollback", 0.0, null);
        db.Rollback();

        db.Begin();
        db.Execute("INSERT INTO orders VALUES (?, ?, ?, ?);", 5L, "carol", 10.0, null);
        db.Commit();

        // Validation & error paths
        AssertThrows<ArgumentException>(() => db.InsertBatch("orders", [
            [10L, "a", 1.0, null],
            [11L, "b"],
        ]));
        AssertThrows<SquishException>(() => db.Execute("SELECT * FROM nonexistent_table;"));

        var agg = db.Query("SELECT customer, COUNT(*) AS cnt, SUM(total) AS spent FROM orders GROUP BY customer ORDER BY spent DESC;");
        if (agg.Count != 4) throw new Exception($"Expected 4 rows, got {agg.Count}");
        if ((string?)agg[0]["customer"] != "bob") throw new Exception("Expected bob first");
        if ((long?)agg[1]["cnt"] != 2) throw new Exception("Expected 2 orders for alice");

        var row = db.Query("SELECT receipt FROM orders WHERE id = ?;", 1L);
        byte[]? got = (byte[]?)row[0]["receipt"];
        if (got == null || !got.SequenceEqual(receipt)) throw new Exception("Blob mismatch");

        var uni = db.Query("SELECT customer, receipt FROM orders WHERE id = ?;", 4L);
        if ((string?)uni[0]["customer"] != "日本語 🚀") throw new Exception("Unicode mismatch");
        if (((byte[]?)uni[0]["receipt"])?.Length != 0) throw new Exception("Empty blob mismatch");

        string stats = db.StatsJson();
        if (!stats.Contains("\"orders\"")) throw new Exception("Missing orders in stats");

        db.Vacuum();
        db.Checkpoint();
        db.BackupTo(backupPath);
        if (!File.Exists(backupPath)) throw new Exception("Backup missing");
    }

    // Disposed database must throw ObjectDisposedException before native calls
    AssertThrows<ObjectDisposedException>(() => disposedRef.Begin());
    AssertThrows<ObjectDisposedException>(() => disposedRef.Execute("SELECT 1;"));

    using (var ro = SquishDatabase.OpenReadOnly(backupPath))
    {
        var count = ro.Query("SELECT COUNT(*) AS c FROM orders;");
        if ((long?)count[0]["c"] != 5) throw new Exception("Wrong RO count");
        AssertThrows<SquishException>(() => ro.Execute("INSERT INTO orders VALUES (50, 'ro', 0.0, NULL);"));
    }

    Console.WriteLine("C# / .NET test OK!");
}
finally
{
    Directory.Delete(dir, recursive: true);
}

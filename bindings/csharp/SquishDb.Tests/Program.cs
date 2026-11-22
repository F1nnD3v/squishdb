using SquishDb;

string dir = Path.Combine(Path.GetTempPath(), $"squishdb-csharp-{Guid.NewGuid():N}");
Directory.CreateDirectory(dir);
string dbPath = Path.Combine(dir, "test.sqdb");

try
{
    Console.WriteLine($"SquishDB C#/.NET test (version {SquishDatabase.Version})");

    using (var db = SquishDatabase.OpenWithOptions(dbPath, CompressionProfile.Balanced, syncWal: false))
    {
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

        var agg = db.Query("SELECT customer, COUNT(*) AS cnt, SUM(total) AS spent FROM orders GROUP BY customer ORDER BY spent DESC;");
        if (agg.Count != 2) throw new Exception("Expected 2 rows");
        if ((string?)agg[0]["customer"] != "bob") throw new Exception("Expected bob first");
        if ((long?)agg[1]["cnt"] != 2) throw new Exception("Expected 2 orders for alice");

        var row = db.Query("SELECT receipt FROM orders WHERE id = ?;", 1L);
        byte[]? got = (byte[]?)row[0]["receipt"];
        if (got == null || !got.SequenceEqual(receipt)) throw new Exception("Blob mismatch");

        string stats = db.StatsJson();
        if (!stats.Contains("\"orders\"")) throw new Exception("Missing orders in stats");

        db.Checkpoint();
    }

    using (var ro = SquishDatabase.OpenReadOnly(dbPath))
    {
        var count = ro.Query("SELECT COUNT(*) AS c FROM orders;");
        if ((long?)count[0]["c"] != 3) throw new Exception("Wrong RO count");
    }

    Console.WriteLine("C# / .NET test OK!");
}
finally
{
    Directory.Delete(dir, recursive: true);
}

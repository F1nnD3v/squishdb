import com.squishdb.SquishDb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SquishDbTest {
    private static void assertThrows(Class<? extends Throwable> expected, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            if (expected.isInstance(t)) {
                return;
            }
            throw new AssertionError("Expected " + expected.getName() + " but got " + t, t);
        }
        throw new AssertionError("Expected " + expected.getName() + " to be thrown");
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("squishdb-java-");
        Path dbPath = dir.resolve("test.sqdb");
        Path backupPath = dir.resolve("backup.sqdb");
        try {
            System.out.println("SquishDB Java FFM test (version " + SquishDb.version() + ")");

            SquishDb closedRef;
            try (SquishDb db = SquishDb.openWithOptions(
                    dbPath.toString(), SquishDb.CompressionProfile.BALANCED, false)) {
                closedRef = db;
                db.execute("""
                        CREATE TABLE metrics (
                            id INTEGER PRIMARY KEY,
                            host TEXT NOT NULL,
                            cpu REAL,
                            raw BLOB
                        ) CLUSTER BY (host);
                        """);

                byte[] payload = new byte[] { 0x10, 0x20, (byte) 0xFF };
                db.execute("INSERT INTO metrics VALUES (?, ?, ?, ?);", 1L, "web-01", 0.42, payload);

                long inserted = db.bulkInsert("metrics", List.of(
                        Arrays.asList(2L, "web-01", 0.58, null),
                        Arrays.asList(3L, "web-02", 0.15, new byte[] { 1, 2 })
                ));
                if (inserted != 2) throw new AssertionError("Expected 2 bulk rows, got " + inserted);

                // WAL insertBatch + Unicode + empty blob
                long batched = db.insertBatch("metrics", List.of(
                        Arrays.asList(4L, "東京-node 🚀", 0.99, new byte[0])
                ));
                if (batched != 1) throw new AssertionError("Expected 1 batch row");

                // Transaction rollback & commit
                db.begin();
                db.execute("INSERT INTO metrics VALUES (?, ?, ?, ?);", 99L, "rolled", 0.0, null);
                db.rollback();

                db.begin();
                db.execute("INSERT INTO metrics VALUES (?, ?, ?, ?);", 5L, "web-02", 0.25, null);
                db.commit();

                // Validation & error paths
                assertThrows(IllegalArgumentException.class, () -> db.insertBatch("metrics", List.of(
                        Arrays.asList(10L, "a", 1.0, null),
                        Arrays.asList(11L, "b")
                )));
                assertThrows(SquishDb.SquishException.class, () ->
                        db.execute("SELECT * FROM missing_table;"));

                List<Map<String, Object>> agg = db.query(
                        "SELECT host, COUNT(*) AS n, AVG(cpu) AS avg_cpu FROM metrics GROUP BY host ORDER BY host;");
                if (agg.size() != 3) throw new AssertionError("Expected 3 groups, got " + agg.size());
                if (!"web-01".equals(agg.get(0).get("host"))) throw new AssertionError("Wrong host");
                if ((Long) agg.get(0).get("n") != 2L) throw new AssertionError("Wrong count");

                List<Map<String, Object>> one = db.query("SELECT raw FROM metrics WHERE id = ?;", 1L);
                byte[] got = (byte[]) one.get(0).get("raw");
                if (!Arrays.equals(payload, got)) throw new AssertionError("Blob mismatch");

                List<Map<String, Object>> uni = db.query("SELECT host, raw FROM metrics WHERE id = ?;", 4L);
                if (!"東京-node 🚀".equals(uni.get(0).get("host"))) throw new AssertionError("Unicode mismatch");
                if (((byte[]) uni.get(0).get("raw")).length != 0) throw new AssertionError("Empty blob mismatch");

                String stats = db.statsJson();
                if (!stats.contains("\"metrics\"")) throw new AssertionError("Missing table in stats");

                db.vacuum();
                db.checkpoint();
                db.backupTo(backupPath.toString());
                if (!Files.isRegularFile(backupPath)) throw new AssertionError("Backup missing");
            }

            // Closed handle must throw
            assertThrows(SquishDb.SquishException.class, () -> closedRef.execute("SELECT 1;"));

            try (SquishDb ro = SquishDb.openReadOnly(backupPath.toString())) {
                List<Map<String, Object>> c = ro.query("SELECT COUNT(*) AS c FROM metrics;");
                if ((Long) c.get(0).get("c") != 5L) throw new AssertionError("Wrong RO count");
                assertThrows(SquishDb.SquishException.class, () ->
                        ro.execute("INSERT INTO metrics VALUES (50, 'ro', 0.0, NULL);"));
            }

            System.out.println("Java FFM test OK!");
        } finally {
            try (var stream = Files.list(dir)) {
                for (Path p : stream.toList()) {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(dir);
        }
    }
}

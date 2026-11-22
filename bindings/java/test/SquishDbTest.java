import com.squishdb.SquishDb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SquishDbTest {
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("squishdb-java-");
        Path dbPath = dir.resolve("test.sqdb");
        try {
            System.out.println("SquishDB Java FFM test (version " + SquishDb.version() + ")");

            try (SquishDb db = SquishDb.openWithOptions(
                    dbPath.toString(), SquishDb.CompressionProfile.BALANCED, false)) {
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

                List<Map<String, Object>> agg = db.query(
                        "SELECT host, COUNT(*) AS n, AVG(cpu) AS avg_cpu FROM metrics GROUP BY host ORDER BY host;");
                if (agg.size() != 2) throw new AssertionError("Expected 2 groups");
                if (!"web-01".equals(agg.get(0).get("host"))) throw new AssertionError("Wrong host");
                if ((Long) agg.get(0).get("n") != 2L) throw new AssertionError("Wrong count");

                List<Map<String, Object>> one = db.query("SELECT raw FROM metrics WHERE id = ?;", 1L);
                byte[] got = (byte[]) one.get(0).get("raw");
                if (!Arrays.equals(payload, got)) throw new AssertionError("Blob mismatch");

                String stats = db.statsJson();
                if (!stats.contains("\"metrics\"")) throw new AssertionError("Missing table in stats");

                db.checkpoint();
            }

            try (SquishDb ro = SquishDb.openReadOnly(dbPath.toString())) {
                List<Map<String, Object>> c = ro.query("SELECT COUNT(*) AS c FROM metrics;");
                if ((Long) c.get(0).get("c") != 3L) throw new AssertionError("Wrong RO count");
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

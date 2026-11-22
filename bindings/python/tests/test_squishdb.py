from pathlib import Path
import tempfile
import unittest

import squishdb


class TestSquishDBPython(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.sqdb"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_dbapi_and_bulk_roundtrip(self) -> None:
        self.assertTrue(len(squishdb.version()) > 0)

        with squishdb.connect(self.db_path, compression="balanced") as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE readings (
                    id INTEGER PRIMARY KEY,
                    device TEXT NOT NULL,
                    temp REAL,
                    payload BLOB
                ) CLUSTER BY (device);
                """
            )

            evil = "dev-01'; DROP TABLE readings; --"
            cur.execute(
                "INSERT INTO readings VALUES (?, ?, ?, ?);",
                (1, evil, 21.5, b"\x00\x01\x02\xff"),
            )
            self.assertEqual(cur.rowcount, 1)

            cur.executemany(
                "INSERT INTO readings VALUES (?, ?, ?, ?);",
                [
                    (2, "dev-01", 22.0, None),
                    (3, "dev-02", 19.25, b"ok"),
                ],
            )

            # High-throughput PAX bulk insert
            inserted = conn.bulk_insert(
                "readings",
                [(i, f"dev-0{i % 2 + 1}", 20.0 + i * 0.1, None) for i in range(4, 104)],
            )
            self.assertEqual(inserted, 100)

            # WAL-backed insert_batch
            batch_n = conn.insert_batch(
                "readings",
                [(200, "dev-batch", 25.0, b""), (201, "日本語 🚀", -12.5, b"\xab\xcd")],
            )
            self.assertEqual(batch_n, 2)

            # Transaction + rollback test
            conn.begin()
            conn.execute("INSERT INTO readings VALUES (?, ?, ?, ?);", (999, "temp", 0.0, None))
            self.assertTrue(conn.in_transaction)
            conn.rollback()
            self.assertFalse(conn.in_transaction)

            rows = conn.query("SELECT id, device, temp, payload FROM readings WHERE id = ?;", (1,))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["device"], evil)
            self.assertAlmostEqual(rows[0]["temp"], 21.5)
            self.assertEqual(rows[0]["payload"], b"\x00\x01\x02\xff")

            # Numbered $1 params + cursor fetchmany & iteration
            cur.execute("SELECT id, device FROM readings WHERE id >= $1 ORDER BY id;", (200,))
            self.assertEqual(cur.fetchmany(1), [(200, "dev-batch")])
            self.assertEqual(list(cur), [(201, "日本語 🚀")])
            self.assertIsNone(cur.fetchone())

            cur.execute("SELECT COUNT(*) FROM readings;")
            self.assertEqual(cur.fetchone(), (105,))

            with self.assertRaises(OverflowError):
                conn.execute("SELECT * FROM readings WHERE id = ?;", (1 << 80,))

            self.assertEqual(conn.tables(), ["readings"])
            stats = conn.stats()
            self.assertEqual(stats["tables"][0]["live_rows"], 105)
            self.assertIn("Table 'readings'", conn.stats_report())

            conn.vacuum()
            conn.checkpoint()

            backup_path = Path(self.tmpdir.name) / "backup.sqdb"
            conn.backup_to(backup_path)
            self.assertTrue(backup_path.is_file())

        # Re-open read-only concurrently
        with squishdb.connect(self.db_path, read_only=True) as ro1, squishdb.connect(
            self.db_path, read_only=True
        ) as ro2:
            self.assertEqual(ro1.execute("SELECT COUNT(*) FROM readings;").fetchone(), (105,))
            self.assertEqual(ro2.execute("SELECT COUNT(*) FROM readings;").fetchone(), (105,))
            with self.assertRaises(squishdb.DatabaseError):
                ro1.execute("INSERT INTO readings VALUES (500, 'ro', 0.0, NULL);")

    def test_edge_values_context_rollback_and_errors(self) -> None:
        i64_min = -(1 << 63)
        i64_max = (1 << 63) - 1

        with squishdb.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE edge (id INTEGER PRIMARY KEY, txt TEXT, raw BLOB);")
            conn.execute(
                "INSERT INTO edge VALUES (?, ?, ?);",
                (i64_min, "embedded\x00nul", b""),
            )
            conn.execute(
                "INSERT INTO edge VALUES (?, ?, ?);",
                (i64_max, "", b"\x00\xff"),
            )

            # Mismatched batch column count
            with self.assertRaises(squishdb.ProgrammingError):
                conn.insert_batch("edge", [(1, "a", b""), (2, "b")])

            # Invalid SQL raises DatabaseError
            with self.assertRaises(squishdb.DatabaseError):
                conn.execute("SELECT * FROM nonexistent_table;")

            rows = conn.query("SELECT id, txt, raw FROM edge ORDER BY id;")
            self.assertEqual(rows[0]["id"], i64_min)
            self.assertEqual(rows[0]["txt"], "embedded\x00nul")
            self.assertEqual(rows[0]["raw"], b"")
            self.assertEqual(rows[1]["id"], i64_max)
            self.assertEqual(rows[1]["txt"], "")
            self.assertEqual(rows[1]["raw"], b"\x00\xff")

        # Context manager auto-rollbacks uncommitted tx on exception
        try:
            with squishdb.connect(self.db_path) as conn:
                conn.begin()
                conn.execute("INSERT INTO edge VALUES (0, 'abort', NULL);")
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        conn = squishdb.connect(self.db_path)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM edge;").fetchone(), (2,))
        cur = conn.cursor()
        cur.close()
        with self.assertRaises(squishdb.ProgrammingError):
            cur.execute("SELECT 1;")
        conn.close()
        with self.assertRaises(squishdb.ProgrammingError):
            conn.execute("SELECT 1;")


if __name__ == "__main__":
    unittest.main()

import os
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

            cur.execute("SELECT COUNT(*) FROM readings;")
            self.assertEqual(cur.fetchone(), (103,))

            with self.assertRaises(OverflowError):
                conn.execute("SELECT * FROM readings WHERE id = ?;", (1 << 80,))

            self.assertEqual(conn.tables(), ["readings"])
            stats = conn.stats()
            self.assertEqual(stats["tables"][0]["live_rows"], 103)

            backup_path = Path(self.tmpdir.name) / "backup.sqdb"
            conn.backup_to(backup_path)
            self.assertTrue(backup_path.is_file())

        # Re-open read-only concurrently
        with squishdb.connect(self.db_path, read_only=True) as ro1, squishdb.connect(
            self.db_path, read_only=True
        ) as ro2:
            self.assertEqual(ro1.execute("SELECT COUNT(*) FROM readings;").fetchone(), (103,))
            self.assertEqual(ro2.execute("SELECT COUNT(*) FROM readings;").fetchone(), (103,))


if __name__ == "__main__":
    unittest.main()

package squishdb

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	if Version() == "" {
		t.Fatal("empty version")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.sqdb")
	backupPath := filepath.Join(dir, "backup.sqdb")

	db, err := OpenWithOptions(dbPath, DefaultOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	_, err = db.Execute(`
		CREATE TABLE sensors (
			id INTEGER PRIMARY KEY,
			device TEXT NOT NULL,
			temp REAL,
			payload BLOB
		) CLUSTER BY (device);
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	blob := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := db.Execute("INSERT INTO sensors VALUES (?, ?, ?, ?);", 1, "dev-01", 21.5, blob); err != nil {
		t.Fatalf("insert: %v", err)
	}

	n, err := db.BulkInsert("sensors", [][]any{
		{2, "dev-01", 22.5, nil},
		{3, "dev-02", 19.0, []byte{1, 2}},
	})
	if err != nil || n != 2 {
		t.Fatalf("bulk insert: n=%d err=%v", n, err)
	}

	// WAL InsertBatch + Unicode + empty blob
	bn, err := db.InsertBatch("sensors", [][]any{
		{4, "日本語 🚀", 18.5, []byte{}},
	})
	if err != nil || bn != 1 {
		t.Fatalf("insert batch: n=%d err=%v", bn, err)
	}

	// Mismatched batch column count must error
	if _, err := db.InsertBatch("sensors", [][]any{{10, "a", 1.0, nil}, {11, "b"}}); err == nil {
		t.Fatal("expected inconsistent column count error")
	}

	// Transaction rollback & commit
	if err := db.Begin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := db.Execute("INSERT INTO sensors VALUES (?, ?, ?, ?);", 99, "rolled", 0.0, nil); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if err := db.Begin(); err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if _, err := db.Execute("INSERT INTO sensors VALUES (?, ?, ?, ?);", 5, "dev-02", 21.0, nil); err != nil {
		t.Fatalf("tx insert 2: %v", err)
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, err := db.Query("SELECT device, COUNT(*) AS n, AVG(temp) AS avg_temp FROM sensors GROUP BY device ORDER BY device;")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 3 || rows[0]["device"] != "dev-01" || rows[0]["n"] != int64(2) {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	one, err := db.Query("SELECT payload FROM sensors WHERE id = ?;", 1)
	if err != nil || !bytes.Equal(one[0]["payload"].([]byte), blob) {
		t.Fatalf("blob mismatch: %+v err=%v", one, err)
	}

	stats, err := db.StatsJSON()
	if err != nil || !strings.Contains(stats, `"sensors"`) {
		t.Fatalf("stats failed: %s %v", stats, err)
	}

	if err := db.Vacuum(); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.BackupTo(backupPath); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Closed DB must error
	if _, err := db.Execute("SELECT 1;"); err == nil {
		t.Fatal("expected closed db error")
	}

	// Read-only verification
	ro, err := OpenReadOnly(backupPath)
	if err != nil {
		t.Fatalf("open readonly: %v", err)
	}
	defer ro.Close()
	cnt, err := ro.Query("SELECT COUNT(*) AS c FROM sensors;")
	if err != nil || cnt[0]["c"] != int64(5) {
		t.Fatalf("unexpected ro count: %+v err=%v", cnt, err)
	}
	if _, err := ro.Execute("INSERT INTO sensors VALUES (50, 'ro', 0.0, NULL);"); err == nil {
		t.Fatal("expected write on read-only db to fail")
	}
}

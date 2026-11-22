package squishdb

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	if Version() == "" {
		t.Fatal("empty version")
	}
	dbPath := filepath.Join(t.TempDir(), "test.sqdb")
	db, err := OpenWithOptions(dbPath, Options{Compression: ProfileBalanced})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

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

	rows, err := db.Query("SELECT device, COUNT(*) AS n, AVG(temp) AS avg_temp FROM sensors GROUP BY device ORDER BY device;")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 || rows[0]["device"] != "dev-01" || rows[0]["n"] != int64(2) {
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

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
}

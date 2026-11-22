// Package squishdb provides Go bindings for SquishDB via cgo.
//
// Build the native library first from the repository root:
//
//	cargo build --release
//
// Then run tests (on Linux/macOS set LD_LIBRARY_PATH / DYLD_LIBRARY_PATH if needed):
//
//	go test ./...
package squishdb

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/release -lsquishdb -Wl,-rpath,${SRCDIR}/../../target/release
#cgo darwin LDFLAGS: -L${SRCDIR}/../../target/release -lsquishdb -Wl,-rpath,${SRCDIR}/../../target/release
#cgo windows LDFLAGS: -L${SRCDIR}/../../target/release -lsquishdb
#include "squishdb.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

// CompressionProfile controls PAX-v4 secondary block compression.
type CompressionProfile int

const (
	ProfileFast     CompressionProfile = C.SQUISHDB_PROFILE_FAST
	ProfileBalanced CompressionProfile = C.SQUISHDB_PROFILE_BALANCED
	ProfileMax      CompressionProfile = C.SQUISHDB_PROFILE_MAX
	ProfileNone     CompressionProfile = C.SQUISHDB_PROFILE_NONE
)

// Options configures Database creation/opening.
// Use DefaultOptions() to get balanced compression defaults.
type Options struct {
	Compression        CompressionProfile
	SyncWAL            bool
	PageRows           int
	AutoCheckpointRows int
	CachePages         int
	IndexCacheBlocks   int
}

// DefaultOptions returns Options with Balanced compression and default cache sizes.
func DefaultOptions() Options {
	return Options{Compression: ProfileBalanced}
}

// StatementResult holds the result of one executed SQL statement.
type StatementResult struct {
	Kind     string // "rows", "affected", or "message"
	Columns  []string
	Rows     [][]any
	Affected uint64
	Message  string
}

// DB represents an open SquishDB connection.
type DB struct {
	ptr *C.SquishDb
}

// Version returns the native libsquishdb version string.
func Version() string {
	return C.GoString(C.squishdb_version())
}

func lastErr(fallback string) error {
	msg := C.GoString(C.squishdb_last_error())
	if msg == "" {
		msg = fallback
	}
	return errors.New(msg)
}

// Open opens or creates a SquishDB database with default options.
func Open(path string) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ptr := C.squishdb_open(cpath)
	if ptr == nil {
		return nil, lastErr("failed to open database")
	}
	return &DB{ptr: ptr}, nil
}

// OpenReadOnly opens a SquishDB database in shared read-only mode.
func OpenReadOnly(path string) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ptr := C.squishdb_open_readonly(cpath)
	if ptr == nil {
		return nil, lastErr("failed to open read-only database")
	}
	return &DB{ptr: ptr}, nil
}

// OpenWithOptions opens or creates a SquishDB database with explicit options.
func OpenWithOptions(path string, opts Options) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	syncWal := C.int(0)
	if opts.SyncWAL {
		syncWal = 1
	}
	ptr := C.squishdb_open_with_options(
		cpath,
		C.int(opts.Compression),
		syncWal,
		C.size_t(opts.PageRows),
		C.size_t(opts.AutoCheckpointRows),
		C.size_t(opts.CachePages),
		C.size_t(opts.IndexCacheBlocks),
	)
	if ptr == nil {
		return nil, lastErr("failed to open database with options")
	}
	return &DB{ptr: ptr}, nil
}

// Close closes the database handle.
func (db *DB) Close() error {
	if db.ptr != nil {
		C.squishdb_close(db.ptr)
		db.ptr = nil
	}
	return nil
}

func (db *DB) ensureOpen() error {
	if db.ptr == nil {
		return errors.New("squishdb: database is closed")
	}
	return nil
}

func pushParam(p *C.SquishParams, v any) error {
	switch val := v.(type) {
	case nil:
		C.squishdb_params_push_null(p)
	case bool:
		if val {
			C.squishdb_params_push_int(p, 1)
		} else {
			C.squishdb_params_push_int(p, 0)
		}
	case int:
		C.squishdb_params_push_int(p, C.int64_t(val))
	case int32:
		C.squishdb_params_push_int(p, C.int64_t(val))
	case int64:
		C.squishdb_params_push_int(p, C.int64_t(val))
	case float32:
		C.squishdb_params_push_real(p, C.double(val))
	case float64:
		C.squishdb_params_push_real(p, C.double(val))
	case string:
		b := []byte(val)
		var ptr *C.char
		if len(b) > 0 {
			ptr = (*C.char)(unsafe.Pointer(&b[0]))
		}
		if rc := C.squishdb_params_push_text(p, ptr, C.size_t(len(b))); rc != C.SQUISHDB_OK {
			return lastErr("invalid text parameter")
		}
	case []byte:
		var ptr *C.uint8_t
		if len(val) > 0 {
			ptr = (*C.uint8_t)(unsafe.Pointer(&val[0]))
		}
		if rc := C.squishdb_params_push_blob(p, ptr, C.size_t(len(val))); rc != C.SQUISHDB_OK {
			return lastErr("invalid blob parameter")
		}
	default:
		return fmt.Errorf("squishdb: unsupported parameter type %T", v)
	}
	return nil
}

func pushBatchVal(b *C.SquishBatch, v any) error {
	switch val := v.(type) {
	case nil:
		C.squishdb_batch_push_null(b)
	case bool:
		if val {
			C.squishdb_batch_push_int(b, 1)
		} else {
			C.squishdb_batch_push_int(b, 0)
		}
	case int:
		C.squishdb_batch_push_int(b, C.int64_t(val))
	case int32:
		C.squishdb_batch_push_int(b, C.int64_t(val))
	case int64:
		C.squishdb_batch_push_int(b, C.int64_t(val))
	case float32:
		C.squishdb_batch_push_real(b, C.double(val))
	case float64:
		C.squishdb_batch_push_real(b, C.double(val))
	case string:
		bytes := []byte(val)
		var ptr *C.char
		if len(bytes) > 0 {
			ptr = (*C.char)(unsafe.Pointer(&bytes[0]))
		}
		if rc := C.squishdb_batch_push_text(b, ptr, C.size_t(len(bytes))); rc != C.SQUISHDB_OK {
			return lastErr("invalid batch text")
		}
	case []byte:
		var ptr *C.uint8_t
		if len(val) > 0 {
			ptr = (*C.uint8_t)(unsafe.Pointer(&val[0]))
		}
		if rc := C.squishdb_batch_push_blob(b, ptr, C.size_t(len(val))); rc != C.SQUISHDB_OK {
			return lastErr("invalid batch blob")
		}
	default:
		return fmt.Errorf("squishdb: unsupported batch value type %T", v)
	}
	return nil
}

// Execute runs one or more SQL statements with optional positional (?) or numbered ($1) parameters.
func (db *DB) Execute(sql string, params ...any) ([]StatementResult, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var res *C.SquishResults
	if len(params) == 0 {
		res = C.squishdb_execute(db.ptr, csql)
	} else {
		p := C.squishdb_params_new()
		defer C.squishdb_params_free(p)
		for _, v := range params {
			if err := pushParam(p, v); err != nil {
				return nil, err
			}
		}
		res = C.squishdb_execute_params(db.ptr, csql, p)
	}
	if res == nil {
		return nil, lastErr("SQL execution failed")
	}
	defer C.squishdb_results_free(res)

	count := int(C.squishdb_results_count(res))
	out := make([]StatementResult, 0, count)
	for s := 0; s < count; s++ {
		cs := C.size_t(s)
		kind := C.squishdb_result_kind(res, cs)
		switch kind {
		case C.SQUISHDB_RESULT_ROWS:
			ncols := int(C.squishdb_result_column_count(res, cs))
			nrows := int(C.squishdb_result_row_count(res, cs))
			cols := make([]string, ncols)
			for c := 0; c < ncols; c++ {
				cols[c] = C.GoString(C.squishdb_result_column_name(res, cs, C.size_t(c)))
			}
			rows := make([][]any, nrows)
			for r := 0; r < nrows; r++ {
				cr := C.size_t(r)
				row := make([]any, ncols)
				for c := 0; c < ncols; c++ {
					cc := C.size_t(c)
					switch C.squishdb_result_value_type(res, cs, cr, cc) {
					case C.SQUISHDB_TYPE_NULL:
						row[c] = nil
					case C.SQUISHDB_TYPE_INTEGER:
						row[c] = int64(C.squishdb_result_value_int(res, cs, cr, cc))
					case C.SQUISHDB_TYPE_REAL:
						row[c] = float64(C.squishdb_result_value_real(res, cs, cr, cc))
					case C.SQUISHDB_TYPE_TEXT:
						var n C.size_t
						tp := C.squishdb_result_value_text(res, cs, cr, cc, &n)
						row[c] = C.GoStringN(tp, C.int(n))
					case C.SQUISHDB_TYPE_BLOB:
						var n C.size_t
						bp := C.squishdb_result_value_blob(res, cs, cr, cc, &n)
						row[c] = C.GoBytes(unsafe.Pointer(bp), C.int(n))
					}
				}
				rows[r] = row
			}
			out = append(out, StatementResult{Kind: "rows", Columns: cols, Rows: rows})
		case C.SQUISHDB_RESULT_AFFECTED:
			out = append(out, StatementResult{
				Kind:     "affected",
				Affected: uint64(C.squishdb_result_affected(res, cs)),
			})
		default:
			out = append(out, StatementResult{
				Kind:    "message",
				Message: C.GoString(C.squishdb_result_message(res, cs)),
			})
		}
	}
	return out, nil
}

// Query executes a SELECT and returns rows as maps keyed by column name.
func (db *DB) Query(sql string, params ...any) ([]map[string]any, error) {
	results, err := db.Execute(sql, params...)
	if err != nil {
		return nil, err
	}
	for i := len(results) - 1; i >= 0; i-- {
		if results[i].Kind == "rows" {
			r := results[i]
			maps := make([]map[string]any, len(r.Rows))
			for idx, row := range r.Rows {
				m := make(map[string]any, len(r.Columns))
				for c, col := range r.Columns {
					m[col] = row[c]
				}
				maps[idx] = m
			}
			return maps, nil
		}
	}
	return nil, nil
}

func (db *DB) runBatch(table string, rows [][]any, bulk bool) (int64, error) {
	if err := db.ensureOpen(); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	ncols := len(rows[0])
	batch := C.squishdb_batch_new(C.size_t(ncols))
	defer C.squishdb_batch_free(batch)
	for _, row := range rows {
		if len(row) != ncols {
			return 0, fmt.Errorf("squishdb: inconsistent column count in batch: expected %d, got %d", ncols, len(row))
		}
		for _, v := range row {
			if err := pushBatchVal(batch, v); err != nil {
				return 0, err
			}
		}
	}
	ctable := C.CString(table)
	defer C.free(unsafe.Pointer(ctable))
	var n C.int64_t
	if bulk {
		n = C.squishdb_bulk_insert(db.ptr, ctable, batch)
	} else {
		n = C.squishdb_insert_batch(db.ptr, ctable, batch)
	}
	if n < 0 {
		return 0, lastErr("batch insert failed")
	}
	return int64(n), nil
}

// InsertBatch performs a crash-safe WAL-backed batch insert.
func (db *DB) InsertBatch(table string, rows [][]any) (int64, error) {
	return db.runBatch(table, rows, false)
}

// BulkInsert loads rows directly into compressed PAX-v4 pages.
func (db *DB) BulkInsert(table string, rows [][]any) (int64, error) {
	return db.runBatch(table, rows, true)
}

func (db *DB) Begin() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if C.squishdb_begin(db.ptr) != C.SQUISHDB_OK {
		return lastErr("begin failed")
	}
	return nil
}

func (db *DB) Commit() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if C.squishdb_commit(db.ptr) != C.SQUISHDB_OK {
		return lastErr("commit failed")
	}
	return nil
}

func (db *DB) Rollback() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if C.squishdb_rollback(db.ptr) != C.SQUISHDB_OK {
		return lastErr("rollback failed")
	}
	return nil
}

func (db *DB) Checkpoint() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if C.squishdb_checkpoint(db.ptr) != C.SQUISHDB_OK {
		return lastErr("checkpoint failed")
	}
	return nil
}

func (db *DB) Vacuum() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if C.squishdb_vacuum(db.ptr) != C.SQUISHDB_OK {
		return lastErr("vacuum failed")
	}
	return nil
}

func (db *DB) BackupTo(destPath string) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	cdest := C.CString(destPath)
	defer C.free(unsafe.Pointer(cdest))
	if C.squishdb_backup_to(db.ptr, cdest) != C.SQUISHDB_OK {
		return lastErr("backup failed")
	}
	return nil
}

func (db *DB) StatsJSON() (string, error) {
	if err := db.ensureOpen(); err != nil {
		return "", err
	}
	ptr := C.squishdb_stats_json(db.ptr)
	if ptr == nil {
		return "", lastErr("stats failed")
	}
	defer C.squishdb_string_free(ptr)
	return C.GoString(ptr), nil
}

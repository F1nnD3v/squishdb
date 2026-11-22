/*
 * SquishDB C99 API Header
 * https://github.com/F1nnD3v/squishdb
 *
 * License: MIT
 */

#ifndef SQUISHDB_H
#define SQUISHDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================= */
/* Status / Error Codes                                                      */
/* ========================================================================= */

#define SQUISHDB_OK             0
#define SQUISHDB_ERR_IO         1
#define SQUISHDB_ERR_CORRUPTION 2
#define SQUISHDB_ERR_SQL        3
#define SQUISHDB_ERR_SCHEMA     4
#define SQUISHDB_ERR_INVALID    5
#define SQUISHDB_ERR_BUSY       6
#define SQUISHDB_ERR_PANIC      7
#define SQUISHDB_ERR_NULL       8

/* ========================================================================= */
/* Compression Profiles                                                      */
/* ========================================================================= */

#define SQUISHDB_PROFILE_FAST     0
#define SQUISHDB_PROFILE_BALANCED 1
#define SQUISHDB_PROFILE_MAX      2
#define SQUISHDB_PROFILE_NONE     3

/* ========================================================================= */
/* Result Kind & Value Type Tags                                             */
/* ========================================================================= */

#define SQUISHDB_RESULT_ROWS     0
#define SQUISHDB_RESULT_AFFECTED 1
#define SQUISHDB_RESULT_MESSAGE  2

#define SQUISHDB_TYPE_NULL    0
#define SQUISHDB_TYPE_INTEGER 1
#define SQUISHDB_TYPE_REAL    2
#define SQUISHDB_TYPE_TEXT    3
#define SQUISHDB_TYPE_BLOB    4

/* ========================================================================= */
/* Opaque Handles                                                            */
/* ========================================================================= */

typedef struct SquishDb SquishDb;
typedef struct SquishResults SquishResults;
typedef struct SquishParams SquishParams;
typedef struct SquishBatch SquishBatch;

/* ========================================================================= */
/* Version & Error Inspection                                                */
/* ========================================================================= */

/** Returns the static NUL-terminated SquishDB version string. */
const char* squishdb_version(void);

/** Returns the last error message on the current thread (empty string if none). */
const char* squishdb_last_error(void);

/** Frees a heap string returned by squishdb_stats_report / squishdb_stats_json / squishdb_table_names_json. */
void squishdb_string_free(char* s);

/* ========================================================================= */
/* Database Lifecycle                                                        */
/* ========================================================================= */

/** Opens or creates a read-write database with default options. Returns NULL on error. */
SquishDb* squishdb_open(const char* path);

/** Opens a read-only database using a shared OS lock. Returns NULL on error. */
SquishDb* squishdb_open_readonly(const char* path);

/**
 * Opens or creates a database with explicit options.
 * Pass 0 for page_rows, auto_checkpoint_rows, cache_pages, or index_cache_blocks
 * to keep the default value.
 */
SquishDb* squishdb_open_with_options(
    const char* path,
    int compression_profile,
    int sync_wal,
    size_t page_rows,
    size_t auto_checkpoint_rows,
    size_t cache_pages,
    size_t index_cache_blocks
);

/** Closes the database, checkpointing dirty writes outside an active transaction. */
void squishdb_close(SquishDb* db);

/* ========================================================================= */
/* SQL Parameter Builder (? and $1/$2 placeholders)                          */
/* ========================================================================= */

SquishParams* squishdb_params_new(void);
void squishdb_params_clear(SquishParams* params);
void squishdb_params_free(SquishParams* params);

void squishdb_params_push_null(SquishParams* params);
void squishdb_params_push_int(SquishParams* params, int64_t value);
void squishdb_params_push_real(SquishParams* params, double value);
int squishdb_params_push_text(SquishParams* params, const char* utf8, size_t len);
int squishdb_params_push_cstr(SquishParams* params, const char* utf8_nul);
int squishdb_params_push_blob(SquishParams* params, const uint8_t* data, size_t len);

/* ========================================================================= */
/* SQL Execution & Result Inspection                                         */
/* ========================================================================= */

/** Executes one or more SQL statements. Free result with squishdb_results_free(). */
SquishResults* squishdb_execute(SquishDb* db, const char* sql);

/** Executes one or more SQL statements with bound parameters (params may be NULL). */
SquishResults* squishdb_execute_params(
    SquishDb* db,
    const char* sql,
    const SquishParams* params
);

void squishdb_results_free(SquishResults* res);

size_t squishdb_results_count(const SquishResults* res);
int squishdb_result_kind(const SquishResults* res, size_t stmt_idx);
uint64_t squishdb_result_affected(const SquishResults* res, size_t stmt_idx);
const char* squishdb_result_message(const SquishResults* res, size_t stmt_idx);

size_t squishdb_result_column_count(const SquishResults* res, size_t stmt_idx);
const char* squishdb_result_column_name(const SquishResults* res, size_t stmt_idx, size_t col_idx);
size_t squishdb_result_row_count(const SquishResults* res, size_t stmt_idx);

int squishdb_result_value_type(
    const SquishResults* res,
    size_t stmt_idx,
    size_t row_idx,
    size_t col_idx
);
int64_t squishdb_result_value_int(
    const SquishResults* res,
    size_t stmt_idx,
    size_t row_idx,
    size_t col_idx
);
double squishdb_result_value_real(
    const SquishResults* res,
    size_t stmt_idx,
    size_t row_idx,
    size_t col_idx
);
const char* squishdb_result_value_text(
    const SquishResults* res,
    size_t stmt_idx,
    size_t row_idx,
    size_t col_idx,
    size_t* out_len
);
const uint8_t* squishdb_result_value_blob(
    const SquishResults* res,
    size_t stmt_idx,
    size_t row_idx,
    size_t col_idx,
    size_t* out_len
);

/* ========================================================================= */
/* High-Throughput Batch Insert Builder                                      */
/* ========================================================================= */

SquishBatch* squishdb_batch_new(size_t num_cols);
void squishdb_batch_free(SquishBatch* batch);

void squishdb_batch_push_null(SquishBatch* batch);
void squishdb_batch_push_int(SquishBatch* batch, int64_t value);
void squishdb_batch_push_real(SquishBatch* batch, double value);
int squishdb_batch_push_text(SquishBatch* batch, const char* utf8, size_t len);
int squishdb_batch_push_cstr(SquishBatch* batch, const char* utf8_nul);
int squishdb_batch_push_blob(SquishBatch* batch, const uint8_t* data, size_t len);

/** Crash-safe WAL-backed batch insert. Consumes batch rows. Returns count or -1 on error. */
int64_t squishdb_insert_batch(SquishDb* db, const char* table, SquishBatch* batch);

/** Direct PAX-v4 bulk load (fastest initial ingestion). Consumes batch rows. Returns count or -1. */
int64_t squishdb_bulk_insert(SquishDb* db, const char* table, SquishBatch* batch);

/* ========================================================================= */
/* Transactions, Maintenance & Introspection                                 */
/* ========================================================================= */

int squishdb_begin(SquishDb* db);
int squishdb_commit(SquishDb* db);
int squishdb_rollback(SquishDb* db);
int squishdb_in_transaction(const SquishDb* db);

int squishdb_checkpoint(SquishDb* db);
int squishdb_vacuum(SquishDb* db);
int squishdb_backup_to(SquishDb* db, const char* dest_path);

/** Returns a newly allocated formatted stats report. Free with squishdb_string_free(). */
char* squishdb_stats_report(const SquishDb* db);

/** Returns a newly allocated JSON stats document. Free with squishdb_string_free(). */
char* squishdb_stats_json(const SquishDb* db);

/** Returns a newly allocated JSON array of table names. Free with squishdb_string_free(). */
char* squishdb_table_names_json(const SquishDb* db);

#ifdef __cplusplus
}
#endif

#endif /* SQUISHDB_H */

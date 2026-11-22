/*
 * SquishDB C99 Example & Smoke Test
 *
 * Build on Linux:
 *   cc -O2 -I../../include example.c -L../../target/release -lsquishdb -Wl,-rpath,../../target/release -o example
 * Build on macOS:
 *   cc -O2 -I../../include example.c -L../../target/release -lsquishdb -Wl,-rpath,../../target/release -o example
 * Build on Windows (MSVC):
 *   cl /O2 /I..\..\include example.c ..\..\target\release\squishdb.dll.lib
 */

#include "squishdb.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : "c_example.sqdb";
    remove(path);

    printf("SquishDB C example (version %s)\n", squishdb_version());

    SquishDb* db = squishdb_open(path);
    if (!db) {
        fprintf(stderr, "open failed: %s\n", squishdb_last_error());
        return 1;
    }

    SquishResults* r = squishdb_execute(
        db,
        "CREATE TABLE sensors ("
        "  id INTEGER PRIMARY KEY,"
        "  device TEXT NOT NULL,"
        "  temp REAL,"
        "  payload BLOB"
        ") CLUSTER BY (device);"
    );
    assert(r != NULL);
    squishdb_results_free(r);

    /* Parameterized insert */
    SquishParams* p = squishdb_params_new();
    squishdb_params_push_int(p, 1);
    squishdb_params_push_cstr(p, "dev-01");
    squishdb_params_push_real(p, 21.5);
    const uint8_t blob[] = { 0xCA, 0xFE, 0xBA, 0xBE };
    squishdb_params_push_blob(p, blob, sizeof(blob));

    r = squishdb_execute_params(db, "INSERT INTO sensors VALUES (?, ?, ?, ?);", p);
    assert(r != NULL);
    assert(squishdb_result_kind(r, 0) == SQUISHDB_RESULT_AFFECTED);
    assert(squishdb_result_affected(r, 0) == 1);
    squishdb_results_free(r);
    squishdb_params_free(p);

    /* Bulk insert */
    SquishBatch* batch = squishdb_batch_new(4);
    for (int64_t i = 2; i <= 10; ++i) {
        squishdb_batch_push_int(batch, i);
        squishdb_batch_push_cstr(batch, (i % 2 == 0) ? "dev-01" : "dev-02");
        squishdb_batch_push_real(batch, 20.0 + (double)i * 0.25);
        squishdb_batch_push_null(batch);
    }
    int64_t inserted = squishdb_bulk_insert(db, "sensors", batch);
    assert(inserted == 9);
    squishdb_batch_free(batch);

    /* Query back */
    r = squishdb_execute(
        db,
        "SELECT device, COUNT(*) AS n, AVG(temp) AS avg_temp "
        "FROM sensors GROUP BY device ORDER BY device;"
    );
    assert(r != NULL);
    assert(squishdb_result_row_count(r, 0) == 2);
    for (size_t row = 0; row < squishdb_result_row_count(r, 0); ++row) {
        size_t len = 0;
        const char* dev = squishdb_result_value_text(r, 0, row, 0, &len);
        int64_t cnt = squishdb_result_value_int(r, 0, row, 1);
        double avg = squishdb_result_value_real(r, 0, row, 2);
        printf("  %.*s: count=%lld avg=%.2f\n", (int)len, dev, (long long)cnt, avg);
    }
    squishdb_results_free(r);

    /* Verify blob round-trip */
    p = squishdb_params_new();
    squishdb_params_push_int(p, 1);
    r = squishdb_execute_params(db, "SELECT payload FROM sensors WHERE id = ?;", p);
    assert(r != NULL);
    assert(squishdb_result_row_count(r, 0) == 1);
    assert(squishdb_result_value_type(r, 0, 0, 0) == SQUISHDB_TYPE_BLOB);
    size_t blen = 0;
    const uint8_t* bptr = squishdb_result_value_blob(r, 0, 0, 0, &blen);
    assert(blen == sizeof(blob));
    assert(memcmp(bptr, blob, sizeof(blob)) == 0);
    squishdb_results_free(r);
    squishdb_params_free(p);

    char* stats = squishdb_stats_json(db);
    assert(stats != NULL);
    assert(strstr(stats, "\"sensors\"") != NULL);
    squishdb_string_free(stats);

    assert(squishdb_checkpoint(db) == SQUISHDB_OK);
    squishdb_close(db);
    remove(path);

    printf("C example OK!\n");
    return 0;
}

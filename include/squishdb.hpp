// SquishDB Modern C++17 Header-Only Wrapper
// https://github.com/F1nnD3v/squishdb
// License: MIT

#pragma once

#include "squishdb.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace squishdb {

enum class CompressionProfile : int {
    Fast = SQUISHDB_PROFILE_FAST,
    Balanced = SQUISHDB_PROFILE_BALANCED,
    Max = SQUISHDB_PROFILE_MAX,
    None = SQUISHDB_PROFILE_NONE,
};

struct Options {
    CompressionProfile compression = CompressionProfile::Balanced;
    bool sync_wal = false;
    size_t page_rows = 0;
    size_t auto_checkpoint_rows = 0;
    size_t cache_pages = 0;
    size_t index_cache_blocks = 0;
};

class Error : public std::runtime_error {
public:
    explicit Error(const std::string& msg, int code = -1)
        : std::runtime_error(msg), code_(code) {}
    int code() const noexcept { return code_; }

private:
    int code_;
};

using Blob = std::vector<uint8_t>;
using Value = std::variant<std::monostate, int64_t, double, std::string, Blob>;

inline Value null_value() { return std::monostate{}; }

enum class ResultKind {
    Rows,
    Affected,
    Message,
};

struct StatementResult {
    ResultKind kind = ResultKind::Message;
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> rows;
    uint64_t affected = 0;
    std::string message;
};

inline std::string version() {
    const char* v = squishdb_version();
    return v ? std::string(v) : std::string();
}

namespace detail {

inline std::string last_error_or(const char* fallback) {
    const char* e = squishdb_last_error();
    return (e && *e) ? std::string(e) : std::string(fallback);
}

inline void check_rc(int rc, const char* fallback) {
    if (rc != SQUISHDB_OK) {
        throw Error(last_error_or(fallback), rc);
    }
}

inline void push_param(SquishParams* p, const Value& v) {
    if (std::holds_alternative<std::monostate>(v)) {
        squishdb_params_push_null(p);
    } else if (auto* i = std::get_if<int64_t>(&v)) {
        squishdb_params_push_int(p, *i);
    } else if (auto* d = std::get_if<double>(&v)) {
        squishdb_params_push_real(p, *d);
    } else if (auto* s = std::get_if<std::string>(&v)) {
        check_rc(squishdb_params_push_text(p, s->data(), s->size()), "Failed to push text param");
    } else if (auto* b = std::get_if<Blob>(&v)) {
        check_rc(
            squishdb_params_push_blob(p, b->empty() ? nullptr : b->data(), b->size()),
            "Failed to push blob param");
    }
}

inline void push_batch_val(SquishBatch* b, const Value& v) {
    if (std::holds_alternative<std::monostate>(v)) {
        squishdb_batch_push_null(b);
    } else if (auto* i = std::get_if<int64_t>(&v)) {
        squishdb_batch_push_int(b, *i);
    } else if (auto* d = std::get_if<double>(&v)) {
        squishdb_batch_push_real(b, *d);
    } else if (auto* s = std::get_if<std::string>(&v)) {
        check_rc(squishdb_batch_push_text(b, s->data(), s->size()), "Failed to push batch text");
    } else if (auto* bl = std::get_if<Blob>(&v)) {
        check_rc(
            squishdb_batch_push_blob(b, bl->empty() ? nullptr : bl->data(), bl->size()),
            "Failed to push batch blob");
    }
}

inline std::vector<StatementResult> extract_results(SquishResults* raw) {
    struct Guard {
        SquishResults* ptr;
        ~Guard() { squishdb_results_free(ptr); }
    } guard{raw};

    size_t count = squishdb_results_count(raw);
    std::vector<StatementResult> out;
    out.reserve(count);

    for (size_t s = 0; s < count; ++s) {
        StatementResult item;
        int kind = squishdb_result_kind(raw, s);
        if (kind == SQUISHDB_RESULT_ROWS) {
            item.kind = ResultKind::Rows;
            size_t ncols = squishdb_result_column_count(raw, s);
            size_t nrows = squishdb_result_row_count(raw, s);
            item.columns.reserve(ncols);
            for (size_t c = 0; c < ncols; ++c) {
                item.columns.emplace_back(squishdb_result_column_name(raw, s, c));
            }
            item.rows.reserve(nrows);
            for (size_t r = 0; r < nrows; ++r) {
                std::vector<Value> row;
                row.reserve(ncols);
                for (size_t c = 0; c < ncols; ++c) {
                    int t = squishdb_result_value_type(raw, s, r, c);
                    switch (t) {
                    case SQUISHDB_TYPE_NULL:
                        row.emplace_back(std::monostate{});
                        break;
                    case SQUISHDB_TYPE_INTEGER:
                        row.emplace_back(squishdb_result_value_int(raw, s, r, c));
                        break;
                    case SQUISHDB_TYPE_REAL:
                        row.emplace_back(squishdb_result_value_real(raw, s, r, c));
                        break;
                    case SQUISHDB_TYPE_TEXT: {
                        size_t len = 0;
                        const char* ptr = squishdb_result_value_text(raw, s, r, c, &len);
                        row.emplace_back(std::string(ptr, len));
                        break;
                    }
                    case SQUISHDB_TYPE_BLOB: {
                        size_t len = 0;
                        const uint8_t* ptr = squishdb_result_value_blob(raw, s, r, c, &len);
                        row.emplace_back(Blob(ptr, ptr + len));
                        break;
                    }
                    default:
                        row.emplace_back(std::monostate{});
                        break;
                    }
                }
                item.rows.push_back(std::move(row));
            }
        } else if (kind == SQUISHDB_RESULT_AFFECTED) {
            item.kind = ResultKind::Affected;
            item.affected = squishdb_result_affected(raw, s);
        } else {
            item.kind = ResultKind::Message;
            item.message = squishdb_result_message(raw, s);
        }
        out.push_back(std::move(item));
    }
    return out;
}

} // namespace detail

class Database {
public:
    static Database open(const std::string& path) {
        SquishDb* db = squishdb_open(path.c_str());
        if (!db) {
            throw Error(detail::last_error_or("Failed to open database"));
        }
        return Database(db);
    }

    static Database open_readonly(const std::string& path) {
        SquishDb* db = squishdb_open_readonly(path.c_str());
        if (!db) {
            throw Error(detail::last_error_or("Failed to open read-only database"));
        }
        return Database(db);
    }

    static Database open_with_options(const std::string& path, const Options& opts) {
        SquishDb* db = squishdb_open_with_options(
            path.c_str(),
            static_cast<int>(opts.compression),
            opts.sync_wal ? 1 : 0,
            opts.page_rows,
            opts.auto_checkpoint_rows,
            opts.cache_pages,
            opts.index_cache_blocks);
        if (!db) {
            throw Error(detail::last_error_or("Failed to open database with options"));
        }
        return Database(db);
    }

    ~Database() { close(); }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    Database(Database&& other) noexcept : db_(other.db_) { other.db_ = nullptr; }
    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            other.db_ = nullptr;
        }
        return *this;
    }

    void close() noexcept {
        if (db_) {
            squishdb_close(db_);
            db_ = nullptr;
        }
    }

    std::vector<StatementResult> execute(const std::string& sql) {
        ensure_open();
        SquishResults* res = squishdb_execute(db_, sql.c_str());
        if (!res) {
            throw Error(detail::last_error_or("SQL execution failed"));
        }
        return detail::extract_results(res);
    }

    std::vector<StatementResult> execute(const std::string& sql, const std::vector<Value>& params) {
        ensure_open();
        SquishParams* p = squishdb_params_new();
        struct Guard {
            SquishParams* ptr;
            ~Guard() { squishdb_params_free(ptr); }
        } guard{p};
        for (const auto& v : params) {
            detail::push_param(p, v);
        }
        SquishResults* res = squishdb_execute_params(db_, sql.c_str(), p);
        if (!res) {
            throw Error(detail::last_error_or("Parameterized SQL execution failed"));
        }
        return detail::extract_results(res);
    }

    int64_t insert_batch(const std::string& table, const std::vector<std::vector<Value>>& rows) {
        return run_batch(table, rows, false);
    }

    int64_t bulk_insert(const std::string& table, const std::vector<std::vector<Value>>& rows) {
        return run_batch(table, rows, true);
    }

    void begin() {
        ensure_open();
        detail::check_rc(squishdb_begin(db_), "begin failed");
    }

    void commit() {
        ensure_open();
        detail::check_rc(squishdb_commit(db_), "commit failed");
    }

    void rollback() {
        ensure_open();
        detail::check_rc(squishdb_rollback(db_), "rollback failed");
    }

    bool in_transaction() const noexcept {
        return db_ != nullptr && squishdb_in_transaction(db_) != 0;
    }

    void checkpoint() {
        ensure_open();
        detail::check_rc(squishdb_checkpoint(db_), "checkpoint failed");
    }

    void vacuum() {
        ensure_open();
        detail::check_rc(squishdb_vacuum(db_), "vacuum failed");
    }

    void backup_to(const std::string& dest) {
        ensure_open();
        detail::check_rc(squishdb_backup_to(db_, dest.c_str()), "backup_to failed");
    }

    std::string stats_report() const {
        ensure_open();
        char* s = squishdb_stats_report(db_);
        if (!s) {
            throw Error(detail::last_error_or("stats_report failed"));
        }
        std::string out(s);
        squishdb_string_free(s);
        return out;
    }

    std::string stats_json() const {
        ensure_open();
        char* s = squishdb_stats_json(db_);
        if (!s) {
            throw Error(detail::last_error_or("stats_json failed"));
        }
        std::string out(s);
        squishdb_string_free(s);
        return out;
    }

private:
    explicit Database(SquishDb* db) : db_(db) {}

    void ensure_open() const {
        if (!db_) {
            throw Error("Database is closed", SQUISHDB_ERR_NULL);
        }
    }

    int64_t run_batch(
        const std::string& table,
        const std::vector<std::vector<Value>>& rows,
        bool bulk) {
        ensure_open();
        if (rows.empty()) {
            return 0;
        }
        size_t ncols = rows[0].size();
        SquishBatch* batch = squishdb_batch_new(ncols);
        struct Guard {
            SquishBatch* ptr;
            ~Guard() { squishdb_batch_free(ptr); }
        } guard{batch};
        for (const auto& row : rows) {
            if (row.size() != ncols) {
                throw Error("Inconsistent column count in batch", SQUISHDB_ERR_INVALID);
            }
            for (const auto& v : row) {
                detail::push_batch_val(batch, v);
            }
        }
        int64_t n = bulk
            ? squishdb_bulk_insert(db_, table.c_str(), batch)
            : squishdb_insert_batch(db_, table.c_str(), batch);
        if (n < 0) {
            throw Error(detail::last_error_or("batch insert failed"));
        }
        return n;
    }

    SquishDb* db_ = nullptr;
};

} // namespace squishdb

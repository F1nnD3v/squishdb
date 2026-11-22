// SquishDB C++17 Example & Smoke Test
//
// Build on Linux/macOS:
//   c++ -std=c++17 -O2 -I../../include example.cpp -L../../target/release -lsquishdb -Wl,-rpath,../../target/release -o example
// Build on Windows (MSVC):
//   cl /std:c++17 /EHsc /O2 /I..\..\include example.cpp ..\..\target\release\squishdb.dll.lib

#include "squishdb.hpp"

#include <cassert>
#include <cstdio>
#include <iostream>

int main(int argc, char** argv) {
    const std::string path = (argc > 1) ? argv[1] : "cpp_example.sqdb";
    std::remove(path.c_str());

    std::cout << "SquishDB C++17 example (version " << squishdb::version() << ")\n";

    {
        auto db = squishdb::Database::open(path);
        db.execute(
            "CREATE TABLE events ("
            "  id INTEGER PRIMARY KEY,"
            "  kind TEXT NOT NULL,"
            "  amount REAL,"
            "  raw BLOB"
            ") CLUSTER BY (kind);");

        db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?);",
            {int64_t{1}, std::string{"purchase"}, 49.99, squishdb::Blob{0x01, 0x02, 0x03}});

        db.begin();
        db.insert_batch(
            "events",
            {
                {int64_t{2}, std::string{"signup"}, squishdb::null_value(), squishdb::null_value()},
                {int64_t{3}, std::string{"purchase"}, 19.50, squishdb::Blob{0xFF}},
            });
        db.commit();

        auto results = db.execute(
            "SELECT kind, COUNT(*), SUM(amount) FROM events GROUP BY kind ORDER BY kind;");
        assert(results.size() == 1);
        assert(results[0].rows.size() == 2);

        const auto& purchase_row = results[0].rows[0];
        assert(std::get<std::string>(purchase_row[0]) == "purchase");
        assert(std::get<int64_t>(purchase_row[1]) == 2);
        assert(std::get<double>(purchase_row[2]) > 69.4 && std::get<double>(purchase_row[2]) < 69.6);

        // Verify parameterized blob lookup
        auto blob_res = db.execute("SELECT raw FROM events WHERE id = $1;", {int64_t{1}});
        const auto& blob = std::get<squishdb::Blob>(blob_res[0].rows[0][0]);
        assert((blob == squishdb::Blob{0x01, 0x02, 0x03}));

        db.checkpoint();
    }

    std::remove(path.c_str());
    std::cout << "C++17 example OK!\n";
    return 0;
}

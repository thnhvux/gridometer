#include <filesystem>
#include <iostream>
#include <string>

#include "database.hpp"
#include "csvparser.hpp"
#include "minimalcsvparser.hpp"
#include "analytic.hpp"

int main() {
    try {
        std::cout << "Starting pipeline\n";

        std::string db_file   = "./data/database/grid_data_demo.db";
        std::string data_file = "./data/raw/PUB_Demand_2026_v90.csv";
        std::string out_dir   = "./data/export";

        std::filesystem::create_directories("./data/database");
        std::filesystem::create_directories(out_dir);
        std::filesystem::remove(db_file);

        std::cout << "Initializing SQLite database...\n";
        Database db(db_file);
        db.initSchema();

        std::cout << "Ingesting CSV data from: " << data_file << "\n";
        MINCSVParser::csvIngest(data_file, db);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db.getDB(), "SELECT COUNT(*) FROM DemandData;", -1, &stmt, nullptr);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << "Rows in DemandData: " << sqlite3_column_int(stmt, 0) << "\n";
        }
        sqlite3_finalize(stmt);

        std::cout << "Running analysis and generating forecast...\n";
        Analytic::generateForecast(db.getDB(), out_dir);

        std::cout << "Completed\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "Pipeline failed: " << e.what() << "\n";
        return 1;
    }
}
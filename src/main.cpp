#include <filesystem>
#include <iostream>
#include <string>

#include "pipeline.hpp"
#include "analytic.hpp"

void debugger(){
  std::string db_path = "data/processed/gridometer.db";
  std::cout << "Current working directory: " << std::filesystem::current_path() << "\n";
  Pipeline pipeline(db_path);
  sqlite3_stmt* stmt = nullptr;
  //Debug start
  auto printCount = [&](const char* sql, const char* label) {
    if (sqlite3_prepare_v2(pipeline.getDB(), sql, -1, &stmt, nullptr) == SQLITE_OK) {
      if (sqlite3_step(stmt) == SQLITE_ROW) {
        std::cout << label << ": " << sqlite3_column_int(stmt, 0) << "\n";
      }
    }
    sqlite3_finalize(stmt);
    stmt = nullptr;
  };

  printCount("SELECT COUNT(*) FROM DEMAND_HISTORY;", "DEMAND_HISTORY rows");
  printCount("SELECT COUNT(*) FROM ADEQUACY;", "ADEQUACY rows");
  printCount(
    "SELECT COUNT(*) "
    "FROM DEMAND_HISTORY d "
    "JOIN ADEQUACY a ON d.DATE = a.DATE AND d.HOUR = a.HOUR;",
    "JOIN rows"
  );
  //Debug ends
}

//Temporary hardcode paths, reading cfg for next version
int main() {
  try{
    std::string db_path = "data/processed/gridometer.db";
    std::string demand_csv = "data/raw/demand/PUB_Demand_2026_v91.csv";
    std::string adequacy_dir = "data/raw/adequacy";
    std::string processed_dir = "data/processed";

    std::filesystem::create_directories(processed_dir);

    Pipeline pipeline(db_path);
    sqlite3_stmt* stmt = nullptr;
    /*
    void debugger();
    */

    pipeline.initDBSchema();
    pipeline.ingestDemandCSV(demand_csv);
    pipeline.ingestAdequacyDir(adequacy_dir);

    Analytic::runAnalysis(pipeline.getDB(), processed_dir);
    std::cout << "Pipeline completed successfully\n";
    return 0;
  } 
  catch(const std::exception& e){
    std::cerr << "Pipeline failed: " << e.what() << "\n";
    return 1;
  }

  //If internal does not run, return error
  return 1; 
}
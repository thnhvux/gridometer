#include <iostream>
#include <string>

#include "database.hpp"
#include "csvparser.hpp"
#include "analytic.hpp"

int main() {
    std::cout << "=== Gridometer Pipeline Started ===" << std::endl;

    std::cout << "[1/3] Initializing SQLite database..." << std::endl;
    Database db("grid_data.db");
    db.initSchema();

    std::string data_file = "../data/raw/PUB_Demand.csv"; 
    std::cout << "[2/3] Ingesting CSV data from: " << data_file << std::endl;
    CSVParser::csvIngest(data_file, db);

    std::cout << "[3/3] Running technical analysis and generating forecast..." << std::endl;
    
    Analytic::generateForecast(db.getDB(), "Forecast_7Days.csv");

    std::cout << "=== Pipeline Execution Finished ===" << std::endl;
    return 0;
}
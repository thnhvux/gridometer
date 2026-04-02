#pragma once
#ifndef PIPELINE_HPP
#define PIPELINE_HPP

#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sstream>
#include <vector>

#include "pugixml.hpp"
#include "sqlite3.h"

class Pipeline{
  private:
    sqlite3* db;
    std::string db_name;
    std::string csv_name;

  public:
    void initDBSchema(){
      std::string sql = R"sql(
        CREATE TABLE IF NOT EXISTS DEMAND_HISTORY (
          DATE TEXT NOT NULL,
          HOUR INTEGER NOT NULL,
          DEMAND REAL NOT NULL,
          PRIMARY KEY (DATE, HOUR)
        );

        CREATE TABLE IF NOT EXISTS ADEQUACY (
          DATE TEXT NOT NULL,
          HOUR INTEGER NOT NULL,
          FORECAST REAL NOT NULL,
          AVERAGE REAL,
          SOURCE_FILE TEXT,
          PRIMARY KEY (DATE, HOUR, SOURCE_FILE)
        );

        CREATE TABLE IF NOT EXISTS JOINED_OUTPUT (
          DATE TEXT NOT NULL,
          HOUR INTEGER NOT NULL,
          DEMAND REAL NOT NULL,
          FORECAST REAL NOT NULL,
          PRIMARY KEY (DATE, HOUR)
        );

        CREATE TABLE IF NOT EXISTS FEATURE_OUTPUT (
          DATE TEXT NOT NULL,
          HOUR INTEGER NOT NULL,
          DEMAND REAL NOT NULL,
          FORECAST REAL NOT NULL,
          LAG1 REAL,
          LAG24 REAL,
          LAG168 REAL,
          EMA24 REAL,
          MACD REAL,
          MACD_HIST REAL,
          RSI14 REAL,
          PERCENT_B REAL,
          BANDWIDTH REAL,
          PRIMARY KEY (DATE, HOUR)
        );
      )sql";

        char* err_msg;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK){
          std::cerr << "Unable to create schema `" << sql << "`: " << err_msg;
          sqlite3_free(err_msg);
          throw std::runtime_error(err_msg);
        }
    }

    //Abstract constructor and destructor
    Pipeline(const std::string& db_path, const std::string& csv_output = ""): db(nullptr), db_name(db_path), csv_name(csv_output){
      if (sqlite3_open(db_name.c_str(), &db) != SQLITE_OK) {
          std::string err = sqlite3_errmsg(db);
          sqlite3_close(db);
          db = nullptr;
          throw std::runtime_error("Failed to open database: " + err);
      }
    }
    sqlite3* getDB() const {
        return db;
    }
    ~Pipeline(){if (db){sqlite3_close(db);}}

    void ingestDemandCSV(const std::string& csv_path){
      std::ifstream in(csv_path);
      if (!in.is_open()){throw std::runtime_error("Could not open historical demand file: " + csv_path);}
      std::string line;
      //Skip the first 3 lines
      for (int i = 0; i < 3; i++){std::getline(in, line);}
      //Get the first line containing column name
      //Date,Hour,Market Demand,Ontario Demand
      std::getline(in, line);
      std::string sql = 
        "INSERT OR REPLACE INTO DEMAND_HISTORY (DATE, HOUR, DEMAND) "
        "VALUES (?, ?, ?);";

      sqlite3_stmt* stmt = nullptr;
      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK){
        std::string err = std::format("Unable to prepare statement `{}`: {}", sql, sqlite3_errmsg(db));
        throw std::runtime_error(err);
      }

      char* err_msg;
      sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &err_msg);

      while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string date, hour_text, market_demand, ontario_demand;

        std::getline(ss, date, ',');
        std::getline(ss, hour_text, ',');
        std::getline(ss, market_demand, ',');
        std::getline(ss, ontario_demand, ',');

        if (date.empty() || hour_text.empty() || ontario_demand.empty()) continue;
        if (sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK){throw std::runtime_error("Failed to bind date");}
        if (sqlite3_bind_int(stmt, 2, std::stoi(hour_text)) != SQLITE_OK){throw std::runtime_error("Failed to bind hour");}
        if (sqlite3_bind_double(stmt, 3, std::stod(ontario_demand)) != SQLITE_OK){throw std::runtime_error("Failed to bind Ontario Demand");}

        if (sqlite3_step(stmt) != SQLITE_DONE){std::cerr << "Step failed: " << sqlite3_errmsg(db) << "\n";}
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
      }
      sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &err_msg);
      sqlite3_finalize(stmt);
    }

    void ingestAdequacyFile(const std::filesystem::path& xml_path){
      if (!db) return;

      pugi::xml_document xml_doc;
      pugi::xml_parse_result result = xml_doc.load_file(xml_path.string().c_str());

      if (!result){throw std::runtime_error("Failed to parse XML: " + xml_path.string());}

      pugi::xml_node root = xml_doc.child("Document");
      pugi::xml_node body = root.child("DocBody");
      std::string date = body.child_value("DeliveryDate");
      if (date.empty()){throw std::runtime_error("Missing DeliveryDate in " + xml_path.string());}

      struct HourRow{
        double forecast;
        double average;
      };

      std::vector<HourRow> hours(25);
      pugi::xml_node ont_demand = body.child("ForecastDemand").child("OntarioDemand");

      for (pugi::xml_node demand : ont_demand.child("ForecastOntDemand").children("Demand")){
        int hour = std::stoi(demand.child_value("DeliveryHour"));
        double val = std::stod(demand.child_value("EnergyMW"));
        if (hour >= 1 && hour <= 24){hours[hour].forecast = val;}
      }

      for (pugi::xml_node demand : ont_demand.child("AverageDemand").children("Demand")){
        int hour = std::stoi(demand.child_value("DeliveryHour"));
        std::string average_demand = demand.child_value("EnergyMW");
        if (!average_demand.empty() && hour >= 1 && hour <= 24){hours[hour].average = std::stod(average_demand);}
      }

      std::string src_name = xml_path.filename().string();
      std::string sql = 
      "INSERT OR REPLACE INTO ADEQUACY "
      "(DATE, HOUR, FORECAST, AVERAGE, SOURCE_FILE) "
      "VALUES (?, ?, ?, ?, ?);";

      sqlite3_stmt* stmt = nullptr;
      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK){
        std::string err = std::format("Unable to prepare statement `{}`: {}", sql, sqlite3_errmsg(db));
        throw std::runtime_error(err);
      }
      
      for (int h = 1; h <= 24; h++){
        if (sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK){throw std::runtime_error("Failed to bind date");}
        if (sqlite3_bind_int(stmt, 2, h) != SQLITE_OK){throw std::runtime_error("Failed to bind hour");}
        if (sqlite3_bind_double(stmt, 3, hours[h].forecast) != SQLITE_OK){throw std::runtime_error("Failed to bind forecast");}
        if (sqlite3_bind_double(stmt, 4, hours[h].average) != SQLITE_OK){throw std::runtime_error("Failed to bind average");}
        if (sqlite3_bind_text(stmt, 5, src_name.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK){throw std::runtime_error("Failed to bind source name");}

        //Continue the loop 
        if (sqlite3_step(stmt) != SQLITE_DONE){std::cerr << "Step failed: " << sqlite3_errmsg(db) << "\n";}
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
      }
      sqlite3_finalize(stmt);
    }

    void ingestAdequacyDir(const std::string& adequacy_dir){
      if (!db) return;

      for (const auto& entry : std::filesystem::directory_iterator(adequacy_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".xml") {
          ingestAdequacyFile(entry.path());
        }
      }
    }

    void exportToCSV(){
      if (!db) return;

      std::ofstream csv_file(csv_name, std::ios::binary);
      if (!csv_file.is_open()){std::cerr << "Failed to open CSV to export\n"; return;}
      csv_file << "Date,Hour,Forecast Ontario Demand,Average Forecast\n";

      std::string sql = "SELECT DATE, HOUR, FORECAST, AVERAGE FROM ADEQUACY ORDER BY DATE ASC, HOUR ASC";
      sqlite3_stmt* stmt;

      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK){
        std::cerr << "Unable to prepare database to export\n";
        sqlite3_finalize(stmt);
        csv_file.close();
        return;
      }
      
      while(sqlite3_step(stmt) == SQLITE_ROW){
        csv_file << reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)) << "," 
        << sqlite3_column_int(stmt, 1) << "," << sqlite3_column_double(stmt, 2) << ","
        << sqlite3_column_double(stmt, 3) << "\n";
      }

      sqlite3_finalize(stmt);
      csv_file.close();
    }
};

#endif
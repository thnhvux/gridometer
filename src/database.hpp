#pragma once
#ifndef DATABASE_HPP
#define DATABASE_HPP

#include <iostream>
#include <string>

#include <sqlite3.h>

class Database{
  private:
    sqlite3* db_;
  
  public:
    Database(const std::string& db_path){
      if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK){
        std::cerr << "Fatal" << sqlite3_errmsg(db_) << "\n";
        exit(1);
      }
    }

    ~Database(){sqlite3_close(db_);}

    void initSchema(){
      const char* sql = 
        "CREATE TABLE IF NOT EXISTS DemandData("
        "region TEXT, "
        "utc_time TEXT, "
        "local_time TEXT, "
        "timezone TEXT, "
        "demand REAL"
        ");";

      char* _err_msg = nullptr;
      if (sqlite3_exec(db_, sql, nullptr, nullptr, &_err_msg) != SQLITE_OK){
        std::cerr << "SQL error" << _err_msg << "\n";
        sqlite3_free(_err_msg);
      }
      else std::cout << "Success\n"; //<> Log
    }

    void insertDataRow(const std::string& region, const std::string& utc, const std::string& local, const std::string& tz, double demand){
      const char* sql = "INSERT INTO DemandData (region, utc_time, local_time, timezone, demand) VALUES (?, ?, ?, ?, ?);";
      sqlite3_stmt* stmt;

      if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) == SQLITE_OK){
        sqlite3_bind_text(stmt, 1, region.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, utc.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, local.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, tz.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(stmt, 5, demand);

        sqlite3_step(stmt);
      }

      sqlite3_finalize(stmt);
    }

    sqlite3* getDB() { return db_; }
};

#endif // DATABASE_HPP
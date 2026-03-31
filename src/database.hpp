#pragma once
#ifndef DATABASE_HPP
#define DATABASE_HPP

#include <iostream>
#include <stdexcept>
#include <string>

#include "sqlite3.h"

class Database {
  private:
    sqlite3* db_ = nullptr;

  public:
    explicit Database(const std::string& db_file) {
      if (sqlite3_open(db_file.c_str(), &db_) != SQLITE_OK) {
        std::string msg = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        throw std::runtime_error("Failed to open SQLite database: " + msg);
      }
    }

    ~Database() {
      if (db_ != nullptr) {
        sqlite3_close(db_);
        db_ = nullptr;
      }
    }

    sqlite3* getDB() const {
      return db_;
    }

    void initSchema() {
      const char* sql = R"(
        CREATE TABLE IF NOT EXISTS DemandData (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          region TEXT NOT NULL,
          utc_time TEXT NOT NULL,
          local_time TEXT NOT NULL,
          timezone TEXT NOT NULL,
          demand REAL NOT NULL
        );
      )";

      char* err_msg = nullptr;
      int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err_msg);
      if (rc != SQLITE_OK) {
        std::string msg = err_msg ? err_msg : "unknown schema error";
        sqlite3_free(err_msg);
        throw std::runtime_error("Failed to initialize schema: " + msg);
      }

      std::cout << "Success\n";
    }

    void insertDataRow(const std::string& region, const std::string& utc_time, const std::string& local_time, const std::string& timezone, double demand) {
      const char* sql =
          "INSERT INTO DemandData (region, utc_time, local_time, timezone, demand) "
          "VALUES (?, ?, ?, ?, ?);";

      sqlite3_stmt* stmt = nullptr;
      int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
      if (rc != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare insert statement.");
      }

      sqlite3_bind_text(stmt, 1, region.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 2, utc_time.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 3, local_time.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 4, timezone.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_double(stmt, 5, demand);

      rc = sqlite3_step(stmt);
      if (rc != SQLITE_DONE) {
        std::string msg = sqlite3_errmsg(db_);
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to insert row: " + msg);
      }

      sqlite3_finalize(stmt);
    }
};

#endif
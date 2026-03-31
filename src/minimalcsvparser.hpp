#pragma once
#ifndef MINIMALCSVPARSER_HPP
#define MINIMALCSVPARSER_HPP

#include <cctype>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "database.hpp"

class MINCSVParser {
  private:
    static std::string trim(const std::string& s) {
      size_t start = 0;
      while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
      }

      size_t end = s.size();
      while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
      }

      return s.substr(start, end - start);
    }

    static std::vector<std::string> splitCSVLine(const std::string& line) {
      std::vector<std::string> cols;
      std::stringstream ss(line);
      std::string cell;

      while (std::getline(ss, cell, ',')) {
        cols.push_back(trim(cell));
      }

      return cols;
    }

    static std::string twoDigit(int x) {
      std::ostringstream out;
      out << std::setw(2) << std::setfill('0') << x;
      return out.str();
    }

  public:
    static void csvIngest(const std::string& file_path, Database& db) {
      std::ifstream file(file_path);
      if (!file.is_open()) {
        std::cerr << "ERROR: could not open file: " << file_path << "\n";
        return;
      }

      std::string line;
      bool found_header = false;

      while (std::getline(file, line)) {
        line = trim(line);

        if (line == "Date,Hour,Market Demand,Ontario Demand") {
          found_header = true;
          std::cout << "Found header\n";
          break;
        }
      }

      if (!found_header) {
        std::cerr << "ERROR: header not found.\n";
        return;
      }

      int rows_ingested = 0;
      int rows_skipped = 0;
      int debug_prints = 0;

      while (std::getline(file, line)) {
        line = trim(line);

        if (line.empty()) {
          continue;
        }

        std::vector<std::string> cols = splitCSVLine(line);

        if (cols.size() != 4) {
          ++rows_skipped;
          if (debug_prints < 10) {
            std::cout << "Skipped row (wrong column count): " << line << "\n";
            ++debug_prints;
          }
          continue;
        }

        try {
          const std::string date_in = cols[0];
          const int hour_raw = std::stoi(cols[1]);

          if (hour_raw < 1 || hour_raw > 24) {
            ++rows_skipped;
            if (debug_prints < 10) {
              std::cout << "Skipped row (bad hour): " << line << "\n";
              ++debug_prints;
            }
            continue;
          }

          const int hour_zero_based = hour_raw - 1;

          std::string demand_text = cols[3];
          if (demand_text.empty()) {
            demand_text = cols[2];
          }

          if (demand_text.empty()) {
            ++rows_skipped;
            if (debug_prints < 10) {
              std::cout << "Skipped row (empty demand): " << line << "\n";
              ++debug_prints;
            }
            continue;
          }

          const double demand = std::stod(demand_text);
          const std::string hh = twoDigit(hour_zero_based);

          const std::string local_time = date_in + "T" + hh + ":00:00-05:00";
          const std::string utc_time   = date_in + "T" + hh + ":00:00Z";

          db.insertDataRow("Ontario", utc_time, local_time, "EST", demand);
          ++rows_ingested;
        }
        catch (const std::exception& e) {
          ++rows_skipped;
          if (debug_prints < 10) {
            std::cout << "Skipped row (exception: " << e.what() << "): " << line << "\n";
            ++debug_prints;
          }
        }
      }

      std::cout << "Rows ingested: " << rows_ingested << "\n";
      std::cout << "Rows skipped: " << rows_skipped << "\n";
    }
};

#endif
#pragma once
#ifndef ANALYTIC_HPP
#define ANALYTIC_HPP

#include <cmath>
#include <fstream>
#include <format>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "database.hpp"
#include "indicator.hpp"

class Analytic{
  public:
    static void generateForecast(sqlite3* db, const std::string& f_out){
      std::vector<double> history_contents;

      const char* sql = "SELECT demand FROM DemandData ORDER BY ROWID;";
      sqlite3_stmt* stmt;
      if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK){
        while(sqlite3_step(stmt) == SQLITE_ROW) history_contents.push_back(sqlite3_column_double(stmt, 0));
      }
      sqlite3_finalize(stmt);

      //7 days forecast = 7*24=168 rows
      //Safe checking for forecast
      const int kForecastWindowInDay = 7;
      const int kForecastWindowInHour = kForecastWindowInDay*24; 
      if (history_contents.size() < kForecastWindowInHour){
        std::cerr << "Error";
        return;
      }

      //Anomaly detection using z-score
      //z = (x - mu)/sigma
      //with mu = mean; sigma = standard deviation
      double sum = 0;
      for (const double val : history_contents) sum += val;
      double mean = sum / history_contents.size();
      double sq_sum = 0;
      for (const double val : history_contents) sq_sum += std::pow((val-mean), 2);
      double std_dev = std::sqrt(sq_sum/history_contents.size());
      int anomaly_cnt = 0;
      for (const double val : history_contents){
        double z_score = std::abs((val-mean)/std_dev);
        if (z_score > 2.5) anomaly_cnt++;
      }

      /*
      std::filesystem::path current_path = std::filesystem::current_path();
      std::filesystem::path root_path = current_path.parent_path().parent_path();
      const std::string export_dir_name = root_path.string() + "/export";
      std::filesystem::path export_dir{export_dir_name};
      */

      std::ofstream out(f_out);
      if (!out.is_open()) throw std::runtime_error("Could not open file: " + f_out);
      out << "Hour,Forecast Ontario Demand\n";

      size_t start_idx = history_contents.size() - kForecastWindowInHour;
      for (int i = 0; i < kForecastWindowInHour; i++){
        double forecast_val = history_contents[start_idx+i];
        out << std::format("{},{}\n", (i+1), forecast_val); 
      }
      out.close();

      Indicator::MACD macd = Indicator::calculateMACD(history_contents);
      std::vector<double> rsi = Indicator::calculateRSI(history_contents, 14);
      Indicator::BoillingerBand boillinger_band = Indicator::calculateBoillingerBand(history_contents, 20);

      //Console output
      std::cout << "\nDaily Report\n";
      std::cout << std::format("Total Hours: {}\n", history_contents.size());
      std::cout << std::format("Average Ontario Demand: {}\n", mean);
      std::cout << std::format("Anomaly point: {}\n", anomaly_cnt);
      std::cout << std::format("Forecast for next {} days ({} hours)\n", kForecastWindowInDay, kForecastWindowInHour);
    }
};

#endif
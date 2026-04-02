#pragma once
#ifndef ANALYTIC_HPP
#define ANALYTIC_HPP

#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <cmath>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "pipeline.hpp"
#include "indicator.hpp"


class Analytic{ 
  private:
  /*
    struct JoinedPoint{
      std::string date;
      int hour;
      double demand;
      double adequacy_forecast;
    };
  */

    struct DemandPoint{
      std::string date;
      int hour = 0;
      double demand = 0.0;
    };

    struct AdequacyPoint{
      std::string date;
      int hour = 0;
      double forecast = 0.0;
      double average = 0.0;
    };

    struct CalculatedForecastRow{
      std::string date;
      int hour = 0;
      double forecast = 0.0;
    };

    struct FeatureRow{
      std::string date;
      int hour;
      double demand;
      double adequacy_forecast;
      double calculated_forecast;

      double lag1;
      double lag24;
      double lag168;
      double ema24;
      double macd;
      double macd_hist;
      double rsi14;
      double percent_b;
      double bandwidth;
    };

    struct ComparisonRow{
      std::string date;
      int hour;
      double demand;
      double adequacy_forecast;
      double calculated_forecast;
      double abs_diff; //abs(calculated-adequacy)
      double pct_diff; //%diff vs adequacy
    };

  public: 
  /*
    static std::vector<JoinedPoint> loadJoinedRows(sqlite3* db){
      std::vector<JoinedPoint> rows;
      std::string sql = R"sql(
        SELECT D.DATE, D.HOUR, D.DEMAND, A.FORECAST
        FROM DEMAND_HISTORY D
        JOIN ADEQUACY A
          ON D.DATE = A.DATE AND D.HOUR = A.HOUR
        ORDER BY D.DATE ASC, D.HOUR ASC;
      )sql";

      sqlite3_stmt* stmt = nullptr;
      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK){
        std::string err = std::format("Unable to prepare statement `{}`: {}", sql, sqlite3_errmsg(db));
        throw std::runtime_error(err);
      }
      while (sqlite3_step(stmt) == SQLITE_ROW){
        JoinedPoint row;
        row.date = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        row.hour = sqlite3_column_int(stmt, 1);
        row.demand = sqlite3_column_double(stmt, 2);
        row.adequacy_forecast = sqlite3_column_double(stmt, 3);
        rows.push_back(row);
      }

      sqlite3_finalize(stmt);
      return rows;
    }
    */

    static std::vector<DemandPoint> loadDemandHistory(sqlite3* db){
      std::vector<DemandPoint> rows;

      std::string sql = R"sql(
          SELECT DATE, HOUR, DEMAND
          FROM DEMAND_HISTORY
          ORDER BY DATE ASC, HOUR ASC;
      )sql";

      sqlite3_stmt* stmt = nullptr;
      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {throw std::runtime_error("Failed to load demand history");}

      while (sqlite3_step(stmt) == SQLITE_ROW) {
          DemandPoint row;
          row.date = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
          row.hour = sqlite3_column_int(stmt, 1);
          row.demand = sqlite3_column_double(stmt, 2);
          rows.push_back(row);
      }

      sqlite3_finalize(stmt);
      return rows;
    }

    static std::vector<AdequacyPoint> loadAdequacyRows(sqlite3* db) {
      std::vector<AdequacyPoint> rows;

      std::string sql = R"sql(
        SELECT DATE, HOUR, FORECAST, AVERAGE
        FROM ADEQUACY
        ORDER BY DATE ASC, HOUR ASC;
      )sql";

      sqlite3_stmt* stmt = nullptr;
      if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {throw std::runtime_error("Failed to load adequacy rows");}

      while (sqlite3_step(stmt) == SQLITE_ROW) {
        AdequacyPoint row;
        row.date = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        row.hour = sqlite3_column_int(stmt, 1);
        row.forecast = sqlite3_column_double(stmt, 2);
        row.average = sqlite3_column_double(stmt, 3);
        rows.push_back(row);
      }

      sqlite3_finalize(stmt);
      return rows;
    }

    static double calculateNextHourForecast(const std::vector<double>& history){
      if (history.size() < 168){throw std::runtime_error("Need at least 168 historical demand data to forecast");}

      const size_t n = history.size();
      const double lag24 = history[n-24];
      const double lag168 = history[n-168];

      auto ema24_series = Indicator::calculateEMA(history, 24);
      auto macd_series = Indicator::calculateMACD(history);
      auto rsi14_series = Indicator::calculateRSI(history, 14);
      auto bb20_series = Indicator::calculateBoillingerBand(history, 20);

      const double ema24 = ema24_series.back();
      const double macd_hist = macd_series.hist.back();
      const double rsi14 = rsi14_series.back();
      const double percent_b = bb20_series.percent_b.back();

      double forecast = 0.60*lag24+0.30*lag168+0.10*ema24;

      if (rsi14 > 70.0) forecast *= 0.995;
      else if (rsi14 < 30) forecast *= 1.005;
      
      if (macd_hist > 0.0) forecast *= 1.002;
      else if (macd_hist < 0.0) forecast *= 0.998;

      if (percent_b > 0.95) forecast *= 0.997;
      else if (percent_b < 0.05) forecast *= 1.003;

      return std::max(0.0, forecast);
    }

    static std::vector<CalculatedForecastRow> generateCalculatedForecastRows(const std::vector<DemandPoint>& demand_history, const std::vector<AdequacyPoint>& adequacy_rows){
      std::vector<CalculatedForecastRow> output;
      std::vector<double> history;

      for (const auto& row : demand_history){history.push_back(row.demand);}

      for (const auto& a : adequacy_rows){
        double next_forecast = calculateNextHourForecast(history);

        CalculatedForecastRow out;
        out.date = a.date;
        out.hour = a.hour;
        out.forecast = next_forecast;
        output.push_back(out);

        history.push_back(next_forecast);
      }
      return output;
    }

    static double computePCTDifference(double calculated, double adequacy){
      if (adequacy == 0.0) return 0.0;
      return ((calculated-adequacy)/adequacy)*100.0;
    }

    static std::vector<ComparisonRow> compareCalculatedToAdequacy(const std::vector<CalculatedForecastRow>& calculated, const std::vector<AdequacyPoint>& adequacy) {
      if (calculated.size() != adequacy.size()){throw std::runtime_error("Calculated and adequacy forecast sizes do not match");}

      std::vector<ComparisonRow> output;

      for (size_t i = 0; i < adequacy.size(); ++i){
        ComparisonRow row;
        row.date = adequacy[i].date;
        row.hour = adequacy[i].hour;
        row.adequacy_forecast = adequacy[i].forecast;
        row.calculated_forecast = calculated[i].forecast;
        row.abs_diff = row.calculated_forecast - row.adequacy_forecast;
        row.pct_diff = computePCTDifference(row.calculated_forecast, row.adequacy_forecast);
        output.push_back(row);
      }
      return output;
    }

    /*
    static std::vector<ComparisonRow> buildComparisonRows(const std::vector<JoinedPoint>& rows){
      std::vector<ComparisonRow> outputs;
      std::vector<double> histories;

      for (const auto& row : rows){
        if(histories.size() >= 168){
          double calculated = calculateNextHourForecast(histories);

          ComparisonRow out;
          out.date = row.date;
          out.hour = row.hour;
          out.demand = row.demand;
          out.adequacy_forecast = row.adequacy_forecast;
          out.calculated_forecast = calculated;
          out.abs_diff = std::abs(calculated - row.adequacy_forecast);
          out.pct_diff = computePCTDifference(calculated, row.adequacy_forecast);

          outputs.push_back(out);
        }

        histories.push_back(row.demand);
      }

      return outputs;
    }
    */

    static FeatureRow buildFeatureRow(const ComparisonRow& current, const std::vector<double>& history) {
      if (history.size() < 168) {throw std::runtime_error("Need at least 168 rows of history");}

      auto ema24 = Indicator::calculateEMA(history, 24);
      auto macd = Indicator::calculateMACD(history);
      auto rsi14 = Indicator::calculateRSI(history, 14);
      auto bb20 = Indicator::calculateBoillingerBand(history, 20);

      const size_t n = history.size();

      FeatureRow out;
      out.date = current.date;
      out.hour = current.hour;
      out.demand = current.demand;
      out.adequacy_forecast = current.adequacy_forecast;
      out.calculated_forecast = current.calculated_forecast;

      out.lag1 = history[n - 1];
      out.lag24 = history[n - 24];
      out.lag168 = history[n - 168];
      out.ema24 = ema24.back();
      out.macd = macd.macd.back();
      out.macd_hist = macd.hist.back();
      out.rsi14 = rsi14.back();
      out.percent_b = bb20.percent_b.back();
      out.bandwidth = bb20.bandwitdth.back();

      return out;
    }

    static std::vector<FeatureRow> buildFeatureRows(const std::vector<ComparisonRow>& rows) {
      std::vector<FeatureRow> output;
      std::vector<double> history;

      for (const auto& row : rows) {
        if (history.size() >= 168) {output.push_back(buildFeatureRow(row, history));}
        history.push_back(row.demand);
      }

      return output;
    }

    static void exportComparisonCSV(const std::vector<ComparisonRow> rows, const std::filesystem::path& out_path){
      std::ofstream out(out_path);
      if (!out.is_open()){throw std::runtime_error("Failed to open " + out_path.string());}
      out << "Date,Hour,ActualDemand,AdequacyForecast,CalculatedForecast,AbsDiff,PctDiff\n";

      for (const auto& row : rows){
        out << row.date << "," << row.hour << "," << row.demand << ","
        << row.adequacy_forecast << "," << row.calculated_forecast << ","
        << row.abs_diff << "," << row.pct_diff << "\n";
      }
    }

    /*
    static void exportJoinedCSV(const std::vector<JoinedPoint>& rows, const std::filesystem::path& out_path){
      std::ofstream out(out_path);
      if (!out.is_open()){throw std::runtime_error("Failed to open " + out_path.string());}
      out << "Date,Hour,Demand,Forecast\n";

      for (const auto& row : rows){
        out << row.date << "," << row.hour << "," << row.demand << "," << row.adequacy_forecast << "\n";
      }
    }
    */

    static void exportFeatureCSV(const std::vector<FeatureRow>& rows, const std::filesystem::path& out_path){
      std::ofstream out(out_path);
      if (!out.is_open()){throw std::runtime_error("Failed to open " + out_path.string());}
      out << "Date,Hour,Demand,AdequacyForecast,CalculatedForecast,Lag1,Lag24,Lag168,EMA24,MACD,MACD_HIST,RSI14,PERCENT_B,BANDWIDTH\n";

      for (const auto& row : rows){
        out << row.date << "," << row.hour << "," << row.demand << "," << row.adequacy_forecast
        << "," << row.calculated_forecast << "," << "," << row.lag1 << "," << row.lag24 
        << "," << row.lag168 << "," << row.ema24 << "," << row.macd << "," << row.macd_hist 
        << "," << row.rsi14 << "," << row.percent_b << "," << row.bandwidth << "\n";
      }
    }

    /*
    static void runAnalysis(sqlite3* db, const std::string& processed_dir){
      std::filesystem::create_directories(processed_dir);

      auto joined_rows = loadJoinedRows(db);
      if (joined_rows.empty()){throw std::runtime_error("No joined demand/forecast rows found");}
      auto comparison_rows = buildComparisonRows(joined_rows);
      auto feature_rows = buildFeatureRows(comparison_rows);
      exportJoinedCSV(joined_rows, std::filesystem::path(processed_dir)/"demand_forecast.csv");

      auto demand_history = loadDemandHistory(db);
      auto adequacy_rows  = loadAdequacyRows(db);

      auto calculated_rows = generateCalculatedForecastRows(
          demand_history,
          adequacy_rows.size(),
          adequacy_rows.front().date
      );

      auto comparison_rows = compareCalculatedToAdequacy(
          calculated_rows,
          adequacy_rows
      );
      exportComparisonCSV(comparison_rows, std::filesystem::path(processed_dir)/"forecast_comparison.csv");
      exportFeatureCSV(feature_rows, std::filesystem::path(processed_dir)/"feature_rows.csv");

      //std::cout << "Joined rows: " << joined_rows.size() << "\n";
      std::cout << "Comparison rows: " << comparison_rows.size() << "\n";
      std::cout << "Feature rows: " << feature_rows.size() << "\n";
    }
    */

    static void runAnalysis(sqlite3* db, const std::string& processed_dir) {
      std::filesystem::create_directories(processed_dir);

      auto demand_history = loadDemandHistory(db);
      if (demand_history.empty()) {throw std::runtime_error("No demand history rows found");}

      auto adequacy_rows = loadAdequacyRows(db);
      if (adequacy_rows.empty()) {throw std::runtime_error("No adequacy forecast rows found");}

      auto calculated_rows = generateCalculatedForecastRows(demand_history, adequacy_rows);
      auto comparison_rows = compareCalculatedToAdequacy(calculated_rows, adequacy_rows);

      exportComparisonCSV(comparison_rows, std::filesystem::path(processed_dir) / "forecast_comparison.csv");
    }
};

#endif
#pragma once
#ifndef ANALYTIC_HPP
#define ANALYTIC_HPP

#include <cmath>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "database.hpp"
#include "indicator.hpp"


class Analytic{
  private:
    struct DemandPoint{
    std::string region;
    std::string utc_time;
    std::string local_time;
    std::string timezone;
    double demand;
  };

  struct FeaturePoint{
    double lag1 = 0.0;
    double lag24 = 0.0;
    double lag168 = 0.0;
    double ema24 = 0.0;
    double macd = 0.0;
    double macd_hist = 0.0;
    double rsi14 = 0.0;
    double percent_b = 0.0;
    double bandwidth = 0.0;
  };

  struct ForecastPoint{
    std::string region;
    std::string utc_time;
    std::string local_time;
    std::string timezone;
    double forecast_demand = 0.0;
  };

  //Helper
  static std::vector<DemandPoint> loadDemandHistory(sqlite3* db){
    std::vector<DemandPoint> history_demands;
    const char* sql = 
    "SELECT region, utc_time, local_time, timezone, demand "
    "FROM DemandData ORDER BY utc_time";
    
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
      throw std::runtime_error("Could not get demand history\n");

    while (sqlite3_step(stmt) == SQLITE_ROW){
      DemandPoint data_row;
      //Convert const unsigned char* to string
      data_row.region = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
      data_row.utc_time = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
      data_row.local_time = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
      data_row.timezone = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
      data_row.demand = sqlite3_column_double(stmt, 4);

      history_demands.push_back(data_row);
    }

    sqlite3_finalize(stmt);
    return history_demands;
  }

  static std::chrono::local_time<std::chrono::seconds> getLocalTime(const std::string& iso8601_time){
    if (iso8601_time.size() < 19) throw std::runtime_error("Bad format\n");
    //Parser from C++20
    std::istringstream in(iso8601_time.substr(0, 19));
    std::chrono::local_time<std::chrono::seconds> time;
    in >> std::chrono::parse("%FT%T", time);
    if (in.fail()) throw std::runtime_error("Unable to get local time\n");
    
    return time;
  }

  static DemandPoint advanceToNextHourDemand(const DemandPoint& last_hour, int hour){
    const std::chrono::local_time<std::chrono::seconds> current_lt = getLocalTime(last_hour.local_time);
    const std::chrono::local_time<std::chrono::seconds> next_lt = current_lt + std::chrono::hours(hour);

    DemandPoint next_entry;
    next_entry.region = last_hour.region;
    next_entry.timezone = "";
    if (last_hour.timezone.empty()) next_entry.timezone = "EST";
    else next_entry.timezone = last_hour.timezone;
    next_entry.local_time = std::format("{:%FT%T}", next_lt);
    //For simplicity, EST here is always -5
    //and not accounting for daylight saving time
    //Move local time forward by adding 5 to it in chrono
    const std::chrono::sys_seconds next_utc_time{next_lt.time_since_epoch() + std::chrono::hours(5)};
    next_entry.utc_time = std::format("{:%FT%TZ}", next_utc_time);

    return next_entry;
  }

  //Anomaly detection using z-score
    //z = (x - mu)/sigma
    //with mu = mean; sigma = standard deviation
  static int countAnomaly(const std::vector<DemandPoint>& history_demands){
    
    if (history_demands.empty()) return 0;
    double sum = 0;
    for (const DemandPoint point : history_demands) sum += point.demand;
    double mean = sum / history_demands.size();
    double sq_sum = 0;
    for (const DemandPoint& point : history_demands) sq_sum += std::pow((point.demand-mean), 2);
    double std_dev = std::sqrt(sq_sum/history_demands.size());
    
    int anomaly_cnt = 0;

    for (const DemandPoint& point : history_demands){
      double z_score = std::abs((point.demand-mean)/std_dev);
      if (z_score > 2.5) anomaly_cnt++;
    }

    return anomaly_cnt;
  }

  static FeaturePoint generateFeature(const std::vector<double>& series) {
    FeaturePoint feature_point;

    const size_t n = series.size();
    feature_point.lag1 = series[n - 1];
    feature_point.lag24 = series[n - 24];
    feature_point.lag168 = series[n - 168];

    auto calculated_ema24 = Indicator::calculateEMA(series, 24);
    auto calculated_macd = Indicator::calculateMACD(series);
    auto calculated_rsi14 = Indicator::calculateRSI(series, 14);
    auto calculated_bb = Indicator::calculateBoillingerBand(series, 20);

    feature_point.ema24 = calculated_ema24.back();
    feature_point.macd = calculated_macd.macd.back();
    feature_point.macd_hist = calculated_macd.hist.back();
    feature_point.rsi14 = calculated_rsi14.back();
    feature_point.percent_b = calculated_bb.percent_b.back();
    feature_point.bandwidth = calculated_bb.bandwitdth.back();

    return feature_point;
  }

  static double forecastNextHourDemand(const FeaturePoint& point) {
    // Strong seasonal baseline first
    double prediction_point = 0.60 * point.lag24 + 0.30 * point.lag168 + 0.10 * point.ema24;

    if (point.rsi14 > 70.0) prediction_point *= 0.995;
    else if (point.rsi14 < 30.0) prediction_point *= 1.005;

    if (point.macd_hist > 0.0) prediction_point *= 1.002;
    else if (point.macd_hist < 0.0) prediction_point *= 0.998;

    if (point.percent_b > 0.95) prediction_point *= 0.997;
    else if (point.percent_b < 0.05) prediction_point *= 1.003;

    return std::max(0.0, prediction_point);
  }

  //Separate export into multiple .csv files
  static void exportDailyForecast(const std::vector<ForecastPoint>& points, const std::string& out_dir){
    std::filesystem::create_directory(out_dir);

    std::map<std::string, std::vector<ForecastPoint>> data_sorted_by_day;
    //Example local time entry: 2026-01-31T12:00:00-05:00
    for (const ForecastPoint& point : points) 
      data_sorted_by_day[point.local_time.substr(0, 10)].push_back(point);
    //Temp var is in std::map<std::string, std::vector<ForecastPoint>>
    for (const auto& [date, rows] : data_sorted_by_day){
      std::string data_date = date;
      //File name will be Forecast_20260131.csv
      data_date.erase(std::remove(data_date.begin(), data_date.end(), '-'), data_date.end());
      const std::filesystem::path out_path = std::filesystem::path(out_dir)/("Forecast_" + data_date + ".csv");

      std::ofstream out(out_path);
      if (!out.is_open()) throw std::runtime_error("Unable to open export file\n");
      out << "Region,UTC Time,Local Time,Timezone,Ontario Demand,Forecast Ontario Demand\n";
      //Temp var is in std::vector<ForecastPoint>
      for (const auto&row : rows)
        out << row.region << "," << row.utc_time << "," << row.local_time << "," << row.timezone << "," << row.forecast_demand << "\n";
    }
  }
  
  public:
    static void generateForecast(sqlite3* db, const std::string& out_dir){
      const int kForecastWindowInDay = 7;
      const int kForecastWindowInHour = kForecastWindowInDay*24; 
      std::vector<DemandPoint> history_contents = loadDemandHistory(db);
      if (history_contents.size() < kForecastWindowInHour){
        const std::string err_msg = std::format("Not enough history for {}-day forecast", kForecastWindowInDay);
        throw std::runtime_error(err_msg);
      }
      
      std::vector<double> history_demands;
      history_demands.reserve(history_contents.size() + kForecastWindowInHour);
      for (const auto& row : history_contents) history_demands.push_back(row.demand);
      std::vector<ForecastPoint> forecast_out;
      forecast_out.reserve(kForecastWindowInHour);

      DemandPoint last_hour_data = history_contents.back();
      for (size_t i = 0; i < kForecastWindowInHour; i++){
        FeaturePoint feature = generateFeature(history_demands);
        double next_val = forecastNextHourDemand(feature);
        DemandPoint next_hour_data = advanceToNextHourDemand(last_hour_data, i+1);
        forecast_out.push_back(ForecastPoint{
          next_hour_data.region, next_hour_data.utc_time, next_hour_data.local_time, next_hour_data.timezone, next_val
        });
        history_demands.push_back(next_val);
      }

      exportDailyForecast(forecast_out, out_dir);

      //Anomaly checking
      size_t anomaly_cnt = countAnomaly(history_contents);

      //Console output
      double sum = 0.0;
      for (const auto& row : history_contents) sum += row.demand;
      double mean = sum/history_contents.size();

      std::cout << "\nDaily Report\n";
      std::cout << std::format("Total Hours: {}\n", history_contents.size());
      std::cout << std::format("Average Ontario Demand: {}\n", mean);
      std::cout << std::format("Anomaly point: {}\n", anomaly_cnt);
      std::cout << std::format("Forecast for next {} days ({} hours)\n", kForecastWindowInDay, kForecastWindowInHour);
    }
    
};

#endif
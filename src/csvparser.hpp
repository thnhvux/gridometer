#pragma once
#ifndef CSVPARSER_HPP
#define CSVPARSER_HPP

#include <chrono>
#include <fstream>
#include <format>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "database.hpp"

class CSVParser{
  public: 
    static void csvIngest(const std::string& file_path, Database& db){
      std::ifstream file(file_path);
      if (!file.is_open()){
        std::cerr << "Error";
        return;
      }

      std::string data_line;
      bool found_header = false;

      //Skipping comments in file
      while (std::getline(file, data_line)){
        if (data_line.find("Date,Hour") != std::string::npos){
          found_header = true;
          break;
        }
      }
      if (!found_header){
        std::cerr << "Error";
        return;
      }

      //Data ingestion
      int row_ingested = 0;
      while (std::getline(file, data_line)){
        if (data_line.empty()) continue;

        std::stringstream ss(data_line);
        std::string data;
        std::vector<std::string> cols;

        while (std::getline(ss, data, ',')){cols.push_back(data);}

        if (cols.size() >= 4){
          try{
            //Handle bad input
            std::string date_in = cols[0];
            int hour_in = std::stod(cols[1]);
            std::string hour_in_adjusted = std::format("{:02}:00:00",hour_in - 1); 
            
            //Format follows ISO 8601
            //YYYY-MM-DDTHH:mm:ss.sssZ
            std::string local_time = std::format("{}T{}", date_in, hour_in_adjusted);
            //Time conversion
            std::stringstream ss_time(local_time);
            std::chrono::local_time<std::chrono::seconds> lt;
            ss_time >> std::chrono::parse("%FT%T", lt);
            auto zone = std::chrono::locate_zone("America/Toronto");
            auto local_time_conversion_utc = zone->to_sys(lt, std::chrono::choose::earliest);
            
            std::string region_out = "Ontario";
            std::string utc_out = std::format("{:%FT%TZ}", local_time_conversion_utc);
            std::string lt_out = local_time + "-05:00";
            std::string tz_out = "EST";
            double demand_out = std::stod(cols[3]);

            db.insertDataRow(region_out, utc_out, lt_out, tz_out, demand_out);
            row_ingested++;
          } catch (const std::exception& e){
            //Log empty row was passed
          }
          
        }
      }
    }
};

#endif
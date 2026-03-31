#pragma once
#ifndef INDICATOR_HPP
#define INDICATOR_HPP

#include <algorithm>
#include <cmath>
#include <vector>

class Indicator{
  public:
    //MACD = EMA12 - EMA26
    //EMA calculated using exponential moving window (emw) 
    //with span = 12 or 26
    struct MACD{
      std::vector<double> macd;
      std::vector<double> signal;
      std::vector<double> hist;
    };

    static std::vector<double> calculateEMA(const std::vector<double>& data, int span){
      std::vector<double> ema(data.size(), 0);
      if (data.empty()) return ema;

      double alpha = 2.0/(span + 1.0);
      ema[0] = data[0];
      for (size_t i = 1; i < data.size(); i++) 
        ema[i] = (data[i] * alpha) + (ema[i-1] * (1.0-alpha)); 

      return ema;
    }

    static MACD calculateMACD(const std::vector<double>& data){
      MACD res;
      res.macd.resize(data.size(), 0.0);
      res.signal.resize(data.size(), 0.0);
      res.hist.resize(data.size(), 0.0);

      std::vector<double> ema12 = calculateEMA(data, 12);
      std::vector<double> ema26 = calculateEMA(data, 26);

      for (size_t i = 0; i < data.size(); i++)
        res.macd[i] = ema12[i] - ema26[i];

      //MACD signal is emw with span = 9
      res.signal = calculateEMA(res.macd, 9);

      for (size_t i = 0; i < data.size(); i++)
        res.hist[i] = res.macd[i] - res.signal[i];

      return res;
    }

    //14-day RSI
    static std::vector<double> calculateRSI(const std::vector<double>& data, int period = 14){
      std::vector<double> rsi(data.size(), 0.0);
      if (data.size() < 2) return rsi;

      std::vector<double> up_points_14(data.size(), 0.0);
      std::vector<double> down_points_14(data.size(), 0.0);

      for (size_t i = 1; i < data.size(); i++){
        double delta_end_of_day = data[i] - data[i-1];
        if (delta_end_of_day > 0) up_points_14[i] = delta_end_of_day;
        else down_points_14[i] = std::abs(delta_end_of_day);
      }

      //Use Wilder's smoothing method (span = period*2-1) for ewm
      int span = period*2-1;
      std::vector<double> avg_gain_points_14 = calculateEMA(up_points_14, span);
      std::vector<double> avg_down_points_14 = calculateEMA(down_points_14, span);
      //Calculate average points by taking mean of the ewm
      //and removing points that are equal to 0
      for (size_t i = 1; i < data.size(); i++){
        if (avg_down_points_14[i] == 0) rsi[i] = 100;
        else {
          double rs = avg_gain_points_14[i] / avg_down_points_14[i];
          rsi[i] = 100.0 - (100.0/(1.0+rs));
        }
      }

      return rsi;
    }

    //Bollinger Band (20-day SMA)
    struct BoillingerBand{
      std::vector<double> upper = {}, lower = {}, percent_b = {}, bandwitdth = {};
    };

    static BoillingerBand calculateBoillingerBand(const std::vector<double>& data, int window = 20){
      BoillingerBand res;
      size_t data_size = data.size();
      res.upper.resize(data_size, 0.0);
      res.lower.resize(data_size, 0.0);
      res.percent_b.resize(data_size, 0.0);
      res.bandwitdth.resize(data_size, 0.0);

      if (data_size < window) return res;

      for (size_t i = window-1; i < data_size; i++){
        double sum = 0.0;
        for (size_t j = 0; j < window; j++) sum += data[i-j];
        double sma20 = sum / window;
        double vary_sum = 0.0;
        for (size_t j = 0; j < window; j++) vary_sum += std::pow(data[i-j]-sma20,2);
        double std20 = std::sqrt(vary_sum/window);
        double upper_bb = sma20 + (2.0*std20);
        double lower_bb = sma20 - (2.0*std20);

        res.upper[i] = upper_bb;
        res.lower[i] = lower_bb;
        res.percent_b[i] = (data[i] - lower_bb) / (upper_bb - lower_bb);
        res.bandwitdth[i] = (upper_bb - lower_bb) / sma20;
      }

      return res;
    }
};

#endif
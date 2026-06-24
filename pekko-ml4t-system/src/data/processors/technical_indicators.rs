//! 기술적 지표 계산 모듈
//! 
//! 다양한 기술적 지표를 계산합니다:
//! - 이동평균 (SMA, EMA, WMA)
//! - 모멘텀 지표 (RSI, MACD, Stochastic)
//! - 변동성 지표 (Bollinger Bands, ATR)
//! - 거래량 지표 (OBV, ADL)
//! - 기타 지표 (Williams %R, CCI)

use super::{TechnicalIndicatorConfig, ProcessedData, RawData};
use crate::data::MarketData;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{ warn, debug};

/// 기술적 지표 계산기
pub struct TechnicalIndicatorCalculator {
    config: TechnicalIndicatorConfig,
}

impl TechnicalIndicatorCalculator {
    /// 새로운 기술적 지표 계산기 생성
    pub fn new(config: TechnicalIndicatorConfig) -> Self {
        Self { config }
    }

    /// 시장 데이터에서 기술적 지표 계산
    pub fn calculate_indicators(&self, data: &[MarketData]) -> Result<HashMap<String, Vec<f64>>> {
        let mut indicators = HashMap::new();
        
        // 가격 데이터 추출
        let closes: Vec<f64> = data.iter().map(|d| d.close).collect();
        let highs: Vec<f64> = data.iter().map(|d| d.high).collect();
        let lows: Vec<f64> = data.iter().map(|d| d.low).collect();
        let volumes: Vec<u64> = data.iter().map(|d| d.volume).collect();
        
        // 1. 단순 이동평균 (SMA)
        for &period in &self.config.sma_periods {
            let sma_values = self.calculate_sma(&closes, period);
            indicators.insert(format!("sma_{}", period), sma_values);
        }
        
        // 2. 지수 이동평균 (EMA)
        for &period in &self.config.ema_periods {
            let ema_values = self.calculate_ema(&closes, period);
            indicators.insert(format!("ema_{}", period), ema_values);
        }
        
        // 3. RSI (Relative Strength Index)
        let rsi_values = self.calculate_rsi(&closes, self.config.rsi_period);
        indicators.insert("rsi".to_string(), rsi_values);
        
        // 4. MACD (Moving Average Convergence Divergence)
        let macd_result = self.calculate_macd(&closes, self.config.macd_fast, self.config.macd_slow, self.config.macd_signal);
        indicators.insert("macd".to_string(), macd_result.macd_line);
        indicators.insert("macd_signal".to_string(), macd_result.signal_line);
        indicators.insert("macd_histogram".to_string(), macd_result.histogram);
        
        // 5. 볼린저 밴드
        let bb_result = self.calculate_bollinger_bands(&closes, self.config.bollinger_period, self.config.bollinger_std);
        indicators.insert("bb_upper".to_string(), bb_result.upper_band);
        indicators.insert("bb_middle".to_string(), bb_result.middle_band);
        indicators.insert("bb_lower".to_string(), bb_result.lower_band);
        indicators.insert("bb_width".to_string(), bb_result.band_width);
        indicators.insert("bb_position".to_string(), bb_result.price_position);
        
        // 6. ATR (Average True Range)
        let atr_values = self.calculate_atr(&highs, &lows, &closes, self.config.atr_period);
        indicators.insert("atr".to_string(), atr_values);
        
        // 7. Stochastic Oscillator
        let stoch_result = self.calculate_stochastic(&highs, &lows, &closes, self.config.stochastic_k, self.config.stochastic_d);
        indicators.insert("stoch_k".to_string(), stoch_result.k_percent);
        indicators.insert("stoch_d".to_string(), stoch_result.d_percent);
        
        // 8. Williams %R
        let williams_r_values = self.calculate_williams_r(&highs, &lows, &closes, self.config.williams_r_period);
        indicators.insert("williams_r".to_string(), williams_r_values);
        
        // 9. 거래량 지표들
        let obv_values = self.calculate_obv(&closes, &volumes);
        indicators.insert("obv".to_string(), obv_values);
        
        let adl_values = self.calculate_adl(&highs, &lows, &closes, &volumes);
        indicators.insert("adl".to_string(), adl_values);
        
        // 10. 가격 기반 특성들
        let price_features = self.calculate_price_features(data);
        indicators.extend(price_features);
        Ok(indicators)
    }

    /// 단순 이동평균 계산
    fn calculate_sma(&self, prices: &[f64], period: u32) -> Vec<f64> {
        let mut sma_values = vec![f64::NAN; prices.len()];
        
        for i in (period - 1) as usize..prices.len() {
            let sum: f64 = prices[i - (period - 1) as usize..=i].iter().sum();
            sma_values[i] = sum / period as f64;
        }
        
        sma_values
    }

    /// 지수 이동평균 계산
    fn calculate_ema(&self, prices: &[f64], period: u32) -> Vec<f64> {
        let mut ema_values = vec![f64::NAN; prices.len()];
        let multiplier = 2.0 / (period as f64 + 1.0);
        
        if prices.is_empty() {
            return ema_values;
        }
        
        // 첫 번째 값은 SMA로 초기화
        if period as usize <= prices.len() {
            let initial_sum: f64 = prices[0..period as usize].iter().sum();
            ema_values[period as usize - 1] = initial_sum / period as f64;
            
            // EMA 계산
            for i in period as usize..prices.len() {
                ema_values[i] = (prices[i] * multiplier) + (ema_values[i - 1] * (1.0 - multiplier));
            }
        }
        
        ema_values
    }

    /// RSI 계산
    fn calculate_rsi(&self, prices: &[f64], period: u32) -> Vec<f64> {
        let mut rsi_values = vec![f64::NAN; prices.len()];
        
        if prices.len() < period as usize + 1 {
            return rsi_values;
        }
        
        // 가격 변화 계산
        let mut gains = Vec::new();
        let mut losses = Vec::new();
        
        for i in 1..prices.len() {
            let change = prices[i] - prices[i - 1];
            gains.push(change.max(0.0));
            losses.push((-change).max(0.0));
        }
        
        // 초기 평균 계산
        let mut avg_gain: f64 = gains[0..period as usize].iter().sum::<f64>() / period as f64;
        let mut avg_loss: f64 = losses[0..period as usize].iter().sum::<f64>() / period as f64;
        
        if avg_loss == 0.0 {
            rsi_values[period as usize] = 100.0;
        } else {
            let rs = avg_gain / avg_loss;
            rsi_values[period as usize] = 100.0 - (100.0 / (1.0 + rs));
        }
        
        // 나머지 RSI 값들 계산
        for i in (period as usize + 1)..prices.len() {
            avg_gain = ((avg_gain * (period as f64 - 1.0)) + gains[i - 1]) / period as f64;
            avg_loss = ((avg_loss * (period as f64 - 1.0)) + losses[i - 1]) / period as f64;
            
            if avg_loss == 0.0 {
                rsi_values[i] = 100.0;
            } else {
                let rs = avg_gain / avg_loss;
                rsi_values[i] = 100.0 - (100.0 / (1.0 + rs));
            }
        }
        
        rsi_values
    }

    /// MACD 계산
    fn calculate_macd(&self, prices: &[f64], fast_period: u32, slow_period: u32, signal_period: u32) -> MacdResult {
        let ema_fast = self.calculate_ema(prices, fast_period);
        let ema_slow = self.calculate_ema(prices, slow_period);
        
        // MACD 라인 = EMA(빠른) - EMA(느린)
        let macd_line: Vec<f64> = ema_fast.iter().zip(ema_slow.iter())
            .map(|(fast, slow)| {
                if fast.is_nan() || slow.is_nan() {
                    f64::NAN
                } else {
                    fast - slow
                }
            })
            .collect();
        
        // 신호 라인 = MACD 라인의 EMA
        let signal_line = self.calculate_ema(&macd_line, signal_period);
        
        // 히스토그램 = MACD 라인 - 신호 라인
        let histogram: Vec<f64> = macd_line.iter().zip(signal_line.iter())
            .map(|(macd, signal)| {
                if macd.is_nan() || signal.is_nan() {
                    f64::NAN
                } else {
                    macd - signal
                }
            })
            .collect();
        
        MacdResult {
            macd_line,
            signal_line,
            histogram,
        }
    }

    /// 볼린저 밴드 계산
    fn calculate_bollinger_bands(&self, prices: &[f64], period: u32, std_dev: f64) -> BollingerBandsResult {
        let sma = self.calculate_sma(prices, period);
        let mut upper_band = vec![f64::NAN; prices.len()];
        let mut lower_band = vec![f64::NAN; prices.len()];
        let mut band_width = vec![f64::NAN; prices.len()];
        let mut price_position = vec![f64::NAN; prices.len()];
        
        for i in (period - 1) as usize..prices.len() {
            let window = &prices[i - (period - 1) as usize..=i];
            let mean = sma[i];
            
            // 표준편차 계산
            let variance: f64 = window.iter()
                .map(|&x| (x - mean).powi(2))
                .sum::<f64>() / period as f64;
            let std = variance.sqrt();
            
            upper_band[i] = mean + (std * std_dev);
            lower_band[i] = mean - (std * std_dev);
            band_width[i] = upper_band[i] - lower_band[i];
            
            // 가격 위치 (0~1 사이, 0.5는 중간 밴드)
            if band_width[i] > 0.0 {
                price_position[i] = (prices[i] - lower_band[i]) / band_width[i];
            }
        }
        
        BollingerBandsResult {
            upper_band,
            middle_band: sma,
            lower_band,
            band_width,
            price_position,
        }
    }

    /// ATR (Average True Range) 계산
    fn calculate_atr(&self, highs: &[f64], lows: &[f64], closes: &[f64], period: u32) -> Vec<f64> {
        let mut atr_values = vec![f64::NAN; highs.len()];
        
        if highs.len() < 2 {
            return atr_values;
        }
        
        // True Range 계산
        let mut true_ranges = Vec::new();
        
        for i in 1..highs.len() {
            let high_low = highs[i] - lows[i];
            let high_close_prev = (highs[i] - closes[i - 1]).abs();
            let low_close_prev = (lows[i] - closes[i - 1]).abs();
            
            let tr = high_low.max(high_close_prev).max(low_close_prev);
            true_ranges.push(tr);
        }
        
        // ATR 계산 (True Range의 이동평균)
        if true_ranges.len() >= period as usize {
            for i in (period - 1) as usize..true_ranges.len() {
                let sum: f64 = true_ranges[i - (period - 1) as usize..=i].iter().sum();
                atr_values[i + 1] = sum / period as f64;
            }
        }
        
        atr_values
    }

    /// Stochastic Oscillator 계산
    fn calculate_stochastic(&self, highs: &[f64], lows: &[f64], closes: &[f64], k_period: u32, d_period: u32) -> StochasticResult {
        let mut k_percent = vec![f64::NAN; closes.len()];
        
        for i in (k_period - 1) as usize..closes.len() {
            let window_highs = &highs[i - (k_period - 1) as usize..=i];
            let window_lows = &lows[i - (k_period - 1) as usize..=i];
            
            let highest_high = window_highs.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let lowest_low = window_lows.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            
            if highest_high != lowest_low {
                k_percent[i] = ((closes[i] - lowest_low) / (highest_high - lowest_low)) * 100.0;
            }
        }
        
        // %D 라인 (%K의 이동평균)
        let d_percent = self.calculate_sma(&k_percent, d_period);
        
        StochasticResult {
            k_percent,
            d_percent,
        }
    }

    /// Williams %R 계산
    fn calculate_williams_r(&self, highs: &[f64], lows: &[f64], closes: &[f64], period: u32) -> Vec<f64> {
        let mut williams_r = vec![f64::NAN; closes.len()];
        
        for i in (period - 1) as usize..closes.len() {
            let window_highs = &highs[i - (period - 1) as usize..=i];
            let window_lows = &lows[i - (period - 1) as usize..=i];
            
            let highest_high = window_highs.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let lowest_low = window_lows.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            
            if highest_high != lowest_low {
                williams_r[i] = ((highest_high - closes[i]) / (highest_high - lowest_low)) * -100.0;
            }
        }
        
        williams_r
    }

    /// OBV (On-Balance Volume) 계산
    fn calculate_obv(&self, closes: &[f64], volumes: &[u64]) -> Vec<f64> {
        let mut obv = vec![0.0; closes.len()];
        
        for i in 1..closes.len() {
            if closes[i] > closes[i - 1] {
                obv[i] = obv[i - 1] + volumes[i] as f64;
            } else if closes[i] < closes[i - 1] {
                obv[i] = obv[i - 1] - volumes[i] as f64;
            } else {
                obv[i] = obv[i - 1];
            }
        }
        
        obv
    }

    /// ADL (Accumulation/Distribution Line) 계산
    fn calculate_adl(&self, highs: &[f64], lows: &[f64], closes: &[f64], volumes: &[u64]) -> Vec<f64> {
        let mut adl = vec![0.0; closes.len()];
        
        for i in 0..closes.len() {
            let high_low_range = highs[i] - lows[i];
            
            if high_low_range > 0.0 {
                let money_flow_multiplier = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / high_low_range;
                adl[i] = if i == 0 { 0.0 } else { adl[i - 1] } + (money_flow_multiplier * volumes[i] as f64);
            } else {
                adl[i] = if i == 0 { 0.0 } else { adl[i - 1] };
            }
        }
        
        adl
    }

    /// 가격 기반 특성 계산
    fn calculate_price_features(&self, data: &[MarketData]) -> HashMap<String, Vec<f64>> {
        let mut features = HashMap::new();
        
        // 수익률 계산
        let mut returns = vec![f64::NAN; data.len()];
        for i in 1..data.len() {
            returns[i] = (data[i].close - data[i - 1].close) / data[i - 1].close;
        }
        features.insert("returns".to_string(), returns);
        
        // 로그 수익률
        let mut log_returns = vec![f64::NAN; data.len()];
        for i in 1..data.len() {
            log_returns[i] = (data[i].close / data[i - 1].close).ln();
        }
        features.insert("log_returns".to_string(), log_returns);
        
        // 가격 범위 비율
        let mut price_range_ratio = Vec::new();
        for d in data {
            let ratio = if d.close > 0.0 { (d.high - d.low) / d.close } else { 0.0 };
            price_range_ratio.push(ratio);
        }
        features.insert("price_range_ratio".to_string(), price_range_ratio);
        
        // VWAP 편차
        let mut vwap_deviation = Vec::new();
        for d in data {
            let deviation = if d.vwap > 0.0 { (d.close - d.vwap) / d.vwap } else { 0.0 };
            vwap_deviation.push(deviation);
        }
        features.insert("vwap_deviation".to_string(), vwap_deviation);
        
        // 거래량 가중 가격
        let mut vwap_custom = Vec::new();
        for d in data {
            vwap_custom.push(d.vwap);
        }
        features.insert("vwap".to_string(), vwap_custom);
        
        features
    }
}

/// MACD 결과 구조체
#[derive(Debug, Clone)]
struct MacdResult {
    macd_line: Vec<f64>,
    signal_line: Vec<f64>,
    histogram: Vec<f64>,
}

/// 볼린저 밴드 결과 구조체
#[derive(Debug, Clone)]
struct BollingerBandsResult {
    upper_band: Vec<f64>,
    middle_band: Vec<f64>,
    lower_band: Vec<f64>,
    band_width: Vec<f64>,
    price_position: Vec<f64>,
}

/// Stochastic 결과 구조체
#[derive(Debug, Clone)]
struct StochasticResult {
    k_percent: Vec<f64>,
    d_percent: Vec<f64>,
}

impl Default for TechnicalIndicatorCalculator {
    fn default() -> Self {
        Self::new(TechnicalIndicatorConfig {
            sma_periods: vec![5, 10, 20, 50, 200],
            ema_periods: vec![12, 26],
            rsi_period: 14,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            bollinger_period: 20,
            bollinger_std: 2.0,
            atr_period: 14,
            stochastic_k: 14,
            stochastic_d: 3,
            williams_r_period: 14,
        })
    }
}

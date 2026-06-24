//! 특성 추출 모듈
//! 
//! 원시 데이터로부터 머신러닝에 사용할 특성들을 추출합니다.
//! - 기술적 지표 특성
//! - 통계적 특성
//! - 시계열 특성
//! - 크로스 섹션 특성
//! - 특성 선택 및 엔지니어링

use anyhow::Result;
use std::collections::HashMap;
use tracing::{ debug, warn, error};

/// 특성 벡터
#[derive(Debug, Clone)]
pub struct FeatureVector {
    pub symbol: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub features: HashMap<String, f64>,
    pub feature_names: Vec<String>,
}

/// 특성 추출기 설정
#[derive(Debug, Clone)]
pub struct FeatureExtractionConfig {
    pub technical_indicators: Vec<String>,
    pub statistical_features: Vec<String>,
    pub time_series_features: Vec<String>,
    pub cross_sectional_features: Vec<String>,
    pub lookback_periods: Vec<usize>,
    pub normalization: NormalizationMethod,
}

/// 정규화 방법
#[derive(Debug, Clone)]
pub enum NormalizationMethod {
    None,
    MinMax,
    ZScore,
    Robust,
}

/// 특성 추출기
pub struct FeatureExtractor {
    config: FeatureExtractionConfig,
}

impl FeatureExtractor {
    pub fn new(config: FeatureExtractionConfig) -> Self {
        Self { config }
    }
    
    /// 특성 추출 실행
    pub async fn extract_features(
        &self,
        data: &HashMap<String, Vec<f64>>,
        symbol: &str,
    ) -> Result<FeatureVector> {
        let mut features = HashMap::new();
        let mut feature_names = Vec::new();
        
        // 기술적 지표 특성
        for indicator in &self.config.technical_indicators {
            if let Ok(indicator_features) = self.extract_technical_indicator(data, indicator).await {
                for (name, value) in indicator_features {
                    features.insert(name.clone(), value);
                    feature_names.push(name);
                }
            }
        }
        
        // 통계적 특성
        for stat_feature in &self.config.statistical_features {
            if let Ok(stat_features) = self.extract_statistical_feature(data, stat_feature).await {
                for (name, value) in stat_features {
                    features.insert(name.clone(), value);
                    feature_names.push(name);
                }
            }
        }
        
        // 시계열 특성
        for ts_feature in &self.config.time_series_features {
            if let Ok(ts_features) = self.extract_time_series_feature(data, ts_feature).await {
                for (name, value) in ts_features {
                    features.insert(name.clone(), value);
                    feature_names.push(name);
                }
            }
        }
        
        // 크로스 섹션 특성
        for cs_feature in &self.config.cross_sectional_features {
            if let Ok(cs_features) = self.extract_cross_sectional_feature(data, cs_feature).await {
                for (name, value) in cs_features {
                    features.insert(name.clone(), value);
                    feature_names.push(name);
                }
            }
        }
        
        // 특성 정규화
        if matches!(self.config.normalization, NormalizationMethod::None) == false {
            self.normalize_features(&mut features).await?;
        }
        Ok(FeatureVector {
            symbol: symbol.to_string(),
            timestamp: chrono::Utc::now(),
            features,
            feature_names,
        })
    }
    
    /// 기술적 지표 특성 추출
    async fn extract_technical_indicator(
        &self,
        data: &HashMap<String, Vec<f64>>,
        indicator: &str,
    ) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        
        match indicator {
            "sma" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(sma) = self.calculate_sma(prices, *period) {
                            features.insert(format!("sma_{}", period), sma);
                        }
                    }
                }
            }
            "ema" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(ema) = self.calculate_ema(prices, *period) {
                            features.insert(format!("ema_{}", period), ema);
                        }
                    }
                }
            }
            "rsi" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(rsi) = self.calculate_rsi(prices, *period) {
                            features.insert(format!("rsi_{}", period), rsi);
                        }
                    }
                }
            }
            "macd" => {
                if let Some(prices) = data.get("close") {
                    if let Some((macd, signal, histogram)) = self.calculate_macd(prices) {
                        features.insert("macd".to_string(), macd);
                        features.insert("macd_signal".to_string(), signal);
                        features.insert("macd_histogram".to_string(), histogram);
                    }
                }
            }
            "bollinger_bands" => {
                if let Some(prices) = data.get("close") {
                    if let Some((upper, middle, lower)) = self.calculate_bollinger_bands(prices) {
                        features.insert("bb_upper".to_string(), upper);
                        features.insert("bb_middle".to_string(), middle);
                        features.insert("bb_lower".to_string(), lower);
                        features.insert("bb_width".to_string(), upper - lower);
                        features.insert("bb_position".to_string(), (prices.last().unwrap_or(&0.0) - lower) / (upper - lower));
                    }
                }
            }
            "atr" => {
                for period in &self.config.lookback_periods {
                    if let (Some(high), Some(low), Some(close)) = (data.get("high"), data.get("low"), data.get("close")) {
                        if let Some(atr) = self.calculate_atr(high, low, close, *period) {
                            features.insert(format!("atr_{}", period), atr);
                        }
                    }
                }
            }
            _ => {
                warn!("지원하지 않는 기술적 지표: {}", indicator);
            }
        }
        
        Ok(features)
    }
    
    /// 통계적 특성 추출
    async fn extract_statistical_feature(
        &self,
        data: &HashMap<String, Vec<f64>>,
        feature: &str,
    ) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        
        match feature {
            "returns" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(returns) = self.calculate_returns(prices, *period) {
                            features.insert(format!("return_{}", period), returns);
                            features.insert(format!("abs_return_{}", period), returns.abs());
                            features.insert(format!("squared_return_{}", period), returns.powi(2));
                        }
                    }
                }
            }
            "volatility" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(volatility) = self.calculate_volatility(prices, *period) {
                            features.insert(format!("volatility_{}", period), volatility);
                        }
                    }
                }
            }
            "skewness" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(skewness) = self.calculate_skewness(prices, *period) {
                            features.insert(format!("skewness_{}", period), skewness);
                        }
                    }
                }
            }
            "kurtosis" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(kurtosis) = self.calculate_kurtosis(prices, *period) {
                            features.insert(format!("kurtosis_{}", period), kurtosis);
                        }
                    }
                }
            }
            _ => {
                warn!("지원하지 않는 통계적 특성: {}", feature);
            }
        }
        
        Ok(features)
    }
    
    /// 시계열 특성 추출
    async fn extract_time_series_feature(
        &self,
        data: &HashMap<String, Vec<f64>>,
        feature: &str,
    ) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        
        match feature {
            "trend" => {
                if let Some(prices) = data.get("close") {
                    if let Some(trend) = self.calculate_trend(prices) {
                        features.insert("trend_slope".to_string(), trend);
                    }
                }
            }
            "seasonality" => {
                if let Some(prices) = data.get("close") {
                    for period in &self.config.lookback_periods {
                        if let Some(seasonality) = self.calculate_seasonality(prices, *period) {
                            features.insert(format!("seasonality_{}", period), seasonality);
                        }
                    }
                }
            }
            "autocorrelation" => {
                if let Some(prices) = data.get("close") {
                    for lag in &self.config.lookback_periods {
                        if let Some(autocorr) = self.calculate_autocorrelation(prices, *lag) {
                            features.insert(format!("autocorr_{}", lag), autocorr);
                        }
                    }
                }
            }
            _ => {
                warn!("지원하지 않는 시계열 특성: {}", feature);
            }
        }
        
        Ok(features)
    }
    
    /// 크로스 섹션 특성 추출
    async fn extract_cross_sectional_feature(
        &self,
        data: &HashMap<String, Vec<f64>>,
        feature: &str,
    ) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        
        match feature {
            "relative_strength" => {
                if let Some(prices) = data.get("close") {
                    if let Some(rs) = self.calculate_relative_strength(prices) {
                        features.insert("relative_strength".to_string(), rs);
                    }
                }
            }
            "momentum" => {
                for period in &self.config.lookback_periods {
                    if let Some(prices) = data.get("close") {
                        if let Some(momentum) = self.calculate_momentum(prices, *period) {
                            features.insert(format!("momentum_{}", period), momentum);
                        }
                    }
                }
            }
            _ => {
                warn!("지원하지 않는 크로스 섹션 특성: {}", feature);
            }
        }
        
        Ok(features)
    }
    
    /// 특성 정규화
    async fn normalize_features(&self, features: &mut HashMap<String, f64>) -> Result<()> {
        match self.config.normalization {
            NormalizationMethod::MinMax => {
                self.min_max_normalize(features).await?;
            }
            NormalizationMethod::ZScore => {
                self.z_score_normalize(features).await?;
            }
            NormalizationMethod::Robust => {
                self.robust_normalize(features).await?;
            }
            NormalizationMethod::None => {
                // 정규화하지 않음
            }
        }
        Ok(())
    }
    
    /// Min-Max 정규화
    async fn min_max_normalize(&self, features: &mut HashMap<String, f64>) -> Result<()> {
        // TODO: 전체 데이터셋에서 min/max 계산
        // 여기서는 개별 특성을 0-1 범위로 정규화
        for (_, value) in features.iter_mut() {
            *value = value.tanh(); // 간단한 정규화
        }
        Ok(())
    }
    
    /// Z-Score 정규화
    async fn z_score_normalize(&self, features: &mut HashMap<String, f64>) -> Result<()> {
        // TODO: 전체 데이터셋에서 평균/표준편차 계산
        // 여기서는 개별 특성을 표준화
        for (_, value) in features.iter_mut() {
            *value = *value / (1.0 + value.abs()); // 간단한 정규화
        }
        Ok(())
    }
    
    /// Robust 정규화
    async fn robust_normalize(&self, features: &mut HashMap<String, f64>) -> Result<()> {
        // TODO: 전체 데이터셋에서 중앙값/IQR 계산
        // 여기서는 간단한 정규화
        for (_, value) in features.iter_mut() {
            *value = value.signum() * (value.abs().ln_1p()); // 로그 변환
        }
        Ok(())
    }
    
    // 기술적 지표 계산 함수들
    fn calculate_sma(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }
        let sum: f64 = prices[prices.len() - period..].iter().sum();
        Some(sum / period as f64)
    }
    
    fn calculate_ema(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }
        let alpha = 2.0 / (period + 1) as f64;
        let mut ema = prices[0];
        for &price in &prices[1..] {
            ema = alpha * price + (1.0 - alpha) * ema;
        }
        Some(ema)
    }
    
    fn calculate_rsi(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period + 1 {
            return None;
        }
        
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        for i in 1..=period {
            let change = prices[prices.len() - i] - prices[prices.len() - i - 1];
            if change > 0.0 {
                gains += change;
            } else {
                losses += change.abs();
            }
        }
        
        if losses == 0.0 {
            return Some(100.0);
        }
        
        let rs = gains / losses;
        Some(100.0 - (100.0 / (1.0 + rs)))
    }
    
    fn calculate_macd(&self, prices: &[f64]) -> Option<(f64, f64, f64)> {
        if prices.len() < 26 {
            return None;
        }
        
        let ema12 = self.calculate_ema(prices, 12)?;
        let ema26 = self.calculate_ema(prices, 26)?;
        let macd = ema12 - ema26;
        
        // MACD Signal Line (9-period EMA of MACD)
        let signal = macd; // 간단화
        let histogram = macd - signal;
        
        Some((macd, signal, histogram))
    }
    
    fn calculate_bollinger_bands(&self, prices: &[f64]) -> Option<(f64, f64, f64)> {
        if prices.len() < 20 {
            return None;
        }
        
        let sma = self.calculate_sma(prices, 20)?;
        let variance: f64 = prices[prices.len() - 20..]
            .iter()
            .map(|&price| (price - sma).powi(2))
            .sum::<f64>() / 20.0;
        let std_dev = variance.sqrt();
        
        let upper = sma + 2.0 * std_dev;
        let lower = sma - 2.0 * std_dev;
        
        Some((upper, sma, lower))
    }
    
    fn calculate_atr(&self, high: &[f64], low: &[f64], close: &[f64], period: usize) -> Option<f64> {
        if high.len() < period + 1 || low.len() < period + 1 || close.len() < period + 1 {
            return None;
        }
        
        let mut true_ranges = Vec::new();
        for i in 1..=period {
            let idx = high.len() - i;
            let tr1 = high[idx] - low[idx];
            let tr2 = (high[idx] - close[idx - 1]).abs();
            let tr3 = (low[idx] - close[idx - 1]).abs();
            true_ranges.push(tr1.max(tr2).max(tr3));
        }
        
        Some(true_ranges.iter().sum::<f64>() / period as f64)
    }
    
    fn calculate_returns(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period + 1 {
            return None;
        }
        let current = prices[prices.len() - 1];
        let past = prices[prices.len() - 1 - period];
        Some((current - past) / past)
    }
    
    fn calculate_volatility(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period + 1 {
            return None;
        }
        
        let returns: Vec<f64> = (0..period)
            .map(|i| {
                let idx = prices.len() - 1 - i;
                (prices[idx] - prices[idx - 1]) / prices[idx - 1]
            })
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|&r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        Some(variance.sqrt())
    }
    
    fn calculate_skewness(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }
        
        let slice = &prices[prices.len() - period..];
        let mean = slice.iter().sum::<f64>() / slice.len() as f64;
        let std_dev = (slice.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / slice.len() as f64).sqrt();
        
        if std_dev == 0.0 {
            return Some(0.0);
        }
        
        let skewness = slice.iter()
            .map(|&x| ((x - mean) / std_dev).powi(3))
            .sum::<f64>() / slice.len() as f64;
        
        Some(skewness)
    }
    
    fn calculate_kurtosis(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }
        
        let slice = &prices[prices.len() - period..];
        let mean = slice.iter().sum::<f64>() / slice.len() as f64;
        let std_dev = (slice.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / slice.len() as f64).sqrt();
        
        if std_dev == 0.0 {
            return Some(0.0);
        }
        
        let kurtosis = slice.iter()
            .map(|&x| ((x - mean) / std_dev).powi(4))
            .sum::<f64>() / slice.len() as f64;
        
        Some(kurtosis - 3.0) // Excess kurtosis
    }
    
    fn calculate_trend(&self, prices: &[f64]) -> Option<f64> {
        if prices.len() < 2 {
            return None;
        }
        
        let first = prices[0];
        let last = prices[prices.len() - 1];
        Some((last - first) / first)
    }
    
    fn calculate_seasonality(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period * 2 {
            return None;
        }
        
        // 간단한 계절성 계산
        let current_period = &prices[prices.len() - period..];
        let previous_period = &prices[prices.len() - period * 2..prices.len() - period];
        
        let current_avg = current_period.iter().sum::<f64>() / current_period.len() as f64;
        let previous_avg = previous_period.iter().sum::<f64>() / previous_period.len() as f64;
        
        Some((current_avg - previous_avg) / previous_avg)
    }
    
    fn calculate_autocorrelation(&self, prices: &[f64], lag: usize) -> Option<f64> {
        if prices.len() < lag + 1 {
            return None;
        }
        
        let returns: Vec<f64> = (1..prices.len())
            .map(|i| (prices[i] - prices[i - 1]) / prices[i - 1])
            .collect();
        
        if returns.len() < lag + 1 {
            return None;
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|&r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        if variance == 0.0 {
            return Some(0.0);
        }
        
        let autocovariance = returns[lag..].iter()
            .zip(returns[..returns.len() - lag].iter())
            .map(|(&r1, &r2)| (r1 - mean) * (r2 - mean))
            .sum::<f64>() / (returns.len() - lag) as f64;
        
        Some(autocovariance / variance)
    }
    
    fn calculate_relative_strength(&self, prices: &[f64]) -> Option<f64> {
        if prices.len() < 14 {
            return None;
        }
        
        let gains = prices[1..14].iter()
            .zip(prices[..13].iter())
            .map(|(&curr, &prev)| if curr > prev { curr - prev } else { 0.0 })
            .sum::<f64>();
        
        let losses = prices[1..14].iter()
            .zip(prices[..13].iter())
            .map(|(&curr, &prev)| if curr < prev { prev - curr } else { 0.0 })
            .sum::<f64>();
        
        if losses == 0.0 {
            return Some(100.0);
        }
        
        let rs = gains / losses;
        Some(100.0 - (100.0 / (1.0 + rs)))
    }
    
    fn calculate_momentum(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period + 1 {
            return None;
        }
        
        let current = prices[prices.len() - 1];
        let past = prices[prices.len() - 1 - period];
        Some(current - past)
    }
}

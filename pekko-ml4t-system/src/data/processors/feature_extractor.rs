//! 특성 추출 모듈
//! 
//! 다양한 데이터 소스로부터 ML 모델에 필요한 특성을 추출합니다:
//! - 기술적 지표 특성
//! - 가격 및 거래량 특성
//! - 시간 특성
//! - 통계적 특성
//! - Alpha Factor 특성

use super::{FeatureExtractionConfig, ProcessedData, RawData, PreprocessingResult, PreprocessingStatistics};
use crate::data::{MarketData, NewsData, AlternativeData};
use anyhow::Result;
use chrono::{DateTime, Utc, Timelike, Datelike};
use std::collections::HashMap;
use tracing::{ warn, debug};

/// 특성 추출기
pub struct FeatureExtractor {
    config: FeatureExtractionConfig,
    technical_calculator: super::TechnicalIndicatorCalculator,
}

impl FeatureExtractor {
    /// 새로운 특성 추출기 생성
    pub fn new(config: FeatureExtractionConfig) -> Self {
        let technical_config = config.technical_indicators.clone();
        Self {
            config,
            technical_calculator: super::TechnicalIndicatorCalculator::new(technical_config),
        }
    }

    /// 시장 데이터에서 특성 추출
    pub async fn extract_market_features(&self, data: &[MarketData]) -> Result<Vec<ProcessedData>> {
        // 1. 기술적 지표 계산
        let technical_indicators = self.technical_calculator.calculate_indicators(data)?;
        
        // 2. 가격 특성 추출
        let price_features = self.extract_price_features(data)?;
        
        // 3. 거래량 특성 추출
        let volume_features = self.extract_volume_features(data)?;
        
        // 4. 시간 특성 추출
        let time_features = self.extract_time_features(data)?;
        
        // 5. 통계적 특성 추출
        let statistical_features = self.extract_statistical_features(data)?;
        
        // 6. 모든 특성을 ProcessedData로 결합
        let mut processed_data = Vec::new();
        
        for (i, market_data) in data.iter().enumerate() {
            let mut features = HashMap::new();
            
            // 기술적 지표 추가
            for (indicator_name, indicator_values) in &technical_indicators {
                if i < indicator_values.len() && !indicator_values[i].is_nan() {
                    features.insert(indicator_name.clone(), indicator_values[i]);
                }
            }
            
            // 가격 특성 추가
            if let Some(price_feature) = price_features.get(i) {
                features.extend(price_feature.clone());
            }
            
            // 거래량 특성 추가
            if let Some(volume_feature) = volume_features.get(i) {
                features.extend(volume_feature.clone());
            }
            
            // 시간 특성 추가
            if let Some(time_feature) = time_features.get(i) {
                features.extend(time_feature.clone());
            }
            
            // 통계적 특성 추가
            if let Some(statistical_feature) = statistical_features.get(i) {
                features.extend(statistical_feature.clone());
            }
            
            processed_data.push(ProcessedData {
                symbol: market_data.symbol.clone(),
                timestamp: market_data.timestamp,
                raw_data: RawData::MarketData(market_data.clone()),
                features,
                quality_score: 1.0, // 기본값, 나중에 품질 평가에서 업데이트
                outlier_flags: HashMap::new(),
                metadata: HashMap::new(),
            });
        }
        Ok(processed_data)
    }

    /// 뉴스 데이터에서 특성 추출
    pub async fn extract_news_features(&self, data: &[NewsData]) -> Result<Vec<ProcessedData>> {
        let mut processed_data = Vec::new();
        
        for news_data in data {
            let mut features = HashMap::new();
            
            // 1. 센티먼트 특성
            if let Some(sentiment_score) = news_data.sentiment_score {
                features.insert("sentiment_score".to_string(), sentiment_score);
            }
            
            if let Some(sentiment_magnitude) = news_data.sentiment_magnitude {
                features.insert("sentiment_magnitude".to_string(), sentiment_magnitude);
            }
            
            // 2. 텍스트 길이 특성
            features.insert("title_length".to_string(), news_data.title.len() as f64);
            features.insert("content_length".to_string(), news_data.content.len() as f64);
            
            // 3. 엔티티 특성
            features.insert("entity_count".to_string(), news_data.entities.len() as f64);
            features.insert("symbol_count".to_string(), news_data.symbols.len() as f64);
            features.insert("category_count".to_string(), news_data.categories.len() as f64);
            
            // 4. 엔티티 타입별 개수
            let mut entity_type_counts = HashMap::new();
            for entity in &news_data.entities {
                *entity_type_counts.entry(entity.entity_type.clone()).or_insert(0) += 1;
            }
            
            for (entity_type, count) in entity_type_counts {
                features.insert(format!("entity_{:?}_count", entity_type), count as f64);
            }
            
            // 5. 시간 특성
            let time_features = self.extract_news_time_features(news_data.timestamp)?;
            features.extend(time_features);
            
            // 6. 중요도 점수
            features.insert("importance_score".to_string(), news_data.importance_score());
            
            // 7. 텍스트 복잡도 (간단한 휴리스틱)
            let word_count = news_data.content.split_whitespace().count();
            features.insert("word_count".to_string(), word_count as f64);
            
            let avg_word_length = if word_count > 0 {
                news_data.content.chars().filter(|c| c.is_alphanumeric()).count() as f64 / word_count as f64
            } else {
                0.0
            };
            features.insert("avg_word_length".to_string(), avg_word_length);
            
            processed_data.push(ProcessedData {
                symbol: news_data.symbols.first().unwrap_or(&"".to_string()).clone(),
                timestamp: news_data.timestamp,
                raw_data: RawData::NewsData(news_data.clone()),
                features,
                quality_score: 1.0,
                outlier_flags: HashMap::new(),
                metadata: HashMap::new(),
            });
        }
        Ok(processed_data)
    }

    /// 대체 데이터에서 특성 추출
    pub async fn extract_alternative_features(&self, data: &[AlternativeData]) -> Result<Vec<ProcessedData>> {
        let mut processed_data = Vec::new();
        
        for alt_data in data {
            let mut features = HashMap::new();
            
            // 1. 이미 처리된 특성들 추가
            features.extend(alt_data.processed_features.clone());
            
            // 2. 메타데이터 기반 특성
            features.insert("metadata_count".to_string(), alt_data.metadata.len() as f64);
            
            // 3. 데이터 타입별 특성
            match alt_data.data_type {
                crate::data::AlternativeDataType::SatelliteImage => {
                    features.insert("data_type_satellite".to_string(), 1.0);
                    features.insert("data_type_other".to_string(), 0.0);
                }
                crate::data::AlternativeDataType::SocialMediaSentiment => {
                    features.insert("data_type_social".to_string(), 1.0);
                    features.insert("data_type_other".to_string(), 0.0);
                }
                crate::data::AlternativeDataType::EconomicIndicator => {
                    features.insert("data_type_economic".to_string(), 1.0);
                    features.insert("data_type_other".to_string(), 0.0);
                }
                _ => {
                    features.insert("data_type_other".to_string(), 1.0);
                }
            }
            
            // 4. 시간 특성
            let time_features = self.extract_alternative_time_features(alt_data.timestamp)?;
            features.extend(time_features);
            
            processed_data.push(ProcessedData {
                symbol: "alternative".to_string(),
                timestamp: alt_data.timestamp,
                raw_data: RawData::AlternativeData(alt_data.clone()),
                features,
                quality_score: 1.0,
                outlier_flags: HashMap::new(),
                metadata: alt_data.metadata.clone(),
            });
        }
        Ok(processed_data)
    }

    /// 가격 특성 추출
    fn extract_price_features(&self, data: &[MarketData]) -> Result<Vec<HashMap<String, f64>>> {
        let mut features = Vec::new();
        
        for (i, market_data) in data.iter().enumerate() {
            let mut price_features = HashMap::new();
            
            if self.config.price_features.returns {
                // 수익률 (이전 가격 대비)
                if i > 0 {
                    let return_val = (market_data.close - data[i - 1].close) / data[i - 1].close;
                    price_features.insert("price_return".to_string(), return_val);
                }
            }
            
            if self.config.price_features.log_returns {
                // 로그 수익률
                if i > 0 {
                    let log_return = (market_data.close / data[i - 1].close).ln();
                    price_features.insert("log_return".to_string(), log_return);
                }
            }
            
            if self.config.price_features.price_changes {
                // 절대 가격 변화
                if i > 0 {
                    let price_change = market_data.close - data[i - 1].close;
                    price_features.insert("price_change".to_string(), price_change);
                }
            }
            
            if self.config.price_features.price_ratios {
                // 가격 비율
                price_features.insert("high_close_ratio".to_string(), market_data.high / market_data.close);
                price_features.insert("low_close_ratio".to_string(), market_data.low / market_data.close);
                price_features.insert("open_close_ratio".to_string(), market_data.open / market_data.close);
            }
            
            if self.config.price_features.volatility {
                // 가격 변동성 (간단한 계산)
                if i >= 4 {
                    let recent_closes: Vec<f64> = data[i-4..=i].iter().map(|d| d.close).collect();
                    let mean = recent_closes.iter().sum::<f64>() / recent_closes.len() as f64;
                    let variance = recent_closes.iter()
                        .map(|&x| (x - mean).powi(2))
                        .sum::<f64>() / recent_closes.len() as f64;
                    let volatility = variance.sqrt();
                    price_features.insert("price_volatility".to_string(), volatility);
                }
            }
            
            if self.config.price_features.price_position {
                // 가격 위치 (High-Low 범위 내에서의 위치)
                if market_data.high != market_data.low {
                    let position = (market_data.close - market_data.low) / (market_data.high - market_data.low);
                    price_features.insert("price_position".to_string(), position);
                }
            }
            
            features.push(price_features);
        }
        
        Ok(features)
    }

    /// 거래량 특성 추출
    fn extract_volume_features(&self, data: &[MarketData]) -> Result<Vec<HashMap<String, f64>>> {
        let mut features = Vec::new();
        
        for (i, market_data) in data.iter().enumerate() {
            let mut volume_features = HashMap::new();
            
            if self.config.volume_features.volume_ma {
                // 거래량 이동평균 비율
                if i >= 4 {
                    let recent_volumes: Vec<u64> = data[i-4..=i].iter().map(|d| d.volume).collect();
                    let avg_volume = recent_volumes.iter().sum::<u64>() as f64 / recent_volumes.len() as f64;
                    let volume_ratio = market_data.volume as f64 / avg_volume;
                    volume_features.insert("volume_ma_ratio".to_string(), volume_ratio);
                }
            }
            
            if self.config.volume_features.volume_ratio {
                // 이전 거래량 대비 비율
                if i > 0 {
                    let volume_ratio = market_data.volume as f64 / data[i - 1].volume as f64;
                    volume_features.insert("volume_change_ratio".to_string(), volume_ratio);
                }
            }
            
            if self.config.volume_features.volume_price_trend {
                // 거래량-가격 트렌드 (간단한 구현)
                if i > 0 {
                    let price_change = market_data.close - data[i - 1].close;
                    let volume_change = market_data.volume as i64 - data[i - 1].volume as i64;
                    let trend = price_change * volume_change as f64;
                    volume_features.insert("volume_price_trend".to_string(), trend);
                }
            }
            
            if self.config.volume_features.on_balance_volume {
                // OBV 기반 특성
                if i > 0 {
                    let price_change = market_data.close - data[i - 1].close;
                    let obv_signal = if price_change > 0.0 { 1.0 } else if price_change < 0.0 { -1.0 } else { 0.0 };
                    volume_features.insert("obv_signal".to_string(), obv_signal);
                }
            }
            
            features.push(volume_features);
        }
        
        Ok(features)
    }

    /// 시간 특성 추출
    fn extract_time_features(&self, data: &[MarketData]) -> Result<Vec<HashMap<String, f64>>> {
        let mut features = Vec::new();
        
        for market_data in data {
            let mut time_features = HashMap::new();
            let dt = market_data.timestamp;
            
            if self.config.time_features.day_of_week {
                time_features.insert("day_of_week".to_string(), dt.weekday().num_days_from_monday() as f64);
            }
            
            if self.config.time_features.month {
                time_features.insert("month".to_string(), dt.month() as f64);
            }
            
            if self.config.time_features.quarter {
                time_features.insert("quarter".to_string(), ((dt.month() - 1) / 3 + 1) as f64);
            }
            
            if self.config.time_features.year {
                time_features.insert("year".to_string(), dt.year() as f64);
            }
            
            if self.config.time_features.is_month_end {
                let is_month_end = dt.day() > 25; // 간단한 휴리스틱
                time_features.insert("is_month_end".to_string(), if is_month_end { 1.0 } else { 0.0 });
            }
            
            if self.config.time_features.is_quarter_end {
                let month = dt.month();
                let is_quarter_end = month == 3 || month == 6 || month == 9 || month == 12;
                time_features.insert("is_quarter_end".to_string(), if is_quarter_end { 1.0 } else { 0.0 });
            }
            
            if self.config.time_features.is_year_end {
                let is_year_end = dt.month() == 12;
                time_features.insert("is_year_end".to_string(), if is_year_end { 1.0 } else { 0.0 });
            }
            
            if self.config.time_features.trading_session {
                let hour = dt.hour();
                let session = match hour {
                    9..=15 => 1.0,  // 정규 거래 시간
                    8..=8 => 0.5,   // 장전 시간
                    16..=17 => 0.5, // 장후 시간
                    _ => 0.0,       // 비거래 시간
                };
                time_features.insert("trading_session".to_string(), session);
            }
            
            features.push(time_features);
        }
        
        Ok(features)
    }

    /// 뉴스 시간 특성 추출
    fn extract_news_time_features(&self, timestamp: DateTime<Utc>) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        let dt = timestamp;
        
        features.insert("news_day_of_week".to_string(), dt.weekday().num_days_from_monday() as f64);
        features.insert("news_hour".to_string(), dt.hour() as f64);
        features.insert("news_month".to_string(), dt.month() as f64);
        
        // 시장 시간과의 관계
        let hour = dt.hour();
        let is_market_hours = hour >= 9 && hour <= 15;
        features.insert("is_market_hours".to_string(), if is_market_hours { 1.0 } else { 0.0 });
        
        Ok(features)
    }

    /// 대체 데이터 시간 특성 추출
    fn extract_alternative_time_features(&self, timestamp: DateTime<Utc>) -> Result<HashMap<String, f64>> {
        let mut features = HashMap::new();
        let dt = timestamp;
        
        features.insert("alt_day_of_week".to_string(), dt.weekday().num_days_from_monday() as f64);
        features.insert("alt_hour".to_string(), dt.hour() as f64);
        features.insert("alt_month".to_string(), dt.month() as f64);
        
        Ok(features)
    }

    /// 통계적 특성 추출
    fn extract_statistical_features(&self, data: &[MarketData]) -> Result<Vec<HashMap<String, f64>>> {
        let mut features = Vec::new();
        let window_size = 20; // 롤링 윈도우 크기
        
        for (i, market_data) in data.iter().enumerate() {
            let mut statistical_features = HashMap::new();
            
            if i >= window_size - 1 {
                let window_start = i - window_size + 1;
                let window_data = &data[window_start..=i];
                
                if self.config.statistical_features.rolling_mean {
                    let mean = window_data.iter().map(|d| d.close).sum::<f64>() / window_size as f64;
                    statistical_features.insert("rolling_mean".to_string(), mean);
                }
                
                if self.config.statistical_features.rolling_std {
                    let mean = window_data.iter().map(|d| d.close).sum::<f64>() / window_size as f64;
                    let variance = window_data.iter()
                        .map(|d| (d.close - mean).powi(2))
                        .sum::<f64>() / window_size as f64;
                    let std_dev = variance.sqrt();
                    statistical_features.insert("rolling_std".to_string(), std_dev);
                }
                
                if self.config.statistical_features.rolling_skew {
                    // 간단한 왜도 계산
                    let mean = window_data.iter().map(|d| d.close).sum::<f64>() / window_size as f64;
                    let std_dev = statistical_features.get("rolling_std").copied().unwrap_or(1.0);
                    if std_dev > 0.0 {
                        let skew = window_data.iter()
                            .map(|d| ((d.close - mean) / std_dev).powi(3))
                            .sum::<f64>() / window_size as f64;
                        statistical_features.insert("rolling_skew".to_string(), skew);
                    }
                }
                
                if self.config.statistical_features.rolling_kurtosis {
                    // 간단한 첨도 계산
                    let mean = window_data.iter().map(|d| d.close).sum::<f64>() / window_size as f64;
                    let std_dev = statistical_features.get("rolling_std").copied().unwrap_or(1.0);
                    if std_dev > 0.0 {
                        let kurtosis = window_data.iter()
                            .map(|d| ((d.close - mean) / std_dev).powi(4))
                            .sum::<f64>() / window_size as f64;
                        statistical_features.insert("rolling_kurtosis".to_string(), kurtosis);
                    }
                }
                
                // 백분위수 계산
                for &percentile in &self.config.statistical_features.rolling_percentiles {
                    let mut values: Vec<f64> = window_data.iter().map(|d| d.close).collect();
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    let index = (percentile * (values.len() - 1) as f64).round() as usize;
                    if index < values.len() {
                        statistical_features.insert(format!("rolling_p{}", (percentile * 100.0) as u32), values[index]);
                    }
                }
            }
            
            features.push(statistical_features);
        }
        
        Ok(features)
    }

    /// 특성 중요도 계산 (간단한 상관관계 기반)
    pub fn calculate_feature_importance(&self, processed_data: &[ProcessedData], target_feature: &str) -> HashMap<String, f64> {
        let mut importance_scores = HashMap::new();
        
        if processed_data.len() < 2 {
            return importance_scores;
        }
        
        // 타겟 특성 값들 추출
        let target_values: Vec<f64> = processed_data.iter()
            .filter_map(|d| d.features.get(target_feature).copied())
            .collect();
        
        if target_values.len() < 2 {
            return importance_scores;
        }
        
        // 모든 특성에 대해 상관관계 계산
        let feature_names: std::collections::HashSet<String> = processed_data.iter()
            .flat_map(|d| d.features.keys().cloned())
            .collect();
        
        for feature_name in feature_names {
            if feature_name == target_feature {
                continue;
            }
            
            let feature_values: Vec<f64> = processed_data.iter()
                .filter_map(|d| d.features.get(&feature_name).copied())
                .collect();
            
            if feature_values.len() == target_values.len() {
                let correlation = self.calculate_correlation(&target_values, &feature_values);
                importance_scores.insert(feature_name, correlation.abs());
            }
        }
        
        importance_scores
    }

    /// 상관관계 계산 (피어슨 상관계수)
    fn calculate_correlation(&self, x: &[f64], y: &[f64]) -> f64 {
        if x.len() != y.len() || x.len() < 2 {
            return 0.0;
        }
        
        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;
        
        let numerator: f64 = x.iter().zip(y.iter())
            .map(|(&xi, &yi)| (xi - mean_x) * (yi - mean_y))
            .sum();
        
        let sum_sq_x: f64 = x.iter().map(|&xi| (xi - mean_x).powi(2)).sum();
        let sum_sq_y: f64 = y.iter().map(|&yi| (yi - mean_y).powi(2)).sum();
        
        let denominator = (sum_sq_x * sum_sq_y).sqrt();
        
        if denominator == 0.0 {
            0.0
        } else {
            numerator / denominator
        }
    }
}

impl Default for FeatureExtractor {
    fn default() -> Self {
        Self::new(FeatureExtractionConfig::default())
    }
}

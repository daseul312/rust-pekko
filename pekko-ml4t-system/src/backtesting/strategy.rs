//! 거래 전략 구현
//! 
//! 다양한 거래 전략들을 구현합니다.
//! - Buy & Hold 전략
//! - Mean Reversion 전략
//! - Momentum 전략
//! - ML 기반 전략
//! - Alpha Factor 전략

use super::engine::TradingStrategy;
use super::PortfolioSnapshot;
use crate::actors::{MarketData, TradingSignal};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{debug};

/// Buy & Hold 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyHoldStrategy {
    pub name: String,
    pub weights: HashMap<String, f64>, // 종목별 비중
    pub rebalance_threshold: f64, // 리밸런싱 임계값
}

impl TradingStrategy for BuyHoldStrategy {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>> {
        let mut signals = HashMap::new();
        
        // 현재 포트폴리오 비중과 목표 비중 비교
        for (symbol, target_weight) in &self.weights {
            if let Some(data) = market_data.get(symbol) {
                let current_weight = portfolio.positions.get(symbol)
                    .map(|p| p.weight)
                    .unwrap_or(0.0);
                
                let weight_diff = target_weight - current_weight;
                
                if weight_diff.abs() > self.rebalance_threshold {
                    if weight_diff > 0.0 {
                        signals.insert(symbol.clone(), TradingSignal::Buy { strength: weight_diff });
                    } else {
                        signals.insert(symbol.clone(), TradingSignal::Sell { strength: weight_diff.abs() });
                    }
                } else {
                    signals.insert(symbol.clone(), TradingSignal::Hold);
                }
            }
        }
        
        debug!("Buy & Hold 신호 생성: {:?}", signals);
        Ok(signals)
    }
    
    fn get_name(&self) -> String {
        self.name.clone()
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("rebalance_threshold".to_string(), self.rebalance_threshold);
        params
    }
}

/// Mean Reversion 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionStrategy {
    pub name: String,
    pub lookback_period: u32, // 이동평균 계산 기간
    pub entry_threshold: f64, // 진입 임계값 (표준편차)
    pub exit_threshold: f64, // 청산 임계값
    pub max_position_size: f64, // 최대 포지션 크기
    pub price_data: HashMap<String, Vec<f64>>, // 가격 데이터 캐시
}

impl MeanReversionStrategy {
    /// 새로운 Mean Reversion 전략 생성
    pub fn new(
        name: String,
        lookback_period: u32,
        entry_threshold: f64,
        exit_threshold: f64,
        max_position_size: f64,
    ) -> Self {
        Self {
            name,
            lookback_period,
            entry_threshold,
            exit_threshold,
            max_position_size,
            price_data: HashMap::new(),
        }
    }
    
    /// 가격 데이터 업데이트
    fn update_price_data(&mut self, symbol: &str, price: f64) {
        let prices = self.price_data.entry(symbol.to_string()).or_insert_with(Vec::new);
        prices.push(price);
        
        // 최대 기간 유지
        if prices.len() > self.lookback_period as usize * 2 {
            prices.drain(0..prices.len() - (self.lookback_period as usize * 2));
        }
    }
    
    /// 이동평균 계산
    fn calculate_moving_average(&self, symbol: &str) -> Option<f64> {
        if let Some(prices) = self.price_data.get(symbol) {
            if prices.len() >= self.lookback_period as usize {
                let sum: f64 = prices.iter().rev().take(self.lookback_period as usize).sum();
                return Some(sum / self.lookback_period as f64);
            }
        }
        None
    }
    
    /// 표준편차 계산
    fn calculate_standard_deviation(&self, symbol: &str) -> Option<f64> {
        if let Some(prices) = self.price_data.get(symbol) {
            if prices.len() >= self.lookback_period as usize {
                let recent_prices: Vec<f64> = prices.iter().rev().take(self.lookback_period as usize).cloned().collect();
                let mean = recent_prices.iter().sum::<f64>() / recent_prices.len() as f64;
                let variance = recent_prices.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / recent_prices.len() as f64;
                return Some(variance.sqrt());
            }
        }
        None
    }
    
    /// Z-Score 계산
    fn calculate_z_score(&self, symbol: &str, current_price: f64) -> Option<f64> {
        if let (Some(mean), Some(std_dev)) = (self.calculate_moving_average(symbol), self.calculate_standard_deviation(symbol)) {
            if std_dev > 0.0 {
                return Some((current_price - mean) / std_dev);
            }
        }
        None
    }
}

impl TradingStrategy for MeanReversionStrategy {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>> {
        let mut signals = HashMap::new();
        
        for (symbol, data) in market_data {
            // 가격 데이터 업데이트 (클론 필요)
            let mut strategy = self.clone();
            strategy.update_price_data(symbol, data.close);
            
            if let Some(z_score) = strategy.calculate_z_score(symbol, data.close) {
                let current_weight = portfolio.positions.get(symbol)
                    .map(|p| p.weight)
                    .unwrap_or(0.0);
                
                // Mean Reversion 로직
                if z_score < -self.entry_threshold && current_weight < self.max_position_size {
                    // 저평가 상태 - 매수
                    let target_weight = self.max_position_size.min(current_weight + 0.1);
                    let strength = (target_weight - current_weight).min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Buy { strength });
                } else if z_score > self.entry_threshold && current_weight > 0.0 {
                    // 고평가 상태 - 매도
                    let strength = current_weight.min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength });
                } else if z_score.abs() < self.exit_threshold && current_weight > 0.0 {
                    // 평균 회귀 - 포지션 청산
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength: current_weight });
                } else {
                    signals.insert(symbol.clone(), TradingSignal::Hold);
                }
            } else {
                signals.insert(symbol.clone(), TradingSignal::Hold);
            }
        }
        
        debug!("Mean Reversion 신호 생성: {:?}", signals);
        Ok(signals)
    }
    
    fn get_name(&self) -> String {
        self.name.clone()
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("lookback_period".to_string(), self.lookback_period as f64);
        params.insert("entry_threshold".to_string(), self.entry_threshold);
        params.insert("exit_threshold".to_string(), self.exit_threshold);
        params.insert("max_position_size".to_string(), self.max_position_size);
        params
    }
}

/// Momentum 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumStrategy {
    pub name: String,
    pub lookback_period: u32, // 모멘텀 계산 기간
    pub entry_threshold: f64, // 진입 임계값
    pub exit_threshold: f64, // 청산 임계값
    pub max_position_size: f64, // 최대 포지션 크기
    pub price_data: HashMap<String, Vec<f64>>, // 가격 데이터 캐시
}

impl MomentumStrategy {
    /// 새로운 Momentum 전략 생성
    pub fn new(
        name: String,
        lookback_period: u32,
        entry_threshold: f64,
        exit_threshold: f64,
        max_position_size: f64,
    ) -> Self {
        Self {
            name,
            lookback_period,
            entry_threshold,
            exit_threshold,
            max_position_size,
            price_data: HashMap::new(),
        }
    }
    
    /// 가격 데이터 업데이트
    fn update_price_data(&mut self, symbol: &str, price: f64) {
        let prices = self.price_data.entry(symbol.to_string()).or_insert_with(Vec::new);
        prices.push(price);
        
        // 최대 기간 유지
        if prices.len() > self.lookback_period as usize * 2 {
            prices.drain(0..prices.len() - (self.lookback_period as usize * 2));
        }
    }
    
    /// 모멘텀 계산 (수익률)
    fn calculate_momentum(&self, symbol: &str) -> Option<f64> {
        if let Some(prices) = self.price_data.get(symbol) {
            if prices.len() >= self.lookback_period as usize + 1 {
                let current_price = prices[prices.len() - 1];
                let past_price = prices[prices.len() - 1 - self.lookback_period as usize];
                return Some((current_price - past_price) / past_price);
            }
        }
        None
    }
}

impl TradingStrategy for MomentumStrategy {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>> {
        let mut signals = HashMap::new();
        
        for (symbol, data) in market_data {
            // 가격 데이터 업데이트
            let mut strategy = self.clone();
            strategy.update_price_data(symbol, data.close);
            
            if let Some(momentum) = strategy.calculate_momentum(symbol) {
                let current_weight = portfolio.positions.get(symbol)
                    .map(|p| p.weight)
                    .unwrap_or(0.0);
                
                // Momentum 로직
                if momentum > self.entry_threshold && current_weight < self.max_position_size {
                    // 강한 상승 모멘텀 - 매수
                    let target_weight = self.max_position_size.min(current_weight + 0.1);
                    let strength = (target_weight - current_weight).min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Buy { strength });
                } else if momentum < -self.entry_threshold && current_weight > 0.0 {
                    // 강한 하락 모멘텀 - 매도
                    let strength = current_weight.min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength });
                } else if momentum.abs() < self.exit_threshold && current_weight > 0.0 {
                    // 모멘텀 소실 - 포지션 청산
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength: current_weight });
                } else {
                    signals.insert(symbol.clone(), TradingSignal::Hold);
                }
            } else {
                signals.insert(symbol.clone(), TradingSignal::Hold);
            }
        }
        
        debug!("Momentum 신호 생성: {:?}", signals);
        Ok(signals)
    }
    
    fn get_name(&self) -> String {
        self.name.clone()
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("lookback_period".to_string(), self.lookback_period as f64);
        params.insert("entry_threshold".to_string(), self.entry_threshold);
        params.insert("exit_threshold".to_string(), self.exit_threshold);
        params.insert("max_position_size".to_string(), self.max_position_size);
        params
    }
}

/// ML 기반 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLStrategy {
    pub name: String,
    pub model_predictions: HashMap<String, f64>, // 심볼별 예측값
    pub confidence_threshold: f64, // 신뢰도 임계값
    pub max_position_size: f64, // 최대 포지션 크기
    pub prediction_horizon: u32, // 예측 기간 (일)
}

impl MLStrategy {
    /// 새로운 ML 전략 생성
    pub fn new(
        name: String,
        confidence_threshold: f64,
        max_position_size: f64,
        prediction_horizon: u32,
    ) -> Self {
        Self {
            name,
            model_predictions: HashMap::new(),
            confidence_threshold,
            max_position_size,
            prediction_horizon,
        }
    }
    
    /// 모델 예측값 업데이트
    pub fn update_predictions(&mut self, predictions: HashMap<String, f64>) {
        self.model_predictions = predictions;
    }
}

impl TradingStrategy for MLStrategy {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>> {
        let mut signals = HashMap::new();
        
        for (symbol, _data) in market_data {
            let current_weight = portfolio.positions.get(symbol)
                .map(|p| p.weight)
                .unwrap_or(0.0);
            
            if let Some(prediction) = self.model_predictions.get(symbol) {
                // ML 예측 기반 신호 생성
                if *prediction > self.confidence_threshold && current_weight < self.max_position_size {
                    // 강한 상승 예측 - 매수
                    let target_weight = self.max_position_size.min(current_weight + 0.1);
                    let strength = (target_weight - current_weight).min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Buy { strength });
                } else if *prediction < -self.confidence_threshold && current_weight > 0.0 {
                    // 강한 하락 예측 - 매도
                    let strength = current_weight.min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength });
                } else {
                    signals.insert(symbol.clone(), TradingSignal::Hold);
                }
            } else {
                signals.insert(symbol.clone(), TradingSignal::Hold);
            }
        }
        
        debug!("ML 전략 신호 생성: {:?}", signals);
        Ok(signals)
    }
    
    fn get_name(&self) -> String {
        self.name.clone()
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("confidence_threshold".to_string(), self.confidence_threshold);
        params.insert("max_position_size".to_string(), self.max_position_size);
        params.insert("prediction_horizon".to_string(), self.prediction_horizon as f64);
        params
    }
}

/// Alpha Factor 전략
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaFactorStrategy {
    pub name: String,
    pub alpha_factors: HashMap<String, HashMap<String, f64>>, // 심볼별 Alpha Factor 값들
    pub factor_weights: HashMap<String, f64>, // Factor별 가중치
    pub entry_threshold: f64, // 진입 임계값
    pub exit_threshold: f64, // 청산 임계값
    pub max_position_size: f64, // 최대 포지션 크기
}

impl AlphaFactorStrategy {
    /// 새로운 Alpha Factor 전략 생성
    pub fn new(
        name: String,
        factor_weights: HashMap<String, f64>,
        entry_threshold: f64,
        exit_threshold: f64,
        max_position_size: f64,
    ) -> Self {
        Self {
            name,
            alpha_factors: HashMap::new(),
            factor_weights,
            entry_threshold,
            exit_threshold,
            max_position_size,
        }
    }
    
    /// Alpha Factor 값 업데이트
    pub fn update_alpha_factors(&mut self, symbol: String, factors: HashMap<String, f64>) {
        self.alpha_factors.insert(symbol, factors);
    }
    
    /// Alpha Score 계산 (가중합)
    fn calculate_alpha_score(&self, symbol: &str) -> Option<f64> {
        if let Some(factors) = self.alpha_factors.get(symbol) {
            let mut weighted_score = 0.0;
            let mut total_weight = 0.0;
            
            for (factor_name, factor_value) in factors {
                if let Some(weight) = self.factor_weights.get(factor_name) {
                    weighted_score += factor_value * weight;
                    total_weight += weight;
                }
            }
            
            if total_weight > 0.0 {
                return Some(weighted_score / total_weight);
            }
        }
        None
    }
}

impl TradingStrategy for AlphaFactorStrategy {
    fn generate_signals(
        &self,
        market_data: &HashMap<String, MarketData>,
        portfolio: &PortfolioSnapshot,
    ) -> Result<HashMap<String, TradingSignal>> {
        let mut signals = HashMap::new();
        
        for (symbol, _data) in market_data {
            let current_weight = portfolio.positions.get(symbol)
                .map(|p| p.weight)
                .unwrap_or(0.0);
            
            if let Some(alpha_score) = self.calculate_alpha_score(symbol) {
                // Alpha Score 기반 신호 생성
                if alpha_score > self.entry_threshold && current_weight < self.max_position_size {
                    // 높은 Alpha Score - 매수
                    let target_weight = self.max_position_size.min(current_weight + 0.1);
                    let strength = (target_weight - current_weight).min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Buy { strength });
                } else if alpha_score < -self.entry_threshold && current_weight > 0.0 {
                    // 낮은 Alpha Score - 매도
                    let strength = current_weight.min(0.2);
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength });
                } else if alpha_score.abs() < self.exit_threshold && current_weight > 0.0 {
                    // 중립적 Alpha Score - 포지션 청산
                    signals.insert(symbol.clone(), TradingSignal::Sell { strength: current_weight });
                } else {
                    signals.insert(symbol.clone(), TradingSignal::Hold);
                }
            } else {
                signals.insert(symbol.clone(), TradingSignal::Hold);
            }
        }
        
        debug!("Alpha Factor 전략 신호 생성: {:?}", signals);
        Ok(signals)
    }
    
    fn get_name(&self) -> String {
        self.name.clone()
    }
    
    fn get_parameters(&self) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        params.insert("entry_threshold".to_string(), self.entry_threshold);
        params.insert("exit_threshold".to_string(), self.exit_threshold);
        params.insert("max_position_size".to_string(), self.max_position_size);
        params
    }
}

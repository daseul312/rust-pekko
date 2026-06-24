//! 시장 데이터 함수형 Actor
//!
//! 실시간 및 과거 시장 데이터를 수집하고 관리합니다.

use pekko_actor::{FunctionalActor, FunctionalActorContext, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 시장 데이터 상태 (불변)
#[derive(Debug, Clone)]
pub struct MarketDataState {
    /// 심볼별 가격 히스토리
    pub price_history: HashMap<String, Vec<PricePoint>>,
    /// 심볼별 거래량 히스토리
    pub volume_history: HashMap<String, Vec<VolumePoint>>,
    /// 시장 지표
    pub market_indicators: HashMap<String, MarketIndicator>,
    /// 데이터 수집 설정
    pub config: MarketDataConfig,
    /// 마지막 업데이트 시간
    pub last_update: chrono::DateTime<chrono::Utc>,
}

/// 가격 데이터 포인트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricePoint {
    pub price: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub volume: u64,
    pub high: f64,
    pub low: f64,
    pub open: f64,
    pub close: f64,
}

/// 거래량 데이터 포인트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumePoint {
    pub volume: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub price: f64,
}

/// 시장 지표
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketIndicator {
    pub symbol: String,
    pub sma_20: Option<f64>,     // 20일 단순 이동 평균
    pub sma_50: Option<f64>,     // 50일 단순 이동 평균
    pub rsi: Option<f64>,        // 상대 강도 지수
    pub macd: Option<f64>,       // MACD
    pub bollinger_upper: Option<f64>, // 볼린저 밴드 상단
    pub bollinger_lower: Option<f64>, // 볼린저 밴드 하단
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

/// 시장 데이터 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataConfig {
    pub max_history_size: usize,
    pub update_interval_seconds: u64,
    pub enable_real_time: bool,
    pub enable_historical: bool,
    pub indicators_enabled: Vec<String>,
    pub data_sources: Vec<String>,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            max_history_size: 1000,
            update_interval_seconds: 60,
            enable_real_time: true,
            enable_historical: true,
            indicators_enabled: vec![
                "sma".to_string(),
                "rsi".to_string(),
                "macd".to_string(),
                "bollinger".to_string(),
            ],
            data_sources: vec![
                "alpha_vantage".to_string(),
                "yahoo_finance".to_string(),
            ],
        }
    }
}

impl MarketDataState {
    pub fn new() -> Self {
        Self {
            price_history: HashMap::new(),
            volume_history: HashMap::new(),
            market_indicators: HashMap::new(),
            config: MarketDataConfig::default(),
            last_update: chrono::Utc::now(),
        }
    }

    /// 가격 데이터 추가 (불변 방식)
    pub fn add_price_data(mut self, symbol: String, price_data: PricePoint) -> Self {
        let history = self.price_history.entry(symbol.clone()).or_insert_with(Vec::new);

        // 최대 히스토리 크기 제한
        if history.len() >= self.config.max_history_size {
            history.remove(0);
        }

        history.push(price_data);

        // 지표 재계산
        self = self.update_indicators(symbol);

        self.last_update = chrono::Utc::now();
        self
    }

    /// 거래량 데이터 추가
    pub fn add_volume_data(mut self, symbol: String, volume_data: VolumePoint) -> Self {
        let history = self.volume_history.entry(symbol).or_insert_with(Vec::new);

        // 최대 히스토리 크기 제한
        if history.len() >= self.config.max_history_size {
            history.remove(0);
        }

        history.push(volume_data);
        self.last_update = chrono::Utc::now();
        self
    }

    /// 기술적 지표 계산
    fn update_indicators(mut self, symbol: String) -> Self {
        if let Some(prices) = self.price_history.get(&symbol) {
            if prices.len() < 20 {
                return self; // 충분한 데이터가 없음
            }

            let closes: Vec<f64> = prices.iter().map(|p| p.close).collect();

            let indicator = MarketIndicator {
                symbol: symbol.clone(),
                sma_20: self.calculate_sma(&closes, 20),
                sma_50: if closes.len() >= 50 { self.calculate_sma(&closes, 50) } else { None },
                rsi: self.calculate_rsi(&closes, 14),
                macd: self.calculate_macd(&closes),
                bollinger_upper: self.calculate_bollinger_upper(&closes, 20),
                bollinger_lower: self.calculate_bollinger_lower(&closes, 20),
                last_updated: chrono::Utc::now(),
            };

            self.market_indicators.insert(symbol, indicator);
        }

        self
    }

    /// 단순 이동 평균 계산
    fn calculate_sma(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }

        let sum: f64 = prices.iter().rev().take(period).sum();
        Some(sum / period as f64)
    }

    /// RSI 계산
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

    /// MACD 계산 (단순 버전)
    fn calculate_macd(&self, prices: &[f64]) -> Option<f64> {
        if prices.len() < 26 {
            return None;
        }

        let ema_12 = self.calculate_ema(prices, 12)?;
        let ema_26 = self.calculate_ema(prices, 26)?;

        Some(ema_12 - ema_26)
    }

    /// EMA 계산
    fn calculate_ema(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }

        let multiplier = 2.0 / (period as f64 + 1.0);
        let mut ema = prices[0];

        for &price in &prices[1..] {
            ema = (price * multiplier) + (ema * (1.0 - multiplier));
        }

        Some(ema)
    }

    /// 볼린저 밴드 상단 계산
    fn calculate_bollinger_upper(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }

        let sma = self.calculate_sma(prices, period)?;
        let variance = self.calculate_variance(prices, period, sma)?;
        let std_dev = variance.sqrt();

        Some(sma + (std_dev * 2.0))
    }

    /// 볼린저 밴드 하단 계산
    fn calculate_bollinger_lower(&self, prices: &[f64], period: usize) -> Option<f64> {
        if prices.len() < period {
            return None;
        }

        let sma = self.calculate_sma(prices, period)?;
        let variance = self.calculate_variance(prices, period, sma)?;
        let std_dev = variance.sqrt();

        Some(sma - (std_dev * 2.0))
    }

    /// 분산 계산
    fn calculate_variance(&self, prices: &[f64], period: usize, mean: f64) -> Option<f64> {
        if prices.len() < period {
            return None;
        }

        let recent_prices = &prices[prices.len().saturating_sub(period)..];
        let sum_squared_diff: f64 = recent_prices.iter()
            .map(|price| (price - mean).powi(2))
            .sum();

        Some(sum_squared_diff / period as f64)
    }

    /// 특정 심볼의 최근 가격 조회
    pub fn get_recent_price(&self, symbol: &str) -> Option<&PricePoint> {
        self.price_history.get(symbol)?.last()
    }

    /// 특정 심볼의 가격 히스토리 조회
    pub fn get_price_history(&self, symbol: &str, limit: Option<usize>) -> Vec<&PricePoint> {
        if let Some(history) = self.price_history.get(symbol) {
            let limit = limit.unwrap_or(history.len());
            history.iter().rev().take(limit).collect::<Vec<_>>().into_iter().rev().collect()
        } else {
            Vec::new()
        }
    }

    /// 특정 심볼의 지표 조회
    pub fn get_indicators(&self, symbol: &str) -> Option<&MarketIndicator> {
        self.market_indicators.get(symbol)
    }

    /// 가격 변동률 계산
    pub fn calculate_price_change(&self, symbol: &str, periods: usize) -> Option<f64> {
        let history = self.price_history.get(symbol)?;

        if history.len() < periods + 1 {
            return None;
        }

        let current = history.last()?.close;
        let past = history[history.len() - periods - 1].close;

        Some((current - past) / past * 100.0)
    }
}

/// 시장 데이터 메시지
#[derive(Debug, Clone)]
pub enum MarketDataMessage {
    /// 가격 데이터 업데이트
    UpdatePriceData {
        symbol: String,
        price_data: PricePoint,
    },

    /// 거래량 데이터 업데이트
    UpdateVolumeData {
        symbol: String,
        volume_data: VolumePoint,
    },

    /// 가격 데이터 조회
    GetPriceData {
        symbol: String,
        limit: Option<usize>,
    },

    /// 지표 데이터 조회
    GetIndicators {
        symbol: String,
    },

    /// 가격 변동률 조회
    GetPriceChange {
        symbol: String,
        periods: usize,
    },

    /// 데이터 정제 (오래된 데이터 제거)
    CleanOldData {
        max_age_hours: u64,
    },

    /// 설정 업데이트
    UpdateConfig {
        config: MarketDataConfig,
    },
}

/// 시장 데이터 함수형 Actor
pub struct MarketDataActor;

impl MarketDataActor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MarketDataActor {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionalActor for MarketDataActor {
    type State = MarketDataState;
    type Message = MarketDataMessage;

    fn init(_ctx: &FunctionalActorContext) -> Self::State {
        MarketDataState::new()
    }

    fn receive(
        state: Self::State,
        msg: Self::Message,
        _ctx: &FunctionalActorContext,
    ) -> (Self::State, Effect) {
        match msg {
            MarketDataMessage::UpdatePriceData { symbol, price_data } => {
                let new_state = state.add_price_data(symbol, price_data);
                (new_state, Effect::None)
            }

            MarketDataMessage::UpdateVolumeData { symbol, volume_data } => {
                let new_state = state.add_volume_data(symbol, volume_data);
                (new_state, Effect::None)
            }

            MarketDataMessage::GetPriceData { symbol, limit } => {
                if let Some(recent_price) = state.get_recent_price(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 가격 데이터가 없습니다", symbol);
                }
                (state, Effect::None)
            }

            MarketDataMessage::GetIndicators { symbol } => {
                if let Some(indicators) = state.get_indicators(&symbol) {
                } else {
                    tracing::warn!("{} 심볼의 지표 데이터가 없습니다", symbol);
                }
                (state, Effect::None)
            }

            MarketDataMessage::GetPriceChange { symbol, periods } => {
                if let Some(change_percent) = state.calculate_price_change(&symbol, periods) {
                } else {
                    tracing::warn!("{} 심볼의 충분한 가격 데이터가 없습니다", symbol);
                }
                (state, Effect::None)
            }

            MarketDataMessage::CleanOldData { max_age_hours } => {
                // 실제로는 데이터 정리 로직 구현 필요
                (state, Effect::None)
            }

            MarketDataMessage::UpdateConfig { config } => {
                let mut new_state = state;
                new_state.config = config;
                (new_state, Effect::None)
            }
        }
    }

    fn apply_effect(
        _effect: Effect,
        _ctx: &mut FunctionalActorContext
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            // API 호출 등의 비동기 작업
            tracing::debug!("MarketDataActor Effect 적용");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_price_data() {
        let state = MarketDataState::new();

        let price_data = PricePoint {
            price: 150.0,
            timestamp: chrono::Utc::now(),
            volume: 1000000,
            high: 152.0,
            low: 148.0,
            open: 149.0,
            close: 150.0,
        };

        let new_state = state.add_price_data("AAPL".to_string(), price_data);

        assert!(new_state.price_history.contains_key("AAPL"));
        assert_eq!(new_state.price_history["AAPL"].len(), 1);
    }

    #[test]
    fn test_calculate_sma() {
        let state = MarketDataState::new();

        // 충분한 데이터를 만들기 위해 25개의 가격 포인트 생성
        let mut state_with_data = state;
        for i in 0..25 {
            let price = 100.0 + i as f64;
            let price_data = PricePoint {
                price,
                timestamp: chrono::Utc::now(),
                volume: 1000000,
                high: price + 1.0,
                low: price - 1.0,
                open: price - 0.5,
                close: price,
            };
            state_with_data = state_with_data.add_price_data("AAPL".to_string(), price_data);
        }

        let indicators = state_with_data.get_indicators("AAPL").unwrap();
        assert!(indicators.sma_20.is_some());
    }

    #[test]
    fn test_get_recent_price() {
        let state = MarketDataState::new();

        let price_data = PricePoint {
            price: 150.0,
            timestamp: chrono::Utc::now(),
            volume: 1000000,
            high: 152.0,
            low: 148.0,
            open: 149.0,
            close: 150.0,
        };

        let state_with_data = state.add_price_data("AAPL".to_string(), price_data);
        let recent_price = state_with_data.get_recent_price("AAPL");

        assert!(recent_price.is_some());
        assert_eq!(recent_price.unwrap().close, 150.0);
    }
}
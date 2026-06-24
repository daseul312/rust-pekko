//! Stream-based Trading Pipeline
//!
//! Uses pekko-streams for reactive, backpressure-aware trading pipelines.
//! 
//! Pipeline: 실시간시세 → 분석 → 매매신호 → 주문실행
//!
//! ## Features
//! - Backpressure handling for high-frequency data
//! - Composable trading strategies as Flow operators
//! - Actor integration for order execution

use crate::korea_investment_api::{KoreaInvestmentAPI, StockPrice};
use crate::models::Stock;
use crate::news_analysis::{NewsReader, KeywordSentimentAnalyzer, BatchSentiment, NewsTradingSignal, Sentiment};
use pekko_streams::prelude::*;
use tokio::sync::mpsc;
use std::time::Duration;
use tracing::{ warn, error};
use anyhow::Result;

// ============= Trading Signal Types =============

/// 매매 신호
#[derive(Debug, Clone)]
pub enum TradingSignal {
    Buy { symbol: String, price: f64, reason: String },
    Sell { symbol: String, price: f64, reason: String },
    Hold { symbol: String },
}

impl TradingSignal {
    pub fn symbol(&self) -> &str {
        match self {
            TradingSignal::Buy { symbol, .. } => symbol,
            TradingSignal::Sell { symbol, .. } => symbol,
            TradingSignal::Hold { symbol } => symbol,
        }
    }

    pub fn is_actionable(&self) -> bool {
        !matches!(self, TradingSignal::Hold { .. })
    }
}

/// 시세 데이터 with 메타정보
#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub symbol: String,
    pub price: StockPrice,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 분석된 시세 데이터
#[derive(Debug, Clone)]
pub struct AnalyzedPrice {
    pub update: PriceUpdate,
    pub volatility: f64,
    pub trend: Trend,
    pub signal_strength: f64,  // 0.0 ~ 1.0
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trend {
    StrongUp,
    Up,
    Neutral,
    Down,
    StrongDown,
}

// ============= Price Stream Source =============

/// 실시간 시세 스트림 생성
/// Actor에서 시세 데이터를 받아 스트림으로 변환
pub fn price_stream(buffer_size: usize) -> (Source<PriceUpdate>, mpsc::Sender<PriceUpdate>) {
    ActorSourceBuilder::new(buffer_size).build()
}

/// API 폴링 기반 시세 스트림 (테스트/백테스트용)
pub struct PollingPriceSource {
    api: KoreaInvestmentAPI,
    symbols: Vec<String>,
    interval: Duration,
}

impl PollingPriceSource {
    pub fn new(api: KoreaInvestmentAPI, symbols: Vec<String>, interval: Duration) -> Self {
        Self { api, symbols, interval }
    }

    /// 폴링 시작 - 시세를 주기적으로 조회하여 스트림에 푸시
    pub async fn start(self, sender: mpsc::Sender<PriceUpdate>) {
        let mut interval = tokio::time::interval(self.interval);
        
        loop {
            interval.tick().await;
            
            for symbol in &self.symbols {
                match self.api.get_stock_price(symbol).await {
                    Ok(price) => {
                        let update = PriceUpdate {
                            symbol: symbol.clone(),
                            price,
                            timestamp: chrono::Utc::now(),
                        };
                        
                        if sender.send(update).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("[PriceSource] Failed to fetch {}: {}", symbol, e);
                    }
                }
            }
        }
    }
}

// ============= Analysis Flow =============

/// 기술적 분석 Flow
pub fn analyze_flow() -> Flow<PriceUpdate, AnalyzedPrice> {
    Flow::map(|update: PriceUpdate| {
        let volatility = update.price.volatility();
        let change_rate = update.price.change_rate.parse::<f64>().unwrap_or(0.0);
        
        let trend = match change_rate {
            r if r > 3.0 => Trend::StrongUp,
            r if r > 0.5 => Trend::Up,
            r if r > -0.5 => Trend::Neutral,
            r if r > -3.0 => Trend::Down,
            _ => Trend::StrongDown,
        };
        
        // 신호 강도: 변동성 + 등락률 기반
        let signal_strength = (volatility * 0.3 + change_rate.abs() * 0.1).min(1.0);
        
        AnalyzedPrice {
            update,
            volatility,
            trend,
            signal_strength,
        }
    })
}

// ============= Signal Generation Flow =============

/// 매매 신호 생성 전략
pub trait StreamTradingStrategy: Send + Sync {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal;
    fn name(&self) -> &str;
}

/// 모멘텀 전략
#[derive(Debug, Clone)]
pub struct MomentumStrategy {
    pub buy_threshold: f64,   // 매수 신호 강도 임계값
    pub sell_threshold: f64,  // 매도 신호 강도 임계값
}

impl Default for MomentumStrategy {
    fn default() -> Self {
        Self {
            buy_threshold: 0.6,
            sell_threshold: 0.4,
        }
    }
}

impl StreamTradingStrategy for MomentumStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        
        match analyzed.trend {
            Trend::StrongUp if analyzed.signal_strength >= self.buy_threshold => {
                TradingSignal::Buy {
                    symbol,
                    price,
                    reason: format!("Strong uptrend with {:.0}% signal strength", 
                                   analyzed.signal_strength * 100.0),
                }
            }
            Trend::StrongDown if analyzed.signal_strength >= self.sell_threshold => {
                TradingSignal::Sell {
                    symbol,
                    price,
                    reason: format!("Strong downtrend with {:.0}% signal strength",
                                   analyzed.signal_strength * 100.0),
                }
            }
            _ => TradingSignal::Hold { symbol },
        }
    }

    fn name(&self) -> &str {
        "Momentum"
    }
}

/// 평균회귀 전략
#[derive(Debug, Clone)]
pub struct MeanReversionStrategy {
    pub oversold_threshold: f64,   // 과매도 판단 등락률
    pub overbought_threshold: f64, // 과매수 판단 등락률
}

impl Default for MeanReversionStrategy {
    fn default() -> Self {
        Self {
            oversold_threshold: -5.0,
            overbought_threshold: 5.0,
        }
    }
}

impl StreamTradingStrategy for MeanReversionStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        let change_rate = analyzed.update.price.change_rate.parse::<f64>().unwrap_or(0.0);
        
        if change_rate <= self.oversold_threshold {
            TradingSignal::Buy {
                symbol,
                price,
                reason: format!("Oversold: {:.2}% drop, expecting mean reversion", change_rate),
            }
        } else if change_rate >= self.overbought_threshold {
            TradingSignal::Sell {
                symbol,
                price,
                reason: format!("Overbought: {:.2}% gain, expecting mean reversion", change_rate),
            }
        } else {
            TradingSignal::Hold { symbol }
        }
    }

    fn name(&self) -> &str {
        "MeanReversion"
    }
}

// ============= RSI Strategy =============

/// RSI (Relative Strength Index) 전략
/// RSI < 30: 과매도 → 매수
/// RSI > 70: 과매수 → 매도
#[derive(Debug, Clone)]
pub struct RSIStrategy {
    pub oversold: f64,    // 과매도 임계값 (기본 30)
    pub overbought: f64,  // 과매수 임계값 (기본 70)
    pub period: usize,    // RSI 계산 기간 (기본 14)
}

impl Default for RSIStrategy {
    fn default() -> Self {
        Self {
            oversold: 30.0,
            overbought: 70.0,
            period: 14,
        }
    }
}

impl RSIStrategy {
    /// 등락률로 간이 RSI 계산 (실제 RSI는 가격 히스토리 필요)
    fn estimate_rsi(&self, change_rate: f64) -> f64 {
        // 간이 변환: -5% ~ +5% 등락률을 0~100 RSI로 매핑
        let normalized = (change_rate + 5.0) / 10.0; // -5~+5 → 0~1
        (normalized * 100.0).clamp(0.0, 100.0)
    }
}

impl StreamTradingStrategy for RSIStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        let change_rate = analyzed.update.price.change_rate.parse::<f64>().unwrap_or(0.0);
        
        let rsi = self.estimate_rsi(change_rate);
        
        if rsi < self.oversold {
            TradingSignal::Buy {
                symbol,
                price,
                reason: format!("RSI oversold: {:.1} < {:.0}", rsi, self.oversold),
            }
        } else if rsi > self.overbought {
            TradingSignal::Sell {
                symbol,
                price,
                reason: format!("RSI overbought: {:.1} > {:.0}", rsi, self.overbought),
            }
        } else {
            TradingSignal::Hold { symbol }
        }
    }

    fn name(&self) -> &str {
        "RSI"
    }
}

// ============= MACD Strategy =============

/// MACD (Moving Average Convergence Divergence) 전략
/// MACD > Signal: 골든크로스 → 매수
/// MACD < Signal: 데드크로스 → 매도
#[derive(Debug, Clone)]
pub struct MACDStrategy {
    pub fast_period: usize,   // 빠른 EMA (기본 12)
    pub slow_period: usize,   // 느린 EMA (기본 26)
    pub signal_period: usize, // 시그널 (기본 9)
}

impl Default for MACDStrategy {
    fn default() -> Self {
        Self {
            fast_period: 12,
            slow_period: 26,
            signal_period: 9,
        }
    }
}

impl MACDStrategy {
    /// 등락률과 신호강도로 MACD 크로스 추정
    fn estimate_cross(&self, change_rate: f64, signal_strength: f64) -> (f64, f64) {
        // 간이 MACD: 단기 등락률 vs 신호강도
        let macd = change_rate * 0.7 + (signal_strength - 0.5) * 2.0 * 0.3;
        let signal = change_rate * 0.3 + (signal_strength - 0.5) * 2.0 * 0.7;
        (macd, signal)
    }
}

impl StreamTradingStrategy for MACDStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        let change_rate = analyzed.update.price.change_rate.parse::<f64>().unwrap_or(0.0);
        
        let signal_strength = analyzed.signal_strength;
        let (macd, signal) = self.estimate_cross(change_rate, signal_strength);
        let histogram = macd - signal;
        
        if histogram > 0.5 && macd > 0.0 {
            TradingSignal::Buy {
                symbol,
                price,
                reason: format!("MACD golden cross: {:.2} > {:.2}", macd, signal),
            }
        } else if histogram < -0.5 && macd < 0.0 {
            TradingSignal::Sell {
                symbol,
                price,
                reason: format!("MACD death cross: {:.2} < {:.2}", macd, signal),
            }
        } else {
            TradingSignal::Hold { symbol }
        }
    }

    fn name(&self) -> &str {
        "MACD"
    }
}

// ============= Bollinger Bands Strategy =============

/// 볼린저밴드 전략
/// 하단밴드 돌파: 과매도 → 매수
/// 상단밴드 돌파: 과매수 → 매도
#[derive(Debug, Clone)]
pub struct BollingerBandsStrategy {
    pub period: usize,      // 이동평균 기간 (기본 20)
    pub std_dev: f64,       // 표준편차 배수 (기본 2.0)
}

impl Default for BollingerBandsStrategy {
    fn default() -> Self {
        Self {
            period: 20,
            std_dev: 2.0,
        }
    }
}

impl BollingerBandsStrategy {
    /// 등락률로 밴드 위치 추정 (0~1, 0=하단, 1=상단)
    fn estimate_band_position(&self, change_rate: f64) -> f64 {
        // 간이 계산: ±4% 범위를 밴드로 가정
        let band_range = 4.0 * self.std_dev / 2.0;
        let position = (change_rate + band_range) / (2.0 * band_range);
        position.clamp(0.0, 1.0)
    }
}

impl StreamTradingStrategy for BollingerBandsStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        let change_rate = analyzed.update.price.change_rate.parse::<f64>().unwrap_or(0.0);
        
        let position = self.estimate_band_position(change_rate);
        
        if position < 0.1 {
            TradingSignal::Buy {
                symbol,
                price,
                reason: format!("BB lower band touch: {:.1}% (pos={:.2})", change_rate, position),
            }
        } else if position > 0.9 {
            TradingSignal::Sell {
                symbol,
                price,
                reason: format!("BB upper band touch: {:.1}% (pos={:.2})", change_rate, position),
            }
        } else {
            TradingSignal::Hold { symbol }
        }
    }

    fn name(&self) -> &str {
        "BollingerBands"
    }
}

// ============= Combined Strategy =============

/// 복합 전략: RSI + MACD + Momentum 종합
#[derive(Debug, Clone)]
pub struct CombinedStrategy {
    pub momentum: MomentumStrategy,
    pub rsi: RSIStrategy,
    pub macd: MACDStrategy,
    pub min_agreement: usize,  // 최소 동의 수 (기본 2)
}

impl Default for CombinedStrategy {
    fn default() -> Self {
        Self {
            momentum: MomentumStrategy::default(),
            rsi: RSIStrategy::default(),
            macd: MACDStrategy::default(),
            min_agreement: 2,
        }
    }
}

impl CombinedStrategy {
    pub fn with_min_agreement(mut self, min: usize) -> Self {
        self.min_agreement = min;
        self
    }
}

impl StreamTradingStrategy for CombinedStrategy {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let symbol = analyzed.update.symbol.clone();
        let price = analyzed.update.price.current_price.parse::<f64>().unwrap_or(0.0);
        
        let mut buy_votes = 0;
        let mut sell_votes = 0;
        let mut reasons = Vec::new();
        
        // Momentum
        match self.momentum.generate_signal(analyzed) {
            TradingSignal::Buy { .. } => { buy_votes += 1; reasons.push("Momentum:BUY".to_string()); }
            TradingSignal::Sell { .. } => { sell_votes += 1; reasons.push("Momentum:SELL".to_string()); }
            _ => {}
        }
        
        // RSI
        match self.rsi.generate_signal(analyzed) {
            TradingSignal::Buy { .. } => { buy_votes += 1; reasons.push("RSI:BUY".to_string()); }
            TradingSignal::Sell { .. } => { sell_votes += 1; reasons.push("RSI:SELL".to_string()); }
            _ => {}
        }
        
        // MACD
        match self.macd.generate_signal(analyzed) {
            TradingSignal::Buy { .. } => { buy_votes += 1; reasons.push("MACD:BUY".to_string()); }
            TradingSignal::Sell { .. } => { sell_votes += 1; reasons.push("MACD:SELL".to_string()); }
            _ => {}
        }
        
        let reason = reasons.join(", ");
        
        if buy_votes >= self.min_agreement {
            TradingSignal::Buy {
                symbol,
                price,
                reason: format!("Combined BUY ({}/3) [{}]", buy_votes, reason),
            }
        } else if sell_votes >= self.min_agreement {
            TradingSignal::Sell {
                symbol,
                price,
                reason: format!("Combined SELL ({}/3) [{}]", sell_votes, reason),
            }
        } else {
            TradingSignal::Hold { symbol }
        }
    }

    fn name(&self) -> &str {
        "Combined"
    }
}

/// 전략 기반 신호 생성 Flow
pub fn signal_flow<S: StreamTradingStrategy + Clone + 'static>(strategy: S) -> Flow<AnalyzedPrice, TradingSignal> {
    Flow::map(move |analyzed: AnalyzedPrice| {
        strategy.generate_signal(&analyzed)
    })
}

/// 실행 가능한 신호만 필터링
pub fn actionable_only() -> Flow<TradingSignal, TradingSignal> {
    Flow::<TradingSignal, TradingSignal>::filter(|signal| signal.is_actionable())
}

// ============= Order Execution Sink =============

/// 주문 실행 결과
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub signal: TradingSignal,
    pub executed: bool,
    pub order_id: Option<String>,
    pub message: String,
}

/// 로깅 전용 Sink (드라이런 모드)
pub fn logging_sink() -> Sink<TradingSignal, ()> {
    Sink::foreach(|signal: TradingSignal| {
        match &signal {
            TradingSignal::Buy { symbol, price, reason } => {
            }
            TradingSignal::Sell { symbol, price, reason } => {
            }
            TradingSignal::Hold { symbol } => {
                // Hold는 로깅하지 않음 (너무 많음)
                let _ = symbol;
            }
        }
    })
}

/// 주문 실행 Sink (실제 매매)
/// 주문 실행 설정
#[derive(Debug, Clone)]
pub struct OrderConfig {
    /// 드라이런 모드 (실제 주문 안 함)
    pub dry_run: bool,
    /// 기본 주문 수량
    pub default_quantity: u32,
    /// 최대 1회 주문 금액 (원)
    pub max_order_amount: u64,
}

impl Default for OrderConfig {
    fn default() -> Self {
        Self {
            dry_run: true,  // 기본값: 드라이런 (안전)
            default_quantity: 1,
            max_order_amount: 100_000,  // 10만원
        }
    }
}

impl OrderConfig {
    /// 라이브 트레이딩 모드
    pub fn live() -> Self {
        Self {
            dry_run: false,
            ..Default::default()
        }
    }
    
    /// 주어진 가격으로 살 수 있는 최대 수량 계산
    pub fn calculate_quantity(&self, price: f64) -> u32 {
        if price <= 0.0 {
            return self.default_quantity;
        }
        
        let max_qty = (self.max_order_amount as f64 / price).floor() as u32;
        max_qty.max(1).min(self.default_quantity.max(max_qty))
    }
}

/// 주문 실행 Sink (실제 매매)
/// sender로 결과를 전달받을 수 있음
pub fn order_execution_sink(
    api: KoreaInvestmentAPI,
    config: OrderConfig,
    result_sender: mpsc::Sender<OrderResult>,
) -> Sink<TradingSignal, ()> {
    Sink::from_stage(OrderExecutionStage {
        api,
        config,
        result_sender,
    })
}

struct OrderExecutionStage {
    api: KoreaInvestmentAPI,
    config: OrderConfig,
    result_sender: mpsc::Sender<OrderResult>,
}

#[async_trait::async_trait]
impl pekko_streams::stage::SinkStage<TradingSignal, ()> for OrderExecutionStage {
    async fn process(&mut self, signal: TradingSignal) -> StreamResult<()> {
        let result = match &signal {
            TradingSignal::Buy { symbol, price, reason } => {
                let quantity = self.config.calculate_quantity(*price);
                let price_u32 = *price as u32;
                
                if self.config.dry_run {
                    OrderResult {
                        signal: signal.clone(),
                        executed: false,
                        order_id: None,
                        message: format!("[DRY-RUN] Would buy {} x {} @ {}원", symbol, quantity, price_u32),
                    }
                } else {
                    match self.api.buy_stock(symbol, quantity, price_u32).await {
                        Ok(order) => {
                            OrderResult {
                                signal: signal.clone(),
                                executed: true,
                                order_id: order.order_no,
                                message: format!("Buy executed: {}", reason),
                            }
                        }
                        Err(e) => {
                            error!("❌ Buy order failed: {}", e);
                            OrderResult {
                                signal: signal.clone(),
                                executed: false,
                                order_id: None,
                                message: format!("Buy failed: {}", e),
                            }
                        }
                    }
                }
            }
            TradingSignal::Sell { symbol, price, reason } => {
                let quantity = self.config.default_quantity;  // 매도는 보유량 기준으로 해야 함
                let price_u32 = *price as u32;
                
                if self.config.dry_run {
                    OrderResult {
                        signal: signal.clone(),
                        executed: false,
                        order_id: None,
                        message: format!("[DRY-RUN] Would sell {} x {} @ {}원", symbol, quantity, price_u32),
                    }
                } else {
                    match self.api.sell_stock(symbol, quantity, price_u32).await {
                        Ok(order) => {
                            OrderResult {
                                signal: signal.clone(),
                                executed: true,
                                order_id: order.order_no,
                                message: format!("Sell executed: {}", reason),
                            }
                        }
                        Err(e) => {
                            error!("❌ Sell order failed: {}", e);
                            OrderResult {
                                signal: signal.clone(),
                                executed: false,
                                order_id: None,
                                message: format!("Sell failed: {}", e),
                            }
                        }
                    }
                }
            }
            TradingSignal::Hold { symbol } => {
                // Hold는 로깅도 안 함 (너무 많음)
                OrderResult {
                    signal: signal.clone(),
                    executed: false,
                    order_id: None,
                    message: format!("Holding {}", symbol),
                }
            }
        };

        let _ = self.result_sender.send(result).await;
        Ok(())
    }

    async fn on_complete(&mut self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&mut self, error: String) -> StreamResult<()> {
        error!("Order execution error: {}", error);
        Ok(())
    }
}

// ============= Complete Trading Pipeline =============

/// 완성된 트레이딩 파이프라인
pub struct TradingPipeline {
    pub buffer_size: usize,
}

impl TradingPipeline {
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }

    /// 드라이런 파이프라인 (로깅만)
    pub fn dry_run<S: StreamTradingStrategy + Clone + 'static>(
        &self,
        strategy: S,
    ) -> (Source<TradingSignal>, mpsc::Sender<PriceUpdate>) {
        let (source, sender) = price_stream(self.buffer_size);
        
        let processed = source
            .via(analyze_flow())
            .via(signal_flow(strategy))
            .via(actionable_only());
        
        (processed, sender)
    }

    /// 라이브 트레이딩 파이프라인 (드라이런)
    pub async fn live_trading<S: StreamTradingStrategy + Clone + 'static>(
        &self,
        strategy: S,
        api: KoreaInvestmentAPI,
    ) -> (mpsc::Sender<PriceUpdate>, mpsc::Receiver<OrderResult>) {
        self.live_trading_with_config(strategy, api, OrderConfig::default()).await
    }
    
    /// 라이브 트레이딩 파이프라인 (설정 가능)
    pub async fn live_trading_with_config<S: StreamTradingStrategy + Clone + 'static>(
        &self,
        strategy: S,
        api: KoreaInvestmentAPI,
        config: OrderConfig,
    ) -> (mpsc::Sender<PriceUpdate>, mpsc::Receiver<OrderResult>) {
        let (source, price_sender) = price_stream(self.buffer_size);
        let (result_sender, result_receiver) = mpsc::channel(self.buffer_size);
        
        let mode = if config.dry_run { "DRY-RUN" } else { "LIVE" };
        let sink = order_execution_sink(api, config, result_sender);
        
        // 백그라운드에서 파이프라인 실행
        tokio::spawn(async move {
            let result = source
                .via(analyze_flow())
                .via(signal_flow(strategy))
                .via(actionable_only())
                .run_with(sink)
                .await;
            
            if let Err(e) = result {
                error!("Trading pipeline error: {:?}", e);
            }
        });
        
        (price_sender, result_receiver)
    }
    
    /// 실제 매매 파이프라인 (LIVE 모드)
    pub async fn real_trading<S: StreamTradingStrategy + Clone + 'static>(
        &self,
        strategy: S,
        api: KoreaInvestmentAPI,
        max_order_amount: u64,
    ) -> (mpsc::Sender<PriceUpdate>, mpsc::Receiver<OrderResult>) {
        let config = OrderConfig {
            dry_run: false,
            default_quantity: 1,
            max_order_amount,
        };
        warn!("⚠️ LIVE TRADING MODE - Real orders will be placed!");
        self.live_trading_with_config(strategy, api, config).await
    }
}

// ============= News Sentiment Integration =============

/// 뉴스 감성 기반 신호 조정
#[derive(Debug, Clone)]
pub struct NewsSentimentAdjuster {
    /// 뉴스 감성이 신호에 미치는 가중치 (0.0 ~ 1.0)
    pub news_weight: f64,
    /// 최근 뉴스만 고려할 시간 (시간 단위)
    pub news_hours: i64,
}

impl Default for NewsSentimentAdjuster {
    fn default() -> Self {
        Self {
            news_weight: 0.3,  // 30% 뉴스 영향
            news_hours: 24,    // 최근 24시간 뉴스
        }
    }
}

impl NewsSentimentAdjuster {
    /// 뉴스 감성으로 신호 강도 조정
    pub fn adjust_signal(&self, signal: TradingSignal, news_sentiment: &BatchSentiment) -> TradingSignal {
        let news_score = news_sentiment.average_score;
        
        match signal {
            TradingSignal::Buy { symbol, price, reason } => {
                // 긍정 뉴스면 매수 신호 강화, 부정이면 약화
                if news_score > 0.2 {
                    TradingSignal::Buy {
                        symbol,
                        price,
                        reason: format!("{} [뉴스 긍정 +{:.0}%]", reason, news_score * 100.0),
                    }
                } else if news_score < -0.2 {
                    // 부정 뉴스가 강하면 Hold로 전환
                    TradingSignal::Hold { symbol }
                } else {
                    TradingSignal::Buy { symbol, price, reason }
                }
            }
            TradingSignal::Sell { symbol, price, reason } => {
                // 부정 뉴스면 매도 신호 강화, 긍정이면 약화
                if news_score < -0.2 {
                    TradingSignal::Sell {
                        symbol,
                        price,
                        reason: format!("{} [뉴스 부정 {:.0}%]", reason, news_score * 100.0),
                    }
                } else if news_score > 0.3 {
                    // 긍정 뉴스가 강하면 Hold로 전환
                    TradingSignal::Hold { symbol }
                } else {
                    TradingSignal::Sell { symbol, price, reason }
                }
            }
            hold => hold,
        }
    }
}

/// 뉴스 + 기술적 분석 통합 전략
#[derive(Clone)]
pub struct NewsAwareStrategy<S: StreamTradingStrategy> {
    base_strategy: S,
    adjuster: NewsSentimentAdjuster,
    news_sentiment: Option<BatchSentiment>,
}

impl<S: StreamTradingStrategy + Clone> NewsAwareStrategy<S> {
    pub fn new(base_strategy: S) -> Self {
        Self {
            base_strategy,
            adjuster: NewsSentimentAdjuster::default(),
            news_sentiment: None,
        }
    }

    pub fn with_adjuster(mut self, adjuster: NewsSentimentAdjuster) -> Self {
        self.adjuster = adjuster;
        self
    }

    /// 뉴스 감성 업데이트
    pub fn update_news_sentiment(&mut self, sentiment: BatchSentiment) {
        self.news_sentiment = Some(sentiment);
    }

    /// 최신 뉴스 분석 수행
    pub fn refresh_news(&mut self) -> Result<()> {
        let reader = NewsReader::new();
        let articles = reader.load_recent_news(self.adjuster.news_hours)?;
        
        let analyzer = KeywordSentimentAnalyzer::new();
        let sentiment = analyzer.analyze_batch(&articles);
        
        self.update_news_sentiment(sentiment);
        Ok(())
    }
}

impl<S: StreamTradingStrategy + Clone> StreamTradingStrategy for NewsAwareStrategy<S> {
    fn generate_signal(&self, analyzed: &AnalyzedPrice) -> TradingSignal {
        let base_signal = self.base_strategy.generate_signal(analyzed);
        
        // 뉴스 감성이 있으면 조정
        if let Some(ref sentiment) = self.news_sentiment {
            self.adjuster.adjust_signal(base_signal, sentiment)
        } else {
            base_signal
        }
    }

    fn name(&self) -> &str {
        "NewsAware"
    }
}

/// 통합 파이프라인: 뉴스 + 시세 → 매매신호
pub struct IntegratedTradingPipeline {
    pipeline: TradingPipeline,
    news_refresh_interval: Duration,
}

impl IntegratedTradingPipeline {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            pipeline: TradingPipeline::new(buffer_size),
            news_refresh_interval: Duration::from_secs(3600), // 1시간마다 뉴스 갱신
        }
    }

    /// 뉴스 통합 드라이런
    pub fn news_aware_dry_run<S: StreamTradingStrategy + Clone + 'static>(
        &self,
        base_strategy: S,
    ) -> Result<(Source<TradingSignal>, mpsc::Sender<PriceUpdate>)> {
        let mut strategy = NewsAwareStrategy::new(base_strategy);
        strategy.refresh_news()?;
        
        Ok(self.pipeline.dry_run(strategy))
    }

    /// 뉴스 감성 리포트 생성
    pub fn generate_news_report(&self) -> Result<String> {
        let reader = NewsReader::new();
        let articles = reader.load_recent_news(24)?;
        
        let analyzer = KeywordSentimentAnalyzer::new();
        let sentiment = analyzer.analyze_batch(&articles);
        
        let recommendation = sentiment.trading_recommendation();
        
        Ok(format!(
            "📊 뉴스 분석 리포트\n\
            ━━━━━━━━━━━━━━━━━━━━\n\
            {}\n\
            \n\
            📌 매매 추천: {} {}\n\
            \n\
            🔥 주요 뉴스:\n\
            {}",
            sentiment.summary(),
            recommendation.emoji(),
            format!("{:?}", recommendation),
            sentiment.articles
                .iter()
                .take(5)
                .map(|a| format!("  • {} [{:?}]", a.article.title, a.sentiment))
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_momentum_strategy_buy() {
        let strategy = MomentumStrategy::default();
        
        let analyzed = AnalyzedPrice {
            update: PriceUpdate {
                symbol: "005930".to_string(),
                price: StockPrice {
                    current_price: "70000".to_string(),
                    price_change: "2000".to_string(),
                    change_rate: "3.5".to_string(),
                    volume: "1000000".to_string(),
                    open_price: "68000".to_string(),
                    high_price: "71000".to_string(),
                    low_price: "67500".to_string(),
                },
                timestamp: chrono::Utc::now(),
            },
            volatility: 5.0,
            trend: Trend::StrongUp,
            signal_strength: 0.8,
        };
        
        let signal = strategy.generate_signal(&analyzed);
        assert!(matches!(signal, TradingSignal::Buy { .. }));
    }

    #[tokio::test]
    async fn test_pipeline_dry_run() {
        let pipeline = TradingPipeline::new(10);
        let strategy = MomentumStrategy::default();
        
        let (source, sender) = pipeline.dry_run(strategy);
        
        // 테스트 데이터 전송
        sender.send(PriceUpdate {
            symbol: "005930".to_string(),
            price: StockPrice {
                current_price: "70000".to_string(),
                price_change: "2500".to_string(),
                change_rate: "4.0".to_string(),
                volume: "1000000".to_string(),
                open_price: "68000".to_string(),
                high_price: "71000".to_string(),
                low_price: "67500".to_string(),
            },
            timestamp: chrono::Utc::now(),
        }).await.unwrap();
        
        drop(sender);
        
        let signals = source.run_collect().await.unwrap();
        assert!(!signals.is_empty());
    }
}

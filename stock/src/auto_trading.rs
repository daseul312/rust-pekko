use crate::user_account::{UserAccount, TradingStrategy, TradingSignal, TransactionType};
use crate::korean_models::{KoreanStock, KoreanRiskLevel};
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{ warn, error};
use chrono::Utc;

/// 자동 매매 엔진 액터
#[derive(Debug, Clone)]
pub struct AutoTradingEngine {
    user_accounts: HashMap<String, UserAccount>,
    trading_strategies: HashMap<String, TradingStrategy>,
    stock_prices: HashMap<String, f64>,
    market_analysis: MarketAnalysis,
}

#[derive(Debug, Clone)]
pub struct MarketAnalysis {
    pub market_trend: MarketTrend,
    pub volatility: f64,
    pub analysis_time: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum MarketTrend {
    Bullish,    // 상승장
    Bearish,    // 하락장
    Sideways,   // 횡보장
}

impl AutoTradingEngine {
    pub fn new() -> Self {
        Self {
            user_accounts: HashMap::new(),
            trading_strategies: HashMap::new(),
            stock_prices: HashMap::new(),
            market_analysis: MarketAnalysis {
                market_trend: MarketTrend::Sideways,
                volatility: 0.2,
                analysis_time: Utc::now(),
            },
        }
    }

    /// 사용자 계정 등록
    pub fn register_user(&mut self, user_id: String, username: String, initial_cash: f64) {
        let account = UserAccount::new(user_id.clone(), username, initial_cash);
        self.user_accounts.insert(user_id.clone(), account);
    }

    /// 매매 전략 설정
    pub fn set_trading_strategy(&mut self, user_id: String, strategy: TradingStrategy) {
        self.trading_strategies.insert(user_id.clone(), strategy);
    }

    /// 주식 가격 업데이트
    pub fn update_stock_prices(&mut self, prices: HashMap<String, f64>) {
        self.stock_prices = prices;
        
        // 모든 사용자 계정 업데이트
        for (_, account) in self.user_accounts.iter_mut() {
            account.update_account(&self.stock_prices);
        }
    }

    /// 매매 신호 생성
    pub fn generate_trading_signals(&self, user_id: &str) -> Vec<TradingSignal> {
        let mut signals = Vec::new();

        if let (Some(account), Some(strategy)) = (
            self.user_accounts.get(user_id),
            self.trading_strategies.get(user_id)
        ) {
            if !strategy.auto_trading {
                return signals;
            }

            // 1. 수익 실현 신호 (목표 수익률 달성)
            for (symbol, position) in &account.portfolio {
                if position.profit_loss_rate >= strategy.profit_target {
                    signals.push(TradingSignal::Sell {
                        symbol: symbol.clone(),
                        stock_name: position.stock_name.clone(),
                        quantity: position.quantity,
                        price: position.current_price,
                        reason: format!("목표 수익률 달성: {:.1}%", position.profit_loss_rate),
                        confidence: 0.9,
                    });
                }
            }

            // 2. 손절매 신호
            for (symbol, position) in &account.portfolio {
                if position.profit_loss_rate <= strategy.stop_loss {
                    signals.push(TradingSignal::Sell {
                        symbol: symbol.clone(),
                        stock_name: position.stock_name.clone(),
                        quantity: position.quantity,
                        price: position.current_price,
                        reason: format!("손절매: {:.1}%", position.profit_loss_rate),
                        confidence: 0.95,
                    });
                }
            }

            // 3. 매수 신호 (현금 비율이 높을 때)
            let cash_ratio = account.get_cash_ratio();
            if cash_ratio > (1.0 - strategy.min_cash_ratio) {
                // 추천 주식 목록에서 매수할 종목 선택
                let buy_candidates = self.get_buy_candidates(account, strategy);
                for candidate in buy_candidates {
                    signals.push(TradingSignal::Buy {
                        symbol: candidate.symbol,
                        stock_name: candidate.stock_name,
                        quantity: candidate.quantity,
                        price: candidate.price,
                        reason: candidate.reason,
                        confidence: candidate.confidence,
                    });
                }
            }

            // 4. 리밸런싱 신호
            if self.needs_rebalancing(account, strategy) {
                let target_allocation = self.calculate_target_allocation(account, strategy);
                signals.push(TradingSignal::Rebalance {
                    target_allocation,
                    reason: "포트폴리오 리밸런싱 필요".to_string(),
                });
            }
        }

        signals
    }

    /// 매수 후보 종목 선정
    fn get_buy_candidates(&self, account: &UserAccount, strategy: &TradingStrategy) -> Vec<BuyCandidate> {
        let mut candidates = Vec::new();
        
        // 샘플 추천 주식들 (실제로는 추천 엔진에서 가져옴)
        let recommended_stocks = vec![
            ("005930", "삼성전자", 75000.0, 0.65),
            ("105560", "KB금융", 55000.0, 0.45),
            ("055550", "신한지주", 42000.0, 0.42),
            ("017670", "SK텔레콤", 48000.0, 0.35),
            ("003550", "LG", 95000.0, 0.38),
        ];

        let available_cash = account.total_cash * 0.8; // 사용 가능 현금의 80%
        let max_position_value = account.total_value * strategy.max_position_ratio;

        for (symbol, name, price, beta) in recommended_stocks {
            // 이미 보유 중인 종목은 제외
            if account.portfolio.contains_key(symbol) {
                continue;
            }

            // 베타가 낮은 저위험 주식만 선택
            if beta > 0.8 {
                continue;
            }

            // 매수 가능 수량 계산
            let max_quantity = (max_position_value / price) as i32;
            let affordable_quantity = (available_cash / price) as i32;
            let quantity = max_quantity.min(affordable_quantity).min(100); // 최대 100주

            if quantity > 0 {
                candidates.push(BuyCandidate {
                    symbol: symbol.to_string(),
                    stock_name: name.to_string(),
                    quantity,
                    price,
                    reason: format!("저위험 주식 추천 (베타: {:.2})", beta),
                    confidence: 0.7,
                });
            }
        }

        // 신뢰도 순으로 정렬하고 상위 3개만 반환
        candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
        candidates.into_iter().take(3).collect()
    }

    /// 리밸런싱 필요 여부 확인
    fn needs_rebalancing(&self, account: &UserAccount, strategy: &TradingStrategy) -> bool {
        let allocation = account.get_portfolio_allocation();
        
        for (_, ratio) in allocation {
            if ratio > strategy.max_position_ratio + strategy.rebalance_threshold {
                return true;
            }
        }

        false
    }

    /// 목표 배분 비율 계산
    fn calculate_target_allocation(&self, _account: &UserAccount, strategy: &TradingStrategy) -> HashMap<String, f64> {
        let mut target_allocation = HashMap::new();
        let equal_weight = 1.0 / strategy.max_stocks as f64;
        
        // 균등 배분 (실제로는 더 정교한 로직 사용)
        let sample_stocks = vec!["005930", "105560", "055550", "017670", "003550"];
        for symbol in sample_stocks {
            target_allocation.insert(symbol.to_string(), equal_weight);
        }

        target_allocation
    }

    /// 매매 신호 실행
    pub fn execute_trading_signal(&mut self, user_id: &str, signal: TradingSignal) -> Result<String, String> {
        if let Some(account) = self.user_accounts.get_mut(user_id) {
            match signal {
                TradingSignal::Buy { symbol, stock_name, quantity, price, .. } => {
                    match account.buy_stock(symbol.clone(), stock_name.clone(), quantity, price) {
                        Ok(_) => {
                            Ok(format!("매수 완료: {} {}주", symbol, quantity))
                        }
                        Err(e) => {
                            error!("매수 실행 실패: {}", e);
                            Err(e)
                        }
                    }
                }
                TradingSignal::Sell { symbol, quantity, price, .. } => {
                    match account.sell_stock(symbol.clone(), quantity, price) {
                        Ok(_) => {
                            Ok(format!("매도 완료: {} {}주", symbol, quantity))
                        }
                        Err(e) => {
                            error!("매도 실행 실패: {}", e);
                            Err(e)
                        }
                    }
                }
                TradingSignal::Rebalance { target_allocation, .. } => {
                    // 리밸런싱 로직 (간단한 구현)
                    Ok("리밸런싱 신호 처리됨".to_string())
                }
                TradingSignal::Hold { .. } => {
                    Ok("보유 신호 처리됨".to_string())
                }
            }
        } else {
            Err(format!("사용자 계정을 찾을 수 없음: {}", user_id))
        }
    }

    /// 사용자 계정 정보 조회
    pub fn get_user_account(&self, user_id: &str) -> Option<&UserAccount> {
        self.user_accounts.get(user_id)
    }

    /// 모든 사용자 계정 조회
    pub fn get_all_accounts(&self) -> &HashMap<String, UserAccount> {
        &self.user_accounts
    }
}

#[derive(Debug, Clone)]
struct BuyCandidate {
    symbol: String,
    stock_name: String,
    quantity: i32,
    price: f64,
    reason: String,
    confidence: f64,
}

/// 자동 매매 메시지
#[derive(Debug, Clone)]
pub enum AutoTradingMessage {
    RegisterUser { user_id: String, username: String, initial_cash: f64 },
    SetStrategy { user_id: String, strategy: TradingStrategy },
    UpdatePrices { prices: HashMap<String, f64> },
    GenerateSignals { user_id: String },
    ExecuteSignal { user_id: String, signal: TradingSignal },
    GetAccount { user_id: String },
    GetAllAccounts,
    RunAutoTrading,
}

#[async_trait]
impl Actor for AutoTradingEngine {
    type Message = AutoTradingMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            AutoTradingMessage::RegisterUser { user_id, username, initial_cash } => {
                self.register_user(user_id, username, initial_cash);
            }
            
            AutoTradingMessage::SetStrategy { user_id, strategy } => {
                self.set_trading_strategy(user_id, strategy);
            }
            
            AutoTradingMessage::UpdatePrices { prices } => {
                self.update_stock_prices(prices);
            }
            
            AutoTradingMessage::GenerateSignals { user_id } => {
                let signals = self.generate_trading_signals(&user_id);
            }
            
            AutoTradingMessage::ExecuteSignal { user_id, signal } => {
                match self.execute_trading_signal(&user_id, signal) {
                    Ok(result) => (),
                    Err(e) => error!("매매 신호 실행 실패: {}", e),
                }
            }
            
            AutoTradingMessage::GetAccount { user_id } => {
                if let Some(account) = self.get_user_account(&user_id) {
                } else {
                    warn!("사용자 {} 계정을 찾을 수 없음", user_id);
                }
            }
            
            AutoTradingMessage::GetAllAccounts => {
            }
            
            AutoTradingMessage::RunAutoTrading => {
                let user_ids: Vec<String> = self.user_accounts.keys().cloned().collect();
                for user_id in user_ids {
                    let signals = self.generate_trading_signals(&user_id);
                    for signal in signals {
                        if let Err(e) = self.execute_trading_signal(&user_id, signal) {
                            error!("자동 매매 실행 실패: {}", e);
                        }
                    }
                }
            }
        }
    }

    async fn pre_start(&mut self) {
    }

    async fn post_stop(&mut self) {
    }
}

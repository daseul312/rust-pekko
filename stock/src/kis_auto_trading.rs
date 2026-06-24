mod korea_investment_api;
mod daily_report;
mod notion_reporter;
mod cloudflare_sync;

use korea_investment_api::*;
use daily_report::*;
use notion_reporter::NotionReporter;
use cloudflare_sync::*;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{ warn, error};
use chrono::Local;

/// 자동매매 전략
#[derive(Debug, Clone)]
pub struct TradingStrategy {
    pub profit_target: f64,      // 목표 수익률 (%)
    pub stop_loss: f64,          // 손절매 (%)
    pub max_position_ratio: f64, // 최대 포지션 비율 (%)
    pub min_cash_ratio: f64,     // 최소 현금 보유 비율 (%)
}

impl Default for TradingStrategy {
    fn default() -> Self {
        Self {
            profit_target: 5.0,      // 5% 수익 시 매도
            stop_loss: -3.0,         // -3% 손실 시 손절
            max_position_ratio: 30.0, // 최대 30% 포지션
            min_cash_ratio: 20.0,    // 최소 20% 현금 보유
        }
    }
}

/// 보유 종목 정보
#[derive(Debug, Clone)]
pub struct Position {
    pub stock_code: String,
    pub stock_name: String,
    pub quantity: u32,
    pub avg_price: f64,
    pub current_price: f64,
    pub profit_loss_rate: f64,
}

/// 자동매매 시스템
pub struct AutoTradingSystem {
    api: KoreaInvestmentAPI,
    strategy: TradingStrategy,
    positions: HashMap<String, Position>,
    watchlist: Vec<String>,  // 관심 종목
    total_cash: f64,
    dry_run: bool,  // true면 실제 주문 안 함 (시뮬레이션만)
    daily_report: DailyTradingReport,  // 일일 거래 리포트
    notion_reporter: Option<NotionReporter>,  // 노션 리포터
    last_report_date: Option<String>,  // 마지막 리포트 생성 날짜
    starting_balance: f64,  // 장 시작 시 자산
    // Cloudflare KV 동기화
    kv_sync: CloudflareKVSync,
    kv_state: AutoTradingState,
}

impl AutoTradingSystem {
    pub async fn new(strategy: TradingStrategy, dry_run: bool) -> Result<Self> {
        let mut api = KoreaInvestmentAPI::from_env()?;
        
        // 토큰 발급
        api.get_access_token().await?;
        
        // 노션 리포터 초기화 (실패해도 계속 진행)
        let notion_reporter = NotionReporter::from_env().ok();
        if notion_reporter.is_some() {
        } else {
            warn!("⚠️  노션 리포터 초기화 실패 - 리포트 기능 비활성화");
        }
        
        // 오늘 날짜로 리포트 초기화
        let today = Local::now().format("%Y-%m-%d").to_string();
        let daily_report = DailyTradingReport::new(today.clone());
        
        Ok(Self {
            api,
            strategy,
            positions: HashMap::new(),
            watchlist: vec![
                // 🏆 대형주 (시총 상위)
                "005930".to_string(),  // 삼성전자 (반도체)
                "000660".to_string(),  // SK하이닉스 (반도체)
                "373220".to_string(),  // LG에너지솔루션 (배터리)
                "207940".to_string(),  // 삼성바이오로직스 (바이오)
                
                // 💻 IT/인터넷
                "035720".to_string(),  // 카카오
                "035420".to_string(),  // NAVER
                "051910".to_string(),  // LG화학
                "006400".to_string(),  // 삼성SDI (배터리)
                
                // 🏦 금융
                "055550".to_string(),  // 신한지주
                "105560".to_string(),  // KB금융
                "086790".to_string(),  // 하나금융지주
                "316140".to_string(),  // 우리금융지주
                
                // 🚗 자동차
                "005380".to_string(),  // 현대차
                "000270".to_string(),  // 기아
                
                // 🏭 산업재
                "012330".to_string(),  // 현대모비스
                "009150".to_string(),  // 삼성전기
                
                // 🛒 소비재
                "097950".to_string(),  // CJ제일제당
                "271560".to_string(),  // 오리온
                
                // ⚡ 에너지/유틸리티
                "015760".to_string(),  // 한국전력
                "034730".to_string(),  // SK
            ],
            total_cash: 0.0,
            dry_run,
            daily_report,
            notion_reporter,
            last_report_date: Some(today),
            starting_balance: 0.0,
            kv_sync: CloudflareKVSync::new(),
            kv_state: AutoTradingState::default(),
        })
    }

    /// 현재 주가 조회
    pub async fn get_current_price(&self, stock_code: &str) -> Result<f64> {
        let stock_price = self.api.get_stock_price(stock_code).await?;
        let price = stock_price.current_price.parse::<f64>()?;
        Ok(price)
    }
    
    /// 주가 정보 상세 조회 (기술적 분석용)
    pub async fn get_stock_info(&self, stock_code: &str) -> Result<korea_investment_api::StockPrice> {
        self.api.get_stock_price(stock_code).await
    }

    /// Cloudflare KV에 상태 동기화
    pub fn sync_to_kv(&mut self) {
        // 포지션 정보 업데이트
        let positions: Vec<PositionState> = self.positions.values().map(|p| {
            PositionState {
                stock_code: p.stock_code.clone(),
                stock_name: p.stock_name.clone(),
                quantity: p.quantity,
                avg_price: p.avg_price,
                current_price: p.current_price,
                profit_loss_rate: p.profit_loss_rate,
                profit_loss_amount: ((p.current_price - p.avg_price) * p.quantity as f64) as i64,
            }
        }).collect();
        
        self.kv_state.update_positions(positions);
        
        // 전략 설정 업데이트
        self.kv_state.strategy = TradingStrategyState {
            profit_target: self.strategy.profit_target,
            stop_loss: self.strategy.stop_loss,
            max_position_ratio: self.strategy.max_position_ratio,
            min_cash_ratio: self.strategy.min_cash_ratio,
        };
        
        // KV에 저장
        if let Err(e) = self.kv_sync.sync_state(&self.kv_state) {
            warn!("⚠️ KV 동기화 실패: {}", e);
        }
    }

    /// 자동매매 시작 (KV 상태 업데이트)
    pub fn start_trading(&mut self) {
        self.kv_state.set_active(true);
        self.sync_to_kv();
    }

    /// 자동매매 중지 (KV 상태 업데이트)
    pub fn stop_trading(&mut self) {
        self.kv_state.set_active(false);
        self.sync_to_kv();
    }

    /// 이벤트 로그 추가
    pub fn log_event(&mut self, level: &str, message: &str) {
        self.kv_state.add_event(level, message);
    }

    /// 거래 기록 추가
    pub fn log_trade(&mut self, stock_code: &str, stock_name: &str, trade_type: &str, quantity: u32, price: f64, reason: &str) {
        let trade = TradeRecord {
            timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            stock_code: stock_code.to_string(),
            stock_name: stock_name.to_string(),
            trade_type: trade_type.to_string(),
            quantity,
            price,
            reason: reason.to_string(),
        };
        self.kv_state.add_trade(trade);
    }

    /// 매수 신호 확인 (가격순 정렬)
    pub async fn check_buy_signals(&self) -> Vec<(String, f64)> {
        let mut buy_candidates = Vec::new();

        for stock_code in &self.watchlist {
            // 포지션 비율 체크 (이미 보유한 종목도 추가 매수 가능)
            let total_asset = self.total_cash + self.get_total_position_value();
            
            // 해당 종목의 현재 포지션 비율 확인
            let current_position_value = if let Some(pos) = self.positions.get(stock_code) {
                pos.current_price * pos.quantity as f64
            } else {
                0.0
            };
            
            let position_ratio = (current_position_value / total_asset) * 100.0;
            
            // 이미 최대 포지션에 도달했으면 스킵
            if position_ratio >= self.strategy.max_position_ratio {
                continue;
            }

            // 현재가 및 기술적 정보 조회
            match self.get_stock_info(stock_code).await {
                Ok(stock_info) => {
                    let price = stock_info.current_price.parse::<f64>().unwrap_or(0.0);
                    let change_rate = stock_info.change_rate.parse::<f64>().unwrap_or(0.0);
                    let volatility = stock_info.volatility();
                    
                    // 매수 조건 체크
                    let cash_ratio = (self.total_cash / total_asset) * 100.0;
                    
                    // 기본 조건: 현금이 충분해야 함
                    if cash_ratio <= self.strategy.min_cash_ratio + 10.0 {
                        continue;
                    }
                    
                    // 🎯 기술적 분석 기반 매수 조건
                    let is_buyable = self.analyze_buy_condition(&stock_info, stock_code);
                    
                    if is_buyable {
                        let status = if self.positions.contains_key(stock_code) {
                            format!("(추가 매수, 현재 {:.1}%)", position_ratio)
                        } else {
                            "(신규 매수)".to_string()
                        };
                        buy_candidates.push((stock_code.clone(), price));
                    } else {
                    }
                }
                Err(e) => {
                    warn!("시세 조회 실패 {}: {}", stock_code, e);
                }
            }
        }

        // 🎯 가격순 정렬 (저렴한 종목부터)
        buy_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        if !buy_candidates.is_empty() {
            for (idx, (code, price)) in buy_candidates.iter().enumerate() {
            }
        }

        buy_candidates
    }
    
    /// 매수 조건 분석 (기술적 분석)
    fn analyze_buy_condition(&self, stock_info: &korea_investment_api::StockPrice, stock_code: &str) -> bool {
        let change_rate = stock_info.change_rate.parse::<f64>().unwrap_or(0.0);
        let volatility = stock_info.volatility();
        let is_rising = stock_info.is_rising();
        let above_open = stock_info.above_open();
        
        // 🎯 매수 조건 (여러 조건 중 하나라도 만족)
        
        // 조건 1: 전일 대비 상승 중 (0.5% 이상)
        let condition1 = change_rate >= 0.5;
        
        // 조건 2: 전일 소폭 하락이지만 장중 반등 (-1% ~ 0% && 시가보다 높음)
        let condition2 = change_rate >= -1.0 && change_rate < 0.0 && above_open;
        
        // 조건 3: 이미 보유 중이고 평단가 낮추기 (추가 매수)
        let condition3 = self.positions.contains_key(stock_code) && change_rate < 0.0 && change_rate > -2.0;
        
        // 조건 4: 변동성이 낮고 안정적 (변동성 < 3%)
        let condition4 = volatility < 3.0 && change_rate >= -0.5;
        
        if condition1 {
        } else if condition2 {
        } else if condition3 {
        } else if condition4 {
        } else {
        }
        
        condition1 || condition2 || condition3 || condition4
    }

    /// 매도 신호 확인
    /// 
    /// 📅 매도 가능 시기:
    /// - 매수 당일: 가능 (장중 09:00~15:30)
    /// - T+0 거래: 매수 즉시 매도 가능 (단, 결제일은 T+2)
    /// - 제한 없음: 목표 수익률이나 손절매 조건 충족 시 즉시 매도
    pub async fn check_sell_signals(&mut self) -> Vec<(String, String, u32, f64)> {
        let mut sell_signals = Vec::new();

        // 먼저 모든 stock_code를 수집
        let stock_codes: Vec<String> = self.positions.keys().cloned().collect();

        if !stock_codes.is_empty() {
        }

        // 각 종목의 현재가 조회 및 업데이트
        for stock_code in stock_codes {
            match self.get_current_price(&stock_code).await {
                Ok(current_price) => {
                    // 이제 positions를 mutable하게 접근
                    if let Some(position) = self.positions.get_mut(&stock_code) {
                        let prev_price = position.current_price;
                        position.current_price = current_price;
                        
                        // 수익률 계산
                        let profit_loss_rate = ((current_price - position.avg_price) / position.avg_price) * 100.0;
                        position.profit_loss_rate = profit_loss_rate;

                        let price_change = ((current_price - prev_price) / prev_price) * 100.0;
                        // 매도 조건 확인
                        let (should_sell, reason) = if profit_loss_rate >= self.strategy.profit_target {
                            (true, format!("🎯 목표 수익률 달성 (+{:.1}%)", profit_loss_rate))
                        } else if profit_loss_rate <= self.strategy.stop_loss {
                            (true, format!("⚠️  손절매 실행 ({:.1}%)", profit_loss_rate))
                        } else {
                            (false, String::new())
                        };

                        if should_sell {
                            sell_signals.push((
                                stock_code.clone(),
                                position.stock_name.clone(),
                                position.quantity,
                                current_price,
                            ));
                        }
                    }
                }
                Err(e) => {
                    error!("시세 조회 실패 {}: {}", stock_code, e);
                }
            }
        }

        sell_signals
    }

    /// 포지션 총 가치
    fn get_total_position_value(&self) -> f64 {
        self.positions.values()
            .map(|p| p.current_price * p.quantity as f64)
            .sum()
    }
    
    /// 일일 리포트 업데이트
    fn update_daily_report(&mut self) {
        // 현재 포지션 정보 업데이트
        self.daily_report.positions.clear();
        for (_, position) in &self.positions {
            self.daily_report.positions.push(PositionSummary {
                symbol: position.stock_code.clone(),
                name: position.stock_name.clone(),
                quantity: position.quantity,
                avg_price: position.avg_price,
                current_price: position.current_price,
                profit_loss_rate: position.profit_loss_rate,
                total_value: position.current_price * position.quantity as f64,
            });
        }
        
        // 수익률 계산
        let ending_balance = self.total_cash + self.get_total_position_value();
        let daily_return = ending_balance - self.starting_balance;
        let daily_return_rate = if self.starting_balance > 0.0 {
            (daily_return / self.starting_balance) * 100.0
        } else {
            0.0
        };
        
        self.daily_report.performance.ending_balance = ending_balance;
        self.daily_report.performance.daily_return = daily_return;
        self.daily_report.performance.daily_return_rate = daily_return_rate;
        
        // 총 손익률 계산
        if self.daily_report.summary.total_trades > 0 {
            self.daily_report.summary.total_profit_loss_rate = 
                (self.daily_report.summary.total_profit_loss / self.starting_balance) * 100.0;
        }
    }

    /// 매수 실행
    pub async fn execute_buy(&mut self, stock_code: &str, price: f64) -> Result<()> {
        // 🔍 매수 전 실시간 계좌 잔고 확인 (실전 모드에서만)
        if !self.dry_run {
            match self.api.get_account_info().await {
                Ok((cash, _holdings)) => {
                    self.total_cash = cash;
                }
                Err(e) => {
                    warn!("  ⚠️  계좌 조회 실패, 기존 잔고 사용: {}", e);
                }
            }
        }

        // 🎯 증권사 API로 실제 매수 가능 금액 조회 (실전 모드)
        let buyable_from_api = if !self.dry_run {
            match self.api.get_buyable_cash(stock_code).await {
                Ok(buyable) => {
                    Some(buyable)
                }
                Err(e) => {
                    warn!("  ⚠️  매수가능금액 조회 실패: {} - 보수적으로 계산", e);
                    None
                }
            }
        } else {
            None
        };
        
        // 매수 가능 금액 계산
        let total_asset = self.total_cash + self.get_total_position_value();
        let max_buy_amount = total_asset * (self.strategy.max_position_ratio / 100.0);
        
        // 실제 보유 현금 확인
        let available_cash = self.total_cash;
        
        // 매수 가능 금액은 여러 제한 중 가장 작은 값
        let mut buy_limit = max_buy_amount.min(available_cash);
        
        // 증권사 API 응답이 있으면 그것을 우선 사용
        if let Some(buyable) = buyable_from_api {
            buy_limit = buy_limit.min(buyable);
        }
        // 매수 수량 계산 (극도로 보수적으로)
        // 증권사의 주문 가능 금액 계산이 복잡하므로 안전 마진을 크게 설정
        let safety_ratio = if buyable_from_api.is_some() {
            0.95  // API로 정확한 금액 조회한 경우 95% 사용
        } else {
            0.50  // 추정치인 경우 50%만 사용 (매우 보수적)
        };
        
        let safe_buy_amount = buy_limit * safety_ratio;
        let fee_rate = 0.0005;  // 수수료 + 기타 비용 여유 (0.05%)
        let total_cost_per_share = price * (1.0 + fee_rate);
        let quantity = (safe_buy_amount / total_cost_per_share) as u32;
        if quantity == 0 {
            warn!("  ❌ 매수 수량이 0입니다. 현금이 부족합니다.");
            warn!("     필요 금액: 약 {:.0}원, 보유 현금: {:.0}원", price, available_cash);
            return Err(anyhow!("매수 가능 금액 부족 (수량 0)"));
        }

        let total_cost = price * quantity as f64;
        let fee = total_cost * 0.00015;  // 실제 수수료 0.015%
        let total_with_fee = total_cost + fee;

        // 최종 검증: 보유 현금으로 매수 가능한지 확인 (더 엄격하게)
        // 증권사는 추가 비용을 고려하므로 20% 여유를 더 둠
        let required_cash = total_with_fee * 1.20;  // 20% 안전 마진 추가
        
        if required_cash > available_cash {
            warn!("  ❌ 보유 현금 부족! (안전 마진 포함)");
            warn!("     필요 금액: {:.0}원 (안전 마진 포함)", required_cash);
            warn!("     보유 현금: {:.0}원", available_cash);
            warn!("     부족 금액: {:.0}원", required_cash - available_cash);
            return Ok(());
        }
        // 실제 주문 (주의: 실전 거래!)
        if self.dry_run {
            // 포지션 추가 (시뮬레이션)
            self.positions.insert(stock_code.to_string(), Position {
                stock_code: stock_code.to_string(),
                stock_name: stock_code.to_string(),
                quantity,
                avg_price: price,
                current_price: price,
                profit_loss_rate: 0.0,
            });

            self.total_cash -= total_with_fee;  // 수수료 포함
            
            // 거래 기록 추가
            self.daily_report.trades.push(TradeRecord {
                time: Local::now().format("%H:%M:%S").to_string(),
                action: "매수".to_string(),
                symbol: stock_code.to_string(),
                name: stock_code.to_string(),
                quantity,
                price,
                total_amount: total_with_fee,
                profit_loss: None,
            });
            self.daily_report.summary.total_trades += 1;
            self.daily_report.summary.buy_count += 1;
            
            return Ok(());
        }
        
        match self.api.buy_stock(stock_code, quantity, price as u32).await {
            Ok(order_response) => {
                // 포지션 추가 또는 업데이트
                if let Some(existing_pos) = self.positions.get_mut(&stock_code.to_string()) {
                    // 기존 포지션이 있으면 평단가 재계산
                    let total_qty = existing_pos.quantity + quantity;
                    let total_cost = (existing_pos.avg_price * existing_pos.quantity as f64) + total_with_fee;
                    let new_avg_price = total_cost / total_qty as f64;
                    existing_pos.quantity = total_qty;
                    existing_pos.avg_price = new_avg_price;
                    existing_pos.current_price = price;
                } else {
                    // 신규 포지션 생성
                    self.positions.insert(stock_code.to_string(), Position {
                        stock_code: stock_code.to_string(),
                        stock_name: stock_code.to_string(),
                        quantity,
                        avg_price: price,
                        current_price: price,
                        profit_loss_rate: 0.0,
                    });
                }

                self.total_cash -= total_with_fee;  // 수수료 포함
                
                // 거래 기록 추가
                self.daily_report.trades.push(TradeRecord {
                    time: Local::now().format("%H:%M:%S").to_string(),
                    action: "매수".to_string(),
                    symbol: stock_code.to_string(),
                    name: stock_code.to_string(),
                    quantity,
                    price,
                    total_amount: total_with_fee,
                    profit_loss: None,
                });
                self.daily_report.summary.total_trades += 1;
                self.daily_report.summary.buy_count += 1;
            }
            Err(e) => {
                error!("❌ 매수 주문 실패: {}", e);
            }
        }

        Ok(())
    }

    /// 매도 실행
    pub async fn execute_sell(&mut self, stock_code: &str, quantity: u32, price: f64) -> Result<()> {
        let sell_amount = price * quantity as f64;
        let fee = sell_amount * 0.00015;  // 0.015% 수수료
        let tax = sell_amount * 0.0025;   // 0.25% 증권거래세
        let total_fee = fee + tax;
        let net_amount = sell_amount - total_fee;
        // 실제 주문 (주의: 실전 거래!)
        if self.dry_run {
            // 포지션 제거 (시뮬레이션)
            if let Some(position) = self.positions.remove(stock_code) {
                let gross_profit = (price - position.avg_price) * quantity as f64;
                let net_profit = gross_profit - total_fee;
                
                self.total_cash += net_amount;  // 수수료/세금 제외한 실수령액
                // 거래 기록 추가
                self.daily_report.trades.push(TradeRecord {
                    time: Local::now().format("%H:%M:%S").to_string(),
                    action: "매도".to_string(),
                    symbol: stock_code.to_string(),
                    name: stock_code.to_string(),
                    quantity,
                    price,
                    total_amount: net_amount,
                    profit_loss: Some(net_profit),
                });
                self.daily_report.summary.total_trades += 1;
                self.daily_report.summary.sell_count += 1;
                self.daily_report.summary.total_profit_loss += net_profit;
                
                // 승률 계산
                if net_profit > 0.0 {
                    let win_count = self.daily_report.trades.iter()
                        .filter(|t| t.action == "매도" && t.profit_loss.unwrap_or(0.0) > 0.0)
                        .count();
                    self.daily_report.summary.win_rate = (win_count as f64 / self.daily_report.summary.sell_count as f64) * 100.0;
                }
            }
            return Ok(());
        }
        
        match self.api.sell_stock(stock_code, quantity, price as u32).await {
            Ok(order_response) => {
                // 포지션 제거
                if let Some(position) = self.positions.remove(stock_code) {
                    let gross_profit = (price - position.avg_price) * quantity as f64;
                    let net_profit = gross_profit - total_fee;
                    
                    self.total_cash += net_amount;  // 수수료/세금 제외한 실수령액
                    // 거래 기록 추가
                    self.daily_report.trades.push(TradeRecord {
                        time: Local::now().format("%H:%M:%S").to_string(),
                        action: "매도".to_string(),
                        symbol: stock_code.to_string(),
                        name: stock_code.to_string(),
                        quantity,
                        price,
                        total_amount: net_amount,
                        profit_loss: Some(net_profit),
                    });
                    self.daily_report.summary.total_trades += 1;
                    self.daily_report.summary.sell_count += 1;
                    self.daily_report.summary.total_profit_loss += net_profit;
                    
                    // 승률 계산
                    if net_profit > 0.0 {
                        let win_count = self.daily_report.trades.iter()
                            .filter(|t| t.action == "매도" && t.profit_loss.unwrap_or(0.0) > 0.0)
                            .count();
                        self.daily_report.summary.win_rate = (win_count as f64 / self.daily_report.summary.sell_count as f64) * 100.0;
                    }
                }
            }
            Err(e) => {
                error!("❌ 매도 주문 실패: {}", e);
            }
        }

        Ok(())
    }

    /// 자동매매 실행 (메인 루프)
    pub async fn run(&mut self) -> Result<()> {
        if self.dry_run {
        } else {
            warn!("⚠️  모드: 실전 거래 (실제 주문 실행!)");
        }
        // 초기 계좌 정보 조회
        match self.api.get_account_info().await {
            Ok((cash, holdings)) => {
                self.total_cash = cash;
                // 보유 종목을 포지션에 추가
                for (stock_code, quantity, avg_price) in holdings {
                    self.positions.insert(stock_code.clone(), Position {
                        stock_code: stock_code.clone(),
                        stock_name: stock_code.clone(),
                        quantity,
                        avg_price,
                        current_price: avg_price,
                        profit_loss_rate: 0.0,
                    });
                }
            }
            Err(e) => {
                warn!("계좌 정보 조회 실패, 기본값 사용: {}", e);
                self.total_cash = 400001.0;
            }
        }
        // 시작 자산 기록
        self.starting_balance = self.total_cash + self.get_total_position_value();
        self.daily_report.performance.starting_balance = self.starting_balance;
        // Cloudflare KV에 시작 상태 동기화
        self.start_trading();
        let mut loop_count = 0;
        let mut last_market_check_time = Local::now().time();
        // 무한 루프 (Ctrl+C로 중지)

        loop {
            loop_count += 1;
            let now = Local::now();
            let current_date = now.format("%Y-%m-%d").to_string();
            let current_time = now.time();
            // 0. 주기적으로 계좌 잔고 업데이트 (실전 모드에서만)
            if !self.dry_run && loop_count % 3 == 0 {  // 3번째 루프마다 확인
                match self.api.get_account_info().await {
                    Ok((cash, holdings)) => {
                        self.total_cash = cash;
                    }
                    Err(e) => {
                        warn!("  ⚠️  계좌 정보 업데이트 실패: {}", e);
                    }
                }
            }

            // 1. 매도 신호 확인 및 실행
            let sell_signals = self.check_sell_signals().await;
            for (stock_code, _stock_name, quantity, price) in sell_signals {
                // 오류 발생해도 계속 진행
                if let Err(e) = self.execute_sell(&stock_code, quantity, price).await {
                    error!("❌ 매도 실행 중 오류 (계속 진행): {}", e);
                }
            }

            // 2. 매수 신호 확인 및 실행 (가격순으로 시도)
            let buy_signals = self.check_buy_signals().await;
            
            if !buy_signals.is_empty() {
            }
            
            for (idx, (stock_code, price)) in buy_signals.iter().enumerate() {
                // 오류 발생해도 계속 진행 (다음 순위 시도)
                match self.execute_buy(stock_code, *price).await {
                    Ok(_) => {
                        break;  // 한 종목 매수 성공하면 이번 루프 종료 (다음 루프에서 계속)
                    }
                    Err(e) => {
                        warn!("⚠️  {}순위 매수 실패: {} - 다음 순위 시도...", idx + 1, e);
                        // 계속해서 다음 순위 시도
                    }
                }
            }

            // 3. 현재 상태 출력
            for (stock_code, position) in &self.positions {
            }

            // 3.5. Cloudflare KV에 상태 동기화 (5번째 루프마다)
            if loop_count % 5 == 0 {
                self.sync_to_kv();
            }
            // 4. 장 마감 후 일일 리포트 생성 (15:30 이후 한 번만)
            if KoreanMarketHours::is_market_closed() && current_time >= KoreanMarketHours::report_time() {
                // 마지막 체크 시간 이후 처음 리포트 시간을 지나면 리포트 생성
                if last_market_check_time < KoreanMarketHours::report_time() {
                    // 리포트 업데이트
                    self.update_daily_report();
                    
                    // 노션에 업로드
                    if let Some(reporter) = &self.notion_reporter {
                        match reporter.create_daily_report(&self.daily_report).await {
                            Ok(url) => {
                            }
                            Err(e) => {
                                error!("❌ 노션 리포트 생성 실패: {}", e);
                            }
                        }
                    } else {
                        warn!("⚠️  노션 리포터가 비활성화되어 있습니다");
                    }
                    last_market_check_time = current_time;
                }
            }
            
            // 5. 날짜가 바뀌면 새로운 리포트 시작
            if Some(&current_date) != self.last_report_date.as_ref() {
                // 전날 리포트를 노션에 저장 (장 마감 후에 이미 저장했을 수도 있음)
                if !self.daily_report.trades.is_empty() && self.notion_reporter.is_some() {
                    self.update_daily_report();
                    
                    if let Some(reporter) = &self.notion_reporter {
                        match reporter.create_daily_report(&self.daily_report).await {
                            Ok(url) => {
                            }
                            Err(e) => {
                                error!("❌ 전날 리포트 저장 실패: {}", e);
                            }
                        }
                    }
                }
                
                // 새로운 리포트 시작
                self.daily_report = DailyTradingReport::new(current_date.clone());
                self.last_report_date = Some(current_date);
                self.starting_balance = self.total_cash + self.get_total_position_value();
                self.daily_report.performance.starting_balance = self.starting_balance;
            }
            
            let wait_seconds = if self.dry_run { 10 } else { 30 };  // DRY-RUN: 10초, 실전: 30초
            // 대기
            sleep(Duration::from_secs(wait_seconds)).await;
        }
        
        // 최종 결과 출력
        let total_position_value = self.get_total_position_value();
        let total_asset = self.total_cash + total_position_value;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // .env 파일 로드
    dotenv::dotenv().ok();
    
    // 로깅 초기화
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n🚀 한국투자증권 자동매매 시스템\n");

    // 명령행 인자로 모드 선택
    let args: Vec<String> = std::env::args().collect();
    let dry_run = if args.len() > 1 && args[1] == "--real" {
        println!("⚠️  ═══════════════════════════════════════");
        println!("⚠️  실전 거래 모드로 실행합니다!");
        println!("⚠️  실제 주문이 체결됩니다!");
        println!("⚠️  실제 돈이 사용됩니다!");
        println!("⚠️  ═══════════════════════════════════════");
        println!();
        println!("💰 현재 계좌: 약 82만원 예수금");
        println!("📊 보유 주식: 삼성전자 7주, 카카오 1주");
        println!();
        println!("⏱️  30초마다 자동 매매 체크");
        println!("🛑 중지: Ctrl+C\n");
        false
    } else {
        println!("🔍 DRY-RUN 모드로 실행합니다 (시뮬레이션만)");
        println!("💡 실전 거래를 원하시면: cargo run --bin kis-auto-trading -- --real");
        println!("⏱️  10초마다 체크\n");
        true
    };

    // 매매 전략 설정 (소액 투자자용으로 조정)
    // 총 자금이 100만원 이하인 경우 더 높은 포지션 비율 사용
    let strategy = TradingStrategy {
        profit_target: 3.0,       // 3% 수익 시 매도
        stop_loss: -2.0,          // -2% 손실 시 손절
        max_position_ratio: 50.0, // 최대 50% 포지션 (소액 투자자용)
        min_cash_ratio: 20.0,     // 최소 20% 현금 보유
    };
    
    println!("📋 매매 전략 (소액 투자자용):");
    println!("  - 40만원 기준: 종목당 최대 약 20만원 매수 가능");
    println!("  - 100만원 이상 보유 시 비율 조정을 권장합니다\n");

    // 자동매매 시스템 생성 및 실행
    let mut trading_system = AutoTradingSystem::new(strategy, dry_run).await?;
    
    trading_system.run().await?;

    Ok(())
}


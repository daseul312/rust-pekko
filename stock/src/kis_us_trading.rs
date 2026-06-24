mod korea_investment_api;
mod us_stocks;
mod daily_report;
mod notion_reporter;

use korea_investment_api::KoreaInvestmentAPI;
use us_stocks::*;
use daily_report::*;
use notion_reporter::NotionReporter;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{ warn, error};
use chrono::Local;

/// 미국 주식 자동매매 전략
#[derive(Debug, Clone)]
pub struct UsTradingStrategy {
    pub profit_target: f64,      // 목표 수익률 (%)
    pub stop_loss: f64,          // 손절매 (%)
    pub max_position_ratio: f64, // 최대 포지션 비율 (%)
    pub min_cash_ratio: f64,     // 최소 현금 보유 비율 (%)
    pub usd_krw_rate: f64,       // USD/KRW 환율
}

impl Default for UsTradingStrategy {
    fn default() -> Self {
        Self {
            profit_target: 5.0,      // 미국 주식은 5% 목표 (변동성 고려)
            stop_loss: -3.0,         // -3% 손절
            max_position_ratio: 30.0, // 최대 30% 포지션
            min_cash_ratio: 20.0,    // 최소 20% 현금
            usd_krw_rate: 1350.0,    // 기본 환율 (실시간 조회 권장)
        }
    }
}

/// 미국 주식 포지션
#[derive(Debug, Clone)]
pub struct UsPosition {
    pub symbol: String,
    pub name: String,
    pub quantity: u32,
    pub avg_price_usd: f64,
    pub current_price_usd: f64,
    pub profit_loss_rate: f64,
}

/// 미국 주식 자동매매 시스템
pub struct UsAutoTradingSystem {
    api: KoreaInvestmentAPI,
    strategy: UsTradingStrategy,
    positions: HashMap<String, UsPosition>,
    watchlist: Vec<(String, String)>,  // (Symbol, Name)
    total_cash_krw: f64,  // 원화 기준
    dry_run: bool,
    daily_report: DailyTradingReport,  // 일일 거래 리포트
    notion_reporter: Option<NotionReporter>,  // 노션 리포터
    last_report_date: Option<String>,  // 마지막 리포트 생성 날짜
    starting_balance_krw: f64,  // 장 시작 시 자산 (KRW)
}

impl UsAutoTradingSystem {
    pub async fn new(strategy: UsTradingStrategy, dry_run: bool) -> Result<Self> {
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
        
        // 미국 주요 종목 (선별)
        let watchlist_symbols: Vec<String> = get_us_watchlist().into_iter()
            .take(10)  // 상위 10개만 (API 호출 제한 고려)
            .collect();
        
        // (symbol, name) 튜플로 변환
        let watchlist: Vec<(String, String)> = watchlist_symbols.iter()
            .map(|symbol| (symbol.clone(), symbol.clone())) // 실제로는 symbol로부터 name을 가져와야 함
            .collect();
        
        Ok(Self {
            api,
            strategy,
            positions: HashMap::new(),
            watchlist,
            total_cash_krw: 0.0,
            dry_run,
            daily_report,
            notion_reporter,
            last_report_date: Some(today),
            starting_balance_krw: 0.0,
        })
    }

    /// 미국 주식 현재가 조회 (USD)
    pub async fn get_us_price(&self, symbol: &str) -> Result<f64> {
        self.api.get_us_stock_price(symbol).await
    }

    /// 달러로 환산 (원화 → 달러)
    fn krw_to_usd(&self, krw: f64) -> f64 {
        krw / self.strategy.usd_krw_rate
    }

    /// 원화로 환산 (달러 → 원화)
    fn usd_to_krw(&self, usd: f64) -> f64 {
        usd * self.strategy.usd_krw_rate
    }

    /// 매수 신호 확인
    pub async fn check_buy_signals(&self) -> Vec<(String, String, f64)> {
        let mut buy_candidates = Vec::new();
        for (symbol, name) in &self.watchlist {
            // 이미 최대 포지션 도달한 종목 스킵
            let total_asset = self.total_cash_krw + self.get_total_position_value_krw();
            
            let current_position_value = if let Some(pos) = self.positions.get(symbol) {
                self.usd_to_krw(pos.current_price_usd * pos.quantity as f64)
            } else {
                0.0
            };
            
            let position_ratio = (current_position_value / total_asset) * 100.0;
            
            if position_ratio >= self.strategy.max_position_ratio {
                continue;
            }

            // 시세 조회
            match self.get_us_price(symbol).await {
                Ok(price_usd) => {
                    let price_krw = self.usd_to_krw(price_usd);
                    let cash_ratio = (self.total_cash_krw / total_asset) * 100.0;
                    
                    if cash_ratio > self.strategy.min_cash_ratio + 10.0 {
                        let status = if self.positions.contains_key(symbol) {
                            format!("(추가, {:.1}%)", position_ratio)
                        } else {
                            "(신규)".to_string()
                        };
                        buy_candidates.push((symbol.clone(), name.clone(), price_usd));
                    }
                }
                Err(e) => {
                    warn!("시세 조회 실패 {} ({}): {}", symbol, name, e);
                }
            }
            
            // API 호출 제한 대응
            sleep(Duration::from_millis(200)).await;
        }

        // 가격순 정렬 (USD 기준)
        buy_candidates.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
        
        if !buy_candidates.is_empty() {
            for (idx, (symbol, name, price)) in buy_candidates.iter().enumerate() {
            }
        }

        buy_candidates
    }

    /// 매도 신호 확인
    pub async fn check_sell_signals(&mut self) -> Vec<(String, String, u32, f64)> {
        let mut sell_signals = Vec::new();

        let stock_symbols: Vec<String> = self.positions.keys().cloned().collect();

        if !stock_symbols.is_empty() {
        }

        for symbol in stock_symbols {
            match self.get_us_price(&symbol).await {
                Ok(current_price_usd) => {
                    if let Some(position) = self.positions.get_mut(&symbol) {
                        position.current_price_usd = current_price_usd;
                        
                        let profit_loss_rate = ((current_price_usd - position.avg_price_usd) / position.avg_price_usd) * 100.0;
                        position.profit_loss_rate = profit_loss_rate;
                        let (should_sell, reason) = if profit_loss_rate >= self.strategy.profit_target {
                            (true, format!("🎯 목표 수익률 달성 (+{:.1}%)", profit_loss_rate))
                        } else if profit_loss_rate <= self.strategy.stop_loss {
                            (true, format!("⚠️  손절매 ({:.1}%)", profit_loss_rate))
                        } else {
                            (false, String::new())
                        };

                        if should_sell {
                            sell_signals.push((
                                symbol.clone(),
                                position.name.clone(),
                                position.quantity,
                                current_price_usd,
                            ));
                        }
                    }
                }
                Err(e) => {
                    error!("시세 조회 실패 {}: {}", symbol, e);
                }
            }
            
            sleep(Duration::from_millis(200)).await;
        }

        sell_signals
    }

    /// 포지션 총 가치 (원화)
    fn get_total_position_value_krw(&self) -> f64 {
        self.positions.values()
            .map(|p| self.usd_to_krw(p.current_price_usd * p.quantity as f64))
            .sum()
    }
    
    /// 일일 리포트 업데이트
    fn update_daily_report(&mut self) {
        // 현재 포지션 정보 업데이트
        self.daily_report.positions.clear();
        for (_, position) in &self.positions {
            let total_value_krw = self.usd_to_krw(position.current_price_usd * position.quantity as f64);
            self.daily_report.positions.push(PositionSummary {
                symbol: position.symbol.clone(),
                name: position.name.clone(),
                quantity: position.quantity,
                avg_price: self.usd_to_krw(position.avg_price_usd),  // 원화로 변환
                current_price: self.usd_to_krw(position.current_price_usd),  // 원화로 변환
                profit_loss_rate: position.profit_loss_rate,
                total_value: total_value_krw,
            });
        }
        
        // 수익률 계산
        let ending_balance_krw = self.total_cash_krw + self.get_total_position_value_krw();
        let daily_return = ending_balance_krw - self.starting_balance_krw;
        let daily_return_rate = if self.starting_balance_krw > 0.0 {
            (daily_return / self.starting_balance_krw) * 100.0
        } else {
            0.0
        };
        
        self.daily_report.performance.ending_balance = ending_balance_krw;
        self.daily_report.performance.daily_return = daily_return;
        self.daily_report.performance.daily_return_rate = daily_return_rate;
        
        // 총 손익률 계산
        if self.daily_report.summary.total_trades > 0 {
            self.daily_report.summary.total_profit_loss_rate = 
                (self.daily_report.summary.total_profit_loss / self.starting_balance_krw) * 100.0;
        }
    }

    /// 매수 실행
    pub async fn execute_buy(&mut self, symbol: &str, name: &str, price_usd: f64) -> Result<()> {
        // 실전 모드에서 계좌 조회
        if !self.dry_run {
            match self.api.get_account_info().await {
                Ok((cash, _)) => {
                    self.total_cash_krw = cash;
                }
                Err(e) => {
                    warn!("  ⚠️  계좌 조회 실패: {}", e);
                }
            }
        }

        let total_asset = self.total_cash_krw + self.get_total_position_value_krw();
        let max_buy_krw = total_asset * (self.strategy.max_position_ratio / 100.0);
        let available_cash_krw = self.total_cash_krw;
        
        let buy_limit_krw = max_buy_krw.min(available_cash_krw);
        let buy_limit_usd = self.krw_to_usd(buy_limit_krw * 0.90);  // 90% 안전 마진
        
        // 최소 수수료 $5 고려
        let min_order_usd = 50.0;  // 최소 $50 주문 권장
        
        if buy_limit_usd < min_order_usd {
            warn!("  ❌ 매수 금액 부족: ${:.0} (최소 ${} 권장)", buy_limit_usd, min_order_usd);
            return Err(anyhow!("매수 가능 금액 부족 (최소 ${} 필요)", min_order_usd));
        }

        let fee = UsStockFee::buy_fee(buy_limit_usd);
        let quantity = ((buy_limit_usd - fee.total) / price_usd) as u32;

        if quantity == 0 {
            warn!("  ❌ 매수 수량 0 (가격: ${:.2}, 가능 금액: ${:.2})", price_usd, buy_limit_usd);
            return Err(anyhow!("매수 가능 금액 부족 (수량 0)"));
        }

        let total_cost_usd = price_usd * quantity as f64;
        let total_with_fee_usd = total_cost_usd + fee.total;
        let total_with_fee_krw = self.usd_to_krw(total_with_fee_usd);
        if self.dry_run {
            // 포지션 추가/업데이트
            if let Some(pos) = self.positions.get_mut(symbol) {
                let total_qty = pos.quantity + quantity;
                let total_cost = (pos.avg_price_usd * pos.quantity as f64) + total_with_fee_usd;
                let new_avg = total_cost / total_qty as f64;
                pos.quantity = total_qty;
                pos.avg_price_usd = new_avg;
            } else {
                self.positions.insert(symbol.to_string(), UsPosition {
                    symbol: symbol.to_string(),
                    name: name.to_string(),
                    quantity,
                    avg_price_usd: price_usd,
                    current_price_usd: price_usd,
                    profit_loss_rate: 0.0,
                });
            }
            
            self.total_cash_krw -= total_with_fee_krw;
            
            // 거래 기록 추가
            self.daily_report.trades.push(TradeRecord {
                time: Local::now().format("%H:%M:%S").to_string(),
                action: "매수".to_string(),
                symbol: symbol.to_string(),
                name: name.to_string(),
                quantity,
                price: total_with_fee_krw,  // 원화로 기록
                total_amount: total_with_fee_krw,
                profit_loss: None,
            });
            self.daily_report.summary.total_trades += 1;
            self.daily_report.summary.buy_count += 1;
            
            return Ok(());
        }

        // 실제 주문 (지정가로 주문)
        match self.api.buy_us_stock(symbol, quantity, price_usd).await {
            Ok(order_response) => {
                // 포지션 업데이트 (실제)
                if let Some(pos) = self.positions.get_mut(symbol) {
                    let total_qty = pos.quantity + quantity;
                    let total_cost = (pos.avg_price_usd * pos.quantity as f64) + total_with_fee_usd;
                    pos.quantity = total_qty;
                    pos.avg_price_usd = total_cost / total_qty as f64;
                } else {
                    self.positions.insert(symbol.to_string(), UsPosition {
                        symbol: symbol.to_string(),
                        name: name.to_string(),
                        quantity,
                        avg_price_usd: price_usd,
                        current_price_usd: price_usd,
                        profit_loss_rate: 0.0,
                    });
                }
                
                self.total_cash_krw -= total_with_fee_krw;
                
                // 거래 기록 추가
                self.daily_report.trades.push(TradeRecord {
                    time: Local::now().format("%H:%M:%S").to_string(),
                    action: "매수".to_string(),
                    symbol: symbol.to_string(),
                    name: name.to_string(),
                    quantity,
                    price: total_with_fee_krw,  // 원화로 기록
                    total_amount: total_with_fee_krw,
                    profit_loss: None,
                });
                self.daily_report.summary.total_trades += 1;
                self.daily_report.summary.buy_count += 1;
                
                Ok(())
            }
            Err(e) => {
                error!("❌ 매수 실패: {}", e);
                Err(e)  // 오류를 그대로 반환
            }
        }
    }

    /// 자동매매 실행
    pub async fn run(&mut self) -> Result<()> {
        if self.dry_run {
        } else {
            warn!("⚠️  모드: 실전 거래!");
        }
        // 계좌 정보 조회
        match self.api.get_account_info().await {
            Ok((cash, _)) => {
                self.total_cash_krw = cash;
            }
            Err(e) => {
                warn!("계좌 조회 실패: {}", e);
                self.total_cash_krw = 820001.0;
            }
        }
        for (symbol, name) in &self.watchlist {
        }
        // 시작 자산 기록
        self.starting_balance_krw = self.total_cash_krw + self.get_total_position_value_krw();
        self.daily_report.performance.starting_balance = self.starting_balance_krw;
        let mut loop_count = 0;
        let mut last_market_check_time = Local::now().time();

        loop {
            loop_count += 1;
            let now = Local::now();
            let current_date = now.format("%Y-%m-%d").to_string();
            let current_time = now.time();
            // 1. 매도 신호 확인
            let sell_signals = self.check_sell_signals().await;
            for (symbol, name, qty, price) in sell_signals {
                // TODO: execute_sell 구현
            }

            // 2. 매수 신호 확인 및 실행
            let buy_signals = self.check_buy_signals().await;
            
            for (idx, (symbol, name, price_usd)) in buy_signals.iter().enumerate() {
                match self.execute_buy(symbol, name, *price_usd).await {
                    Ok(_) => {
                        break;  // 한 종목만 매수하고 다음 루프
                    }
                    Err(e) => {
                        warn!("⚠️  {}순위 매수 실패: {} - 다음 순위 시도...", idx + 1, e);
                        // 다음 순위 계속 시도
                    }
                }
            }
            
            if buy_signals.is_empty() {
            }

            // 3. 현재 상태
            for (symbol, pos) in &self.positions {
                let value_usd = pos.current_price_usd * pos.quantity as f64;
                let value_krw = self.usd_to_krw(value_usd);
            }
            // 4. 미국 장 마감 후 일일 리포트 생성 (06:10 또는 05:10)
            // 미국 장 시간: 서머타임 23:30~06:00, 동계 22:30~05:00 (한국 시간)
            use chrono::NaiveTime;
            let report_time_summer = NaiveTime::from_hms_opt(6, 10, 0).unwrap();
            let report_time_winter = NaiveTime::from_hms_opt(5, 10, 0).unwrap();
            
            // 현재 시간이 리포트 생성 시간이고, 마지막 체크 시간이 리포트 시간 이전이면 리포트 생성
            if (current_time >= report_time_summer && last_market_check_time < report_time_summer) ||
               (current_time >= report_time_winter && last_market_check_time < report_time_winter) {
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
                self.starting_balance_krw = self.total_cash_krw + self.get_total_position_value_krw();
                self.daily_report.performance.starting_balance = self.starting_balance_krw;
            }
            sleep(Duration::from_secs(60)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // .env 파일 로드
    dotenv::dotenv().ok();
    
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n🌎 한국투자증권 미국 주식 자동매매\n");

    let args: Vec<String> = std::env::args().collect();
    let dry_run = if args.len() > 1 && args[1] == "--real" {
        println!("⚠️  실전 거래 모드!");
        println!("⚠️  미국 주식은 수수료가 높습니다!");
        println!("⚠️  최소 수수료: $5 (~7,000원)\n");
        false
    } else {
        println!("🔍 DRY-RUN 모드\n");
        true
    };

    let strategy = UsTradingStrategy::default();
    
    println!("💱 환율: 1 USD = {:.0} KRW", strategy.usd_krw_rate);
    println!("💵 최소 투자 권장: $100 (~135,000원)\n");

    let mut trading_system = UsAutoTradingSystem::new(strategy, dry_run).await?;
    
    trading_system.run().await?;

    Ok(())
}


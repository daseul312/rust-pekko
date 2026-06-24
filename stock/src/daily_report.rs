use chrono::{Local, NaiveTime};
use serde::{Serialize, Deserialize};

/// 한국 주식 시장 거래 시간
pub struct KoreanMarketHours;

impl KoreanMarketHours {
    pub fn market_open() -> NaiveTime {
        NaiveTime::from_hms_opt(9, 0, 0).unwrap()
    }
    
    pub fn market_close() -> NaiveTime {
        NaiveTime::from_hms_opt(15, 30, 0).unwrap()
    }
    
    /// 장 마감 여부 확인
    pub fn is_market_closed() -> bool {
        let now = Local::now().time();
        now >= Self::market_close()
    }
    
    /// 리포트 생성 시간 (장 마감 후 10분)
    pub fn report_time() -> NaiveTime {
        NaiveTime::from_hms_opt(15, 40, 0).unwrap()
    }
}

/// 일일 거래 리포트
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DailyTradingReport {
    pub date: String,
    pub summary: TradingSummary,
    pub trades: Vec<TradeRecord>,
    pub positions: Vec<PositionSummary>,
    pub performance: PerformanceMetrics,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradingSummary {
    pub total_trades: usize,
    pub buy_count: usize,
    pub sell_count: usize,
    pub total_profit_loss: f64,
    pub total_profit_loss_rate: f64,
    pub win_rate: f64,
}

impl Default for TradingSummary {
    fn default() -> Self {
        Self {
            total_trades: 0,
            buy_count: 0,
            sell_count: 0,
            total_profit_loss: 0.0,
            total_profit_loss_rate: 0.0,
            win_rate: 0.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeRecord {
    pub time: String,
    pub action: String,  // "매수" or "매도"
    pub symbol: String,
    pub name: String,
    pub quantity: u32,
    pub price: f64,
    pub total_amount: f64,
    pub profit_loss: Option<f64>,  // 매도 시만
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PositionSummary {
    pub symbol: String,
    pub name: String,
    pub quantity: u32,
    pub avg_price: f64,
    pub current_price: f64,
    pub profit_loss_rate: f64,
    pub total_value: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceMetrics {
    pub starting_balance: f64,
    pub ending_balance: f64,
    pub daily_return: f64,
    pub daily_return_rate: f64,
    pub max_drawdown: f64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            starting_balance: 0.0,
            ending_balance: 0.0,
            daily_return: 0.0,
            daily_return_rate: 0.0,
            max_drawdown: 0.0,
        }
    }
}

impl DailyTradingReport {
    pub fn new(date: String) -> Self {
        Self {
            date,
            summary: TradingSummary::default(),
            trades: Vec::new(),
            positions: Vec::new(),
            performance: PerformanceMetrics::default(),
        }
    }
    
    /// 노션 마크다운 형식으로 변환
    pub fn to_markdown(&self) -> String {
        format!(
            r#"# 📊 일일 주식 거래 리포트

## 날짜: {}

## 📈 거래 요약
- 총 거래 횟수: {}회
- 매수: {}회 / 매도: {}회
- 총 손익: {:+.0}원 ({:+.2}%)
- 승률: {:.1}%

## 💰 수익 현황
- 시작 자산: {:.0}원
- 종료 자산: {:.0}원
- 일일 수익: {:+.0}원
- 일일 수익률: {:+.2}%
- 최대 낙폭: {:.2}%

## 📋 거래 내역
{}

## 🎯 현재 보유 종목
{}

## 💡 분석 및 인사이트
{}
"#,
            self.date,
            self.summary.total_trades,
            self.summary.buy_count,
            self.summary.sell_count,
            self.summary.total_profit_loss,
            self.summary.total_profit_loss_rate,
            self.summary.win_rate,
            self.performance.starting_balance,
            self.performance.ending_balance,
            self.performance.daily_return,
            self.performance.daily_return_rate,
            self.performance.max_drawdown,
            self.format_trades(),
            self.format_positions(),
            self.generate_insights()
        )
    }
    
    fn format_trades(&self) -> String {
        if self.trades.is_empty() {
            return "거래 없음".to_string();
        }
        
        self.trades.iter().map(|t| {
            let pl_str = if let Some(pl) = t.profit_loss {
                format!(" (손익: {:+.0}원)", pl)
            } else {
                String::new()
            };
            
            format!(
                "- {} | {} | {} ({}) | {}주 @ {:.0}원 | 총 {:.0}원{}",
                t.time, t.action, t.symbol, t.name, 
                t.quantity, t.price, t.total_amount, pl_str
            )
        }).collect::<Vec<_>>().join("\n")
    }
    
    fn format_positions(&self) -> String {
        if self.positions.is_empty() {
            return "보유 종목 없음 (올인 현금)".to_string();
        }
        
        self.positions.iter().map(|p| {
            format!(
                "- {} ({}) | {}주 @ {:.0}원 | 현재가: {:.0}원 | 수익률: {:+.2}% | 평가액: {:.0}원",
                p.symbol, p.name, p.quantity, p.avg_price,
                p.current_price, p.profit_loss_rate, p.total_value
            )
        }).collect::<Vec<_>>().join("\n")
    }
    
    fn generate_insights(&self) -> String {
        let mut insights = Vec::new();
        
        if self.summary.win_rate >= 70.0 {
            insights.push("✅ 높은 승률을 기록했습니다!");
        } else if self.summary.win_rate < 40.0 && self.summary.total_trades > 0 {
            insights.push("⚠️ 승률이 낮습니다. 전략 점검 필요");
        }
        
        if self.performance.daily_return_rate > 3.0 {
            insights.push("🎉 일일 목표 수익률 달성!");
        } else if self.performance.daily_return_rate < -2.0 {
            insights.push("⚠️ 손실이 발생했습니다. 리스크 관리 점검");
        }
        
        if self.summary.total_trades == 0 {
            insights.push("📭 오늘은 거래 조건을 만족하는 종목이 없었습니다");
        } else if self.summary.total_trades > 10 {
            insights.push("⚡ 활발한 거래가 이루어졌습니다");
        }
        
        if insights.is_empty() {
            insights.push("📊 정상적인 거래 활동");
        }
        
        insights.join("\n- ")
    }
}


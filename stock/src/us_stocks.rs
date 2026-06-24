use serde::{Deserialize, Serialize};
// use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsStockFee {
    pub commission: f64,
    pub tax: f64,
    pub total: f64,
}

impl UsStockFee {
    pub fn buy_fee(amount_usd: f64) -> Self {
        let commission = if amount_usd <= 1000.0 { 2.0 } else { 5.0 };
        let tax = amount_usd * 0.001; // 0.1% tax
        Self {
            commission,
            tax,
            total: commission + tax,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UsTradingHours {
    pub market_open: String,
    pub market_close: String,
    pub timezone: String,
}

pub fn get_us_watchlist() -> Vec<String> {
    vec![
        "AAPL".to_string(),
        "GOOGL".to_string(),
        "MSFT".to_string(),
        "AMZN".to_string(),
        "TSLA".to_string(),
        "META".to_string(),
        "NVDA".to_string(),
        "NFLX".to_string(),
    ]
}

impl Default for UsStockFee {
    fn default() -> Self {
        Self {
            commission: 0.5,
            tax: 0.1,
            total: 0.6,
        }
    }
}

impl Default for UsTradingHours {
    fn default() -> Self {
        Self {
            market_open: "09:30".to_string(),
            market_close: "16:00".to_string(),
            timezone: "EST".to_string(),
        }
    }
}

impl UsTradingHours {
    pub fn regular_hours() -> String {
        "09:30 - 16:00 EST (Regular Trading Hours)".to_string()
    }
}
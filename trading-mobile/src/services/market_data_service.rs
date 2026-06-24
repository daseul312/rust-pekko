use crate::StockData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataRequest {
    pub symbols: Vec<String>,
}

pub struct MarketDataService {
    // This would integrate with the Pekko actor system
    // and the existing market data infrastructure
}

impl MarketDataService {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_stock_data(&self, symbols: Vec<String>) -> Result<Vec<StockData>, String> {
        // TODO: Integrate with Pekko actor system
        // This would request data from the data agent actor
        
        // For now, return mock data
        let mut stocks = Vec::new();
        
        for symbol in symbols {
            stocks.push(StockData {
                symbol: symbol.clone(),
                price: match symbol.as_str() {
                    "AAPL" => 175.43,
                    "GOOGL" => 142.56,
                    "MSFT" => 378.85,
                    "TSLA" => 245.67,
                    _ => 100.0,
                },
                change: match symbol.as_str() {
                    "AAPL" => 2.34,
                    "GOOGL" => -1.23,
                    "MSFT" => 4.67,
                    "TSLA" => -3.21,
                    _ => 0.0,
                },
                change_percent: match symbol.as_str() {
                    "AAPL" => 1.35,
                    "GOOGL" => -0.85,
                    "MSFT" => 1.25,
                    "TSLA" => -1.29,
                    _ => 0.0,
                },
            });
        }
        
        Ok(stocks)
    }

    pub async fn get_watchlist(&self) -> Result<Vec<StockData>, String> {
        // TODO: Fetch user's watchlist from actor system
        let symbols = vec![
            "AAPL".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "TSLA".to_string(),
        ];
        
        self.get_stock_data(symbols).await
    }

    pub async fn search_symbols(&self, query: &str) -> Result<Vec<String>, String> {
        // TODO: Search symbols through actor system
        let symbols = vec![
            "AAPL".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "TSLA".to_string(),
            "AMZN".to_string(),
            "META".to_string(),
        ];
        
        let filtered: Vec<String> = symbols
            .into_iter()
            .filter(|s| s.to_lowercase().contains(&query.to_lowercase()))
            .collect();
            
        Ok(filtered)
    }
}

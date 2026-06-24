use crate::models::{Stock, StockRecommendationMessage};
use pekko_actor::{Actor, ActorContext};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{ warn};

/// 주식 데이터베이스를 관리하는 액터
#[derive(Debug, Clone)]
pub struct StockDatabaseActor {
    stocks: HashMap<String, Stock>,
    low_risk_stocks: Vec<Stock>,
}

impl StockDatabaseActor {
    pub fn new() -> Self {
        let mut actor = Self {
            stocks: HashMap::new(),
            low_risk_stocks: Vec::new(),
        };
        
        // 샘플 데이터 초기화
        actor.initialize_sample_data();
        actor
    }

    /// 샘플 주식 데이터 초기화
    fn initialize_sample_data(&mut self) {
        let sample_stocks = vec![
            // 대형 안전 주식들
            Stock::new(
                "AAPL".to_string(),
                "Apple Inc.".to_string(),
                "Technology".to_string(),
                2_800_000_000_000.0, // 2.8조 달러
                175.0,
                Some(28.5),
                Some(0.5),
                Some(0.65),
                0.25,
            ),
            Stock::new(
                "MSFT".to_string(),
                "Microsoft Corporation".to_string(),
                "Technology".to_string(),
                2_600_000_000_000.0, // 2.6조 달러
                380.0,
                Some(32.1),
                Some(0.7),
                Some(0.72),
                0.28,
            ),
            Stock::new(
                "JNJ".to_string(),
                "Johnson & Johnson".to_string(),
                "Healthcare".to_string(),
                450_000_000_000.0, // 4500억 달러
                160.0,
                Some(15.2),
                Some(2.8),
                Some(0.45),
                0.18,
            ),
            Stock::new(
                "PG".to_string(),
                "Procter & Gamble".to_string(),
                "Consumer Goods".to_string(),
                380_000_000_000.0, // 3800억 달러
                155.0,
                Some(24.8),
                Some(2.4),
                Some(0.38),
                0.15,
            ),
            Stock::new(
                "KO".to_string(),
                "The Coca-Cola Company".to_string(),
                "Consumer Goods".to_string(),
                260_000_000_000.0, // 2600억 달러
                60.0,
                Some(22.5),
                Some(3.2),
                Some(0.42),
                0.16,
            ),
            Stock::new(
                "PEP".to_string(),
                "PepsiCo Inc.".to_string(),
                "Consumer Goods".to_string(),
                240_000_000_000.0, // 2400억 달러
                170.0,
                Some(26.3),
                Some(2.9),
                Some(0.48),
                0.19,
            ),
            Stock::new(
                "WMT".to_string(),
                "Walmart Inc.".to_string(),
                "Retail".to_string(),
                480_000_000_000.0, // 4800억 달러
                160.0,
                Some(28.9),
                Some(1.4),
                Some(0.52),
                0.22,
            ),
            Stock::new(
                "VZ".to_string(),
                "Verizon Communications".to_string(),
                "Telecommunications".to_string(),
                180_000_000_000.0, // 1800억 달러
                40.0,
                Some(8.5),
                Some(6.8),
                Some(0.35),
                0.14,
            ),
            Stock::new(
                "T".to_string(),
                "AT&T Inc.".to_string(),
                "Telecommunications".to_string(),
                120_000_000_000.0, // 1200억 달러
                18.0,
                Some(7.2),
                Some(7.5),
                Some(0.58),
                0.24,
            ),
            Stock::new(
                "MCD".to_string(),
                "McDonald's Corporation".to_string(),
                "Food & Beverage".to_string(),
                200_000_000_000.0, // 2000억 달러
                280.0,
                Some(25.4),
                Some(2.1),
                Some(0.48),
                0.20,
            ),
            // 중형 안전 주식들
            Stock::new(
                "CL".to_string(),
                "Colgate-Palmolive".to_string(),
                "Consumer Goods".to_string(),
                65_000_000_000.0, // 650억 달러
                80.0,
                Some(28.7),
                Some(2.3),
                Some(0.42),
                0.17,
            ),
            Stock::new(
                "GIS".to_string(),
                "General Mills".to_string(),
                "Food & Beverage".to_string(),
                45_000_000_000.0, // 450억 달러
                70.0,
                Some(16.8),
                Some(3.4),
                Some(0.38),
                0.15,
            ),
            // 위험한 주식들 (비교용)
            Stock::new(
                "TSLA".to_string(),
                "Tesla Inc.".to_string(),
                "Automotive".to_string(),
                800_000_000_000.0, // 8000억 달러
                250.0,
                Some(65.2),
                Some(0.0),
                Some(2.1),
                0.45,
            ),
            Stock::new(
                "NVDA".to_string(),
                "NVIDIA Corporation".to_string(),
                "Technology".to_string(),
                1_200_000_000_000.0, // 1.2조 달러
                450.0,
                Some(45.8),
                Some(0.1),
                Some(1.8),
                0.42,
            ),
        ];

        for stock in sample_stocks {
            let symbol = stock.symbol.clone();
            self.add_stock(stock);
        }
    }

    /// 주식 추가
    fn add_stock(&mut self, stock: Stock) {
        let symbol = stock.symbol.clone();
        let is_low_risk = stock.is_low_risk();
        
        self.stocks.insert(symbol.clone(), stock.clone());
        
        if is_low_risk {
            self.low_risk_stocks.push(stock);
            self.low_risk_stocks.sort_by(|a, b| {
                b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap()
            });
        }
    }

    /// 주식 업데이트
    fn update_stock(&mut self, symbol: &str, updated_stock: Stock) {
        if let Some(old_stock) = self.stocks.get(symbol) {
            let was_low_risk = old_stock.is_low_risk();
            let is_low_risk = updated_stock.is_low_risk();
            
            // 기존 저위험 주식 목록에서 제거
            if was_low_risk {
                self.low_risk_stocks.retain(|s| s.symbol != symbol);
            }
            
            // 새 주식 추가
            self.stocks.insert(symbol.to_string(), updated_stock.clone());
            
            // 저위험 주식이면 목록에 추가
            if is_low_risk {
                self.low_risk_stocks.push(updated_stock);
                self.low_risk_stocks.sort_by(|a, b| {
                    b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap()
                });
            }
        } else {
            warn!("업데이트할 주식을 찾을 수 없음: {}", symbol);
        }
    }

    /// 저위험 주식 목록 반환
    fn get_low_risk_stocks(&self) -> Vec<Stock> {
        self.low_risk_stocks.clone()
    }

    /// 섹터별 추천 주식 반환
    fn get_recommendations_by_sector(&self, sector: &str) -> Vec<Stock> {
        let mut sector_stocks: Vec<Stock> = self.low_risk_stocks
            .iter()
            .filter(|stock| stock.sector.to_lowercase() == sector.to_lowercase())
            .cloned()
            .collect();
        
        sector_stocks.sort_by(|a, b| {
            b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap()
        });
        
        sector_stocks
    }

    /// 상위 추천 주식 반환
    fn get_top_recommendations(&self, limit: usize) -> Vec<Stock> {
        self.low_risk_stocks
            .iter()
            .take(limit)
            .cloned()
            .collect()
    }

    /// 특정 주식 정보 반환
    fn get_stock_info(&self, symbol: &str) -> Option<Stock> {
        self.stocks.get(symbol).cloned()
    }
}

#[async_trait]
impl Actor for StockDatabaseActor {
    type Message = StockRecommendationMessage;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            StockRecommendationMessage::GetLowRiskStocks => {
                let stocks = self.get_low_risk_stocks();
                // 실제 구현에서는 응답을 보내는 로직이 필요
            }
            
            StockRecommendationMessage::GetRecommendationsBySector(sector) => {
                let stocks = self.get_recommendations_by_sector(&sector);
            }
            
            StockRecommendationMessage::GetTopRecommendations(limit) => {
                let stocks = self.get_top_recommendations(limit);
            }
            
            StockRecommendationMessage::AddStock(stock) => {
                self.add_stock(stock);
            }
            
            StockRecommendationMessage::UpdateStock(symbol, stock) => {
                self.update_stock(&symbol, stock);
            }
            
            StockRecommendationMessage::GetStockInfo(symbol) => {
                if let Some(_stock) = self.get_stock_info(&symbol) {
                } else {
                    warn!("주식 정보를 찾을 수 없음: {}", symbol);
                }
            }
        }
    }

    async fn pre_start(&mut self) {
    }

    async fn post_stop(&mut self) {
    }
}

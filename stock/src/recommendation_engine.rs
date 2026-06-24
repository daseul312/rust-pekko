use crate::models::{Stock, RiskLevel};
use crate::database::StockDatabaseActor;
use pekko_actor::{Actor, ActorContext, ActorRef};
use async_trait::async_trait;
use std::collections::HashMap;
/// 주식 추천 엔진 액터
#[derive(Debug, Clone)]
pub struct StockRecommendationEngine {
    database_ref: Option<ActorRef<StockDatabaseActor>>,
    recommendation_history: HashMap<String, Vec<Stock>>, // 사용자별 추천 히스토리
    market_conditions: MarketConditions,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub market_volatility: f64,    // 시장 변동성 (0-1)
    pub interest_rate: f64,         // 금리 (%)
    pub economic_outlook: EconomicOutlook,
}

#[derive(Debug, Clone)]
pub enum EconomicOutlook {
    Bullish,    // 상승장
    Bearish,    // 하락장
    Neutral,    // 횡보장
}

impl StockRecommendationEngine {
    pub fn new() -> Self {
        Self {
            database_ref: None,
            recommendation_history: HashMap::new(),
            market_conditions: MarketConditions {
                market_volatility: 0.3,
                interest_rate: 5.25,
                economic_outlook: EconomicOutlook::Neutral,
            },
        }
    }

    /// 데이터베이스 액터 참조 설정
    pub fn set_database_ref(&mut self, database_ref: ActorRef<StockDatabaseActor>) {
        self.database_ref = Some(database_ref);
    }

    /// 저위험 주식 추천
    fn recommend_low_risk_stocks(&self) -> Vec<Stock> {
        // 실제 구현에서는 데이터베이스에서 조회
        // 여기서는 샘플 데이터를 직접 생성
        vec![
            Stock::new(
                "JNJ".to_string(),
                "Johnson & Johnson".to_string(),
                "Healthcare".to_string(),
                450_000_000_000.0,
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
                380_000_000_000.0,
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
                260_000_000_000.0,
                60.0,
                Some(22.5),
                Some(3.2),
                Some(0.42),
                0.16,
            ),
        ]
    }

    /// 섹터별 추천
    fn recommend_by_sector(&self, sector: &str) -> Vec<Stock> {
        let all_stocks = self.recommend_low_risk_stocks();
        all_stocks
            .into_iter()
            .filter(|stock| stock.sector.to_lowercase() == sector.to_lowercase())
            .collect()
    }

    /// 포트폴리오 추천
    fn recommend_portfolio(&self, risk_tolerance: RiskLevel, _investment_amount: f64) -> PortfolioRecommendation {
        let stocks = self.recommend_low_risk_stocks();
        
        match risk_tolerance {
            RiskLevel::VeryLow => {
                // 매우 안전한 포트폴리오: 대형 배당주 중심
                let conservative_stocks: Vec<Stock> = stocks
                    .into_iter()
                    .filter(|s| s.dividend_yield.unwrap_or(0.0) > 2.0)
                    .take(5)
                    .collect();
                
                PortfolioRecommendation {
                    stocks: conservative_stocks,
                    allocation_strategy: AllocationStrategy::Conservative,
                    expected_return: 4.0,
                    risk_level: RiskLevel::VeryLow,
                }
            }
            RiskLevel::Low => {
                // 안전한 포트폴리오: 균형 잡힌 구성
                let balanced_stocks: Vec<Stock> = stocks
                    .into_iter()
                    .take(8)
                    .collect();
                
                PortfolioRecommendation {
                    stocks: balanced_stocks,
                    allocation_strategy: AllocationStrategy::Balanced,
                    expected_return: 6.0,
                    risk_level: RiskLevel::Low,
                }
            }
            _ => {
                // 기본 추천
                PortfolioRecommendation {
                    stocks: stocks.into_iter().take(3).collect(),
                    allocation_strategy: AllocationStrategy::Conservative,
                    expected_return: 5.0,
                    risk_level: RiskLevel::Low,
                }
            }
        }
    }

    /// 시장 조건에 따른 추천 조정
    fn adjust_recommendations_for_market(&self, stocks: Vec<Stock>) -> Vec<Stock> {
        let mut adjusted_stocks = stocks;
        
        match self.market_conditions.economic_outlook {
            EconomicOutlook::Bearish => {
                // 하락장에서는 방어적 주식 선호
                adjusted_stocks.sort_by(|a, b| {
                    let a_score = a.dividend_yield.unwrap_or(0.0) + (1.0 - a.beta.unwrap_or(1.0));
                    let b_score = b.dividend_yield.unwrap_or(0.0) + (1.0 - b.beta.unwrap_or(1.0));
                    b_score.partial_cmp(&a_score).unwrap()
                });
            }
            EconomicOutlook::Bullish => {
                // 상승장에서는 성장주도 고려하되 안전성 유지
                adjusted_stocks.sort_by(|a, b| {
                    b.recommendation_score.partial_cmp(&a.recommendation_score).unwrap()
                });
            }
            EconomicOutlook::Neutral => {
                // 횡보장에서는 배당 수익률 중심
                adjusted_stocks.sort_by(|a, b| {
                    b.dividend_yield.unwrap_or(0.0).partial_cmp(&a.dividend_yield.unwrap_or(0.0)).unwrap()
                });
            }
        }
        
        adjusted_stocks
    }
}

#[derive(Debug, Clone)]
pub struct PortfolioRecommendation {
    pub stocks: Vec<Stock>,
    pub allocation_strategy: AllocationStrategy,
    pub expected_return: f64,
    pub risk_level: RiskLevel,
}

#[derive(Debug, Clone)]
pub enum AllocationStrategy {
    Conservative,  // 보수적 배분
    Balanced,     // 균형 배분
    Aggressive,   // 공격적 배분
}

/// 추천 요청 메시지
#[derive(Debug, Clone)]
pub enum RecommendationRequest {
    GetLowRiskRecommendations,
    GetSectorRecommendations(String),
    GetPortfolioRecommendation(RiskLevel, f64), // 위험도, 투자금액
    UpdateMarketConditions(MarketConditions),
}

#[async_trait]
impl Actor for StockRecommendationEngine {
    type Message = RecommendationRequest;

    async fn receive(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self>) {
        match msg {
            RecommendationRequest::GetLowRiskRecommendations => {
                let stocks = self.recommend_low_risk_stocks();
                let adjusted_stocks = self.adjust_recommendations_for_market(stocks);
            }
            
            RecommendationRequest::GetSectorRecommendations(sector) => {
                let stocks = self.recommend_by_sector(&sector);
                let adjusted_stocks = self.adjust_recommendations_for_market(stocks);
            }
            
            RecommendationRequest::GetPortfolioRecommendation(risk_level, amount) => {
                let _portfolio = self.recommend_portfolio(risk_level.clone(), amount);
            }
            
            RecommendationRequest::UpdateMarketConditions(conditions) => {
                self.market_conditions = conditions;
            }
        }
    }

    async fn pre_start(&mut self) {
    }

    async fn post_stop(&mut self) {
    }
}

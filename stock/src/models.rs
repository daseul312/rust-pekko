use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// 주식 정보를 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stock {
    pub symbol: String,
    pub name: String,
    pub sector: String,
    pub market_cap: f64,
    pub current_price: f64,
    pub pe_ratio: Option<f64>,
    pub dividend_yield: Option<f64>,
    pub beta: Option<f64>,
    pub volatility: f64,
    pub risk_score: f64,
    pub recommendation_score: f64,
    pub last_updated: DateTime<Utc>,
}

/// 위험도 등급
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RiskLevel {
    VeryLow,    // 매우 낮음 (베타 < 0.5)
    Low,        // 낮음 (베타 0.5-0.8)
    Medium,     // 보통 (베타 0.8-1.2)
    High,       // 높음 (베타 1.2-1.5)
    VeryHigh,   // 매우 높음 (베타 > 1.5)
}

impl Stock {
    pub fn new(
        symbol: String,
        name: String,
        sector: String,
        market_cap: f64,
        current_price: f64,
        pe_ratio: Option<f64>,
        dividend_yield: Option<f64>,
        beta: Option<f64>,
        volatility: f64,
    ) -> Self {
        let risk_score = Self::calculate_risk_score(beta, volatility, market_cap);
        let recommendation_score = Self::calculate_recommendation_score(
            pe_ratio, dividend_yield, beta, volatility, market_cap
        );

        Self {
            symbol,
            name,
            sector,
            market_cap,
            current_price,
            pe_ratio,
            dividend_yield,
            beta,
            volatility,
            risk_score,
            recommendation_score,
            last_updated: Utc::now(),
        }
    }

    /// 위험도 점수 계산 (0-100, 낮을수록 안전)
    fn calculate_risk_score(beta: Option<f64>, volatility: f64, market_cap: f64) -> f64 {
        let mut score: f64 = 0.0;

        // 베타 계수 기반 점수 (40% 가중치)
        if let Some(beta) = beta {
            score += beta * 20.0; // 베타 1.0 = 20점
        } else {
            score += 15.0; // 베타 정보 없으면 중간 점수
        }

        // 변동성 기반 점수 (30% 가중치)
        score += volatility * 30.0; // 변동성 1% = 30점

        // 시가총액 기반 점수 (30% 가중치) - 큰 회사일수록 안전
        let market_cap_score = if market_cap > 100_000_000_000.0 {
            5.0  // 대형주 (1000억 이상)
        } else if market_cap > 10_000_000_000.0 {
            15.0 // 중형주 (100억 이상)
        } else if market_cap > 1_000_000_000.0 {
            25.0 // 소형주 (10억 이상)
        } else {
            35.0 // 초소형주
        };
        score += market_cap_score;

        score.min(100.0)
    }

    /// 추천 점수 계산 (0-100, 높을수록 추천)
    fn calculate_recommendation_score(
        pe_ratio: Option<f64>,
        dividend_yield: Option<f64>,
        beta: Option<f64>,
        _volatility: f64,
        market_cap: f64,
    ) -> f64 {
        let mut score: f64 = 0.0;

        // PER 기반 점수 (25% 가중치) - 적정 PER일수록 높은 점수
        if let Some(pe) = pe_ratio {
            if pe > 0.0 && pe < 20.0 {
                score += 25.0; // 적정 PER
            } else if pe >= 20.0 && pe < 30.0 {
                score += 15.0; // 약간 높은 PER
            } else if pe >= 30.0 {
                score += 5.0;  // 높은 PER
            }
        } else {
            score += 12.5; // PER 정보 없으면 중간 점수
        }

        // 배당 수익률 기반 점수 (25% 가중치)
        if let Some(dividend) = dividend_yield {
            if dividend > 3.0 {
                score += 25.0; // 높은 배당
            } else if dividend > 2.0 {
                score += 20.0; // 적정 배당
            } else if dividend > 1.0 {
                score += 15.0; // 낮은 배당
            } else {
                score += 10.0; // 매우 낮은 배당
            }
        } else {
            score += 12.5; // 배당 정보 없으면 중간 점수
        }

        // 베타 기반 점수 (25% 가중치) - 낮은 베타일수록 높은 점수
        if let Some(beta) = beta {
            if beta < 0.5 {
                score += 25.0; // 매우 안전
            } else if beta < 0.8 {
                score += 20.0; // 안전
            } else if beta < 1.2 {
                score += 15.0; // 보통
            } else if beta < 1.5 {
                score += 10.0; // 위험
            } else {
                score += 5.0;  // 매우 위험
            }
        } else {
            score += 12.5; // 베타 정보 없으면 중간 점수
        }

        // 시가총액 기반 점수 (25% 가중치) - 큰 회사일수록 높은 점수
        let market_cap_score = if market_cap > 100_000_000_000.0 {
            25.0  // 대형주
        } else if market_cap > 10_000_000_000.0 {
            20.0  // 중형주
        } else if market_cap > 1_000_000_000.0 {
            15.0  // 소형주
        } else {
            10.0  // 초소형주
        };
        score += market_cap_score;

        score.min(100.0)
    }

    /// 위험도 등급 반환
    pub fn get_risk_level(&self) -> RiskLevel {
        match self.beta {
            Some(beta) if beta < 0.5 => RiskLevel::VeryLow,
            Some(beta) if beta < 0.8 => RiskLevel::Low,
            Some(beta) if beta < 1.2 => RiskLevel::Medium,
            Some(beta) if beta < 1.5 => RiskLevel::High,
            Some(_) => RiskLevel::VeryHigh,
            None => RiskLevel::Medium, // 베타 정보 없으면 보통으로 분류
        }
    }

    /// 저위험 주식인지 확인
    pub fn is_low_risk(&self) -> bool {
        matches!(self.get_risk_level(), RiskLevel::VeryLow | RiskLevel::Low)
    }

    /// 추천 가능한 주식인지 확인
    pub fn is_recommendable(&self) -> bool {
        self.is_low_risk() && self.recommendation_score >= 60.0
    }
}

/// 주식 추천 요청 메시지
#[derive(Debug, Clone)]
pub enum StockRecommendationMessage {
    GetLowRiskStocks,
    GetRecommendationsBySector(String),
    GetTopRecommendations(usize),
    AddStock(Stock),
    UpdateStock(String, Stock), // symbol, updated stock
    GetStockInfo(String),       // symbol
}

/// 주식 추천 응답 메시지
#[derive(Debug, Clone)]
pub enum StockRecommendationResponse {
    LowRiskStocks(Vec<Stock>),
    SectorRecommendations(String, Vec<Stock>),
    TopRecommendations(Vec<Stock>),
    StockInfo(Option<Stock>),
    Success(String),
    Error(String),
}

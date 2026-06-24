use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// 한국 주식 정보를 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KoreanStock {
    pub symbol: String,           // 종목 코드 (예: 005930)
    pub name: String,             // 종목명 (예: 삼성전자)
    pub market: KoreanMarket,     // 시장 구분
    pub sector: String,           // 섹터
    pub current_price: f64,       // 현재가
    pub previous_close: f64,      // 전일 종가
    pub price_change: f64,        // 등락률
    pub volume: i64,              // 거래량
    pub market_cap: i64,          // 시가총액
    pub per: Option<f64>,         // PER
    pub pbr: Option<f64>,         // PBR
    pub dividend_yield: Option<f64>, // 배당수익률
    pub beta: Option<f64>,        // 베타
    pub volatility: f64,          // 변동성
    pub risk_score: f64,          // 위험도 점수
    pub recommendation_score: f64, // 추천 점수
    pub last_updated: DateTime<Utc>,
}

/// 한국 주식 시장 구분
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KoreanMarket {
    KOSPI,      // 코스피
    KOSDAQ,     // 코스닥
    KONEX,      // 코넥스
}

/// 한국 주식 위험도 등급
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KoreanRiskLevel {
    VeryLow,    // 매우 낮음 (베타 < 0.5)
    Low,        // 낮음 (베타 0.5-0.8)
    Medium,     // 보통 (베타 0.8-1.2)
    High,       // 높음 (베타 1.2-1.5)
    VeryHigh,   // 매우 높음 (베타 > 1.5)
}

impl KoreanStock {
    pub fn new(
        symbol: String,
        name: String,
        market: KoreanMarket,
        sector: String,
        current_price: f64,
        previous_close: f64,
        price_change: f64,
        volume: i64,
        market_cap: i64,
        per: Option<f64>,
        pbr: Option<f64>,
        dividend_yield: Option<f64>,
        beta: Option<f64>,
        volatility: f64,
    ) -> Self {
        let risk_score = Self::calculate_korean_risk_score(beta, volatility, market_cap, market.clone());
        let recommendation_score = Self::calculate_korean_recommendation_score(
            per, pbr, dividend_yield, beta, volatility, market_cap, market.clone()
        );

        Self {
            symbol,
            name,
            market,
            sector,
            current_price,
            previous_close,
            price_change,
            volume,
            market_cap,
            per,
            pbr,
            dividend_yield,
            beta,
            volatility,
            risk_score,
            recommendation_score,
            last_updated: Utc::now(),
        }
    }

    /// 한국 주식 위험도 점수 계산
    fn calculate_korean_risk_score(
        beta: Option<f64>, 
        volatility: f64, 
        market_cap: i64, 
        market: KoreanMarket
    ) -> f64 {
        let mut score: f64 = 0.0;

        // 베타 계수 기반 점수 (40% 가중치)
        if let Some(beta) = beta {
            score += beta * 20.0;
        } else {
            score += 15.0;
        }

        // 변동성 기반 점수 (25% 가중치)
        score += volatility * 25.0;

        // 시가총액 기반 점수 (25% 가중치)
        let market_cap_score = if market_cap > 10_000_000_000_000 { // 10조원 이상
            5.0
        } else if market_cap > 1_000_000_000_000 { // 1조원 이상
            15.0
        } else if market_cap > 100_000_000_000 { // 1000억원 이상
            25.0
        } else {
            35.0
        };
        score += market_cap_score;

        // 시장 구분 기반 점수 (10% 가중치)
        let market_score = match market {
            KoreanMarket::KOSPI => 5.0,    // 코스피가 가장 안전
            KoreanMarket::KOSDAQ => 15.0,  // 코스닥은 중간
            KoreanMarket::KONEX => 25.0,   // 코넥스는 위험
        };
        score += market_score;

        score.min(100.0)
    }

    /// 한국 주식 추천 점수 계산
    fn calculate_korean_recommendation_score(
        per: Option<f64>,
        pbr: Option<f64>,
        dividend_yield: Option<f64>,
        beta: Option<f64>,
        volatility: f64,
        market_cap: i64,
        market: KoreanMarket,
    ) -> f64 {
        let mut score: f64 = 0.0;

        // PER 기반 점수 (20% 가중치)
        if let Some(per) = per {
            if per > 0.0 && per < 15.0 {
                score += 20.0; // 저평가
            } else if per >= 15.0 && per < 25.0 {
                score += 15.0; // 적정
            } else if per >= 25.0 && per < 40.0 {
                score += 10.0; // 고평가
            } else {
                score += 5.0;  // 매우 고평가
            }
        } else {
            score += 10.0;
        }

        // PBR 기반 점수 (15% 가중치)
        if let Some(pbr) = pbr {
            if pbr > 0.0 && pbr < 1.0 {
                score += 15.0; // 저평가
            } else if pbr >= 1.0 && pbr < 2.0 {
                score += 12.0; // 적정
            } else if pbr >= 2.0 && pbr < 3.0 {
                score += 8.0;  // 고평가
            } else {
                score += 5.0;  // 매우 고평가
            }
        } else {
            score += 7.5;
        }

        // 배당수익률 기반 점수 (20% 가중치)
        if let Some(dividend) = dividend_yield {
            if dividend > 3.0 {
                score += 20.0; // 높은 배당
            } else if dividend > 2.0 {
                score += 15.0; // 적정 배당
            } else if dividend > 1.0 {
                score += 10.0; // 낮은 배당
            } else {
                score += 5.0;  // 매우 낮은 배당
            }
        } else {
            score += 10.0;
        }

        // 베타 기반 점수 (20% 가중치)
        if let Some(beta) = beta {
            if beta < 0.5 {
                score += 20.0; // 매우 안전
            } else if beta < 0.8 {
                score += 16.0; // 안전
            } else if beta < 1.2 {
                score += 12.0; // 보통
            } else if beta < 1.5 {
                score += 8.0;  // 위험
            } else {
                score += 4.0;  // 매우 위험
            }
        } else {
            score += 10.0;
        }

        // 시가총액 기반 점수 (15% 가중치)
        let market_cap_score = if market_cap > 10_000_000_000_000 {
            15.0  // 대형주
        } else if market_cap > 1_000_000_000_000 {
            12.0  // 중형주
        } else if market_cap > 100_000_000_000 {
            9.0   // 소형주
        } else {
            6.0   // 초소형주
        };
        score += market_cap_score;

        // 시장 구분 보너스 (10% 가중치)
        let market_bonus = match market {
            KoreanMarket::KOSPI => 10.0,   // 코스피 보너스
            KoreanMarket::KOSDAQ => 5.0,   // 코스닥 보너스
            KoreanMarket::KONEX => 0.0,    // 코넥스 보너스 없음
        };
        score += market_bonus;

        score.min(100.0)
    }

    /// 한국 주식 위험도 등급 반환
    pub fn get_korean_risk_level(&self) -> KoreanRiskLevel {
        match self.beta {
            Some(beta) if beta < 0.5 => KoreanRiskLevel::VeryLow,
            Some(beta) if beta < 0.8 => KoreanRiskLevel::Low,
            Some(beta) if beta < 1.2 => KoreanRiskLevel::Medium,
            Some(beta) if beta < 1.5 => KoreanRiskLevel::High,
            Some(_) => KoreanRiskLevel::VeryHigh,
            None => KoreanRiskLevel::Medium,
        }
    }

    /// 저위험 한국 주식인지 확인
    pub fn is_korean_low_risk(&self) -> bool {
        matches!(self.get_korean_risk_level(), KoreanRiskLevel::VeryLow | KoreanRiskLevel::Low)
    }

    /// 추천 가능한 한국 주식인지 확인
    pub fn is_korean_recommendable(&self) -> bool {
        self.is_korean_low_risk() && self.recommendation_score >= 60.0
    }

    /// 한국 주식 섹터 분류
    pub fn classify_korean_sector(&self) -> String {
        match self.name.as_str() {
            name if name.contains("삼성") || name.contains("LG") || name.contains("SK") => "대기업".to_string(),
            name if name.contains("은행") || name.contains("금융") || name.contains("지주") => "금융".to_string(),
            name if name.contains("바이오") || name.contains("제약") || name.contains("의료") => "바이오".to_string(),
            name if name.contains("화학") || name.contains("석유") || name.contains("에너지") => "화학".to_string(),
            name if name.contains("건설") || name.contains("부동산") => "건설".to_string(),
            name if name.contains("통신") || name.contains("텔레콤") => "통신".to_string(),
            name if name.contains("항공") || name.contains("운송") => "운송".to_string(),
            _ => "기타".to_string(),
        }
    }
}

/// 한국 주식 추천 요청 메시지
#[derive(Debug, Clone)]
pub enum KoreanStockRecommendationMessage {
    GetKoreanLowRiskStocks,
    GetKoreanRecommendationsBySector(String),
    GetKoreanTopRecommendations(usize),
    GetKoreanMarketAnalysis,
    UpdateKoreanStockData,
}

/// 한국 주식 추천 응답 메시지
#[derive(Debug, Clone)]
pub enum KoreanStockRecommendationResponse {
    KoreanLowRiskStocks(Vec<KoreanStock>),
    KoreanSectorRecommendations(String, Vec<KoreanStock>),
    KoreanTopRecommendations(Vec<KoreanStock>),
    KoreanMarketAnalysis(MarketAnalysis),
    Success(String),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct MarketAnalysis {
    pub kospi_index: Option<f64>,
    pub kosdaq_index: Option<f64>,
    pub market_trend: String,
    pub analysis_time: DateTime<Utc>,
}

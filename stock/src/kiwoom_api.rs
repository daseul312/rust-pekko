use reqwest::Client;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use base64::Engine;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tracing::{ warn, error};

/// 한국투자증권 API 클라이언트
#[derive(Debug, Clone)]
pub struct KiwoomApiClient {
    client: Client,
    app_key: String,
    app_secret: String,
    access_token: Option<String>,
    base_url: String,
}

/// 한국투자증권 API 응답 구조체들
#[derive(Debug, Deserialize)]
pub struct KiwoomTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
}

#[derive(Debug, Deserialize)]
pub struct KiwoomStockInfo {
    #[serde(rename = "mksc_shrn_iscd")]
    pub market_code: String,
    #[serde(rename = "hts_kor_isnm")]
    pub korean_name: String,
    #[serde(rename = "marg_rate")]
    pub margin_rate: Option<f64>,
    #[serde(rename = "stck_prpr")]
    pub current_price: f64,
    #[serde(rename = "prdy_vrss")]
    pub previous_close: f64,
    #[serde(rename = "prdy_vrss_sign")]
    pub price_change_sign: String,
    #[serde(rename = "prdy_ctrt")]
    pub price_change_rate: f64,
    #[serde(rename = "acml_vol")]
    pub accumulated_volume: i64,
    #[serde(rename = "acml_tr_pbmn")]
    pub accumulated_amount: i64,
    #[serde(rename = "stck_hgpr")]
    pub high_price: f64,
    #[serde(rename = "stck_lwpr")]
    pub low_price: f64,
    #[serde(rename = "stck_oprc")]
    pub open_price: f64,
    #[serde(rename = "stck_sdpr")]
    pub standard_price: f64,
    #[serde(rename = "w52_hgpr")]
    pub week52_high: f64,
    #[serde(rename = "w52_lwpr")]
    pub week52_low: f64,
    #[serde(rename = "hts_avls")]
    pub market_cap: i64,
    #[serde(rename = "per")]
    pub per: Option<f64>,
    #[serde(rename = "pbr")]
    pub pbr: Option<f64>,
    #[serde(rename = "dy")]
    pub dividend_yield: Option<f64>,
    #[serde(rename = "beta")]
    pub beta: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct KiwoomStockListResponse {
    pub output: Vec<KiwoomStockInfo>,
    #[serde(rename = "rt_cd")]
    pub return_code: String,
    #[serde(rename = "msg_cd")]
    pub message_code: String,
    #[serde(rename = "msg1")]
    pub message: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KiwoomMarketIndex {
    #[serde(rename = "bstp_nmix_prpr")]
    pub current_price: f64,
    #[serde(rename = "bstp_nmix_prdy_vrss")]
    pub previous_close: f64,
    #[serde(rename = "bstp_nmix_prdy_vrss_sign")]
    pub price_change_sign: String,
    #[serde(rename = "bstp_nmix_prdy_ctrt")]
    pub price_change_rate: f64,
}

#[derive(Debug, Deserialize)]
pub struct KiwoomMarketIndexResponse {
    pub output: Vec<KiwoomMarketIndex>,
    #[serde(rename = "rt_cd")]
    pub return_code: String,
}

impl KiwoomApiClient {
    pub fn new(app_key: String, app_secret: String) -> Self {
        Self {
            client: Client::new(),
            app_key,
            app_secret,
            access_token: None,
            base_url: "https://openapi.koreainvestment.com:9443".to_string(),
        }
    }

    /// 액세스 토큰 발급
    pub async fn get_access_token(&mut self) -> Result<()> {
        let url = format!("{}/oauth2/tokenP", self.base_url);
        
        let mut params = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("appkey", &self.app_key);
        params.insert("appsecret", &self.app_secret);

        let response = self.client
            .post(&url)
            .form(&params)
            .send()
            .await?;

        if response.status().is_success() {
            let token_response: KiwoomTokenResponse = response.json().await?;
            self.access_token = Some(token_response.access_token);
            Ok(())
        } else {
            let error_text = response.text().await?;
            error!("토큰 발급 실패: {}", error_text);
            Err(anyhow!("토큰 발급 실패"))
        }
    }

    /// 주식 현재가 조회
    pub async fn get_stock_current_price(&self, stock_code: &str) -> Result<KiwoomStockInfo> {
        let url = format!("{}/uapi/domestic-stock/v1/quotations/inquire-price", self.base_url);
        
        let headers = self.create_headers()?;
        
        let mut params = HashMap::new();
        params.insert("fid_cond_mrkt_div_code", "J");
        params.insert("fid_input_iscd", stock_code);

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if response.status().is_success() {
            let stock_response: KiwoomStockListResponse = response.json().await?;
            if stock_response.return_code == "0" {
                stock_response.output.into_iter().next()
                    .ok_or_else(|| anyhow!("주식 정보가 없습니다"))
            } else {
                Err(anyhow!("API 오류: {}", stock_response.message))
            }
        } else {
            let error_text = response.text().await?;
            error!("주식 조회 실패: {}", error_text);
            Err(anyhow!("주식 조회 실패"))
        }
    }

    /// 시장 지수 조회 (KOSPI, KOSDAQ)
    pub async fn get_market_index(&self, market_code: &str) -> Result<KiwoomMarketIndex> {
        let url = format!("{}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", self.base_url);
        
        let headers = self.create_headers()?;
        
        let mut params = HashMap::new();
        params.insert("fid_cond_mrkt_div_code", market_code);
        params.insert("fid_input_iscd", "0000");
        params.insert("fid_input_date_1", "");
        params.insert("fid_input_date_2", "");
        params.insert("fid_period_div_code", "D");
        params.insert("fid_org_adj_prc", "0");

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if response.status().is_success() {
            let index_response: KiwoomMarketIndexResponse = response.json().await?;
            if index_response.return_code == "0" {
                index_response.output.into_iter().next()
                    .ok_or_else(|| anyhow!("시장 지수 정보가 없습니다"))
            } else {
                Err(anyhow!("시장 지수 조회 실패"))
            }
        } else {
            let error_text = response.text().await?;
            error!("시장 지수 조회 실패: {}", error_text);
            Err(anyhow!("시장 지수 조회 실패"))
        }
    }

    /// 저위험 주식 목록 조회 (대형주 중심)
    pub async fn get_low_risk_stocks(&self) -> Result<Vec<KiwoomStockInfo>> {
        let mut low_risk_stocks = Vec::new();
        
        // 대형주 종목 코드들 (시가총액 상위)
        let large_cap_stocks = vec![
            "005930", // 삼성전자
            "000660", // SK하이닉스
            "035420", // NAVER
            "207940", // 삼성바이오로직스
            "006400", // 삼성SDI
            "051910", // LG화학
            "068270", // 셀트리온
            "035720", // 카카오
            "323410", // 카카오뱅크
            "066570", // LG전자
            "003550", // LG
            "000270", // 기아
            "012330", // 현대모비스
            "105560", // KB금융
            "055550", // 신한지주
            "086790", // 하나금융지주
            "032830", // 삼성생명
            "000810", // 삼성화재
            "003490", // 대한항공
            "017670", // SK텔레콤
        ];

        for stock_code in large_cap_stocks {
            match self.get_stock_current_price(stock_code).await {
                Ok(stock_info) => {
                    // 저위험 주식 필터링 (베타 < 1.0, PER < 30, 배당수익률 > 1%)
                    if stock_info.beta.unwrap_or(1.0) < 1.0 
                        && stock_info.per.unwrap_or(0.0) < 30.0 
                        && stock_info.per.unwrap_or(0.0) > 0.0
                        && stock_info.dividend_yield.unwrap_or(0.0) > 1.0 {
                        low_risk_stocks.push(stock_info);
                    }
                }
                Err(e) => {
                    warn!("주식 {} 조회 실패: {}", stock_code, e);
                }
            }
            
            // API 호출 제한을 위한 대기
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        Ok(low_risk_stocks)
    }

    /// 헤더 생성
    fn create_headers(&self) -> Result<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();
        
        if let Some(token) = &self.access_token {
            headers.insert("Authorization", format!("Bearer {}", token).parse()?);
        }
        headers.insert("appkey", self.app_key.parse()?);
        headers.insert("appsecret", self.app_secret.parse()?);
        headers.insert("tr_id", "FHKST01010100".parse()?);
        headers.insert("Content-Type", "application/json; charset=utf-8".parse()?);
        
        Ok(headers)
    }

    /// 토큰 유효성 검사
    pub fn is_token_valid(&self) -> bool {
        self.access_token.is_some()
    }
}

/// 한국 주식 정보를 국제 표준으로 변환
pub fn convert_korean_stock_to_international(korean_stock: KiwoomStockInfo) -> crate::models::Stock {
    use crate::models::Stock;
    
    // 한국 주식 시장 코드를 섹터로 매핑
    let sector = match korean_stock.market_code.as_str() {
        "KOSPI" => "Large Cap",
        "KOSDAQ" => "Mid Cap",
        _ => "Unknown",
    };

    // 베타 값이 없으면 시가총액 기반으로 추정
    let estimated_beta = korean_stock.beta.unwrap_or_else(|| {
        if korean_stock.market_cap > 10_000_000_000_000 { // 10조원 이상
            0.6
        } else if korean_stock.market_cap > 1_000_000_000_000 { // 1조원 이상
            0.8
        } else {
            1.0
        }
    });

    Stock::new(
        korean_stock.korean_name.clone(),
        korean_stock.korean_name,
        sector.to_string(),
        korean_stock.market_cap as f64,
        korean_stock.current_price,
        korean_stock.per,
        korean_stock.dividend_yield,
        Some(estimated_beta),
        0.2, // 기본 변동성
    )
}

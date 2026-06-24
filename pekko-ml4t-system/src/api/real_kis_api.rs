use super::*;
use crate::config::api_config::{ApiConfig, KisApiConfig};
use anyhow::{anyhow, Result};
use base64::{Engine as _, engine::general_purpose};
use chrono::{Utc, Duration};
use reqwest::{Client, header::{HeaderMap, HeaderValue, HeaderName}};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::time::sleep;
use tracing::{ warn, error, debug};

/// 실제 KIS API 클라이언트
#[derive(Debug, Clone)]
pub struct RealKisApi {
    config: KisApiConfig,
    client: Client,
    access_token: Option<String>,
    token_expires_at: Option<chrono::DateTime<Utc>>,
}

/// KIS API 인증 토큰 응답
#[derive(Debug, Deserialize)]
struct KisTokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

/// KIS API 시세 조회 응답
#[derive(Debug, Deserialize)]
struct KisQuoteResponse {
    #[serde(rename = "output")]
    output: KisQuoteOutput,
}

#[derive(Debug, Deserialize)]
struct KisQuoteOutput {
    #[serde(rename = "stck_prpr")]
    current_price: String,
    #[serde(rename = "stck_oprc")]
    open_price: String,
    #[serde(rename = "stck_hgpr")]
    high_price: String,
    #[serde(rename = "stck_lwpr")]
    low_price: String,
    #[serde(rename = "acml_vol")]
    volume: String,
    #[serde(rename = "prdy_vrss")]
    price_change: String,
    #[serde(rename = "prdy_vrss_sign")]
    price_change_sign: String,
}

/// KIS API 계좌 잔고 조회 응답
#[derive(Debug, Deserialize)]
struct KisBalanceResponse {
    #[serde(rename = "output1")]
    output1: Vec<KisBalanceOutput>,
}

#[derive(Debug, Deserialize)]
struct KisBalanceOutput {
    #[serde(rename = "pdno")]
    product_code: String,
    #[serde(rename = "prdt_name")]
    product_name: String,
    #[serde(rename = "hldg_qty")]
    holding_quantity: String,
    #[serde(rename = "ord_psbl_qty")]
    orderable_quantity: String,
    #[serde(rename = "pchs_avg_pric")]
    purchase_average_price: String,
    #[serde(rename = "prpr")]
    current_price: String,
    #[serde(rename = "evlu_amt")]
    evaluation_amount: String,
    #[serde(rename = "evlu_pfls_amt")]
    evaluation_profit_loss_amount: String,
    #[serde(rename = "evlu_pfls_rt")]
    evaluation_profit_loss_rate: String,
    #[serde(rename = "evlu_erng_rt")]
    evaluation_earning_rate: String,
}

impl RealKisApi {
    /// 새로운 KIS API 클라이언트 생성
    pub fn new(config: KisApiConfig) -> Self {
        let client = Client::new();
        Self {
            config,
            client,
            access_token: None,
            token_expires_at: None,
        }
    }

    /// 액세스 토큰을 가져오거나 갱신합니다
    async fn get_access_token(&mut self) -> Result<String> {
        // 토큰이 유효한지 확인
        if let (Some(token), Some(expires_at)) = (&self.access_token, &self.token_expires_at) {
            if Utc::now() < *expires_at {
                return Ok(token.clone());
            }
        }

        // 새로운 토큰 요청
        let auth_string = format!("{}:{}", self.config.app_key, self.config.app_secret);
        let encoded_auth = general_purpose::STANDARD.encode(auth_string);

        let url = format!("{}/oauth2/tokenP", self.config.base_url);
        let mut params = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("appkey", &self.config.app_key);
        params.insert("appsecret", &self.config.app_secret);

        let response = self.client
            .post(&url)
            .header("Authorization", format!("Basic {}", encoded_auth))
            .header("Content-Type", "application/json")
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("KIS API 토큰 요청 실패: {}", error_text));
        }

        let token_response: KisTokenResponse = response.json().await?;
        
        // 토큰 저장
        self.access_token = Some(token_response.access_token.clone());
        self.token_expires_at = Some(Utc::now() + Duration::seconds(token_response.expires_in as i64));
        Ok(token_response.access_token)
    }

    /// API 요청을 위한 공통 헤더 생성
    async fn get_auth_headers(&mut self) -> Result<HashMap<String, String>> {
        let token = self.get_access_token().await?;
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        headers.insert("appkey".to_string(), self.config.app_key.clone());
        headers.insert("appsecret".to_string(), self.config.app_secret.clone());
        headers.insert("tr_id".to_string(), "FHKST01010100".to_string()); // 주식현재가조회
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        Ok(headers)
    }
}

#[async_trait::async_trait]
impl ApiClient for RealKisApi {
    type Config = KisApiConfig;
    type Response = MarketDataResponse;

    async fn new(config: Self::Config) -> Result<Self> {
        Ok(Self::new(config))
    }

    async fn authenticate(&mut self) -> Result<()> {
        self.get_access_token().await?;
        Ok(())
    }

    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse> {
        // KIS API는 한국 주식만 지원하므로 심볼 변환 필요
        let kis_symbol = match symbol {
            "AAPL" => "005930", // 삼성전자 (예시)
            "GOOGL" => "000660", // SK하이닉스 (예시)
            "MSFT" => "035420", // NAVER (예시)
            _ => symbol, // 이미 한국 종목코드인 경우
        };

        let mut client = self.clone();
        let headers = client.get_auth_headers().await?;
        
        let url = format!("{}/uapi/domestic-stock/v1/quotations/inquire-price", self.config.base_url);
        let mut params = HashMap::new();
        params.insert("fid_cond_mrkt_div_code", "J");
        params.insert("fid_input_iscd", kis_symbol);

        let response = self.client
            .get(&url)
            .headers(
                headers.into_iter().map(|(k, v)| {
                    (HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(&v).unwrap())
                }).collect()
            )
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("KIS API 시세 조회 실패: {}", error_text));
        }

        let quote_response: KisQuoteResponse = response.json().await?;
        let output = quote_response.output;

        // 가격 문자열을 f64로 변환
        let current_price = output.current_price.parse::<f64>()?;
        let open_price = output.open_price.parse::<f64>()?;
        let high_price = output.high_price.parse::<f64>()?;
        let low_price = output.low_price.parse::<f64>()?;
        let volume = output.volume.parse::<u64>()?;

        Ok(MarketDataResponse {
            symbol: symbol.to_string(),
            price: current_price,
            volume,
            timestamp: Utc::now(),
            open: Some(open_price),
            high: Some(high_price),
            low: Some(low_price),
            close: Some(current_price),
        })
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let mut client = self.clone();
        let headers = client.get_auth_headers().await?;
        
        let url = format!("{}/uapi/domestic-stock/v1/trading/inquire-balance", self.config.base_url);
        let mut params = HashMap::new();
        params.insert("CANO", self.config.account_number.clone());
        params.insert("ACNT_PRDT_CD", "01".to_string());
        params.insert("AFHR_FLPR_YN", "N".to_string());
        params.insert("OFL_YN", "".to_string());
        params.insert("INQR_DVSN", "02".to_string());
        params.insert("UNPR_DVSN", "01".to_string());
        params.insert("FUND_STTL_ICLD_YN", "N".to_string());
        params.insert("FNCG_AMT_AUTO_RDPT_YN", "N".to_string());
        params.insert("PRCS_DVSN", "01".to_string());
        params.insert("CTX_AREA_FK100", "".to_string());
        params.insert("CTX_AREA_NK100", "".to_string());

        let response = self.client
            .get(&url)
            .headers(
                headers.into_iter().map(|(k, v)| {
                    (HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(&v).unwrap())
                }).collect()
            )
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("KIS API 계좌 조회 실패: {}", error_text));
        }

        let balance_response: KisBalanceResponse = response.json().await?;
        
        // 총 평가금액 계산
        let total_evaluation: f64 = balance_response.output1
            .iter()
            .filter_map(|item| item.evaluation_amount.parse::<f64>().ok())
            .sum();

        // 현금 잔고는 별도 조회 필요 (간단히 시뮬레이션)
        let cash_balance = 1_000_000.0; // 예시 금액

        Ok(AccountInfo {
            account_id: self.config.account_number.clone(),
            total_balance: total_evaluation + cash_balance,
            cash_balance,
            positions: balance_response.output1
                .into_iter()
                .map(|item| Position {
                    symbol: item.product_code,
                    quantity: item.holding_quantity.parse().unwrap_or(0.0),
                    average_price: item.purchase_average_price.parse().unwrap_or(0.0),
                    current_price: item.current_price.parse().unwrap_or(0.0),
                    market_value: item.evaluation_amount.parse().unwrap_or(0.0),
                    unrealized_pnl: item.evaluation_profit_loss_amount.parse().unwrap_or(0.0),
                })
                .collect(),
        })
    }

    async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        let mut client = self.clone();
        let headers = client.get_auth_headers().await?;
        
        // 주문 파라미터 설정
        let side_code = match order.side {
            OrderSide::Buy => "2", // 매수
            OrderSide::Sell => "1", // 매도
        };

        let url = format!("{}/uapi/domestic-stock/v1/trading/order-cash", self.config.base_url);
        let mut params = HashMap::new();
        params.insert("CANO".to_string(), self.config.account_number.clone());
        params.insert("ACNT_PRDT_CD".to_string(), "01".to_string());
        params.insert("PDNO".to_string(), order.symbol.clone());
        params.insert("ORD_DVSN".to_string(), "01".to_string()); // 시장가
        params.insert("ORD_QTY".to_string(), order.quantity.to_string());
        params.insert("ORD_UNPR".to_string(), "0".to_string()); // 시장가는 0
        params.insert("CTAC_TLNO".to_string(), "".to_string());
        params.insert("SLL_TYPE".to_string(), "01".to_string());
        params.insert("ALGO_NO".to_string(), "".to_string());

        let response = self.client
            .post(&url)
            .headers(
                headers.into_iter().map(|(k, v)| {
                    (HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(&v).unwrap())
                }).collect()
            )
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("KIS API 주문 실패: {}", error_text));
        }

        // 주문 응답 처리 (실제 응답 구조에 맞게 수정 필요)
        let response_text = response.text().await?;
        debug!("KIS API 주문 응답: {}", response_text);

        Ok(OrderResponse {
            order_id: format!("KIS_{}", Utc::now().timestamp()),
            status: OrderStatus::Submitted,
            filled_quantity: 0.0,
            filled_price: 0.0,
            commission: order.quantity * 0.001, // 0.1% 수수료 가정
            timestamp: Utc::now(),
        })
    }

    async fn get_positions(&self) -> Result<Vec<Position>> {
        let account_info = self.get_account_info().await?;
        Ok(account_info.positions)
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus> {
        // KIS API 주문 상태 조회 (구현 필요)
        warn!("KIS API 주문 상태 조회 미구현: {}", order_id);
        Ok(OrderStatus::Completed)
    }
}

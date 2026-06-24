use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;
use chrono::{DateTime, Utc};

/// 한국투자증권 API 클라이언트
#[derive(Debug, Clone)]
pub struct KoreaInvestmentAPI {
    client: Client,
    base_url: String,
    app_key: String,
    app_secret: String,
    account_no: String,
    access_token: Option<String>,
}

/// OAuth 토큰 응답
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: i64,
}

/// 토큰 캐시
#[derive(Debug, Serialize, Deserialize)]
struct TokenCache {
    access_token: String,
    expires_at: String,  // ISO 8601 형식
}

/// API 응답 공통 구조
#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    rt_cd: String,      // 응답 코드
    msg_cd: String,     // 메시지 코드
    msg1: String,       // 응답 메시지
    output: Option<T>,  // 응답 데이터
}

/// 계좌 잔고 조회 응답
#[derive(Debug, Deserialize)]
pub struct BalanceOutput {
    #[serde(rename = "dnca_tot_amt")]
    pub total_cash: String,  // 예수금 총액
    #[serde(rename = "nass_amt")]
    pub net_asset: String,   // 순자산액
    #[serde(rename = "tot_evlu_amt")]
    pub total_eval: String,  // 총평가금액
}

/// 주식 시세 조회 응답
#[derive(Debug, Deserialize, Clone)]
pub struct StockPrice {
    #[serde(rename = "stck_prpr")]
    pub current_price: String,  // 현재가
    #[serde(rename = "prdy_vrss")]
    pub price_change: String,   // 전일 대비
    #[serde(rename = "prdy_ctrt")]
    pub change_rate: String,    // 전일 대비율
    #[serde(rename = "acml_vol")]
    pub volume: String,         // 누적 거래량
    #[serde(rename = "stck_oprc")]
    pub open_price: String,     // 시가
    #[serde(rename = "stck_hgpr")]
    pub high_price: String,     // 고가
    #[serde(rename = "stck_lwpr")]
    pub low_price: String,      // 저가
}

impl StockPrice {
    /// 등락률이 양수인지 확인 (상승 중)
    pub fn is_rising(&self) -> bool {
        self.change_rate.parse::<f64>().unwrap_or(0.0) > 0.0
    }
    
    /// 변동성 계산 (고가-저가 비율)
    pub fn volatility(&self) -> f64 {
        let high = self.high_price.parse::<f64>().unwrap_or(0.0);
        let low = self.low_price.parse::<f64>().unwrap_or(0.0);
        let current = self.current_price.parse::<f64>().unwrap_or(1.0);
        
        ((high - low) / current) * 100.0
    }
    
    /// 현재가가 시가보다 높은지 (장중 상승)
    pub fn above_open(&self) -> bool {
        let current = self.current_price.parse::<f64>().unwrap_or(0.0);
        let open = self.open_price.parse::<f64>().unwrap_or(0.0);
        current > open
    }
}

/// 주문 응답
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "KRX_FWDG_ORD_ORGNO")]
    pub order_org_no: Option<String>,  // 주문 조직 번호
    #[serde(rename = "ODNO")]
    pub order_no: Option<String>,      // 주문 번호
    #[serde(rename = "ORD_TMD")]
    pub order_time: Option<String>,    // 주문 시각
}

impl KoreaInvestmentAPI {
    /// 환경 변수에서 API 정보 로드
    pub fn from_env() -> Result<Self> {
        let app_key = env::var("KIS_APP_KEY")
            .map_err(|_| anyhow!("KIS_APP_KEY 환경 변수가 설정되지 않았습니다"))?;
        
        let app_secret = env::var("KIS_APP_SECRET")
            .map_err(|_| anyhow!("KIS_APP_SECRET 환경 변수가 설정되지 않았습니다"))?;
        
        let account_no = env::var("KIS_ACCOUNT_NO")
            .map_err(|_| anyhow!("KIS_ACCOUNT_NO 환경 변수가 설정되지 않았습니다"))?;
        
        let base_url = env::var("KIS_BASE_URL")
            .unwrap_or_else(|_| "https://openapi.koreainvestment.com:9443".to_string());

        Ok(Self {
            client: Client::builder()
                .danger_accept_invalid_certs(false)
                .build()?,
            base_url,
            app_key,
            app_secret,
            account_no,
            access_token: None,
        })
    }

    /// 토큰 캐시 파일 경로
    fn token_cache_path() -> &'static str {
        ".token_cache.json"
    }
    
    /// 캐시된 토큰 로드
    fn load_cached_token() -> Option<TokenCache> {
        let path = Self::token_cache_path();
        if !Path::new(path).exists() {
            return None;
        }
        
        match fs::read_to_string(path) {
            Ok(content) => {
                match serde_json::from_str::<TokenCache>(&content) {
                    Ok(cache) => {
                        // 만료 시간 확인
                        if let Ok(expires_at) = DateTime::parse_from_rfc3339(&cache.expires_at) {
                            let now = Utc::now();
                            if expires_at.signed_duration_since(now).num_minutes() > 5 {
                                return Some(cache);
                            } else {
                            }
                        }
                    }
                    Err(_) => tracing::warn!("토큰 캐시 파일 파싱 실패"),
                }
            }
            Err(_) => tracing::warn!("토큰 캐시 파일 읽기 실패"),
        }
        
        None
    }
    
    /// 토큰 캐시 저장
    fn save_token_cache(token: &str, expires_in: i64) -> Result<()> {
        let expires_at = Utc::now() + chrono::Duration::seconds(expires_in);
        let cache = TokenCache {
            access_token: token.to_string(),
            expires_at: expires_at.to_rfc3339(),
        };
        
        let json = serde_json::to_string_pretty(&cache)?;
        fs::write(Self::token_cache_path(), json)?;
        Ok(())
    }
    
    /// OAuth 토큰 발급 (캐시 지원)
    pub async fn get_access_token(&mut self) -> Result<String> {
        // 1. 캐시된 토큰 확인
        if let Some(cache) = Self::load_cached_token() {
            self.access_token = Some(cache.access_token.clone());
            return Ok(cache.access_token);
        }
        
        // 2. 새로운 토큰 발급
        let url = format!("{}/oauth2/tokenP", self.base_url);
        
        let body = serde_json::json!({
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        });
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            tracing::error!("❌ 토큰 발급 실패!");
            tracing::error!("💡 1분당 1회 제한이 있습니다. 잠시 후 다시 시도하세요.");
            return Err(anyhow!("토큰 발급 실패 ({}): {}", status, error_text));
        }

        let token_response: TokenResponse = response.json().await?;
        self.access_token = Some(token_response.access_token.clone());
        
        // 3. 토큰 캐시 저장
        Self::save_token_cache(&token_response.access_token, token_response.expires_in).ok();
        
        Ok(token_response.access_token)
    }

    /// 주문 가능 금액 조회 (매수 가능 현금)
    pub async fn get_buyable_cash(&self, stock_code: &str) -> Result<f64> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            self.base_url
        );

        let response = self.client
            .get(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC8908R")  // 매수가능조회
            .query(&[
                ("CANO", account_num),
                ("ACNT_PRDT_CD", product_code),
                ("PDNO", stock_code),
                ("ORD_UNPR", "0"),  // 시장가 기준
                ("ORD_DVSN", "01"),  // 시장가
                ("CMA_EVLU_AMT_ICLD_YN", "N"),
                ("OVRS_ICLD_YN", "N"),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("매수가능금액 조회 실패 ({}): {}", status, error_text));
        }

        let response_json: serde_json::Value = response.json().await?;
        
        // 매수 가능 금액 추출
        let buyable_cash = if let Some(output) = response_json.get("output") {
            output["ord_psbl_cash"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(buyable_cash)
    }

    /// 계좌 상세 정보 조회
    pub async fn get_account_info(&self) -> Result<(f64, Vec<(String, u32, f64)>)> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다. 형식: 12345678-01"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/inquire-balance",
            self.base_url
        );

        let response = self.client
            .get(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC8434R")
            .query(&[
                ("CANO", account_num),
                ("ACNT_PRDT_CD", product_code),
                ("AFHR_FLPR_YN", "N"),
                ("OFL_YN", ""),
                ("INQR_DVSN", "02"),
                ("UNPR_DVSN", "01"),
                ("FUND_STTL_ICLD_YN", "N"),
                ("FNCG_AMT_AUTO_RDPT_YN", "N"),
                ("PRCS_DVSN", "00"),
                ("CTX_AREA_FK100", ""),
                ("CTX_AREA_NK100", ""),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("계좌 조회 실패 ({}): {}", status, error_text));
        }

        let response_json: serde_json::Value = response.json().await?;
        
        // 디버깅: 응답 전체 확인
        tracing::debug!("계좌 조회 응답: {}", serde_json::to_string_pretty(&response_json).unwrap_or_default());
        
        // 예수금 조회
        let cash = if let Some(output2) = response_json.get("output2").and_then(|v| v.as_array()) {
            if let Some(first) = output2.first() {
                first["dnca_tot_amt"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0)
            } else {
                0.0
            }
        } else {
            0.0
        };

        // 보유 종목 조회
        let mut holdings = Vec::new();
        if let Some(output1) = response_json.get("output1").and_then(|v| v.as_array()) {
            for (idx, item) in output1.iter().enumerate() {
                let stock_code = item["pdno"].as_str().unwrap_or("").to_string();
                let stock_name = item["prdt_name"].as_str().unwrap_or("").to_string();
                let quantity = item["hldg_qty"].as_str().unwrap_or("0").parse::<u32>().unwrap_or(0);
                let avg_price = item["pchs_avg_pric"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let current_price = item["prpr"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                if quantity > 0 {
                    holdings.push((stock_code, quantity, avg_price));
                }
            }
        } else {
            tracing::warn!("⚠️  output1이 없거나 배열이 아닙니다");
        }
        // 보유 종목 요약
        for (code, qty, avg) in &holdings {
        }

        Ok((cash, holdings))
    }

    /// 계좌 잔고 조회 (레거시)
    pub async fn get_balance(&self) -> Result<()> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        // 계좌번호 파싱 (12345678-01 -> 계좌번호: 12345678, 상품코드: 01)
        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다. 형식: 12345678-01"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/inquire-balance",
            self.base_url
        );
        let response = self.client
            .get(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC8434R")  // 실전투자: TTTC8434R, 모의투자: VTTC8434R
            .query(&[
                ("CANO", account_num),
                ("ACNT_PRDT_CD", product_code),
                ("AFHR_FLPR_YN", "N"),
                ("OFL_YN", ""),
                ("INQR_DVSN", "02"),
                ("UNPR_DVSN", "01"),
                ("FUND_STTL_ICLD_YN", "N"),
                ("FNCG_AMT_AUTO_RDPT_YN", "N"),
                ("PRCS_DVSN", "00"),
                ("CTX_AREA_FK100", ""),
                ("CTX_AREA_NK100", ""),
            ])
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;
        
        if !status.is_success() {
            return Err(anyhow!("잔고 조회 실패 ({}): {}", status, response_text));
        }
        Ok(())
    }

    /// 주식 현재가 조회
    pub async fn get_stock_price(&self, stock_code: &str) -> Result<StockPrice> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let url = format!(
            "{}/uapi/domestic-stock/v1/quotations/inquire-price",
            self.base_url
        );
        let response = self.client
            .get(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "FHKST01010100")  // 실전투자: FHKST01010100
            .query(&[
                ("FID_COND_MRKT_DIV_CODE", "J"),
                ("FID_INPUT_ISCD", stock_code),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("시세 조회 실패 ({}): {}", status, error_text));
        }

        let response_json: serde_json::Value = response.json().await?;
        
        let output = response_json["output"]
            .as_object()
            .ok_or_else(|| anyhow!("응답 데이터 파싱 실패"))?;

        let stock_price: StockPrice = serde_json::from_value(serde_json::Value::Object(output.clone()))?;
        Ok(stock_price)
    }

    /// 주식 매수 주문
    pub async fn buy_stock(
        &self,
        stock_code: &str,
        quantity: u32,
        price: u32,
    ) -> Result<OrderResponse> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/order-cash",
            self.base_url
        );

        let body = serde_json::json!({
            "CANO": account_num,
            "ACNT_PRDT_CD": product_code,
            "PDNO": stock_code,
            "ORD_DVSN": "00",  // 00: 지정가
            "ORD_QTY": quantity.to_string(),
            "ORD_UNPR": price.to_string(),
        });
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC0802U")  // 실전투자: TTTC0802U, 모의투자: VTTC0802U
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if !status.is_success() {
            return Err(anyhow!("매수 주문 실패 ({}): {}", status, response_text));
        }

        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
        
        // 응답 코드 확인
        let rt_cd = response_json.get("rt_cd").and_then(|v| v.as_str()).unwrap_or("");
        let msg1 = response_json.get("msg1").and_then(|v| v.as_str()).unwrap_or("");
        
        // 오류 처리
        if rt_cd != "0" {
            return Err(anyhow!("매수 주문 거부: {} (코드: {})", msg1, rt_cd));
        }
        
        if let Some(output) = response_json.get("output") {
            let order_response: OrderResponse = serde_json::from_value(output.clone())?;
            Ok(order_response)
        } else {
            Err(anyhow!("주문 응답 파싱 실패: {}", response_text))
        }
    }

    /// 주식 매도 주문
    pub async fn sell_stock(
        &self,
        stock_code: &str,
        quantity: u32,
        price: u32,
    ) -> Result<OrderResponse> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/order-cash",
            self.base_url
        );

        let body = serde_json::json!({
            "CANO": account_num,
            "ACNT_PRDT_CD": product_code,
            "PDNO": stock_code,
            "ORD_DVSN": "00",  // 00: 지정가
            "ORD_QTY": quantity.to_string(),
            "ORD_UNPR": price.to_string(),
        });
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC0801U")  // 실전투자: TTTC0801U, 모의투자: VTTC0801U
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if !status.is_success() {
            return Err(anyhow!("매도 주문 실패 ({}): {}", status, response_text));
        }

        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
        
        // 응답 코드 확인
        let rt_cd = response_json.get("rt_cd").and_then(|v| v.as_str()).unwrap_or("");
        let msg1 = response_json.get("msg1").and_then(|v| v.as_str()).unwrap_or("");
        
        // 오류 처리
        if rt_cd != "0" {
            return Err(anyhow!("매도 주문 거부: {} (코드: {})", msg1, rt_cd));
        }
        
        if let Some(output) = response_json.get("output") {
            let order_response: OrderResponse = serde_json::from_value(output.clone())?;
            Ok(order_response)
        } else {
            Err(anyhow!("주문 응답 파싱 실패: {}", response_text))
        }
    }

    /// 주식 시장가 매도 주문
    pub async fn sell_stock_market(
        &self,
        stock_code: &str,
        quantity: u32,
    ) -> Result<OrderResponse> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/domestic-stock/v1/trading/order-cash",
            self.base_url
        );

        let body = serde_json::json!({
            "CANO": account_num,
            "ACNT_PRDT_CD": product_code,
            "PDNO": stock_code,
            "ORD_DVSN": "01",  // 01: 시장가
            "ORD_QTY": quantity.to_string(),
            "ORD_UNPR": "0",  // 시장가는 가격 0
        });
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTC0801U")  // 실전투자: TTTC0801U
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if !status.is_success() {
            return Err(anyhow!("시장가 매도 주문 실패 ({}): {}", status, response_text));
        }

        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
        
        let rt_cd = response_json.get("rt_cd").and_then(|v| v.as_str()).unwrap_or("");
        let msg1 = response_json.get("msg1").and_then(|v| v.as_str()).unwrap_or("");
        
        if rt_cd != "0" {
            return Err(anyhow!("시장가 매도 주문 거부: {} (코드: {})", msg1, rt_cd));
        }
        
        if let Some(output) = response_json.get("output") {
            let order_response: OrderResponse = serde_json::from_value(output.clone())?;
            Ok(order_response)
        } else {
            Err(anyhow!("주문 응답 파싱 실패: {}", response_text))
        }
    }

    /// 미국 주식 현재가 조회
    pub async fn get_us_stock_price(&self, symbol: &str) -> Result<f64> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let url = format!(
            "{}/uapi/overseas-price/v1/quotations/price",
            self.base_url
        );
        let response = self.client
            .get(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "HHDFS00000300")  // 미국 주식 현재가
            .query(&[
                ("AUTH", ""),
                ("EXCD", "NAS"),  // NASDAQ (또는 NYS: NYSE)
                ("SYMB", symbol),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("미국 주식 시세 조회 실패 ({}): {}", status, error_text));
        }

        let response_json: serde_json::Value = response.json().await?;
        
        let price = if let Some(output) = response_json.get("output") {
            output["last"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(price)
    }

    /// 미국 주식 매수
    pub async fn buy_us_stock(
        &self,
        symbol: &str,
        quantity: u32,
        price_usd: f64,  // 지정가 (필수)
    ) -> Result<OrderResponse> {
        let token = self.access_token.as_ref()
            .ok_or_else(|| anyhow!("먼저 get_access_token()을 호출하여 토큰을 발급받으세요"))?;

        let parts: Vec<&str> = self.account_no.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("계좌번호 형식이 올바르지 않습니다"));
        }
        
        let account_num = parts[0];
        let product_code = parts[1];

        let url = format!(
            "{}/uapi/overseas-stock/v1/trading/order",
            self.base_url
        );

        // 지정가를 소수점 2자리로 포맷
        let price_str = format!("{:.2}", price_usd);
        
        let body = serde_json::json!({
            "CANO": account_num,
            "ACNT_PRDT_CD": product_code,
            "OVRS_EXCG_CD": "NASD",  // NASDAQ (NYS: NYSE)
            "PDNO": symbol,
            "ORD_DVSN": "00",  // 00: 지정가
            "ORD_QTY": quantity.to_string(),
            "OVRS_ORD_UNPR": price_str,  // 지정가 (현재가)
            "ORD_SVR_DVSN_CD": "0",
        });
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json; charset=utf-8")
            .header("authorization", format!("Bearer {}", token))
            .header("appkey", &self.app_key)
            .header("appsecret", &self.app_secret)
            .header("tr_id", "TTTT1002U")  // 미국 주식 매수
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if !status.is_success() {
            return Err(anyhow!("미국 주식 매수 실패 ({}): {}", status, response_text));
        }

        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
        
        let rt_cd = response_json.get("rt_cd").and_then(|v| v.as_str()).unwrap_or("");
        let msg1 = response_json.get("msg1").and_then(|v| v.as_str()).unwrap_or("");
        
        if rt_cd != "0" {
            return Err(anyhow!("미국 주식 매수 거부: {} (코드: {})", msg1, rt_cd));
        }

        if let Some(output) = response_json.get("output") {
            let order_response: OrderResponse = serde_json::from_value(output.clone())?;
            Ok(order_response)
        } else {
            Err(anyhow!("주문 응답 파싱 실패: {}", response_text))
        }
    }

    /// API 연결 테스트
    pub async fn test_connection(&mut self) -> Result<()> {
        // 1. 토큰 발급 테스트
        match self.get_access_token().await {
            Ok(token) => {
            }
            Err(e) => {
                tracing::error!("❌ 토큰 발급 실패: {}", e);
                return Err(e);
            }
        }
        // 2. 잔고 조회 테스트
        match self.get_balance().await {
            Ok(_) => {
            }
            Err(e) => {
                tracing::error!("❌ 잔고 조회 실패: {}", e);
                tracing::warn!("⚠️  잔고 조회는 실패했지만 토큰 발급은 성공했습니다.");
                tracing::warn!("   계좌번호를 확인하거나, 모의투자 계좌인 경우 TR_ID를 VTTC8434R로 변경하세요.");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_api_connection() {
        // .env 파일 로드
        dotenv::dotenv().ok();

        // 로깅 초기화
        tracing_subscriber::fmt::init();

        let mut api = KoreaInvestmentAPI::from_env().expect("환경 변수 로드 실패");
        
        api.test_connection().await.expect("API 연결 테스트 실패");
    }
}


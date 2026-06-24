use reqwest::Client;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tracing::{ warn, error};

/// 한국투자증권 실매매 API 클라이언트
#[derive(Debug, Clone)]
pub struct KiwoomRealTradingClient {
    client: Client,
    app_key: String,
    app_secret: String,
    access_token: Option<String>,
    base_url: String,
    account_number: Option<String>,  // 계좌번호
    is_simulation: bool,             // 모의투자 여부
}

/// 실매매 주문 요청
#[derive(Debug, Serialize)]
pub struct OrderRequest {
    #[serde(rename = "CANO")]
    pub account_number: String,      // 계좌번호
    #[serde(rename = "ACNT_PRDT_CD")]
    pub account_product_code: String, // 계좌상품코드
    #[serde(rename = "PDNO")]
    pub stock_code: String,         // 종목코드
    #[serde(rename = "ORD_DVSN")]
    pub order_type: String,         // 주문구분 (01: 시장가, 00: 지정가)
    #[serde(rename = "ORD_QTY")]
    pub quantity: i32,              // 주문수량
    #[serde(rename = "ORD_UNPR")]
    pub price: i32,                 // 주문가격 (시장가는 0)
}

/// 실매매 주문 응답
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "rt_cd")]
    pub return_code: String,
    #[serde(rename = "msg_cd")]
    pub message_code: String,
    #[serde(rename = "msg1")]
    pub message: String,
    #[serde(rename = "output")]
    pub order_info: Option<OrderInfo>,
}

#[derive(Debug, Deserialize)]
pub struct OrderInfo {
    #[serde(rename = "ODNO")]
    pub order_number: String,        // 주문번호
    #[serde(rename = "ORD_TMD")]
    pub order_time: String,         // 주문시간
}

/// 계좌 잔고 조회
#[derive(Debug, Deserialize)]
pub struct AccountBalance {
    #[serde(rename = "CANO")]
    pub account_number: String,
    #[serde(rename = "ACNT_PRDT_CD")]
    pub account_product_code: String,
    #[serde(rename = "PRDT_NAME")]
    pub product_name: String,
    #[serde(rename = "EVAL_AMT")]
    pub evaluation_amount: i64,     // 평가금액
    #[serde(rename = "SLL_AMT")]
    pub sell_amount: i64,           // 매도금액
    #[serde(rename = "BUY_AMT")]
    pub buy_amount: i64,            // 매수금액
    #[serde(rename = "DNCA_TOT_AMT")]
    pub deposit_total_amount: i64,  // 예수금총금액
    #[serde(rename = "NXST_EXCC_AMT")]
    pub next_exercise_amount: i64,  // 익일정산금액
    #[serde(rename = "PRCS_STAT_CD")]
    pub process_status_code: String, // 처리상태코드
}

/// 보유 종목 조회
#[derive(Debug, Deserialize)]
pub struct HoldingStock {
    #[serde(rename = "CANO")]
    pub account_number: String,
    #[serde(rename = "ACNT_PRDT_CD")]
    pub account_product_code: String,
    #[serde(rename = "PDNO")]
    pub stock_code: String,
    #[serde(rename = "PRDT_NAME")]
    pub stock_name: String,
    #[serde(rename = "HOLD_QTY")]
    pub holding_quantity: i32,      // 보유수량
    #[serde(rename = "ORD_PSBL_QTY")]
    pub orderable_quantity: i32,    // 주문가능수량
    #[serde(rename = "PCHS_AVRG_PRIC")]
    pub purchase_average_price: f64, // 매수평균가
    #[serde(rename = "PRPR")]
    pub current_price: f64,         // 현재가
    #[serde(rename = "EVAL_AMT")]
    pub evaluation_amount: i64,     // 평가금액
    #[serde(rename = "EVAL_PFLS_AMT")]
    pub evaluation_profit_loss: i64, // 평가손익
    #[serde(rename = "EVAL_PFLS_RT")]
    pub evaluation_profit_loss_rate: f64, // 평가손익률
}

/// 실매매 결과
#[derive(Debug, Clone)]
pub struct RealTradingResult {
    pub success: bool,
    pub order_number: Option<String>,
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

impl KiwoomRealTradingClient {
    pub fn new(app_key: String, app_secret: String, account_number: Option<String>, is_simulation: bool) -> Self {
        let base_url = if is_simulation {
            "https://openapivts.koreainvestment.com:29443".to_string() // 모의투자
        } else {
            "https://openapi.koreainvestment.com:9443".to_string() // 실거래
        };

        Self {
            client: Client::new(),
            app_key,
            app_secret,
            access_token: None,
            base_url,
            account_number,
            is_simulation,
        }
    }

    /// 액세스 토큰 발급 (실매매용)
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
            let token_response: serde_json::Value = response.json().await?;
            if let Some(access_token) = token_response.get("access_token").and_then(|v| v.as_str()) {
                self.access_token = Some(access_token.to_string());
                Ok(())
            } else {
                Err(anyhow!("토큰 응답에서 access_token을 찾을 수 없음"))
            }
        } else {
            let error_text = response.text().await?;
            error!("실매매 토큰 발급 실패: {}", error_text);
            Err(anyhow!("실매매 토큰 발급 실패"))
        }
    }

    /// 계좌 잔고 조회
    pub async fn get_account_balance(&self) -> Result<AccountBalance> {
        let url = format!("{}/uapi/domestic-stock/v1/trading/inquire-balance", self.base_url);
        
        let headers = self.create_real_trading_headers("TTTC8434R")?;
        
        let mut params: HashMap<&str, String> = HashMap::new();
        params.insert("CANO", self.account_number.as_ref().unwrap_or(&"".to_string()).clone());
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
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if response.status().is_success() {
            let balance_response: serde_json::Value = response.json().await?;
            // 실제 응답 구조에 맞게 파싱 필요
            // 여기서는 간단한 예시
            Ok(AccountBalance {
                account_number: self.account_number.clone().unwrap_or_default(),
                account_product_code: "01".to_string(),
                product_name: "주식계좌".to_string(),
                evaluation_amount: 0,
                sell_amount: 0,
                buy_amount: 0,
                deposit_total_amount: 0,
                next_exercise_amount: 0,
                process_status_code: "00".to_string(),
            })
        } else {
            let error_text = response.text().await?;
            error!("계좌 잔고 조회 실패: {}", error_text);
            Err(anyhow!("계좌 잔고 조회 실패"))
        }
    }

    /// 보유 종목 조회
    pub async fn get_holding_stocks(&self) -> Result<Vec<HoldingStock>> {
        let url = format!("{}/uapi/domestic-stock/v1/trading/inquire-balance", self.base_url);
        
        let headers = self.create_real_trading_headers("TTTC8434R")?;
        
        let mut params: HashMap<&str, String> = HashMap::new();
        params.insert("CANO", self.account_number.as_ref().unwrap_or(&"".to_string()).clone());
        params.insert("ACNT_PRDT_CD", "01".to_string());
        params.insert("AFHR_FLPR_YN", "N".to_string());
        params.insert("OFL_YN", "".to_string());
        params.insert("INQR_DVSN", "01".to_string());
        params.insert("UNPR_DVSN", "01".to_string());
        params.insert("FUND_STTL_ICLD_YN", "N".to_string());
        params.insert("FNCG_AMT_AUTO_RDPT_YN", "N".to_string());
        params.insert("PRCS_DVSN", "01".to_string());
        params.insert("CTX_AREA_FK100", "".to_string());
        params.insert("CTX_AREA_NK100", "".to_string());

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if response.status().is_success() {
            let holdings_response: serde_json::Value = response.json().await?;
            // 실제 응답 구조에 맞게 파싱 필요
            Ok(vec![])
        } else {
            let error_text = response.text().await?;
            error!("보유 종목 조회 실패: {}", error_text);
            Err(anyhow!("보유 종목 조회 실패"))
        }
    }

    /// 매수 주문 실행
    pub async fn buy_stock(&self, stock_code: &str, quantity: i32, price: i32) -> Result<RealTradingResult> {
        let url = format!("{}/uapi/domestic-stock/v1/trading/order-cash", self.base_url);
        
        let headers = self.create_real_trading_headers("TTTC0802U")?;
        
        let order_request = OrderRequest {
            account_number: self.account_number.clone().unwrap_or_default(),
            account_product_code: "01".to_string(),
            stock_code: stock_code.to_string(),
            order_type: if price == 0 { "01".to_string() } else { "00".to_string() }, // 시장가/지정가
            quantity,
            price,
        };

        let response = self.client
            .post(&url)
            .headers(headers)
            .json(&order_request)
            .send()
            .await?;

        if response.status().is_success() {
            let order_response: OrderResponse = response.json().await?;
            if order_response.return_code == "0" {
                Ok(RealTradingResult {
                    success: true,
                    order_number: order_response.order_info.map(|info| info.order_number),
                    message: "매수 주문 성공".to_string(),
                    timestamp: Utc::now(),
                })
            } else {
                error!("매수 주문 실패: {}", order_response.message);
                Ok(RealTradingResult {
                    success: false,
                    order_number: None,
                    message: order_response.message,
                    timestamp: Utc::now(),
                })
            }
        } else {
            let error_text = response.text().await?;
            error!("매수 주문 API 호출 실패: {}", error_text);
            Err(anyhow!("매수 주문 API 호출 실패"))
        }
    }

    /// 매도 주문 실행
    pub async fn sell_stock(&self, stock_code: &str, quantity: i32, price: i32) -> Result<RealTradingResult> {
        let url = format!("{}/uapi/domestic-stock/v1/trading/order-cash", self.base_url);
        
        let headers = self.create_real_trading_headers("TTTC0801U")?;
        
        let order_request = OrderRequest {
            account_number: self.account_number.clone().unwrap_or_default(),
            account_product_code: "01".to_string(),
            stock_code: stock_code.to_string(),
            order_type: if price == 0 { "01".to_string() } else { "00".to_string() },
            quantity,
            price,
        };

        let response = self.client
            .post(&url)
            .headers(headers)
            .json(&order_request)
            .send()
            .await?;

        if response.status().is_success() {
            let order_response: OrderResponse = response.json().await?;
            if order_response.return_code == "0" {
                Ok(RealTradingResult {
                    success: true,
                    order_number: order_response.order_info.map(|info| info.order_number),
                    message: "매도 주문 성공".to_string(),
                    timestamp: Utc::now(),
                })
            } else {
                error!("매도 주문 실패: {}", order_response.message);
                Ok(RealTradingResult {
                    success: false,
                    order_number: None,
                    message: order_response.message,
                    timestamp: Utc::now(),
                })
            }
        } else {
            let error_text = response.text().await?;
            error!("매도 주문 API 호출 실패: {}", error_text);
            Err(anyhow!("매도 주문 API 호출 실패"))
        }
    }

    /// 실매매용 헤더 생성
    fn create_real_trading_headers(&self, tr_id: &str) -> Result<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();
        
        if let Some(token) = &self.access_token {
            headers.insert("Authorization", format!("Bearer {}", token).parse()?);
        }
        headers.insert("appkey", self.app_key.parse()?);
        headers.insert("appsecret", self.app_secret.parse()?);
        headers.insert("tr_id", tr_id.parse()?);
        headers.insert("Content-Type", "application/json; charset=utf-8".parse()?);
        
        Ok(headers)
    }

    /// 토큰 유효성 검사
    pub fn is_token_valid(&self) -> bool {
        self.access_token.is_some()
    }

    /// 모의투자 여부 확인
    pub fn is_simulation(&self) -> bool {
        self.is_simulation
    }
}

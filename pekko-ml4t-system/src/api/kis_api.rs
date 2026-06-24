//! 한국투자증권 API 클라이언트
//! 
//! KIS API를 사용하여 한국 주식 시장 데이터 수집 및 거래 실행

use super::{
    ApiClient, ApiConfig, MarketDataResponse, AccountInfo, OrderRequest, 
    OrderResponse, Position, OrderStatus, ApiError
};
use crate::actors::{MarketData, Position as ActorPosition};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{ warn, error};

/// KIS API 클라이언트
#[derive(Clone)]
pub struct KisApiClient {
    client: Client,
    config: ApiConfig,
    access_token: Option<String>,
    token_expires_at: Option<DateTime<Utc>>,
}

/// KIS API 토큰 응답
#[derive(Debug, Deserialize)]
struct KisTokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

/// KIS API 주식 현재가 조회 응답
#[derive(Debug, Deserialize)]
struct KisStockPriceResponse {
    output: Vec<KisStockPriceOutput>,
}

#[derive(Debug, Deserialize)]
struct KisStockPriceOutput {
    #[serde(rename = "stck_prpr")]
    current_price: String,
    #[serde(rename = "stck_oprc")]
    opening_price: String,
    #[serde(rename = "stck_hgpr")]
    high_price: String,
    #[serde(rename = "stck_lwpr")]
    low_price: String,
    #[serde(rename = "stck_prdy_vrss")]
    price_change: String,
    #[serde(rename = "stck_prdy_ctrt")]
    price_change_rate: String,
    #[serde(rename = "acml_vol")]
    accumulated_volume: String,
    #[serde(rename = "prdy_vrss_vol")]
    volume_change: String,
    #[serde(rename = "hts_kor_isnm")]
    stock_name: String,
    #[serde(rename = "stck_prdt")]
    price_date: String,
    #[serde(rename = "stck_prtm")]
    price_time: String,
}

/// KIS API 계좌 잔고 조회 응답
#[derive(Debug, Deserialize)]
struct KisBalanceResponse {
    output1: Vec<KisBalanceOutput>,
    output2: Vec<KisStockBalanceOutput>,
}

#[derive(Debug, Deserialize)]
struct KisBalanceOutput {
    #[serde(rename = "dnca_tot_amt")]
    total_amount: String,
    #[serde(rename = "nxdy_excc_amt")]
    next_day_exercise_amount: String,
    #[serde(rename = "prvs_rcdl_excc_amt")]
    previous_day_exercise_amount: String,
    #[serde(rename = "cma_evlu_amt")]
    cash_margin_evaluation_amount: String,
    #[serde(rename = "bfdy_buy_amt")]
    previous_day_buy_amount: String,
    #[serde(rename = "nxdy_auto_rdpt_amt")]
    next_day_auto_repayment_amount: String,
    #[serde(rename = "bfdy_sll_amt")]
    previous_day_sell_amount: String,
    #[serde(rename = "d2_auto_rdpt_amt")]
    day_2_auto_repayment_amount: String,
    #[serde(rename = "bfdy_tlex_amt")]
    previous_day_tax_amount: String,
    #[serde(rename = "tot_loan_amt")]
    total_loan_amount: String,
    #[serde(rename = "scts_evlu_amt")]
    securities_evaluation_amount: String,
    #[serde(rename = "tot_evlu_amt")]
    total_evaluation_amount: String,
    #[serde(rename = "nass_amt")]
    net_amount: String,
    #[serde(rename = "fncg_gld_auto_rdpt_yn")]
    financial_gold_auto_repayment_yn: String,
    #[serde(rename = "pchs_amt_smtl_amt")]
    purchase_amount_sum_total_amount: String,
    #[serde(rename = "evlu_amt_smtl_amt")]
    evaluation_amount_sum_total_amount: String,
    #[serde(rename = "evlu_pfls_smtl_amt")]
    evaluation_profit_loss_sum_total_amount: String,
    #[serde(rename = "tot_stln_slng_chgs")]
    total_stock_loan_interest_charges: String,
    #[serde(rename = "bfdy_tlex_amt")]
    previous_day_tax_amount_2: String,
    #[serde(rename = "stln_rat")]
    stock_loan_rate: String,
    #[serde(rename = "stln_amt")]
    stock_loan_amount: String,
    #[serde(rename = "stln_use_rat")]
    stock_loan_use_rate: String,
    #[serde(rename = "loan_dt")]
    loan_date: String,
    #[serde(rename = "excc_amt")]
    exercise_amount: String,
    #[serde(rename = "ord_psbl_cash")]
    orderable_cash: String,
    #[serde(rename = "ruse_psbl_amt")]
    reusable_amount: String,
    #[serde(rename = "fndr_ord_psbl_cash")]
    founder_order_possible_cash: String,
    #[serde(rename = "psbl_sell_cash")]
    possible_sell_cash: String,
    #[serde(rename = "crdt_ord_psbl_amt")]
    credit_order_possible_amount: String,
    #[serde(rename = "expc_depo_wdrc_amt")]
    expected_deposit_withdrawal_amount: String,
}

#[derive(Debug, Deserialize)]
struct KisStockBalanceOutput {
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

#[async_trait::async_trait]
impl ApiClient for KisApiClient {
    type Config = ApiConfig;
    type Response = MarketDataResponse;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()?;

        Ok(Self {
            client,
            config,
            access_token: None,
            token_expires_at: None,
        })
    }

    async fn authenticate(&mut self) -> Result<()> {
        let url = format!("{}/oauth2/tokenP", self.config.base_url);
        
        let mut params = HashMap::new();
        params.insert("grant_type", "client_credentials");
        params.insert("appkey", &self.config.api_key);
        params.insert("appsecret", &self.config.api_secret);

        let response = self.client
            .post(&url)
            .form(&params)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::AuthenticationError(error_text).into());
        }

        let token_response: KisTokenResponse = response.json().await?;
        
        self.access_token = Some(token_response.access_token);
        self.token_expires_at = Some(Utc::now() + chrono::Duration::seconds(token_response.expires_in as i64));
        Ok(())
    }

    async fn get_market_data(&self, symbol: &str) -> Result<MarketDataResponse> {
        self.ensure_authenticated().await?;

        let url = format!("{}/uapi/domestic-stock/v1/quotations/inquire-price", self.config.base_url);
        
        let headers = self.get_auth_headers();
        let params = [
            ("fid_cond_mrkt_div_code", "J"),
            ("fid_input_iscd", symbol),
        ];

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::NetworkError(error_text).into());
        }

        let price_response: KisStockPriceResponse = response.json().await?;
        
        if price_response.output.is_empty() {
            return Err(ApiError::ParseError("주가 데이터가 없습니다".to_string()).into());
        }

        let stock_data = &price_response.output[0];
        
        let current_price = stock_data.current_price.parse::<f64>()?;
        let open_price = stock_data.opening_price.parse::<f64>()?;
        let high_price = stock_data.high_price.parse::<f64>()?;
        let low_price = stock_data.low_price.parse::<f64>()?;
        let volume = stock_data.accumulated_volume.parse::<u64>()?;

        Ok(MarketDataResponse {
            symbol: symbol.to_string(),
            timestamp: Utc::now(),
            price: current_price,
            volume,
            open: Some(open_price),
            high: Some(high_price),
            low: Some(low_price),
            close: Some(current_price),
        })
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        self.ensure_authenticated().await?;

        let url = format!("{}/uapi/domestic-stock/v1/trading/inquire-balance", self.config.base_url);
        
        let headers = self.get_auth_headers();
        let params = [
            ("CANO", &self.config.api_key[..8]), // 계좌번호 앞 8자리
            ("ACNT_PRDT_CD", "01"), // 계좌상품코드
            ("AFHR_FLPR_YN", "N"), // 시간외단일가여부
            ("OFL_YN", ""), // 오프라인여부
            ("INQR_DVSN", "02"), // 조회구분
            ("UNPR_DVSN", "01"), // 단가구분
            ("FUND_STTL_ICLD_YN", "N"), // 펀드결제분포함여부
            ("FNCG_AMT_AUTO_RDPT_YN", "N"), // 융자금액자동상환여부
            ("PRCS_DVSN", "01"), // 처리구분
            ("CTX_AREA_FK100", ""), // 연속조회검색조건100
            ("CTX_AREA_NK100", ""), // 연속조회키100
        ];

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::AccountError(error_text).into());
        }

        let balance_response: KisBalanceResponse = response.json().await?;
        
        if balance_response.output1.is_empty() {
            return Err(ApiError::AccountError("계좌 정보가 없습니다".to_string()).into());
        }

        let balance = &balance_response.output1[0];
        
        Ok(AccountInfo {
            account_id: self.config.api_key[..8].to_string(),
            total_balance: balance.total_evaluation_amount.parse::<f64>().unwrap_or(0.0),
            cash_balance: balance.orderable_cash.parse::<f64>().unwrap_or(0.0),
            positions: vec![],
        })
    }

    async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        self.ensure_authenticated().await?;

        let url = format!("{}/uapi/domestic-stock/v1/trading/order-cash", self.config.base_url);
        
        let headers = self.get_auth_headers();
        
        let side_code = match order.side {
            super::OrderSide::Buy => "01",
            super::OrderSide::Sell => "02",
        };
        
        // 가격이 있으면 지정가, 없으면 시장가
        let order_type_code = if order.price.is_some() { "00" } else { "01" };

        let params = [
            ("CANO", &self.config.api_key[..8]),
            ("ACNT_PRDT_CD", "01"),
            ("PDNO", &order.symbol),
            ("ORD_DVSN", order_type_code),
            ("ORD_QTY", &order.quantity.to_string()),
            ("ORD_UNPR", &order.price.unwrap_or(0.0).to_string()),
            ("CTAC_TLNO", ""),
            ("SLL_TYPE", "01"),
            ("ALGO_NO", ""),
        ];

        let response = self.client
            .post(&url)
            .headers(headers)
            .form(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::OrderError(error_text).into());
        }

        // TODO: 실제 주문 응답 파싱
        Ok(OrderResponse {
            order_id: format!("KIS_{}", Utc::now().timestamp()),
            status: OrderStatus::Submitted,
            filled_quantity: 0.0,
            filled_price: order.price.unwrap_or(0.0),
            commission: 0.0,
            timestamp: Utc::now(),
        })
    }

    async fn get_positions(&self) -> Result<Vec<Position>> {
        self.ensure_authenticated().await?;

        let url = format!("{}/uapi/domestic-stock/v1/trading/inquire-balance", self.config.base_url);
        
        let headers = self.get_auth_headers();
        let params = [
            ("CANO", &self.config.api_key[..8]),
            ("ACNT_PRDT_CD", "01"),
            ("AFHR_FLPR_YN", "N"),
            ("OFL_YN", ""),
            ("INQR_DVSN", "02"),
            ("UNPR_DVSN", "01"),
            ("FUND_STTL_ICLD_YN", "N"),
            ("FNCG_AMT_AUTO_RDPT_YN", "N"),
            ("PRCS_DVSN", "01"),
            ("CTX_AREA_FK100", ""),
            ("CTX_AREA_NK100", ""),
        ];

        let response = self.client
            .get(&url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(ApiError::AccountError(error_text).into());
        }

        let balance_response: KisBalanceResponse = response.json().await?;
        
        let mut positions = Vec::new();
        
        for stock in balance_response.output2 {
            let quantity = stock.holding_quantity.parse::<i32>().unwrap_or(0);
            if quantity > 0 {
                let avg_price = stock.purchase_average_price.parse::<f64>().unwrap_or(0.0);
                let current_price = stock.current_price.parse::<f64>().unwrap_or(0.0);
                let market_value = stock.evaluation_amount.parse::<f64>().unwrap_or(0.0);
                let unrealized_pnl = stock.evaluation_profit_loss_amount.parse::<f64>().unwrap_or(0.0);
                
                positions.push(Position {
                    symbol: stock.product_code,
                    quantity: quantity as f64,
                    average_price: avg_price,
                    current_price,
                    market_value,
                    unrealized_pnl,
                });
            }
        }
        
        Ok(positions)
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus> {
        // TODO: KIS API 주문 상태 조회 구현
        Ok(OrderStatus::Submitted)
    }
}

impl KisApiClient {
    /// 인증 토큰이 유효한지 확인하고 필요시 갱신
    async fn ensure_authenticated(&self) -> Result<()> {
        if let Some(expires_at) = self.token_expires_at {
            if Utc::now() > expires_at {
                return Err(ApiError::AuthenticationError("토큰이 만료되었습니다".to_string()).into());
            }
        } else {
            return Err(ApiError::AuthenticationError("인증되지 않았습니다".to_string()).into());
        }
        Ok(())
    }

    /// 인증 헤더 생성
    fn get_auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        
        if let Some(token) = &self.access_token {
            headers.insert(
                "Authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
        }
        
        headers.insert("appkey", self.config.api_key.parse().unwrap());
        headers.insert("appsecret", self.config.api_secret.parse().unwrap());
        headers.insert("tr_id", "FHKST01010100".parse().unwrap());
        headers.insert("tr_cont", "N".parse().unwrap());
        headers.insert("custtype", "P".parse().unwrap());
        
        headers
    }

    /// Actor용 MarketData로 변환
    pub fn to_actor_market_data(&self, response: &MarketDataResponse) -> MarketData {
        MarketData {
            symbol: response.symbol.clone(),
            timestamp: response.timestamp,
            open: response.open.unwrap_or(response.price),
            high: response.high.unwrap_or(response.price),
            low: response.low.unwrap_or(response.price),
            close: response.close.unwrap_or(response.price),
            volume: response.volume,
            vwap: response.price,
        }
    }

    /// Actor용 Position으로 변환
    pub fn to_actor_position(&self, position: &Position) -> ActorPosition {
        ActorPosition {
            symbol: position.symbol.clone(),
            quantity: position.quantity as i64,
            average_price: position.average_price,
            current_price: position.current_price,
            unrealized_pnl: position.unrealized_pnl,
            realized_pnl: position.unrealized_pnl,
        }
    }
}

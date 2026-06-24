use super::*;
use crate::api::{ApiClient, real_kis_api::RealKisApi, real_alpha_vantage_api::RealAlphaVantageApi};
use crate::config::api_config::ApiConfig;
use anyhow::Result;
use chrono::{Utc, Duration, Datelike, Timelike};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, interval};
use tracing::{ warn, error, debug};

/// 실제 API를 사용하는 데이터 수집기
pub struct RealApiDataCollector {
    config: ApiConfig,
    kis_api: Option<RealKisApi>,
    alpha_vantage_api: Option<RealAlphaVantageApi>,
    market_data_cache: Arc<RwLock<HashMap<String, MarketData>>>,
    last_update_time: Arc<RwLock<HashMap<String, chrono::DateTime<Utc>>>>,
    is_running: Arc<RwLock<bool>>,
}

impl RealApiDataCollector {
    /// 새로운 실제 API 데이터 수집기 생성
    pub async fn new(config: ApiConfig) -> Result<Self> {
        let mut kis_api = None;
        let mut alpha_vantage_api = None;

        // KIS API 초기화
        if config.is_kis_enabled() {
            let mut kis = RealKisApi::new(config.kis.clone());
            if let Err(e) = kis.authenticate().await {
                warn!("KIS API 인증 실패: {}", e);
            } else {
                kis_api = Some(kis);
            }
        }

        // Alpha Vantage API 초기화
        if config.is_alpha_vantage_enabled() {
            let mut av = RealAlphaVantageApi::new(config.alpha_vantage.clone());
            if let Err(e) = av.authenticate().await {
                warn!("Alpha Vantage API 인증 실패: {}", e);
            } else {
                alpha_vantage_api = Some(av);
            }
        }

        Ok(Self {
            config,
            kis_api,
            alpha_vantage_api,
            market_data_cache: Arc::new(RwLock::new(HashMap::new())),
            last_update_time: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// 데이터 수집 시작
    pub async fn start_collection(&self, symbols: Vec<String>, interval_seconds: u64) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            warn!("데이터 수집이 이미 실행 중입니다");
            return Ok(());
        }
        *is_running = true;
        drop(is_running);
        let symbols_arc = Arc::new(symbols.clone());
        let cache = Arc::clone(&self.market_data_cache);
        let last_update = Arc::clone(&self.last_update_time);
        let is_running_ref = Arc::clone(&self.is_running);

        // 백그라운드 수집 태스크 시작
        let mut collector = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = interval(std::time::Duration::from_secs(interval_seconds));
            
            while *is_running_ref.read().await {
                interval_timer.tick().await;
                
                if let Err(e) = collector.collect_all_symbols(&symbols_arc, &cache, &last_update).await {
                    error!("데이터 수집 오류: {}", e);
                }
            }
        });

        Ok(())
    }

    /// 데이터 수집 중지
    pub async fn stop_collection(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        *is_running = false;
        Ok(())
    }

    /// 모든 종목 데이터 수집
    async fn collect_all_symbols(
        &mut self,
        symbols: &Arc<Vec<String>>,
        cache: &Arc<RwLock<HashMap<String, MarketData>>>,
        last_update: &Arc<RwLock<HashMap<String, chrono::DateTime<Utc>>>>,
    ) -> Result<()> {
        let symbols = symbols.as_ref();
        let mut successful_collections = 0;
        let mut failed_collections = 0;

        for symbol in symbols {
            match self.collect_single_symbol(symbol).await {
                Ok(market_data) => {
                    // 캐시에 저장
                    let mut cache_guard = cache.write().await;
                    cache_guard.insert(symbol.clone(), market_data.clone());
                    drop(cache_guard);

                    // 마지막 업데이트 시간 기록
                    let mut last_update_guard = last_update.write().await;
                    last_update_guard.insert(symbol.clone(), Utc::now());
                    drop(last_update_guard);

                    successful_collections += 1;
                    debug!("{} 시세 수집 성공: ${:.2}", symbol, market_data.close);
                }
                Err(e) => {
                    failed_collections += 1;
                    error!("{} 시세 수집 실패: {}", symbol, e);
                }
            }

            // API 요청 제한을 위한 짧은 대기
            sleep(std::time::Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// 단일 종목 데이터 수집
    async fn collect_single_symbol(&mut self, symbol: &str) -> Result<MarketData> {
        // KIS API 우선 시도 (한국 종목인 경우)
        if self.is_korean_stock(symbol) {
            if let Some(ref mut kis_api) = self.kis_api {
                match kis_api.get_market_data(symbol).await {
                    Ok(response) => {
                        return Ok(MarketData {
                            symbol: symbol.to_string(),
                            timestamp: response.timestamp,
                            open: response.open.unwrap_or(response.price),
                            high: response.high.unwrap_or(response.price),
                            low: response.low.unwrap_or(response.price),
                            close: response.close.unwrap_or(response.price),
                            volume: response.volume,
                            vwap: response.price, // VWAP는 별도 계산 필요
                            adjusted_close: Some(response.close.unwrap_or(response.price)),
                            dividend: Some(0.0),
                            split_ratio: Some(1.0),
                        });
                    }
                    Err(e) => {
                        debug!("KIS API 실패, Alpha Vantage 시도: {}", e);
                    }
                }
            }
        }

        // Alpha Vantage API 시도
        if let Some(ref mut alpha_vantage_api) = self.alpha_vantage_api {
            match alpha_vantage_api.get_market_data(symbol).await {
                Ok(response) => {
                    return Ok(MarketData {
                        symbol: symbol.to_string(),
                        timestamp: response.timestamp,
                        open: response.open.unwrap_or(response.price),
                        high: response.high.unwrap_or(response.price),
                        low: response.low.unwrap_or(response.price),
                        close: response.close.unwrap_or(response.price),
                        volume: response.volume,
                        vwap: response.price, // VWAP는 별도 계산 필요
                        adjusted_close: Some(response.close.unwrap_or(response.price)),
                        dividend: Some(0.0),
                        split_ratio: Some(1.0),
                    });
                }
                Err(e) => {
                    error!("Alpha Vantage API도 실패: {}", e);
                    return Err(e);
                }
            }
        }

        Err(anyhow::anyhow!("사용 가능한 API가 없습니다"))
    }

    /// 한국 주식인지 확인 (간단한 휴리스틱)
    fn is_korean_stock(&self, symbol: &str) -> bool {
        // 6자리 숫자로 시작하는 경우 한국 종목코드로 간주
        symbol.len() == 6 && symbol.chars().all(|c| c.is_ascii_digit())
    }

    /// 캐시된 시장 데이터 조회
    pub async fn get_market_data(&self, symbol: &str) -> Result<Option<MarketData>> {
        let cache = self.market_data_cache.read().await;
        Ok(cache.get(symbol).cloned())
    }

    /// 모든 캐시된 데이터 조회
    pub async fn get_all_market_data(&self) -> HashMap<String, MarketData> {
        let cache = self.market_data_cache.read().await;
        cache.clone()
    }

    /// 데이터 신선도 확인 (마지막 업데이트로부터 5분 이내)
    pub async fn is_data_fresh(&self, symbol: &str) -> bool {
        let last_update = self.last_update_time.read().await;
        if let Some(last_time) = last_update.get(symbol) {
            let now = Utc::now();
            let age = now - *last_time;
            age < Duration::minutes(5)
        } else {
            false
        }
    }

    /// API 상태 확인
    pub fn get_api_status(&self) -> HashMap<String, bool> {
        let mut status = HashMap::new();
        status.insert("KIS".to_string(), self.kis_api.is_some());
        status.insert("Alpha Vantage".to_string(), self.alpha_vantage_api.is_some());
        status
    }

    /// 수집 통계 정보
    pub async fn get_collection_stats(&self) -> CollectionStats {
        let cache = self.market_data_cache.read().await;
        let last_update = self.last_update_time.read().await;
        
        let total_symbols = cache.len();
        let fresh_symbols = last_update.values()
            .filter(|&time| Utc::now() - *time < Duration::minutes(5))
            .count();

        CollectionStats {
            total_requests: total_symbols as u64,
            successful_requests: total_symbols as u64,
            failed_requests: 0, // 별도 추적 필요
            total_records: total_symbols as u64,
            average_latency_ms: 0.0, // 별도 추적 필요
            last_success: last_update.values().max().copied(),
            last_error: None,
        }
    }
}

impl Clone for RealApiDataCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            kis_api: self.kis_api.clone(),
            alpha_vantage_api: self.alpha_vantage_api.clone(),
            market_data_cache: Arc::clone(&self.market_data_cache),
            last_update_time: Arc::clone(&self.last_update_time),
            is_running: Arc::clone(&self.is_running),
        }
    }
}

/// 실제 API 데이터 수집기 확장 기능
impl RealApiDataCollector {
    /// 특정 종목의 RSI 지표 조회
    pub async fn get_rsi(&mut self, symbol: &str, period: u32) -> Result<Option<f64>> {
        if let Some(ref mut alpha_vantage_api) = self.alpha_vantage_api {
            match alpha_vantage_api.get_rsi(symbol, period).await {
                Ok(rsi) => Ok(Some(rsi)),
                Err(e) => {
                    warn!("RSI 조회 실패: {}", e);
                    Ok(None)
                }
            }
        } else {
            warn!("Alpha Vantage API가 활성화되지 않았습니다");
            Ok(None)
        }
    }

    /// 시장 개장 상태 확인
    pub async fn is_market_open(&self) -> Result<bool> {
        if let Some(ref alpha_vantage_api) = self.alpha_vantage_api {
            alpha_vantage_api.is_market_open().await
        } else {
            // 간단한 휴리스틱: 평일 09:30-16:00 KST
            let now = Utc::now();
            let weekday = now.weekday();
            let hour = now.hour();
            
            let is_weekday = weekday != chrono::Weekday::Sat && weekday != chrono::Weekday::Sun;
            let is_market_hours = hour >= 0 && hour < 7; // UTC 기준 (KST 09:00-16:00)
            
            Ok(is_weekday && is_market_hours)
        }
    }

    /// API 헬스 체크
    pub async fn health_check(&self) -> HealthStatus {
        let mut issues = Vec::new();
        
        // KIS API 체크
        if self.config.is_kis_enabled() && self.kis_api.is_none() {
            issues.push("KIS API 연결 실패".to_string());
        }

        // Alpha Vantage API 체크
        if self.config.is_alpha_vantage_enabled() && self.alpha_vantage_api.is_none() {
            issues.push("Alpha Vantage API 연결 실패".to_string());
        }

        // 데이터 신선도 체크
        let cache = self.market_data_cache.read().await;
        let stale_count = cache.len() - cache.values()
            .filter(|data| Utc::now() - data.timestamp < Duration::minutes(10))
            .count();

        if stale_count > cache.len() / 2 {
            issues.push(format!("{}개 종목의 데이터가 오래됨", stale_count));
        }

        if issues.is_empty() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Degraded(issues.join(", "))
        }
    }
}

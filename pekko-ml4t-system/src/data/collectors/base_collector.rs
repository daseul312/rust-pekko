//! 기본 데이터 수집기 구현
//! 
//! 모든 데이터 수집기의 공통 기능을 제공하는 기본 구현체

use super::{DataCollector, CollectParams, CollectionStats, HealthStatus};
use crate::data::{DataSourceConfig, RateLimitConfig};
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use tracing::{ warn, error, debug};

/// 기본 수집기 구현
pub struct BaseCollector {
    client: Client,
    config: DataSourceConfig,
    stats: Arc<CollectionStatsInternal>,
    rate_limiter: Arc<RateLimiter>,
}

/// 내부 통계 구조체
#[derive(Debug)]
struct CollectionStatsInternal {
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_records: AtomicU64,
    latency_sum: AtomicU64, // 마이크로초 단위
    latency_count: AtomicU64,
    last_success: tokio::sync::RwLock<Option<chrono::DateTime<chrono::Utc>>>,
    last_error: tokio::sync::RwLock<Option<String>>,
}

/// 속도 제한기
pub struct RateLimiter {
    requests_per_minute: u32,
    requests_per_hour: u32,
    requests_per_day: u32,
    minute_tokens: tokio::sync::RwLock<u32>,
    hour_tokens: tokio::sync::RwLock<u32>,
    day_tokens: tokio::sync::RwLock<u32>,
    last_minute_reset: tokio::sync::RwLock<Instant>,
    last_hour_reset: tokio::sync::RwLock<Instant>,
    last_day_reset: tokio::sync::RwLock<Instant>,
}

impl BaseCollector {
    /// 새로운 기본 수집기 생성
    pub fn new(config: DataSourceConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.api_config.timeout_seconds))
            .build()
            .expect("HTTP 클라이언트 생성 실패");

        let rate_limiter = Arc::new(RateLimiter::new(&config.api_config.rate_limit));

        Self {
            client,
            config: config.clone(),
            stats: Arc::new(CollectionStatsInternal {
                total_requests: AtomicU64::new(0),
                successful_requests: AtomicU64::new(0),
                failed_requests: AtomicU64::new(0),
                total_records: AtomicU64::new(0),
                latency_sum: AtomicU64::new(0),
                latency_count: AtomicU64::new(0),
                last_success: tokio::sync::RwLock::new(None),
                last_error: tokio::sync::RwLock::new(None),
            }),
            rate_limiter,
        }
    }

    /// HTTP 요청 실행 (속도 제한 적용)
    pub async fn execute_request<T>(&self, request_builder: reqwest::RequestBuilder) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        // 속도 제한 확인
        self.rate_limiter.wait_for_permission().await?;

        let start_time = Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        debug!("HTTP 요청 시작: {}", self.config.source_name);

        match request_builder.send().await {
            Ok(response) => {
                let latency = start_time.elapsed();
                self.update_latency_stats(latency);

                let status = response.status();
                if status.is_success() {
                    let data: T = response.json().await?;
                    self.stats.successful_requests.fetch_add(1, Ordering::Relaxed);
                    
                    {
                        let mut last_success = self.stats.last_success.write().await;
                        *last_success = Some(chrono::Utc::now());
                    }
                    Ok(data)
                } else {
                    let error_text = response.text().await.unwrap_or_default();
                    let error_msg = format!("HTTP 오류 {}: {}", status, error_text);
                    
                    self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut last_error = self.stats.last_error.write().await;
                        *last_error = Some(error_msg.clone());
                    }
                    
                    error!("HTTP 요청 실패: {}", error_msg);
                    Err(anyhow::anyhow!(error_msg))
                }
            }
            Err(e) => {
                let latency = start_time.elapsed();
                self.update_latency_stats(latency);
                
                self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
                {
                    let mut last_error = self.stats.last_error.write().await;
                    *last_error = Some(e.to_string());
                }
                
                error!("HTTP 요청 오류: {}", e);
                Err(anyhow::anyhow!(e))
            }
        }
    }

    /// 레이턴시 통계 업데이트
    fn update_latency_stats(&self, latency: Duration) {
        let latency_micros = latency.as_micros() as u64;
        self.stats.latency_sum.fetch_add(latency_micros, Ordering::Relaxed);
        self.stats.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 레코드 수 업데이트
    pub fn update_record_count(&self, count: u64) {
        self.stats.total_records.fetch_add(count, Ordering::Relaxed);
    }

    /// 설정 조회
    pub fn get_config(&self) -> &DataSourceConfig {
        &self.config
    }

    /// HTTP 클라이언트 조회
    pub fn get_client(&self) -> &Client {
        &self.client
    }
}

impl RateLimiter {
    /// 새로운 속도 제한기 생성
    pub fn new(config: &RateLimitConfig) -> Self {
        let now = Instant::now();
        Self {
            requests_per_minute: config.requests_per_minute,
            requests_per_hour: config.requests_per_hour,
            requests_per_day: config.requests_per_day,
            minute_tokens: tokio::sync::RwLock::new(config.requests_per_minute),
            hour_tokens: tokio::sync::RwLock::new(config.requests_per_hour),
            day_tokens: tokio::sync::RwLock::new(config.requests_per_day),
            last_minute_reset: tokio::sync::RwLock::new(now),
            last_hour_reset: tokio::sync::RwLock::new(now),
            last_day_reset: tokio::sync::RwLock::new(now),
        }
    }

    /// 요청 권한 대기
    pub async fn wait_for_permission(&self) -> Result<()> {
        loop {
            // 토큰 리셋 확인 및 실행
            self.reset_tokens_if_needed().await;

            // 토큰 확인
            if self.has_tokens().await {
                self.consume_tokens().await;
                return Ok(());
            }

            // 토큰이 없으면 잠시 대기
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 토큰 리셋 확인 및 실행
    async fn reset_tokens_if_needed(&self) {
        let now = Instant::now();

        // 분 단위 토큰 리셋
        {
            let mut last_reset = self.last_minute_reset.write().await;
            if now.duration_since(*last_reset) >= Duration::from_secs(60) {
                let mut tokens = self.minute_tokens.write().await;
                *tokens = self.requests_per_minute;
                *last_reset = now;
                debug!("분 단위 토큰 리셋: {}개", self.requests_per_minute);
            }
        }

        // 시간 단위 토큰 리셋
        {
            let mut last_reset = self.last_hour_reset.write().await;
            if now.duration_since(*last_reset) >= Duration::from_secs(3600) {
                let mut tokens = self.hour_tokens.write().await;
                *tokens = self.requests_per_hour;
                *last_reset = now;
                debug!("시간 단위 토큰 리셋: {}개", self.requests_per_hour);
            }
        }

        // 일 단위 토큰 리셋
        {
            let mut last_reset = self.last_day_reset.write().await;
            if now.duration_since(*last_reset) >= Duration::from_secs(86400) {
                let mut tokens = self.day_tokens.write().await;
                *tokens = self.requests_per_day;
                *last_reset = now;
                debug!("일 단위 토큰 리셋: {}개", self.requests_per_day);
            }
        }
    }

    /// 토큰 보유 여부 확인
    async fn has_tokens(&self) -> bool {
        let minute_tokens = *self.minute_tokens.read().await;
        let hour_tokens = *self.hour_tokens.read().await;
        let day_tokens = *self.day_tokens.read().await;

        minute_tokens > 0 && hour_tokens > 0 && day_tokens > 0
    }

    /// 토큰 소비
    async fn consume_tokens(&self) {
        {
            let mut tokens = self.minute_tokens.write().await;
            if *tokens > 0 {
                *tokens -= 1;
            }
        }

        {
            let mut tokens = self.hour_tokens.write().await;
            if *tokens > 0 {
                *tokens -= 1;
            }
        }

        {
            let mut tokens = self.day_tokens.write().await;
            if *tokens > 0 {
                *tokens -= 1;
            }
        }
    }

    /// 남은 토큰 수 조회
    pub async fn get_remaining_tokens(&self) -> (u32, u32, u32) {
        let minute_tokens = *self.minute_tokens.read().await;
        let hour_tokens = *self.hour_tokens.read().await;
        let day_tokens = *self.day_tokens.read().await;

        (minute_tokens, hour_tokens, day_tokens)
    }
}

impl BaseCollector {
    /// 통계 조회
    pub fn get_collection_stats(&self) -> CollectionStats {
        let total_requests = self.stats.total_requests.load(Ordering::Relaxed);
        let successful_requests = self.stats.successful_requests.load(Ordering::Relaxed);
        let failed_requests = self.stats.failed_requests.load(Ordering::Relaxed);
        let total_records = self.stats.total_records.load(Ordering::Relaxed);
        
        let latency_sum = self.stats.latency_sum.load(Ordering::Relaxed);
        let latency_count = self.stats.latency_count.load(Ordering::Relaxed);
        let average_latency_ms = if latency_count > 0 {
            (latency_sum / latency_count) as f64 / 1000.0
        } else {
            0.0
        };

        CollectionStats {
            total_requests,
            successful_requests,
            failed_requests,
            total_records,
            average_latency_ms,
            last_success: None, // TODO: 비동기적으로 조회
            last_error: None,   // TODO: 비동기적으로 조회
        }
    }

    /// 헬스 체크
    pub async fn perform_health_check(&self) -> Result<HealthStatus> {
        let stats = self.get_collection_stats();
        
        // 최근 성공률 계산
        let success_rate = if stats.total_requests > 0 {
            stats.successful_requests as f64 / stats.total_requests as f64
        } else {
            1.0
        };

        // 헬스 상태 판정
        if success_rate >= 0.95 {
            Ok(HealthStatus::Healthy)
        } else if success_rate >= 0.8 {
            Ok(HealthStatus::Degraded(format!("성공률: {:.1}%", success_rate * 100.0)))
        } else {
            Ok(HealthStatus::Unhealthy(format!("성공률: {:.1}%", success_rate * 100.0)))
        }
    }
}

impl DataCollector for BaseCollector {
    type DataType = serde_json::Value;

    fn initialize(&mut self, _config: DataSourceConfig) -> Result<()> {
        Ok(())
    }

    fn collect(&self, _params: CollectParams) -> Result<Self::DataType> {
        Err(anyhow::anyhow!("기본 수집기는 직접 사용할 수 없습니다"))
    }

    fn collect_batch(&self, _params: Vec<CollectParams>) -> Result<Vec<Self::DataType>> {
        Err(anyhow::anyhow!("기본 수집기는 직접 사용할 수 없습니다"))
    }

    fn health_check(&self) -> Result<HealthStatus> {
        tokio::runtime::Handle::current().block_on(self.perform_health_check())
    }

    fn get_stats(&self) -> CollectionStats {
        self.get_collection_stats()
    }
}

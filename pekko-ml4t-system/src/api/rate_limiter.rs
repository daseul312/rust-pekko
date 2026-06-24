//! API 호출 제한 관리
//! 
//! 다양한 API 제공업체의 호출 제한을 관리하고 준수합니다.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{ warn, debug};

/// API 호출 제한 관리자
#[derive(Debug)]
pub struct RateLimiter {
    limits: HashMap<String, RateLimit>,
    counters: Arc<RwLock<HashMap<String, RateCounter>>>,
}

/// API별 호출 제한 설정
#[derive(Debug, Clone)]
pub struct RateLimit {
    pub requests_per_second: u32,
    pub requests_per_minute: u32,
    pub requests_per_hour: u32,
    pub requests_per_day: u32,
    pub burst_limit: u32,
}

/// API별 호출 카운터
#[derive(Debug, Clone)]
pub struct RateCounter {
    pub second_count: u32,
    pub minute_count: u32,
    pub hour_count: u32,
    pub day_count: u32,
    pub last_second: Instant,
    pub last_minute: Instant,
    pub last_hour: Instant,
    pub last_day: Instant,
}

impl RateLimiter {
    /// 새로운 RateLimiter 생성
    pub fn new() -> Self {
        Self {
            limits: HashMap::new(),
            counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// API 제한 설정 추가
    pub fn add_limit(&mut self, api_name: String, limit: RateLimit) {
        let api_name_clone = api_name.clone();
        let limit_clone = limit.clone();
        self.limits.insert(api_name, limit);
    }

    /// API 호출 허용 여부 확인
    pub async fn is_allowed(&self, api_name: &str) -> bool {
        let limit = match self.limits.get(api_name) {
            Some(limit) => limit,
            None => {
                warn!("API 제한 설정이 없습니다: {}", api_name);
                return true; // 제한이 없으면 허용
            }
        };

        let mut counters = self.counters.write().await;
        let counter = counters.entry(api_name.to_string()).or_insert_with(|| {
            let now = Instant::now();
            RateCounter {
                second_count: 0,
                minute_count: 0,
                hour_count: 0,
                day_count: 0,
                last_second: now,
                last_minute: now,
                last_hour: now,
                last_day: now,
            }
        });

        let now = Instant::now();
        
        // 초 단위 제한 확인
        if now.duration_since(counter.last_second) >= Duration::from_secs(1) {
            counter.second_count = 0;
            counter.last_second = now;
        }
        
        if counter.second_count >= limit.requests_per_second {
            debug!("초당 호출 제한 초과: {} ({}/{})", api_name, counter.second_count, limit.requests_per_second);
            return false;
        }

        // 분 단위 제한 확인
        if now.duration_since(counter.last_minute) >= Duration::from_secs(60) {
            counter.minute_count = 0;
            counter.last_minute = now;
        }
        
        if counter.minute_count >= limit.requests_per_minute {
            debug!("분당 호출 제한 초과: {} ({}/{})", api_name, counter.minute_count, limit.requests_per_minute);
            return false;
        }

        // 시간 단위 제한 확인
        if now.duration_since(counter.last_hour) >= Duration::from_secs(3600) {
            counter.hour_count = 0;
            counter.last_hour = now;
        }
        
        if counter.hour_count >= limit.requests_per_hour {
            debug!("시간당 호출 제한 초과: {} ({}/{})", api_name, counter.hour_count, limit.requests_per_hour);
            return false;
        }

        // 일 단위 제한 확인
        if now.duration_since(counter.last_day) >= Duration::from_secs(86400) {
            counter.day_count = 0;
            counter.last_day = now;
        }
        
        if counter.day_count >= limit.requests_per_day {
            debug!("일당 호출 제한 초과: {} ({}/{})", api_name, counter.day_count, limit.requests_per_day);
            return false;
        }

        true
    }

    /// API 호출 기록
    pub async fn record_call(&self, api_name: &str) {
        let mut counters = self.counters.write().await;
        if let Some(counter) = counters.get_mut(api_name) {
            counter.second_count += 1;
            counter.minute_count += 1;
            counter.hour_count += 1;
            counter.day_count += 1;
        }
    }

    /// 다음 호출 가능 시간까지 대기
    pub async fn wait_for_permission(&self, api_name: &str) -> Duration {
        let limit = match self.limits.get(api_name) {
            Some(limit) => limit,
            None => return Duration::from_millis(0),
        };

        let counters = self.counters.read().await;
        let counter = match counters.get(api_name) {
            Some(counter) => counter,
            None => return Duration::from_millis(0),
        };

        let now = Instant::now();
        let mut max_wait = Duration::from_millis(0);

        // 초 단위 제한 대기 시간 계산
        if counter.second_count >= limit.requests_per_second {
            let elapsed = now.duration_since(counter.last_second);
            let wait_time = Duration::from_secs(1) - elapsed;
            max_wait = max_wait.max(wait_time);
        }

        // 분 단위 제한 대기 시간 계산
        if counter.minute_count >= limit.requests_per_minute {
            let elapsed = now.duration_since(counter.last_minute);
            let wait_time = Duration::from_secs(60) - elapsed;
            max_wait = max_wait.max(wait_time);
        }

        // 시간 단위 제한 대기 시간 계산
        if counter.hour_count >= limit.requests_per_hour {
            let elapsed = now.duration_since(counter.last_hour);
            let wait_time = Duration::from_secs(3600) - elapsed;
            max_wait = max_wait.max(wait_time);
        }

        // 일 단위 제한 대기 시간 계산
        if counter.day_count >= limit.requests_per_day {
            let elapsed = now.duration_since(counter.last_day);
            let wait_time = Duration::from_secs(86400) - elapsed;
            max_wait = max_wait.max(wait_time);
        }

        max_wait
    }

    /// API 호출 (제한 확인 후 실행)
    pub async fn call_api<F, R>(&self, api_name: &str, api_call: F) -> Result<R, RateLimitError>
    where
        F: std::future::Future<Output = Result<R, Box<dyn std::error::Error + Send + Sync>>>,
    {
        // 호출 허용 여부 확인
        while !self.is_allowed(api_name).await {
            let wait_time = self.wait_for_permission(api_name).await;
            if wait_time > Duration::from_millis(0) {
                debug!("API 호출 제한으로 대기: {} - {}ms", api_name, wait_time.as_millis());
                tokio::time::sleep(wait_time).await;
            }
        }

        // API 호출 기록
        self.record_call(api_name).await;

        // API 호출 실행
        match api_call.await {
            Ok(result) => Ok(result),
            Err(e) => {
                if e.to_string().contains("rate limit") || e.to_string().contains("too many requests") {
                    Err(RateLimitError::RateLimitExceeded(e.to_string()))
                } else {
                    Err(RateLimitError::ApiError(e.to_string()))
                }
            }
        }
    }

    /// 현재 호출 통계 조회
    pub async fn get_stats(&self, api_name: &str) -> Option<RateCounter> {
        let counters = self.counters.read().await;
        counters.get(api_name).cloned()
    }

    /// 모든 API 통계 조회
    pub async fn get_all_stats(&self) -> HashMap<String, RateCounter> {
        let counters = self.counters.read().await;
        counters.clone()
    }

    /// 카운터 리셋
    pub async fn reset_counters(&self, api_name: &str) {
        let mut counters = self.counters.write().await;
        if let Some(counter) = counters.get_mut(api_name) {
            let now = Instant::now();
            counter.second_count = 0;
            counter.minute_count = 0;
            counter.hour_count = 0;
            counter.day_count = 0;
            counter.last_second = now;
            counter.last_minute = now;
            counter.last_hour = now;
            counter.last_day = now;
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Rate Limiter 에러
#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    #[error("호출 제한 초과: {0}")]
    RateLimitExceeded(String),
    
    #[error("API 오류: {0}")]
    ApiError(String),
}

/// 기본 API 제한 설정
impl RateLimit {
    /// KIS API 제한 설정
    pub fn kis_default() -> Self {
        Self {
            requests_per_second: 10,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
            burst_limit: 20,
        }
    }

    /// Alpha Vantage API 제한 설정 (무료)
    pub fn alpha_vantage_free() -> Self {
        Self {
            requests_per_second: 5,
            requests_per_minute: 5,
            requests_per_hour: 300,
            requests_per_day: 500,
            burst_limit: 10,
        }
    }

    /// Alpha Vantage API 제한 설정 (프리미엄)
    pub fn alpha_vantage_premium() -> Self {
        Self {
            requests_per_second: 12,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
            burst_limit: 25,
        }
    }

    /// Polygon API 제한 설정
    pub fn polygon_default() -> Self {
        Self {
            requests_per_second: 5,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
            burst_limit: 15,
        }
    }

    /// News API 제한 설정
    pub fn news_api_default() -> Self {
        Self {
            requests_per_second: 1,
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
            burst_limit: 5,
        }
    }
}

/// RateLimiter 빌더
pub struct RateLimiterBuilder {
    rate_limiter: RateLimiter,
}

impl RateLimiterBuilder {
    pub fn new() -> Self {
        Self {
            rate_limiter: RateLimiter::new(),
        }
    }

    pub fn add_kis_api(mut self) -> Self {
        self.rate_limiter.add_limit("kis".to_string(), RateLimit::kis_default());
        self
    }

    pub fn add_alpha_vantage_free(mut self) -> Self {
        self.rate_limiter.add_limit("alpha_vantage".to_string(), RateLimit::alpha_vantage_free());
        self
    }

    pub fn add_alpha_vantage_premium(mut self) -> Self {
        self.rate_limiter.add_limit("alpha_vantage".to_string(), RateLimit::alpha_vantage_premium());
        self
    }

    pub fn add_polygon_api(mut self) -> Self {
        self.rate_limiter.add_limit("polygon".to_string(), RateLimit::polygon_default());
        self
    }

    pub fn add_news_api(mut self) -> Self {
        self.rate_limiter.add_limit("news_api".to_string(), RateLimit::news_api_default());
        self
    }

    pub fn add_custom_api(mut self, name: String, limit: RateLimit) -> Self {
        self.rate_limiter.add_limit(name, limit);
        self
    }

    pub fn build(self) -> RateLimiter {
        self.rate_limiter
    }
}

impl Default for RateLimiterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

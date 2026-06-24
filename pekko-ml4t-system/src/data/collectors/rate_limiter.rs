//! 속도 제한기 모듈
//! 
//! API 호출 속도 제한을 관리하는 고급 속도 제한기

use crate::data::RateLimitConfig;
use anyhow::Result;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

/// 고급 속도 제한기
pub struct AdvancedRateLimiter {
    config: RateLimitConfig,
    minute_window: Arc<RwLock<VecDeque<Instant>>>,
    hour_window: Arc<RwLock<VecDeque<Instant>>>,
    day_window: Arc<RwLock<VecDeque<Instant>>>,
    burst_queue: Arc<RwLock<VecDeque<Instant>>>,
    last_request: Arc<RwLock<Option<Instant>>>,
}

/// 속도 제한 상태
#[derive(Debug, Clone)]
pub struct RateLimitStatus {
    pub minute_remaining: u32,
    pub hour_remaining: u32,
    pub day_remaining: u32,
    pub burst_remaining: u32,
    pub next_reset_minute: Duration,
    pub next_reset_hour: Duration,
    pub next_reset_day: Duration,
}

impl AdvancedRateLimiter {
    /// 새로운 고급 속도 제한기 생성
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            minute_window: Arc::new(RwLock::new(VecDeque::new())),
            hour_window: Arc::new(RwLock::new(VecDeque::new())),
            day_window: Arc::new(RwLock::new(VecDeque::new())),
            burst_queue: Arc::new(RwLock::new(VecDeque::new())),
            last_request: Arc::new(RwLock::new(None)),
        }
    }

    /// 요청 허용 여부 확인 및 대기
    pub async fn wait_for_permission(&self) -> Result<()> {
        loop {
            // 윈도우 정리
            self.cleanup_windows().await;

            // 버스트 제한 확인
            if self.check_burst_limit().await {
                // 요청 허용
                self.record_request().await;
                return Ok(());
            }

            // 다음 요청까지 대기 시간 계산
            let wait_time = self.calculate_wait_time().await;
            
            if wait_time > Duration::from_millis(0) {
                debug!("속도 제한으로 인한 대기: {:?}", wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }

    /// 윈도우 정리 (오래된 요청 제거)
    async fn cleanup_windows(&self) {
        let now = Instant::now();

        // 분 윈도우 정리
        {
            let mut window = self.minute_window.write().await;
            while let Some(&front) = window.front() {
                if now.duration_since(front) >= Duration::from_secs(60) {
                    window.pop_front();
                } else {
                    break;
                }
            }
        }

        // 시간 윈도우 정리
        {
            let mut window = self.hour_window.write().await;
            while let Some(&front) = window.front() {
                if now.duration_since(front) >= Duration::from_secs(3600) {
                    window.pop_front();
                } else {
                    break;
                }
            }
        }

        // 일 윈도우 정리
        {
            let mut window = self.day_window.write().await;
            while let Some(&front) = window.front() {
                if now.duration_since(front) >= Duration::from_secs(86400) {
                    window.pop_front();
                } else {
                    break;
                }
            }
        }

        // 버스트 큐 정리
        {
            let mut queue = self.burst_queue.write().await;
            while let Some(&front) = queue.front() {
                if now.duration_since(front) >= Duration::from_secs(1) {
                    queue.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// 버스트 제한 확인
    async fn check_burst_limit(&self) -> bool {
        let minute_count = self.minute_window.read().await.len() as u32;
        let hour_count = self.hour_window.read().await.len() as u32;
        let day_count = self.day_window.read().await.len() as u32;
        let burst_count = self.burst_queue.read().await.len() as u32;

        minute_count < self.config.requests_per_minute &&
        hour_count < self.config.requests_per_hour &&
        day_count < self.config.requests_per_day &&
        burst_count < self.config.burst_limit
    }

    /// 대기 시간 계산
    async fn calculate_wait_time(&self) -> Duration {
        let now = Instant::now();
        let mut max_wait = Duration::from_millis(0);

        // 분 윈도우 대기 시간
        {
            let window = self.minute_window.read().await;
            if window.len() as u32 >= self.config.requests_per_minute {
                if let Some(&oldest) = window.front() {
                    let elapsed = now.duration_since(oldest);
                    let remaining = Duration::from_secs(60) - elapsed;
                    if remaining > Duration::from_millis(0) {
                        max_wait = max_wait.max(remaining);
                    }
                }
            }
        }

        // 시간 윈도우 대기 시간
        {
            let window = self.hour_window.read().await;
            if window.len() as u32 >= self.config.requests_per_hour {
                if let Some(&oldest) = window.front() {
                    let elapsed = now.duration_since(oldest);
                    let remaining = Duration::from_secs(3600) - elapsed;
                    if remaining > Duration::from_millis(0) {
                        max_wait = max_wait.max(remaining);
                    }
                }
            }
        }

        // 일 윈도우 대기 시간
        {
            let window = self.day_window.read().await;
            if window.len() as u32 >= self.config.requests_per_day {
                if let Some(&oldest) = window.front() {
                    let elapsed = now.duration_since(oldest);
                    let remaining = Duration::from_secs(86400) - elapsed;
                    if remaining > Duration::from_millis(0) {
                        max_wait = max_wait.max(remaining);
                    }
                }
            }
        }

        // 버스트 대기 시간
        {
            let queue = self.burst_queue.read().await;
            if queue.len() as u32 >= self.config.burst_limit {
                if let Some(&oldest) = queue.front() {
                    let elapsed = now.duration_since(oldest);
                    let remaining = Duration::from_secs(1) - elapsed;
                    if remaining > Duration::from_millis(0) {
                        max_wait = max_wait.max(remaining);
                    }
                }
            }
        }

        max_wait
    }

    /// 요청 기록
    async fn record_request(&self) {
        let now = Instant::now();

        // 각 윈도우에 요청 시간 추가
        {
            let mut window = self.minute_window.write().await;
            window.push_back(now);
        }

        {
            let mut window = self.hour_window.write().await;
            window.push_back(now);
        }

        {
            let mut window = self.day_window.write().await;
            window.push_back(now);
        }

        // 버스트 큐에 추가
        {
            let mut queue = self.burst_queue.write().await;
            queue.push_back(now);
        }

        // 마지막 요청 시간 업데이트
        {
            let mut last_request = self.last_request.write().await;
            *last_request = Some(now);
        }
    }

    /// 현재 상태 조회
    pub async fn get_status(&self) -> RateLimitStatus {
        self.cleanup_windows().await;

        let minute_count = self.minute_window.read().await.len() as u32;
        let hour_count = self.hour_window.read().await.len() as u32;
        let day_count = self.day_window.read().await.len() as u32;
        let burst_count = self.burst_queue.read().await.len() as u32;

        let minute_remaining = self.config.requests_per_minute.saturating_sub(minute_count);
        let hour_remaining = self.config.requests_per_hour.saturating_sub(hour_count);
        let day_remaining = self.config.requests_per_day.saturating_sub(day_count);
        let burst_remaining = self.config.burst_limit.saturating_sub(burst_count);

        let now = Instant::now();

        // 다음 리셋 시간 계산
        let next_reset_minute = {
            let window = self.minute_window.read().await;
            if let Some(&oldest) = window.front() {
                let elapsed = now.duration_since(oldest);
                Duration::from_secs(60).saturating_sub(elapsed)
            } else {
                Duration::from_millis(0)
            }
        };

        let next_reset_hour = {
            let window = self.hour_window.read().await;
            if let Some(&oldest) = window.front() {
                let elapsed = now.duration_since(oldest);
                Duration::from_secs(3600).saturating_sub(elapsed)
            } else {
                Duration::from_millis(0)
            }
        };

        let next_reset_day = {
            let window = self.day_window.read().await;
            if let Some(&oldest) = window.front() {
                let elapsed = now.duration_since(oldest);
                Duration::from_secs(86400).saturating_sub(elapsed)
            } else {
                Duration::from_millis(0)
            }
        };

        RateLimitStatus {
            minute_remaining,
            hour_remaining,
            day_remaining,
            burst_remaining,
            next_reset_minute,
            next_reset_hour,
            next_reset_day,
        }
    }

    /// 설정 업데이트
    pub fn update_config(&mut self, new_config: RateLimitConfig) {
        self.config = new_config;
        debug!("속도 제한 설정 업데이트: {:?}", self.config);
    }

    /// 리셋 (모든 윈도우 초기화)
    pub async fn reset(&self) {
        {
            let mut window = self.minute_window.write().await;
            window.clear();
        }

        {
            let mut window = self.hour_window.write().await;
            window.clear();
        }

        {
            let mut window = self.day_window.write().await;
            window.clear();
        }

        {
            let mut queue = self.burst_queue.write().await;
            queue.clear();
        }

        {
            let mut last_request = self.last_request.write().await;
            *last_request = None;
        }

        debug!("속도 제한기 리셋 완료");
    }

    /// 마지막 요청으로부터 경과 시간
    pub async fn time_since_last_request(&self) -> Option<Duration> {
        let last_request = self.last_request.read().await;
        last_request.map(|time| Instant::now().duration_since(time))
    }
}

impl Default for AdvancedRateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig {
            requests_per_minute: 60,
            requests_per_hour: 3600,
            requests_per_day: 86400,
            burst_limit: 10,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let config = RateLimitConfig {
            requests_per_minute: 2,
            requests_per_hour: 10,
            requests_per_day: 100,
            burst_limit: 1,
        };

        let limiter = AdvancedRateLimiter::new(config);

        // 첫 번째 요청은 즉시 허용되어야 함
        let start = Instant::now();
        limiter.wait_for_permission().await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));

        // 두 번째 요청도 즉시 허용되어야 함
        let start = Instant::now();
        limiter.wait_for_permission().await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));

        // 세 번째 요청은 대기해야 함 (분당 2개 제한)
        let start = Instant::now();
        limiter.wait_for_permission().await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(500)); // 최소 1초 대기
    }

    #[tokio::test]
    async fn test_rate_limiter_status() {
        let config = RateLimitConfig {
            requests_per_minute: 5,
            requests_per_hour: 50,
            requests_per_day: 500,
            burst_limit: 2,
        };

        let limiter = AdvancedRateLimiter::new(config);

        // 초기 상태 확인
        let status = limiter.get_status().await;
        assert_eq!(status.minute_remaining, 5);
        assert_eq!(status.hour_remaining, 50);
        assert_eq!(status.day_remaining, 500);
        assert_eq!(status.burst_remaining, 2);

        // 요청 실행
        limiter.wait_for_permission().await.unwrap();

        // 상태 업데이트 확인
        let status = limiter.get_status().await;
        assert_eq!(status.minute_remaining, 4);
        assert_eq!(status.hour_remaining, 49);
        assert_eq!(status.day_remaining, 499);
        assert_eq!(status.burst_remaining, 1);
    }
}

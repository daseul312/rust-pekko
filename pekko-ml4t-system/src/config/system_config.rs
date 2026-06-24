//! 시스템 전체 설정 구조체
//!
//! ML4T 시스템의 전반적인 설정을 담당하는 구조체를 정의합니다.

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;

/// 시스템 전체 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    /// 시스템 이름
    pub name: String,
    /// 시스템 버전
    pub version: String,
    /// 시스템 모드 (development, staging, production)
    pub mode: String,
    /// 로그 레벨
    pub log_level: String,
    /// 데이터베이스 설정
    pub database_url: Option<String>,
    /// Redis 설정
    pub redis_url: Option<String>,
    /// API 설정
    pub api_configs: HashMap<String, serde_json::Value>,
    /// 모니터링 설정
    pub monitoring: MonitoringSystemConfig,
    /// 백업 설정
    pub backup: BackupConfig,
}

/// 모니터링 시스템 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringSystemConfig {
    /// 메트릭 수집 활성화
    pub enable_metrics: bool,
    /// 알림 활성화
    pub enable_alerts: bool,
    /// 메트릭 수집 간격 (초)
    pub collection_interval: u64,
    /// 알림 임계값
    pub alert_thresholds: HashMap<String, f64>,
}

/// 백업 설정
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// 백업 활성화
    pub enable_backup: bool,
    /// 백업 간격 (시간)
    pub backup_interval_hours: u64,
    /// 백업 보관 일수
    pub retention_days: u32,
    /// 백업 디렉토리
    pub backup_directory: String,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            name: "ML4T Trading System".to_string(),
            version: "1.0.0".to_string(),
            mode: "development".to_string(),
            log_level: "info".to_string(),
            database_url: None,
            redis_url: None,
            api_configs: HashMap::new(),
            monitoring: MonitoringSystemConfig {
                enable_metrics: true,
                enable_alerts: true,
                collection_interval: 60,
                alert_thresholds: HashMap::new(),
            },
            backup: BackupConfig {
                enable_backup: false,
                backup_interval_hours: 24,
                retention_days: 30,
                backup_directory: "./backups".to_string(),
            },
        }
    }
}

impl SystemConfig {
    /// 설정 파일에서 로드
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: SystemConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// 환경변수에서 로드
    pub fn load_from_env() -> Result<Self> {
        Ok(Self {
            name: std::env::var("SYSTEM_NAME").unwrap_or_else(|_| "ML4T Trading System".to_string()),
            version: std::env::var("SYSTEM_VERSION").unwrap_or_else(|_| "1.0.0".to_string()),
            mode: std::env::var("SYSTEM_MODE").unwrap_or_else(|_| "development".to_string()),
            log_level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            database_url: std::env::var("DATABASE_URL").ok(),
            redis_url: std::env::var("REDIS_URL").ok(),
            api_configs: HashMap::new(),
            monitoring: MonitoringSystemConfig {
                enable_metrics: std::env::var("ENABLE_METRICS").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true),
                enable_alerts: std::env::var("ENABLE_ALERTS").unwrap_or_else(|_| "true".to_string()).parse().unwrap_or(true),
                collection_interval: std::env::var("COLLECTION_INTERVAL").unwrap_or_else(|_| "60".to_string()).parse().unwrap_or(60),
                alert_thresholds: HashMap::new(),
            },
            backup: BackupConfig {
                enable_backup: std::env::var("ENABLE_BACKUP").unwrap_or_else(|_| "false".to_string()).parse().unwrap_or(false),
                backup_interval_hours: std::env::var("BACKUP_INTERVAL_HOURS").unwrap_or_else(|_| "24".to_string()).parse().unwrap_or(24),
                retention_days: std::env::var("RETENTION_DAYS").unwrap_or_else(|_| "30".to_string()).parse().unwrap_or(30),
                backup_directory: std::env::var("BACKUP_DIRECTORY").unwrap_or_else(|_| "./backups".to_string()),
            },
        })
    }

    /// 개발 모드인지 확인
    pub fn is_development(&self) -> bool {
        self.mode == "development"
    }

    /// 프로덕션 모드인지 확인
    pub fn is_production(&self) -> bool {
        self.mode == "production"
    }
}

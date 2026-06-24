//! 모니터링 모듈
//! 
//! 시스템 성능과 거래 결과를 실시간으로 모니터링합니다.
//! - 성능 메트릭 수집
//! - 알림 시스템
//! - 리포트 생성
//! - 대시보드 데이터

use anyhow::Result;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tracing::{ debug, warn, error};

/// 모니터링 시스템
pub struct MonitoringSystem {
    config: MonitoringConfig,
    metrics_history: Vec<PerformanceMetrics>,
    alerts: Vec<Alert>,
}

/// 모니터링 설정
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    pub update_interval_seconds: u64,
    pub alert_thresholds: AlertThresholds,
    pub retention_days: u32,
    pub report_frequency: ReportFrequency,
}

/// 알림 임계값
#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub drawdown_threshold: f64,
    pub loss_threshold: f64,
    pub volatility_threshold: f64,
    pub model_drift_threshold: f64,
}

/// 리포트 빈도
#[derive(Debug, Clone)]
pub enum ReportFrequency {
    Daily,
    Weekly,
    Monthly,
    RealTime,
}

/// 성능 메트릭스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub portfolio_value: f64,
    pub total_return: f64,
    pub daily_return: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub total_trades: u32,
    pub active_positions: u32,
    pub cash_ratio: f64,
    pub risk_metrics: RiskMetrics,
}

/// 리스크 메트릭스 (간단한 버전)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub var_1d: f64,
    pub var_5d: f64,
    pub volatility: f64,
    pub beta: f64,
}

/// 알림
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub level: AlertLevel,
    pub category: AlertCategory,
    pub message: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub acknowledged: bool,
}

/// 알림 레벨
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

/// 알림 카테고리
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCategory {
    Performance,
    Risk,
    System,
    Trading,
    Model,
}

impl MonitoringSystem {
    pub fn new(config: MonitoringConfig) -> Self {
        Self {
            config,
            metrics_history: Vec::new(),
            alerts: Vec::new(),
        }
    }
    
    /// 성능 메트릭 업데이트
    pub async fn update_metrics(
        &mut self,
        portfolio_value: f64,
        positions: &HashMap<String, crate::trading::Position>,
        trades: &[crate::trading::Order],
        initial_capital: f64,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        
        // 기본 메트릭 계산
        let total_return = (portfolio_value - initial_capital) / initial_capital;
        
        // 일일 수익률 계산 (이전 메트릭과 비교)
        let daily_return = if let Some(last_metrics) = self.metrics_history.last() {
            (portfolio_value - last_metrics.portfolio_value) / last_metrics.portfolio_value
        } else {
            0.0
        };
        
        // 거래 통계
        let total_trades = trades.len() as u32;
        let active_positions = positions.len() as u32;
        
        // 현금 비율
        let positions_value: f64 = positions.values()
            .map(|p| p.current_price * p.quantity.abs() as f64)
            .sum();
        let cash_ratio = if portfolio_value > 0.0 {
            (portfolio_value - positions_value) / portfolio_value
        } else {
            0.0
        };
        
        // 승률 계산 (간단한 구현)
        let win_rate = if total_trades > 0 {
            let winning_trades = trades.iter()
                .filter(|trade| matches!(trade.status, crate::trading::OrderStatus::Filled))
                .count() as f64;
            winning_trades / total_trades as f64
        } else {
            0.0
        };
        
        // 리스크 메트릭 (더미 값)
        let risk_metrics = RiskMetrics {
            var_1d: -0.02, // 2% VaR
            var_5d: -0.05, // 5% VaR
            volatility: 0.15, // 15% 변동성
            beta: 1.0, // 베타 1.0
        };
        
        // 샤프 비율 (더미 계산)
        let sharpe_ratio = if risk_metrics.volatility > 0.0 {
            daily_return * 252.0 / (risk_metrics.volatility * (252.0_f64).sqrt())
        } else {
            0.0
        };
        
        // 최대 드로우다운 (더미 계산)
        let max_drawdown = 0.05; // 5%
        
        let metrics = PerformanceMetrics {
            timestamp: now,
            portfolio_value,
            total_return,
            daily_return,
            sharpe_ratio,
            max_drawdown,
            win_rate,
            total_trades,
            active_positions,
            cash_ratio,
            risk_metrics,
        };
        
        self.metrics_history.push(metrics);
        
        // 메트릭 히스토리 크기 제한
        self.cleanup_old_metrics().await?;
        
        // 알림 확인
        if let Some(last_metric) = self.metrics_history.last().cloned() {
            self.check_alerts(&last_metric).await?;
        }
        Ok(())
    }
    
    /// 알림 확인
    async fn check_alerts(&mut self, metrics: &PerformanceMetrics) -> Result<()> {
        // 드로우다운 알림
        if metrics.max_drawdown > self.config.alert_thresholds.drawdown_threshold {
            self.create_alert(
                AlertLevel::Warning,
                AlertCategory::Performance,
                format!("최대 드로우다운 초과: {:.2}%", metrics.max_drawdown * 100.0),
            ).await?;
        }
        
        // 손실 알림
        if metrics.total_return < -self.config.alert_thresholds.loss_threshold {
            self.create_alert(
                AlertLevel::Error,
                AlertCategory::Performance,
                format!("총 손실 초과: {:.2}%", metrics.total_return * 100.0),
            ).await?;
        }
        
        // 변동성 알림
        if metrics.risk_metrics.volatility > self.config.alert_thresholds.volatility_threshold {
            self.create_alert(
                AlertLevel::Warning,
                AlertCategory::Risk,
                format!("변동성 초과: {:.2}%", metrics.risk_metrics.volatility * 100.0),
            ).await?;
        }
        
        // 샤프 비율 알림
        if metrics.sharpe_ratio < 0.5 {
            self.create_alert(
                AlertLevel::Warning,
                AlertCategory::Performance,
                format!("샤프 비율 낮음: {:.2}", metrics.sharpe_ratio),
            ).await?;
        }
        
        Ok(())
    }
    
    /// 알림 생성
    async fn create_alert(
        &mut self,
        level: AlertLevel,
        category: AlertCategory,
        message: String,
    ) -> Result<()> {
        let alert = Alert {
            id: format!("ALERT_{}", chrono::Utc::now().timestamp_millis()),
            level,
            category,
            message,
            timestamp: chrono::Utc::now(),
            acknowledged: false,
        };
        
        match alert.level {
            AlertLevel::Info => (),
            AlertLevel::Warning => warn!("경고: {}", alert.message),
            AlertLevel::Error => error!("오류: {}", alert.message),
            AlertLevel::Critical => error!("치명적: {}", alert.message),
        }
        
        self.alerts.push(alert);
        
        Ok(())
    }
    
    /// 오래된 메트릭 정리
    async fn cleanup_old_metrics(&mut self) -> Result<()> {
        let cutoff_date = chrono::Utc::now() - chrono::Duration::days(self.config.retention_days as i64);
        
        self.metrics_history.retain(|metrics| metrics.timestamp > cutoff_date);
        
        Ok(())
    }
    
    /// 성능 리포트 생성
    pub async fn generate_performance_report(&self) -> Result<String> {
        if self.metrics_history.is_empty() {
            return Ok("성능 데이터가 없습니다.".to_string());
        }
        
        let latest = self.metrics_history.last().unwrap();
        let mut report = String::new();
        
        report.push_str("# 성능 리포트\n\n");
        report.push_str(&format!("**생성 시간**: {}\n\n", latest.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
        
        report.push_str("## 📊 포트폴리오 현황\n\n");
        report.push_str(&format!("- **포트폴리오 가치**: {:.0}원\n", latest.portfolio_value));
        report.push_str(&format!("- **총 수익률**: {:.2}%\n", latest.total_return * 100.0));
        report.push_str(&format!("- **일일 수익률**: {:.2}%\n", latest.daily_return * 100.0));
        report.push_str(&format!("- **활성 포지션**: {}개\n", latest.active_positions));
        report.push_str(&format!("- **현금 비율**: {:.2}%\n\n", latest.cash_ratio * 100.0));
        
        report.push_str("## 📈 성능 지표\n\n");
        report.push_str(&format!("- **샤프 비율**: {:.2}\n", latest.sharpe_ratio));
        report.push_str(&format!("- **최대 드로우다운**: {:.2}%\n", latest.max_drawdown * 100.0));
        report.push_str(&format!("- **승률**: {:.2}%\n", latest.win_rate * 100.0));
        report.push_str(&format!("- **총 거래 수**: {}\n\n", latest.total_trades));
        
        report.push_str("## ⚠️ 리스크 지표\n\n");
        report.push_str(&format!("- **1일 VaR**: {:.2}%\n", latest.risk_metrics.var_1d * 100.0));
        report.push_str(&format!("- **5일 VaR**: {:.2}%\n", latest.risk_metrics.var_5d * 100.0));
        report.push_str(&format!("- **변동성**: {:.2}%\n", latest.risk_metrics.volatility * 100.0));
        report.push_str(&format!("- **베타**: {:.2}\n\n", latest.risk_metrics.beta));
        
        // 최근 알림
        let recent_alerts: Vec<&Alert> = self.alerts.iter()
            .filter(|alert| alert.timestamp > chrono::Utc::now() - chrono::Duration::hours(24))
            .collect();
        
        if !recent_alerts.is_empty() {
            report.push_str("## 🚨 최근 알림\n\n");
            for alert in recent_alerts {
                let level_icon = match alert.level {
                    AlertLevel::Info => "ℹ️",
                    AlertLevel::Warning => "⚠️",
                    AlertLevel::Error => "❌",
                    AlertLevel::Critical => "🚨",
                };
                
                report.push_str(&format!("{} **{}**: {}\n", 
                    level_icon, 
                    format!("{:?}", alert.level), 
                    alert.message
                ));
            }
        }
        
        Ok(report)
    }
    
    /// 대시보드 데이터 생성
    pub async fn get_dashboard_data(&self) -> Result<DashboardData> {
        let latest_metrics = self.metrics_history.last().cloned();
        
        let recent_alerts: Vec<Alert> = self.alerts.iter()
            .filter(|alert| alert.timestamp > chrono::Utc::now() - chrono::Duration::hours(24))
            .cloned()
            .collect();
        
        let performance_trend = if self.metrics_history.len() >= 7 {
            let recent_returns: Vec<f64> = self.metrics_history.iter()
                .rev()
                .take(7)
                .map(|m| m.daily_return)
                .collect();
            
            let trend = if recent_returns.iter().sum::<f64>() > 0.0 {
                "상승"
            } else {
                "하락"
            };
            
            Some(trend.to_string())
        } else {
            None
        };
        
        Ok(DashboardData {
            latest_metrics,
            recent_alerts,
            performance_trend,
            system_status: "정상".to_string(),
            last_update: chrono::Utc::now(),
        })
    }
    
    /// 알림 승인
    pub async fn acknowledge_alert(&mut self, alert_id: &str) -> Result<()> {
        if let Some(alert) = self.alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
        }
        Ok(())
    }
    
    /// 모든 알림 조회
    pub fn get_all_alerts(&self) -> &Vec<Alert> {
        &self.alerts
    }
    
    /// 미승인 알림 조회
    pub fn get_unacknowledged_alerts(&self) -> Vec<&Alert> {
        self.alerts.iter().filter(|alert| !alert.acknowledged).collect()
    }
    
    /// 메트릭 히스토리 조회
    pub fn get_metrics_history(&self) -> &Vec<PerformanceMetrics> {
        &self.metrics_history
    }
}

/// 대시보드 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardData {
    pub latest_metrics: Option<PerformanceMetrics>,
    pub recent_alerts: Vec<Alert>,
    pub performance_trend: Option<String>,
    pub system_status: String,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

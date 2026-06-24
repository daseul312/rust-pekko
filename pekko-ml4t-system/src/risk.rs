//! 리스크 관리 모듈
//! 
//! 포트폴리오 리스크를 관리하고 모니터링합니다.
//! - VaR (Value at Risk) 계산
//! - 포트폴리오 최적화
//! - 리스크 한도 관리
//! - 스트레스 테스트

use anyhow::Result;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tracing::{ debug, warn, error};

/// 리스크 관리 시스템
pub struct RiskManager {
    config: RiskConfig,
    risk_metrics: RiskMetrics,
}

/// 리스크 설정
#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub max_portfolio_risk: f64,
    pub max_position_size: f64,
    pub var_confidence: f64,
    pub max_drawdown: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
}

/// 리스크 메트릭스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub portfolio_var: f64,
    pub portfolio_es: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub volatility: f64,
    pub beta: f64,
}

/// 포트폴리오 리스크 분석 결과
#[derive(Debug, Clone)]
pub struct RiskAnalysis {
    pub metrics: RiskMetrics,
    pub risk_alerts: Vec<RiskAlert>,
    pub recommendations: Vec<String>,
}

/// 리스크 알림
#[derive(Debug, Clone)]
pub struct RiskAlert {
    pub level: RiskLevel,
    pub message: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 리스크 레벨
#[derive(Debug, Clone)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl RiskManager {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            risk_metrics: RiskMetrics {
                portfolio_var: 0.0,
                portfolio_es: 0.0,
                max_drawdown: 0.0,
                sharpe_ratio: 0.0,
                sortino_ratio: 0.0,
                calmar_ratio: 0.0,
                volatility: 0.0,
                beta: 0.0,
            },
        }
    }
    
    /// 포트폴리오 리스크 분석
    pub async fn analyze_portfolio_risk(
        &mut self,
        positions: &HashMap<String, crate::trading::Position>,
        returns_history: &[f64],
        benchmark_returns: &[f64],
    ) -> Result<RiskAnalysis> {
        // VaR 계산
        self.risk_metrics.portfolio_var = self.calculate_var(returns_history, self.config.var_confidence).await?;
        
        // Expected Shortfall 계산
        self.risk_metrics.portfolio_es = self.calculate_expected_shortfall(returns_history, self.config.var_confidence).await?;
        
        // 최대 드로우다운 계산
        self.risk_metrics.max_drawdown = self.calculate_max_drawdown(returns_history).await?;
        
        // 샤프 비율 계산
        self.risk_metrics.sharpe_ratio = self.calculate_sharpe_ratio(returns_history).await?;
        
        // 소르티노 비율 계산
        self.risk_metrics.sortino_ratio = self.calculate_sortino_ratio(returns_history).await?;
        
        // 칼마 비율 계산
        self.risk_metrics.calmar_ratio = self.calculate_calmar_ratio(returns_history).await?;
        
        // 변동성 계산
        self.risk_metrics.volatility = self.calculate_volatility(returns_history).await?;
        
        // 베타 계산
        self.risk_metrics.beta = self.calculate_beta(returns_history, benchmark_returns).await?;
        
        // 리스크 알림 생성
        let risk_alerts = self.generate_risk_alerts(positions).await?;
        
        // 권장사항 생성
        let recommendations = self.generate_recommendations(&risk_alerts).await?;
        Ok(RiskAnalysis {
            metrics: self.risk_metrics.clone(),
            risk_alerts,
            recommendations,
        })
    }
    
    /// VaR 계산
    async fn calculate_var(&self, returns: &[f64], confidence: f64) -> Result<f64> {
        if returns.is_empty() {
            return Ok(0.0);
        }
        
        let mut sorted_returns = returns.to_vec();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let index = ((1.0 - confidence) * sorted_returns.len() as f64) as usize;
        let var = sorted_returns.get(index).copied().unwrap_or(0.0);
        
        Ok(var)
    }
    
    /// Expected Shortfall 계산
    async fn calculate_expected_shortfall(&self, returns: &[f64], confidence: f64) -> Result<f64> {
        let var = self.calculate_var(returns, confidence).await?;
        
        let tail_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r <= var)
            .cloned()
            .collect();
        
        if tail_returns.is_empty() {
            return Ok(var);
        }
        
        let es = tail_returns.iter().sum::<f64>() / tail_returns.len() as f64;
        Ok(es)
    }
    
    /// 최대 드로우다운 계산
    async fn calculate_max_drawdown(&self, returns: &[f64]) -> Result<f64> {
        if returns.is_empty() {
            return Ok(0.0);
        }
        
        let mut cumulative_returns = vec![1.0];
        for &ret in returns {
            let last_value = cumulative_returns.last().unwrap();
            cumulative_returns.push(last_value * (1.0 + ret));
        }
        
        let mut peak = 1.0;
        let mut max_dd: f64 = 0.0;
        
        for &cumulative_return in &cumulative_returns {
            if cumulative_return > peak {
                peak = cumulative_return;
            }
            let drawdown = (peak - cumulative_return) / peak;
            max_dd = max_dd.max(drawdown);
        }
        
        Ok(max_dd)
    }
    
    /// 샤프 비율 계산
    async fn calculate_sharpe_ratio(&self, returns: &[f64]) -> Result<f64> {
        if returns.is_empty() {
            return Ok(0.0);
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        let volatility = variance.sqrt();
        
        if volatility == 0.0 {
            return Ok(0.0);
        }
        
        // 연율화 (252 거래일 가정)
        let annualized_return = mean_return * 252.0;
        let annualized_volatility = volatility * (252.0_f64).sqrt();
        
        Ok(annualized_return / annualized_volatility)
    }
    
    /// 소르티노 비율 계산
    async fn calculate_sortino_ratio(&self, returns: &[f64]) -> Result<f64> {
        if returns.is_empty() {
            return Ok(0.0);
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let negative_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();
        
        if negative_returns.is_empty() {
            return Ok(f64::INFINITY);
        }
        
        let downside_variance = negative_returns.iter()
            .map(|r| r.powi(2))
            .sum::<f64>() / negative_returns.len() as f64;
        let downside_volatility = downside_variance.sqrt();
        
        if downside_volatility == 0.0 {
            return Ok(0.0);
        }
        
        // 연율화
        let annualized_return = mean_return * 252.0;
        let annualized_downside_volatility = downside_volatility * (252.0_f64).sqrt();
        
        Ok(annualized_return / annualized_downside_volatility)
    }
    
    /// 칼마 비율 계산
    async fn calculate_calmar_ratio(&self, returns: &[f64]) -> Result<f64> {
        let max_dd = self.calculate_max_drawdown(returns).await?;
        if max_dd == 0.0 {
            return Ok(f64::INFINITY);
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let annualized_return = mean_return * 252.0;
        
        Ok(annualized_return / max_dd)
    }
    
    /// 변동성 계산
    async fn calculate_volatility(&self, returns: &[f64]) -> Result<f64> {
        if returns.is_empty() {
            return Ok(0.0);
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        // 연율화
        Ok(variance.sqrt() * (252.0_f64).sqrt())
    }
    
    /// 베타 계산
    async fn calculate_beta(&self, portfolio_returns: &[f64], benchmark_returns: &[f64]) -> Result<f64> {
        if portfolio_returns.len() != benchmark_returns.len() || portfolio_returns.is_empty() {
            return Ok(1.0);
        }
        
        let portfolio_mean = portfolio_returns.iter().sum::<f64>() / portfolio_returns.len() as f64;
        let benchmark_mean = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;
        
        let covariance = portfolio_returns.iter()
            .zip(benchmark_returns.iter())
            .map(|(p, b)| (p - portfolio_mean) * (b - benchmark_mean))
            .sum::<f64>() / portfolio_returns.len() as f64;
        
        let benchmark_variance = benchmark_returns.iter()
            .map(|b| (b - benchmark_mean).powi(2))
            .sum::<f64>() / benchmark_returns.len() as f64;
        
        if benchmark_variance == 0.0 {
            return Ok(1.0);
        }
        
        Ok(covariance / benchmark_variance)
    }
    
    /// 리스크 알림 생성
    async fn generate_risk_alerts(&self, positions: &HashMap<String, crate::trading::Position>) -> Result<Vec<RiskAlert>> {
        let mut alerts = Vec::new();
        
        // VaR 알림
        if self.risk_metrics.portfolio_var < -self.config.max_portfolio_risk {
            alerts.push(RiskAlert {
                level: RiskLevel::High,
                message: format!("포트폴리오 VaR 초과: {:.2}%", self.risk_metrics.portfolio_var * 100.0),
                timestamp: chrono::Utc::now(),
            });
        }
        
        // 최대 드로우다운 알림
        if self.risk_metrics.max_drawdown > self.config.max_drawdown {
            alerts.push(RiskAlert {
                level: RiskLevel::Critical,
                message: format!("최대 드로우다운 초과: {:.2}%", self.risk_metrics.max_drawdown * 100.0),
                timestamp: chrono::Utc::now(),
            });
        }
        
        // 포지션 크기 알림
        for (symbol, position) in positions {
            let position_value = position.current_price * position.quantity.abs() as f64;
            let portfolio_value = positions.values()
                .map(|p| p.current_price * p.quantity.abs() as f64)
                .sum::<f64>();
            
            if portfolio_value > 0.0 {
                let position_ratio = position_value / portfolio_value;
                if position_ratio > self.config.max_position_size {
                    alerts.push(RiskAlert {
                        level: RiskLevel::Medium,
                        message: format!("{} 포지션 크기 초과: {:.2}%", symbol, position_ratio * 100.0),
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
        }
        
        Ok(alerts)
    }
    
    /// 권장사항 생성
    async fn generate_recommendations(&self, alerts: &[RiskAlert]) -> Result<Vec<String>> {
        let mut recommendations = Vec::new();
        
        for alert in alerts {
            match alert.level {
                RiskLevel::Critical => {
                    recommendations.push("즉시 포지션 축소를 고려하세요".to_string());
                }
                RiskLevel::High => {
                    recommendations.push("리스크 관리 규칙을 강화하세요".to_string());
                }
                RiskLevel::Medium => {
                    recommendations.push("포지션 다각화를 고려하세요".to_string());
                }
                RiskLevel::Low => {
                    recommendations.push("현재 리스크 수준을 모니터링하세요".to_string());
                }
            }
        }
        
        if recommendations.is_empty() {
            recommendations.push("현재 리스크 수준이 적절합니다".to_string());
        }
        
        Ok(recommendations)
    }
    
    /// 포트폴리오 최적화 제안
    pub async fn suggest_portfolio_optimization(
        &self,
        positions: &HashMap<String, crate::trading::Position>,
    ) -> Result<HashMap<String, f64>> {
        let mut suggestions = HashMap::new();
        
        for (symbol, position) in positions {
            let position_value = position.current_price * position.quantity.abs() as f64;
            let portfolio_value = positions.values()
                .map(|p| p.current_price * p.quantity.abs() as f64)
                .sum::<f64>();
            
            if portfolio_value > 0.0 {
                let current_weight = position_value / portfolio_value;
                let optimal_weight = 1.0 / positions.len() as f64; // 동일 가중치
                let adjustment = optimal_weight - current_weight;
                
                if adjustment.abs() > 0.05 { // 5% 이상 차이
                    suggestions.insert(symbol.clone(), adjustment);
                }
            }
        }
        
        Ok(suggestions)
    }
}

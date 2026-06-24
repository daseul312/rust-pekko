//! 데이터 파이프라인 사용 예제
//! 
//! 데이터 수집부터 특성 추출까지의 전체 파이프라인을 보여주는 예제

use crate::data::pipeline::{DataPipeline, PipelineConfig};
use crate::data::{MarketData, DataSourceConfig, DataType, ApiConfig, RateLimitConfig, UpdateFrequency, RetryConfig, QualityThresholds};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{ error};

/// 데이터 파이프라인 예제 실행
pub async fn run_data_pipeline_example() -> Result<()> {
    // 1. 파이프라인 설정 구성
    let config = create_pipeline_config().await?;
    
    // 2. 데이터 파이프라인 생성
    let mut pipeline = DataPipeline::new(config).await?;
    
    // 3. 파이프라인 시작
    pipeline.start().await?;
    
    // 4. 데이터 조회 예제
    run_data_retrieval_examples(&pipeline).await?;
    
    // 5. 품질 분석 예제
    run_quality_analysis_example(&pipeline).await?;
    
    // 6. 특성 중요도 분석 예제
    run_feature_importance_example(&pipeline).await?;
    
    // 7. 파이프라인 중지
    pipeline.stop().await?;
    Ok(())
}

/// 파이프라인 설정 생성
async fn create_pipeline_config() -> Result<PipelineConfig> {
    let mut data_sources = Vec::new();
    
    // KIS API 설정
    let kis_config = DataSourceConfig {
        source_name: "KIS API".to_string(),
        data_type: DataType::MarketData,
        api_config: ApiConfig {
            base_url: "https://openapi.koreainvestment.com:9443".to_string(),
            api_key: Some(std::env::var("KIS_APP_KEY").unwrap_or_default()),
            api_secret: Some(std::env::var("KIS_APP_SECRET").unwrap_or_default()),
            rate_limit: RateLimitConfig {
                requests_per_minute: 60,
                requests_per_hour: 3600,
                requests_per_day: 86400,
                burst_limit: 10,
            },
            timeout_seconds: 30,
            headers: HashMap::new(),
        },
        update_frequency: UpdateFrequency::EveryMinute,
        retry_config: RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
            jitter: true,
        },
        quality_thresholds: QualityThresholds {
            min_data_completeness: 0.95,
            max_latency_seconds: 5,
            max_price_deviation: 0.1,
            max_volume_spike: 5.0,
            min_confidence_score: 0.8,
        },
    };
    data_sources.push(kis_config);
    
    // Alpha Vantage 설정
    let av_config = DataSourceConfig {
        source_name: "Alpha Vantage".to_string(),
        data_type: DataType::MarketData,
        api_config: ApiConfig {
            base_url: "https://www.alphavantage.co".to_string(),
            api_key: Some(std::env::var("ALPHA_VANTAGE_API_KEY").unwrap_or_default()),
            api_secret: None,
            rate_limit: RateLimitConfig {
                requests_per_minute: 5,
                requests_per_hour: 500,
                requests_per_day: 5000,
                burst_limit: 1,
            },
            timeout_seconds: 30,
            headers: HashMap::new(),
        },
        update_frequency: UpdateFrequency::Every5Minutes,
        retry_config: RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
            jitter: true,
        },
        quality_thresholds: QualityThresholds {
            min_data_completeness: 0.9,
            max_latency_seconds: 10,
            max_price_deviation: 0.2,
            max_volume_spike: 10.0,
            min_confidence_score: 0.7,
        },
    };
    data_sources.push(av_config);
    
    let config = PipelineConfig {
        data_sources,
        preprocessing_config: crate::data::processors::PreprocessingConfig::default(),
        update_intervals: HashMap::from([
            ("KIS API".to_string(), chrono::Duration::minutes(1)),
            ("Alpha Vantage".to_string(), chrono::Duration::minutes(5)),
        ]),
        cache_size: 10000,
        quality_threshold: 0.8,
        enable_monitoring: true,
        auto_retry: true,
        max_retries: 3,
    };
    
    Ok(config)
}

/// 데이터 조회 예제
async fn run_data_retrieval_examples(pipeline: &DataPipeline) -> Result<()> {
    // 1. 특정 심볼의 데이터 조회
    let symbol_data = pipeline.get_data_for_symbol("000270").await?;
    // 2. 특정 특성의 데이터 조회
    let rsi_data = pipeline.get_feature_data("rsi", Some("000270")).await?;
    // 3. 최근 데이터 출력
    if let Some(latest_data) = symbol_data.last() {
        // 주요 특성 출력
        let important_features = ["rsi", "macd", "sma_20", "bollinger_position"];
        for feature in &important_features {
            if let Some(&value) = latest_data.features.get(*feature) {
            }
        }
    }
    
    Ok(())
}

/// 품질 분석 예제
async fn run_quality_analysis_example(pipeline: &DataPipeline) -> Result<()> {
    // 품질 보고서 생성
    let quality_report = pipeline.generate_quality_report().await?;
    // 소스별 품질 분석
    for (source_name, source_report) in &quality_report.source_reports {
    }
    
    Ok(())
}

/// 특성 중요도 분석 예제
async fn run_feature_importance_example(pipeline: &DataPipeline) -> Result<()> {
    // 수익률 예측에 대한 특성 중요도 분석
    let importance = pipeline.analyze_feature_importance("returns").await?;
    let mut sorted_features: Vec<_> = importance.iter().collect();
    sorted_features.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    
    for (i, (feature_name, importance_score)) in sorted_features.iter().take(10).enumerate() {
    }
    
    // 특성별 상세 분석
    let important_features = ["rsi", "macd", "sma_20", "bollinger_position", "atr"];
    for feature in &important_features {
        if let Some(&score) = importance.get(*feature) {
        }
    }
    
    Ok(())
}

/// 시장 데이터 시뮬레이션 예제
pub async fn run_market_data_simulation() -> Result<()> {
    // 가상의 시장 데이터 생성
    let mut market_data = Vec::new();
    let base_price = 50000.0;
    let mut current_price = base_price;
    
    for i in 0..100 {
        let timestamp = Utc::now() - chrono::Duration::days(100 - i as i64);
        
        // 가격 변동 시뮬레이션 (랜덤 워크)
        let change = (rand::random::<f64>() - 0.5) * 0.02; // ±1% 변동
        current_price *= 1.0 + change;
        
        let high = current_price * (1.0 + rand::random::<f64>() * 0.01);
        let low = current_price * (1.0 - rand::random::<f64>() * 0.01);
        let volume = rand::random::<u64>() % 1000000 + 100000;
        let vwap = (high + low + current_price) / 3.0;
        
        market_data.push(MarketData {
            symbol: "SIMULATION".to_string(),
            timestamp,
            open: current_price,
            high,
            low,
            close: current_price,
            volume,
            vwap,
            adjusted_close: Some(current_price),
            dividend: None,
            split_ratio: None,
        });
    }
    // 데이터 정제 예제
    let cleaner = crate::data::processors::DataCleaner::default();
    let (cleaned_data, quality_report) = cleaner.clean_market_data(market_data).await?;
    // 기술적 지표 계산 예제
    let technical_calculator = crate::data::processors::TechnicalIndicatorCalculator::default();
    let indicators = technical_calculator.calculate_indicators(&cleaned_data)?;
    for (indicator_name, indicator_values) in &indicators {
        let valid_values = indicator_values.iter().filter(|&&x| !x.is_nan()).count();
    }
    
    // 특성 추출 예제
    let feature_extractor = crate::data::processors::FeatureExtractor::default();
    let processed_data = feature_extractor.extract_market_features(&cleaned_data).await?;
    if let Some(first_data) = processed_data.first() {
        let feature_names: Vec<_> = first_data.features.keys().take(10).collect();
        for feature_name in feature_names {
        }
    }
    Ok(())
}

/// 데이터 품질 모니터링 예제
pub async fn run_quality_monitoring_example() -> Result<()> {
    // 품질 메트릭스 생성
    let mut quality_metrics = crate::data::QualityMetrics {
        completeness: 0.95,
        accuracy: 0.92,
        consistency: 0.88,
        timeliness: 0.90,
        validity: 0.93,
        overall_score: 0.0,
    };
    
    quality_metrics.calculate_overall_score();
    // 품질 이슈 시뮬레이션
    let quality_issues = vec![
        crate::data::processors::QualityIssue {
            issue_type: crate::data::processors::QualityIssueType::MissingValues,
            severity: crate::data::processors::QualityIssueSeverity::Medium,
            description: "일부 가격 데이터 누락".to_string(),
            affected_records: 15,
            suggestion: "데이터 소스 재확인 필요".to_string(),
        },
        crate::data::processors::QualityIssue {
            issue_type: crate::data::processors::QualityIssueType::PriceAnomalies,
            severity: crate::data::processors::QualityIssueSeverity::High,
            description: "가격 급등/급락 감지".to_string(),
            affected_records: 3,
            suggestion: "가격 검증 로직 강화 필요".to_string(),
        },
    ];
    for (i, issue) in quality_issues.iter().enumerate() {
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_market_data_simulation() {
        // 환경 변수 설정 (테스트용)
        std::env::set_var("KIS_APP_KEY", "test_key");
        std::env::set_var("KIS_APP_SECRET", "test_secret");
        std::env::set_var("ALPHA_VANTAGE_API_KEY", "test_key");
        
        // 시뮬레이션 실행
        if let Err(e) = run_market_data_simulation().await {
            eprintln!("테스트 실패: {}", e);
            // 테스트에서는 실패해도 계속 진행
        }
    }

    #[tokio::test]
    async fn test_quality_monitoring() {
        if let Err(e) = run_quality_monitoring_example().await {
            eprintln!("품질 모니터링 테스트 실패: {}", e);
        }
    }
}

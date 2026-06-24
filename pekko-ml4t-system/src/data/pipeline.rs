//! 데이터 파이프라인 메인 모듈
//! 
//! 전체 데이터 수집부터 특성 추출까지의 파이프라인을 관리합니다:
//! - 데이터 수집 오케스트레이션
//! - 데이터 정제 및 전처리
//! - 특성 추출 및 엔지니어링
//! - 데이터 품질 관리
//! - 파이프라인 모니터링

use super::{MarketData, NewsData, AlternativeData, DataSourceConfig, PipelineStatus, CollectionStats, QualityMetrics};
use crate::data::collectors::{DataCollector, CollectParams};

// 임시 더미 collector
struct DummyCollector;

impl DataCollector for DummyCollector {
    type DataType = MarketData;
    
    fn initialize(&mut self, _config: DataSourceConfig) -> Result<()> {
        Ok(())
    }
    
    fn collect(&self, _params: CollectParams) -> Result<Self::DataType> {
        // 더미 MarketData 반환
        Ok(MarketData {
            symbol: "DUMMY".to_string(),
            timestamp: chrono::Utc::now(),
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 102.0,
            volume: 1000,
            vwap: 101.0,
            adjusted_close: Some(102.0),
            dividend: None,
            split_ratio: None,
        })
    }
    
    fn collect_batch(&self, _params: Vec<CollectParams>) -> Result<Vec<Self::DataType>> {
        Ok(vec![])
    }
    
    fn health_check(&self) -> Result<crate::data::collectors::HealthStatus> {
        Ok(crate::data::collectors::HealthStatus::Healthy)
    }
    
    fn get_stats(&self) -> crate::data::collectors::CollectionStats {
        crate::data::collectors::CollectionStats {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_records: 0,
            average_latency_ms: 0.0,
            last_success: None,
            last_error: None,
        }
    }
}
use crate::data::processors::{PreprocessingConfig, DataCleaner, FeatureExtractor, ProcessedData, PreprocessingResult};
use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, interval};
use tracing::{ warn, error, debug};

/// 데이터 파이프라인
pub struct DataPipeline {
    config: PipelineConfig,
    collectors: HashMap<String, Box<dyn DataCollector<DataType = MarketData> + Send + Sync>>,
    cleaners: HashMap<String, DataCleaner>,
    feature_extractors: HashMap<String, FeatureExtractor>,
    processed_data_cache: Arc<RwLock<HashMap<String, Vec<ProcessedData>>>>,
    status: Arc<RwLock<PipelineStatus>>,
    stats: Arc<RwLock<PipelineStatistics>>,
}

/// 파이프라인 설정
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub data_sources: Vec<DataSourceConfig>,
    pub preprocessing_config: PreprocessingConfig,
    pub update_intervals: HashMap<String, Duration>,
    pub cache_size: usize,
    pub quality_threshold: f64,
    pub enable_monitoring: bool,
    pub auto_retry: bool,
    pub max_retries: u32,
}

/// 파이프라인 통계
#[derive(Debug, Clone)]
pub struct PipelineStatistics {
    pub total_collections: u64,
    pub successful_collections: u64,
    pub failed_collections: u64,
    pub total_processed_records: u64,
    pub average_quality_score: f64,
    pub last_update: Option<DateTime<Utc>>,
    pub collection_stats: HashMap<String, CollectionStats>,
    pub processing_times: HashMap<String, Duration>,
}

impl DataPipeline {
    /// 새로운 데이터 파이프라인 생성
    pub async fn new(config: PipelineConfig) -> Result<Self> {
        let mut collectors = HashMap::new();
        let mut cleaners = HashMap::new();
        let mut feature_extractors = HashMap::new();
        
        // 수집기 초기화
        for source_config in &config.data_sources {
            // 임시로 더미 collector 사용
            let collector: Box<dyn DataCollector<DataType = MarketData> + Send + Sync> = match source_config.data_type {
                crate::data::DataType::MarketData => {
                    // BaseCollector 대신 더미 구현 사용
                    Box::new(DummyCollector)
                }
                _ => {
                    warn!("지원하지 않는 데이터 타입: {:?}", source_config.data_type);
                    continue;
                }
            };
            
            collectors.insert(source_config.source_name.clone(), collector);
            
            // 정제기 초기화
            cleaners.insert(
                source_config.source_name.clone(),
                DataCleaner::new(config.preprocessing_config.cleaning.clone())
            );
            
            // 특성 추출기 초기화
            feature_extractors.insert(
                source_config.source_name.clone(),
                FeatureExtractor::new(config.preprocessing_config.feature_extraction.clone())
            );
        }
        
        let pipeline = Self {
            config,
            collectors,
            cleaners,
            feature_extractors,
            processed_data_cache: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(PipelineStatus::Initializing)),
            stats: Arc::new(RwLock::new(PipelineStatistics {
                total_collections: 0,
                successful_collections: 0,
                failed_collections: 0,
                total_processed_records: 0,
                average_quality_score: 0.0,
                last_update: None,
                collection_stats: HashMap::new(),
                processing_times: HashMap::new(),
            })),
        };
        Ok(pipeline)
    }

    /// 파이프라인 시작
    pub async fn start(&self) -> Result<()> {
        // 상태를 Running으로 변경
        {
            let mut status = self.status.write().await;
            *status = PipelineStatus::Running;
        }
        
        // 초기 데이터 수집
        self.collect_all_data().await?;
        
        // 주기적 업데이트 시작
        if self.config.enable_monitoring {
            self.start_monitoring().await?;
        }
        
        Ok(())
    }

    /// 파이프라인 중지
    pub async fn stop(&self) -> Result<()> {
        let mut status = self.status.write().await;
        *status = PipelineStatus::Stopped;
        
        Ok(())
    }

    /// 모든 데이터 수집
    pub async fn collect_all_data(&self) -> Result<()> {
        let mut all_processed_data = HashMap::new();
        
        for (source_name, collector) in &self.collectors {
            match self.collect_and_process_data(source_name, collector).await {
                Ok(processed_data) => {
                    all_processed_data.insert(source_name.clone(), processed_data);
                    
                    // 통계 업데이트
                    let mut stats = self.stats.write().await;
                    stats.successful_collections += 1;
                    stats.last_update = Some(Utc::now());
                }
                Err(e) => {
                    error!("{} 데이터 수집 실패: {}", source_name, e);
                    
                    let mut stats = self.stats.write().await;
                    stats.failed_collections += 1;
                    
                    if self.config.auto_retry {
                        warn!("{} 자동 재시도 예정", source_name);
                        // TODO: 재시도 로직 구현
                    }
                }
            }
            
            // 통계 업데이트
            let mut stats = self.stats.write().await;
            stats.total_collections += 1;
            // 통계는 일단 스킵하고 나중에 구현
            // let collector_stats = collector.get_stats();
            // stats.collection_stats.insert(source_name.clone(), collector_stats);
        }
        
        // 캐시 업데이트
        {
            let mut cache = self.processed_data_cache.write().await;
            *cache = all_processed_data;
        }
        Ok(())
    }

    /// 개별 데이터 소스 수집 및 처리
    async fn collect_and_process_data(
        &self,
        source_name: &str,
        collector: &Box<dyn DataCollector<DataType = MarketData> + Send + Sync>,
    ) -> Result<Vec<ProcessedData>> {
        let start_time = std::time::Instant::now();
        
        // 1. 데이터 수집
        let collect_params = self.create_collect_params(source_name).await?;
        let raw_data = collector.collect_batch(vec![collect_params])?;
        
        debug!("{}에서 {}개 레코드 수집됨", source_name, raw_data.len());
        
        // 2. 데이터 정제
        let cleaner = self.cleaners.get(source_name)
            .ok_or_else(|| anyhow::anyhow!("정제기를 찾을 수 없음: {}", source_name))?;
        
        let (cleaned_data, quality_report) = cleaner.clean_market_data(raw_data).await?;
        
        debug!("{} 데이터 정제 완료: {}개 레코드, 품질 점수: {:.2}", 
               source_name, cleaned_data.len(), quality_report.overall_quality);
        
        // 품질 임계값 확인
        if quality_report.overall_quality < self.config.quality_threshold {
            warn!("{} 데이터 품질이 임계값 미만: {:.2} < {:.2}", 
                  source_name, quality_report.overall_quality, self.config.quality_threshold);
        }
        
        // 3. 특성 추출
        let feature_extractor = self.feature_extractors.get(source_name)
            .ok_or_else(|| anyhow::anyhow!("특성 추출기를 찾을 수 없음: {}", source_name))?;
        
        let processed_data = feature_extractor.extract_market_features(&cleaned_data).await?;
        
        // 품질 점수 업데이트
        let mut updated_processed_data = Vec::new();
        for mut data in processed_data {
            data.update_quality_score(quality_report.overall_quality);
            updated_processed_data.push(data);
        }
        
        // 처리 시간 기록
        let processing_time = start_time.elapsed();
        {
            let mut stats = self.stats.write().await;
            stats.processing_times.insert(source_name.to_string(), Duration::from_std(processing_time).unwrap_or_default());
            stats.total_processed_records += updated_processed_data.len() as u64;
            
            // 평균 품질 점수 업데이트
            let total_quality: f64 = updated_processed_data.iter()
                .map(|d| d.quality_score)
                .sum();
            stats.average_quality_score = total_quality / updated_processed_data.len() as f64;
        }
        Ok(updated_processed_data)
    }

    /// 수집 파라미터 생성
    async fn create_collect_params(&self, source_name: &str) -> Result<CollectParams> {
        // 기본 파라미터 설정
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(365); // 1년간 데이터
        
        let mut params = CollectParams {
            symbol: Some("000270".to_string()), // 기본값: 기아
            start_date: Some(start_date),
            end_date: Some(end_date),
            interval: Some("1d".to_string()),
            limit: Some(1000),
            additional_params: HashMap::new(),
        };
        
        // 소스별 파라미터 조정
        match source_name {
            "KIS API" => {
                params.symbol = Some("000270".to_string()); // 한국 주식
                params.additional_params.insert("market".to_string(), "KOSPI".to_string());
            }
            "Alpha Vantage" => {
                params.symbol = Some("AAPL".to_string()); // 미국 주식
                params.additional_params.insert("market".to_string(), "US".to_string());
            }
            "Yahoo Finance" => {
                params.symbol = Some("^GSPC".to_string()); // S&P 500
                params.additional_params.insert("market".to_string(), "US".to_string());
            }
            _ => {}
        }
        
        Ok(params)
    }

    /// 모니터링 시작
    async fn start_monitoring(&self) -> Result<()> {
        let mut interval_timer = interval(std::time::Duration::from_secs(60));
        
        loop {
            interval_timer.tick().await;
            
            // 상태 확인
            let status = self.status.read().await;
            match *status {
                PipelineStatus::Running => {
                    // 주기적 데이터 수집
                    if let Err(e) = self.collect_all_data().await {
                        error!("주기적 데이터 수집 실패: {}", e);
                    }
                }
                PipelineStatus::Stopped => {
                    break;
                }
                _ => {
                    // 다른 상태에서는 대기
                    sleep(std::time::Duration::from_secs(10)).await;
                }
            }
        }
        
        Ok(())
    }

    /// 특정 심볼의 데이터 조회
    pub async fn get_data_for_symbol(&self, symbol: &str) -> Result<Vec<ProcessedData>> {
        let cache = self.processed_data_cache.read().await;
        let mut all_data = Vec::new();
        
        for (source_name, source_data) in cache.iter() {
            let symbol_data: Vec<ProcessedData> = source_data.iter()
                .filter(|d| d.symbol == symbol)
                .cloned()
                .collect();
            
            all_data.extend(symbol_data);
            debug!("{}에서 {} 심볼 데이터 {}개 조회", source_name, symbol, all_data.len());
        }
        
        // 시간순 정렬
        all_data.sort_by_key(|d| d.timestamp);
        
        Ok(all_data)
    }

    /// 특정 특성의 데이터 조회
    pub async fn get_feature_data(&self, feature_name: &str, symbol: Option<&str>) -> Result<Vec<(DateTime<Utc>, f64)>> {
        let cache = self.processed_data_cache.read().await;
        let mut feature_data = Vec::new();
        
        for source_data in cache.values() {
            for data in source_data {
                if let Some(symbol_filter) = symbol {
                    if data.symbol != symbol_filter {
                        continue;
                    }
                }
                
                if let Some(&feature_value) = data.features.get(feature_name) {
                    if !feature_value.is_nan() {
                        feature_data.push((data.timestamp, feature_value));
                    }
                }
            }
        }
        
        // 시간순 정렬
        feature_data.sort_by_key(|(timestamp, _)| *timestamp);
        
        Ok(feature_data)
    }

    /// 파이프라인 상태 조회
    pub async fn get_status(&self) -> PipelineStatus {
        let status = self.status.read().await;
        status.clone()
    }

    /// 파이프라인 통계 조회
    pub async fn get_statistics(&self) -> PipelineStatistics {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// 데이터 품질 보고서 생성
    pub async fn generate_quality_report(&self) -> Result<QualityReport> {
        let cache = self.processed_data_cache.read().await;
        let mut overall_quality = 0.0;
        let mut total_records = 0;
        let mut source_reports = HashMap::new();
        
        for (source_name, source_data) in cache.iter() {
            if !source_data.is_empty() {
                let source_quality: f64 = source_data.iter()
                    .map(|d| d.quality_score)
                    .sum::<f64>() / source_data.len() as f64;
                
                overall_quality += source_quality * source_data.len() as f64;
                total_records += source_data.len();
                
                source_reports.insert(source_name.clone(), SourceQualityReport {
                    record_count: source_data.len(),
                    average_quality: source_quality,
                    quality_distribution: self.calculate_quality_distribution(source_data),
                });
            }
        }
        
        if total_records > 0 {
            overall_quality /= total_records as f64;
        }
        
        Ok(QualityReport {
            overall_quality,
            total_records,
            source_reports,
            generated_at: Utc::now(),
        })
    }

    /// 품질 분포 계산
    fn calculate_quality_distribution(&self, data: &[ProcessedData]) -> QualityDistribution {
        let mut excellent = 0; // >= 0.9
        let mut good = 0;      // 0.7 ~ 0.9
        let mut fair = 0;      // 0.5 ~ 0.7
        let mut poor = 0;      // < 0.5
        
        for d in data {
            match d.quality_score {
                score if score >= 0.9 => excellent += 1,
                score if score >= 0.7 => good += 1,
                score if score >= 0.5 => fair += 1,
                _ => poor += 1,
            }
        }
        
        QualityDistribution {
            excellent,
            good,
            fair,
            poor,
        }
    }

    /// 특성 중요도 분석
    pub async fn analyze_feature_importance(&self, target_feature: &str) -> Result<HashMap<String, f64>> {
        let cache = self.processed_data_cache.read().await;
        let mut all_importance = HashMap::new();
        
        for (source_name, source_data) in cache.iter() {
            if let Some(feature_extractor) = self.feature_extractors.get(source_name) {
                let importance = feature_extractor.calculate_feature_importance(source_data, target_feature);
                
                // 소스별 중요도 통합
                for (feature_name, importance_score) in importance {
                    let entry = all_importance.entry(feature_name).or_insert(0.0);
                    *entry += importance_score;
                }
            }
        }
        
        // 평균 계산
        let source_count = cache.len() as f64;
        for importance_score in all_importance.values_mut() {
            *importance_score /= source_count;
        }
        
        Ok(all_importance)
    }
}

/// 품질 보고서
#[derive(Debug, Clone)]
pub struct QualityReport {
    pub overall_quality: f64,
    pub total_records: usize,
    pub source_reports: HashMap<String, SourceQualityReport>,
    pub generated_at: DateTime<Utc>,
}

/// 소스별 품질 보고서
#[derive(Debug, Clone)]
pub struct SourceQualityReport {
    pub record_count: usize,
    pub average_quality: f64,
    pub quality_distribution: QualityDistribution,
}

/// 품질 분포
#[derive(Debug, Clone)]
pub struct QualityDistribution {
    pub excellent: usize, // >= 0.9
    pub good: usize,      // 0.7 ~ 0.9
    pub fair: usize,      // 0.5 ~ 0.7
    pub poor: usize,      // < 0.5
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            data_sources: vec![
                DataSourceConfig {
                    source_name: "KIS API".to_string(),
                    data_type: crate::data::DataType::MarketData,
                    api_config: crate::data::ApiConfig {
                        base_url: "https://openapi.koreainvestment.com:9443".to_string(),
                        api_key: None,
                        api_secret: None,
                        rate_limit: crate::data::RateLimitConfig {
                            requests_per_minute: 60,
                            requests_per_hour: 3600,
                            requests_per_day: 86400,
                            burst_limit: 10,
                        },
                        timeout_seconds: 30,
                        headers: HashMap::new(),
                    },
                    update_frequency: crate::data::UpdateFrequency::EveryMinute,
                    retry_config: crate::data::RetryConfig {
                        max_retries: 3,
                        initial_delay_ms: 1000,
                        max_delay_ms: 10000,
                        backoff_multiplier: 2.0,
                        jitter: true,
                    },
                    quality_thresholds: crate::data::QualityThresholds {
                        min_data_completeness: 0.95,
                        max_latency_seconds: 5,
                        max_price_deviation: 0.1,
                        max_volume_spike: 5.0,
                        min_confidence_score: 0.8,
                    },
                }
            ],
            preprocessing_config: PreprocessingConfig::default(),
            update_intervals: HashMap::from([
                ("KIS API".to_string(), Duration::minutes(1)),
                ("Alpha Vantage".to_string(), Duration::minutes(5)),
                ("Yahoo Finance".to_string(), Duration::minutes(1)),
            ]),
            cache_size: 10000,
            quality_threshold: 0.8,
            enable_monitoring: true,
            auto_retry: true,
            max_retries: 3,
        }
    }
}

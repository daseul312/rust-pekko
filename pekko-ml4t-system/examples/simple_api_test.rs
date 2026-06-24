//! 간단한 API 연동 테스트 예제
//!
//! 이 예제는 Mock API를 사용하여 실제 API 연동 기능을 테스트합니다.

use pekko_ml4t_system::config::api_config::ApiConfig;
use pekko_ml4t_system::data::collectors::RealApiDataCollector;
use anyhow::Result;
use dotenv::dotenv;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{ error};

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();

    println!("🚀 간단한 API 연동 테스트");
    println!("════════════════════════════════════════");

    // 환경변수 로드 (선택사항)
    dotenv().ok();

    // Mock API 설정 사용
    let api_config = ApiConfig::mock_config();
    // 실제 API 데이터 수집기 생성
    match RealApiDataCollector::new(api_config).await {
        Ok(collector) => {
            // API 상태 확인
            let api_status = collector.get_api_status();
            for (api_name, is_connected) in &api_status {
            }

            // 헬스 체크
            match collector.health_check().await {
                pekko_ml4t_system::data::collectors::HealthStatus::Healthy => {
                }
                pekko_ml4t_system::data::collectors::HealthStatus::Degraded(issues) => {
                    for issue in issues {
                    }
                }
            }

            // 간단한 데이터 수집 테스트 (Mock이므로 실제 데이터는 없음)
            let symbols = vec!["AAPL".to_string(), "GOOGL".to_string()];
            
            // 데이터 수집 시작 (5초 간격)
            if let Err(e) = collector.start_collection(symbols.clone(), 5).await {
                error!("데이터 수집 시작 실패: {}", e);
                return Err(e);
            }

            // 10초간 대기
            sleep(Duration::from_secs(10)).await;

            // 수집 중지
            if let Err(e) = collector.stop_collection().await {
                error!("데이터 수집 중지 실패: {}", e);
                return Err(e);
            }

            // 수집 통계 확인
            let stats = collector.get_collection_stats().await;
        }
        Err(e) => {
            error!("❌ API 데이터 수집기 생성 실패: {}", e);
            return Err(e);
        }
    }

    println!("\n🎉 간단한 API 연동 테스트가 완료되었습니다!");
    println!("실제 API 키를 설정하면 실제 시장 데이터를 가져올 수 있습니다! 🚀");

    Ok(())
}

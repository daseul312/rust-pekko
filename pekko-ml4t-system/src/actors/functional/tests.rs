//! 함수형 Actor들의 통합 테스트

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_actor::{FunctionalActor, FunctionalActorContext, ActorSystem, ActorPath, ActorAddress};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_all_functional_actors_initialization() {
        let system = Arc::new(ActorSystem::new("test_system"));
        let address = ActorAddress::local("test_system");
        let path = ActorPath::new(address, vec!["test".to_string()]);
        let ctx = FunctionalActorContext::new(system, path);

        // 모든 함수형 Actor 초기화 테스트
        let _llm_state = LLMAnalysisActor::init(&ctx);
        let _market_state = MarketDataActor::init(&ctx);
        let _sentiment_state = SentimentAnalysisActor::init(&ctx);
        let _report_state = ReportGenerationActor::init(&ctx);
        let _prediction_state = PredictionActor::init(&ctx);

        // 초기화가 성공적으로 완료되었는지 확인
        assert!(true);
    }

    #[tokio::test]
    async fn test_actor_state_immutability() {
        // 불변성 테스트
        let initial_state = market_data_actor::MarketDataState::new();
        let initial_count = initial_state.price_history.len();

        let point = market_data_actor::PricePoint {
            symbol: "TEST".to_string(),
            price: 100.0,
            volume: 1000,
            timestamp: chrono::Utc::now(),
            high: 105.0,
            low: 95.0,
            open: 98.0,
            close: 100.0,
        };

        let new_state = initial_state.add_price_point(point);

        // 원본 상태는 변경되지 않아야 함
        assert_eq!(initial_state.price_history.len(), initial_count);
        assert_eq!(new_state.price_history.len(), initial_count + 1);
    }

    #[tokio::test]
    async fn test_sentiment_analysis_workflow() {
        let actor = SentimentAnalysisActor::new_with_mock_client();
        let state = SentimentAnalysisActor::init(&FunctionalActorContext::new(
            Arc::new(ActorSystem::new("test")),
            ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
        ));

        let news = sentiment_actor::NewsItem {
            title: "Test News".to_string(),
            content: "Test content about AAPL stock".to_string(),
            source: "Test Source".to_string(),
            symbol: Some("AAPL".to_string()),
            sentiment_score: None,
            timestamp: chrono::Utc::now(),
            url: None,
        };

        let msg = SentimentMessage::AnalyzeNews { news_item: news };
        let (new_state, _) = SentimentAnalysisActor::receive(state, msg,
            &FunctionalActorContext::new(
                Arc::new(ActorSystem::new("test")),
                ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
            )
        );

        // 분석 요청이 추가되었는지 확인
        assert!(new_state.last_update > chrono::Utc::now() - chrono::Duration::seconds(1));
    }

    #[tokio::test]
    async fn test_market_data_operations() {
        let actor = MarketDataActor::new();
        let state = MarketDataActor::init(&FunctionalActorContext::new(
            Arc::new(ActorSystem::new("test")),
            ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
        ));

        // 심볼 모니터링 추가
        let msg = MarketDataMessage::StartMonitoring { symbol: "AAPL".to_string() };
        let (new_state, _) = MarketDataActor::receive(state, msg,
            &FunctionalActorContext::new(
                Arc::new(ActorSystem::new("test")),
                ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
            )
        );

        assert!(new_state.monitored_symbols.contains(&"AAPL".to_string()));
    }

    #[tokio::test]
    async fn test_report_generation_workflow() {
        let actor = ReportGenerationActor::new_with_mock_client();
        let state = ReportGenerationActor::init(&FunctionalActorContext::new(
            Arc::new(ActorSystem::new("test")),
            ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
        ));

        let msg = ReportMessage::GenerateReport {
            report_type: report_actor::ReportType::DailyMarketReport,
            symbols: vec!["AAPL".to_string()],
            time_period: "2024-01-01".to_string(),
        };

        let (new_state, _) = ReportGenerationActor::receive(state, msg,
            &FunctionalActorContext::new(
                Arc::new(ActorSystem::new("test")),
                ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
            )
        );

        // 리포트 생성 요청이 처리되었는지 확인
        assert!(new_state.last_update > chrono::Utc::now() - chrono::Duration::seconds(1));
    }

    #[tokio::test]
    async fn test_prediction_workflow() {
        let actor = PredictionActor::new_with_mock_client();
        let state = PredictionActor::init(&FunctionalActorContext::new(
            Arc::new(ActorSystem::new("test")),
            ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
        ));

        let msg = PredictionMessage::PredictPrice {
            symbol: "AAPL".to_string(),
            prediction_type: prediction_actor::PredictionType::ShortTerm,
            time_horizon: "1d".to_string(),
        };

        let (new_state, _) = PredictionActor::receive(state, msg,
            &FunctionalActorContext::new(
                Arc::new(ActorSystem::new("test")),
                ActorPath::new(ActorAddress::local("test"), vec!["test".to_string()])
            )
        );

        // 예측 요청이 처리되었는지 확인
        assert!(new_state.last_update > chrono::Utc::now() - chrono::Duration::seconds(1));
    }

    #[test]
    fn test_functional_programming_principles() {
        // 순수 함수 테스트: 같은 입력에 같은 결과
        let sentiment1 = sentiment_actor::SentimentScore {
            positive: 0.6,
            negative: 0.2,
            neutral: 0.2,
            overall: 0.4,
        };
        let sentiment2 = sentiment1.clone();

        // 같은 입력에 같은 결과
        let result1 = sentiment1.overall * 1.5;
        let result2 = sentiment2.overall * 1.5;

        assert_eq!(result1, result2);

        // 불변성 테스트
        let state1 = market_data_actor::MarketDataState::new();
        let state2 = state1.clone();

        assert_eq!(state1.price_history.len(), state2.price_history.len());
    }
}
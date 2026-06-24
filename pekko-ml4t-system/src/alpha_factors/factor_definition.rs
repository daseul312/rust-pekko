//! Factor 정의 모듈
//! 
//! Alpha Factor의 정의, 생성, 검증을 위한 시스템:
//! - Factor 정의 및 메타데이터 관리
//! - Factor 파라미터 검증
//! - Factor 의존성 관리
//! - Factor 버전 관리
//! - Factor 템플릿 및 팩토리

use super::*;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{ warn, error};

/// Factor 정의 관리자
pub struct FactorDefinitionManager {
    factor_registry: HashMap<String, FactorDefinition>,
    factor_templates: HashMap<String, FactorTemplate>,
    factor_validators: Vec<Box<dyn FactorValidator>>,
    dependency_graph: DependencyGraph,
}

/// Factor 정의
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub status: FactorStatus,
    pub category: FactorCategory,
    pub formula: FactorFormula,
    pub parameters: HashMap<String, FactorParameter>,
    pub dependencies: Vec<FactorDependency>,
    pub universe: UniverseDefinition,
    pub metadata: FactorMetadata,
    pub validation_results: Vec<ValidationResult>,
}

/// Factor 상태
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FactorStatus {
    Draft,
    Testing,
    Validated,
    Production,
    Deprecated,
    Retired,
}

/// Factor 공식
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorFormula {
    pub expression: String,
    pub language: FormulaLanguage,
    pub complexity: FormulaComplexity,
    pub variables: Vec<FormulaVariable>,
    pub constants: HashMap<String, f64>,
    pub operations: Vec<FormulaOperation>,
}

/// 공식 언어
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FormulaLanguage {
    Rust,
    Python,
    SQL,
    Mathematical,
    Custom(String),
}

/// 공식 복잡도
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FormulaComplexity {
    Simple,    // 기본 수학 연산
    Medium,    // 중간 수학 연산
    Complex,   // 복잡한 수학 연산
    Advanced,  // 고급 수학 연산
}

/// 공식 변수
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormulaVariable {
    pub name: String,
    pub data_type: VariableDataType,
    pub description: String,
    pub source: VariableSource,
    pub required: bool,
    pub default_value: Option<f64>,
}

/// 변수 데이터 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VariableDataType {
    Price,
    Volume,
    Returns,
    TechnicalIndicator,
    Fundamental,
    Alternative,
    Custom(String),
}

/// 변수 소스
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VariableSource {
    MarketData,
    TechnicalAnalysis,
    FundamentalData,
    AlternativeData,
    ExternalAPI,
    Database,
    Custom(String),
}

/// 공식 연산
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormulaOperation {
    pub operation_type: OperationType,
    pub description: String,
    pub parameters: Vec<String>,
    pub implementation: String,
}

/// 연산 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationType {
    Mathematical,
    Statistical,
    TimeSeries,
    CrossSectional,
    Technical,
    Custom(String),
}

/// Factor 파라미터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorParameter {
    pub name: String,
    pub parameter_type: ParameterType,
    pub description: String,
    pub default_value: f64,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub step_size: Option<f64>,
    pub validation_rules: Vec<ValidationRule>,
    pub sensitivity_analysis: Option<SensitivityAnalysis>,
}

/// 검증 규칙
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    pub rule_type: ValidationRuleType,
    pub description: String,
    pub condition: String,
    pub error_message: String,
}

/// 검증 규칙 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationRuleType {
    Range,
    Positive,
    NonZero,
    Integer,
    Custom(String),
}

/// 민감도 분석
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensitivityAnalysis {
    pub parameter_name: String,
    pub sensitivity_score: f64,
    pub impact_analysis: HashMap<String, f64>,
    pub optimal_range: (f64, f64),
    pub stability_threshold: f64,
}

/// Factor 의존성
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorDependency {
    pub dependency_id: String,
    pub dependency_type: DependencyType,
    pub version_constraint: String,
    pub required: bool,
    pub description: String,
}

/// 의존성 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DependencyType {
    Factor,
    Data,
    Model,
    Library,
    API,
    Database,
}

/// Factor 메타데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorMetadata {
    pub tags: Vec<String>,
    pub keywords: Vec<String>,
    pub documentation_url: Option<String>,
    pub source_code_url: Option<String>,
    pub paper_reference: Option<String>,
    pub license: String,
    pub contact_info: ContactInfo,
    pub performance_benchmarks: Vec<PerformanceBenchmark>,
    pub usage_examples: Vec<UsageExample>,
}

/// 연락처 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactInfo {
    pub author_name: String,
    pub email: Option<String>,
    pub organization: Option<String>,
    pub github: Option<String>,
}

/// 성능 벤치마크
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBenchmark {
    pub benchmark_name: String,
    pub period: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub metrics: FactorPerformanceMetrics,
    pub conditions: String,
}

/// 사용 예시
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageExample {
    pub example_name: String,
    pub description: String,
    pub code: String,
    pub expected_output: String,
    pub parameters: HashMap<String, f64>,
}

/// Factor 템플릿
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorTemplate {
    pub template_id: String,
    pub name: String,
    pub description: String,
    pub category: FactorCategory,
    pub template_type: TemplateType,
    pub base_formula: FactorFormula,
    pub parameter_templates: HashMap<String, FactorParameter>,
    pub usage_instructions: String,
    pub customization_guide: String,
}

/// 템플릿 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemplateType {
    Momentum,
    MeanReversion,
    Value,
    Quality,
    Growth,
    Volatility,
    Volume,
    Technical,
    Fundamental,
    Alternative,
    Custom,
}

/// 검증 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub validator_name: String,
    pub validation_type: ValidationType,
    pub passed: bool,
    pub score: f64,
    pub message: String,
    pub details: HashMap<String, String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 검증 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationType {
    Syntax,
    Semantics,
    Performance,
    Risk,
    Stability,
    Dependencies,
    Documentation,
    Custom(String),
}

/// 의존성 그래프
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    nodes: HashMap<String, DependencyNode>,
    edges: Vec<DependencyEdge>,
}

/// 의존성 노드
#[derive(Debug, Clone)]
pub struct DependencyNode {
    pub id: String,
    pub node_type: DependencyType,
    pub metadata: HashMap<String, String>,
}

/// 의존성 엣지
#[derive(Debug, Clone)]
pub struct DependencyEdge {
    pub from: String,
    pub to: String,
    pub edge_type: EdgeType,
    pub weight: f64,
}

/// 엣지 타입
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EdgeType {
    Direct,
    Indirect,
    Conditional,
    Optional,
}

/// Factor 검증 트레이트
pub trait FactorValidator: Send + Sync {
    fn validate(&self, factor: &FactorDefinition) -> Result<ValidationResult>;
    fn name(&self) -> &str;
    fn validation_type(&self) -> ValidationType;
}

/// Factor 팩토리
pub struct FactorFactory {
    templates: HashMap<String, FactorTemplate>,
    builders: HashMap<String, Box<dyn FactorBuilder>>,
}

/// Factor 빌더 트레이트
pub trait FactorBuilder: Send + Sync {
    fn build(&self, template: &FactorTemplate, parameters: HashMap<String, f64>) -> Result<FactorDefinition>;
    fn supported_template(&self) -> String;
}

impl FactorDefinitionManager {
    /// 새로운 Factor 정의 관리자 생성
    pub fn new() -> Self {
        Self {
            factor_registry: HashMap::new(),
            factor_templates: HashMap::new(),
            factor_validators: Vec::new(),
            dependency_graph: DependencyGraph::new(),
        }
    }

    /// Factor 등록
    pub fn register_factor(&mut self, factor: FactorDefinition) -> Result<()> {
        // 의존성 검증
        self.validate_dependencies(&factor)?;

        // Factor 검증
        let validation_results = self.validate_factor(&factor)?;
        
        // 검증 결과 추가
        let mut validated_factor = factor;
        validated_factor.validation_results = validation_results;

        // 등록
        self.factor_registry.insert(validated_factor.id.clone(), validated_factor);
        
        // 의존성 그래프 업데이트
        self.update_dependency_graph(&self.factor_registry.get(&self.factor_registry.keys().next().unwrap().clone()).unwrap())?;
        Ok(())
    }

    /// Factor 검증
    pub fn validate_factor(&self, factor: &FactorDefinition) -> Result<Vec<ValidationResult>> {
        let mut results = Vec::new();

        for validator in &self.factor_validators {
            match validator.validate(factor) {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!("Factor 검증 실패 ({}): {}", validator.name(), e);
                    results.push(ValidationResult {
                        validator_name: validator.name().to_string(),
                        validation_type: validator.validation_type(),
                        passed: false,
                        score: 0.0,
                        message: format!("검증 오류: {}", e),
                        details: HashMap::new(),
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
        }

        Ok(results)
    }

    /// 의존성 검증
    fn validate_dependencies(&self, factor: &FactorDefinition) -> Result<()> {
        for dependency in &factor.dependencies {
            match dependency.dependency_type {
                DependencyType::Factor => {
                    if !self.factor_registry.contains_key(&dependency.dependency_id) {
                        return Err(anyhow::anyhow!("필수 Factor 의존성을 찾을 수 없습니다: {}", dependency.dependency_id));
                    }
                }
                DependencyType::Data => {
                    // 데이터 의존성 검증
                }
                DependencyType::Model => {
                    // 모델 의존성 검증
                }
                _ => {
                    // 기타 의존성 검증
                }
            }
        }

        Ok(())
    }

    /// 의존성 그래프 업데이트
    fn update_dependency_graph(&mut self, factor: &FactorDefinition) -> Result<()> {
        // Factor 노드 추가
        let node = DependencyNode {
            id: factor.id.clone(),
            node_type: DependencyType::Factor,
            metadata: HashMap::new(),
        };
        self.dependency_graph.add_node(node);

        // 의존성 엣지 추가
        for dependency in &factor.dependencies {
            let edge = DependencyEdge {
                from: factor.id.clone(),
                to: dependency.dependency_id.clone(),
                edge_type: if dependency.required { EdgeType::Direct } else { EdgeType::Optional },
                weight: if dependency.required { 1.0 } else { 0.5 },
            };
            self.dependency_graph.add_edge(edge);
        }

        Ok(())
    }

    /// Factor 조회
    pub fn get_factor(&self, factor_id: &str) -> Option<&FactorDefinition> {
        self.factor_registry.get(factor_id)
    }

    /// 모든 Factor 조회
    pub fn get_all_factors(&self) -> Vec<&FactorDefinition> {
        self.factor_registry.values().collect()
    }

    /// 카테고리별 Factor 조회
    pub fn get_factors_by_category(&self, category: &FactorCategory) -> Vec<&FactorDefinition> {
        self.factor_registry.values()
            .filter(|factor| std::mem::discriminant(&factor.category) == std::mem::discriminant(category))
            .collect()
    }

    /// 상태별 Factor 조회
    pub fn get_factors_by_status(&self, status: &FactorStatus) -> Vec<&FactorDefinition> {
        self.factor_registry.values()
            .filter(|factor| std::mem::discriminant(&factor.status) == std::mem::discriminant(status))
            .collect()
    }

    /// 검증자 추가
    pub fn add_validator(&mut self, validator: Box<dyn FactorValidator>) {
        self.factor_validators.push(validator);
    }

    /// Factor 템플릿 등록
    pub fn register_template(&mut self, template: FactorTemplate) {
        self.factor_templates.insert(template.template_id.clone(), template);
    }

    /// 템플릿으로부터 Factor 생성
    pub fn create_factor_from_template(&self, template_id: &str, parameters: HashMap<String, f64>) -> Result<FactorDefinition> {
        let template = self.factor_templates.get(template_id)
            .ok_or_else(|| anyhow::anyhow!("템플릿을 찾을 수 없습니다: {}", template_id))?;

        let factor = FactorDefinition {
            id: format!("{}_{}", template_id, chrono::Utc::now().timestamp()),
            name: template.name.clone(),
            description: template.description.clone(),
            version: "1.0.0".to_string(),
            author: "System".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            status: FactorStatus::Draft,
            category: template.category.clone(),
            formula: template.base_formula.clone(),
            parameters: template.parameter_templates.clone(),
            dependencies: Vec::new(),
            universe: UniverseDefinition::default(),
            metadata: FactorMetadata::default(),
            validation_results: Vec::new(),
        };

        Ok(factor)
    }

    /// 의존성 분석
    pub fn analyze_dependencies(&self, factor_id: &str) -> Result<DependencyAnalysis> {
        let factor = self.get_factor(factor_id)
            .ok_or_else(|| anyhow::anyhow!("Factor를 찾을 수 없습니다: {}", factor_id))?;

        let direct_dependencies = factor.dependencies.clone();
        let indirect_dependencies = self.find_indirect_dependencies(factor_id)?;
        let dependents = self.find_dependents(factor_id)?;

        Ok(DependencyAnalysis {
            factor_id: factor_id.to_string(),
            direct_dependencies,
            indirect_dependencies,
            dependents,
            dependency_depth: self.calculate_dependency_depth(factor_id)?,
            circular_dependencies: self.detect_circular_dependencies(factor_id)?,
        })
    }

    /// 간접 의존성 찾기
    fn find_indirect_dependencies(&self, factor_id: &str) -> Result<Vec<FactorDependency>> {
        let mut indirect_deps = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut queue = vec![factor_id.to_string()];

        while let Some(current_id) = queue.pop() {
            if visited.contains(&current_id) {
                continue;
            }
            visited.insert(current_id.clone());

            if let Some(factor) = self.factor_registry.get(&current_id) {
                for dep in &factor.dependencies {
                    if dep.dependency_type == DependencyType::Factor && !visited.contains(&dep.dependency_id) {
                        indirect_deps.push(dep.clone());
                        queue.push(dep.dependency_id.clone());
                    }
                }
            }
        }

        Ok(indirect_deps)
    }

    /// 의존하는 Factor들 찾기
    fn find_dependents(&self, factor_id: &str) -> Result<Vec<String>> {
        let mut dependents = Vec::new();

        for (id, factor) in &self.factor_registry {
            for dep in &factor.dependencies {
                if dep.dependency_id == factor_id {
                    dependents.push(id.clone());
                }
            }
        }

        Ok(dependents)
    }

    /// 의존성 깊이 계산
    fn calculate_dependency_depth(&self, factor_id: &str) -> Result<u32> {
        let mut max_depth = 0;
        let mut visited = std::collections::HashSet::new();

        fn calculate_depth_recursive(
            factor_id: &str,
            registry: &HashMap<String, FactorDefinition>,
            visited: &mut std::collections::HashSet<String>,
            depth: u32,
        ) -> u32 {
            if visited.contains(factor_id) {
                return depth;
            }
            visited.insert(factor_id.to_string());

            let mut max_depth = depth;
            if let Some(factor) = registry.get(factor_id) {
                for dep in &factor.dependencies {
                    if dep.dependency_type == DependencyType::Factor {
                        let dep_depth = calculate_depth_recursive(&dep.dependency_id, registry, visited, depth + 1);
                        max_depth = max_depth.max(dep_depth);
                    }
                }
            }

            max_depth
        }

        max_depth = calculate_depth_recursive(factor_id, &self.factor_registry, &mut visited, 0);
        Ok(max_depth)
    }

    /// 순환 의존성 감지
    fn detect_circular_dependencies(&self, factor_id: &str) -> Result<Vec<Vec<String>>> {
        let mut circular_deps = Vec::new();
        let mut path = Vec::new();
        let mut visited = std::collections::HashSet::new();

        fn detect_circular_recursive(
            factor_id: &str,
            registry: &HashMap<String, FactorDefinition>,
            path: &mut Vec<String>,
            visited: &mut std::collections::HashSet<String>,
            circular_deps: &mut Vec<Vec<String>>,
        ) {
            if path.contains(&factor_id.to_string()) {
                // 순환 의존성 발견
                let cycle_start = path.iter().position(|id| id == factor_id).unwrap();
                let cycle = path[cycle_start..].to_vec();
                circular_deps.push(cycle);
                return;
            }

            if visited.contains(factor_id) {
                return;
            }

            visited.insert(factor_id.to_string());
            path.push(factor_id.to_string());

            if let Some(factor) = registry.get(factor_id) {
                for dep in &factor.dependencies {
                    if dep.dependency_type == DependencyType::Factor {
                        detect_circular_recursive(&dep.dependency_id, registry, path, visited, circular_deps);
                    }
                }
            }

            path.pop();
        }

        detect_circular_recursive(factor_id, &self.factor_registry, &mut path, &mut visited, &mut circular_deps);
        Ok(circular_deps)
    }
}

/// 의존성 분석 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyAnalysis {
    pub factor_id: String,
    pub direct_dependencies: Vec<FactorDependency>,
    pub indirect_dependencies: Vec<FactorDependency>,
    pub dependents: Vec<String>,
    pub dependency_depth: u32,
    pub circular_dependencies: Vec<Vec<String>>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: DependencyNode) {
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn add_edge(&mut self, edge: DependencyEdge) {
        self.edges.push(edge);
    }

    pub fn get_dependencies(&self, node_id: &str) -> Vec<String> {
        self.edges.iter()
            .filter(|edge| edge.from == node_id)
            .map(|edge| edge.to.clone())
            .collect()
    }

    pub fn get_dependents(&self, node_id: &str) -> Vec<String> {
        self.edges.iter()
            .filter(|edge| edge.to == node_id)
            .map(|edge| edge.from.clone())
            .collect()
    }

    pub fn topological_sort(&self) -> Result<Vec<String>> {
        let mut in_degree = HashMap::new();
        let mut queue = Vec::new();
        let mut result = Vec::new();

        // 모든 노드의 in-degree 계산
        for node_id in self.nodes.keys() {
            in_degree.insert(node_id.clone(), 0);
        }

        for edge in &self.edges {
            *in_degree.get_mut(&edge.to).unwrap() += 1;
        }

        // in-degree가 0인 노드들을 큐에 추가
        for (node_id, degree) in &in_degree {
            if *degree == 0 {
                queue.push(node_id.clone());
            }
        }

        // 위상 정렬 수행
        while let Some(current) = queue.pop() {
            result.push(current.clone());

            for edge in &self.edges {
                if edge.from == current {
                    let to_degree = in_degree.get_mut(&edge.to).unwrap();
                    *to_degree -= 1;
                    if *to_degree == 0 {
                        queue.push(edge.to.clone());
                    }
                }
            }
        }

        if result.len() != self.nodes.len() {
            return Err(anyhow::anyhow!("순환 의존성이 존재합니다"));
        }

        Ok(result)
    }
}

impl FactorFactory {
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
            builders: HashMap::new(),
        }
    }

    pub fn register_template(&mut self, template: FactorTemplate) {
        self.templates.insert(template.template_id.clone(), template);
    }

    pub fn register_builder(&mut self, template_id: String, builder: Box<dyn FactorBuilder>) {
        self.builders.insert(template_id, builder);
    }

    pub fn create_factor(&self, template_id: &str, parameters: HashMap<String, f64>) -> Result<FactorDefinition> {
        let builder = self.builders.get(template_id)
            .ok_or_else(|| anyhow::anyhow!("빌더를 찾을 수 없습니다: {}", template_id))?;

        let template = self.templates.get(template_id)
            .ok_or_else(|| anyhow::anyhow!("템플릿을 찾을 수 없습니다: {}", template_id))?;

        builder.build(template, parameters)
    }
}

impl Default for FactorMetadata {
    fn default() -> Self {
        Self {
            tags: Vec::new(),
            keywords: Vec::new(),
            documentation_url: None,
            source_code_url: None,
            paper_reference: None,
            license: "MIT".to_string(),
            contact_info: ContactInfo {
                author_name: "Unknown".to_string(),
                email: None,
                organization: None,
                github: None,
            },
            performance_benchmarks: Vec::new(),
            usage_examples: Vec::new(),
        }
    }
}

// 기본 검증자들

/// 구문 검증자
pub struct SyntaxValidator;

impl FactorValidator for SyntaxValidator {
    fn validate(&self, factor: &FactorDefinition) -> Result<ValidationResult> {
        let mut passed = true;
        let mut score = 1.0;
        let mut message = "구문 검증 통과".to_string();

        // 공식 구문 검증
        if factor.formula.expression.is_empty() {
            passed = false;
            score = 0.0;
            message = "공식이 비어있습니다".to_string();
        }

        // 변수 검증
        for variable in &factor.formula.variables {
            if variable.name.is_empty() {
                passed = false;
                score = 0.0;
                message = "변수명이 비어있습니다".to_string();
                break;
            }
        }

        Ok(ValidationResult {
            validator_name: "SyntaxValidator".to_string(),
            validation_type: ValidationType::Syntax,
            passed,
            score,
            message,
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn name(&self) -> &str {
        "SyntaxValidator"
    }

    fn validation_type(&self) -> ValidationType {
        ValidationType::Syntax
    }
}

/// 성능 검증자
pub struct PerformanceValidator;

impl FactorValidator for PerformanceValidator {
    fn validate(&self, factor: &FactorDefinition) -> Result<ValidationResult> {
        let mut passed = true;
        let mut score = 1.0;
        let mut message = "성능 검증 통과".to_string();

        // 복잡도 기반 성능 예측
        let complexity_score = match factor.formula.complexity {
            FormulaComplexity::Simple => 1.0,
            FormulaComplexity::Medium => 0.8,
            FormulaComplexity::Complex => 0.6,
            FormulaComplexity::Advanced => 0.4,
        };

        // 변수 수 기반 성능 예측
        let variable_score = if factor.formula.variables.len() > 20 {
            0.5
        } else if factor.formula.variables.len() > 10 {
            0.7
        } else {
            1.0
        };

        score = (complexity_score + variable_score) / 2.0;

        if score < 0.5 {
            passed = false;
            message = "성능이 예상보다 낮을 수 있습니다".to_string();
        }

        Ok(ValidationResult {
            validator_name: "PerformanceValidator".to_string(),
            validation_type: ValidationType::Performance,
            passed,
            score,
            message,
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn name(&self) -> &str {
        "PerformanceValidator"
    }

    fn validation_type(&self) -> ValidationType {
        ValidationType::Performance
    }
}

/// 리스크 검증자
pub struct RiskValidator;

impl FactorValidator for RiskValidator {
    fn validate(&self, factor: &FactorDefinition) -> Result<ValidationResult> {
        let mut passed = true;
        let mut score = 1.0;
        let mut message = "리스크 검증 통과".to_string();

        // 파라미터 검증
        for (_, param) in &factor.parameters {
            if let Some(min_val) = param.min_value {
                if let Some(max_val) = param.max_value {
                    if min_val >= max_val {
                        passed = false;
                        score = 0.0;
                        message = "파라미터 범위가 잘못되었습니다".to_string();
                        break;
                    }
                }
            }
        }

        // 의존성 리스크 평가
        let dependency_count = factor.dependencies.len();
        if dependency_count > 10 {
            score *= 0.7;
            message = "의존성이 많아 리스크가 증가할 수 있습니다".to_string();
        }

        Ok(ValidationResult {
            validator_name: "RiskValidator".to_string(),
            validation_type: ValidationType::Risk,
            passed,
            score,
            message,
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn name(&self) -> &str {
        "RiskValidator"
    }

    fn validation_type(&self) -> ValidationType {
        ValidationType::Risk
    }
}

impl Default for FactorDefinitionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for FactorFactory {
    fn default() -> Self {
        Self::new()
    }
}

-- =============================================================================
-- ENHANCED POSTGRESQL GOLD SCHEMA
-- =============================================================================
-- Matches the comprehensive Hive gold data structure with predictions,
-- risk scores, interaction features, normalized features, and BI features
-- =============================================================================

-- Drop existing gold tables if they exist
DROP TABLE IF EXISTS gold.diabetes_gold CASCADE;
DROP TABLE IF EXISTS gold.health_insights_gold CASCADE;
DROP TABLE IF EXISTS gold.diabetes_features CASCADE;

-- =============================================================================
-- ENHANCED GOLD LAYER - Comprehensive Feature Set
-- =============================================================================

-- Main diabetes gold table with comprehensive features (matching Hive schema)
CREATE TABLE gold.diabetes_gold (
    -- Primary key and references
    id SERIAL PRIMARY KEY,
    source_id UUID, -- Reference to silver layer
    
    -- Original diabetes features
    pregnancies INTEGER,
    glucose DOUBLE PRECISION,
    blood_pressure DOUBLE PRECISION,
    skin_thickness DOUBLE PRECISION,
    insulin DOUBLE PRECISION,
    bmi DOUBLE PRECISION,
    diabetes_pedigree_function DOUBLE PRECISION,
    age INTEGER,
    diabetes INTEGER, -- Original outcome
    
    -- Categorical features
    bmi_category VARCHAR(20),
    age_group VARCHAR(20),
    glucose_category VARCHAR(20),
    bp_category VARCHAR(20),
    
    -- Risk scoring and predictions
    risk_score DOUBLE PRECISION,
    prediction DOUBLE PRECISION, -- ML model prediction
    prediction_probability DOUBLE PRECISION, -- Prediction confidence
    
    -- Interaction features
    glucose_bmi_interaction DOUBLE PRECISION,
    age_glucose_interaction DOUBLE PRECISION,
    insulin_glucose_interaction DOUBLE PRECISION,
    age_bmi_interaction DOUBLE PRECISION,
    
    -- Normalized features (0-1 scale)
    glucose_normalized DOUBLE PRECISION,
    bmi_normalized DOUBLE PRECISION,
    age_normalized DOUBLE PRECISION,
    bp_normalized DOUBLE PRECISION,
    insulin_normalized DOUBLE PRECISION,
    
    -- Business Intelligence features
    health_risk_level VARCHAR(20), -- Low, Medium, High, Critical
    intervention_priority VARCHAR(20), -- Immediate, Soon, Routine, Monitor
    estimated_cost_category VARCHAR(20), -- Low, Medium, High, Very High
    treatment_recommendation TEXT,
    
    -- Additional derived features
    metabolic_syndrome_risk DOUBLE PRECISION,
    cardiovascular_risk DOUBLE PRECISION,
    pregnancy_risk_factor DOUBLE PRECISION,
    lifestyle_risk_score DOUBLE PRECISION,
    
    -- Data lineage and quality
    data_source VARCHAR(50),
    processing_version VARCHAR(20),
    quality_score DOUBLE PRECISION,
    anomaly_flags JSONB,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Partitioning helper
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Create comprehensive indexes for analytics performance
CREATE INDEX idx_diabetes_gold_health_risk_level ON gold.diabetes_gold(health_risk_level);
CREATE INDEX idx_diabetes_gold_processed_timestamp ON gold.diabetes_gold(processed_timestamp);
CREATE INDEX idx_diabetes_gold_risk_score ON gold.diabetes_gold(risk_score);
CREATE INDEX idx_diabetes_gold_age_group ON gold.diabetes_gold(age_group);
CREATE INDEX idx_diabetes_gold_bmi_category ON gold.diabetes_gold(bmi_category);
CREATE INDEX idx_diabetes_gold_glucose_category ON gold.diabetes_gold(glucose_category);
CREATE INDEX idx_diabetes_gold_intervention_priority ON gold.diabetes_gold(intervention_priority);
CREATE INDEX idx_diabetes_gold_processing_date ON gold.diabetes_gold(processing_date);
CREATE INDEX idx_diabetes_gold_prediction ON gold.diabetes_gold(prediction);
CREATE INDEX idx_diabetes_gold_diabetes ON gold.diabetes_gold(diabetes);

-- Composite indexes for common queries
CREATE INDEX idx_diabetes_gold_risk_age_bmi ON gold.diabetes_gold(health_risk_level, age_group, bmi_category);
CREATE INDEX idx_diabetes_gold_prediction_confidence ON gold.diabetes_gold(prediction, prediction_probability);

-- =============================================================================
-- AGGREGATED HEALTH INSIGHTS (Enhanced)
-- =============================================================================

CREATE TABLE gold.health_insights_gold (
    id SERIAL PRIMARY KEY,
    
    -- Grouping dimensions
    age_group VARCHAR(20),
    gender VARCHAR(10), -- For future use
    health_risk_category VARCHAR(20),
    bmi_category VARCHAR(20),
    
    -- Patient counts and demographics
    total_patients INTEGER,
    diabetic_patients INTEGER,
    high_risk_patients INTEGER,
    
    -- Average health metrics
    avg_bmi DOUBLE PRECISION,
    avg_glucose DOUBLE PRECISION,
    avg_age DOUBLE PRECISION,
    avg_risk_score DOUBLE PRECISION,
    avg_prediction_probability DOUBLE PRECISION,
    
    -- Health score aggregations
    avg_health_score DOUBLE PRECISION,
    avg_sleep_efficiency DOUBLE PRECISION, -- For future personal health data
    avg_steps_daily DOUBLE PRECISION,
    avg_exercise_minutes DOUBLE PRECISION,
    avg_heart_rate DOUBLE PRECISION,
    
    -- Risk distributions
    high_risk_count INTEGER,
    high_risk_percentage DOUBLE PRECISION,
    critical_risk_count INTEGER,
    
    -- Medical insights
    common_medical_condition VARCHAR(100),
    intervention_urgency_distribution JSONB,
    cost_category_distribution JSONB,
    
    -- Behavioral and lifestyle insights
    stress_level_distribution JSONB,
    mood_distribution JSONB,
    lifestyle_risk_distribution JSONB,
    
    -- Reporting metadata
    report_date DATE,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for health insights
CREATE INDEX idx_health_insights_report_date ON gold.health_insights_gold(report_date);
CREATE INDEX idx_health_insights_age_group ON gold.health_insights_gold(age_group);
CREATE INDEX idx_health_insights_risk_category ON gold.health_insights_gold(health_risk_category);
CREATE INDEX idx_health_insights_bmi_category ON gold.health_insights_gold(bmi_category);

-- =============================================================================
-- MODEL METADATA AND PERFORMANCE TRACKING (Enhanced)
-- =============================================================================

-- Enhanced model metadata table
CREATE TABLE gold.model_metadata_gold (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50), -- RandomForest, LogisticRegression, etc.
    
    -- Performance metrics
    accuracy DOUBLE PRECISION,
    precision_score DOUBLE PRECISION,
    recall DOUBLE PRECISION,
    f1_score DOUBLE PRECISION,
    auc_roc DOUBLE PRECISION,
    auc_pr DOUBLE PRECISION,
    
    -- Detailed performance by category
    precision_class_0 DOUBLE PRECISION,
    precision_class_1 DOUBLE PRECISION,
    recall_class_0 DOUBLE PRECISION,
    recall_class_1 DOUBLE PRECISION,
    
    -- Feature importance (top features)
    feature_importance JSONB,
    
    -- Training metadata
    training_dataset_size INTEGER,
    validation_dataset_size INTEGER,
    test_dataset_size INTEGER,
    training_date TIMESTAMP,
    
    -- Model deployment info
    deployment_date TIMESTAMP,
    model_status VARCHAR(20), -- Active, Deprecated, Testing
    
    -- Data and processing info
    data_version VARCHAR(50),
    processing_pipeline_version VARCHAR(50),
    
    -- Notes and comments
    notes TEXT,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for model metadata
CREATE INDEX idx_model_metadata_name_version ON gold.model_metadata_gold(model_name, model_version);
CREATE INDEX idx_model_metadata_status ON gold.model_metadata_gold(model_status);
CREATE INDEX idx_model_metadata_deployment_date ON gold.model_metadata_gold(deployment_date);

-- =============================================================================
-- DATA QUALITY AND MONITORING (Enhanced)
-- =============================================================================

-- Pipeline monitoring table
CREATE TABLE gold.pipeline_monitoring_gold (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    pipeline_stage VARCHAR(50), -- bronze_to_silver, silver_to_gold, etc.
    
    -- Execution metrics
    execution_id UUID DEFAULT uuid_generate_v4(),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    status VARCHAR(20), -- Success, Failed, Warning
    
    -- Data metrics
    records_processed INTEGER,
    records_valid INTEGER,
    records_invalid INTEGER,
    data_quality_score DOUBLE PRECISION,
    
    -- Error and warning details
    error_count INTEGER,
    warning_count INTEGER,
    error_details JSONB,
    warning_details JSONB,
    
    -- Resource usage
    memory_usage_mb DOUBLE PRECISION,
    cpu_usage_percent DOUBLE PRECISION,
    
    -- Data lineage
    input_sources JSONB,
    output_targets JSONB,
    
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for pipeline monitoring
CREATE INDEX idx_pipeline_monitoring_pipeline_name ON gold.pipeline_monitoring_gold(pipeline_name);
CREATE INDEX idx_pipeline_monitoring_status ON gold.pipeline_monitoring_gold(status);
CREATE INDEX idx_pipeline_monitoring_start_time ON gold.pipeline_monitoring_gold(start_time);
CREATE INDEX idx_pipeline_monitoring_execution_id ON gold.pipeline_monitoring_gold(execution_id);

-- =============================================================================
-- ENHANCED ANALYTICS VIEWS FOR GRAFANA
-- =============================================================================

-- Comprehensive daily statistics
CREATE OR REPLACE VIEW analytics.enhanced_daily_stats AS
SELECT 
    processing_date as date,
    COUNT(*) as total_records,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    SUM(CASE WHEN diabetes = 0 THEN 1 ELSE 0 END) as non_diabetic_count,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(age), 2) as avg_age,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    ROUND(AVG(prediction_probability), 2) as avg_prediction_confidence,
    SUM(CASE WHEN health_risk_level = 'High' THEN 1 ELSE 0 END) as high_risk_count,
    SUM(CASE WHEN health_risk_level = 'Critical' THEN 1 ELSE 0 END) as critical_risk_count,
    SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_intervention_count,
    ROUND(AVG(quality_score), 2) as avg_data_quality
FROM gold.diabetes_gold
GROUP BY processing_date
ORDER BY date DESC;

-- Risk level distribution analysis
CREATE OR REPLACE VIEW analytics.risk_level_analysis AS
SELECT 
    health_risk_level,
    COUNT(*) as patient_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as diabetes_rate,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(age), 2) as avg_age,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    ROUND(AVG(prediction_probability), 2) as avg_prediction_confidence
FROM gold.diabetes_gold
GROUP BY health_risk_level
ORDER BY 
    CASE health_risk_level 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        WHEN 'Low' THEN 4 
        ELSE 5 
    END;

-- Intervention priority analysis
CREATE OR REPLACE VIEW analytics.intervention_priority_analysis AS
SELECT 
    intervention_priority,
    COUNT(*) as patient_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    ROUND(AVG(prediction_probability), 2) as avg_prediction_confidence,
    STRING_AGG(DISTINCT estimated_cost_category, ', ') as cost_categories
FROM gold.diabetes_gold
GROUP BY intervention_priority
ORDER BY 
    CASE intervention_priority 
        WHEN 'Immediate' THEN 1 
        WHEN 'Soon' THEN 2 
        WHEN 'Routine' THEN 3 
        WHEN 'Monitor' THEN 4 
        ELSE 5 
    END;

-- Feature correlation analysis for ML insights
CREATE OR REPLACE VIEW analytics.feature_correlation_insights AS
SELECT 
    'Glucose-BMI Interaction' as feature_pair,
    ROUND(AVG(glucose_bmi_interaction), 4) as avg_interaction,
    ROUND(CORR(glucose_bmi_interaction, diabetes::numeric), 4) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE glucose_bmi_interaction IS NOT NULL
UNION ALL
SELECT 
    'Age-Glucose Interaction' as feature_pair,
    ROUND(AVG(age_glucose_interaction), 4) as avg_interaction,
    ROUND(CORR(age_glucose_interaction, diabetes::numeric), 4) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE age_glucose_interaction IS NOT NULL
UNION ALL
SELECT 
    'Insulin-Glucose Interaction' as feature_pair,
    ROUND(AVG(insulin_glucose_interaction), 4) as avg_interaction,
    ROUND(CORR(insulin_glucose_interaction, diabetes::numeric), 4) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE insulin_glucose_interaction IS NOT NULL;

-- Model performance tracking view
CREATE OR REPLACE VIEW analytics.model_performance_tracking AS
SELECT 
    model_name,
    model_version,
    model_type,
    accuracy,
    precision_score,
    recall,
    f1_score,
    auc_roc,
    deployment_date,
    model_status,
    EXTRACT(DAYS FROM CURRENT_DATE - deployment_date::date) as days_since_deployment
FROM gold.model_metadata_gold
WHERE model_status = 'Active'
ORDER BY deployment_date DESC;

-- Latest comprehensive metrics for dashboards
CREATE OR REPLACE VIEW analytics.latest_comprehensive_metrics AS
SELECT 
    'total_patients' as metric_name,
    COUNT(*)::TEXT as metric_value,
    'count' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
UNION ALL
SELECT 
    'diabetic_patients' as metric_name,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END)::TEXT as metric_value,
    'count' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
UNION ALL
SELECT 
    'high_risk_patients' as metric_name,
    SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END)::TEXT as metric_value,
    'count' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
UNION ALL
SELECT 
    'immediate_interventions' as metric_name,
    SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END)::TEXT as metric_value,
    'count' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
UNION ALL
SELECT 
    'avg_prediction_confidence' as metric_name,
    ROUND(AVG(prediction_probability), 3)::TEXT as metric_value,
    'percentage' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
WHERE prediction_probability IS NOT NULL
UNION ALL
SELECT 
    'avg_data_quality' as metric_name,
    ROUND(AVG(quality_score), 3)::TEXT as metric_value,
    'score' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
WHERE quality_score IS NOT NULL;

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Grant permissions for enhanced gold schema
GRANT SELECT ON gold.diabetes_gold TO warehouse;
GRANT SELECT ON gold.health_insights_gold TO warehouse;
GRANT SELECT ON gold.model_metadata_gold TO warehouse;
GRANT SELECT ON gold.pipeline_monitoring_gold TO warehouse;

GRANT INSERT, UPDATE, DELETE ON gold.diabetes_gold TO warehouse;
GRANT INSERT, UPDATE, DELETE ON gold.health_insights_gold TO warehouse;
GRANT INSERT, UPDATE, DELETE ON gold.model_metadata_gold TO warehouse;
GRANT INSERT, UPDATE, DELETE ON gold.pipeline_monitoring_gold TO warehouse;

-- Grant sequence permissions
GRANT USAGE ON ALL SEQUENCES IN SCHEMA gold TO warehouse;

-- Log successful schema enhancement
INSERT INTO gold.model_metadata_gold (model_name, model_version, model_type, notes, created_timestamp)
VALUES ('schema_enhancement', '2.0', 'database_schema', 'Enhanced PostgreSQL schema matching comprehensive Hive gold structure with predictions, risk scores, and BI features', CURRENT_TIMESTAMP);

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Enhanced PostgreSQL Gold Schema created successfully!';
    RAISE NOTICE 'Tables created: diabetes_gold, health_insights_gold, model_metadata_gold, pipeline_monitoring_gold';
    RAISE NOTICE 'Analytics views created with comprehensive metrics for Grafana';
    RAISE NOTICE 'Schema now matches comprehensive Hive gold structure';
END $$;

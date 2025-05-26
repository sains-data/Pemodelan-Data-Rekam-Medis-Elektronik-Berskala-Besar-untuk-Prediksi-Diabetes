-- =============================================================================
-- POSTGRESQL-COMPATIBLE ANALYTICS VIEWS FIX
-- =============================================================================
-- Fix PostgreSQL function compatibility issues in the analytics views
-- =============================================================================

-- Drop the problematic views first
DROP VIEW IF EXISTS analytics.enhanced_daily_stats CASCADE;
DROP VIEW IF EXISTS analytics.risk_level_analysis CASCADE;
DROP VIEW IF EXISTS analytics.intervention_priority_analysis CASCADE;
DROP VIEW IF EXISTS analytics.feature_correlation_insights CASCADE;
DROP VIEW IF EXISTS analytics.model_performance_tracking CASCADE;
DROP VIEW IF EXISTS analytics.latest_comprehensive_metrics CASCADE;

-- =============================================================================
-- FIXED ANALYTICS VIEWS FOR GRAFANA (PostgreSQL Compatible)
-- =============================================================================

-- Comprehensive daily statistics (PostgreSQL compatible)
CREATE OR REPLACE VIEW analytics.enhanced_daily_stats AS
SELECT 
    processing_date as date,
    COUNT(*) as total_records,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    SUM(CASE WHEN diabetes = 0 THEN 1 ELSE 0 END) as non_diabetic_count,
    CAST(ROUND(CAST(AVG(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_glucose,
    CAST(ROUND(CAST(AVG(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_bmi,
    CAST(ROUND(CAST(AVG(age) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_age,
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_prediction_confidence,
    SUM(CASE WHEN health_risk_level = 'High' THEN 1 ELSE 0 END) as high_risk_count,
    SUM(CASE WHEN health_risk_level = 'Critical' THEN 1 ELSE 0 END) as critical_risk_count,
    SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_intervention_count,
    CAST(ROUND(CAST(AVG(quality_score) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_data_quality
FROM gold.diabetes_gold
GROUP BY processing_date
ORDER BY date DESC;

-- Risk level distribution analysis (PostgreSQL compatible)
CREATE OR REPLACE VIEW analytics.risk_level_analysis AS
SELECT 
    health_risk_level,
    COUNT(*) as patient_count,
    CAST(ROUND(CAST((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS NUMERIC), 2) AS DOUBLE PRECISION) as percentage,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    CAST(ROUND(CAST((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate,
    CAST(ROUND(CAST(AVG(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_glucose,
    CAST(ROUND(CAST(AVG(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_bmi,
    CAST(ROUND(CAST(AVG(age) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_age,
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_prediction_confidence
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

-- Intervention priority analysis (PostgreSQL compatible)
CREATE OR REPLACE VIEW analytics.intervention_priority_analysis AS
SELECT 
    intervention_priority,
    COUNT(*) as patient_count,
    CAST(ROUND(CAST((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS NUMERIC), 2) AS DOUBLE PRECISION) as percentage,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_count,
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_prediction_confidence,
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

-- Feature correlation analysis for ML insights (PostgreSQL compatible)
CREATE OR REPLACE VIEW analytics.feature_correlation_insights AS
SELECT 
    'Glucose-BMI Interaction' as feature_pair,
    CAST(ROUND(CAST(AVG(glucose_bmi_interaction) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_interaction,
    CAST(ROUND(CAST(CORR(glucose_bmi_interaction, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE glucose_bmi_interaction IS NOT NULL
UNION ALL
SELECT 
    'Age-Glucose Interaction' as feature_pair,
    CAST(ROUND(CAST(AVG(age_glucose_interaction) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_interaction,
    CAST(ROUND(CAST(CORR(age_glucose_interaction, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE age_glucose_interaction IS NOT NULL
UNION ALL
SELECT 
    'Insulin-Glucose Interaction' as feature_pair,
    CAST(ROUND(CAST(AVG(insulin_glucose_interaction) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_interaction,
    CAST(ROUND(CAST(CORR(insulin_glucose_interaction, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as correlation_with_diabetes
FROM gold.diabetes_gold
WHERE insulin_glucose_interaction IS NOT NULL;

-- Model performance tracking view (PostgreSQL compatible)
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
    CAST(EXTRACT(DAYS FROM (CURRENT_DATE - deployment_date::date)) AS INTEGER) as days_since_deployment
FROM gold.model_metadata_gold
WHERE model_status = 'Active'
ORDER BY deployment_date DESC;

-- Latest comprehensive metrics for dashboards (PostgreSQL compatible)
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
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 3) AS TEXT) as metric_value,
    'percentage' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
WHERE prediction_probability IS NOT NULL
UNION ALL
SELECT 
    'avg_data_quality' as metric_name,
    CAST(ROUND(CAST(AVG(quality_score) AS NUMERIC), 3) AS TEXT) as metric_value,
    'score' as metric_type,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_gold
WHERE quality_score IS NOT NULL;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL-compatible analytics views created successfully!';
    RAISE NOTICE 'All ROUND() and EXTRACT() function issues resolved';
    RAISE NOTICE 'Views ready for Grafana integration';
END $$;

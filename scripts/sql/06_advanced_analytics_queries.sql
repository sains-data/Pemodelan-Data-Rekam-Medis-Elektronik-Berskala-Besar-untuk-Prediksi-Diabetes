-- =============================================================================
-- ADVANCED ANALYTICS QUERIES FOR DEEP ANALYSIS
-- =============================================================================
-- Comprehensive SQL queries for diabetes prediction pipeline deep analysis
-- Optimized for Grafana dashboard visualization and business intelligence
-- =============================================================================

\echo 'Creating advanced analytics views for deep analysis...'

-- =============================================================================
-- TEMPORAL ANALYSIS VIEWS
-- =============================================================================

-- Comprehensive time-series analysis
CREATE OR REPLACE VIEW analytics.temporal_health_trends AS
SELECT 
    DATE_TRUNC('week', processed_timestamp) as week_start,
    DATE_TRUNC('month', processed_timestamp) as month_start,
    COUNT(*) as total_patients,
    
    -- Diabetes progression analysis
    CAST(ROUND(CAST(AVG(CASE WHEN diabetes = 1 THEN 1.0 ELSE 0.0 END) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_prevalence_pct,
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_prediction_confidence,
    
    -- Health metrics trends
    CAST(ROUND(CAST(AVG(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_glucose,
    CAST(ROUND(CAST(AVG(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_bmi,
    CAST(ROUND(CAST(AVG(age) AS NUMERIC), 1) AS DOUBLE PRECISION) as avg_age,
    CAST(ROUND(CAST(AVG(blood_pressure) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_blood_pressure,
    CAST(ROUND(CAST(AVG(insulin) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_insulin,
    
    -- Risk distribution
    COUNT(CASE WHEN health_risk_level = 'Critical' THEN 1 END) as critical_risk_count,
    COUNT(CASE WHEN health_risk_level = 'High' THEN 1 END) as high_risk_count,
    COUNT(CASE WHEN health_risk_level = 'Medium' THEN 1 END) as medium_risk_count,
    COUNT(CASE WHEN health_risk_level = 'Low' THEN 1 END) as low_risk_count,
    
    -- Intervention urgency
    COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_interventions,
    COUNT(CASE WHEN intervention_priority = 'Soon' THEN 1 END) as soon_interventions,
    COUNT(CASE WHEN intervention_priority = 'Routine' THEN 1 END) as routine_interventions,
    
    -- Quality metrics
    CAST(ROUND(CAST(AVG(quality_score) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_data_quality,
    COUNT(CASE WHEN quality_score < 0.8 THEN 1 END) as low_quality_records
    
FROM gold.diabetes_gold
WHERE processed_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('week', processed_timestamp), DATE_TRUNC('month', processed_timestamp)
ORDER BY week_start DESC;

-- =============================================================================
-- DEMOGRAPHIC AND SEGMENTATION ANALYSIS
-- =============================================================================

-- Advanced demographic health analysis
CREATE OR REPLACE VIEW analytics.demographic_health_profile AS
SELECT 
    age_group,
    bmi_category,
    glucose_category,
    
    -- Population metrics
    COUNT(*) as population_count,
    CAST(ROUND(CAST((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS NUMERIC), 2) AS DOUBLE PRECISION) as population_percentage,
    
    -- Diabetes metrics
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_cases,
    CAST(ROUND(CAST((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate,
    
    -- Risk assessment
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(STDDEV(risk_score) AS NUMERIC), 3) AS DOUBLE PRECISION) as risk_score_stddev,
    COUNT(CASE WHEN risk_score > 0.7 THEN 1 END) as high_risk_individuals,
    
    -- Health indicators
    CAST(ROUND(CAST(AVG(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_glucose,
    CAST(ROUND(CAST(AVG(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_bmi,
    CAST(ROUND(CAST(AVG(blood_pressure) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_blood_pressure,
    CAST(ROUND(CAST(AVG(insulin) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_insulin,
    
    -- Intervention analysis
    COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_care_needed,
    COUNT(CASE WHEN intervention_priority = 'Soon' THEN 1 END) as soon_care_needed,
    
    -- Economic impact
    COUNT(CASE WHEN estimated_cost_category = 'High' THEN 1 END) as high_cost_cases,
    COUNT(CASE WHEN estimated_cost_category = 'Medium' THEN 1 END) as medium_cost_cases,
    COUNT(CASE WHEN estimated_cost_category = 'Low' THEN 1 END) as low_cost_cases,
    
    -- Comorbidity risks
    COUNT(CASE WHEN cardiovascular_risk > 0.5 THEN 1 END) as cardiovascular_risk_cases,
    COUNT(CASE WHEN metabolic_syndrome_risk > 0.5 THEN 1 END) as metabolic_syndrome_cases

FROM gold.diabetes_gold
GROUP BY age_group, bmi_category, glucose_category
ORDER BY diabetes_rate DESC, population_count DESC;

-- =============================================================================
-- PREDICTIVE MODEL PERFORMANCE ANALYSIS
-- =============================================================================

-- Model performance deep analysis
CREATE OR REPLACE VIEW analytics.model_performance_deep_analysis AS
SELECT 
    -- Time-based performance
    DATE_TRUNC('day', processed_timestamp) as analysis_date,
    
    -- Prediction confidence analysis
    COUNT(*) as total_predictions,
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_confidence,
    CAST(ROUND(CAST(STDDEV(prediction_probability) AS NUMERIC), 4) AS DOUBLE PRECISION) as confidence_stddev,
    
    -- Confidence distribution
    COUNT(CASE WHEN prediction_probability >= 0.9 THEN 1 END) as very_high_confidence,
    COUNT(CASE WHEN prediction_probability >= 0.7 AND prediction_probability < 0.9 THEN 1 END) as high_confidence,
    COUNT(CASE WHEN prediction_probability >= 0.5 AND prediction_probability < 0.7 THEN 1 END) as medium_confidence,
    COUNT(CASE WHEN prediction_probability < 0.5 THEN 1 END) as low_confidence,
    
    -- Accuracy indicators (pseudo-accuracy based on risk alignment)
    COUNT(CASE WHEN (prediction >= 0.5 AND diabetes = 1) OR (prediction < 0.5 AND diabetes = 0) THEN 1 END) as correct_predictions,
    CAST(ROUND(CAST((COUNT(CASE WHEN (prediction >= 0.5 AND diabetes = 1) OR (prediction < 0.5 AND diabetes = 0) THEN 1 END) * 100.0 / COUNT(*)) AS NUMERIC), 2) AS DOUBLE PRECISION) as estimated_accuracy,
    
    -- False positive/negative analysis
    COUNT(CASE WHEN prediction >= 0.5 AND diabetes = 0 THEN 1 END) as false_positives,
    COUNT(CASE WHEN prediction < 0.5 AND diabetes = 1 THEN 1 END) as false_negatives,
    COUNT(CASE WHEN prediction >= 0.5 AND diabetes = 1 THEN 1 END) as true_positives,
    COUNT(CASE WHEN prediction < 0.5 AND diabetes = 0 THEN 1 END) as true_negatives,
    
    -- Risk-stratified performance
    CAST(ROUND(CAST(AVG(CASE WHEN health_risk_level = 'Critical' THEN prediction_probability END) AS NUMERIC), 4) AS DOUBLE PRECISION) as critical_risk_avg_confidence,
    CAST(ROUND(CAST(AVG(CASE WHEN health_risk_level = 'High' THEN prediction_probability END) AS NUMERIC), 4) AS DOUBLE PRECISION) as high_risk_avg_confidence,
    CAST(ROUND(CAST(AVG(CASE WHEN health_risk_level = 'Medium' THEN prediction_probability END) AS NUMERIC), 4) AS DOUBLE PRECISION) as medium_risk_avg_confidence,
    CAST(ROUND(CAST(AVG(CASE WHEN health_risk_level = 'Low' THEN prediction_probability END) AS NUMERIC), 4) AS DOUBLE PRECISION) as low_risk_avg_confidence

FROM gold.diabetes_gold
WHERE prediction_probability IS NOT NULL
GROUP BY DATE_TRUNC('day', processed_timestamp)
ORDER BY analysis_date DESC;

-- =============================================================================
-- CLINICAL INSIGHTS AND FEATURE ANALYSIS
-- =============================================================================

-- Advanced feature interaction analysis
CREATE OR REPLACE VIEW analytics.clinical_feature_insights AS
SELECT 
    'Feature Interactions' as analysis_type,
    
    -- Glucose-BMI correlation analysis
    CAST(ROUND(CAST(CORR(glucose, bmi) AS NUMERIC), 4) AS DOUBLE PRECISION) as glucose_bmi_correlation,
    CAST(ROUND(CAST(AVG(glucose_bmi_interaction) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_glucose_bmi_interaction,
    
    -- Age-related correlations
    CAST(ROUND(CAST(CORR(age, glucose) AS NUMERIC), 4) AS DOUBLE PRECISION) as age_glucose_correlation,
    CAST(ROUND(CAST(CORR(age, bmi) AS NUMERIC), 4) AS DOUBLE PRECISION) as age_bmi_correlation,
    CAST(ROUND(CAST(CORR(age, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as age_diabetes_correlation,
    
    -- Insulin resistance indicators
    CAST(ROUND(CAST(CORR(insulin, glucose) AS NUMERIC), 4) AS DOUBLE PRECISION) as insulin_glucose_correlation,
    CAST(ROUND(CAST(AVG(insulin_glucose_interaction) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_insulin_glucose_interaction,
    
    -- Blood pressure relationships
    CAST(ROUND(CAST(CORR(blood_pressure, bmi) AS NUMERIC), 4) AS DOUBLE PRECISION) as bp_bmi_correlation,
    CAST(ROUND(CAST(CORR(blood_pressure, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as bp_diabetes_correlation,
    
    -- Pregnancy factor analysis
    CAST(ROUND(CAST(CORR(pregnancies, diabetes::numeric) AS NUMERIC), 4) AS DOUBLE PRECISION) as pregnancy_diabetes_correlation,
    CAST(ROUND(CAST(AVG(pregnancy_risk_factor) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_pregnancy_risk_factor

FROM gold.diabetes_gold
WHERE glucose IS NOT NULL AND bmi IS NOT NULL;

-- Threshold analysis for clinical decision support
CREATE OR REPLACE VIEW analytics.clinical_threshold_analysis AS
SELECT 
    threshold_type,
    threshold_value,
    population_above_threshold,
    diabetes_cases_above_threshold,
    diabetes_rate_above_threshold,
    avg_risk_score_above_threshold,
    intervention_recommendations
FROM (
    -- Glucose thresholds
    SELECT 
        'Glucose' as threshold_type,
        140 as threshold_value,
        COUNT(CASE WHEN glucose >= 140 THEN 1 END) as population_above_threshold,
        COUNT(CASE WHEN glucose >= 140 AND diabetes = 1 THEN 1 END) as diabetes_cases_above_threshold,
        CAST(ROUND(CAST((COUNT(CASE WHEN glucose >= 140 AND diabetes = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN glucose >= 140 THEN 1 END), 0)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate_above_threshold,
        CAST(ROUND(CAST(AVG(CASE WHEN glucose >= 140 THEN risk_score END) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score_above_threshold,
        'Monitor glucose levels, consider glucose tolerance test' as intervention_recommendations
    FROM gold.diabetes_gold
    
    UNION ALL
    
    -- BMI thresholds
    SELECT 
        'BMI' as threshold_type,
        30 as threshold_value,
        COUNT(CASE WHEN bmi >= 30 THEN 1 END) as population_above_threshold,
        COUNT(CASE WHEN bmi >= 30 AND diabetes = 1 THEN 1 END) as diabetes_cases_above_threshold,
        CAST(ROUND(CAST((COUNT(CASE WHEN bmi >= 30 AND diabetes = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN bmi >= 30 THEN 1 END), 0)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate_above_threshold,
        CAST(ROUND(CAST(AVG(CASE WHEN bmi >= 30 THEN risk_score END) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score_above_threshold,
        'Weight management program, nutritional counseling' as intervention_recommendations
    FROM gold.diabetes_gold
    
    UNION ALL
    
    -- Age thresholds
    SELECT 
        'Age' as threshold_type,
        45 as threshold_value,
        COUNT(CASE WHEN age >= 45 THEN 1 END) as population_above_threshold,
        COUNT(CASE WHEN age >= 45 AND diabetes = 1 THEN 1 END) as diabetes_cases_above_threshold,
        CAST(ROUND(CAST((COUNT(CASE WHEN age >= 45 AND diabetes = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN age >= 45 THEN 1 END), 0)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate_above_threshold,
        CAST(ROUND(CAST(AVG(CASE WHEN age >= 45 THEN risk_score END) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score_above_threshold,
        'Annual diabetes screening, lifestyle modification' as intervention_recommendations
    FROM gold.diabetes_gold
    
    UNION ALL
    
    -- Blood pressure thresholds
    SELECT 
        'Blood Pressure' as threshold_type,
        140 as threshold_value,
        COUNT(CASE WHEN blood_pressure >= 140 THEN 1 END) as population_above_threshold,
        COUNT(CASE WHEN blood_pressure >= 140 AND diabetes = 1 THEN 1 END) as diabetes_cases_above_threshold,
        CAST(ROUND(CAST((COUNT(CASE WHEN blood_pressure >= 140 AND diabetes = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN blood_pressure >= 140 THEN 1 END), 0)) AS NUMERIC), 2) AS DOUBLE PRECISION) as diabetes_rate_above_threshold,
        CAST(ROUND(CAST(AVG(CASE WHEN blood_pressure >= 140 THEN risk_score END) AS NUMERIC), 3) AS DOUBLE PRECISION) as avg_risk_score_above_threshold,
        'Blood pressure management, cardiovascular screening' as intervention_recommendations
    FROM gold.diabetes_gold
) threshold_analysis
ORDER BY diabetes_rate_above_threshold DESC;

-- =============================================================================
-- OPERATIONAL AND DATA QUALITY INSIGHTS
-- =============================================================================

-- Comprehensive data quality dashboard
CREATE OR REPLACE VIEW analytics.data_quality_comprehensive AS
SELECT 
    layer_name,
    total_records,
    data_completeness_score,
    data_accuracy_score,
    data_consistency_score,
    overall_quality_score,
    quality_issues,
    last_quality_check,
    recommendations
FROM (
    SELECT 
        'Bronze Layer' as layer_name,
        COUNT(*) as total_records,
        CAST(ROUND(CAST((COUNT(*) - COUNT(CASE WHEN glucose IS NULL OR bmi IS NULL OR age IS NULL THEN 1 END)) * 100.0 / COUNT(*) AS NUMERIC), 2) AS DOUBLE PRECISION) as data_completeness_score,
        CAST(ROUND(CAST((COUNT(*) - COUNT(CASE WHEN glucose < 0 OR glucose > 400 OR bmi < 10 OR bmi > 60 OR age < 0 OR age > 120 THEN 1 END)) * 100.0 / COUNT(*) AS NUMERIC), 2) AS DOUBLE PRECISION) as data_accuracy_score,
        95.0 as data_consistency_score, -- Placeholder
        CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as overall_quality_score,
        COUNT(CASE WHEN glucose IS NULL OR bmi IS NULL OR age IS NULL OR glucose < 0 OR glucose > 400 OR bmi < 10 OR bmi > 60 OR age < 0 OR age > 120 THEN 1 END) as quality_issues,
        MAX(processed_timestamp) as last_quality_check,
        'Monitor data ingestion process, validate source data quality' as recommendations
    FROM gold.diabetes_gold
    
    UNION ALL
    
    SELECT 
        'Gold Layer' as layer_name,
        COUNT(*) as total_records,
        CAST(ROUND(CAST((COUNT(*) - COUNT(CASE WHEN prediction_probability IS NULL OR risk_score IS NULL THEN 1 END)) * 100.0 / COUNT(*) AS NUMERIC), 2) AS DOUBLE PRECISION) as data_completeness_score,
        CAST(ROUND(CAST((COUNT(*) - COUNT(CASE WHEN prediction_probability < 0 OR prediction_probability > 1 OR risk_score < 0 OR risk_score > 1 THEN 1 END)) * 100.0 / COUNT(*) AS NUMERIC), 2) AS DOUBLE PRECISION) as data_accuracy_score,
        98.0 as data_consistency_score, -- Placeholder
        CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as overall_quality_score,
        COUNT(CASE WHEN prediction_probability IS NULL OR risk_score IS NULL OR prediction_probability < 0 OR prediction_probability > 1 OR risk_score < 0 OR risk_score > 1 THEN 1 END) as quality_issues,
        MAX(processed_timestamp) as last_quality_check,
        'Monitor ML model performance, validate feature engineering' as recommendations
    FROM gold.diabetes_gold
) quality_metrics;

-- =============================================================================
-- BUSINESS INTELLIGENCE AND KPI TRACKING
-- =============================================================================

-- Executive dashboard KPIs
CREATE OR REPLACE VIEW analytics.executive_kpi_dashboard AS
SELECT 
    'Healthcare Impact' as kpi_category,
    total_patients,
    high_risk_patients,
    immediate_care_needed,
    diabetes_prevention_opportunities,
    estimated_cost_savings,
    population_health_score,
    system_efficiency_score,
    data_quality_score,
    last_updated
FROM (
    SELECT 
        COUNT(*) as total_patients,
        COUNT(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 END) as high_risk_patients,
        COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_care_needed,
        COUNT(CASE WHEN diabetes = 0 AND risk_score > 0.5 THEN 1 END) as diabetes_prevention_opportunities,
        
        -- Estimated cost impact (placeholder calculation)
        COUNT(CASE WHEN estimated_cost_category = 'High' THEN 1 END) * 10000 + 
        COUNT(CASE WHEN estimated_cost_category = 'Medium' THEN 1 END) * 5000 +
        COUNT(CASE WHEN estimated_cost_category = 'Low' THEN 1 END) * 2000 as estimated_cost_savings,
        
        CAST(ROUND(CAST((100 - AVG(risk_score) * 100) AS NUMERIC), 2) AS DOUBLE PRECISION) as population_health_score,
        CAST(ROUND(CAST(AVG(prediction_probability) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as system_efficiency_score,
        CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as data_quality_score,
        MAX(processed_timestamp) as last_updated
        
    FROM gold.diabetes_gold
) kpi_calc;

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Grant read permissions to analytics views
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO warehouse;
GRANT SELECT ON analytics.temporal_health_trends TO PUBLIC;
GRANT SELECT ON analytics.demographic_health_profile TO PUBLIC;
GRANT SELECT ON analytics.model_performance_deep_analysis TO PUBLIC;
GRANT SELECT ON analytics.clinical_feature_insights TO PUBLIC;
GRANT SELECT ON analytics.clinical_threshold_analysis TO PUBLIC;
GRANT SELECT ON analytics.data_quality_comprehensive TO PUBLIC;
GRANT SELECT ON analytics.executive_kpi_dashboard TO PUBLIC;

\echo 'Advanced analytics views created successfully!'
\echo 'Views available for Grafana dashboards:'
\echo '- analytics.temporal_health_trends'
\echo '- analytics.demographic_health_profile'
\echo '- analytics.model_performance_deep_analysis'
\echo '- analytics.clinical_feature_insights'
\echo '- analytics.clinical_threshold_analysis'
\echo '- analytics.data_quality_comprehensive'
\echo '- analytics.executive_kpi_dashboard'

-- =============================================================================
-- MONITORING AND ALERTING QUERIES FOR GRAFANA
-- =============================================================================
-- Real-time monitoring, alerting, and operational intelligence queries
-- =============================================================================

\echo 'Creating monitoring and alerting views for Grafana...'

-- =============================================================================
-- REAL-TIME MONITORING VIEWS
-- =============================================================================

-- Real-time system health monitoring
CREATE OR REPLACE VIEW analytics.real_time_system_health AS
SELECT 
    'System Health' as category,
    
    -- Data pipeline health
    CASE 
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 'Healthy'
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '6 hours' THEN 'Warning'
        ELSE 'Critical'
    END as data_pipeline_status,
    
    -- Data freshness
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(processed_timestamp)))/3600 as data_age_hours,
    
    -- Processing rate
    COUNT(CASE WHEN processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) as records_last_hour,
    COUNT(CASE WHEN processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 END) as records_last_day,
    
    -- Quality indicators
    CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_quality_score,
    COUNT(CASE WHEN quality_score < 0.8 THEN 1 END) as low_quality_count,
    
    -- Model performance indicators
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_prediction_confidence,
    COUNT(CASE WHEN prediction_probability IS NULL THEN 1 END) as missing_predictions,
    
    -- Risk distribution
    COUNT(CASE WHEN health_risk_level = 'Critical' THEN 1 END) as critical_risk_alerts,
    COUNT(CASE WHEN health_risk_level = 'High' THEN 1 END) as high_risk_alerts,
    COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_interventions,
    
    -- Data anomalies
    COUNT(CASE WHEN glucose > 400 OR glucose < 0 THEN 1 END) as glucose_anomalies,
    COUNT(CASE WHEN bmi > 60 OR bmi < 10 THEN 1 END) as bmi_anomalies,
    COUNT(CASE WHEN age > 120 OR age < 0 THEN 1 END) as age_anomalies,
    
    CURRENT_TIMESTAMP as last_updated

FROM gold.diabetes_gold;

-- Alert threshold monitoring
CREATE OR REPLACE VIEW analytics.alert_threshold_monitoring AS
SELECT 
    alert_type,
    current_value,
    threshold_value,
    alert_level,
    alert_message,
    recommendation,
    last_triggered
FROM (
    -- High glucose alert
    SELECT 
        'High Glucose Patients' as alert_type,
        COUNT(CASE WHEN glucose > 200 THEN 1 END) as current_value,
        50 as threshold_value,
        CASE 
            WHEN COUNT(CASE WHEN glucose > 200 THEN 1 END) > 100 THEN 'Critical'
            WHEN COUNT(CASE WHEN glucose > 200 THEN 1 END) > 50 THEN 'Warning'
            ELSE 'Normal'
        END as alert_level,
        'High number of patients with glucose > 200 mg/dL detected' as alert_message,
        'Investigate data quality and consider immediate medical review' as recommendation,
        MAX(processed_timestamp) as last_triggered
    FROM gold.diabetes_gold
    WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    
    UNION ALL
    
    -- Data quality alert
    SELECT 
        'Data Quality Degradation' as alert_type,
        CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as current_value,
        85.0 as threshold_value,
        CASE 
            WHEN AVG(quality_score) < 0.7 THEN 'Critical'
            WHEN AVG(quality_score) < 0.85 THEN 'Warning'
            ELSE 'Normal'
        END as alert_level,
        'Data quality score below acceptable threshold' as alert_message,
        'Review data ingestion process and data sources' as recommendation,
        MAX(processed_timestamp) as last_triggered
    FROM gold.diabetes_gold
    WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    
    UNION ALL
    
    -- Model confidence alert
    SELECT 
        'Low Model Confidence' as alert_type,
        CAST(ROUND(CAST(AVG(prediction_probability) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as current_value,
        70.0 as threshold_value,
        CASE 
            WHEN AVG(prediction_probability) < 0.5 THEN 'Critical'
            WHEN AVG(prediction_probability) < 0.7 THEN 'Warning'
            ELSE 'Normal'
        END as alert_level,
        'Model prediction confidence below acceptable level' as alert_message,
        'Consider model retraining or feature engineering review' as recommendation,
        MAX(processed_timestamp) as last_triggered
    FROM gold.diabetes_gold
    WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    AND prediction_probability IS NOT NULL
    
    UNION ALL
    
    -- High risk patient surge
    SELECT 
        'High Risk Patient Surge' as alert_type,
        COUNT(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 END) as current_value,
        100 as threshold_value,
        CASE 
            WHEN COUNT(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 END) > 200 THEN 'Critical'
            WHEN COUNT(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 END) > 100 THEN 'Warning'
            ELSE 'Normal'
        END as alert_level,
        'Unusual increase in high-risk patients detected' as alert_message,
        'Alert healthcare teams and prepare additional resources' as recommendation,
        MAX(processed_timestamp) as last_triggered
    FROM gold.diabetes_gold
    WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    
) alert_analysis
WHERE alert_level != 'Normal'
ORDER BY 
    CASE alert_level 
        WHEN 'Critical' THEN 1 
        WHEN 'Warning' THEN 2 
        ELSE 3 
    END;

-- =============================================================================
-- PERFORMANCE MONITORING VIEWS
-- =============================================================================

-- ETL pipeline performance monitoring
CREATE OR REPLACE VIEW analytics.etl_pipeline_performance AS
SELECT 
    DATE_TRUNC('hour', processed_timestamp) as processing_hour,
    
    -- Volume metrics
    COUNT(*) as records_processed,
    COUNT(*) / 3600.0 as records_per_second,
    
    -- Quality metrics
    COUNT(CASE WHEN quality_score >= 0.9 THEN 1 END) as high_quality_records,
    COUNT(CASE WHEN quality_score < 0.8 THEN 1 END) as low_quality_records,
    CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_quality_score,
    
    -- Processing efficiency
    COUNT(CASE WHEN prediction_probability IS NOT NULL THEN 1 END) as successful_predictions,
    COUNT(CASE WHEN prediction_probability IS NULL THEN 1 END) as failed_predictions,
    CAST(ROUND(CAST((COUNT(CASE WHEN prediction_probability IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) AS NUMERIC), 2) AS DOUBLE PRECISION) as prediction_success_rate,
    
    -- Feature engineering success
    COUNT(CASE WHEN glucose_bmi_interaction IS NOT NULL THEN 1 END) as feature_engineering_success,
    COUNT(CASE WHEN risk_score IS NOT NULL THEN 1 END) as risk_scoring_success,
    
    -- Error indicators
    COUNT(CASE WHEN glucose IS NULL OR bmi IS NULL OR age IS NULL THEN 1 END) as missing_data_errors,
    COUNT(CASE WHEN glucose < 0 OR glucose > 600 THEN 1 END) as outlier_errors,
    
    -- Processing latency (estimated)
    EXTRACT(EPOCH FROM (MAX(processed_timestamp) - MIN(processed_timestamp))) as processing_duration_seconds

FROM gold.diabetes_gold
WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', processed_timestamp)
ORDER BY processing_hour DESC;

-- Model drift detection
CREATE OR REPLACE VIEW analytics.model_drift_detection AS
SELECT 
    DATE_TRUNC('day', processed_timestamp) as analysis_date,
    
    -- Feature distribution changes
    CAST(ROUND(CAST(AVG(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_glucose,
    CAST(ROUND(CAST(STDDEV(glucose) AS NUMERIC), 2) AS DOUBLE PRECISION) as stddev_glucose,
    CAST(ROUND(CAST(AVG(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_bmi,
    CAST(ROUND(CAST(STDDEV(bmi) AS NUMERIC), 2) AS DOUBLE PRECISION) as stddev_bmi,
    CAST(ROUND(CAST(AVG(age) AS NUMERIC), 2) AS DOUBLE PRECISION) as avg_age,
    CAST(ROUND(CAST(STDDEV(age) AS NUMERIC), 2) AS DOUBLE PRECISION) as stddev_age,
    
    -- Prediction distribution changes
    CAST(ROUND(CAST(AVG(prediction_probability) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_prediction_prob,
    CAST(ROUND(CAST(STDDEV(prediction_probability) AS NUMERIC), 4) AS DOUBLE PRECISION) as stddev_prediction_prob,
    
    -- Risk score distribution
    CAST(ROUND(CAST(AVG(risk_score) AS NUMERIC), 4) AS DOUBLE PRECISION) as avg_risk_score,
    CAST(ROUND(CAST(STDDEV(risk_score) AS NUMERIC), 4) AS DOUBLE PRECISION) as stddev_risk_score,
    
    -- Category distribution changes
    COUNT(CASE WHEN health_risk_level = 'High' THEN 1 END) * 100.0 / COUNT(*) as high_risk_percentage,
    COUNT(CASE WHEN health_risk_level = 'Critical' THEN 1 END) * 100.0 / COUNT(*) as critical_risk_percentage,
    COUNT(CASE WHEN diabetes = 1 THEN 1 END) * 100.0 / COUNT(*) as diabetes_rate,
    
    -- Data volume
    COUNT(*) as daily_volume,
    
    -- Drift indicators (compare with historical averages)
    ABS(AVG(glucose) - 120.0) as glucose_drift_magnitude,
    ABS(AVG(bmi) - 30.0) as bmi_drift_magnitude,
    ABS(AVG(prediction_probability) - 0.5) as prediction_drift_magnitude

FROM gold.diabetes_gold
WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', processed_timestamp)
ORDER BY analysis_date DESC;

-- =============================================================================
-- BUSINESS INTELLIGENCE MONITORING
-- =============================================================================

-- Healthcare impact metrics
CREATE OR REPLACE VIEW analytics.healthcare_impact_metrics AS
SELECT 
    DATE_TRUNC('week', processed_timestamp) as week_start,
    
    -- Patient volume and risk
    COUNT(*) as total_patients_screened,
    COUNT(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 END) as high_risk_patients_identified,
    COUNT(CASE WHEN diabetes = 0 AND risk_score > 0.6 THEN 1 END) as prevention_opportunities,
    
    -- Early intervention metrics
    COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_interventions_needed,
    COUNT(CASE WHEN intervention_priority = 'Soon' THEN 1 END) as soon_interventions_needed,
    
    -- Cost impact estimates (placeholder calculations)
    COUNT(CASE WHEN estimated_cost_category = 'High' THEN 1 END) * 15000 as estimated_high_cost_impact,
    COUNT(CASE WHEN estimated_cost_category = 'Medium' THEN 1 END) * 8000 as estimated_medium_cost_impact,
    COUNT(CASE WHEN estimated_cost_category = 'Low' THEN 1 END) * 3000 as estimated_low_cost_impact,
    
    -- Prevention impact
    COUNT(CASE WHEN diabetes = 0 AND risk_score > 0.7 THEN 1 END) * 25000 as potential_cost_savings_prevention,
    
    -- Population health indicators
    CAST(ROUND(CAST(AVG(CASE WHEN diabetes = 0 THEN (1 - risk_score) * 100 ELSE 0 END) AS NUMERIC), 2) AS DOUBLE PRECISION) as population_health_index,
    CAST(ROUND(CAST(AVG(prediction_probability) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as screening_effectiveness_score,
    
    -- Demographic coverage
    COUNT(DISTINCT age_group) as age_groups_covered,
    COUNT(DISTINCT bmi_category) as bmi_categories_covered,
    
    -- Quality indicators
    CAST(ROUND(CAST(AVG(quality_score) * 100 AS NUMERIC), 2) AS DOUBLE PRECISION) as data_quality_score,
    COUNT(CASE WHEN quality_score < 0.8 THEN 1 END) as data_quality_issues

FROM gold.diabetes_gold
WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', processed_timestamp)
ORDER BY week_start DESC;

-- Service level agreement (SLA) monitoring
CREATE OR REPLACE VIEW analytics.sla_monitoring AS
SELECT 
    'Data Processing SLA' as sla_type,
    
    -- Availability metrics
    CASE 
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 99.9
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '4 hours' THEN 95.0
        ELSE 90.0
    END as availability_percentage,
    
    -- Data freshness SLA
    CASE 
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '30 minutes' THEN 'Met'
        WHEN MAX(processed_timestamp) > CURRENT_TIMESTAMP - INTERVAL '2 hours' THEN 'Warning'
        ELSE 'Breached'
    END as data_freshness_sla,
    
    -- Quality SLA
    CASE 
        WHEN AVG(quality_score) >= 0.95 THEN 'Exceeded'
        WHEN AVG(quality_score) >= 0.90 THEN 'Met'
        WHEN AVG(quality_score) >= 0.85 THEN 'Warning'
        ELSE 'Breached'
    END as data_quality_sla,
    
    -- Processing volume SLA
    CASE 
        WHEN COUNT(CASE WHEN processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) >= 100 THEN 'Met'
        WHEN COUNT(CASE WHEN processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' THEN 1 END) >= 50 THEN 'Warning'
        ELSE 'Breached'
    END as volume_processing_sla,
    
    -- Model performance SLA
    CASE 
        WHEN AVG(prediction_probability) >= 0.8 THEN 'Exceeded'
        WHEN AVG(prediction_probability) >= 0.7 THEN 'Met'
        WHEN AVG(prediction_probability) >= 0.6 THEN 'Warning'
        ELSE 'Breached'
    END as model_performance_sla,
    
    -- Response time SLA (estimated)
    EXTRACT(EPOCH FROM (MAX(processed_timestamp) - MIN(processed_timestamp))) / COUNT(*) as avg_processing_time_per_record,
    
    CURRENT_TIMESTAMP as sla_check_time

FROM gold.diabetes_gold
WHERE processed_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Grant read permissions to monitoring views
GRANT SELECT ON analytics.real_time_system_health TO PUBLIC;
GRANT SELECT ON analytics.alert_threshold_monitoring TO PUBLIC;
GRANT SELECT ON analytics.etl_pipeline_performance TO PUBLIC;
GRANT SELECT ON analytics.model_drift_detection TO PUBLIC;
GRANT SELECT ON analytics.healthcare_impact_metrics TO PUBLIC;
GRANT SELECT ON analytics.sla_monitoring TO PUBLIC;

\echo 'Monitoring and alerting views created successfully!'
\echo 'Real-time monitoring views available:'
\echo '- analytics.real_time_system_health'
\echo '- analytics.alert_threshold_monitoring'
\echo '- analytics.etl_pipeline_performance'
\echo '- analytics.model_drift_detection'
\echo '- analytics.healthcare_impact_metrics'
\echo '- analytics.sla_monitoring'

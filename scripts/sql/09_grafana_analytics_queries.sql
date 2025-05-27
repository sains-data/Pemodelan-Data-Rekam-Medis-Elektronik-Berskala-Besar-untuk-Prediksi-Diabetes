-- =============================================================================
-- GRAFANA ANALYTICS QUERIES FOR DIABETES PREDICTION DASHBOARD
-- =============================================================================
-- Comprehensive SQL queries designed for Grafana dashboard visualization
-- Each query is optimized for time-series, aggregations, and real-time monitoring
-- =============================================================================

-- =============================================================================
-- 1. EXECUTIVE KPI DASHBOARD QUERIES
-- =============================================================================

-- Executive Summary Statistics (for text panels)
WITH kpi_summary AS (
    SELECT 
        COUNT(*) as total_patients,
        SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_cases,
        ROUND(AVG(CASE WHEN diabetes = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as diabetes_rate,
        SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
        ROUND(AVG(risk_score) * 100, 1) as avg_risk_score,
        SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_interventions,
        SUM(estimated_annual_cost) as total_estimated_cost
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    'Total Patients' as metric, total_patients::text as value, 'count' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'Diabetes Cases' as metric, diabetes_cases::text as value, 'count' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'Diabetes Rate' as metric, diabetes_rate::text || '%' as value, 'percentage' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'High Risk Patients' as metric, high_risk_patients::text as value, 'count' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'Average Risk Score' as metric, avg_risk_score::text || '%' as value, 'percentage' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'Immediate Interventions' as metric, immediate_interventions::text as value, 'count' as unit
FROM kpi_summary
UNION ALL
SELECT 
    'Estimated Annual Cost' as metric, '$' || ROUND(total_estimated_cost/1000000, 1)::text || 'M' as value, 'currency' as unit
FROM kpi_summary;

-- =============================================================================
-- 2. TIME SERIES ANALYSIS QUERIES
-- =============================================================================

-- Daily Patient Processing Trend (for time series visualization)
SELECT 
    processing_date as time,
    COUNT(*) as total_patients,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_cases,
    AVG(risk_score) * 100 as avg_risk_score,
    AVG(quality_score) * 100 as data_quality_score
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY processing_date
ORDER BY processing_date;

-- Hourly Processing Statistics (for real-time monitoring)
SELECT 
    DATE_TRUNC('hour', processed_timestamp) as time,
    COUNT(*) as records_processed,
    AVG(quality_score) * 100 as quality_score,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_positive,
    COUNT(DISTINCT health_risk_level) as risk_levels_found
FROM gold.diabetes_gold 
WHERE processed_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', processed_timestamp)
ORDER BY time;

-- =============================================================================
-- 3. CLINICAL RISK DISTRIBUTION QUERIES
-- =============================================================================

-- Risk Level Distribution (for pie chart)
SELECT 
    health_risk_level as risk_level,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY health_risk_level
ORDER BY patient_count DESC;

-- Intervention Priority Distribution (for donut chart)
SELECT 
    intervention_priority as priority,
    COUNT(*) as count,
    AVG(estimated_annual_cost) as avg_cost,
    SUM(estimated_annual_cost) as total_cost
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY intervention_priority
ORDER BY 
    CASE intervention_priority 
        WHEN 'Immediate' THEN 1
        WHEN 'High' THEN 2  
        WHEN 'Medium' THEN 3
        WHEN 'Low' THEN 4
        ELSE 5
    END;

-- =============================================================================
-- 4. DEMOGRAPHIC ANALYSIS QUERIES
-- =============================================================================

-- Age Group Analysis (for bar chart)
SELECT 
    age_group,
    COUNT(*) as total_patients,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_cases,
    ROUND(AVG(CASE WHEN diabetes = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as diabetes_rate,
    AVG(risk_score) * 100 as avg_risk_score,
    AVG(bmi) as avg_bmi,
    AVG(glucose) as avg_glucose
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY age_group
ORDER BY 
    CASE age_group 
        WHEN '20-29' THEN 1
        WHEN '30-39' THEN 2
        WHEN '40-49' THEN 3
        WHEN '50-59' THEN 4
        WHEN '60+' THEN 5
        ELSE 6
    END;

-- BMI Category Analysis (for horizontal bar chart)
SELECT 
    bmi_category,
    COUNT(*) as patient_count,
    ROUND(AVG(bmi), 1) as avg_bmi,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_cases,
    ROUND(AVG(CASE WHEN diabetes = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as diabetes_rate,
    SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_count
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY bmi_category
ORDER BY 
    CASE bmi_category 
        WHEN 'Underweight' THEN 1
        WHEN 'Normal' THEN 2
        WHEN 'Overweight' THEN 3
        WHEN 'Obese' THEN 4
        WHEN 'Severely Obese' THEN 5
        ELSE 6
    END;

-- =============================================================================
-- 5. GLUCOSE AND CLINICAL METRICS QUERIES
-- =============================================================================

-- Glucose Level Distribution (for histogram)
SELECT 
    CASE 
        WHEN glucose < 70 THEN 'Hypoglycemic (<70)'
        WHEN glucose < 100 THEN 'Normal (70-99)'
        WHEN glucose < 126 THEN 'Pre-diabetic (100-125)'
        ELSE 'Diabetic (≥126)'
    END as glucose_range,
    COUNT(*) as patient_count,
    AVG(glucose) as avg_glucose,
    ROUND(AVG(CASE WHEN diabetes = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as diabetes_rate
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
  AND glucose > 0
GROUP BY 
    CASE 
        WHEN glucose < 70 THEN 'Hypoglycemic (<70)'
        WHEN glucose < 100 THEN 'Normal (70-99)'
        WHEN glucose < 126 THEN 'Pre-diabetic (100-125)'
        ELSE 'Diabetic (≥126)'
    END
ORDER BY 
    CASE 
        WHEN glucose < 70 THEN 1
        WHEN glucose < 100 THEN 2
        WHEN glucose < 126 THEN 3
        ELSE 4
    END;

-- Clinical Correlation Matrix (for heatmap)
SELECT 
    'Glucose vs BMI' as correlation_pair,
    ROUND(CORR(glucose, bmi)::NUMERIC, 3) as correlation_coefficient
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
  AND glucose > 0 AND bmi > 0
UNION ALL
SELECT 
    'Glucose vs Age' as correlation_pair,
    ROUND(CORR(glucose, age)::NUMERIC, 3) as correlation_coefficient
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
  AND glucose > 0
UNION ALL
SELECT 
    'BMI vs Blood Pressure' as correlation_pair,
    ROUND(CORR(bmi, blood_pressure)::NUMERIC, 3) as correlation_coefficient
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
  AND bmi > 0 AND blood_pressure > 0
UNION ALL
SELECT 
    'Age vs Risk Score' as correlation_pair,
    ROUND(CORR(age, risk_score)::NUMERIC, 3) as correlation_coefficient
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days';

-- =============================================================================
-- 6. MODEL PERFORMANCE AND ACCURACY QUERIES
-- =============================================================================

-- Prediction Accuracy Analysis (for gauge/stat panel)
WITH prediction_analysis AS (
    SELECT 
        diabetes as actual,
        CASE WHEN risk_score >= 0.5 THEN 1 ELSE 0 END as predicted,
        COUNT(*) as count
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY diabetes, CASE WHEN risk_score >= 0.5 THEN 1 ELSE 0 END
),
confusion_matrix AS (
    SELECT 
        SUM(CASE WHEN actual = 1 AND predicted = 1 THEN count ELSE 0 END) as true_positive,
        SUM(CASE WHEN actual = 0 AND predicted = 0 THEN count ELSE 0 END) as true_negative,
        SUM(CASE WHEN actual = 0 AND predicted = 1 THEN count ELSE 0 END) as false_positive,
        SUM(CASE WHEN actual = 1 AND predicted = 0 THEN count ELSE 0 END) as false_negative
    FROM prediction_analysis
)
SELECT 
    'Accuracy' as metric,
    ROUND((true_positive + true_negative) * 100.0 / (true_positive + true_negative + false_positive + false_negative), 1) as value,
    '%' as unit
FROM confusion_matrix
UNION ALL
SELECT 
    'Precision' as metric,
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_positive, 0), 1) as value,
    '%' as unit
FROM confusion_matrix
UNION ALL
SELECT 
    'Recall' as metric,
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_negative, 0), 1) as value,
    '%' as unit
FROM confusion_matrix
UNION ALL
SELECT 
    'F1 Score' as metric,
    ROUND(2 * true_positive * 100.0 / NULLIF(2 * true_positive + false_positive + false_negative, 0), 1) as value,
    '%' as unit
FROM confusion_matrix;

-- =============================================================================
-- 7. FEATURE IMPORTANCE AND CORRELATION QUERIES
-- =============================================================================

-- Feature Importance Analysis (for bar chart)
SELECT 
    feature_name,
    importance_score,
    clinical_significance,
    correlation_with_diabetes
FROM (
    SELECT 'Glucose' as feature_name, 0.35 as importance_score, 'Primary diagnostic indicator' as clinical_significance,
           ROUND(CORR(glucose, diabetes)::NUMERIC, 3) as correlation_with_diabetes
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'BMI' as feature_name, 0.25 as importance_score, 'Strong obesity indicator',
           ROUND(CORR(bmi, diabetes)::NUMERIC, 3)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'Age' as feature_name, 0.20 as importance_score, 'Age-related risk factor',
           ROUND(CORR(age, diabetes)::NUMERIC, 3)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'Blood Pressure' as feature_name, 0.12 as importance_score, 'Cardiovascular indicator',
           ROUND(CORR(blood_pressure, diabetes)::NUMERIC, 3)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 'Pregnancies' as feature_name, 0.08 as importance_score, 'Gestational diabetes risk',
           ROUND(CORR(pregnancies, diabetes)::NUMERIC, 3)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
) features
ORDER BY importance_score DESC;

-- =============================================================================
-- 8. COST ANALYSIS AND BUSINESS INTELLIGENCE QUERIES
-- =============================================================================

-- Cost Analysis by Risk Level (for stacked bar chart)
SELECT 
    health_risk_level,
    COUNT(*) as patient_count,
    SUM(estimated_annual_cost) as total_cost,
    AVG(estimated_annual_cost) as avg_cost_per_patient,
    SUM(estimated_annual_cost) / COUNT(*) as cost_per_patient
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY health_risk_level
ORDER BY 
    CASE health_risk_level 
        WHEN 'Low' THEN 1
        WHEN 'Moderate' THEN 2
        WHEN 'High' THEN 3
        WHEN 'Critical' THEN 4
        ELSE 5
    END;

-- Treatment Recommendation Distribution (for treemap)
SELECT 
    treatment_recommendation,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    AVG(estimated_annual_cost) as avg_cost,
    SUM(estimated_annual_cost) as total_cost
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
  AND treatment_recommendation IS NOT NULL
GROUP BY treatment_recommendation
ORDER BY patient_count DESC;

-- =============================================================================
-- 9. DATA QUALITY MONITORING QUERIES
-- =============================================================================

-- Data Quality Metrics (for stat panels)
SELECT 
    'Overall Data Quality' as metric,
    ROUND(AVG(quality_score) * 100, 1) as value,
    '%' as unit
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT 
    'Records with High Quality (≥80%)' as metric,
    ROUND(SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as value,
    '%' as unit
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT 
    'Complete Records (No Missing Values)' as metric,
    ROUND(SUM(CASE WHEN quality_score = 1.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as value,
    '%' as unit
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days';

-- Anomaly Detection Summary (for table visualization)
SELECT 
    processing_date,
    COUNT(*) as total_records,
    SUM(CASE WHEN anomaly_flags::text != '{}' THEN 1 ELSE 0 END) as records_with_anomalies,
    ROUND(SUM(CASE WHEN anomaly_flags::text != '{}' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as anomaly_rate,
    AVG(quality_score) as avg_quality_score
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY processing_date
ORDER BY processing_date DESC;

-- =============================================================================
-- 10. REAL-TIME MONITORING AND ALERTS QUERIES
-- =============================================================================

-- Pipeline Processing Status (for status indicators)
SELECT 
    pipeline_stage,
    status,
    COUNT(*) as execution_count,
    MAX(end_time) as last_execution,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds
FROM analytics.pipeline_execution_log 
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY pipeline_stage, status
ORDER BY pipeline_stage, status;

-- Latest Processing Metrics (for current stats)
SELECT 
    'Last ETL Run' as metric,
    TO_CHAR(MAX(end_time), 'YYYY-MM-DD HH24:MI:SS') as value,
    'timestamp' as unit
FROM analytics.pipeline_execution_log 
WHERE status = 'SUCCESS'
UNION ALL
SELECT 
    'Records Processed Today' as metric,
    COUNT(*)::text as value,
    'count' as unit
FROM gold.diabetes_gold 
WHERE processing_date = CURRENT_DATE
UNION ALL
SELECT 
    'Active Alerts' as metric,
    COUNT(*)::text as value,
    'count' as unit
FROM analytics.pipeline_execution_log 
WHERE status = 'ERROR' 
  AND start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- =============================================================================
-- HELPER QUERIES FOR GRAFANA VARIABLES AND FILTERS
-- =============================================================================

-- Available Date Ranges (for Grafana variable)
SELECT DISTINCT 
    processing_date as __text,
    processing_date as __value
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY processing_date DESC;

-- Available Risk Levels (for Grafana variable)
SELECT DISTINCT 
    health_risk_level as __text,
    health_risk_level as __value
FROM gold.diabetes_gold 
WHERE health_risk_level IS NOT NULL
ORDER BY 
    CASE health_risk_level 
        WHEN 'Low' THEN 1
        WHEN 'Moderate' THEN 2
        WHEN 'High' THEN 3
        WHEN 'Critical' THEN 4
        ELSE 5
    END;

-- Available Age Groups (for Grafana variable)
SELECT DISTINCT 
    age_group as __text,
    age_group as __value
FROM gold.diabetes_gold 
WHERE age_group IS NOT NULL
ORDER BY 
    CASE age_group 
        WHEN '20-29' THEN 1
        WHEN '30-39' THEN 2
        WHEN '40-49' THEN 3
        WHEN '50-59' THEN 4
        WHEN '60+' THEN 5
        ELSE 6
    END;

-- Success message
\echo 'Grafana analytics queries created successfully!'
\echo 'Queries include:'
\echo '- Executive KPI dashboards'
\echo '- Time series analysis'
\echo '- Clinical risk distributions'
\echo '- Demographic analysis'
\echo '- Model performance metrics'
\echo '- Cost analysis'
\echo '- Data quality monitoring'
\echo '- Real-time alerts'
\echo 'Ready for Grafana dashboard integration!'

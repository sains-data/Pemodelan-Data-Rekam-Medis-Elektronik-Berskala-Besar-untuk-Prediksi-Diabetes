-- =============================================================================
-- COMPREHENSIVE ANALYTICS VIEWS AND QUERIES FOR GRAFANA DASHBOARD
-- =============================================================================
-- Advanced analytics queries for diabetes prediction deep analysis dashboard
-- Optimized for PostgreSQL with comprehensive clinical insights
-- =============================================================================

-- =============================================================================
-- EXECUTIVE KPI DASHBOARD VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.executive_kpi_dashboard AS
WITH current_stats AS (
    SELECT 
        COUNT(*) as total_patients,
        SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
        SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_care_needed,
        SUM(CASE WHEN diabetes = 0 AND glucose_category = 'Prediabetic' THEN 1 ELSE 0 END) as diabetes_prevention_opportunities,
        AVG(risk_score) as avg_risk_score,
        AVG(quality_score) as avg_data_quality
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
),
cost_analysis AS (
    SELECT 
        SUM(CASE 
            WHEN estimated_cost_category = 'Very High' THEN 50000
            WHEN estimated_cost_category = 'High' THEN 25000
            WHEN estimated_cost_category = 'Medium' THEN 10000
            ELSE 3000
        END) as estimated_total_cost,
        SUM(CASE 
            WHEN diabetes = 0 AND glucose_category = 'Prediabetic' THEN 15000
            ELSE 0
        END) as potential_cost_savings
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    cs.total_patients,
    cs.high_risk_patients,
    cs.immediate_care_needed,
    cs.diabetes_prevention_opportunities,
    ca.potential_cost_savings as estimated_cost_savings,
    ROUND((cs.avg_risk_score * 100)::numeric, 1) as population_health_score,
    ROUND(((cs.total_patients - cs.immediate_care_needed) * 100.0 / NULLIF(cs.total_patients, 0))::numeric, 1) as system_efficiency_score,
    ROUND((cs.avg_data_quality * 100)::numeric, 1) as data_quality_score
FROM current_stats cs, cost_analysis ca;

-- =============================================================================
-- CLINICAL RISK DISTRIBUTION VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.clinical_risk_distribution AS
SELECT 
    health_risk_level,
    intervention_priority,
    COUNT(*) as patient_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 1) as percentage,
    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
    ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
    ROUND(AVG(bmi)::numeric, 1) as avg_bmi,
    ROUND(AVG(age)::numeric, 1) as avg_age,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
    ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as diabetes_rate
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY health_risk_level, intervention_priority
ORDER BY 
    CASE health_risk_level 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        ELSE 4 
    END,
    CASE intervention_priority 
        WHEN 'Immediate' THEN 1 
        WHEN 'Soon' THEN 2 
        WHEN 'Routine' THEN 3 
        ELSE 4 
    END;

-- =============================================================================
-- DEMOGRAPHIC ANALYSIS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.demographic_health_analysis AS
SELECT 
    age_group,
    bmi_category,
    COUNT(*) as total_patients,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
    ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as diabetes_prevalence,
    SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
    ROUND((SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as high_risk_prevalence,
    ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
    ROUND(AVG(bmi)::numeric, 1) as avg_bmi,
    ROUND(AVG(blood_pressure)::numeric, 1) as avg_blood_pressure,
    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
    ROUND(AVG(metabolic_syndrome_risk)::numeric, 3) as avg_metabolic_risk,
    ROUND(AVG(cardiovascular_risk)::numeric, 3) as avg_cardiovascular_risk
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY age_group, bmi_category
ORDER BY age_group, bmi_category;

-- =============================================================================
-- GLUCOSE ANALYSIS AND TRENDS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.glucose_analysis_trends AS
WITH glucose_ranges AS (
    SELECT 
        glucose_category,
        COUNT(*) as patient_count,
        ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
        ROUND(STDDEV(glucose)::numeric, 1) as glucose_stddev,
        MIN(glucose) as min_glucose,
        MAX(glucose) as max_glucose,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY glucose)::numeric, 1) as glucose_q1,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY glucose)::numeric, 1) as glucose_median,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY glucose)::numeric, 1) as glucose_q3,
        SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as confirmed_diabetic,
        ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as diabetes_confirmation_rate
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY glucose_category
)
SELECT 
    glucose_category,
    patient_count,
    ROUND((patient_count * 100.0 / SUM(patient_count) OVER())::numeric, 1) as distribution_percentage,
    avg_glucose,
    glucose_stddev,
    min_glucose,
    max_glucose,
    glucose_q1,
    glucose_median,
    glucose_q3,
    confirmed_diabetic,
    diabetes_confirmation_rate,
    CASE 
        WHEN glucose_category = 'Normal' THEN 'Low Risk'
        WHEN glucose_category = 'Prediabetic' THEN 'Prevention Opportunity'
        WHEN glucose_category = 'Diabetic' THEN 'Management Required'
        ELSE 'Critical Care'
    END as clinical_recommendation
FROM glucose_ranges
ORDER BY 
    CASE glucose_category 
        WHEN 'Hypoglycemic' THEN 1
        WHEN 'Normal' THEN 2 
        WHEN 'Prediabetic' THEN 3 
        WHEN 'Diabetic' THEN 4 
        ELSE 5 
    END;

-- =============================================================================
-- BMI AND LIFESTYLE ANALYSIS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.bmi_lifestyle_analysis AS
SELECT 
    bmi_category,
    COUNT(*) as patient_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 1) as distribution_percentage,
    ROUND(AVG(bmi)::numeric, 1) as avg_bmi,
    ROUND(AVG(age)::numeric, 1) as avg_age,
    ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
    ROUND(AVG(blood_pressure)::numeric, 1) as avg_blood_pressure,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
    ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as diabetes_rate,
    SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
    ROUND((SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as high_risk_rate,
    ROUND(AVG(metabolic_syndrome_risk)::numeric, 3) as avg_metabolic_risk,
    ROUND(AVG(cardiovascular_risk)::numeric, 3) as avg_cardiovascular_risk,
    ROUND(AVG(lifestyle_risk_score)::numeric, 3) as avg_lifestyle_risk,
    CASE 
        WHEN bmi_category LIKE '%Underweight%' THEN 'Nutritional counseling, weight gain program'
        WHEN bmi_category = 'Normal Weight' THEN 'Maintain current lifestyle, regular monitoring'
        WHEN bmi_category = 'Overweight' THEN 'Weight management, increased physical activity'
        WHEN bmi_category LIKE '%Obese%' THEN 'Comprehensive weight loss program, medical supervision'
        ELSE 'Clinical evaluation required'
    END as lifestyle_recommendation
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY bmi_category
ORDER BY 
    CASE 
        WHEN bmi_category LIKE '%Underweight%' THEN 1
        WHEN bmi_category = 'Normal Weight' THEN 2 
        WHEN bmi_category = 'Overweight' THEN 3 
        WHEN bmi_category LIKE '%Obese%' THEN 4 
        ELSE 5 
    END;

-- =============================================================================
-- PREDICTION ACCURACY AND MODEL PERFORMANCE VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.prediction_accuracy_analysis AS
WITH prediction_analysis AS (
    SELECT 
        prediction,
        diabetes as actual_outcome,
        COUNT(*) as case_count,
        ROUND(AVG(prediction_probability)::numeric, 3) as avg_prediction_probability,
        ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
        ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
        ROUND(AVG(bmi)::numeric, 1) as avg_bmi
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY prediction, diabetes
),
confusion_matrix AS (
    SELECT 
        SUM(CASE WHEN prediction = 1 AND diabetes = 1 THEN case_count ELSE 0 END) as true_positive,
        SUM(CASE WHEN prediction = 0 AND diabetes = 0 THEN case_count ELSE 0 END) as true_negative,
        SUM(CASE WHEN prediction = 1 AND diabetes = 0 THEN case_count ELSE 0 END) as false_positive,
        SUM(CASE WHEN prediction = 0 AND diabetes = 1 THEN case_count ELSE 0 END) as false_negative,
        SUM(case_count) as total_cases
    FROM prediction_analysis
)
SELECT 
    pa.*,
    cm.true_positive,
    cm.true_negative,
    cm.false_positive,
    cm.false_negative,
    cm.total_cases,
    ROUND((cm.true_positive + cm.true_negative)::numeric * 100.0 / cm.total_cases, 2) as accuracy_percentage,
    ROUND(cm.true_positive::numeric * 100.0 / NULLIF(cm.true_positive + cm.false_negative, 0), 2) as sensitivity_recall,
    ROUND(cm.true_positive::numeric * 100.0 / NULLIF(cm.true_positive + cm.false_positive, 0), 2) as precision,
    ROUND(cm.true_negative::numeric * 100.0 / NULLIF(cm.true_negative + cm.false_positive, 0), 2) as specificity
FROM prediction_analysis pa, confusion_matrix cm;

-- =============================================================================
-- TREATMENT AND INTERVENTION ANALYSIS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.treatment_intervention_analysis AS
SELECT 
    intervention_priority,
    followup_frequency,
    COUNT(*) as patient_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 1) as distribution_percentage,
    SUM(CASE WHEN specialist_referral_needed = TRUE THEN 1 ELSE 0 END) as referral_needed,
    ROUND((SUM(CASE WHEN specialist_referral_needed = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as referral_rate,
    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
    ROUND(AVG(glucose)::numeric, 1) as avg_glucose,
    ROUND(AVG(bmi)::numeric, 1) as avg_bmi,
    SUM(CASE 
        WHEN estimated_cost_category = 'Very High' THEN 50000
        WHEN estimated_cost_category = 'High' THEN 25000
        WHEN estimated_cost_category = 'Medium' THEN 10000
        ELSE 3000
    END) as estimated_total_cost,
    ROUND((SUM(CASE 
        WHEN estimated_cost_category = 'Very High' THEN 50000
        WHEN estimated_cost_category = 'High' THEN 25000
        WHEN estimated_cost_category = 'Medium' THEN 10000
        ELSE 3000
    END) / COUNT(*))::numeric, 0) as avg_cost_per_patient,
    STRING_AGG(DISTINCT 
        CASE 
            WHEN intervention_priority = 'Immediate' THEN 'Emergency diabetes care, immediate medication adjustment'
            WHEN intervention_priority = 'Soon' THEN 'Diabetes management consultation within 2 weeks'
            WHEN intervention_priority = 'Routine' THEN 'Regular diabetes monitoring and lifestyle counseling'
            ELSE 'Preventive care and annual screening'
        END, '; '
    ) as clinical_recommendations
FROM gold.diabetes_gold 
WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY intervention_priority, followup_frequency
ORDER BY 
    CASE intervention_priority 
        WHEN 'Immediate' THEN 1 
        WHEN 'Soon' THEN 2 
        WHEN 'Routine' THEN 3 
        ELSE 4 
    END,
    CASE followup_frequency 
        WHEN 'Weekly' THEN 1 
        WHEN 'Monthly' THEN 2 
        WHEN 'Quarterly' THEN 3 
        ELSE 4 
    END;

-- =============================================================================
-- TIME SERIES ANALYSIS FOR TRENDS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.diabetes_trends_timeseries AS
WITH daily_aggregates AS (
    SELECT 
        processing_date,
        COUNT(*) as daily_patients,
        SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as daily_diabetic_cases,
        ROUND(AVG(glucose)::numeric, 1) as daily_avg_glucose,
        ROUND(AVG(bmi)::numeric, 1) as daily_avg_bmi,
        ROUND(AVG(risk_score)::numeric, 3) as daily_avg_risk,
        SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as daily_high_risk,
        SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as daily_immediate_care
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY processing_date
),
moving_averages AS (
    SELECT 
        processing_date,
        daily_patients,
        daily_diabetic_cases,
        ROUND((daily_diabetic_cases * 100.0 / NULLIF(daily_patients, 0))::numeric, 1) as daily_diabetes_rate,
        daily_avg_glucose,
        daily_avg_bmi,
        daily_avg_risk,
        daily_high_risk,
        daily_immediate_care,
        ROUND(AVG(daily_avg_glucose) OVER (ORDER BY processing_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 1) as glucose_7day_ma,
        ROUND(AVG(daily_avg_bmi) OVER (ORDER BY processing_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 1) as bmi_7day_ma,
        ROUND(AVG(daily_avg_risk) OVER (ORDER BY processing_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 3) as risk_7day_ma,
        ROUND(AVG(daily_diabetic_cases * 100.0 / NULLIF(daily_patients, 0)) OVER (ORDER BY processing_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 1) as diabetes_rate_7day_ma
    FROM daily_aggregates
)
SELECT 
    *,
    CASE 
        WHEN glucose_7day_ma > LAG(glucose_7day_ma, 1) OVER (ORDER BY processing_date) THEN 'Increasing'
        WHEN glucose_7day_ma < LAG(glucose_7day_ma, 1) OVER (ORDER BY processing_date) THEN 'Decreasing'
        ELSE 'Stable'
    END as glucose_trend,
    CASE 
        WHEN risk_7day_ma > LAG(risk_7day_ma, 1) OVER (ORDER BY processing_date) THEN 'Increasing'
        WHEN risk_7day_ma < LAG(risk_7day_ma, 1) OVER (ORDER BY processing_date) THEN 'Decreasing'
        ELSE 'Stable'
    END as risk_trend
FROM moving_averages
ORDER BY processing_date;

-- =============================================================================
-- FEATURE IMPORTANCE AND MODEL INSIGHTS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.feature_importance_analysis AS
WITH feature_correlations AS (
    SELECT 
        ROUND(CORR(glucose, diabetes)::numeric, 3) as glucose_diabetes_correlation,
        ROUND(CORR(bmi, diabetes)::numeric, 3) as bmi_diabetes_correlation,
        ROUND(CORR(age, diabetes)::numeric, 3) as age_diabetes_correlation,
        ROUND(CORR(blood_pressure, diabetes)::numeric, 3) as bp_diabetes_correlation,
        ROUND(CORR(diabetes_pedigree_function, diabetes)::numeric, 3) as pedigree_diabetes_correlation,
        ROUND(CORR(pregnancies, diabetes)::numeric, 3) as pregnancies_diabetes_correlation,
        ROUND(CORR(glucose_bmi_interaction, diabetes)::numeric, 3) as glucose_bmi_interaction_correlation,
        ROUND(CORR(age_glucose_interaction, diabetes)::numeric, 3) as age_glucose_interaction_correlation
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
),
feature_statistics AS (
    SELECT 
        'glucose' as feature_name, 
        ROUND(AVG(glucose)::numeric, 1) as mean_value, 
        ROUND(STDDEV(glucose)::numeric, 1) as std_value,
        MIN(glucose) as min_value,
        MAX(glucose) as max_value,
        COUNT(CASE WHEN glucose IS NULL THEN 1 END) as null_count,
        COUNT(*) as total_count
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 
        'bmi', ROUND(AVG(bmi)::numeric, 1), ROUND(STDDEV(bmi)::numeric, 1),
        MIN(bmi), MAX(bmi), COUNT(CASE WHEN bmi IS NULL THEN 1 END), COUNT(*)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 
        'age', ROUND(AVG(age)::numeric, 1), ROUND(STDDEV(age)::numeric, 1),
        MIN(age), MAX(age), COUNT(CASE WHEN age IS NULL THEN 1 END), COUNT(*)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    UNION ALL
    SELECT 
        'blood_pressure', ROUND(AVG(blood_pressure)::numeric, 1), ROUND(STDDEV(blood_pressure)::numeric, 1),
        MIN(blood_pressure), MAX(blood_pressure), COUNT(CASE WHEN blood_pressure IS NULL THEN 1 END), COUNT(*)
    FROM gold.diabetes_gold WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    fs.*,
    ROUND((fs.null_count * 100.0 / fs.total_count)::numeric, 1) as null_percentage,
    CASE fs.feature_name
        WHEN 'glucose' THEN fc.glucose_diabetes_correlation
        WHEN 'bmi' THEN fc.bmi_diabetes_correlation
        WHEN 'age' THEN fc.age_diabetes_correlation
        WHEN 'blood_pressure' THEN fc.bp_diabetes_correlation
    END as diabetes_correlation,
    CASE fs.feature_name
        WHEN 'glucose' THEN 'Primary diagnostic indicator - most important feature'
        WHEN 'bmi' THEN 'Strong lifestyle indicator - key modifiable risk factor'
        WHEN 'age' THEN 'Non-modifiable risk factor - important for risk stratification'
        WHEN 'blood_pressure' THEN 'Cardiovascular comorbidity indicator'
    END as clinical_significance
FROM feature_statistics fs, feature_correlations fc
ORDER BY ABS(
    CASE fs.feature_name
        WHEN 'glucose' THEN fc.glucose_diabetes_correlation
        WHEN 'bmi' THEN fc.bmi_diabetes_correlation
        WHEN 'age' THEN fc.age_diabetes_correlation
        WHEN 'blood_pressure' THEN fc.bp_diabetes_correlation
        ELSE 0
    END
) DESC;

-- =============================================================================
-- DATA QUALITY MONITORING VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.data_quality_monitoring AS
WITH quality_metrics AS (
    SELECT 
        processing_date,
        COUNT(*) as total_records,
        ROUND(AVG(quality_score)::numeric, 3) as avg_quality_score,
        COUNT(CASE WHEN quality_score < 0.8 THEN 1 END) as low_quality_records,
        COUNT(CASE WHEN anomaly_flags::text != '{}' THEN 1 END) as records_with_anomalies,
        SUM(CASE WHEN (anomaly_flags->>'zero_glucose')::boolean = true THEN 1 ELSE 0 END) as zero_glucose_count,
        SUM(CASE WHEN (anomaly_flags->>'extreme_glucose')::boolean = true THEN 1 ELSE 0 END) as extreme_glucose_count,
        SUM(CASE WHEN (anomaly_flags->>'zero_bmi')::boolean = true THEN 1 ELSE 0 END) as zero_bmi_count,
        SUM(CASE WHEN (anomaly_flags->>'extreme_bmi')::boolean = true THEN 1 ELSE 0 END) as extreme_bmi_count
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY processing_date
)
SELECT 
    processing_date,
    total_records,
    avg_quality_score,
    low_quality_records,
    ROUND((low_quality_records * 100.0 / total_records)::numeric, 1) as low_quality_percentage,
    records_with_anomalies,
    ROUND((records_with_anomalies * 100.0 / total_records)::numeric, 1) as anomaly_percentage,
    zero_glucose_count,
    extreme_glucose_count,
    zero_bmi_count,
    extreme_bmi_count,
    CASE 
        WHEN avg_quality_score >= 0.95 THEN 'Excellent'
        WHEN avg_quality_score >= 0.85 THEN 'Good'
        WHEN avg_quality_score >= 0.70 THEN 'Acceptable'
        ELSE 'Needs Improvement'
    END as quality_rating,
    CASE 
        WHEN low_quality_percentage > 20 THEN 'High'
        WHEN low_quality_percentage > 10 THEN 'Medium'
        ELSE 'Low'
    END as data_quality_risk
FROM quality_metrics
ORDER BY processing_date DESC;

-- =============================================================================
-- COMPREHENSIVE POPULATION HEALTH DASHBOARD VIEW
-- =============================================================================

CREATE OR REPLACE VIEW analytics.population_health_dashboard AS
WITH population_metrics AS (
    SELECT 
        COUNT(*) as total_population,
        SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_population,
        SUM(CASE WHEN glucose_category = 'Prediabetic' AND diabetes = 0 THEN 1 ELSE 0 END) as prediabetic_population,
        SUM(CASE WHEN health_risk_level = 'Critical' THEN 1 ELSE 0 END) as critical_risk_population,
        SUM(CASE WHEN health_risk_level = 'High' THEN 1 ELSE 0 END) as high_risk_population,
        SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_intervention_needed,
        SUM(CASE WHEN specialist_referral_needed = TRUE THEN 1 ELSE 0 END) as specialist_referrals_needed,
        ROUND(AVG(risk_score)::numeric, 3) as population_avg_risk,
        ROUND(AVG(glucose)::numeric, 1) as population_avg_glucose,
        ROUND(AVG(bmi)::numeric, 1) as population_avg_bmi,
        ROUND(AVG(metabolic_syndrome_risk)::numeric, 3) as population_metabolic_risk,
        ROUND(AVG(cardiovascular_risk)::numeric, 3) as population_cardiovascular_risk
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
),
cost_projections AS (
    SELECT 
        SUM(CASE 
            WHEN estimated_cost_category = 'Very High' THEN 50000
            WHEN estimated_cost_category = 'High' THEN 25000
            WHEN estimated_cost_category = 'Medium' THEN 10000
            ELSE 3000
        END) as total_estimated_cost,
        SUM(CASE 
            WHEN diabetes = 0 AND glucose_category = 'Prediabetic' THEN 15000
            ELSE 0
        END) as preventable_cost,
        COUNT(*) as cost_calculation_base
    FROM gold.diabetes_gold 
    WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    pm.total_population,
    pm.diabetic_population,
    ROUND((pm.diabetic_population * 100.0 / pm.total_population)::numeric, 1) as diabetes_prevalence_rate,
    pm.prediabetic_population,
    ROUND((pm.prediabetic_population * 100.0 / pm.total_population)::numeric, 1) as prediabetes_prevalence_rate,
    (pm.critical_risk_population + pm.high_risk_population) as high_risk_population,
    ROUND(((pm.critical_risk_population + pm.high_risk_population) * 100.0 / pm.total_population)::numeric, 1) as high_risk_prevalence_rate,
    pm.immediate_intervention_needed,
    pm.specialist_referrals_needed,
    pm.population_avg_risk,
    pm.population_avg_glucose,
    pm.population_avg_bmi,
    pm.population_metabolic_risk,
    pm.population_cardiovascular_risk,
    cp.total_estimated_cost,
    cp.preventable_cost,
    ROUND((cp.total_estimated_cost / pm.total_population)::numeric, 0) as avg_cost_per_person,
    ROUND((cp.preventable_cost * 100.0 / cp.total_estimated_cost)::numeric, 1) as cost_prevention_potential,
    CASE 
        WHEN pm.population_avg_risk < 0.3 THEN 'Low Risk Population'
        WHEN pm.population_avg_risk < 0.5 THEN 'Moderate Risk Population'
        WHEN pm.population_avg_risk < 0.7 THEN 'High Risk Population'
        ELSE 'Critical Risk Population'
    END as population_risk_category,
    CURRENT_TIMESTAMP as dashboard_updated_at
FROM population_metrics pm, cost_projections cp;

-- =============================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- =============================================================================

-- Create indexes on frequently queried columns in analytics views
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_processing_date ON gold.diabetes_gold(processing_date);
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_health_risk ON gold.diabetes_gold(health_risk_level);
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_intervention ON gold.diabetes_gold(intervention_priority);
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_diabetes_outcome ON gold.diabetes_gold(diabetes);
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_composite_analysis ON gold.diabetes_gold(processing_date, health_risk_level, diabetes);

-- Partial indexes for common filtering conditions
CREATE INDEX IF NOT EXISTS idx_diabetes_gold_recent_high_risk ON gold.diabetes_gold(processing_date, risk_score) 
WHERE health_risk_level IN ('High', 'Critical');

CREATE INDEX IF NOT EXISTS idx_diabetes_gold_recent_diabetic ON gold.diabetes_gold(processing_date, glucose) 
WHERE diabetes = 1;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'COMPREHENSIVE ANALYTICS VIEWS CREATED!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Views created for Grafana dashboard:';
    RAISE NOTICE '- executive_kpi_dashboard';
    RAISE NOTICE '- clinical_risk_distribution';
    RAISE NOTICE '- demographic_health_analysis';
    RAISE NOTICE '- glucose_analysis_trends';
    RAISE NOTICE '- bmi_lifestyle_analysis';
    RAISE NOTICE '- prediction_accuracy_analysis';
    RAISE NOTICE '- treatment_intervention_analysis';
    RAISE NOTICE '- diabetes_trends_timeseries';
    RAISE NOTICE '- feature_importance_analysis';
    RAISE NOTICE '- data_quality_monitoring';
    RAISE NOTICE '- population_health_dashboard';
    RAISE NOTICE 'Performance indexes created for optimization';
END $$;

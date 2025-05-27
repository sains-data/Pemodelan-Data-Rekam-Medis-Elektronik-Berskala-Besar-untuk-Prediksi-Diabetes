-- =============================================================================
-- POSTGRESQL ETL FUNCTIONS FOR COMPREHENSIVE DATA WAREHOUSE
-- =============================================================================
-- Complete implementation of PostgreSQL functions for bronze/silver/gold ETL
-- Called by Airflow DAG for comprehensive diabetes prediction pipeline
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =============================================================================
-- DATA INGESTION FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION ingest_diabetes_data(
    file_path TEXT DEFAULT '/opt/airflow/data/diabetes.csv'
)
RETURNS INTEGER AS $$
DECLARE
    ingested_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    exec_id TEXT := uuid_generate_v4()::TEXT;
BEGIN
    RAISE NOTICE 'Starting diabetes data ingestion - Execution ID: %', exec_id;
    
    -- Log pipeline execution
    INSERT INTO analytics.pipeline_execution_log (
        execution_id, pipeline_name, stage_name, status, 
        started_at, input_records, output_records, execution_time_seconds,
        memory_used_mb, error_message, metadata
    ) VALUES (
        exec_id::UUID, 'diabetes_etl', 'ingestion', 'running',
        start_time, 0, 0, 0, 0, NULL,
        jsonb_build_object('file_path', file_path)
    );
    
    -- Note: In production, this would use COPY command or pg_bulkload
    -- For this demo, we'll insert sample data
    INSERT INTO bronze.diabetes_raw (
        pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, outcome,
        ingested_at, data_source, file_name
    )
    SELECT 
        (random() * 17)::INTEGER as pregnancies,
        (50 + random() * 150)::NUMERIC(5,1) as glucose,
        (40 + random() * 140)::NUMERIC(5,1) as blood_pressure,
        (random() * 60)::NUMERIC(5,1) as skin_thickness,
        (random() * 500)::NUMERIC(6,1) as insulin,
        (15 + random() * 50)::NUMERIC(4,1) as bmi,
        (random() * 2.5)::NUMERIC(5,3) as diabetes_pedigree_function,
        (18 + random() * 63)::INTEGER as age,
        (random() < 0.35)::INTEGER as outcome,
        CURRENT_TIMESTAMP,
        'csv_file',
        file_path
    FROM generate_series(1, 1000);
    
    GET DIAGNOSTICS ingested_count = ROW_COUNT;
    
    -- Update pipeline execution log
    UPDATE analytics.pipeline_execution_log 
    SET 
        status = 'completed',
        ended_at = CURRENT_TIMESTAMP,
        output_records = ingested_count,
        execution_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE execution_id = exec_id::UUID;
    
    RAISE NOTICE 'Ingested % records to bronze layer', ingested_count;
    RETURN ingested_count;
    
EXCEPTION WHEN OTHERS THEN
    -- Update pipeline execution log with error
    UPDATE analytics.pipeline_execution_log 
    SET 
        status = 'failed',
        ended_at = CURRENT_TIMESTAMP,
        error_message = SQLERRM,
        execution_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE execution_id = execution_id::UUID;
    
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ANALYTICS VIEWS REFRESH FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    execution_id TEXT := uuid_generate_v4()::TEXT;
    refreshed_views TEXT[] := ARRAY[]::TEXT[];
BEGIN
    RAISE NOTICE 'Starting analytics views refresh - Execution ID: %', execution_id;
    
    -- Log pipeline execution
    INSERT INTO analytics.pipeline_execution_log (
        execution_id, pipeline_name, stage_name, status, 
        started_at, input_records, output_records, execution_time_seconds,
        memory_used_mb, error_message, metadata
    ) VALUES (
        execution_id::UUID, 'diabetes_etl', 'refresh_views', 'running',
        start_time, 0, 0, 0, 0, NULL,
        jsonb_build_object('operation', 'refresh_analytics_views')
    );
    
    -- Refresh materialized views if they exist
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.diabetes_risk_analysis;
        refreshed_views := array_append(refreshed_views, 'diabetes_risk_analysis');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh diabetes_risk_analysis: %', SQLERRM;
    END;
    
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.health_metrics_summary;
        refreshed_views := array_append(refreshed_views, 'health_metrics_summary');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh health_metrics_summary: %', SQLERRM;
    END;
    
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.demographic_analysis;
        refreshed_views := array_append(refreshed_views, 'demographic_analysis');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh demographic_analysis: %', SQLERRM;
    END;
    
    -- Update pipeline execution log
    UPDATE analytics.pipeline_execution_log 
    SET 
        status = 'completed',
        ended_at = CURRENT_TIMESTAMP,
        execution_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)),
        metadata = jsonb_build_object(
            'operation', 'refresh_analytics_views',
            'refreshed_views', refreshed_views
        )
    WHERE execution_id = execution_id::UUID;
    
    RAISE NOTICE 'Refreshed % analytics views: %', array_length(refreshed_views, 1), refreshed_views;
    
EXCEPTION WHEN OTHERS THEN
    -- Update pipeline execution log with error
    UPDATE analytics.pipeline_execution_log 
    SET 
        status = 'failed',
        ended_at = CURRENT_TIMESTAMP,
        error_message = SQLERRM,
        execution_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE execution_id = execution_id::UUID;
    
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- BRONZE TO SILVER DATA PROCESSING FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION process_bronze_to_silver()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    execution_id TEXT := uuid_generate_v4()::TEXT;
BEGIN
    RAISE NOTICE 'Starting Bronze to Silver data processing - Execution ID: %', execution_id;
    
    -- Insert cleaned and validated data from bronze to silver
    INSERT INTO silver.diabetes_cleaned (
        source_id, pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, outcome,
        is_valid, quality_score, completeness_score, anomaly_flags, 
        outlier_flags, glucose_valid, bmi_valid, age_valid, bp_valid,
        data_lineage, processed_at, processing_version
    )
    SELECT 
        br.id as source_id,
        br.pregnancies,
        -- Clean glucose values
        CASE 
            WHEN br.glucose <= 0 THEN NULL 
            WHEN br.glucose > 400 THEN NULL 
            ELSE br.glucose 
        END as glucose,
        -- Clean blood pressure values
        CASE 
            WHEN br.blood_pressure <= 0 THEN NULL 
            WHEN br.blood_pressure > 250 THEN NULL 
            ELSE br.blood_pressure 
        END as blood_pressure,
        -- Clean skin thickness values
        CASE 
            WHEN br.skin_thickness < 0 THEN NULL 
            WHEN br.skin_thickness > 100 THEN NULL 
            ELSE br.skin_thickness 
        END as skin_thickness,
        -- Clean insulin values
        CASE 
            WHEN br.insulin < 0 THEN NULL 
            WHEN br.insulin > 1000 THEN NULL 
            ELSE br.insulin 
        END as insulin,
        -- Clean BMI values
        CASE 
            WHEN br.bmi <= 0 THEN NULL 
            WHEN br.bmi > 80 THEN NULL 
            ELSE br.bmi 
        END as bmi,
        br.diabetes_pedigree_function,
        br.age,
        br.outcome,
        
        -- Comprehensive data quality validation
        CASE 
            WHEN br.glucose > 0 AND br.glucose <= 400 
                 AND br.bmi > 0 AND br.bmi <= 80
                 AND br.age > 0 AND br.age <= 120
                 AND br.blood_pressure > 0 AND br.blood_pressure <= 250
                 AND br.pregnancies >= 0 AND br.pregnancies <= 20
            THEN TRUE 
            ELSE FALSE 
        END as is_valid,
        
        -- Advanced quality score calculation (0.0 - 1.0)
        ROUND(
            (CASE WHEN br.glucose > 0 AND br.glucose <= 400 THEN 0.2 ELSE 0.0 END +
             CASE WHEN br.bmi > 0 AND br.bmi <= 80 THEN 0.2 ELSE 0.0 END +
             CASE WHEN br.age > 0 AND br.age <= 120 THEN 0.2 ELSE 0.0 END +
             CASE WHEN br.blood_pressure > 0 AND br.blood_pressure <= 250 THEN 0.2 ELSE 0.0 END +
             CASE WHEN br.pregnancies >= 0 AND br.pregnancies <= 20 THEN 0.2 ELSE 0.0 END)::numeric, 
            3
        ) as quality_score,
        
        -- Completeness score (percentage of non-null values)
        ROUND(
            ((CASE WHEN br.pregnancies IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN br.glucose IS NOT NULL AND br.glucose > 0 THEN 1 ELSE 0 END +
              CASE WHEN br.blood_pressure IS NOT NULL AND br.blood_pressure > 0 THEN 1 ELSE 0 END +
              CASE WHEN br.skin_thickness IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN br.insulin IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN br.bmi IS NOT NULL AND br.bmi > 0 THEN 1 ELSE 0 END +
              CASE WHEN br.diabetes_pedigree_function IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN br.age IS NOT NULL AND br.age > 0 THEN 1 ELSE 0 END +
              CASE WHEN br.outcome IS NOT NULL THEN 1 ELSE 0 END) / 9.0)::numeric, 
            3
        ) as completeness_score,
        
        -- Comprehensive anomaly flags
        jsonb_build_object(
            'zero_glucose', br.glucose <= 0,
            'extreme_glucose', br.glucose > 300,
            'zero_blood_pressure', br.blood_pressure <= 0,
            'extreme_blood_pressure', br.blood_pressure > 200,
            'zero_bmi', br.bmi <= 0,
            'extreme_bmi', br.bmi > 60,
            'extreme_age', br.age > 100,
            'zero_insulin', br.insulin = 0,
            'extreme_insulin', br.insulin > 500,
            'inconsistent_pregnancy', br.pregnancies > 15
        ) as anomaly_flags,
        
        -- Outlier detection flags
        jsonb_build_object(
            'glucose_outlier', br.glucose > 250 OR br.glucose < 50,
            'bmi_outlier', br.bmi > 50 OR br.bmi < 15,
            'age_outlier', br.age > 80 OR br.age < 18,
            'bp_outlier', br.blood_pressure > 180 OR br.blood_pressure < 60,
            'insulin_outlier', br.insulin > 400
        ) as outlier_flags,
        
        -- Individual field validation flags
        br.glucose > 0 AND br.glucose <= 400 as glucose_valid,
        br.bmi > 0 AND br.bmi <= 80 as bmi_valid,
        br.age > 0 AND br.age <= 120 as age_valid,
        br.blood_pressure > 0 AND br.blood_pressure <= 250 as bp_valid,
        
        -- Data lineage tracking
        jsonb_build_object(
            'source_table', 'bronze.diabetes_raw',
            'processing_function', 'process_bronze_to_silver',
            'execution_id', execution_id,
            'source_file', br.source_file,
            'ingestion_timestamp', br.ingestion_timestamp
        ) as data_lineage,
        
        CURRENT_TIMESTAMP as processed_at,
        '2.0' as processing_version
        
    FROM bronze.diabetes_raw br 
    WHERE br.id NOT IN (
        SELECT source_id 
        FROM silver.diabetes_cleaned 
        WHERE source_id IS NOT NULL
    );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Log processing statistics
    INSERT INTO analytics.pipeline_execution_log (
        pipeline_name, pipeline_stage, execution_id, 
        start_time, end_time, records_processed, 
        status, notes
    ) VALUES (
        'diabetes_etl', 'bronze_to_silver', execution_id,
        start_time, CURRENT_TIMESTAMP, processed_count,
        'SUCCESS', 
        format('Processed %s records from bronze to silver layer', processed_count)
    );
    
    RAISE NOTICE 'Bronze to Silver processing completed - Records processed: %', processed_count;
    RETURN processed_count;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        INSERT INTO analytics.pipeline_execution_log (
            pipeline_name, pipeline_stage, execution_id, 
            start_time, end_time, records_processed, 
            status, error_message
        ) VALUES (
            'diabetes_etl', 'bronze_to_silver', execution_id,
            start_time, CURRENT_TIMESTAMP, 0,
            'ERROR', SQLERRM
        );
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SILVER TO GOLD DATA PROCESSING FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION process_silver_to_gold()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    execution_id TEXT := uuid_generate_v4()::TEXT;
BEGIN
    RAISE NOTICE 'Starting Silver to Gold data processing with advanced feature engineering - Execution ID: %', execution_id;
    
    -- Enhanced feature engineering for gold layer
    INSERT INTO gold.diabetes_gold (
        source_id, pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, diabetes,
        
        -- Categorical features
        bmi_category, age_group, glucose_category, bp_category,
        insulin_category, pregnancies_category,
        
        -- Risk calculations
        risk_score, prediction, prediction_probability,
        
        -- Interaction features
        glucose_bmi_interaction, age_glucose_interaction,
        insulin_glucose_interaction, age_bmi_interaction,
        bp_bmi_interaction, pregnancy_glucose_interaction,
        
        -- Normalized features (0-1 scale)
        glucose_normalized, bmi_normalized, age_normalized,
        bp_normalized, insulin_normalized,
        
        -- Clinical risk assessments
        health_risk_level, intervention_priority, estimated_cost_category,
        metabolic_syndrome_risk, cardiovascular_risk, pregnancy_risk_factor,
        lifestyle_risk_score, insulin_resistance_score,
        
        -- Business intelligence features
        treatment_recommendation, followup_frequency,
        specialist_referral_needed,
        
        -- Statistical features
        glucose_zscore, bmi_zscore, age_percentile, risk_percentile,
        
        -- Data quality and metadata
        data_source, processing_version, feature_engineering_version,
        quality_score, anomaly_flags, feature_importance, model_features,
        processed_timestamp, processing_date, etl_batch_id
    )
    SELECT 
        s.id as source_id,
        s.pregnancies, s.glucose, s.blood_pressure, s.skin_thickness,
        s.insulin, s.bmi, s.diabetes_pedigree_function, s.age, s.outcome as diabetes,
        
        -- Enhanced BMI Categories (WHO classification)
        CASE 
            WHEN s.bmi < 16 THEN 'Severely Underweight'
            WHEN s.bmi < 18.5 THEN 'Underweight'
            WHEN s.bmi < 25 THEN 'Normal Weight'
            WHEN s.bmi < 30 THEN 'Overweight'
            WHEN s.bmi < 35 THEN 'Obese Class I'
            WHEN s.bmi < 40 THEN 'Obese Class II'
            ELSE 'Obese Class III'
        END as bmi_category,
        
        -- Detailed Age Groups
        CASE 
            WHEN s.age < 25 THEN 'Young Adult (18-24)'
            WHEN s.age < 35 THEN 'Adult (25-34)'
            WHEN s.age < 45 THEN 'Middle-aged (35-44)'
            WHEN s.age < 55 THEN 'Mature (45-54)'
            WHEN s.age < 65 THEN 'Pre-senior (55-64)'
            ELSE 'Senior (65+)'
        END as age_group,
        
        -- Clinical Glucose Categories (ADA Guidelines)
        CASE 
            WHEN s.glucose < 70 THEN 'Hypoglycemic'
            WHEN s.glucose < 100 THEN 'Normal'
            WHEN s.glucose < 126 THEN 'Prediabetic'
            WHEN s.glucose < 200 THEN 'Diabetic'
            ELSE 'Severely Diabetic'
        END as glucose_category,
        
        -- Blood Pressure Categories (AHA Guidelines)
        CASE 
            WHEN s.blood_pressure < 80 THEN 'Low'
            WHEN s.blood_pressure < 120 THEN 'Normal'
            WHEN s.blood_pressure < 130 THEN 'Elevated'
            WHEN s.blood_pressure < 140 THEN 'High Stage 1'
            WHEN s.blood_pressure < 180 THEN 'High Stage 2'
            ELSE 'Hypertensive Crisis'
        END as bp_category,
        
        -- Insulin Categories
        CASE 
            WHEN s.insulin = 0 THEN 'Not Measured'
            WHEN s.insulin < 16 THEN 'Normal'
            WHEN s.insulin < 25 THEN 'Elevated'
            ELSE 'High'
        END as insulin_category,
        
        -- Pregnancies Categories
        CASE 
            WHEN s.pregnancies = 0 THEN 'Nulliparous'
            WHEN s.pregnancies <= 2 THEN 'Low Parity'
            WHEN s.pregnancies <= 4 THEN 'Moderate Parity'
            ELSE 'High Parity'
        END as pregnancies_category,
        
        -- Advanced Risk Score (weighted clinical factors)
        ROUND(
            (GREATEST(0, LEAST(1, s.glucose / 200.0)) * 0.25 +
             GREATEST(0, LEAST(1, s.bmi / 50.0)) * 0.20 +
             GREATEST(0, LEAST(1, s.age / 100.0)) * 0.15 +
             GREATEST(0, LEAST(1, s.blood_pressure / 200.0)) * 0.15 +
             GREATEST(0, LEAST(1, s.diabetes_pedigree_function / 2.0)) * 0.15 +
             GREATEST(0, LEAST(1, s.pregnancies / 15.0)) * 0.10)::numeric, 
            4
        ) as risk_score,
        
        -- Prediction based on clinical thresholds
        CASE 
            WHEN s.glucose >= 126 OR 
                 (s.bmi >= 30 AND s.glucose >= 100) OR
                 (s.age >= 45 AND s.glucose >= 110 AND s.bmi >= 25)
            THEN 1 
            ELSE 0 
        END as prediction,
        
        -- Prediction probability (sophisticated calculation)
        ROUND(
            LEAST(1.0, 
                (s.glucose / 126.0 * 0.30) +
                (CASE WHEN s.bmi > 25 THEN (s.bmi - 25) / 15.0 * 0.25 ELSE 0 END) +
                (s.age / 100.0 * 0.20) +
                (s.diabetes_pedigree_function * 0.15) +
                (CASE WHEN s.blood_pressure > 140 THEN 0.10 ELSE 0 END)
            )::numeric, 
            4
        ) as prediction_probability,
        
        -- Advanced Interaction Features
        ROUND((s.glucose * s.bmi / 3000.0)::numeric, 4) as glucose_bmi_interaction,
        ROUND((s.age * s.glucose / 5000.0)::numeric, 4) as age_glucose_interaction,
        ROUND((COALESCE(s.insulin, 0) * s.glucose / 50000.0)::numeric, 4) as insulin_glucose_interaction,
        ROUND((s.age * s.bmi / 2000.0)::numeric, 4) as age_bmi_interaction,
        ROUND((s.blood_pressure * s.bmi / 3000.0)::numeric, 4) as bp_bmi_interaction,
        ROUND((s.pregnancies * s.glucose / 500.0)::numeric, 4) as pregnancy_glucose_interaction,
        
        -- Comprehensive Normalized Features (z-score normalization)
        ROUND(((s.glucose - 120.89) / 31.97)::numeric, 4) as glucose_normalized,
        ROUND(((s.bmi - 31.99) / 7.88)::numeric, 4) as bmi_normalized,
        ROUND(((s.age - 33.24) / 11.76)::numeric, 4) as age_normalized,
        ROUND(((s.blood_pressure - 69.11) / 19.36)::numeric, 4) as bp_normalized,
        ROUND(((COALESCE(s.insulin, 79.8) - 79.8) / 115.24)::numeric, 4) as insulin_normalized,
        
        -- Clinical Health Risk Level (comprehensive assessment)
        CASE 
            WHEN s.glucose >= 200 OR (s.bmi >= 40) OR 
                 (s.glucose >= 180 AND s.bmi >= 35) OR
                 (s.blood_pressure >= 180) THEN 'Critical'
            WHEN s.glucose >= 140 OR s.bmi >= 30 OR 
                 (s.glucose >= 126 AND s.bmi >= 25) OR
                 (s.age >= 60 AND s.glucose >= 110) THEN 'High'
            WHEN s.glucose >= 110 OR s.bmi >= 25 OR
                 (s.age >= 45 AND s.glucose >= 100) THEN 'Medium'
            ELSE 'Low'
        END as health_risk_level,
        
        -- Intervention Priority (clinical urgency)
        CASE 
            WHEN s.glucose >= 200 OR s.bmi >= 40 OR s.blood_pressure >= 180 THEN 'Immediate'
            WHEN s.glucose >= 140 OR s.bmi >= 30 OR s.blood_pressure >= 160 THEN 'Soon'
            WHEN s.glucose >= 110 OR s.bmi >= 25 OR s.blood_pressure >= 140 THEN 'Routine'
            ELSE 'Monitor'
        END as intervention_priority,
        
        -- Estimated Cost Category (healthcare cost prediction)
        CASE 
            WHEN s.glucose >= 200 OR s.bmi >= 40 THEN 'Very High'
            WHEN s.glucose >= 140 OR s.bmi >= 30 THEN 'High'
            WHEN s.glucose >= 110 OR s.bmi >= 25 THEN 'Medium'
            ELSE 'Low'
        END as estimated_cost_category,
        
        -- Advanced Clinical Risk Scores
        ROUND(((s.bmi / 50.0) + (s.glucose / 200.0) + (s.blood_pressure / 200.0) + 
               (CASE WHEN s.insulin = 0 THEN 0.2 ELSE 0 END))::numeric / 4, 4) as metabolic_syndrome_risk,
        
        ROUND(((s.age / 100.0) + (s.blood_pressure / 200.0) + (s.bmi / 50.0) + 
               (s.diabetes_pedigree_function))::numeric / 4, 4) as cardiovascular_risk,
        
        ROUND((CASE WHEN s.pregnancies > 0 THEN (s.pregnancies / 10.0) + (s.glucose / 200.0) 
                   ELSE 0 END)::numeric / 2, 4) as pregnancy_risk_factor,
        
        ROUND(((s.bmi / 50.0) + (s.age / 100.0) + 
               (CASE WHEN s.glucose > 100 THEN 0.2 ELSE 0 END))::numeric / 3, 4) as lifestyle_risk_score,
        
        ROUND((CASE WHEN s.insulin = 0 THEN 0.5 ELSE s.insulin / 200.0 END + 
               s.glucose / 200.0 + s.bmi / 50.0)::numeric / 3, 4) as insulin_resistance_score,
        
        -- Treatment Recommendations (clinical decision support)
        CASE 
            WHEN s.glucose >= 200 THEN 'Immediate endocrinologist referral, insulin therapy consideration'
            WHEN s.glucose >= 140 THEN 'Diabetes medication, lifestyle intervention, regular monitoring'
            WHEN s.glucose >= 110 THEN 'Lifestyle modification, diet counseling, exercise program'
            WHEN s.bmi >= 30 THEN 'Weight management program, nutritionist consultation'
            ELSE 'Preventive care, annual screening, healthy lifestyle maintenance'
        END as treatment_recommendation,
        
        -- Follow-up Frequency
        CASE 
            WHEN s.glucose >= 200 OR s.bmi >= 40 THEN 'Weekly'
            WHEN s.glucose >= 140 OR s.bmi >= 30 THEN 'Monthly'
            WHEN s.glucose >= 110 OR s.bmi >= 25 THEN 'Quarterly'
            ELSE 'Annually'
        END as followup_frequency,
        
        -- Specialist Referral Needed
        (s.glucose >= 140 OR s.bmi >= 35 OR s.blood_pressure >= 160 OR 
         (s.glucose >= 126 AND s.pregnancies > 0)) as specialist_referral_needed,
        
        -- Statistical Features (population-based calculations)
        ROUND(((s.glucose - 120.89) / 31.97)::numeric, 4) as glucose_zscore,
        ROUND(((s.bmi - 31.99) / 7.88)::numeric, 4) as bmi_zscore,
        
        -- Age and risk percentiles (simplified calculation)
        ROUND((s.age / 100.0)::numeric, 4) as age_percentile,
        ROUND(LEAST(1.0, ((s.glucose / 126.0) + (s.bmi / 40.0)) / 2)::numeric, 4) as risk_percentile,
        
        -- Metadata and tracking
        'ENHANCED_ETL_PIPELINE' as data_source,
        '2.0' as processing_version,
        '2.0' as feature_engineering_version,
        s.quality_score,
        
        -- Enhanced anomaly flags from silver layer
        s.anomaly_flags,
        
        -- Feature importance (for ML models)
        jsonb_build_object(
            'glucose', 0.25, 'bmi', 0.20, 'age', 0.15, 
            'blood_pressure', 0.15, 'diabetes_pedigree_function', 0.15,
            'pregnancies', 0.10
        ) as feature_importance,
        
        -- Model features (all engineered features for ML)
        jsonb_build_object(
            'glucose_normalized', ROUND(((s.glucose - 120.89) / 31.97)::numeric, 4),
            'bmi_normalized', ROUND(((s.bmi - 31.99) / 7.88)::numeric, 4),
            'interaction_glucose_bmi', ROUND((s.glucose * s.bmi / 3000.0)::numeric, 4),
            'risk_score', ROUND((s.glucose / 200.0 * 0.25 + s.bmi / 50.0 * 0.20)::numeric, 4)
        ) as model_features,
        
        CURRENT_TIMESTAMP as processed_timestamp,
        CURRENT_DATE as processing_date,
        execution_id as etl_batch_id
        
    FROM silver.diabetes_cleaned s 
    WHERE s.is_valid = TRUE 
      AND s.id NOT IN (
          SELECT source_id 
          FROM gold.diabetes_gold 
          WHERE source_id IS NOT NULL
      );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Log processing statistics
    INSERT INTO analytics.pipeline_execution_log (
        pipeline_name, pipeline_stage, execution_id, 
        start_time, end_time, records_processed, 
        status, notes
    ) VALUES (
        'diabetes_etl', 'silver_to_gold', execution_id,
        start_time, CURRENT_TIMESTAMP, processed_count,
        'SUCCESS', 
        format('Processed %s records from silver to gold layer with enhanced feature engineering', processed_count)
    );
    
    RAISE NOTICE 'Silver to Gold processing completed - Records processed: %', processed_count;
    RETURN processed_count;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        INSERT INTO analytics.pipeline_execution_log (
            pipeline_name, pipeline_stage, execution_id, 
            start_time, end_time, records_processed, 
            status, error_message
        ) VALUES (
            'diabetes_etl', 'silver_to_gold', execution_id,
            start_time, CURRENT_TIMESTAMP, 0,
            'ERROR', SQLERRM
        );
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ANALYTICS AGGREGATIONS REFRESH FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_analytics_aggregations()
RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    execution_id TEXT := uuid_generate_v4()::TEXT;
    processed_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    RAISE NOTICE 'Starting analytics aggregations refresh - Execution ID: %', execution_id;
    
    -- 1. Refresh Health Insights Gold Table
    DELETE FROM gold.health_insights_gold WHERE report_date = CURRENT_DATE;
    
    INSERT INTO gold.health_insights_gold (
        age_group, health_risk_category, bmi_category,
        total_patients, diabetic_patients, high_risk_patients,
        avg_bmi, avg_glucose, avg_age, avg_risk_score,
        high_risk_count, high_risk_percentage, report_date
    )
    SELECT 
        dg.age_group,
        dg.health_risk_level as health_risk_category,
        dg.bmi_category,
        COUNT(*) as total_patients,
        SUM(CASE WHEN dg.diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
        SUM(CASE WHEN dg.health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
        ROUND(AVG(dg.bmi)::numeric, 2) as avg_bmi,
        ROUND(AVG(dg.glucose)::numeric, 2) as avg_glucose,
        ROUND(AVG(dg.age)::numeric, 2) as avg_age,
        ROUND(AVG(dg.risk_score)::numeric, 3) as avg_risk_score,
        SUM(CASE WHEN dg.health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_count,
        ROUND((SUM(CASE WHEN dg.health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 2) as high_risk_percentage,
        CURRENT_DATE as report_date
    FROM gold.diabetes_gold dg
    WHERE dg.processing_date = CURRENT_DATE
    GROUP BY dg.age_group, dg.health_risk_level, dg.bmi_category;
    
    processed_tables := array_append(processed_tables, 'health_insights_gold');
    
    -- 2. Refresh Daily Statistics
    INSERT INTO analytics.daily_statistics (
        stat_date, total_records, diabetic_cases, high_risk_cases,
        avg_glucose, avg_bmi, avg_age, avg_risk_score,
        glucose_normal_pct, glucose_prediabetic_pct, glucose_diabetic_pct,
        bmi_normal_pct, bmi_overweight_pct, bmi_obese_pct,
        age_young_pct, age_middle_pct, age_senior_pct,
        immediate_care_needed, routine_monitoring_needed
    )
    SELECT 
        CURRENT_DATE as stat_date,
        COUNT(*) as total_records,
        SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_cases,
        SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_cases,
        ROUND(AVG(glucose)::numeric, 2) as avg_glucose,
        ROUND(AVG(bmi)::numeric, 2) as avg_bmi,
        ROUND(AVG(age)::numeric, 1) as avg_age,
        ROUND(AVG(risk_score)::numeric, 4) as avg_risk_score,
        
        -- Glucose distribution percentages
        ROUND((SUM(CASE WHEN glucose_category = 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as glucose_normal_pct,
        ROUND((SUM(CASE WHEN glucose_category = 'Prediabetic' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as glucose_prediabetic_pct,
        ROUND((SUM(CASE WHEN glucose_category IN ('Diabetic', 'Severely Diabetic') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as glucose_diabetic_pct,
        
        -- BMI distribution percentages
        ROUND((SUM(CASE WHEN bmi_category = 'Normal Weight' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as bmi_normal_pct,
        ROUND((SUM(CASE WHEN bmi_category = 'Overweight' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as bmi_overweight_pct,
        ROUND((SUM(CASE WHEN bmi_category LIKE 'Obese%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as bmi_obese_pct,
        
        -- Age distribution percentages
        ROUND((SUM(CASE WHEN age < 35 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as age_young_pct,
        ROUND((SUM(CASE WHEN age >= 35 AND age < 55 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as age_middle_pct,
        ROUND((SUM(CASE WHEN age >= 55 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1) as age_senior_pct,
        
        -- Care priorities
        SUM(CASE WHEN intervention_priority = 'Immediate' THEN 1 ELSE 0 END) as immediate_care_needed,
        SUM(CASE WHEN intervention_priority = 'Monitor' THEN 1 ELSE 0 END) as routine_monitoring_needed
        
    FROM gold.diabetes_gold
    WHERE processing_date = CURRENT_DATE
    ON CONFLICT (stat_date) DO UPDATE SET
        total_records = EXCLUDED.total_records,
        diabetic_cases = EXCLUDED.diabetic_cases,
        high_risk_cases = EXCLUDED.high_risk_cases,
        avg_glucose = EXCLUDED.avg_glucose,
        avg_bmi = EXCLUDED.avg_bmi,
        avg_age = EXCLUDED.avg_age,
        avg_risk_score = EXCLUDED.avg_risk_score,
        glucose_normal_pct = EXCLUDED.glucose_normal_pct,
        glucose_prediabetic_pct = EXCLUDED.glucose_prediabetic_pct,
        glucose_diabetic_pct = EXCLUDED.glucose_diabetic_pct,
        bmi_normal_pct = EXCLUDED.bmi_normal_pct,
        bmi_overweight_pct = EXCLUDED.bmi_overweight_pct,
        bmi_obese_pct = EXCLUDED.bmi_obese_pct,
        age_young_pct = EXCLUDED.age_young_pct,
        age_middle_pct = EXCLUDED.age_middle_pct,
        age_senior_pct = EXCLUDED.age_senior_pct,
        immediate_care_needed = EXCLUDED.immediate_care_needed,
        routine_monitoring_needed = EXCLUDED.routine_monitoring_needed,
        updated_at = CURRENT_TIMESTAMP;
    
    processed_tables := array_append(processed_tables, 'daily_statistics');
    
    -- 3. Update Latest Metrics for Grafana
    INSERT INTO analytics.latest_metrics (metric_name, metric_value, metric_type, updated_at)
    SELECT * FROM (
        SELECT 'total_patients'::text, COUNT(*)::text, 'count'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'diabetic_patients'::text, SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END)::text, 'count'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'high_risk_patients'::text, SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END)::text, 'count'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'avg_glucose'::text, ROUND(AVG(glucose)::numeric, 1)::text, 'average'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'avg_bmi'::text, ROUND(AVG(bmi)::numeric, 1)::text, 'average'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'diabetes_rate'::text, ROUND((SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1)::text, 'percentage'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
        UNION ALL
        SELECT 'high_risk_rate'::text, ROUND((SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 1)::text, 'percentage'::text, CURRENT_TIMESTAMP
        FROM gold.diabetes_gold WHERE processing_date = CURRENT_DATE
    ) metrics
    ON CONFLICT (metric_name) DO UPDATE SET
        metric_value = EXCLUDED.metric_value,
        updated_at = EXCLUDED.updated_at;
    
    processed_tables := array_append(processed_tables, 'latest_metrics');
    
    -- 4. Log successful completion
    INSERT INTO analytics.pipeline_execution_log (
        pipeline_name, pipeline_stage, execution_id, 
        start_time, end_time, records_processed, 
        status, notes
    ) VALUES (
        'diabetes_etl', 'analytics_refresh', execution_id,
        start_time, CURRENT_TIMESTAMP, array_length(processed_tables, 1),
        'SUCCESS', 
        format('Successfully refreshed analytics aggregations for tables: %s', array_to_string(processed_tables, ', '))
    );
    
    RAISE NOTICE 'Analytics aggregations refresh completed - Tables processed: %', array_to_string(processed_tables, ', ');
    
EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        INSERT INTO analytics.pipeline_execution_log (
            pipeline_name, pipeline_stage, execution_id, 
            start_time, end_time, records_processed, 
            status, error_message
        ) VALUES (
            'diabetes_etl', 'analytics_refresh', execution_id,
            start_time, CURRENT_TIMESTAMP, 0,
            'ERROR', SQLERRM
        );
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ADDITIONAL UTILITY FUNCTIONS
-- =============================================================================

-- Function to get pipeline statistics
CREATE OR REPLACE FUNCTION get_pipeline_statistics()
RETURNS TABLE (
    layer_name TEXT,
    table_name TEXT,
    record_count BIGINT,
    latest_update TIMESTAMP,
    data_quality_avg NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'bronze'::TEXT, 'diabetes_raw'::TEXT, COUNT(*)::BIGINT, MAX(ingestion_timestamp), NULL::NUMERIC
    FROM bronze.diabetes_raw
    UNION ALL
    SELECT 'silver'::TEXT, 'diabetes_cleaned'::TEXT, COUNT(*)::BIGINT, MAX(processed_at), ROUND(AVG(quality_score)::NUMERIC, 3)
    FROM silver.diabetes_cleaned
    UNION ALL
    SELECT 'gold'::TEXT, 'diabetes_gold'::TEXT, COUNT(*)::BIGINT, MAX(processed_timestamp), ROUND(AVG(quality_score)::NUMERIC, 3)
    FROM gold.diabetes_gold;
END;
$$ LANGUAGE plpgsql;

-- Function to clean old pipeline logs (retention policy)
CREATE OR REPLACE FUNCTION cleanup_pipeline_logs(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    DELETE FROM analytics.pipeline_execution_log 
    WHERE start_time < CURRENT_DATE - INTERVAL '1 day' * retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RAISE NOTICE 'Cleaned up % old pipeline log entries (older than % days)', deleted_count, retention_days;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'POSTGRESQL ETL FUNCTIONS CREATED SUCCESSFULLY!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Functions available:';
    RAISE NOTICE '- process_bronze_to_silver()';
    RAISE NOTICE '- process_silver_to_gold()';
    RAISE NOTICE '- refresh_analytics_aggregations()';
    RAISE NOTICE '- get_pipeline_statistics()';
    RAISE NOTICE '- cleanup_pipeline_logs(retention_days)';
    RAISE NOTICE 'Ready for Airflow DAG execution!';
END $$;

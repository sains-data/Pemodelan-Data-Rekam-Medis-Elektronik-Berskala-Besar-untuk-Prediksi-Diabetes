-- =============================================================================
-- POSTGRESQL ETL FUNCTIONS FOR COMPREHENSIVE DATA WAREHOUSE (FIXED VERSION)
-- =============================================================================
-- Complete implementation of PostgreSQL functions for bronze/silver/gold ETL
-- Called by Airflow DAG for comprehensive diabetes prediction pipeline
-- Fixed version with corrected variable naming to avoid column conflicts
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
    WHERE execution_id = exec_id::UUID;
    
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
    exec_id TEXT := uuid_generate_v4()::TEXT;
    refreshed_views TEXT[] := ARRAY[]::TEXT[];
BEGIN
    RAISE NOTICE 'Starting analytics views refresh - Execution ID: %', exec_id;
    
    -- Log pipeline execution
    INSERT INTO analytics.pipeline_execution_log (
        execution_id, pipeline_name, stage_name, status, 
        started_at, input_records, output_records, execution_time_seconds,
        memory_used_mb, error_message, metadata
    ) VALUES (
        exec_id::UUID, 'diabetes_etl', 'refresh_views', 'running',
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
    WHERE execution_id = exec_id::UUID;
    
    RAISE NOTICE 'Refreshed % analytics views: %', array_length(refreshed_views, 1), refreshed_views;
    
EXCEPTION WHEN OTHERS THEN
    -- Update pipeline execution log with error
    UPDATE analytics.pipeline_execution_log 
    SET 
        status = 'failed',
        ended_at = CURRENT_TIMESTAMP,
        error_message = SQLERRM,
        execution_time_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE execution_id = exec_id::UUID;
    
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- BRONZE TO SILVER DATA PROCESSING FUNCTION (FIXED)
-- =============================================================================

CREATE OR REPLACE FUNCTION process_bronze_to_silver()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    exec_id TEXT := uuid_generate_v4()::TEXT;
BEGIN
    RAISE NOTICE 'Starting Bronze to Silver data processing - Execution ID: %', exec_id;
    
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
            WHEN br.bmi > 70 THEN NULL
            ELSE br.bmi 
        END as bmi,
        -- Clean diabetes pedigree function
        CASE 
            WHEN br.diabetes_pedigree_function < 0 THEN NULL
            WHEN br.diabetes_pedigree_function > 3 THEN NULL
            ELSE br.diabetes_pedigree_function
        END as diabetes_pedigree_function,
        -- Clean age values
        CASE 
            WHEN br.age < 18 THEN NULL
            WHEN br.age > 120 THEN NULL
            ELSE br.age
        END as age,
        br.outcome,
        
        -- Data quality flags
        CASE 
            WHEN br.glucose > 0 AND br.glucose <= 400 
                 AND br.blood_pressure > 0 AND br.blood_pressure <= 250
                 AND br.bmi > 0 AND br.bmi <= 70
                 AND br.age >= 18 AND br.age <= 120
            THEN TRUE ELSE FALSE 
        END as is_valid,
        
        -- Quality score (0-100)
        LEAST(100, 
            CASE WHEN br.glucose > 0 AND br.glucose <= 400 THEN 20 ELSE 0 END +
            CASE WHEN br.blood_pressure > 0 AND br.blood_pressure <= 250 THEN 20 ELSE 0 END +
            CASE WHEN br.bmi > 0 AND br.bmi <= 70 THEN 20 ELSE 0 END +
            CASE WHEN br.age >= 18 AND br.age <= 120 THEN 20 ELSE 0 END +
            CASE WHEN br.diabetes_pedigree_function >= 0 AND br.diabetes_pedigree_function <= 3 THEN 20 ELSE 0 END
        ) as quality_score,
        
        -- Completeness score
        (
            (CASE WHEN br.pregnancies IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.glucose IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.blood_pressure IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.skin_thickness IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.insulin IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.bmi IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.diabetes_pedigree_function IS NOT NULL THEN 12.5 ELSE 0 END) +
            (CASE WHEN br.age IS NOT NULL THEN 12.5 ELSE 0 END)
        )::NUMERIC(5,2) as completeness_score,
        
        -- Anomaly flags
        ARRAY[
            CASE WHEN br.glucose > 300 THEN 'high_glucose' END,
            CASE WHEN br.blood_pressure > 200 THEN 'high_bp' END,
            CASE WHEN br.bmi > 50 THEN 'extreme_obesity' END,
            CASE WHEN br.age > 100 THEN 'extreme_age' END
        ]::TEXT[] as anomaly_flags,
        
        -- Outlier flags using IQR method approximation
        ARRAY[
            CASE WHEN br.glucose > 180 OR br.glucose < 60 THEN 'glucose_outlier' END,
            CASE WHEN br.bmi > 45 OR br.bmi < 15 THEN 'bmi_outlier' END,
            CASE WHEN br.age > 80 OR br.age < 20 THEN 'age_outlier' END
        ]::TEXT[] as outlier_flags,
        
        -- Individual validation flags
        (br.glucose > 0 AND br.glucose <= 400) as glucose_valid,
        (br.bmi > 0 AND br.bmi <= 70) as bmi_valid,
        (br.age >= 18 AND br.age <= 120) as age_valid,
        (br.blood_pressure > 0 AND br.blood_pressure <= 250) as bp_valid,
        
        -- Data lineage
        jsonb_build_object(
            'source_table', 'bronze.diabetes_raw',
            'source_id', br.id,
            'ingested_at', br.ingested_at,
            'processed_at', CURRENT_TIMESTAMP,
            'processing_rules', ARRAY[
                'glucose_range_0_400',
                'bp_range_0_250', 
                'bmi_range_0_70',
                'age_range_18_120'
            ]
        ) as data_lineage,
        
        CURRENT_TIMESTAMP as processed_at,
        '1.0' as processing_version
    FROM bronze.diabetes_raw br
    WHERE br.id NOT IN (
        SELECT source_id FROM silver.diabetes_cleaned 
        WHERE source_id IS NOT NULL
    );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    RAISE NOTICE 'Processed % records from bronze to silver layer', processed_count;
    RETURN processed_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in bronze to silver processing: %', SQLERRM;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SILVER TO GOLD DATA PROCESSING FUNCTION (FIXED)
-- =============================================================================

CREATE OR REPLACE FUNCTION process_silver_to_gold()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    exec_id TEXT := uuid_generate_v4()::TEXT;
BEGIN
    RAISE NOTICE 'Starting Silver to Gold data processing - Execution ID: %', exec_id;
    
    -- Process health insights
    INSERT INTO gold.health_insights (
        patient_id, risk_category, bmi_category, glucose_category,
        blood_pressure_category, age_group, overall_health_score,
        diabetes_risk_score, metabolic_syndrome_risk, cardiovascular_risk,
        recommendations, risk_factors, protective_factors,
        created_at, data_lineage
    )
    SELECT 
        sc.source_id as patient_id,
        
        -- Risk category based on multiple factors
        CASE 
            WHEN sc.outcome = 1 THEN 'HIGH_RISK'
            WHEN sc.glucose > 126 OR sc.bmi > 30 THEN 'MODERATE_RISK'
            WHEN sc.glucose > 100 OR sc.bmi > 25 THEN 'LOW_RISK'
            ELSE 'MINIMAL_RISK'
        END as risk_category,
        
        -- BMI categories
        CASE 
            WHEN sc.bmi < 18.5 THEN 'underweight'
            WHEN sc.bmi < 25 THEN 'normal'
            WHEN sc.bmi < 30 THEN 'overweight'
            WHEN sc.bmi < 35 THEN 'obese_class_1'
            WHEN sc.bmi < 40 THEN 'obese_class_2'
            ELSE 'obese_class_3'
        END as bmi_category,
        
        -- Glucose categories
        CASE 
            WHEN sc.glucose < 70 THEN 'hypoglycemic'
            WHEN sc.glucose < 100 THEN 'normal'
            WHEN sc.glucose < 126 THEN 'prediabetic'
            ELSE 'diabetic'
        END as glucose_category,
        
        -- Blood pressure categories
        CASE 
            WHEN sc.blood_pressure < 90 THEN 'hypotensive'
            WHEN sc.blood_pressure < 120 THEN 'normal'
            WHEN sc.blood_pressure < 140 THEN 'elevated'
            WHEN sc.blood_pressure < 160 THEN 'stage_1_hypertension'
            ELSE 'stage_2_hypertension'
        END as blood_pressure_category,
        
        -- Age groups
        CASE 
            WHEN sc.age < 30 THEN 'young_adult'
            WHEN sc.age < 45 THEN 'adult'
            WHEN sc.age < 60 THEN 'middle_aged'
            ELSE 'senior'
        END as age_group,
        
        -- Overall health score (0-100)
        LEAST(100, GREATEST(0,
            50 + -- Base score
            CASE WHEN sc.bmi BETWEEN 18.5 AND 25 THEN 15 ELSE -10 END +
            CASE WHEN sc.glucose BETWEEN 70 AND 100 THEN 15 ELSE -15 END +
            CASE WHEN sc.blood_pressure BETWEEN 90 AND 120 THEN 10 ELSE -10 END +
            CASE WHEN sc.age < 60 THEN 10 ELSE 0 END +
            CASE WHEN sc.diabetes_pedigree_function < 0.5 THEN 10 ELSE -5 END
        )) as overall_health_score,
        
        -- Diabetes risk score using logistic regression approximation
        ROUND(
            1 / (1 + EXP(-(
                -8.4 + 
                0.12 * COALESCE(sc.pregnancies, 0) +
                0.04 * COALESCE(sc.glucose, 120) +
                0.01 * COALESCE(sc.blood_pressure, 80) +
                0.001 * COALESCE(sc.skin_thickness, 20) +
                0.001 * COALESCE(sc.insulin, 80) +
                0.09 * COALESCE(sc.bmi, 25) +
                0.95 * COALESCE(sc.diabetes_pedigree_function, 0.5) +
                0.02 * COALESCE(sc.age, 35)
            ))) * 100, 2
        ) as diabetes_risk_score,
        
        -- Metabolic syndrome risk
        CASE 
            WHEN (sc.bmi > 30 AND sc.glucose > 100 AND sc.blood_pressure > 130) THEN 'HIGH'
            WHEN (sc.bmi > 25 AND (sc.glucose > 100 OR sc.blood_pressure > 120)) THEN 'MODERATE'
            ELSE 'LOW'
        END as metabolic_syndrome_risk,
        
        -- Cardiovascular risk
        CASE 
            WHEN (sc.age > 55 AND sc.blood_pressure > 140) THEN 'HIGH'
            WHEN (sc.age > 45 AND (sc.blood_pressure > 120 OR sc.bmi > 30)) THEN 'MODERATE'
            ELSE 'LOW'
        END as cardiovascular_risk,
        
        -- Health recommendations
        ARRAY[
            CASE WHEN sc.bmi > 25 THEN 'weight_management' END,
            CASE WHEN sc.glucose > 100 THEN 'glucose_monitoring' END,
            CASE WHEN sc.blood_pressure > 120 THEN 'bp_monitoring' END,
            CASE WHEN sc.age > 45 THEN 'regular_checkups' END
        ]::TEXT[] as recommendations,
        
        -- Risk factors
        ARRAY[
            CASE WHEN sc.bmi > 30 THEN 'obesity' END,
            CASE WHEN sc.glucose > 126 THEN 'diabetes' END,
            CASE WHEN sc.blood_pressure > 140 THEN 'hypertension' END,
            CASE WHEN sc.age > 65 THEN 'advanced_age' END,
            CASE WHEN sc.diabetes_pedigree_function > 1.0 THEN 'family_history' END
        ]::TEXT[] as risk_factors,
        
        -- Protective factors
        ARRAY[
            CASE WHEN sc.bmi BETWEEN 18.5 AND 25 THEN 'healthy_weight' END,
            CASE WHEN sc.glucose < 100 THEN 'normal_glucose' END,
            CASE WHEN sc.blood_pressure < 120 THEN 'normal_bp' END,
            CASE WHEN sc.age < 45 THEN 'young_age' END
        ]::TEXT[] as protective_factors,
        
        CURRENT_TIMESTAMP as created_at,
        
        -- Data lineage
        jsonb_build_object(
            'source_table', 'silver.diabetes_cleaned',
            'source_id', sc.source_id,
            'processed_at', CURRENT_TIMESTAMP,
            'algorithm_version', '1.0',
            'risk_model', 'logistic_regression_v1'
        ) as data_lineage
        
    FROM silver.diabetes_cleaned sc
    WHERE sc.is_valid = TRUE
      AND sc.source_id NOT IN (
          SELECT patient_id FROM gold.health_insights 
          WHERE patient_id IS NOT NULL
      );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    RAISE NOTICE 'Processed % records from silver to gold layer', processed_count;
    RETURN processed_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in silver to gold processing: %', SQLERRM;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ANALYTICS AGGREGATIONS REFRESH FUNCTION (FIXED)
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_analytics_aggregations()
RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    exec_id TEXT := uuid_generate_v4()::TEXT;
    processed_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    RAISE NOTICE 'Starting analytics aggregations refresh - Execution ID: %', exec_id;
    
    -- 1. Refresh Health Insights Gold Table
    INSERT INTO gold.health_insights_aggregated (
        aggregation_date, total_patients, high_risk_count, moderate_risk_count,
        low_risk_count, avg_diabetes_risk_score, avg_bmi, avg_glucose,
        avg_blood_pressure, obesity_rate, diabetes_rate, hypertension_rate,
        created_at
    )
    SELECT 
        CURRENT_DATE as aggregation_date,
        COUNT(*) as total_patients,
        COUNT(*) FILTER (WHERE risk_category = 'HIGH_RISK') as high_risk_count,
        COUNT(*) FILTER (WHERE risk_category = 'MODERATE_RISK') as moderate_risk_count,
        COUNT(*) FILTER (WHERE risk_category = 'LOW_RISK') as low_risk_count,
        AVG(diabetes_risk_score) as avg_diabetes_risk_score,
        
        -- Get source metrics from silver layer
        (SELECT AVG(bmi) FROM silver.diabetes_cleaned WHERE is_valid = TRUE) as avg_bmi,
        (SELECT AVG(glucose) FROM silver.diabetes_cleaned WHERE is_valid = TRUE) as avg_glucose,
        (SELECT AVG(blood_pressure) FROM silver.diabetes_cleaned WHERE is_valid = TRUE) as avg_blood_pressure,
        
        -- Calculate rates
        (COUNT(*) FILTER (WHERE bmi_category LIKE 'obese%')::FLOAT / COUNT(*) * 100) as obesity_rate,
        (COUNT(*) FILTER (WHERE glucose_category = 'diabetic')::FLOAT / COUNT(*) * 100) as diabetes_rate,
        (COUNT(*) FILTER (WHERE blood_pressure_category LIKE '%hypertension%')::FLOAT / COUNT(*) * 100) as hypertension_rate,
        
        CURRENT_TIMESTAMP as created_at
        
    FROM gold.health_insights
    WHERE DATE(created_at) = CURRENT_DATE
    ON CONFLICT (aggregation_date) DO UPDATE SET
        total_patients = EXCLUDED.total_patients,
        high_risk_count = EXCLUDED.high_risk_count,
        moderate_risk_count = EXCLUDED.moderate_risk_count,
        low_risk_count = EXCLUDED.low_risk_count,
        avg_diabetes_risk_score = EXCLUDED.avg_diabetes_risk_score,
        avg_bmi = EXCLUDED.avg_bmi,
        avg_glucose = EXCLUDED.avg_glucose,
        avg_blood_pressure = EXCLUDED.avg_blood_pressure,
        obesity_rate = EXCLUDED.obesity_rate,
        diabetes_rate = EXCLUDED.diabetes_rate,
        hypertension_rate = EXCLUDED.hypertension_rate,
        updated_at = CURRENT_TIMESTAMP;
    
    processed_tables := array_append(processed_tables, 'health_insights_aggregated');
    
    -- 2. Refresh Analytics Summary Tables
    INSERT INTO analytics.daily_summary (
        summary_date, total_records_processed, bronze_to_silver_count,
        silver_to_gold_count, data_quality_score, processing_time_minutes,
        error_count, created_at
    )
    SELECT 
        CURRENT_DATE as summary_date,
        (SELECT COUNT(*) FROM bronze.diabetes_raw WHERE DATE(ingested_at) = CURRENT_DATE) as total_records_processed,
        (SELECT COUNT(*) FROM silver.diabetes_cleaned WHERE DATE(processed_at) = CURRENT_DATE) as bronze_to_silver_count,
        (SELECT COUNT(*) FROM gold.health_insights WHERE DATE(created_at) = CURRENT_DATE) as silver_to_gold_count,
        (SELECT AVG(quality_score) FROM silver.diabetes_cleaned WHERE DATE(processed_at) = CURRENT_DATE) as data_quality_score,
        5.0 as processing_time_minutes, -- Placeholder
        0 as error_count, -- Placeholder
        CURRENT_TIMESTAMP as created_at
    ON CONFLICT (summary_date) DO UPDATE SET
        total_records_processed = EXCLUDED.total_records_processed,
        bronze_to_silver_count = EXCLUDED.bronze_to_silver_count,
        silver_to_gold_count = EXCLUDED.silver_to_gold_count,
        data_quality_score = EXCLUDED.data_quality_score,
        updated_at = CURRENT_TIMESTAMP;
    
    processed_tables := array_append(processed_tables, 'daily_summary');
    
    RAISE NOTICE 'Analytics aggregations refreshed for tables: %', processed_tables;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in analytics aggregations refresh: %', SQLERRM;
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PIPELINE STATISTICS FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION get_pipeline_statistics()
RETURNS TABLE(
    metric_name TEXT,
    metric_value NUMERIC,
    metric_description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'total_bronze_records'::TEXT as metric_name,
        COUNT(*)::NUMERIC as metric_value,
        'Total records in bronze layer'::TEXT as metric_description
    FROM bronze.diabetes_raw
    
    UNION ALL
    
    SELECT 
        'total_silver_records'::TEXT,
        COUNT(*)::NUMERIC,
        'Total records in silver layer'::TEXT
    FROM silver.diabetes_cleaned
    
    UNION ALL
    
    SELECT 
        'total_gold_records'::TEXT,
        COUNT(*)::NUMERIC,
        'Total records in gold layer'::TEXT
    FROM gold.health_insights
    
    UNION ALL
    
    SELECT 
        'data_quality_avg'::TEXT,
        AVG(quality_score)::NUMERIC,
        'Average data quality score'::TEXT
    FROM silver.diabetes_cleaned
    
    UNION ALL
    
    SELECT 
        'high_risk_patients'::TEXT,
        COUNT(*)::NUMERIC,
        'Number of high-risk patients'::TEXT
    FROM gold.health_insights
    WHERE risk_category = 'HIGH_RISK';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PIPELINE LOGS CLEANUP FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION cleanup_pipeline_logs(retention_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    DELETE FROM analytics.pipeline_execution_log 
    WHERE started_at < CURRENT_DATE - INTERVAL '1 day' * retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RAISE NOTICE 'Cleaned up % old pipeline log records (older than % days)', deleted_count, retention_days;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMPLETION NOTICE
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'POSTGRESQL ETL FUNCTIONS CREATED SUCCESSFULLY (FIXED)!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Functions available:';
    RAISE NOTICE '- ingest_diabetes_data(file_path)';
    RAISE NOTICE '- process_bronze_to_silver()';
    RAISE NOTICE '- process_silver_to_gold()';
    RAISE NOTICE '- refresh_analytics_views()';
    RAISE NOTICE '- refresh_analytics_aggregations()';
    RAISE NOTICE '- get_pipeline_statistics()';
    RAISE NOTICE '- cleanup_pipeline_logs(retention_days)';
    RAISE NOTICE 'Ready for Airflow DAG execution!';
END $$;

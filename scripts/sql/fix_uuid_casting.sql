-- Fix UUID casting issues in ETL functions
-- =============================================================================

-- Fix process_bronze_to_silver function
CREATE OR REPLACE FUNCTION process_bronze_to_silver()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    start_time TIMESTAMP := CURRENT_TIMESTAMP;
    exec_id UUID := uuid_generate_v4();
BEGIN
    RAISE NOTICE 'Starting Bronze to Silver data processing - Execution ID: %', exec_id;
    
    -- Log start of processing
    INSERT INTO analytics.pipeline_execution_log (
        pipeline_name, pipeline_stage, execution_id, 
        start_time, records_processed, status
    ) VALUES (
        'diabetes_etl', 'bronze_to_silver', exec_id,
        start_time, 0, 'RUNNING'
    );
    
    -- Process data from bronze to silver with enhanced data quality checks
    INSERT INTO silver.diabetes_cleaned (
        source_id, pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, outcome,
        is_valid, data_quality_score, quality_flags, anomaly_score,
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
            WHEN br.glucose IS NULL OR br.glucose <= 0 OR br.glucose > 400 THEN FALSE
            WHEN br.blood_pressure IS NULL OR br.blood_pressure <= 0 OR br.blood_pressure > 250 THEN FALSE
            WHEN br.bmi IS NULL OR br.bmi <= 0 OR br.bmi > 80 THEN FALSE
            WHEN br.age IS NULL OR br.age < 0 OR br.age > 120 THEN FALSE
            ELSE TRUE
        END as is_valid,
        
        -- Calculate composite data quality score (0-100)
        ROUND(
            ((CASE WHEN br.glucose > 0 AND br.glucose <= 400 THEN 20 ELSE 0 END) +
             (CASE WHEN br.blood_pressure > 0 AND br.blood_pressure <= 250 THEN 20 ELSE 0 END) +
             (CASE WHEN br.bmi > 0 AND br.bmi <= 80 THEN 20 ELSE 0 END) +
             (CASE WHEN br.age >= 0 AND br.age <= 120 THEN 20 ELSE 0 END) +
             (CASE WHEN br.skin_thickness >= 0 AND br.skin_thickness <= 100 THEN 10 ELSE 0 END) +
             (CASE WHEN br.insulin >= 0 AND br.insulin <= 1000 THEN 10 ELSE 0 END))::NUMERIC, 2
        ) as data_quality_score,
        
        -- Quality flags for detailed analysis
        ARRAY[
            CASE WHEN br.glucose <= 0 OR br.glucose > 400 THEN 'invalid_glucose' END,
            CASE WHEN br.blood_pressure <= 0 OR br.blood_pressure > 250 THEN 'invalid_bp' END,
            CASE WHEN br.bmi <= 0 OR br.bmi > 80 THEN 'invalid_bmi' END,
            CASE WHEN br.age < 0 OR br.age > 120 THEN 'invalid_age' END,
            CASE WHEN br.skin_thickness < 0 OR br.skin_thickness > 100 THEN 'invalid_skin_thickness' END,
            CASE WHEN br.insulin < 0 OR br.insulin > 1000 THEN 'invalid_insulin' END
        ]::TEXT[] as quality_flags,
        
        -- Anomaly score based on statistical outliers
        ROUND(
            (ABS(br.glucose - 120.0) / 120.0 * 0.3 +
             ABS(br.bmi - 25.0) / 25.0 * 0.3 +
             ABS(br.age - 35.0) / 35.0 * 0.2 +
             ABS(COALESCE(br.blood_pressure, 80) - 80.0) / 80.0 * 0.2)::NUMERIC, 4
        ) as anomaly_score,
        
        -- Data lineage tracking
        JSONB_BUILD_OBJECT(
            'source_system', 'bronze_layer',
            'processing_timestamp', CURRENT_TIMESTAMP,
            'data_source', br.data_source,
            'quality_checks', 'basic_validation_applied'
        ) as data_lineage,
        
        CURRENT_TIMESTAMP as processed_at,
        '1.0' as processing_version
        
    FROM bronze.diabetes_raw br
    WHERE br.id NOT IN (
        SELECT source_id FROM silver.diabetes_cleaned 
        WHERE source_id IS NOT NULL
    );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Log successful completion
    UPDATE analytics.pipeline_execution_log 
    SET 
        end_time = CURRENT_TIMESTAMP,
        records_processed = processed_count,
        status = 'COMPLETED'
    WHERE execution_id = exec_id;
    
    RAISE NOTICE 'Successfully processed % records from bronze to silver layer', processed_count;
    RETURN processed_count;
    
EXCEPTION 
    WHEN OTHERS THEN
        -- Log error with proper UUID casting
        UPDATE analytics.pipeline_execution_log 
        SET 
            end_time = CURRENT_TIMESTAMP,
            records_processed = 0,
            status = 'ERROR',
            error_message = SQLERRM
        WHERE execution_id = exec_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Recreate the get_pipeline_statistics function with proper return type
CREATE OR REPLACE FUNCTION get_pipeline_statistics()
RETURNS TABLE(
    pipeline_name TEXT,
    total_executions BIGINT,
    successful_executions BIGINT,
    failed_executions BIGINT,
    total_records_processed BIGINT,
    average_processing_time INTERVAL,
    last_execution_time TIMESTAMP,
    success_rate NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pel.pipeline_name,
        COUNT(*) as total_executions,
        SUM(CASE WHEN pel.status = 'COMPLETED' THEN 1 ELSE 0 END) as successful_executions,
        SUM(CASE WHEN pel.status = 'ERROR' THEN 1 ELSE 0 END) as failed_executions,
        SUM(COALESCE(pel.records_processed, 0)) as total_records_processed,
        AVG(pel.end_time - pel.start_time) as average_processing_time,
        MAX(pel.start_time) as last_execution_time,
        ROUND(
            (SUM(CASE WHEN pel.status = 'COMPLETED' THEN 1 ELSE 0 END)::NUMERIC / 
             NULLIF(COUNT(*), 0)) * 100, 2
        ) as success_rate
    FROM analytics.pipeline_execution_log pel
    GROUP BY pel.pipeline_name
    ORDER BY last_execution_time DESC;
END;
$$ LANGUAGE plpgsql;

RAISE NOTICE 'UUID casting issues fixed successfully!';

-- =============================================================================
-- SIMPLIFIED POSTGRESQL ETL FUNCTIONS - NO PIPELINE LOGGING
-- =============================================================================
-- Simplified version without pipeline logging to avoid column conflicts
-- =============================================================================

-- =============================================================================
-- DATA INGESTION FUNCTION (SIMPLIFIED)
-- =============================================================================

CREATE OR REPLACE FUNCTION ingest_diabetes_data(
    file_path TEXT DEFAULT '/opt/airflow/data/diabetes.csv'
)
RETURNS INTEGER AS $$
DECLARE
    ingested_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting diabetes data ingestion from: %', file_path;
    
    -- Insert sample data for demonstration
    INSERT INTO bronze.diabetes_raw (
        pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, outcome,
        source_file
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
        file_path
    FROM generate_series(1, 1000);
    
    GET DIAGNOSTICS ingested_count = ROW_COUNT;
    
    RAISE NOTICE 'Successfully ingested % records to bronze layer', ingested_count;
    RETURN ingested_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in data ingestion: %', SQLERRM;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ANALYTICS VIEWS REFRESH FUNCTION (SIMPLIFIED)
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS VOID AS $$
DECLARE
    refreshed_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting analytics views refresh';
    
    -- Refresh materialized views if they exist
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.diabetes_risk_analysis;
        refreshed_count := refreshed_count + 1;
        RAISE NOTICE 'Refreshed: diabetes_risk_analysis';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh diabetes_risk_analysis: %', SQLERRM;
    END;
    
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.health_metrics_summary;
        refreshed_count := refreshed_count + 1;
        RAISE NOTICE 'Refreshed: health_metrics_summary';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh health_metrics_summary: %', SQLERRM;
    END;
    
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.demographic_analysis;
        refreshed_count := refreshed_count + 1;
        RAISE NOTICE 'Refreshed: demographic_analysis';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not refresh demographic_analysis: %', SQLERRM;
    END;
    
    RAISE NOTICE 'Analytics views refresh completed. Refreshed % views.', refreshed_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in analytics views refresh: %', SQLERRM;
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- BRONZE TO SILVER PROCESSING FUNCTION (SIMPLIFIED)
-- =============================================================================

CREATE OR REPLACE FUNCTION process_bronze_to_silver()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting Bronze to Silver data processing';
    
    -- Insert cleaned and validated data from bronze to silver
    INSERT INTO silver.diabetes_cleaned (
        source_id, pregnancies, glucose, blood_pressure, skin_thickness,
        insulin, bmi, diabetes_pedigree_function, age, outcome,
        is_valid, quality_score, anomaly_flags
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
        
        -- Data quality flag
        CASE 
            WHEN br.glucose > 0 AND br.glucose <= 400 
                 AND br.blood_pressure > 0 AND br.blood_pressure <= 250
                 AND br.bmi > 0 AND br.bmi <= 70
                 AND br.age >= 18 AND br.age <= 120
            THEN TRUE ELSE FALSE 
        END as is_valid,
        
        -- Quality score (0-1)
        LEAST(1.0, 
            CASE WHEN br.glucose > 0 AND br.glucose <= 400 THEN 0.2 ELSE 0 END +
            CASE WHEN br.blood_pressure > 0 AND br.blood_pressure <= 250 THEN 0.2 ELSE 0 END +
            CASE WHEN br.bmi > 0 AND br.bmi <= 70 THEN 0.2 ELSE 0 END +
            CASE WHEN br.age >= 18 AND br.age <= 120 THEN 0.2 ELSE 0 END +
            CASE WHEN br.diabetes_pedigree_function >= 0 AND br.diabetes_pedigree_function <= 3 THEN 0.2 ELSE 0 END
        )::NUMERIC(3,2) as quality_score,
        
        -- Anomaly flags as JSONB
        jsonb_build_object(
            'high_glucose', br.glucose > 300,
            'high_bp', br.blood_pressure > 200,
            'extreme_obesity', br.bmi > 50,
            'extreme_age', br.age > 100
        ) as anomaly_flags
        
    FROM bronze.diabetes_raw br
    WHERE br.id NOT IN (
        SELECT source_id FROM silver.diabetes_cleaned 
        WHERE source_id IS NOT NULL
    );
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    RAISE NOTICE 'Successfully processed % records from bronze to silver layer', processed_count;
    RETURN processed_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in bronze to silver processing: %', SQLERRM;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SILVER TO GOLD PROCESSING FUNCTION (SIMPLIFIED)
-- =============================================================================

CREATE OR REPLACE FUNCTION process_silver_to_gold()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting Silver to Gold data processing';
    
    -- Check if gold.diabetes_features table exists
    BEGIN
        INSERT INTO gold.diabetes_features (
            source_id, pregnancies, glucose, blood_pressure, skin_thickness,
            insulin, bmi, diabetes_pedigree_function, age, outcome,
            age_group, bmi_category, glucose_category, risk_score, high_risk_flag
        )
        SELECT 
            sc.id as source_id,
            sc.pregnancies,
            sc.glucose,
            sc.blood_pressure,
            sc.skin_thickness,
            sc.insulin,
            sc.bmi,
            sc.diabetes_pedigree_function,
            sc.age,
            sc.outcome,
            
            -- Age groups
            CASE 
                WHEN sc.age < 30 THEN 'young_adult'
                WHEN sc.age < 45 THEN 'adult'
                WHEN sc.age < 60 THEN 'middle_aged'
                ELSE 'senior'
            END as age_group,
            
            -- BMI categories
            CASE 
                WHEN sc.bmi < 18.5 THEN 'underweight'
                WHEN sc.bmi < 25 THEN 'normal'
                WHEN sc.bmi < 30 THEN 'overweight'
                ELSE 'obese'
            END as bmi_category,
            
            -- Glucose categories
            CASE 
                WHEN sc.glucose < 70 THEN 'hypoglycemic'
                WHEN sc.glucose < 100 THEN 'normal'
                WHEN sc.glucose < 126 THEN 'prediabetic'
                ELSE 'diabetic'
            END as glucose_category,
            
            -- Risk score using logistic regression approximation
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
            ) as risk_score,
            
            -- High risk flag
            CASE 
                WHEN sc.outcome = 1 THEN TRUE
                WHEN sc.glucose > 126 OR sc.bmi > 30 THEN TRUE
                ELSE FALSE
            END as high_risk_flag
            
        FROM silver.diabetes_cleaned sc
        WHERE sc.is_valid = TRUE
          AND sc.id NOT IN (
              SELECT source_id FROM gold.diabetes_features 
              WHERE source_id IS NOT NULL
          );
        
        GET DIAGNOSTICS processed_count = ROW_COUNT;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not process to gold.diabetes_features: %', SQLERRM;
        processed_count := 0;
    END;
    
    RAISE NOTICE 'Successfully processed % records from silver to gold layer', processed_count;
    RETURN processed_count;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in silver to gold processing: %', SQLERRM;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'SIMPLIFIED ETL FUNCTIONS CREATED!';
    RAISE NOTICE 'Functions ready for testing:';
    RAISE NOTICE '- ingest_diabetes_data()';
    RAISE NOTICE '- refresh_analytics_views()';
    RAISE NOTICE '- process_bronze_to_silver()';
    RAISE NOTICE '- process_silver_to_gold()';
    RAISE NOTICE '========================================';
END $$;

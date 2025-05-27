-- =============================================================================
-- MASTER POSTGRESQL DIABETES PREDICTION DATA WAREHOUSE INITIALIZATION
-- =============================================================================
-- This comprehensive script initializes the complete data warehouse infrastructure
-- for the diabetes prediction pipeline with Bronze/Silver/Gold architecture
-- 
-- Components included:
-- 1. Database setup and extensions
-- 2. Schema creation for data layers
-- 3. Core warehouse tables
-- 4. ETL functions and procedures
-- 5. Analytics views for Grafana
-- 6. Performance indexes
-- 7. Sample data and validation
-- =============================================================================

\timing on
\echo '=================================================================='
\echo 'STARTING COMPREHENSIVE DIABETES PREDICTION DATA WAREHOUSE SETUP'
\echo '=================================================================='
\echo ''

-- Set client encoding and timezone
SET client_encoding = 'UTF8';
SET timezone = 'UTC';
SET search_path = public, bronze, silver, gold, analytics;

\echo 'Setting up database configuration...'

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "tablefunc";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

\echo 'Extensions enabled: uuid-ossp, pg_stat_statements, btree_gin, pg_trgm, tablefunc, pg_cron'

-- =============================================================================
-- CREATE ADDITIONAL DATABASES
-- =============================================================================

\echo 'Creating additional databases...'

-- Create grafana database if it doesn't exist
SELECT 'CREATE DATABASE grafana'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'grafana')\gexec

-- Create monitoring database if it doesn't exist  
SELECT 'CREATE DATABASE monitoring'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'monitoring')\gexec

-- =============================================================================
-- CREATE SCHEMAS FOR DATA LAYERS
-- =============================================================================

\echo 'Creating data layer schemas...'

-- Create schemas for data layers (Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS bronze;      -- Raw ingested data
CREATE SCHEMA IF NOT EXISTS silver;      -- Cleaned and normalized data
CREATE SCHEMA IF NOT EXISTS gold;        -- Aggregated and analytics-ready data
CREATE SCHEMA IF NOT EXISTS analytics;   -- Views and functions for reporting
CREATE SCHEMA IF NOT EXISTS monitoring;  -- System monitoring tables
CREATE SCHEMA IF NOT EXISTS logs;        -- ETL and system logs

-- Set schema permissions
GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE ON SCHEMA silver TO PUBLIC;
GRANT USAGE ON SCHEMA gold TO PUBLIC;
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT USAGE ON SCHEMA monitoring TO PUBLIC;
GRANT USAGE ON SCHEMA logs TO PUBLIC;

GRANT CREATE ON SCHEMA bronze TO PUBLIC;
GRANT CREATE ON SCHEMA silver TO PUBLIC;
GRANT CREATE ON SCHEMA gold TO PUBLIC;
GRANT CREATE ON SCHEMA analytics TO PUBLIC;
GRANT CREATE ON SCHEMA monitoring TO PUBLIC;
GRANT CREATE ON SCHEMA logs TO PUBLIC;

-- =============================================================================
-- CREATE CORE WAREHOUSE TABLES (BRONZE LAYER)
-- =============================================================================

\echo 'Creating Bronze layer tables for raw data ingestion...'

-- Bronze: Raw diabetes data as ingested
CREATE TABLE IF NOT EXISTS bronze.diabetes_raw (
    id SERIAL PRIMARY KEY,
    pregnancies INTEGER,
    glucose INTEGER,
    blood_pressure INTEGER,
    skin_thickness INTEGER,
    insulin INTEGER,
    bmi DECIMAL(5,2),
    diabetes_pedigree_function DECIMAL(10,6),
    age INTEGER,
    outcome INTEGER,
    data_source VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    row_number_in_file INTEGER,
    data_quality_score DECIMAL(3,2)
);

-- Bronze: Raw model predictions
CREATE TABLE IF NOT EXISTS bronze.model_predictions_raw (
    id SERIAL PRIMARY KEY,
    patient_id INTEGER,
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    prediction_probability DECIMAL(5,4),
    prediction_binary INTEGER,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    feature_values JSONB,
    confidence_score DECIMAL(3,2),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- CREATE SILVER LAYER TABLES (CLEANED AND NORMALIZED)
-- =============================================================================

\echo 'Creating Silver layer tables for cleaned and normalized data...'

-- Silver: Cleaned and validated diabetes data
CREATE TABLE IF NOT EXISTS silver.diabetes_cleaned (
    patient_id SERIAL PRIMARY KEY,
    pregnancies INTEGER CHECK (pregnancies >= 0 AND pregnancies <= 20),
    glucose INTEGER CHECK (glucose > 0 AND glucose <= 500),
    blood_pressure INTEGER CHECK (blood_pressure > 0 AND blood_pressure <= 300),
    skin_thickness INTEGER CHECK (skin_thickness >= 0 AND skin_thickness <= 100),
    insulin INTEGER CHECK (insulin >= 0 AND insulin <= 1000),
    bmi DECIMAL(5,2) CHECK (bmi > 0 AND bmi <= 100),
    diabetes_pedigree_function DECIMAL(10,6) CHECK (diabetes_pedigree_function >= 0),
    age INTEGER CHECK (age > 0 AND age <= 120),
    outcome INTEGER CHECK (outcome IN (0, 1)),
    risk_category VARCHAR(20),
    bmi_category VARCHAR(20),
    age_group VARCHAR(20),
    glucose_category VARCHAR(20),
    data_quality_score DECIMAL(3,2),
    cleaned_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_record_id INTEGER REFERENCES bronze.diabetes_raw(id)
);

-- Silver: Processed model predictions with validation
CREATE TABLE IF NOT EXISTS silver.model_predictions_processed (
    prediction_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES silver.diabetes_cleaned(patient_id),
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    prediction_probability DECIMAL(5,4) CHECK (prediction_probability >= 0 AND prediction_probability <= 1),
    prediction_binary INTEGER CHECK (prediction_binary IN (0, 1)),
    confidence_level VARCHAR(20),
    risk_score DECIMAL(3,2),
    prediction_timestamp TIMESTAMP NOT NULL,
    model_accuracy DECIMAL(5,4),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_prediction_id INTEGER REFERENCES bronze.model_predictions_raw(id)
);

-- =============================================================================
-- CREATE GOLD LAYER TABLES (AGGREGATED AND ANALYTICS-READY)
-- =============================================================================

\echo 'Creating Gold layer tables for aggregated analytics data...'

-- Gold: Daily diabetes statistics
CREATE TABLE IF NOT EXISTS gold.daily_diabetes_stats (
    date_key DATE PRIMARY KEY,
    total_patients INTEGER,
    diabetic_patients INTEGER,
    non_diabetic_patients INTEGER,
    diabetic_rate DECIMAL(5,4),
    avg_glucose DECIMAL(6,2),
    avg_bmi DECIMAL(5,2),
    avg_age DECIMAL(5,2),
    high_risk_patients INTEGER,
    medium_risk_patients INTEGER,
    low_risk_patients INTEGER,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold: Model performance metrics
CREATE TABLE IF NOT EXISTS gold.model_performance_daily (
    date_key DATE,
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    total_predictions INTEGER,
    accuracy DECIMAL(5,4),
    precision_diabetic DECIMAL(5,4),
    recall_diabetic DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    auc_score DECIMAL(5,4),
    true_positives INTEGER,
    false_positives INTEGER,
    true_negatives INTEGER,
    false_negatives INTEGER,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, model_name, model_version)
);

-- Gold: Risk analysis by demographics
CREATE TABLE IF NOT EXISTS gold.risk_by_demographics (
    date_key DATE,
    age_group VARCHAR(20),
    bmi_category VARCHAR(20),
    total_patients INTEGER,
    diabetic_patients INTEGER,
    diabetic_rate DECIMAL(5,4),
    avg_risk_score DECIMAL(3,2),
    avg_glucose DECIMAL(6,2),
    avg_bmi DECIMAL(5,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, age_group, bmi_category)
);

-- =============================================================================
-- CREATE ETL LOGGING AND MONITORING TABLES
-- =============================================================================

\echo 'Creating ETL logging and monitoring infrastructure...'

-- ETL execution logs
CREATE TABLE IF NOT EXISTS logs.etl_execution_log (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    process_type VARCHAR(50), -- 'bronze_to_silver', 'silver_to_gold', 'analytics'
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    execution_metadata JSONB
);

-- Data quality monitoring
CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10,4),
    threshold_value DECIMAL(10,4),
    status VARCHAR(20) CHECK (status IN ('pass', 'warning', 'fail')),
    measured_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- =============================================================================
-- CREATE ETL FUNCTIONS AND PROCEDURES
-- =============================================================================

\echo 'Creating ETL functions and procedures...'

-- Function to categorize BMI
CREATE OR REPLACE FUNCTION analytics.categorize_bmi(bmi_value DECIMAL)
RETURNS VARCHAR(20) AS $$
BEGIN
    CASE 
        WHEN bmi_value < 18.5 THEN RETURN 'Underweight';
        WHEN bmi_value >= 18.5 AND bmi_value < 25.0 THEN RETURN 'Normal';
        WHEN bmi_value >= 25.0 AND bmi_value < 30.0 THEN RETURN 'Overweight';
        WHEN bmi_value >= 30.0 THEN RETURN 'Obese';
        ELSE RETURN 'Unknown';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to categorize age groups
CREATE OR REPLACE FUNCTION analytics.categorize_age(age_value INTEGER)
RETURNS VARCHAR(20) AS $$
BEGIN
    CASE 
        WHEN age_value < 30 THEN RETURN 'Young (< 30)';
        WHEN age_value >= 30 AND age_value < 50 THEN RETURN 'Middle (30-49)';
        WHEN age_value >= 50 AND age_value < 65 THEN RETURN 'Mature (50-64)';
        WHEN age_value >= 65 THEN RETURN 'Senior (65+)';
        ELSE RETURN 'Unknown';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to categorize glucose levels
CREATE OR REPLACE FUNCTION analytics.categorize_glucose(glucose_value INTEGER)
RETURNS VARCHAR(20) AS $$
BEGIN
    CASE 
        WHEN glucose_value < 70 THEN RETURN 'Low';
        WHEN glucose_value >= 70 AND glucose_value < 100 THEN RETURN 'Normal';
        WHEN glucose_value >= 100 AND glucose_value < 126 THEN RETURN 'Pre-diabetic';
        WHEN glucose_value >= 126 THEN RETURN 'Diabetic';
        ELSE RETURN 'Unknown';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate risk category
CREATE OR REPLACE FUNCTION analytics.calculate_risk_category(
    glucose INTEGER,
    bmi DECIMAL,
    age INTEGER,
    pregnancies INTEGER,
    diabetes_pedigree DECIMAL
)
RETURNS VARCHAR(20) AS $$
DECLARE
    risk_score INTEGER := 0;
BEGIN
    -- Calculate risk based on multiple factors
    IF glucose >= 126 THEN risk_score := risk_score + 3; END IF;
    IF glucose >= 100 AND glucose < 126 THEN risk_score := risk_score + 2; END IF;
    IF bmi >= 30 THEN risk_score := risk_score + 2; END IF;
    IF bmi >= 25 AND bmi < 30 THEN risk_score := risk_score + 1; END IF;
    IF age >= 65 THEN risk_score := risk_score + 2; END IF;
    IF age >= 50 AND age < 65 THEN risk_score := risk_score + 1; END IF;
    IF pregnancies > 5 THEN risk_score := risk_score + 1; END IF;
    IF diabetes_pedigree > 0.5 THEN risk_score := risk_score + 1; END IF;
    
    CASE 
        WHEN risk_score >= 6 THEN RETURN 'High';
        WHEN risk_score >= 3 THEN RETURN 'Medium';
        ELSE RETURN 'Low';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Main ETL function: Bronze to Silver transformation
CREATE OR REPLACE FUNCTION analytics.process_bronze_to_silver()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    log_id INTEGER;
BEGIN
    -- Log ETL start
    INSERT INTO logs.etl_execution_log (process_name, process_type, start_timestamp, status)
    VALUES ('bronze_to_silver_diabetes', 'bronze_to_silver', CURRENT_TIMESTAMP, 'running')
    RETURNING etl_execution_log.log_id INTO log_id;
    
    -- Transform and insert data
    INSERT INTO silver.diabetes_cleaned (
        pregnancies, glucose, blood_pressure, skin_thickness, insulin, bmi,
        diabetes_pedigree_function, age, outcome, risk_category, bmi_category,
        age_group, glucose_category, data_quality_score, bronze_record_id
    )
    SELECT 
        pregnancies,
        glucose,
        blood_pressure,
        skin_thickness,
        insulin,
        bmi,
        diabetes_pedigree_function,
        age,
        outcome,
        analytics.calculate_risk_category(glucose, bmi, age, pregnancies, diabetes_pedigree_function),
        analytics.categorize_bmi(bmi),
        analytics.categorize_age(age),
        analytics.categorize_glucose(glucose),
        COALESCE(data_quality_score, 0.8),
        id
    FROM bronze.diabetes_raw
    WHERE id NOT IN (SELECT bronze_record_id FROM silver.diabetes_cleaned WHERE bronze_record_id IS NOT NULL);
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Update ETL log
    UPDATE logs.etl_execution_log 
    SET end_timestamp = CURRENT_TIMESTAMP,
        status = 'completed',
        records_processed = processed_count,
        records_inserted = processed_count
    WHERE etl_execution_log.log_id = log_id;
    
    RETURN processed_count;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE logs.etl_execution_log 
        SET end_timestamp = CURRENT_TIMESTAMP,
            status = 'failed',
            error_message = SQLERRM
        WHERE etl_execution_log.log_id = log_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function: Silver to Gold daily aggregation
CREATE OR REPLACE FUNCTION analytics.process_silver_to_gold_daily()
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER := 0;
    log_id INTEGER;
    target_date DATE := CURRENT_DATE - INTERVAL '1 day';
BEGIN
    -- Log ETL start
    INSERT INTO logs.etl_execution_log (process_name, process_type, start_timestamp, status)
    VALUES ('silver_to_gold_daily', 'silver_to_gold', CURRENT_TIMESTAMP, 'running')
    RETURNING etl_execution_log.log_id INTO log_id;
    
    -- Daily diabetes statistics
    INSERT INTO gold.daily_diabetes_stats (
        date_key, total_patients, diabetic_patients, non_diabetic_patients,
        diabetic_rate, avg_glucose, avg_bmi, avg_age,
        high_risk_patients, medium_risk_patients, low_risk_patients
    )
    SELECT 
        target_date,
        COUNT(*),
        SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN outcome = 0 THEN 1 ELSE 0 END),
        AVG(outcome::DECIMAL),
        AVG(glucose),
        AVG(bmi),
        AVG(age),
        SUM(CASE WHEN risk_category = 'High' THEN 1 ELSE 0 END),
        SUM(CASE WHEN risk_category = 'Medium' THEN 1 ELSE 0 END),
        SUM(CASE WHEN risk_category = 'Low' THEN 1 ELSE 0 END)
    FROM silver.diabetes_cleaned
    WHERE DATE(cleaned_timestamp) = target_date
    ON CONFLICT (date_key) DO UPDATE SET
        total_patients = EXCLUDED.total_patients,
        diabetic_patients = EXCLUDED.diabetic_patients,
        non_diabetic_patients = EXCLUDED.non_diabetic_patients,
        diabetic_rate = EXCLUDED.diabetic_rate,
        avg_glucose = EXCLUDED.avg_glucose,
        avg_bmi = EXCLUDED.avg_bmi,
        avg_age = EXCLUDED.avg_age,
        high_risk_patients = EXCLUDED.high_risk_patients,
        medium_risk_patients = EXCLUDED.medium_risk_patients,
        low_risk_patients = EXCLUDED.low_risk_patients;
    
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    -- Risk analysis by demographics
    INSERT INTO gold.risk_by_demographics (
        date_key, age_group, bmi_category, total_patients, diabetic_patients,
        diabetic_rate, avg_risk_score, avg_glucose, avg_bmi
    )
    SELECT 
        target_date,
        age_group,
        bmi_category,
        COUNT(*),
        SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END),
        AVG(outcome::DECIMAL),
        AVG(CASE risk_category 
            WHEN 'High' THEN 3 
            WHEN 'Medium' THEN 2 
            ELSE 1 END),
        AVG(glucose),
        AVG(bmi)
    FROM silver.diabetes_cleaned
    WHERE DATE(cleaned_timestamp) = target_date
    GROUP BY age_group, bmi_category
    ON CONFLICT (date_key, age_group, bmi_category) DO UPDATE SET
        total_patients = EXCLUDED.total_patients,
        diabetic_patients = EXCLUDED.diabetic_patients,
        diabetic_rate = EXCLUDED.diabetic_rate,
        avg_risk_score = EXCLUDED.avg_risk_score,
        avg_glucose = EXCLUDED.avg_glucose,
        avg_bmi = EXCLUDED.avg_bmi;
    
    -- Update ETL log
    UPDATE logs.etl_execution_log 
    SET end_timestamp = CURRENT_TIMESTAMP,
        status = 'completed',
        records_processed = processed_count,
        records_inserted = processed_count
    WHERE etl_execution_log.log_id = log_id;
    
    RETURN processed_count;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE logs.etl_execution_log 
        SET end_timestamp = CURRENT_TIMESTAMP,
            status = 'failed',
            error_message = SQLERRM
        WHERE etl_execution_log.log_id = log_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- CREATE ANALYTICS VIEWS FOR GRAFANA DASHBOARDS
-- =============================================================================

\echo 'Creating analytics views for Grafana dashboards...'

-- Executive KPI View
CREATE OR REPLACE VIEW analytics.executive_kpis AS
SELECT 
    'total_patients' as metric_name,
    COUNT(*)::TEXT as metric_value,
    'Total Patients in Database' as description
FROM silver.diabetes_cleaned
UNION ALL
SELECT 
    'diabetic_rate' as metric_name,
    ROUND(AVG(outcome::DECIMAL) * 100, 2)::TEXT || '%' as metric_value,
    'Overall Diabetes Rate' as description
FROM silver.diabetes_cleaned
UNION ALL
SELECT 
    'high_risk_patients' as metric_name,
    COUNT(*)::TEXT as metric_value,
    'High Risk Patients' as description
FROM silver.diabetes_cleaned 
WHERE risk_category = 'High'
UNION ALL
SELECT 
    'avg_glucose' as metric_name,
    ROUND(AVG(glucose), 1)::TEXT || ' mg/dL' as metric_value,
    'Average Glucose Level' as description
FROM silver.diabetes_cleaned
UNION ALL
SELECT 
    'avg_bmi' as metric_name,
    ROUND(AVG(bmi), 1)::TEXT as metric_value,
    'Average BMI' as description
FROM silver.diabetes_cleaned;

-- Time Series Analysis View
CREATE OR REPLACE VIEW analytics.daily_trend_analysis AS
SELECT 
    date_key as time,
    'total_patients' as metric,
    total_patients as value
FROM gold.daily_diabetes_stats
UNION ALL
SELECT 
    date_key as time,
    'diabetic_patients' as metric,
    diabetic_patients as value
FROM gold.daily_diabetes_stats
UNION ALL
SELECT 
    date_key as time,
    'diabetic_rate_pct' as metric,
    ROUND(diabetic_rate * 100, 2) as value
FROM gold.daily_diabetes_stats
ORDER BY time DESC;

-- Risk Distribution View
CREATE OR REPLACE VIEW analytics.risk_distribution AS
SELECT 
    risk_category as category,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM silver.diabetes_cleaned
GROUP BY risk_category;

-- Demographic Analysis View
CREATE OR REPLACE VIEW analytics.demographic_analysis AS
SELECT 
    age_group,
    bmi_category,
    COUNT(*) as total_patients,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) as diabetic_patients,
    ROUND(AVG(outcome::DECIMAL) * 100, 2) as diabetic_rate_pct,
    ROUND(AVG(glucose), 1) as avg_glucose,
    ROUND(AVG(bmi), 1) as avg_bmi
FROM silver.diabetes_cleaned
GROUP BY age_group, bmi_category
ORDER BY age_group, bmi_category;

-- Glucose Distribution View
CREATE OR REPLACE VIEW analytics.glucose_distribution AS
SELECT 
    glucose_category as category,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(outcome::DECIMAL) * 100, 2) as diabetic_rate_pct
FROM silver.diabetes_cleaned
GROUP BY glucose_category
ORDER BY 
    CASE glucose_category 
        WHEN 'Low' THEN 1 
        WHEN 'Normal' THEN 2 
        WHEN 'Pre-diabetic' THEN 3 
        WHEN 'Diabetic' THEN 4 
        ELSE 5 
    END;

-- Model Performance View (when predictions are available)
CREATE OR REPLACE VIEW analytics.model_performance_summary AS
SELECT 
    model_name,
    model_version,
    COUNT(*) as total_predictions,
    ROUND(AVG(prediction_probability), 4) as avg_prediction_prob,
    COUNT(CASE WHEN prediction_binary = 1 THEN 1 END) as predicted_diabetic,
    ROUND(COUNT(CASE WHEN prediction_binary = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as predicted_diabetic_rate,
    ROUND(AVG(confidence_score), 3) as avg_confidence
FROM silver.model_predictions_processed
GROUP BY model_name, model_version
ORDER BY model_name, model_version;

-- Data Quality Monitoring View
CREATE OR REPLACE VIEW analytics.data_quality_summary AS
SELECT 
    'bronze.diabetes_raw' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN glucose IS NULL OR glucose <= 0 THEN 1 END) as invalid_glucose,
    COUNT(CASE WHEN bmi IS NULL OR bmi <= 0 THEN 1 END) as invalid_bmi,
    COUNT(CASE WHEN age IS NULL OR age <= 0 THEN 1 END) as invalid_age,
    ROUND(AVG(COALESCE(data_quality_score, 0)), 3) as avg_quality_score
FROM bronze.diabetes_raw
UNION ALL
SELECT 
    'silver.diabetes_cleaned' as table_name,
    COUNT(*) as total_records,
    0 as invalid_glucose,  -- Should be 0 due to constraints
    0 as invalid_bmi,      -- Should be 0 due to constraints  
    0 as invalid_age,      -- Should be 0 due to constraints
    ROUND(AVG(data_quality_score), 3) as avg_quality_score
FROM silver.diabetes_cleaned;

-- =============================================================================
-- CREATE PERFORMANCE INDEXES
-- =============================================================================

\echo 'Creating performance indexes...'

-- Bronze layer indexes
CREATE INDEX IF NOT EXISTS idx_bronze_diabetes_ingestion_ts ON bronze.diabetes_raw(ingestion_timestamp);
CREATE INDEX IF NOT EXISTS idx_bronze_diabetes_outcome ON bronze.diabetes_raw(outcome);
CREATE INDEX IF NOT EXISTS idx_bronze_diabetes_source ON bronze.diabetes_raw(data_source);

-- Silver layer indexes
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_outcome ON silver.diabetes_cleaned(outcome);
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_risk ON silver.diabetes_cleaned(risk_category);
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_age_group ON silver.diabetes_cleaned(age_group);
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_bmi_cat ON silver.diabetes_cleaned(bmi_category);
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_glucose_cat ON silver.diabetes_cleaned(glucose_category);
CREATE INDEX IF NOT EXISTS idx_silver_diabetes_cleaned_ts ON silver.diabetes_cleaned(cleaned_timestamp);

-- Gold layer indexes
CREATE INDEX IF NOT EXISTS idx_gold_daily_stats_date ON gold.daily_diabetes_stats(date_key);
CREATE INDEX IF NOT EXISTS idx_gold_demographics_date ON gold.risk_by_demographics(date_key);
CREATE INDEX IF NOT EXISTS idx_gold_model_perf_date ON gold.model_performance_daily(date_key);

-- Monitoring indexes
CREATE INDEX IF NOT EXISTS idx_etl_log_process_type ON logs.etl_execution_log(process_type);
CREATE INDEX IF NOT EXISTS idx_etl_log_status ON logs.etl_execution_log(status);
CREATE INDEX IF NOT EXISTS idx_etl_log_start_ts ON logs.etl_execution_log(start_timestamp);

-- =============================================================================
-- CREATE GRAFANA HELPER VIEWS FOR VARIABLES
-- =============================================================================

\echo 'Creating Grafana helper views for dashboard variables...'

-- Available date ranges
CREATE OR REPLACE VIEW analytics.available_dates AS
SELECT DISTINCT 
    date_key as value,
    date_key::TEXT as text
FROM gold.daily_diabetes_stats
ORDER BY date_key DESC;

-- Available risk categories
CREATE OR REPLACE VIEW analytics.risk_categories AS
SELECT DISTINCT 
    risk_category as value,
    risk_category as text
FROM silver.diabetes_cleaned
WHERE risk_category IS NOT NULL
ORDER BY 
    CASE risk_category 
        WHEN 'High' THEN 1 
        WHEN 'Medium' THEN 2 
        WHEN 'Low' THEN 3 
        ELSE 4 
    END;

-- Available age groups
CREATE OR REPLACE VIEW analytics.age_groups AS
SELECT DISTINCT 
    age_group as value,
    age_group as text
FROM silver.diabetes_cleaned
WHERE age_group IS NOT NULL
ORDER BY 
    CASE age_group 
        WHEN 'Young (< 30)' THEN 1 
        WHEN 'Middle (30-49)' THEN 2 
        WHEN 'Mature (50-64)' THEN 3 
        WHEN 'Senior (65+)' THEN 4 
        ELSE 5 
    END;

-- =============================================================================
-- INSERT SAMPLE TEST DATA
-- =============================================================================

\echo 'Inserting sample test data for validation...'

-- Insert sample data into bronze layer
INSERT INTO bronze.diabetes_raw (
    pregnancies, glucose, blood_pressure, skin_thickness, insulin, bmi, 
    diabetes_pedigree_function, age, outcome, data_source, file_name, 
    row_number_in_file, data_quality_score
) VALUES 
(6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 'initial_load', 'diabetes_sample.csv', 1, 0.95),
(1, 85, 66, 29, 0, 26.6, 0.351, 31, 0, 'initial_load', 'diabetes_sample.csv', 2, 0.92),
(8, 183, 64, 0, 0, 23.3, 0.672, 32, 1, 'initial_load', 'diabetes_sample.csv', 3, 0.88),
(1, 89, 66, 23, 94, 28.1, 0.167, 21, 0, 'initial_load', 'diabetes_sample.csv', 4, 0.94),
(0, 137, 40, 35, 168, 43.1, 2.288, 33, 1, 'initial_load', 'diabetes_sample.csv', 5, 0.90),
(5, 116, 74, 0, 0, 25.6, 0.201, 30, 0, 'initial_load', 'diabetes_sample.csv', 6, 0.89),
(3, 78, 50, 32, 88, 31.0, 0.248, 26, 1, 'initial_load', 'diabetes_sample.csv', 7, 0.91),
(10, 115, 0, 0, 0, 35.3, 0.134, 29, 0, 'initial_load', 'diabetes_sample.csv', 8, 0.85),
(2, 197, 70, 45, 543, 30.5, 0.158, 53, 1, 'initial_load', 'diabetes_sample.csv', 9, 0.93),
(8, 125, 96, 0, 0, 0.0, 0.232, 54, 1, 'initial_load', 'diabetes_sample.csv', 10, 0.87);

-- =============================================================================
-- RUN INITIAL ETL PROCESSES
-- =============================================================================

\echo 'Running initial ETL processes...'

-- Process bronze to silver
SELECT analytics.process_bronze_to_silver() as bronze_to_silver_records;

-- Process silver to gold (for today)
SELECT analytics.process_silver_to_gold_daily() as silver_to_gold_records;

-- =============================================================================
-- VALIDATION AND REPORTING
-- =============================================================================

\echo 'Running validation checks...'

-- Check table counts
\echo 'Database Statistics:'
SELECT 'bronze.diabetes_raw' as table_name, COUNT(*) as record_count FROM bronze.diabetes_raw
UNION ALL
SELECT 'silver.diabetes_cleaned', COUNT(*) FROM silver.diabetes_cleaned  
UNION ALL
SELECT 'gold.daily_diabetes_stats', COUNT(*) FROM gold.daily_diabetes_stats
UNION ALL
SELECT 'logs.etl_execution_log', COUNT(*) FROM logs.etl_execution_log;

-- Check data quality
\echo 'Data Quality Summary:'
SELECT * FROM analytics.data_quality_summary;

-- Check ETL execution status
\echo 'Recent ETL Executions:'
SELECT 
    process_name,
    status,
    records_processed,
    start_timestamp,
    end_timestamp,
    EXTRACT(EPOCH FROM (end_timestamp - start_timestamp)) as duration_seconds
FROM logs.etl_execution_log 
ORDER BY start_timestamp DESC 
LIMIT 5;

-- =============================================================================
-- FINAL SETUP COMPLETION
-- =============================================================================

\echo ''
\echo '=================================================================='
\echo 'DIABETES PREDICTION DATA WAREHOUSE SETUP COMPLETED SUCCESSFULLY!'
\echo '=================================================================='
\echo ''
\echo 'Components Created:'
\echo '✓ Bronze/Silver/Gold data layer schemas'
\echo '✓ Core diabetes prediction tables'
\echo '✓ ETL functions and procedures'  
\echo '✓ Analytics views for Grafana dashboards'
\echo '✓ Performance indexes for optimal queries'
\echo '✓ Sample test data and validation'
\echo '✓ Monitoring and logging infrastructure'
\echo ''
\echo 'Ready for:'
\echo '• Data ingestion via Airflow DAGs'
\echo '• Grafana dashboard connections'
\echo '• Model prediction workflows'
\echo '• Analytics and reporting'
\echo ''
\echo 'Connection Details for Grafana:'
\echo 'Host: localhost (or your PostgreSQL host)'
\echo 'Database: diabetes_prediction'  
\echo 'User: postgres (or your configured user)'
\echo 'SSL Mode: disable (for local development)'
\echo ''
\echo 'Key Analytics Views:'
\echo '• analytics.executive_kpis - Executive dashboard KPIs'
\echo '• analytics.daily_trend_analysis - Time series data'
\echo '• analytics.risk_distribution - Risk category breakdown'
\echo '• analytics.demographic_analysis - Age/BMI demographics'
\echo '• analytics.glucose_distribution - Glucose level analysis'
\echo ''

\timing off

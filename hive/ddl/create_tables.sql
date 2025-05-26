-- Hive DDL Scripts for Diabetes Prediction Pipeline
-- Kelompok 8 RA - Big Data Project
-- 
-- This file contains Data Definition Language (DDL) statements for creating
-- Hive tables for the diabetes prediction pipeline data layers.

-- =====================================================
-- BRONZE LAYER TABLES
-- =====================================================

-- Bronze layer: Raw diabetes clinical data
CREATE EXTERNAL TABLE IF NOT EXISTS diabetes_bronze (
    Pregnancies INT,
    Glucose DOUBLE,
    BloodPressure DOUBLE,
    SkinThickness DOUBLE,
    Insulin DOUBLE,
    BMI DOUBLE,
    DiabetesPedigreeFunction DOUBLE,
    Age INT,
    Outcome INT,
    ingestion_timestamp TIMESTAMP,
    data_source STRING,
    ingestion_version STRING
)
STORED AS TEXTFILE
LOCATION '/data/bronze/diabetes/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Raw diabetes clinical data from Pima Indians dataset',
    'data_classification' = 'bronze',
    'skip.header.line.count' = '1'
);

-- Bronze layer: Raw personal health data
CREATE EXTERNAL TABLE IF NOT EXISTS personal_health_bronze (
    Person_ID STRING,
    Age INT,
    Gender STRING,
    Height DOUBLE,
    Weight DOUBLE,
    Heart_Rate DOUBLE,
    Blood_Pressure_Systolic DOUBLE,
    Blood_Pressure_Diastolic DOUBLE,
    Sleep_Duration DOUBLE,
    Deep_Sleep_Duration DOUBLE,
    REM_Sleep_Duration DOUBLE,
    Steps_Daily INT,
    Exercise_Minutes DOUBLE,
    Stress_Level STRING,
    Mood STRING,
    Health_Score DOUBLE,
    Medical_Conditions STRING,
    Timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    data_source STRING,
    ingestion_version STRING
)
STORED AS TEXTFILE
LOCATION '/data/bronze/personal_health/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Raw personal health and wearable device data',
    'data_classification' = 'bronze',
    'skip.header.line.count' = '1'
);

-- =====================================================
-- SILVER LAYER TABLES
-- =====================================================

-- Silver layer: Cleaned diabetes data
CREATE EXTERNAL TABLE IF NOT EXISTS diabetes_silver (
    Pregnancies INT,
    Glucose DOUBLE,
    BloodPressure DOUBLE,
    SkinThickness DOUBLE,
    Insulin DOUBLE,
    BMI DOUBLE,
    DiabetesPedigreeFunction DOUBLE,
    Age INT,
    diabetes INT,
    is_valid_glucose BOOLEAN,
    is_valid_bmi BOOLEAN,
    is_valid_age BOOLEAN,
    data_quality_score DOUBLE,
    processed_timestamp TIMESTAMP,
    data_source STRING,
    processing_version STRING
)
STORED AS PARQUET
LOCATION '/data/silver/diabetes/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Cleaned and validated diabetes clinical data',
    'data_classification' = 'silver',
    'compression' = 'snappy'
);

-- Silver layer: Cleaned personal health data
CREATE EXTERNAL TABLE IF NOT EXISTS personal_health_silver (
    Person_ID STRING,
    Age INT,
    Gender STRING,
    Height DOUBLE,
    Weight DOUBLE,
    Heart_Rate DOUBLE,
    Blood_Pressure_Systolic DOUBLE,
    Blood_Pressure_Diastolic DOUBLE,
    Sleep_Duration DOUBLE,
    Deep_Sleep_Duration DOUBLE,
    REM_Sleep_Duration DOUBLE,
    Steps_Daily INT,
    Exercise_Minutes DOUBLE,
    Stress_Level STRING,
    Mood STRING,
    Health_Score DOUBLE,
    Medical_Conditions STRING,
    BMI_Calculated DOUBLE,
    Sleep_Efficiency DOUBLE,
    Health_Risk_Category STRING,
    Timestamp_Parsed TIMESTAMP,
    processed_timestamp TIMESTAMP,
    data_source STRING,
    processing_version STRING
)
STORED AS PARQUET
LOCATION '/data/silver/personal_health/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Cleaned and standardized personal health data',
    'data_classification' = 'silver',
    'compression' = 'snappy'
);

-- =====================================================
-- GOLD LAYER TABLES
-- =====================================================

-- Gold layer: Feature-engineered diabetes data for ML
CREATE EXTERNAL TABLE IF NOT EXISTS diabetes_gold (
    Pregnancies INT,
    Glucose DOUBLE,
    BloodPressure DOUBLE,
    SkinThickness DOUBLE,
    Insulin DOUBLE,
    BMI DOUBLE,
    DiabetesPedigreeFunction DOUBLE,
    Age INT,
    diabetes INT,
    bmi_category STRING,
    age_group STRING,
    glucose_category STRING,
    bp_category STRING,
    risk_score DOUBLE,
    glucose_bmi_interaction DOUBLE,
    age_glucose_interaction DOUBLE,
    glucose_normalized DOUBLE,
    bmi_normalized DOUBLE,
    age_normalized DOUBLE,
    health_risk_level STRING,
    intervention_priority STRING,
    estimated_cost_category STRING,
    processed_timestamp TIMESTAMP,
    data_source STRING,
    processing_version STRING
)
PARTITIONED BY (health_risk_level STRING)
STORED AS PARQUET
LOCATION '/data/gold/diabetes/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Feature-engineered diabetes data ready for ML training',
    'data_classification' = 'gold',
    'compression' = 'snappy'
);

-- Gold layer: Aggregated health insights
CREATE EXTERNAL TABLE IF NOT EXISTS health_insights_gold (
    Age_Group STRING,
    Gender STRING,
    Health_Risk_Category STRING,
    Total_Patients INT,
    Avg_BMI DOUBLE,
    Avg_Health_Score DOUBLE,
    Avg_Sleep_Efficiency DOUBLE,
    Avg_Steps_Daily DOUBLE,
    Avg_Exercise_Minutes DOUBLE,
    High_Risk_Count INT,
    High_Risk_Percentage DOUBLE,
    Common_Medical_Condition STRING,
    Avg_Heart_Rate DOUBLE,
    Stress_Level_Distribution MAP<STRING, INT>,
    Mood_Distribution MAP<STRING, INT>,
    Report_Date DATE,
    processed_timestamp TIMESTAMP
)
PARTITIONED BY (Report_Date DATE)
STORED AS PARQUET
LOCATION '/data/gold/health_insights/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Aggregated health insights for business reporting',
    'data_classification' = 'gold',
    'compression' = 'snappy'
);

-- =====================================================
-- MODEL METADATA TABLES
-- =====================================================

-- Table for tracking ML model metadata
CREATE TABLE IF NOT EXISTS model_metadata (
    model_id STRING,
    model_name STRING,
    model_type STRING,
    algorithm STRING,
    training_date TIMESTAMP,
    training_data_path STRING,
    model_path STRING,
    hyperparameters MAP<STRING, STRING>,
    training_duration_minutes DOUBLE,
    data_version STRING,
    model_version STRING,
    created_by STRING,
    status STRING
)
STORED AS PARQUET
LOCATION '/models/metadata/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Metadata for trained ML models'
);

-- Table for storing model performance metrics
CREATE TABLE IF NOT EXISTS model_performance (
    model_id STRING,
    evaluation_date TIMESTAMP,
    test_data_path STRING,
    accuracy DOUBLE,
    precision DOUBLE,
    recall DOUBLE,
    f1_score DOUBLE,
    auc_roc DOUBLE,
    auc_pr DOUBLE,
    confusion_matrix_tn INT,
    confusion_matrix_fp INT,
    confusion_matrix_fn INT,
    confusion_matrix_tp INT,
    test_samples INT,
    evaluation_duration_minutes DOUBLE,
    evaluation_version STRING
)
PARTITIONED BY (evaluation_date DATE)
STORED AS PARQUET
LOCATION '/models/performance/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Performance metrics for model evaluations'
);

-- =====================================================
-- DATA QUALITY TABLES
-- =====================================================

-- Table for tracking data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    layer_name STRING,
    table_name STRING,
    check_date TIMESTAMP,
    total_records BIGINT,
    null_count_pregnancies BIGINT,
    null_count_glucose BIGINT,
    null_count_blood_pressure BIGINT,
    null_count_skin_thickness BIGINT,
    null_count_insulin BIGINT,
    null_count_bmi BIGINT,
    null_count_age BIGINT,
    duplicate_records BIGINT,
    outliers_detected BIGINT,
    data_quality_score DOUBLE,
    validation_status STRING,
    validation_errors ARRAY<STRING>
)
PARTITIONED BY (check_date DATE)
STORED AS PARQUET
LOCATION '/logs/data_quality/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'Data quality assessment results'
);

-- =====================================================
-- PIPELINE MONITORING TABLES
-- =====================================================

-- Table for tracking ETL pipeline execution
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id STRING,
    pipeline_name STRING,
    dag_id STRING,
    task_id STRING,
    execution_date TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_minutes DOUBLE,
    status STRING,
    input_records BIGINT,
    output_records BIGINT,
    error_message STRING,
    processing_host STRING,
    spark_application_id STRING
)
PARTITIONED BY (execution_date DATE)
STORED AS PARQUET
LOCATION '/logs/pipeline_execution/'
TBLPROPERTIES (
    'creator' = 'kelompok8_ra',
    'created_at' = '2024-01-01',
    'description' = 'ETL pipeline execution logs and metrics'
);

-- =====================================================
-- USEFUL VIEWS
-- =====================================================

-- View for latest diabetes predictions with risk assessment
CREATE VIEW IF NOT EXISTS latest_diabetes_predictions AS
SELECT 
    d.*,
    CASE 
        WHEN d.risk_score > 0.7 THEN 'Critical'
        WHEN d.risk_score > 0.5 THEN 'High'
        WHEN d.risk_score > 0.3 THEN 'Medium'
        ELSE 'Low'
    END AS risk_assessment,
    CURRENT_TIMESTAMP as view_generated_at
FROM diabetes_gold d
WHERE d.processed_timestamp = (
    SELECT MAX(processed_timestamp) 
    FROM diabetes_gold
);

-- View for health summary by demographics
CREATE VIEW IF NOT EXISTS health_summary_by_demographics AS
SELECT 
    age_group,
    bmi_category,
    COUNT(*) as patient_count,
    AVG(risk_score) as avg_risk_score,
    SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetes_cases,
    ROUND(SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as diabetes_rate_percent,
    COUNT(CASE WHEN intervention_priority = 'Immediate' THEN 1 END) as immediate_intervention_needed
FROM diabetes_gold
GROUP BY age_group, bmi_category
ORDER BY avg_risk_score DESC;

-- View for model performance trends
CREATE VIEW IF NOT EXISTS model_performance_trends AS
SELECT 
    DATE(evaluation_date) as eval_date,
    model_id,
    accuracy,
    f1_score,
    auc_roc,
    test_samples,
    LAG(accuracy) OVER (PARTITION BY model_id ORDER BY evaluation_date) as prev_accuracy,
    accuracy - LAG(accuracy) OVER (PARTITION BY model_id ORDER BY evaluation_date) as accuracy_change
FROM model_performance
ORDER BY model_id, evaluation_date DESC;

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Note: Hive doesn't support traditional indexes, but we can use bucketing
-- and partitioning strategies for better query performance

-- Add bucketing to frequently queried tables
ALTER TABLE diabetes_gold CLUSTERED BY (diabetes) INTO 4 BUCKETS;
ALTER TABLE model_performance CLUSTERED BY (model_id) INTO 4 BUCKETS;

-- =====================================================
-- MAINTENANCE PROCEDURES
-- =====================================================

-- Repair partitions (run when new partitions are added externally)
-- MSCK REPAIR TABLE diabetes_gold;
-- MSCK REPAIR TABLE health_insights_gold;
-- MSCK REPAIR TABLE model_performance;
-- MSCK REPAIR TABLE data_quality_metrics;
-- MSCK REPAIR TABLE pipeline_execution_log;

-- =====================================================
-- DATA RETENTION POLICIES
-- =====================================================

-- Note: These are example retention policies that should be implemented
-- as scheduled maintenance jobs

-- Drop old model performance data (keep 1 year)
-- DELETE FROM model_performance 
-- WHERE evaluation_date < DATE_SUB(CURRENT_DATE, 365);

-- Archive old pipeline execution logs (keep 6 months in main table)
-- INSERT INTO pipeline_execution_log_archive 
-- SELECT * FROM pipeline_execution_log 
-- WHERE execution_date < DATE_SUB(CURRENT_DATE, 180);

-- DELETE FROM pipeline_execution_log 
-- WHERE execution_date < DATE_SUB(CURRENT_DATE, 180);

-- =====================================================
-- GRANTS AND PERMISSIONS
-- =====================================================

-- Grant permissions to different user groups
-- GRANT SELECT ON DATABASE diabetes_pipeline TO GROUP analysts;
-- GRANT SELECT, INSERT ON TABLE diabetes_gold TO GROUP data_engineers;
-- GRANT ALL ON DATABASE diabetes_pipeline TO GROUP admin;

-- =====================================================
-- COMMENTS AND DOCUMENTATION
-- =====================================================

-- Add table comments for documentation
COMMENT ON TABLE diabetes_bronze IS 'Bronze layer table containing raw diabetes clinical data from the Pima Indians dataset. Data is ingested as-is without transformation.';
COMMENT ON TABLE diabetes_silver IS 'Silver layer table with cleaned and validated diabetes data. Missing values imputed, outliers handled, data quality flags added.';
COMMENT ON TABLE diabetes_gold IS 'Gold layer table with feature-engineered diabetes data ready for machine learning. Includes derived features, risk scores, and business categories.';
COMMENT ON TABLE model_metadata IS 'Stores metadata for all trained ML models including hyperparameters, paths, and versioning information.';
COMMENT ON TABLE model_performance IS 'Tracks performance metrics for all model evaluations over time to monitor model drift and performance trends.';

-- Column comments for key tables
ALTER TABLE diabetes_gold ADD COMMENT 'Primary diabetes prediction dataset with engineered features' TO TABLE;
ALTER TABLE diabetes_gold CHANGE COLUMN risk_score risk_score DOUBLE COMMENT 'Calculated risk score (0-1) based on weighted clinical factors';
ALTER TABLE diabetes_gold CHANGE COLUMN health_risk_level health_risk_level STRING COMMENT 'Categorical risk level: Low, Medium, High based on risk_score thresholds';
ALTER TABLE diabetes_gold CHANGE COLUMN intervention_priority intervention_priority STRING COMMENT 'Clinical intervention priority: Routine, Soon, Immediate';

-- =====================================================
-- EXAMPLE QUERIES FOR VALIDATION
-- =====================================================

-- Validate data flow from Bronze to Gold
-- SELECT 
--     'Bronze' as layer, COUNT(*) as record_count FROM diabetes_bronze
-- UNION ALL
-- SELECT 
--     'Silver' as layer, COUNT(*) as record_count FROM diabetes_silver
-- UNION ALL
-- SELECT 
--     'Gold' as layer, COUNT(*) as record_count FROM diabetes_gold;

-- Check data quality across layers
-- SELECT 
--     layer_name,
--     AVG(data_quality_score) as avg_quality_score,
--     MAX(check_date) as latest_check
-- FROM data_quality_metrics
-- GROUP BY layer_name;

-- Monitor model performance over time
-- SELECT 
--     model_id,
--     COUNT(*) as evaluation_count,
--     AVG(accuracy) as avg_accuracy,
--     MAX(evaluation_date) as latest_evaluation
-- FROM model_performance
-- GROUP BY model_id
-- ORDER BY avg_accuracy DESC;

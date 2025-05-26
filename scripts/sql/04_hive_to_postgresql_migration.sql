-- =============================================================================
-- HIVE TO POSTGRESQL MIGRATION SCHEMA
-- =============================================================================
-- Complete migration of Hive DDL to PostgreSQL equivalent structures
-- This creates PostgreSQL tables that match the Hive schema functionality
-- =============================================================================

-- Enable required extensions for advanced features
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas for data layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver; 
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS hive_compat; -- For Hive compatibility layer

-- =============================================================================
-- BRONZE LAYER - Raw Data Tables (Hive Equivalent)
-- =============================================================================

-- Bronze layer: Raw diabetes clinical data (equivalent to diabetes_bronze)
DROP TABLE IF EXISTS bronze.diabetes_clinical CASCADE;
CREATE TABLE bronze.diabetes_clinical (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pregnancies INTEGER,
    glucose DOUBLE PRECISION,
    blood_pressure DOUBLE PRECISION, -- BloodPressure in Hive
    skin_thickness DOUBLE PRECISION, -- SkinThickness in Hive
    insulin DOUBLE PRECISION,
    bmi DOUBLE PRECISION, -- BMI in Hive
    diabetes_pedigree_function DOUBLE PRECISION, -- DiabetesPedigreeFunction in Hive
    age INTEGER,
    outcome INTEGER CHECK (outcome IN (0, 1)), -- Outcome in Hive
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(255), -- data_source in Hive
    ingestion_version VARCHAR(50), -- ingestion_version in Hive
    file_name VARCHAR(255),
    row_number BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze layer: Raw personal health data (equivalent to personal_health_bronze)
DROP TABLE IF EXISTS bronze.personal_health_data CASCADE;
CREATE TABLE bronze.personal_health_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    person_id VARCHAR(50), -- Person_ID in Hive
    age INTEGER,
    gender VARCHAR(10),
    height DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    heart_rate DOUBLE PRECISION, -- Heart_Rate in Hive
    blood_pressure_systolic DOUBLE PRECISION, -- Blood_Pressure_Systolic in Hive
    blood_pressure_diastolic DOUBLE PRECISION, -- Blood_Pressure_Diastolic in Hive
    sleep_duration DOUBLE PRECISION, -- Sleep_Duration in Hive
    deep_sleep_duration DOUBLE PRECISION, -- Deep_Sleep_Duration in Hive
    rem_sleep_duration DOUBLE PRECISION, -- REM_Sleep_Duration in Hive
    steps_daily INTEGER, -- Steps_Daily in Hive
    exercise_minutes DOUBLE PRECISION, -- Exercise_Minutes in Hive
    stress_level VARCHAR(20), -- Stress_Level in Hive
    mood VARCHAR(20),
    medical_condition VARCHAR(100), -- Medical_Condition in Hive
    health_score DOUBLE PRECISION, -- Health_Score in Hive
    sleep_efficiency DOUBLE PRECISION, -- Sleep_Efficiency in Hive
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(255),
    ingestion_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze layer: Raw nutrition data (equivalent to nutrition_bronze)
DROP TABLE IF EXISTS bronze.nutrition_intake CASCADE;
CREATE TABLE bronze.nutrition_intake (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    person_id VARCHAR(50), -- Person_ID in Hive
    date_recorded DATE, -- Date in Hive
    calories_consumed DOUBLE PRECISION, -- Calories_Consumed in Hive
    protein_g DOUBLE PRECISION, -- Protein_g in Hive
    carbs_g DOUBLE PRECISION, -- Carbs_g in Hive
    fat_g DOUBLE PRECISION, -- Fat_g in Hive
    fiber_g DOUBLE PRECISION, -- Fiber_g in Hive
    sugar_g DOUBLE PRECISION, -- Sugar_g in Hive
    sodium_mg DOUBLE PRECISION, -- Sodium_mg in Hive
    water_liters DOUBLE PRECISION, -- Water_Liters in Hive
    meal_type VARCHAR(20), -- Meal_Type in Hive
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(255),
    ingestion_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create partitioned table for large datasets (Hive partitioning equivalent)
CREATE TABLE bronze.diabetes_clinical_partitioned (
    LIKE bronze.diabetes_clinical INCLUDING ALL
) PARTITION BY RANGE (created_at);

-- Create monthly partitions (equivalent to Hive partitioning)
CREATE TABLE bronze.diabetes_clinical_2025_05 PARTITION OF bronze.diabetes_clinical_partitioned
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE bronze.diabetes_clinical_2025_06 PARTITION OF bronze.diabetes_clinical_partitioned
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

-- =============================================================================
-- SILVER LAYER - Cleaned and Validated Data (Hive Equivalent)
-- =============================================================================

-- Silver layer: Cleaned diabetes data (equivalent to diabetes_silver)
DROP TABLE IF EXISTS silver.diabetes_cleaned CASCADE;
CREATE TABLE silver.diabetes_cleaned (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES bronze.diabetes_clinical(id),
    -- Original features (cleaned)
    pregnancies INTEGER,
    glucose DOUBLE PRECISION,
    blood_pressure DOUBLE PRECISION,
    skin_thickness DOUBLE PRECISION,
    insulin DOUBLE PRECISION,
    bmi DOUBLE PRECISION,
    diabetes_pedigree_function DOUBLE PRECISION,
    age INTEGER,
    outcome INTEGER CHECK (outcome IN (0, 1)),
    -- Data quality metrics
    is_valid BOOLEAN DEFAULT TRUE,
    quality_score DOUBLE PRECISION DEFAULT 1.0,
    completeness_score DOUBLE PRECISION,
    anomaly_flags JSONB,
    outlier_flags JSONB,
    -- Validation flags
    glucose_valid BOOLEAN,
    bmi_valid BOOLEAN,
    age_valid BOOLEAN,
    bp_valid BOOLEAN,
    -- Processing metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version VARCHAR(50) DEFAULT '1.0',
    data_lineage JSONB,
    -- Equivalent to Hive silver partitioning
    processing_date DATE DEFAULT CURRENT_DATE
);

-- Silver layer: Cleaned personal health data
DROP TABLE IF EXISTS silver.personal_health_cleaned CASCADE;
CREATE TABLE silver.personal_health_cleaned (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES bronze.personal_health_data(id),
    person_id VARCHAR(50),
    age INTEGER,
    gender VARCHAR(10),
    height DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    heart_rate DOUBLE PRECISION,
    blood_pressure_systolic DOUBLE PRECISION,
    blood_pressure_diastolic DOUBLE PRECISION,
    sleep_duration DOUBLE PRECISION,
    deep_sleep_duration DOUBLE PRECISION,
    rem_sleep_duration DOUBLE PRECISION,
    steps_daily INTEGER,
    exercise_minutes DOUBLE PRECISION,
    stress_level VARCHAR(20),
    mood VARCHAR(20),
    medical_condition VARCHAR(100),
    health_score DOUBLE PRECISION,
    sleep_efficiency DOUBLE PRECISION,
    -- Quality and validation
    is_valid BOOLEAN DEFAULT TRUE,
    quality_score DOUBLE PRECISION DEFAULT 1.0,
    anomaly_flags JSONB,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version VARCHAR(50) DEFAULT '1.0',
    processing_date DATE DEFAULT CURRENT_DATE
);

-- =============================================================================
-- GOLD LAYER - Enhanced Features and Business Logic (Hive Equivalent)
-- =============================================================================

-- Gold layer: Comprehensive diabetes features (equivalent to diabetes_gold)
DROP TABLE IF EXISTS gold.diabetes_comprehensive CASCADE;
CREATE TABLE gold.diabetes_comprehensive (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES silver.diabetes_cleaned(id),
    
    -- Original features
    pregnancies INTEGER,
    glucose DOUBLE PRECISION,
    blood_pressure DOUBLE PRECISION,
    skin_thickness DOUBLE PRECISION,
    insulin DOUBLE PRECISION,
    bmi DOUBLE PRECISION,
    diabetes_pedigree_function DOUBLE PRECISION,
    age INTEGER,
    diabetes INTEGER, -- outcome renamed to diabetes for consistency
    
    -- Categorical features (equivalent to Hive gold)
    bmi_category VARCHAR(20),
    age_group VARCHAR(20),
    glucose_category VARCHAR(30),
    bp_category VARCHAR(20),
    insulin_category VARCHAR(20),
    pregnancy_category VARCHAR(20),
    
    -- Risk scoring and predictions
    risk_score DOUBLE PRECISION,
    prediction DOUBLE PRECISION,
    prediction_probability DOUBLE PRECISION,
    confidence_interval_lower DOUBLE PRECISION,
    confidence_interval_upper DOUBLE PRECISION,
    
    -- Interaction features (equivalent to Hive feature engineering)
    glucose_bmi_interaction DOUBLE PRECISION,
    age_glucose_interaction DOUBLE PRECISION,
    insulin_glucose_interaction DOUBLE PRECISION,
    age_bmi_interaction DOUBLE PRECISION,
    pregnancy_glucose_interaction DOUBLE PRECISION,
    age_pregnancy_interaction DOUBLE PRECISION,
    
    -- Normalized features (0-1 scale)
    glucose_normalized DOUBLE PRECISION,
    bmi_normalized DOUBLE PRECISION,
    age_normalized DOUBLE PRECISION,
    bp_normalized DOUBLE PRECISION,
    insulin_normalized DOUBLE PRECISION,
    dpf_normalized DOUBLE PRECISION,
    
    -- Advanced derived features
    metabolic_syndrome_risk DOUBLE PRECISION,
    cardiovascular_risk DOUBLE PRECISION,
    pregnancy_risk_factor DOUBLE PRECISION,
    lifestyle_risk_score DOUBLE PRECISION,
    insulin_resistance_score DOUBLE PRECISION,
    
    -- Business Intelligence features
    health_risk_level VARCHAR(20), -- Low, Medium, High, Critical
    intervention_priority VARCHAR(20), -- Immediate, Soon, Routine, Monitor
    estimated_cost_category VARCHAR(20), -- Low, Medium, High, Very High
    treatment_recommendation TEXT,
    followup_frequency VARCHAR(20),
    specialist_referral_needed BOOLEAN,
    
    -- Statistical features
    glucose_zscore DOUBLE PRECISION,
    bmi_zscore DOUBLE PRECISION,
    age_percentile DOUBLE PRECISION,
    risk_percentile DOUBLE PRECISION,
    
    -- Data lineage and quality
    data_source VARCHAR(50),
    processing_version VARCHAR(20),
    feature_engineering_version VARCHAR(20),
    quality_score DOUBLE PRECISION,
    anomaly_flags JSONB,
    feature_importance JSONB,
    model_features JSONB,
    
    -- Metadata
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_date DATE DEFAULT CURRENT_DATE,
    etl_batch_id VARCHAR(50),
    
    -- Hive-style partitioning equivalent
    year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM processing_date)) STORED,
    month INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM processing_date)) STORED
);

-- Gold layer: Health insights aggregations (equivalent to health_insights_gold)
DROP TABLE IF EXISTS gold.health_insights_comprehensive CASCADE;
CREATE TABLE gold.health_insights_comprehensive (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Grouping dimensions (equivalent to Hive partitioning)
    age_group VARCHAR(20),
    gender VARCHAR(10),
    health_risk_category VARCHAR(20),
    bmi_category VARCHAR(20),
    geographic_region VARCHAR(50),
    socioeconomic_category VARCHAR(20),
    
    -- Patient demographics and counts
    total_patients INTEGER,
    diabetic_patients INTEGER,
    prediabetic_patients INTEGER,
    high_risk_patients INTEGER,
    critical_risk_patients INTEGER,
    new_patients_this_period INTEGER,
    
    -- Health metrics aggregations
    avg_bmi DOUBLE PRECISION,
    median_bmi DOUBLE PRECISION,
    std_bmi DOUBLE PRECISION,
    avg_glucose DOUBLE PRECISION,
    median_glucose DOUBLE PRECISION,
    std_glucose DOUBLE PRECISION,
    avg_age DOUBLE PRECISION,
    avg_risk_score DOUBLE PRECISION,
    avg_prediction_probability DOUBLE PRECISION,
    
    -- Advanced health analytics
    avg_health_score DOUBLE PRECISION,
    avg_sleep_efficiency DOUBLE PRECISION,
    avg_steps_daily DOUBLE PRECISION,
    avg_exercise_minutes DOUBLE PRECISION,
    avg_heart_rate DOUBLE PRECISION,
    avg_stress_level_numeric DOUBLE PRECISION,
    
    -- Risk analysis
    high_risk_percentage DOUBLE PRECISION,
    diabetes_prevalence DOUBLE PRECISION,
    prediabetes_prevalence DOUBLE PRECISION,
    metabolic_syndrome_prevalence DOUBLE PRECISION,
    
    -- Treatment and intervention analytics
    immediate_intervention_count INTEGER,
    specialist_referral_count INTEGER,
    avg_estimated_treatment_cost DOUBLE PRECISION,
    prevention_opportunity_count INTEGER,
    
    -- Behavioral insights
    high_stress_percentage DOUBLE PRECISION,
    low_activity_percentage DOUBLE PRECISION,
    poor_sleep_percentage DOUBLE PRECISION,
    
    -- Distributions (JSON for flexibility)
    age_distribution JSONB,
    bmi_distribution JSONB,
    glucose_distribution JSONB,
    risk_score_distribution JSONB,
    intervention_urgency_distribution JSONB,
    cost_category_distribution JSONB,
    stress_level_distribution JSONB,
    mood_distribution JSONB,
    
    -- Trend analysis
    trend_diabetes_rate DOUBLE PRECISION,
    trend_avg_bmi DOUBLE PRECISION,
    trend_high_risk_rate DOUBLE PRECISION,
    
    -- Reporting metadata
    report_period VARCHAR(20), -- daily, weekly, monthly
    report_date DATE,
    report_start_date DATE,
    report_end_date DATE,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_freshness_hours DOUBLE PRECISION
);

-- =============================================================================
-- HIVE COMPATIBILITY LAYER
-- =============================================================================

-- Create views that match Hive table names for backward compatibility
CREATE SCHEMA IF NOT EXISTS hive_compat;

-- Hive-compatible view for diabetes_bronze
CREATE OR REPLACE VIEW hive_compat.diabetes_bronze AS
SELECT 
    pregnancies,
    glucose,
    blood_pressure as bloodpressure,
    skin_thickness as skinthickness,
    insulin,
    bmi,
    diabetes_pedigree_function as diabetespedigreefunction,
    age,
    outcome,
    ingestion_timestamp,
    data_source,
    ingestion_version
FROM bronze.diabetes_clinical;

-- Hive-compatible view for personal_health_bronze
CREATE OR REPLACE VIEW hive_compat.personal_health_bronze AS
SELECT 
    person_id,
    age,
    gender,
    height,
    weight,
    heart_rate,
    blood_pressure_systolic,
    blood_pressure_diastolic,
    sleep_duration,
    deep_sleep_duration,
    rem_sleep_duration,
    steps_daily,
    exercise_minutes,
    stress_level,
    mood,
    medical_condition,
    health_score,
    sleep_efficiency,
    ingestion_timestamp,
    data_source,
    ingestion_version
FROM bronze.personal_health_data;

-- Hive-compatible view for diabetes_gold
CREATE OR REPLACE VIEW hive_compat.diabetes_gold AS
SELECT 
    pregnancies,
    glucose,
    blood_pressure,
    skin_thickness,
    insulin,
    bmi,
    diabetes_pedigree_function,
    age,
    diabetes,
    bmi_category,
    age_group,
    glucose_category,
    bp_category,
    risk_score,
    prediction,
    prediction_probability,
    glucose_bmi_interaction,
    age_glucose_interaction,
    insulin_glucose_interaction,
    age_bmi_interaction,
    glucose_normalized,
    bmi_normalized,
    age_normalized,
    bp_normalized,
    insulin_normalized,
    health_risk_level,
    intervention_priority,
    estimated_cost_category,
    metabolic_syndrome_risk,
    cardiovascular_risk,
    pregnancy_risk_factor,
    lifestyle_risk_score,
    data_source,
    processing_version,
    quality_score,
    processed_timestamp,
    processing_date
FROM gold.diabetes_comprehensive;

-- =============================================================================
-- COMPREHENSIVE INDEXES FOR PERFORMANCE
-- =============================================================================

-- Bronze layer indexes
CREATE INDEX idx_diabetes_clinical_outcome ON bronze.diabetes_clinical(outcome);
CREATE INDEX idx_diabetes_clinical_glucose ON bronze.diabetes_clinical(glucose);
CREATE INDEX idx_diabetes_clinical_bmi ON bronze.diabetes_clinical(bmi);
CREATE INDEX idx_diabetes_clinical_age ON bronze.diabetes_clinical(age);
CREATE INDEX idx_diabetes_clinical_ingestion ON bronze.diabetes_clinical(ingestion_timestamp);
CREATE INDEX idx_diabetes_clinical_source ON bronze.diabetes_clinical(data_source);
CREATE INDEX idx_diabetes_clinical_composite ON bronze.diabetes_clinical(outcome, glucose, bmi);

CREATE INDEX idx_personal_health_person_id ON bronze.personal_health_data(person_id);
CREATE INDEX idx_personal_health_age ON bronze.personal_health_data(age);
CREATE INDEX idx_personal_health_health_score ON bronze.personal_health_data(health_score);

-- Silver layer indexes
CREATE INDEX idx_diabetes_cleaned_outcome ON silver.diabetes_cleaned(outcome);
CREATE INDEX idx_diabetes_cleaned_valid ON silver.diabetes_cleaned(is_valid);
CREATE INDEX idx_diabetes_cleaned_quality ON silver.diabetes_cleaned(quality_score);
CREATE INDEX idx_diabetes_cleaned_processed ON silver.diabetes_cleaned(processed_at);
CREATE INDEX idx_diabetes_cleaned_source ON silver.diabetes_cleaned(source_id);
CREATE INDEX idx_diabetes_cleaned_composite ON silver.diabetes_cleaned(outcome, is_valid, quality_score);

-- Gold layer indexes (comprehensive for analytics)
CREATE INDEX idx_diabetes_comprehensive_diabetes ON gold.diabetes_comprehensive(diabetes);
CREATE INDEX idx_diabetes_comprehensive_risk_level ON gold.diabetes_comprehensive(health_risk_level);
CREATE INDEX idx_diabetes_comprehensive_age_group ON gold.diabetes_comprehensive(age_group);
CREATE INDEX idx_diabetes_comprehensive_bmi_category ON gold.diabetes_comprehensive(bmi_category);
CREATE INDEX idx_diabetes_comprehensive_glucose_category ON gold.diabetes_comprehensive(glucose_category);
CREATE INDEX idx_diabetes_comprehensive_intervention ON gold.diabetes_comprehensive(intervention_priority);
CREATE INDEX idx_diabetes_comprehensive_risk_score ON gold.diabetes_comprehensive(risk_score);
CREATE INDEX idx_diabetes_comprehensive_prediction ON gold.diabetes_comprehensive(prediction);
CREATE INDEX idx_diabetes_comprehensive_date ON gold.diabetes_comprehensive(processing_date);
CREATE INDEX idx_diabetes_comprehensive_year_month ON gold.diabetes_comprehensive(year, month);

-- Composite indexes for complex analytics queries
CREATE INDEX idx_diabetes_comprehensive_analytics ON gold.diabetes_comprehensive(health_risk_level, age_group, bmi_category);
CREATE INDEX idx_diabetes_comprehensive_prediction_analysis ON gold.diabetes_comprehensive(prediction, prediction_probability, diabetes);
CREATE INDEX idx_diabetes_comprehensive_risk_intervention ON gold.diabetes_comprehensive(risk_score, intervention_priority, health_risk_level);

-- Partial indexes for high-performance queries
CREATE INDEX idx_diabetes_comprehensive_high_risk ON gold.diabetes_comprehensive(risk_score, diabetes) 
WHERE health_risk_level IN ('High', 'Critical');

CREATE INDEX idx_diabetes_comprehensive_diabetic ON gold.diabetes_comprehensive(health_risk_level, age_group) 
WHERE diabetes = 1;

CREATE INDEX idx_diabetes_comprehensive_immediate_intervention ON gold.diabetes_comprehensive(risk_score, prediction_probability) 
WHERE intervention_priority = 'Immediate';

-- GIN indexes for JSONB columns
CREATE INDEX idx_diabetes_comprehensive_anomaly_flags ON gold.diabetes_comprehensive USING GIN (anomaly_flags);
CREATE INDEX idx_diabetes_comprehensive_feature_importance ON gold.diabetes_comprehensive USING GIN (feature_importance);
CREATE INDEX idx_health_insights_distributions ON gold.health_insights_comprehensive USING GIN (age_distribution);

-- =============================================================================
-- PERFORMANCE OPTIMIZATION
-- =============================================================================

-- Create statistics for query optimization
CREATE STATISTICS diabetes_comprehensive_risk_stats ON health_risk_level, diabetes, risk_score 
FROM gold.diabetes_comprehensive;

CREATE STATISTICS diabetes_comprehensive_demographics_stats ON age_group, bmi_category, glucose_category 
FROM gold.diabetes_comprehensive;

-- Set table storage parameters for performance
ALTER TABLE gold.diabetes_comprehensive SET (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05
);

ALTER TABLE gold.health_insights_comprehensive SET (
    fillfactor = 95,
    autovacuum_vacuum_scale_factor = 0.2,
    autovacuum_analyze_scale_factor = 0.1
);

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Create warehouse role for applications
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_user') THEN
        CREATE ROLE warehouse_user;
    END IF;
END
$$;

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA bronze TO warehouse_user;
GRANT USAGE ON SCHEMA silver TO warehouse_user;
GRANT USAGE ON SCHEMA gold TO warehouse_user;
GRANT USAGE ON SCHEMA analytics TO warehouse_user;
GRANT USAGE ON SCHEMA hive_compat TO warehouse_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO warehouse_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO warehouse_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO warehouse_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO warehouse_user;
GRANT SELECT ON ALL TABLES IN SCHEMA hive_compat TO warehouse_user;

-- Grant permissions on sequences
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bronze TO warehouse_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA silver TO warehouse_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA gold TO warehouse_user;

-- Grant permissions to the configured warehouse user
GRANT warehouse_user TO warehouse;

-- =============================================================================
-- MIGRATION VALIDATION
-- =============================================================================

-- Create a function to validate the migration
CREATE OR REPLACE FUNCTION validate_hive_to_postgresql_migration()
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    status TEXT,
    row_count BIGINT,
    notes TEXT
) AS $$
BEGIN
    -- Check bronze tables
    RETURN QUERY
    SELECT 'bronze'::TEXT, 'diabetes_clinical'::TEXT, 'CREATED'::TEXT, 
           (SELECT count(*) FROM bronze.diabetes_clinical), 
           'Equivalent to Hive diabetes_bronze'::TEXT;
    
    RETURN QUERY
    SELECT 'bronze'::TEXT, 'personal_health_data'::TEXT, 'CREATED'::TEXT, 
           (SELECT count(*) FROM bronze.personal_health_data), 
           'Equivalent to Hive personal_health_bronze'::TEXT;
    
    -- Check silver tables
    RETURN QUERY
    SELECT 'silver'::TEXT, 'diabetes_cleaned'::TEXT, 'CREATED'::TEXT, 
           (SELECT count(*) FROM silver.diabetes_cleaned), 
           'Enhanced version of Hive diabetes_silver'::TEXT;
    
    -- Check gold tables
    RETURN QUERY
    SELECT 'gold'::TEXT, 'diabetes_comprehensive'::TEXT, 'CREATED'::TEXT, 
           (SELECT count(*) FROM gold.diabetes_comprehensive), 
           'Enhanced version of Hive diabetes_gold'::TEXT;
    
    -- Check compatibility views
    RETURN QUERY
    SELECT 'hive_compat'::TEXT, 'diabetes_bronze'::TEXT, 'VIEW_CREATED'::TEXT, 
           (SELECT count(*) FROM hive_compat.diabetes_bronze), 
           'Hive compatibility view'::TEXT;
    
END;
$$ LANGUAGE plpgsql;

-- Log the migration completion
INSERT INTO analytics.model_performance (model_name, model_version, notes, training_date)
VALUES ('hive_to_postgresql_migration', '1.0', 'Complete Hive to PostgreSQL schema migration with enhanced features', CURRENT_TIMESTAMP);

-- Success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'HIVE TO POSTGRESQL MIGRATION COMPLETE!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schemas migrated: bronze, silver, gold';
    RAISE NOTICE 'Compatibility layer: hive_compat schema';
    RAISE NOTICE 'Enhanced features: Advanced analytics, better indexing';
    RAISE NOTICE 'Ready for ETL pipeline integration';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Run SELECT * FROM validate_hive_to_postgresql_migration() to validate';
END $$;

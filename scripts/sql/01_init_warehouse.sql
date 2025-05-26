-- =============================================================================
-- POSTGRESQL DATA WAREHOUSE INITIALIZATION
-- =============================================================================
-- Lightweight alternative to Hive for diabetes prediction pipeline
-- Creates optimized tables and views for data analysis and Grafana visualization
-- =============================================================================

-- =============================================================================
-- BRONZE, SILVER, GOLD LAYER TABLES INITIALIZATION
-- =============================================================================
-- Note: Extensions and schemas are created in 00_master_init.sql

\echo 'Creating Bronze, Silver, and Gold layer tables...'

-- =============================================================================
-- BRONZE LAYER - Raw Data Tables
-- =============================================================================

-- Raw diabetes data table
DROP TABLE IF EXISTS bronze.diabetes_raw CASCADE;
CREATE TABLE bronze.diabetes_raw (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pregnancies INTEGER,
    glucose DECIMAL(5,2),
    blood_pressure DECIMAL(5,2),
    skin_thickness DECIMAL(5,2),
    insulin DECIMAL(6,2),
    bmi DECIMAL(5,2),
    diabetes_pedigree_function DECIMAL(6,3),
    age INTEGER,
    outcome INTEGER CHECK (outcome IN (0, 1)),
    source_file VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_diabetes_raw_outcome ON bronze.diabetes_raw(outcome);
CREATE INDEX idx_diabetes_raw_age ON bronze.diabetes_raw(age);
CREATE INDEX idx_diabetes_raw_glucose ON bronze.diabetes_raw(glucose);
CREATE INDEX idx_diabetes_raw_bmi ON bronze.diabetes_raw(bmi);
CREATE INDEX idx_diabetes_raw_ingestion ON bronze.diabetes_raw(ingestion_timestamp);

-- =============================================================================
-- SILVER LAYER - Cleaned and Validated Data
-- =============================================================================

-- Cleaned diabetes data with quality flags
DROP TABLE IF EXISTS silver.diabetes_cleaned CASCADE;
CREATE TABLE silver.diabetes_cleaned (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES bronze.diabetes_raw(id),
    pregnancies INTEGER,
    glucose DECIMAL(5,2),
    blood_pressure DECIMAL(5,2),
    skin_thickness DECIMAL(5,2),
    insulin DECIMAL(6,2),
    bmi DECIMAL(5,2),
    diabetes_pedigree_function DECIMAL(6,3),
    age INTEGER,
    outcome INTEGER CHECK (outcome IN (0, 1)),
    -- Quality flags
    is_valid BOOLEAN DEFAULT TRUE,
    quality_score DECIMAL(3,2) DEFAULT 1.0,
    anomaly_flags JSONB,
    -- Processing metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version VARCHAR(50) DEFAULT '1.0'
);

-- Create indexes
CREATE INDEX idx_diabetes_cleaned_outcome ON silver.diabetes_cleaned(outcome);
CREATE INDEX idx_diabetes_cleaned_valid ON silver.diabetes_cleaned(is_valid);
CREATE INDEX idx_diabetes_cleaned_quality ON silver.diabetes_cleaned(quality_score);
CREATE INDEX idx_diabetes_cleaned_processed ON silver.diabetes_cleaned(processed_at);

-- =============================================================================
-- GOLD LAYER - Feature Engineering and Aggregations
-- =============================================================================

-- Enhanced features table
DROP TABLE IF EXISTS gold.diabetes_features CASCADE;
CREATE TABLE gold.diabetes_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES silver.diabetes_cleaned(id),
    -- Original features
    pregnancies INTEGER,
    glucose DECIMAL(5,2),
    blood_pressure DECIMAL(5,2),
    skin_thickness DECIMAL(5,2),
    insulin DECIMAL(6,2),
    bmi DECIMAL(5,2),
    diabetes_pedigree_function DECIMAL(6,3),
    age INTEGER,
    outcome INTEGER,
    -- Engineered features
    age_group VARCHAR(20),
    bmi_category VARCHAR(20),
    glucose_category VARCHAR(30),
    risk_score DECIMAL(5,2),
    high_risk_flag BOOLEAN,
    -- Feature statistics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for analytics
CREATE INDEX idx_features_outcome ON gold.diabetes_features(outcome);
CREATE INDEX idx_features_age_group ON gold.diabetes_features(age_group);
CREATE INDEX idx_features_bmi_category ON gold.diabetes_features(bmi_category);
CREATE INDEX idx_features_glucose_category ON gold.diabetes_features(glucose_category);
CREATE INDEX idx_features_high_risk ON gold.diabetes_features(high_risk_flag);
CREATE INDEX idx_features_risk_score ON gold.diabetes_features(risk_score);

-- =============================================================================
-- ANALYTICS SCHEMA - Views and Aggregations for Grafana
-- =============================================================================

-- Daily statistics view
CREATE OR REPLACE VIEW analytics.daily_stats AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_records,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) as diabetic_count,
    SUM(CASE WHEN outcome = 0 THEN 1 ELSE 0 END) as non_diabetic_count,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(age), 2) as avg_age,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    SUM(CASE WHEN high_risk_flag = true THEN 1 ELSE 0 END) as high_risk_count
FROM gold.diabetes_features
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- Age group analysis view
CREATE OR REPLACE VIEW analytics.age_group_analysis AS
SELECT 
    age_group,
    COUNT(*) as total_count,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) as diabetic_count,
    ROUND((SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as diabetes_rate,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(risk_score), 2) as avg_risk_score
FROM gold.diabetes_features
GROUP BY age_group
ORDER BY age_group;

-- BMI category analysis view
CREATE OR REPLACE VIEW analytics.bmi_category_analysis AS
SELECT 
    bmi_category,
    COUNT(*) as total_count,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) as diabetic_count,
    ROUND((SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as diabetes_rate,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(age), 2) as avg_age
FROM gold.diabetes_features
GROUP BY bmi_category
ORDER BY diabetes_rate DESC;

-- Risk analysis view
CREATE OR REPLACE VIEW analytics.risk_analysis AS
SELECT 
    CASE 
        WHEN risk_score < 0.3 THEN 'Low Risk'
        WHEN risk_score < 0.6 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_category,
    COUNT(*) as total_count,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) as diabetic_count,
    ROUND((SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as diabetes_rate,
    ROUND(AVG(glucose), 2) as avg_glucose,
    ROUND(AVG(bmi), 2) as avg_bmi,
    ROUND(AVG(age), 2) as avg_age
FROM gold.diabetes_features
GROUP BY risk_category
ORDER BY diabetes_rate DESC;

-- Latest metrics view for Grafana
CREATE OR REPLACE VIEW analytics.latest_metrics AS
SELECT 
    'total_records' as metric_name,
    COUNT(*)::TEXT as metric_value,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_features
UNION ALL
SELECT 
    'diabetic_count' as metric_name,
    SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END)::TEXT as metric_value,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_features
UNION ALL
SELECT 
    'avg_glucose' as metric_name,
    ROUND(AVG(glucose), 2)::TEXT as metric_value,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_features
UNION ALL
SELECT 
    'avg_bmi' as metric_name,
    ROUND(AVG(bmi), 2)::TEXT as metric_value,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_features
UNION ALL
SELECT 
    'high_risk_count' as metric_name,
    SUM(CASE WHEN high_risk_flag = true THEN 1 ELSE 0 END)::TEXT as metric_value,
    CURRENT_TIMESTAMP as updated_at
FROM gold.diabetes_features;

-- =============================================================================
-- MODEL TRACKING TABLES
-- =============================================================================

-- Model performance tracking
CREATE TABLE analytics.model_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    accuracy DECIMAL(5,4),
    precision_score DECIMAL(5,4),
    recall DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    auc_score DECIMAL(5,4),
    training_date TIMESTAMP,
    evaluation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_version VARCHAR(50),
    notes TEXT
);

-- Create index for model performance
CREATE INDEX idx_model_performance_name ON analytics.model_performance(model_name);
CREATE INDEX idx_model_performance_date ON analytics.model_performance(evaluation_date);

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Grant permissions for analytics access
GRANT USAGE ON SCHEMA bronze TO warehouse;
GRANT USAGE ON SCHEMA silver TO warehouse;
GRANT USAGE ON SCHEMA gold TO warehouse;
GRANT USAGE ON SCHEMA analytics TO warehouse;

GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO warehouse;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO warehouse;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO warehouse;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO warehouse;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO warehouse;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO warehouse;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO warehouse;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics TO warehouse;

-- Grant sequence permissions
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bronze TO warehouse;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA silver TO warehouse;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA gold TO warehouse;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA analytics TO warehouse;

-- =============================================================================
-- SAMPLE DATA INSERT (for testing)
-- =============================================================================

-- This will be populated by the ETL pipeline
-- Sample insert statement (commented out for production):
/*
INSERT INTO bronze.diabetes_raw (pregnancies, glucose, blood_pressure, skin_thickness, insulin, bmi, diabetes_pedigree_function, age, outcome, source_file)
VALUES 
    (6, 148.0, 72.0, 35.0, 0.0, 33.6, 0.627, 50, 1, 'sample_data.csv'),
    (1, 85.0, 66.0, 29.0, 0.0, 26.6, 0.351, 31, 0, 'sample_data.csv');
*/

-- Create a function to refresh materialized views (if needed later)
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    -- Currently using views, but can be converted to materialized views for performance
    -- REFRESH MATERIALIZED VIEW analytics.daily_stats;
    -- Add other materialized views here
    NULL;
END;
$$ LANGUAGE plpgsql;

-- Log initialization
INSERT INTO analytics.model_performance (model_name, model_version, notes, training_date)
VALUES ('initialization', '1.0', 'Database warehouse initialized successfully', CURRENT_TIMESTAMP);

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL Data Warehouse initialized successfully!';
    RAISE NOTICE 'Schemas created: bronze, silver, gold, analytics';
    RAISE NOTICE 'Ready to receive data from ETL pipeline';
END $$;

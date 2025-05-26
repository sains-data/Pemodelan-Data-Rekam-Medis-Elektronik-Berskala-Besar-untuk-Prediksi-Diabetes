-- =============================================================================
-- INITIAL DATA SETUP AND VALIDATION
-- =============================================================================
-- Creates sample data, validates installation, and prepares for ETL
-- =============================================================================

\echo 'Setting up initial data and validating installation...'

-- =============================================================================
-- CREATE SAMPLE REFERENCE DATA
-- =============================================================================

-- Health Risk Categories Reference
CREATE TABLE IF NOT EXISTS gold.health_risk_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE,
    risk_score_min DECIMAL(3,2) NOT NULL,
    risk_score_max DECIMAL(3,2) NOT NULL,
    description TEXT,
    color_code VARCHAR(7), -- For dashboard visualization
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert risk categories
INSERT INTO gold.health_risk_categories (category_name, risk_score_min, risk_score_max, description, color_code) VALUES
('Low Risk', 0.00, 0.30, 'Low probability of diabetes development', '#28a745'),
('Moderate Risk', 0.31, 0.60, 'Moderate probability requiring lifestyle monitoring', '#ffc107'),
('High Risk', 0.61, 0.80, 'High probability requiring medical attention', '#fd7e14'),
('Critical Risk', 0.81, 1.00, 'Very high probability requiring immediate intervention', '#dc3545')
ON CONFLICT (category_name) DO NOTHING;

-- Age Group Reference
CREATE TABLE IF NOT EXISTS gold.age_groups (
    group_id SERIAL PRIMARY KEY,
    group_name VARCHAR(30) NOT NULL UNIQUE,
    age_min INTEGER NOT NULL,
    age_max INTEGER NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert age groups
INSERT INTO gold.age_groups (group_name, age_min, age_max, description) VALUES
('Young Adult', 18, 30, 'Young adults with developing health patterns'),
('Adult', 31, 45, 'Adults in prime health monitoring age'),
('Middle Age', 46, 60, 'Middle-aged adults with increased health risks'),
('Senior', 61, 80, 'Senior adults requiring enhanced health monitoring'),
('Elderly', 81, 120, 'Elderly adults with complex health needs')
ON CONFLICT (group_name) DO NOTHING;

-- BMI Categories Reference
CREATE TABLE IF NOT EXISTS gold.bmi_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(30) NOT NULL UNIQUE,
    bmi_min DECIMAL(4,1) NOT NULL,
    bmi_max DECIMAL(4,1) NOT NULL,
    description TEXT,
    risk_factor DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert BMI categories
INSERT INTO gold.bmi_categories (category_name, bmi_min, bmi_max, description, risk_factor) VALUES
('Underweight', 0.0, 18.4, 'Below normal weight', 1.1),
('Normal Weight', 18.5, 24.9, 'Healthy weight range', 1.0),
('Overweight', 25.0, 29.9, 'Above normal weight', 1.3),
('Obese Class I', 30.0, 34.9, 'Moderately obese', 1.6),
('Obese Class II', 35.0, 39.9, 'Severely obese', 2.0),
('Obese Class III', 40.0, 99.9, 'Very severely obese', 2.5)
ON CONFLICT (category_name) DO NOTHING;

-- =============================================================================
-- SAMPLE DATA FOR TESTING
-- =============================================================================

-- Insert sample data if tables are empty
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bronze.diabetes_raw LIMIT 1) THEN
        \echo 'Inserting sample test data...';
        
        INSERT INTO bronze.diabetes_raw (
            pregnancies, glucose, blood_pressure, skin_thickness, 
            insulin, bmi, diabetes_pedigree_function, age, outcome, source_file
        ) VALUES
        (6, 148.0, 72.0, 35.0, 0.0, 33.6, 0.627, 50, 1, 'sample_data.csv'),
        (1, 85.0, 66.0, 29.0, 0.0, 26.6, 0.351, 31, 0, 'sample_data.csv'),
        (8, 183.0, 64.0, 0.0, 0.0, 23.3, 0.672, 32, 1, 'sample_data.csv'),
        (1, 89.0, 66.0, 23.0, 94.0, 28.1, 0.167, 21, 0, 'sample_data.csv'),
        (0, 137.0, 40.0, 35.0, 168.0, 43.1, 2.288, 33, 1, 'sample_data.csv');
        
        \echo 'Sample data inserted successfully!';
    END IF;
END $$;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- Validate schema creation
\echo ''
\echo 'VALIDATION RESULTS:'
\echo '=================='

-- Check schemas
\echo 'Checking schemas...'
SELECT 'Schema: ' || schema_name as schema_check 
FROM information_schema.schemata 
WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics', 'monitoring', 'logs')
ORDER BY schema_name;

-- Check tables count by schema
\echo ''
\echo 'Table counts by schema:'
SELECT 
    schemaname as schema_name,
    COUNT(*) as table_count
FROM pg_tables 
WHERE schemaname IN ('bronze', 'silver', 'gold', 'analytics', 'monitoring', 'logs')
GROUP BY schemaname
ORDER BY schemaname;

-- Check extensions
\echo ''
\echo 'Installed extensions:'
SELECT 'Extension: ' || extname as extension_check
FROM pg_extension 
WHERE extname IN ('uuid-ossp', 'pg_stat_statements', 'btree_gin', 'pg_trgm')
ORDER BY extname;

-- Check sample data
\echo ''
\echo 'Sample data validation:'
SELECT 
    'Bronze layer records: ' || COUNT(*)::text as data_check
FROM bronze.diabetes_raw;

SELECT 
    'Reference data - Risk categories: ' || COUNT(*)::text as ref_check
FROM gold.health_risk_categories;

-- =============================================================================
-- PERFORMANCE OPTIMIZATION
-- =============================================================================

-- Update table statistics
ANALYZE bronze.diabetes_raw;
ANALYZE gold.health_risk_categories;
ANALYZE gold.age_groups;
ANALYZE gold.bmi_categories;

-- =============================================================================
-- FINAL SETUP LOGGING
-- =============================================================================

-- Log successful initialization
INSERT INTO logs.application_logs (
    log_level, logger_name, message, module_name, function_name
) VALUES (
    'INFO', 
    'postgresql_init', 
    'PostgreSQL Data Warehouse initialization completed successfully', 
    'database_setup', 
    'initial_data_setup'
);

-- Log monitoring setup
SELECT monitoring.log_etl_job(
    'database_initialization',
    'setup',
    'completed',
    (SELECT COUNT(*) FROM bronze.diabetes_raw)::INTEGER,
    0,
    NULL,
    jsonb_build_object(
        'schemas_created', 6,
        'tables_created', (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('bronze', 'silver', 'gold', 'analytics', 'monitoring', 'logs')),
        'extensions_enabled', 4,
        'initialization_timestamp', CURRENT_TIMESTAMP
    )
);

\echo ''
\echo '========================================='
\echo 'PostgreSQL Data Warehouse Setup Complete!'
\echo '========================================='
\echo 'Schemas: bronze, silver, gold, analytics, monitoring, logs'
\echo 'Extensions: uuid-ossp, pg_stat_statements, btree_gin, pg_trgm'
\echo 'Sample data: Available for testing'
\echo 'Monitoring: Enabled with logging functions'
\echo 'Ready for ETL pipeline execution!'
\echo '========================================='

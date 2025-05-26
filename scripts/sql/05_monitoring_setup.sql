-- =============================================================================
-- MONITORING AND LOGGING SETUP
-- =============================================================================
-- Creates monitoring tables, logging functions, and health check views
-- =============================================================================

\echo 'Setting up monitoring and logging infrastructure...'

-- =============================================================================
-- MONITORING SCHEMA TABLES
-- =============================================================================

-- ETL Job Execution Tracking
CREATE TABLE IF NOT EXISTS monitoring.etl_jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL, -- 'ingestion', 'transformation', 'ml_training'
    status VARCHAR(20) NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed'
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Metrics
CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL, -- 'null_count', 'duplicate_count', 'outlier_count'
    metric_value DECIMAL(15,4),
    threshold_value DECIMAL(15,4),
    is_passed BOOLEAN DEFAULT TRUE,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- System Performance Metrics
CREATE TABLE IF NOT EXISTS monitoring.performance_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_type VARCHAR(50) NOT NULL, -- 'cpu', 'memory', 'disk', 'query_time'
    metric_value DECIMAL(15,4),
    metric_unit VARCHAR(20), -- '%', 'MB', 'seconds'
    host_name VARCHAR(100),
    database_name VARCHAR(100),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- LOGGING SCHEMA TABLES
-- =============================================================================

-- Application Logs
CREATE TABLE IF NOT EXISTS logs.application_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    log_level VARCHAR(20) NOT NULL, -- 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    logger_name VARCHAR(100),
    message TEXT NOT NULL,
    module_name VARCHAR(100),
    function_name VARCHAR(100),
    line_number INTEGER,
    exception_info TEXT,
    extra_data JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id UUID
);

-- Data Pipeline Logs
CREATE TABLE IF NOT EXISTS logs.pipeline_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(100) NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    log_level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    execution_id UUID,
    step_duration_ms INTEGER,
    records_count INTEGER,
    error_details JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- MONITORING VIEWS FOR GRAFANA
-- =============================================================================

-- ETL Job Status Summary
CREATE OR REPLACE VIEW monitoring.etl_job_summary AS
SELECT 
    job_type,
    status,
    COUNT(*) as job_count,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(end_time) as last_execution,
    SUM(records_processed) as total_records_processed,
    SUM(records_failed) as total_records_failed
FROM monitoring.etl_jobs 
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY job_type, status;

-- Data Quality Dashboard View
CREATE OR REPLACE VIEW monitoring.data_quality_dashboard AS
SELECT 
    schema_name,
    table_name,
    metric_name,
    metric_value,
    threshold_value,
    is_passed,
    check_timestamp,
    CASE 
        WHEN is_passed THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM monitoring.data_quality_metrics
WHERE check_timestamp >= CURRENT_DATE - INTERVAL '24 hours'
ORDER BY check_timestamp DESC;

-- System Health View
CREATE OR REPLACE VIEW monitoring.system_health AS
SELECT 
    metric_type,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value,
    MIN(metric_value) as min_value,
    COUNT(*) as measurement_count,
    MAX(recorded_at) as last_recorded
FROM monitoring.performance_metrics
WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY metric_type;

-- =============================================================================
-- LOGGING FUNCTIONS
-- =============================================================================

-- Function to log ETL job execution
CREATE OR REPLACE FUNCTION monitoring.log_etl_job(
    p_job_name VARCHAR(100),
    p_job_type VARCHAR(50),
    p_status VARCHAR(20) DEFAULT 'running',
    p_records_processed INTEGER DEFAULT 0,
    p_records_failed INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    job_uuid UUID;
BEGIN
    INSERT INTO monitoring.etl_jobs (
        job_name, job_type, status, records_processed, 
        records_failed, error_message, metadata
    ) VALUES (
        p_job_name, p_job_type, p_status, p_records_processed,
        p_records_failed, p_error_message, p_metadata
    ) RETURNING job_id INTO job_uuid;
    
    RETURN job_uuid;
END;
$$ LANGUAGE plpgsql;

-- Function to update ETL job status
CREATE OR REPLACE FUNCTION monitoring.update_etl_job_status(
    p_job_id UUID,
    p_status VARCHAR(20),
    p_records_processed INTEGER DEFAULT NULL,
    p_records_failed INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE monitoring.etl_jobs 
    SET 
        status = p_status,
        end_time = CASE WHEN p_status IN ('completed', 'failed') THEN CURRENT_TIMESTAMP ELSE end_time END,
        duration_seconds = CASE 
            WHEN p_status IN ('completed', 'failed') 
            THEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))::INTEGER 
            ELSE duration_seconds 
        END,
        records_processed = COALESCE(p_records_processed, records_processed),
        records_failed = COALESCE(p_records_failed, records_failed),
        error_message = COALESCE(p_error_message, error_message)
    WHERE job_id = p_job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to log data quality metrics
CREATE OR REPLACE FUNCTION monitoring.log_data_quality(
    p_table_name VARCHAR(100),
    p_schema_name VARCHAR(50),
    p_metric_name VARCHAR(100),
    p_metric_value DECIMAL(15,4),
    p_threshold_value DECIMAL(15,4) DEFAULT NULL,
    p_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO monitoring.data_quality_metrics (
        table_name, schema_name, metric_name, metric_value, 
        threshold_value, is_passed, details
    ) VALUES (
        p_table_name, p_schema_name, p_metric_name, p_metric_value,
        p_threshold_value, 
        CASE 
            WHEN p_threshold_value IS NULL THEN TRUE
            WHEN p_metric_name LIKE '%_count' THEN p_metric_value <= p_threshold_value
            ELSE p_metric_value >= p_threshold_value
        END,
        p_details
    );
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- ETL Jobs indexes
CREATE INDEX IF NOT EXISTS idx_etl_jobs_type_status ON monitoring.etl_jobs(job_type, status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_start_time ON monitoring.etl_jobs(start_time);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_name ON monitoring.etl_jobs(job_name);

-- Data Quality indexes
CREATE INDEX IF NOT EXISTS idx_data_quality_table ON monitoring.data_quality_metrics(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_data_quality_timestamp ON monitoring.data_quality_metrics(check_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_quality_status ON monitoring.data_quality_metrics(is_passed);

-- Performance metrics indexes
CREATE INDEX IF NOT EXISTS idx_performance_type_time ON monitoring.performance_metrics(metric_type, recorded_at);

-- Logging indexes
CREATE INDEX IF NOT EXISTS idx_app_logs_level_time ON logs.application_logs(log_level, timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_name_time ON logs.pipeline_logs(pipeline_name, timestamp);

-- =============================================================================
-- PERMISSIONS
-- =============================================================================

-- Grant permissions for monitoring and logging
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA monitoring TO PUBLIC;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA logs TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA monitoring TO PUBLIC;

\echo 'Monitoring and logging infrastructure setup completed!'
\echo 'Created tables: etl_jobs, data_quality_metrics, performance_metrics, application_logs, pipeline_logs'
\echo 'Created views: etl_job_summary, data_quality_dashboard, system_health'
\echo 'Created functions: log_etl_job, update_etl_job_status, log_data_quality'

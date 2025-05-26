-- =============================================================================
-- MASTER POSTGRESQL INITIALIZATION SCRIPT
-- =============================================================================
-- This script runs automatically when PostgreSQL container starts
-- It creates all necessary databases, schemas, tables, and initial data
-- =============================================================================

-- Set client encoding and timezone
SET client_encoding = 'UTF8';
SET timezone = 'UTC';

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =============================================================================
-- CREATE ADDITIONAL DATABASES
-- =============================================================================

-- Create grafana database if it doesn't exist
SELECT 'CREATE DATABASE grafana'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'grafana')\gexec

-- Create monitoring database if it doesn't exist  
SELECT 'CREATE DATABASE monitoring'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'monitoring')\gexec

-- =============================================================================
-- CREATE SCHEMAS FOR DATA LAYERS
-- =============================================================================

-- Create schemas for data layers (Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS logs;

-- Set schema permissions
GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE ON SCHEMA silver TO PUBLIC;
GRANT USAGE ON SCHEMA gold TO PUBLIC;
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT USAGE ON SCHEMA monitoring TO PUBLIC;
GRANT USAGE ON SCHEMA logs TO PUBLIC;

-- =============================================================================
-- LOG INITIALIZATION
-- =============================================================================

\echo 'PostgreSQL Data Warehouse initialization started...'
\echo 'Creating schemas: bronze, silver, gold, analytics, monitoring, logs'
\echo 'Extensions enabled: uuid-ossp, pg_stat_statements, btree_gin, pg_trgm'
\echo 'Timezone set to UTC, encoding set to UTF8'
\echo ''
\echo 'Initialization completed successfully!'
\echo 'Ready to run additional initialization scripts...'

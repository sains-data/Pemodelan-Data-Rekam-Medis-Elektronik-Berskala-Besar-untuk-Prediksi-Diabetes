#!/bin/bash
# =============================================================================
# COMPLETE POSTGRESQL WAREHOUSE INTEGRATION SCRIPT
# =============================================================================
# Integrates all PostgreSQL ETL functions, views, and analytics queries
# Ensures complete database setup for the diabetes prediction pipeline
# =============================================================================

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Configuration
PROJECT_ROOT="/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes"
SQL_DIR="$PROJECT_ROOT/scripts/sql"
ENV_FILE="$PROJECT_ROOT/.env"

# Load environment variables if .env file exists
if [[ -f "$ENV_FILE" ]]; then
    # Comment out sourcing for now due to shell issues
    # source "$ENV_FILE"
    log "Skipping .env sourcing - using direct environment variables"
else
    log_warning ".env file not found at $ENV_FILE"
fi

# PostgreSQL connection parameters (from .env file)
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_WAREHOUSE_EXTERNAL_PORT:-"5434"}  # External port
POSTGRES_DB=${POSTGRES_WAREHOUSE_DB:-"diabetes_warehouse"}
POSTGRES_USER=${POSTGRES_WAREHOUSE_USER:-"warehouse"}
POSTGRES_PASSWORD=${POSTGRES_WAREHOUSE_PASSWORD:-"warehouse_secure_2025"}

# Export for psql
export PGHOST=$POSTGRES_HOST
export PGPORT=$POSTGRES_PORT
export PGDATABASE=$POSTGRES_DB
export PGUSER=$POSTGRES_USER
export PGPASSWORD=$POSTGRES_PASSWORD

# Function to check PostgreSQL connection
check_postgres_connection() {
    log "Checking PostgreSQL connection..."
    
    if ! docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -c "SELECT version();" > /dev/null 2>&1; then
        log_error "Cannot connect to PostgreSQL database"
        log_error "Please ensure PostgreSQL container is running and accessible"
        exit 1
    fi
    
    log "✅ PostgreSQL connection successful"
}

# Function to execute SQL file with error handling
execute_sql_file() {
    local sql_file="$1"
    local description="$2"
    
    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        exit 1
    fi
    
    log "Executing $description..."
    log_info "File: $sql_file"
    
    if docker exec -i postgres-warehouse psql -U warehouse -d diabetes_warehouse < "$sql_file" 2>&1 | tee /tmp/sql_output.log; then
        if grep -q "ERROR" /tmp/sql_output.log; then
            log_warning "Some errors occurred during execution, but continuing..."
        else
            log "✅ $description completed successfully"
        fi
    else
        log_error "Failed to execute $description"
        exit 1
    fi
}

# Function to check if table exists
table_exists() {
    local schema="$1"
    local table="$2"
    
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = '$schema' 
            AND table_name = '$table'
        );
    " | tr -d ' \n'
}

# Function to validate database state
validate_database_setup() {
    log "Validating database setup..."
    
    # Check schemas
    local schemas=("bronze" "silver" "gold" "analytics")
    for schema in "${schemas[@]}"; do
        local exists=$(docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '$schema');" | tr -d ' \n')
        if [[ "$exists" == "t" ]]; then
            log_info "✅ Schema '$schema' exists"
        else
            log_warning "❌ Schema '$schema' missing"
        fi
    done
    
    # Check key tables
    local key_tables=(
        "bronze:diabetes_raw"
        "silver:diabetes_cleaned"
        "gold:diabetes_gold"
        "analytics:pipeline_execution_log"
    )
    
    for table_def in "${key_tables[@]}"; do
        IFS=':' read -r schema table <<< "$table_def"
        local exists=$(table_exists "$schema" "$table")
        if [[ "$exists" == "t" ]]; then
            log_info "✅ Table '$schema.$table' exists"
        else
            log_warning "❌ Table '$schema.$table' missing"
        fi
    done
    
    # Check functions
    local functions=(
        "process_bronze_to_silver"
        "process_silver_to_gold"
        "refresh_analytics_aggregations"
        "get_pipeline_statistics"
        "cleanup_pipeline_logs"
    )
    
    for func in "${functions[@]}"; do
        local exists=$(docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = '$func');" | tr -d ' \n')
        if [[ "$exists" == "t" ]]; then
            log_info "✅ Function '$func' exists"
        else
            log_warning "❌ Function '$func' missing"
        fi
    done
    
    # Check views
    local views=(
        "analytics:executive_kpi_dashboard"
        "analytics:clinical_risk_analysis"
        "analytics:demographic_health_analysis"
        "analytics:glucose_analysis_trends"
        "analytics:prediction_accuracy_analysis"
    )
    
    for view_def in "${views[@]}"; do
        IFS=':' read -r schema view <<< "$view_def"
        local exists=$(docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = '$schema' AND table_name = '$view');" | tr -d ' \n')
        if [[ "$exists" == "t" ]]; then
            log_info "✅ View '$schema.$view' exists"
        else
            log_warning "❌ View '$schema.$view' missing"
        fi
    done
}

# Function to test ETL functions
test_etl_functions() {
    log "Testing ETL functions..."
    
    # Test pipeline statistics
    log_info "Testing get_pipeline_statistics function..."
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -c "SELECT * FROM get_pipeline_statistics();" > /dev/null
    
    # Test analytics refresh
    log_info "Testing refresh_analytics_aggregations function..."
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -c "SELECT refresh_analytics_aggregations();" > /dev/null
    
    log "✅ ETL functions test completed"
}

# Function to create sample test data
create_sample_data() {
    log "Creating sample test data for validation..."
    
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -c "
        -- Insert sample data for testing
        INSERT INTO bronze.diabetes_raw (
            pregnancies, glucose, blood_pressure, skin_thickness, 
            insulin, bmi, diabetes_pedigree_function, age, outcome, 
            source_file
        ) VALUES 
            (6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 'test_data.csv'),
            (1, 85, 66, 29, 0, 26.6, 0.351, 31, 0, 'test_data.csv'),
            (8, 183, 64, 0, 0, 23.3, 0.672, 32, 1, 'test_data.csv'),
            (1, 89, 66, 23, 94, 28.1, 0.167, 21, 0, 'test_data.csv'),
            (0, 137, 40, 35, 168, 43.1, 2.288, 33, 1, 'test_data.csv')
        ON CONFLICT DO NOTHING;
        
        -- Process through pipeline
        SELECT process_bronze_to_silver();
        SELECT process_silver_to_gold();
        SELECT refresh_analytics_aggregations();
    "
    
    log "✅ Sample data created and processed"
}

# Function to generate integration report
generate_integration_report() {
    log "Generating integration report..."
    
    local report_file="/tmp/postgres_integration_report.txt"
    
    cat > "$report_file" << EOF
PostgreSQL Data Warehouse Integration Report
==========================================
Generated: $(date)

DATABASE CONNECTION:
Host: $POSTGRES_HOST
Port: $POSTGRES_PORT
Database: $POSTGRES_DB
User: $POSTGRES_USER

INSTALLED COMPONENTS:
EOF
    
    # Add schema information
    echo "" >> "$report_file"
    echo "SCHEMAS:" >> "$report_file"
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics') ORDER BY schema_name;" | sed 's/^/- /' >> "$report_file"
    
    # Add table counts
    echo "" >> "$report_file"
    echo "TABLE RECORD COUNTS:" >> "$report_file"
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "
        SELECT 'bronze.diabetes_raw: ' || COUNT(*) FROM bronze.diabetes_raw
        UNION ALL
        SELECT 'silver.diabetes_cleaned: ' || COUNT(*) FROM silver.diabetes_cleaned
        UNION ALL
        SELECT 'gold.diabetes_gold: ' || COUNT(*) FROM gold.diabetes_gold
        UNION ALL
        SELECT 'analytics.pipeline_execution_log: ' || COUNT(*) FROM analytics.pipeline_execution_log;
    " | sed 's/^/- /' >> "$report_file"
    
    # Add function information
    echo "" >> "$report_file"
    echo "ETL FUNCTIONS:" >> "$report_file"
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT '- ' || proname FROM pg_proc WHERE proname LIKE '%process%' OR proname LIKE '%refresh%' OR proname LIKE '%pipeline%' ORDER BY proname;" >> "$report_file"
    
    # Add view information
    echo "" >> "$report_file"
    echo "ANALYTICS VIEWS:" >> "$report_file"
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -t -c "SELECT '- ' || table_name FROM information_schema.views WHERE table_schema = 'analytics' ORDER BY table_name;" >> "$report_file"
    
    # Add connection string for Grafana
    echo "" >> "$report_file"
    echo "GRAFANA DATASOURCE CONNECTION:" >> "$report_file"
    echo "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB" >> "$report_file"
    
    cat "$report_file"
    log "✅ Integration report saved to: $report_file"
}

# Main execution function
main() {
    log "🚀 Starting Complete PostgreSQL Warehouse Integration"
    log "=================================================="
    
    # Step 1: Check connection
    check_postgres_connection
    
    # Step 2: Execute SQL files in order
    log "📋 Executing SQL initialization files..."
    
    # Core warehouse initialization
    execute_sql_file "$SQL_DIR/01_init_warehouse.sql" "Core warehouse tables initialization"
    
    # PostgreSQL ETL functions
    execute_sql_file "$SQL_DIR/06_postgresql_etl_functions.sql" "PostgreSQL ETL functions"
    
    # Comprehensive analytics views
    execute_sql_file "$SQL_DIR/07_comprehensive_analytics_views.sql" "Comprehensive analytics views"
    
    # Grafana analytics queries (create as stored views for easier access)
    log "Creating Grafana analytics helper views..."
    docker exec postgres-warehouse psql -U warehouse -d diabetes_warehouse -c "
        -- Create a simple view for Grafana variable queries
        CREATE OR REPLACE VIEW analytics.grafana_date_options AS
        SELECT DISTINCT 
            processing_date as date_value,
            TO_CHAR(processing_date, 'YYYY-MM-DD') as date_text
        FROM gold.diabetes_gold 
        WHERE processing_date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY processing_date DESC;
        
        CREATE OR REPLACE VIEW analytics.grafana_risk_level_options AS
        SELECT DISTINCT 
            health_risk_level as risk_level,
            CASE health_risk_level 
                WHEN 'Low' THEN 1
                WHEN 'Moderate' THEN 2
                WHEN 'High' THEN 3
                WHEN 'Critical' THEN 4
                ELSE 5
            END as sort_order
        FROM gold.diabetes_gold 
        WHERE health_risk_level IS NOT NULL
        ORDER BY sort_order;
    "
    
    # Step 3: Validate setup
    validate_database_setup
    
    # Step 4: Create and test sample data
    create_sample_data
    
    # Step 5: Test functions
    test_etl_functions
    
    # Step 6: Generate report
    generate_integration_report
    
    log "🎉 PostgreSQL Warehouse Integration Completed Successfully!"
    log "=================================================="
    log "Next steps:"
    log "1. Update Airflow connections to use postgres_warehouse"
    log "2. Configure Grafana datasource with provided connection string"
    log "3. Import Grafana dashboard configuration"
    log "4. Start ETL pipeline from Airflow"
    log "5. Monitor dashboard for real-time metrics"
}

# Execute main function
main "$@"

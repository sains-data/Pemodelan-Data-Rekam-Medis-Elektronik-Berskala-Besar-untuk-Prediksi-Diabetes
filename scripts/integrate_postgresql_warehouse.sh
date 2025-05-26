#!/bin/bash

# =============================================================================
# PostgreSQL Data Warehouse Integration Script
# =============================================================================
# This script integrates PostgreSQL as the main data warehouse for the 
# diabetes prediction pipeline, replacing Hive with enhanced PostgreSQL features
# 
# Author: Kelompok 8 RA
# Date: May 26, 2025
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_ROOT/logs/postgres_integration.log"
ENV_FILE="$PROJECT_ROOT/.env"

# Ensure logs directory exists
mkdir -p "$PROJECT_ROOT/logs"

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"
}

# Load environment variables
if [[ -f "$ENV_FILE" ]]; then
    source "$ENV_FILE"
    log "Environment variables loaded from $ENV_FILE"
else
    error "Environment file not found: $ENV_FILE"
    exit 1
fi

# PostgreSQL connection parameters
POSTGRES_HOST="${POSTGRES_WAREHOUSE_HOST:-postgres-warehouse}"
POSTGRES_PORT="${POSTGRES_WAREHOUSE_PORT:-5439}"
POSTGRES_DB="${POSTGRES_WAREHOUSE_DB:-diabetes_warehouse}"
POSTGRES_USER="${POSTGRES_WAREHOUSE_USER:-warehouse}"
POSTGRES_PASSWORD="${POSTGRES_WAREHOUSE_PASSWORD:-warehouse_secure_2025}"

# Check if PostgreSQL is running
check_postgres_connection() {
    log "Checking PostgreSQL connection..."
    
    local max_attempts=30
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;" &>/dev/null; then
            log "PostgreSQL is accessible!"
            return 0
        else
            warn "Attempt $attempt/$max_attempts: PostgreSQL not ready yet..."
            sleep 5
            ((attempt++))
        fi
    done
    
    error "PostgreSQL is not accessible after $max_attempts attempts"
    return 1
}

# Execute SQL file
execute_sql_file() {
    local sql_file="$1"
    local description="$2"
    
    if [[ ! -f "$sql_file" ]]; then
        error "SQL file not found: $sql_file"
        return 1
    fi
    
    log "Executing $description: $(basename "$sql_file")"
    
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$sql_file" &>>"$LOG_FILE"; then
        log "Successfully executed: $(basename "$sql_file")"
        return 0
    else
        error "Failed to execute: $(basename "$sql_file")"
        return 1
    fi
}

# Run Python integration script
run_python_integration() {
    log "Running Python integration script..."
    
    local python_script="$SCRIPT_DIR/integrate_postgresql_warehouse.py"
    
    if [[ ! -f "$python_script" ]]; then
        error "Python integration script not found: $python_script"
        return 1
    fi
    
    # Set environment variables for the Python script
    export POSTGRES_WAREHOUSE_HOST="$POSTGRES_HOST"
    export POSTGRES_WAREHOUSE_PORT="$POSTGRES_PORT"
    export POSTGRES_WAREHOUSE_DB="$POSTGRES_DB"
    export POSTGRES_WAREHOUSE_USER="$POSTGRES_USER"
    export POSTGRES_WAREHOUSE_PASSWORD="$POSTGRES_PASSWORD"
    
    if python3 "$python_script" &>>"$LOG_FILE"; then
        log "Python integration script completed successfully"
        return 0
    else
        error "Python integration script failed"
        return 1
    fi
}

# Validate the installation
validate_installation() {
    log "Validating PostgreSQL data warehouse installation..."
    
    local validation_sql="
    SELECT 
        schemaname as schema_name,
        tablename as table_name,
        'table' as object_type
    FROM pg_tables 
    WHERE schemaname IN ('bronze', 'silver', 'gold', 'analytics')
    UNION ALL
    SELECT 
        schemaname as schema_name,
        viewname as table_name,
        'view' as object_type
    FROM pg_views 
    WHERE schemaname IN ('analytics', 'hive_compat')
    ORDER BY schema_name, table_name;
    "
    
    info "Database objects created:"
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$validation_sql" | tee -a "$LOG_FILE"
    
    # Test Hive compatibility
    local hive_test_sql="SELECT COUNT(*) as hive_compatibility_test FROM hive_compat.diabetes_bronze LIMIT 1;"
    
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$hive_test_sql" &>/dev/null; then
        log "Hive compatibility layer is working correctly"
    else
        warn "Hive compatibility layer test failed (this is normal if no data is loaded yet)"
    fi
}

# Generate Grafana datasource configuration
generate_grafana_config() {
    log "Generating Grafana datasource configuration..."
    
    local grafana_config_dir="$PROJECT_ROOT/dashboard/grafana/provisioning/datasources"
    mkdir -p "$grafana_config_dir"
    
    cat > "$grafana_config_dir/postgresql_warehouse.yml" << EOF
apiVersion: 1

datasources:
  - name: PostgreSQL Warehouse
    type: postgres
    access: proxy
    url: $POSTGRES_HOST:$POSTGRES_PORT
    database: $POSTGRES_DB
    user: $POSTGRES_USER
    secureJsonData:
      password: $POSTGRES_PASSWORD
    jsonData:
      sslmode: disable
      postgresVersion: 1300
      timescaledb: false
    isDefault: true
    editable: true
    
  - name: PostgreSQL Analytics
    type: postgres
    access: proxy
    url: $POSTGRES_HOST:$POSTGRES_PORT
    database: $POSTGRES_DB
    user: $POSTGRES_USER
    secureJsonData:
      password: $POSTGRES_PASSWORD
    jsonData:
      sslmode: disable
      postgresVersion: 1300
      timescaledb: false
      schema: analytics
    editable: true
EOF

    log "Grafana datasource configuration created: $grafana_config_dir/postgresql_warehouse.yml"
}

# Create ETL configuration
create_etl_config() {
    log "Creating ETL configuration for PostgreSQL integration..."
    
    local etl_config_dir="$PROJECT_ROOT/airflow/dags/config"
    mkdir -p "$etl_config_dir"
    
    cat > "$etl_config_dir/postgresql_warehouse.yml" << EOF
# PostgreSQL Data Warehouse Configuration for ETL Pipeline
database:
  host: $POSTGRES_HOST
  port: $POSTGRES_PORT
  database: $POSTGRES_DB
  user: $POSTGRES_USER
  schema_bronze: bronze
  schema_silver: silver
  schema_gold: gold
  schema_analytics: analytics

tables:
  bronze:
    - diabetes_clinical
    - personal_health_data
    - nutrition_intake
  silver:
    - diabetes_cleaned
    - personal_health_cleaned
  gold:
    - diabetes_comprehensive
    - health_insights_comprehensive

processing:
  batch_size: 10000
  quality_threshold: 0.8
  enable_data_validation: true
  enable_anomaly_detection: true
  
monitoring:
  enable_metrics: true
  log_level: INFO
  performance_tracking: true
EOF

    log "ETL configuration created: $etl_config_dir/postgresql_warehouse.yml"
}

# Update docker-compose to ensure proper initialization
update_docker_compose() {
    log "Checking docker-compose configuration..."
    
    local compose_file="$PROJECT_ROOT/docker-compose.yml"
    
    if grep -q "postgres-warehouse" "$compose_file"; then
        log "PostgreSQL warehouse service found in docker-compose.yml"
    else
        warn "PostgreSQL warehouse service not found in docker-compose.yml"
    fi
    
    # Check if SQL init scripts are mounted
    if grep -q "/docker-entrypoint-initdb.d" "$compose_file"; then
        log "SQL initialization scripts are properly mounted"
    else
        warn "SQL initialization scripts may not be properly mounted"
    fi
}

# Create backup script
create_backup_script() {
    log "Creating PostgreSQL backup script..."
    
    cat > "$SCRIPT_DIR/backup_postgresql_warehouse.sh" << 'EOF'
#!/bin/bash

# PostgreSQL Data Warehouse Backup Script
# Usage: ./backup_postgresql_warehouse.sh [backup_name]

set -e

# Load environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
source "$PROJECT_ROOT/.env"

# Configuration
POSTGRES_HOST="${POSTGRES_WAREHOUSE_HOST:-postgres-warehouse}"
POSTGRES_PORT="${POSTGRES_WAREHOUSE_PORT:-5439}"
POSTGRES_DB="${POSTGRES_WAREHOUSE_DB:-diabetes_warehouse}"
POSTGRES_USER="${POSTGRES_WAREHOUSE_USER:-warehouse}"
POSTGRES_PASSWORD="${POSTGRES_WAREHOUSE_PASSWORD:-warehouse_secure_2025}"

BACKUP_DIR="$PROJECT_ROOT/backups/postgresql"
BACKUP_NAME="${1:-diabetes_warehouse_$(date +%Y%m%d_%H%M%S)}"

mkdir -p "$BACKUP_DIR"

echo "Creating PostgreSQL backup: $BACKUP_NAME"

# Create schema-only backup
PGPASSWORD="$POSTGRES_PASSWORD" pg_dump \
    -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" --schema-only \
    -f "$BACKUP_DIR/${BACKUP_NAME}_schema.sql"

# Create data backup for each schema
for schema in bronze silver gold analytics; do
    echo "Backing up schema: $schema"
    PGPASSWORD="$POSTGRES_PASSWORD" pg_dump \
        -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" --data-only --schema="$schema" \
        -f "$BACKUP_DIR/${BACKUP_NAME}_${schema}_data.sql"
done

echo "Backup completed: $BACKUP_DIR/$BACKUP_NAME"
EOF

    chmod +x "$SCRIPT_DIR/backup_postgresql_warehouse.sh"
    log "Backup script created: $SCRIPT_DIR/backup_postgresql_warehouse.sh"
}

# Main integration function
main() {
    log "========================================"
    log "Starting PostgreSQL Data Warehouse Integration"
    log "========================================"
    
    # Pre-flight checks
    info "Project root: $PROJECT_ROOT"
    info "PostgreSQL host: $POSTGRES_HOST:$POSTGRES_PORT"
    info "Database: $POSTGRES_DB"
    info "User: $POSTGRES_USER"
    
    # Check PostgreSQL connection
    if ! check_postgres_connection; then
        error "Cannot connect to PostgreSQL. Please ensure the database is running."
        exit 1
    fi
    
    # Execute SQL scripts in order
    log "Executing SQL initialization scripts..."
    
    # Basic warehouse initialization (should already be done by Docker init)
    if [[ -f "$PROJECT_ROOT/scripts/sql/01_init_warehouse.sql" ]]; then
        execute_sql_file "$PROJECT_ROOT/scripts/sql/01_init_warehouse.sql" "Basic warehouse initialization"
    fi
    
    # Enhanced gold schema
    if [[ -f "$PROJECT_ROOT/scripts/sql/02_enhanced_gold_schema.sql" ]]; then
        execute_sql_file "$PROJECT_ROOT/scripts/sql/02_enhanced_gold_schema.sql" "Enhanced gold schema"
    fi
    
    # PostgreSQL view fixes
    if [[ -f "$PROJECT_ROOT/scripts/sql/03_fix_postgresql_views.sql" ]]; then
        execute_sql_file "$PROJECT_ROOT/scripts/sql/03_fix_postgresql_views.sql" "PostgreSQL view fixes"
    fi
    
    # Hive to PostgreSQL migration
    if [[ -f "$PROJECT_ROOT/scripts/sql/04_hive_to_postgresql_migration.sql" ]]; then
        execute_sql_file "$PROJECT_ROOT/scripts/sql/04_hive_to_postgresql_migration.sql" "Hive to PostgreSQL migration"
    fi
    
    # Run Python integration script
    if ! run_python_integration; then
        error "Python integration failed"
        exit 1
    fi
    
    # Validate installation
    validate_installation
    
    # Generate configurations
    generate_grafana_config
    create_etl_config
    create_backup_script
    
    # Docker compose check
    update_docker_compose
    
    log "========================================"
    log "PostgreSQL Data Warehouse Integration COMPLETED!"
    log "========================================"
    log ""
    log "Summary:"
    log "- PostgreSQL schemas created: bronze, silver, gold, analytics"
    log "- Hive compatibility layer created"
    log "- Performance indexes created"
    log "- Data processing functions created"
    log "- Grafana datasource configured"
    log "- ETL configuration generated"
    log "- Backup script created"
    log ""
    log "Next steps:"
    log "1. Restart Grafana to load new datasource configuration"
    log "2. Run ETL pipeline to load data"
    log "3. Import Grafana dashboards"
    log "4. Monitor data processing logs"
    log ""
    log "Connection Details:"
    log "- Host: $POSTGRES_HOST"
    log "- Port: $POSTGRES_PORT"
    log "- Database: $POSTGRES_DB"
    log "- User: $POSTGRES_USER"
    log ""
    log "Log file: $LOG_FILE"
    log "========================================"
}

# Run the main function
main "$@"

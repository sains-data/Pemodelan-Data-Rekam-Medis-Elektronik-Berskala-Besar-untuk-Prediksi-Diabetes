#!/bin/bash

# =============================================================================
# PostgreSQL Database Management Script
# =============================================================================
# This script provides utilities for managing the PostgreSQL data warehouse
# =============================================================================

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Load environment variables
if [[ -f "$ENV_FILE" ]]; then
    source "$ENV_FILE"
else
    echo "❌ Environment file not found: $ENV_FILE"
    exit 1
fi

# Database connection parameters
POSTGRES_HOST="${POSTGRES_WAREHOUSE_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_WAREHOUSE_PORT:-5432}"
POSTGRES_USER="${POSTGRES_WAREHOUSE_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_WAREHOUSE_PASSWORD:-password}"
POSTGRES_DB="${POSTGRES_WAREHOUSE_DB:-diabetes_warehouse}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Function to check if PostgreSQL is running
check_postgres_status() {
    log "Checking PostgreSQL status..."
    
    if docker ps | grep -q "postgres-warehouse"; then
        log "✅ PostgreSQL container is running"
        return 0
    else
        error "❌ PostgreSQL container is not running"
        return 1
    fi
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    log "Waiting for PostgreSQL to be ready..."
    
    local max_attempts=30
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        if docker exec postgres-warehouse pg_isready -h localhost -p 5432 -U "$POSTGRES_USER" > /dev/null 2>&1; then
            log "✅ PostgreSQL is ready!"
            return 0
        fi
        
        info "Attempt $attempt/$max_attempts - PostgreSQL not ready yet..."
        sleep 2
        ((attempt++))
    done
    
    error "❌ PostgreSQL failed to become ready after $max_attempts attempts"
    return 1
}

# Function to run SQL query
run_sql() {
    local sql="$1"
    local database="${2:-$POSTGRES_DB}"
    
    docker exec -i postgres-warehouse psql -h localhost -U "$POSTGRES_USER" -d "$database" -c "$sql"
}

# Function to run SQL file
run_sql_file() {
    local file="$1"
    local database="${2:-$POSTGRES_DB}"
    
    if [[ ! -f "$file" ]]; then
        error "SQL file not found: $file"
        return 1
    fi
    
    log "Executing SQL file: $(basename "$file")"
    docker exec -i postgres-warehouse psql -h localhost -U "$POSTGRES_USER" -d "$database" < "$file"
}

# Function to validate database setup
validate_database() {
    log "Validating database setup..."
    
    # Check schemas
    info "Checking schemas..."
    local schemas=$(run_sql "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics', 'monitoring', 'logs') ORDER BY schema_name;" | grep -E '^(bronze|silver|gold|analytics|monitoring|logs)$' | wc -l)
    
    if [[ $schemas -eq 6 ]]; then
        log "✅ All 6 required schemas exist"
    else
        warning "⚠️  Only $schemas/6 schemas found"
    fi
    
    # Check extensions
    info "Checking extensions..."
    local extensions=$(run_sql "SELECT extname FROM pg_extension WHERE extname IN ('uuid-ossp', 'pg_stat_statements', 'btree_gin', 'pg_trgm') ORDER BY extname;" | grep -E '^(uuid-ossp|pg_stat_statements|btree_gin|pg_trgm)$' | wc -l)
    
    if [[ $extensions -eq 4 ]]; then
        log "✅ All 4 required extensions installed"
    else
        warning "⚠️  Only $extensions/4 extensions found"
    fi
    
    # Check tables
    info "Checking table counts by schema..."
    run_sql "SELECT schemaname as schema_name, COUNT(*) as table_count FROM pg_tables WHERE schemaname IN ('bronze', 'silver', 'gold', 'analytics', 'monitoring', 'logs') GROUP BY schemaname ORDER BY schemaname;"
    
    log "✅ Database validation completed"
}

# Function to show database status
show_status() {
    log "Database Status Report"
    echo "======================"
    
    # Container status
    if check_postgres_status; then
        echo "Container Status: ✅ Running"
    else
        echo "Container Status: ❌ Stopped"
        return 1
    fi
    
    # Connection test
    if wait_for_postgres; then
        echo "Database Connection: ✅ Available"
    else
        echo "Database Connection: ❌ Failed"
        return 1
    fi
    
    # Database info
    echo ""
    echo "Database Information:"
    echo "Host: $POSTGRES_HOST"
    echo "Port: $POSTGRES_PORT" 
    echo "Database: $POSTGRES_DB"
    echo "User: $POSTGRES_USER"
    
    # Version info
    echo ""
    echo "PostgreSQL Version:"
    run_sql "SELECT version();"
    
    # Data summary
    echo ""
    echo "Data Summary:"
    run_sql "SELECT 'Bronze records: ' || COUNT(*)::text FROM bronze.diabetes_raw;" 2>/dev/null || echo "Bronze layer: Not accessible"
    run_sql "SELECT 'Silver records: ' || COUNT(*)::text FROM silver.diabetes_cleaned;" 2>/dev/null || echo "Silver layer: Not accessible"
    run_sql "SELECT 'Gold records: ' || COUNT(*)::text FROM gold.diabetes_gold;" 2>/dev/null || echo "Gold layer: Not accessible"
}

# Function to reset database
reset_database() {
    warning "⚠️  This will completely reset the database!"
    read -p "Are you sure you want to continue? (yes/no): " -r
    
    if [[ $REPLY != "yes" ]]; then
        info "Database reset cancelled"
        return 0
    fi
    
    log "Resetting database..."
    
    # Stop container
    info "Stopping PostgreSQL container..."
    docker stop postgres-warehouse || true
    
    # Remove container and volume
    info "Removing container and data volume..."
    docker rm postgres-warehouse || true
    docker volume rm "$(docker-compose config | grep postgres_warehouse_data | awk '{print $1}' | sed 's/://')" || true
    
    # Restart container
    info "Starting fresh PostgreSQL container..."
    docker-compose up -d postgres-warehouse
    
    # Wait for it to be ready
    wait_for_postgres
    
    log "✅ Database reset completed"
}

# Function to backup database
backup_database() {
    local backup_dir="$PROJECT_ROOT/backups"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="$backup_dir/postgres_warehouse_backup_$timestamp.sql"
    
    log "Creating database backup..."
    
    # Create backup directory
    mkdir -p "$backup_dir"
    
    # Create backup
    docker exec postgres-warehouse pg_dump -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" > "$backup_file"
    
    # Compress backup
    gzip "$backup_file"
    
    log "✅ Database backup created: ${backup_file}.gz"
}

# Function to restore database
restore_database() {
    local backup_file="$1"
    
    if [[ ! -f "$backup_file" ]]; then
        error "Backup file not found: $backup_file"
        return 1
    fi
    
    log "Restoring database from backup: $backup_file"
    
    # Check if file is compressed
    if [[ "$backup_file" == *.gz ]]; then
        gunzip -c "$backup_file" | docker exec -i postgres-warehouse psql -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB"
    else
        docker exec -i postgres-warehouse psql -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$backup_file"
    fi
    
    log "✅ Database restore completed"
}

# Function to run maintenance
run_maintenance() {
    log "Running database maintenance..."
    
    # Update statistics
    info "Updating table statistics..."
    run_sql "ANALYZE;"
    
    # Vacuum tables
    info "Vacuuming tables..."
    run_sql "VACUUM ANALYZE;"
    
    # Check for unused indexes
    info "Checking index usage..."
    run_sql "SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch FROM pg_stat_user_indexes WHERE idx_tup_read = 0 AND idx_tup_fetch = 0 ORDER BY schemaname, tablename;"
    
    log "✅ Database maintenance completed"
}

# Function to show help
show_help() {
    echo "PostgreSQL Database Management Script"
    echo "====================================="
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  status      - Show database status and information"
    echo "  validate    - Validate database setup and configuration"
    echo "  reset       - Reset database (WARNING: destroys all data)"
    echo "  backup      - Create database backup"
    echo "  restore     - Restore database from backup file"
    echo "  maintenance - Run database maintenance tasks"
    echo "  logs        - Show PostgreSQL container logs"
    echo "  help        - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 status"
    echo "  $0 validate"
    echo "  $0 backup"
    echo "  $0 restore /path/to/backup.sql.gz"
}

# Main script logic
case "${1:-help}" in
    "status")
        show_status
        ;;
    "validate")
        check_postgres_status && wait_for_postgres && validate_database
        ;;
    "reset")
        reset_database
        ;;
    "backup")
        check_postgres_status && wait_for_postgres && backup_database
        ;;
    "restore")
        if [[ -z "${2:-}" ]]; then
            error "Please specify backup file path"
            exit 1
        fi
        check_postgres_status && wait_for_postgres && restore_database "$2"
        ;;
    "maintenance")
        check_postgres_status && wait_for_postgres && run_maintenance
        ;;
    "logs")
        docker logs postgres-warehouse --tail=100 -f
        ;;
    "help"|"--help"|"-h")
        show_help
        ;;
    *)
        error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac

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

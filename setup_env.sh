#!/bin/bash

# =============================================================================
# DIABETES PREDICTION PIPELINE - ENVIRONMENT SETUP SCRIPT
# =============================================================================
# Project: Big Data Pipeline for Diabetes Prediction
# Team: Kelompok 8 RA
# Environment: Production-Ready Setup
# Last Updated: May 26, 2025
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to load environment variables
load_env() {
    if [ -f .env ]; then
        export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
        print_status "Environment variables loaded from .env"
    else
        print_error ".env file not found!"
        exit 1
    fi
}

# Function to validate environment variables
validate_environment() {
    print_header "🔍 Validating Environment Configuration"
    
    # Check if Python validation script exists and run it
    if [ -f "scripts/validate_env.py" ]; then
        python scripts/validate_env.py
        if [ $? -ne 0 ]; then
            print_error "Environment validation failed!"
            exit 1
        fi
    else
        print_warning "Environment validation script not found, skipping..."
    fi
}

# Function to check prerequisites
check_prerequisites() {
    print_header "🔍 Checking Prerequisites"
    
    # Check Docker
    if ! command_exists docker; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    print_status "Docker is installed: $(docker --version)"
    
    # Check Docker Compose
    if ! command_exists "docker compose"; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    print_status "Docker Compose is installed: $(docker compose version)"
    
    # Check Python
    if ! command_exists python && ! command_exists python3; then
        print_error "Python is not installed. Please install Python first."
        exit 1
    fi
    
    PYTHON_CMD="python3"
    if command_exists python3; then
        PYTHON_CMD="python3"
    elif command_exists python; then
        PYTHON_CMD="python"
    fi
    print_status "Python is installed: $($PYTHON_CMD --version)"
    
    # Check Make
    if ! command_exists make; then
        print_warning "Make is not installed. Some convenience commands may not work."
    else
        print_status "Make is installed: $(make --version | head -n1)"
    fi
}

# Function to setup project structure
setup_project_structure() {
    print_header "📁 Setting up Project Structure"
    
    # Create directories
    directories=(
        "data/bronze"
        "data/silver"
        "data/gold"
        "data/logs"
        "data/backup"
        "airflow/logs"
        "airflow/plugins"
        "models"
        "logs"
    )
    
    for dir in "${directories[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_status "Created directory: $dir"
        else
            print_status "Directory already exists: $dir"
        fi
    done
}

# Function to validate Docker Compose configuration
validate_docker_config() {
    print_header "🐳 Validating Docker Compose Configuration"
    
    if docker compose config --quiet; then
        print_status "Docker Compose configuration is valid"
    else
        print_error "Docker Compose configuration is invalid!"
        exit 1
    fi
}

# Function to create environment-specific configurations
setup_environment_configs() {
    print_header "⚙️ Setting up Environment-specific Configurations"
    
    # Set environment-specific settings based on ENVIRONMENT variable
    case "${ENVIRONMENT:-development}" in
        "production")
            print_status "Setting up production environment..."
            # Production-specific settings can be added here
            ;;
        "staging")
            print_status "Setting up staging environment..."
            # Staging-specific settings can be added here
            ;;
        "development"|*)
            print_status "Setting up development environment..."
            # Development-specific settings can be added here
            ;;
    esac
}

# Function to pull Docker images
pull_docker_images() {
    print_header "🐳 Pulling Docker Images"
    
    if docker compose pull; then
        print_status "All Docker images pulled successfully"
    else
        print_warning "Some Docker images failed to pull, will build locally"
    fi
}

# Function to build custom images
build_custom_images() {
    print_header "🔨 Building Custom Docker Images"
    
    if docker compose build; then
        print_status "All custom images built successfully"
    else
        print_error "Failed to build custom images!"
        exit 1
    fi
}

# Function to start services
start_services() {
    print_header "🚀 Starting Services"
    
    print_status "Starting all services with environment: ${ENVIRONMENT:-development}"
    print_status "Project: ${PROJECT_NAME:-diabetes-prediction-pipeline} v${PROJECT_VERSION:-1.0.0}"
    
    if docker compose up -d; then
        print_status "All services started successfully!"
    else
        print_error "Failed to start services!"
        exit 1
    fi
}

# Function to wait for services to be ready
wait_for_services() {
    print_header "⏳ Waiting for Services to be Ready"
    
    # Define services and their health check URLs
    declare -A services=(
        ["HDFS NameNode"]="http://localhost:${HDFS_NAMENODE_HTTP_PORT:-9870}"
        ["Spark Master"]="http://localhost:${SPARK_MASTER_WEBUI_PORT:-8081}"
        ["Grafana"]="http://localhost:${GRAFANA_PORT:-3000}/api/health"
        ["Prometheus"]="http://localhost:${PROMETHEUS_PORT:-9090}/-/healthy"
    )
    
    # Wait for services
    for service in "${!services[@]}"; do
        url="${services[$service]}"
        print_status "Waiting for $service..."
        
        for i in {1..30}; do
            if curl -f "$url" >/dev/null 2>&1; then
                print_status "$service is ready!"
                break
            fi
            
            if [ $i -eq 30 ]; then
                print_warning "$service is not responding after 5 minutes"
            else
                sleep 10
            fi
        done
    done
}

# Function to setup initial data
setup_initial_data() {
    print_header "📊 Setting up Initial Data"
    
    # Wait a bit more for HDFS to be fully ready
    sleep 20
    
    # Setup HDFS directories and copy data
    print_status "Creating HDFS directories..."
    docker compose exec namenode hdfs dfs -mkdir -p /data/bronze 2>/dev/null || true
    docker compose exec namenode hdfs dfs -mkdir -p /data/silver 2>/dev/null || true
    docker compose exec namenode hdfs dfs -mkdir -p /data/gold 2>/dev/null || true
    docker compose exec namenode hdfs dfs -mkdir -p /spark-logs 2>/dev/null || true
    
    # Copy sample data if it exists
    if [ -f "data/diabetes.csv" ]; then
        print_status "Copying diabetes.csv to HDFS..."
        docker compose exec namenode hdfs dfs -put -f /opt/data/diabetes.csv /data/bronze/ 2>/dev/null || true
    fi
    
    if [ -f "data/personal_health_data.csv" ]; then
        print_status "Copying personal_health_data.csv to HDFS..."
        docker compose exec namenode hdfs dfs -put -f /opt/data/personal_health_data.csv /data/bronze/ 2>/dev/null || true
    fi
    
    print_status "Initial data setup completed"
}

# Function to display service URLs
display_service_urls() {
    print_header "🌐 Service URLs"
    
    echo "Access your services at:"
    echo "  📊 Airflow UI:      http://localhost:${AIRFLOW_WEBSERVER_PORT:-8089}"
    echo "  ⚡ Spark Master:    http://localhost:${SPARK_MASTER_WEBUI_PORT:-8081}"
    echo "  ⚡ Spark Worker:    http://localhost:${SPARK_WORKER_WEBUI_PORT:-8082}"
    echo "  📁 HDFS UI:         http://localhost:${HDFS_NAMENODE_HTTP_PORT:-9870}"
    echo "  📈 Grafana:         http://localhost:${GRAFANA_PORT:-3000}"
    echo "  📊 Prometheus:      http://localhost:${PROMETHEUS_PORT:-9090}"
    echo "  🔄 NiFi:            http://localhost:${NIFI_WEB_HTTP_PORT:-8080}/nifi"
    echo ""
    echo "Default credentials:"
    echo "  📊 Airflow:         admin / admin"
    echo "  📈 Grafana:         ${GF_SECURITY_ADMIN_USER:-admin} / ${GF_SECURITY_ADMIN_PASSWORD:-admin}"
    echo "  🔄 NiFi:            ${SINGLE_USER_CREDENTIALS_USERNAME:-admin} / [see .env file]"
}

# Function to run post-setup validation
post_setup_validation() {
    print_header "✅ Post-Setup Validation"
    
    # Check if services are running
    if docker compose ps | grep -q "Up"; then
        print_status "Services are running"
    else
        print_warning "Some services may not be running properly"
    fi
    
    # Run validation script again if available
    if [ -f "scripts/validate_env.py" ]; then
        print_status "Running final environment validation..."
        python scripts/validate_env.py
    fi
}

# Main function
main() {
    print_header "🚀 Diabetes Prediction Pipeline - Environment Setup"
    print_header "=================================================="
    
    # Change to project directory if needed
    cd "$(dirname "$0")"
    
    # Load environment variables
    load_env
    
    # Run setup steps
    check_prerequisites
    validate_environment
    setup_project_structure
    validate_docker_config
    setup_environment_configs
    pull_docker_images
    build_custom_images
    start_services
    wait_for_services
    setup_initial_data
    post_setup_validation
    
    # Display final information
    display_service_urls
    
    print_header "🎉 Setup Complete!"
    print_status "Your diabetes prediction pipeline is ready!"
    print_status "Use 'make help' to see available commands"
}

# Run main function
main "$@"

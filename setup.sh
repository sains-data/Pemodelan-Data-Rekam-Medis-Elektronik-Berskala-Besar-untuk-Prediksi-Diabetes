#!/bin/bash

# Diabetes Prediction Pipeline - Complete Setup Script
# This script sets up the entire pipeline from scratch

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to wait for service to be ready
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=60
    local attempt=0

    print_status "Waiting for $service_name to be ready on $host:$port..."
    
    while [ $attempt -lt $max_attempts ]; do
        if timeout 1 bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null; then
            print_success "$service_name is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done
    
    print_error "$service_name failed to start within expected time"
    return 1
}

# Main setup function
main() {
    echo "🩺 Diabetes Prediction Pipeline Setup"
    echo "===================================="
    echo ""
    
    # Check prerequisites
    print_status "Checking prerequisites..."
    
    if ! command_exists docker; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command_exists docker-compose; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check available memory
    available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7/1024}')
    if [ "$available_memory" -lt 6 ]; then
        print_warning "Available memory is ${available_memory}GB. Recommended: 8GB+"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    print_success "Prerequisites check passed"
    echo ""
    
    # Clean up any existing containers
    print_status "Cleaning up existing containers..."
    make clean 2>/dev/null || true
    
    # Initialize project structure
    print_status "Initializing project structure..."
    make init
    
    # Build Docker images
    print_status "Building Docker images..."
    make build
    
    if [ $? -ne 0 ]; then
        print_error "Failed to build Docker images"
        exit 1
    fi
    
    # Start services
    print_status "Starting all services..."
    make up
    
    if [ $? -ne 0 ]; then
        print_error "Failed to start services"
        exit 1
    fi
    
    # Wait for services to be ready
    print_status "Waiting for services to be ready..."
    
    wait_for_service "localhost" "9870" "HDFS NameNode"
    wait_for_service "localhost" "8081" "Spark Master"
    wait_for_service "localhost" "8089" "Airflow"
    wait_for_service "localhost" "3000" "Grafana"
    wait_for_service "localhost" "9090" "Prometheus"
    
    # Setup HDFS directories and data
    print_status "Setting up HDFS directories and uploading data..."
    sleep 10  # Give HDFS more time to fully initialize
    make setup-data
    
    # Install dependencies
    print_status "Installing Python dependencies..."
    make install-deps
    
    # Run tests
    print_status "Running system tests..."
    sleep 5
    python3 scripts/test_pipeline.py
    
    # Final status check
    print_status "Checking final service status..."
    make status
    
    echo ""
    print_success "🎉 Setup completed successfully!"
    echo ""
    echo "📊 Access Points:"
    echo "  • Airflow UI:     http://localhost:8089 (admin/admin)"
    echo "  • Spark Master:   http://localhost:8081"
    echo "  • HDFS NameNode:  http://localhost:9870"
    echo "  • Grafana:        http://localhost:3000 (admin/admin)"
    echo "  • NiFi:           http://localhost:8080/nifi"
    echo "  • Prometheus:     http://localhost:9090"
    echo ""
    echo "🚀 Next Steps:"
    echo "  1. Open Airflow UI and enable the 'diabetes_etl_pipeline' DAG"
    echo "  2. Trigger the pipeline: make ingest-data"
    echo "  3. Monitor progress in Airflow and view results in Grafana"
    echo ""
    echo "📖 For help: make help"
    echo "🧪 Run tests: make test-local"
    echo "📊 Open dashboards: make monitoring"
}

# Run main function
main "$@"

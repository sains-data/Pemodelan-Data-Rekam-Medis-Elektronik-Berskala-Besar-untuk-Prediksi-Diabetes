#!/bin/bash
# Pipeline Test Runner - Comprehensive testing for diabetes prediction pipeline
# This script provides a complete testing workflow for CI/CD integration

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TEST_DIR="$PROJECT_DIR/tests"
REPORT_DIR="$PROJECT_DIR/test_reports"
LOG_FILE="$REPORT_DIR/test_execution.log"

# Test configuration
PYTHON_CMD="python3"
WAIT_TIME=30
MAX_RETRIES=3

# Functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

# Create necessary directories
setup_directories() {
    log "Setting up test directories..."
    mkdir -p "$REPORT_DIR"
    mkdir -p "$REPORT_DIR/logs"
    mkdir -p "$REPORT_DIR/coverage"
    
    # Initialize log file
    echo "Test Execution Log - $(date)" > "$LOG_FILE"
    echo "=======================================" >> "$LOG_FILE"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Python
    if ! command -v $PYTHON_CMD &> /dev/null; then
        error "Python3 is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Check test directory
    if [ ! -d "$TEST_DIR" ]; then
        error "Test directory not found: $TEST_DIR"
        exit 1
    fi
    
    log "✅ All prerequisites satisfied"
}

# Install test dependencies
install_dependencies() {
    log "Installing test dependencies..."
    
    # Install Python packages
    $PYTHON_CMD -m pip install --quiet --upgrade pip
    $PYTHON_CMD -m pip install --quiet \
        pytest \
        pytest-html \
        pytest-cov \
        pytest-xdist \
        requests \
        pandas \
        docker \
        unittest-xml-reporting
    
    log "✅ Dependencies installed"
}

# Wait for services to be ready
wait_for_services() {
    log "Waiting for services to be ready..."
    
    local services=(
        "http://localhost:9870"     # HDFS NameNode
        "http://localhost:8081"     # Spark Master
        "http://localhost:8089"     # Airflow
        "http://localhost:3000"     # Grafana
        "http://localhost:9090"     # Prometheus
    )
    
    for service in "${services[@]}"; do
        local retries=0
        while [ $retries -lt $MAX_RETRIES ]; do
            if curl -f "$service" >/dev/null 2>&1; then
                log "✅ Service ready: $service"
                break
            else
                retries=$((retries + 1))
                if [ $retries -lt $MAX_RETRIES ]; then
                    warn "Service not ready (attempt $retries/$MAX_RETRIES): $service"
                    sleep $WAIT_TIME
                else
                    warn "⚠️  Service not responding after $MAX_RETRIES attempts: $service"
                fi
            fi
        done
    done
    
    log "✅ Service readiness check completed"
}

# Run specific test type
run_test_type() {
    local test_type="$1"
    local output_file="$REPORT_DIR/test_${test_type}_$(date +%Y%m%d_%H%M%S).xml"
    
    log "Running $test_type tests..."
    
    cd "$TEST_DIR"
    
    if $PYTHON_CMD run_tests.py --types "$test_type" --output-dir "../$REPORT_DIR" 2>&1 | tee -a "$LOG_FILE"; then
        log "✅ $test_type tests completed successfully"
        return 0
    else
        error "❌ $test_type tests failed"
        return 1
    fi
}

# Generate coverage report
generate_coverage() {
    log "Generating test coverage report..."
    
    cd "$TEST_DIR"
    
    # Run tests with coverage
    $PYTHON_CMD -m pytest \
        --cov=. \
        --cov-report=html:"../$REPORT_DIR/coverage/html" \
        --cov-report=xml:"../$REPORT_DIR/coverage/coverage.xml" \
        --cov-report=term-missing \
        . 2>&1 | tee -a "$LOG_FILE"
    
    log "✅ Coverage report generated"
}

# Run performance benchmarks
run_benchmarks() {
    log "Running performance benchmarks..."
    
    cd "$TEST_DIR"
    
    # Run performance tests
    if $PYTHON_CMD -m unittest test_performance.TestPerformance -v 2>&1 | tee -a "$LOG_FILE"; then
        log "✅ Performance benchmarks completed"
        return 0
    else
        warn "⚠️  Some performance benchmarks failed"
        return 1
    fi
}

# Validate test results
validate_results() {
    log "Validating test results..."
    
    local failed=0
    
    # Check for test report files
    if ls "$REPORT_DIR"/test_report_*.html 1> /dev/null 2>&1; then
        log "✅ HTML test reports found"
    else
        error "❌ No HTML test reports found"
        failed=1
    fi
    
    # Check for JSON results
    if ls "$REPORT_DIR"/test_results_*.json 1> /dev/null 2>&1; then
        log "✅ JSON test results found"
    else
        error "❌ No JSON test results found"
        failed=1
    fi
    
    return $failed
}

# Generate summary report
generate_summary() {
    log "Generating test summary..."
    
    local summary_file="$REPORT_DIR/test_summary_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$summary_file" << EOF
# Diabetes Prediction Pipeline - Test Summary

**Generated:** $(date)
**Pipeline:** Diabetes Prediction Data Engineering Pipeline
**Environment:** $(docker-compose ps 2>/dev/null | wc -l) services running

## Test Execution Summary

### Test Types Executed
- Unit Tests
- Integration Tests
- End-to-End Tests
- Performance Tests
- Coverage Analysis

### Test Reports Available
- HTML Reports: \`test_reports/test_report_*.html\`
- JSON Results: \`test_reports/test_results_*.json\`
- Coverage Report: \`test_reports/coverage/html/index.html\`
- Execution Log: \`test_reports/test_execution.log\`

### Service Status
\`\`\`
$(docker-compose ps 2>/dev/null || echo "Docker services status unavailable")
\`\`\`

### Next Steps
1. Review test reports for any failures
2. Check coverage report for missing test coverage
3. Address any performance issues identified
4. Deploy if all tests pass successfully

---
*Generated by Pipeline Test Runner*
EOF

    log "✅ Test summary generated: $summary_file"
}

# Cleanup function
cleanup() {
    log "Performing cleanup..."
    
    # Remove temporary files
    find "$REPORT_DIR" -name "*.tmp" -delete 2>/dev/null || true
    
    # Compress old logs
    find "$REPORT_DIR" -name "test_execution_*.log" -mtime +7 -exec gzip {} \; 2>/dev/null || true
    
    log "✅ Cleanup completed"
}

# Main execution function
main() {
    local test_types=("$@")
    
    # Default to all test types if none specified
    if [ ${#test_types[@]} -eq 0 ]; then
        test_types=("unit" "integration" "e2e")
    fi
    
    local start_time=$(date +%s)
    local failed_tests=0
    
    log "🚀 Starting comprehensive test execution..."
    log "Test types: ${test_types[*]}"
    
    # Setup
    setup_directories
    check_prerequisites
    install_dependencies
    
    # Wait for services if running integration/e2e tests
    if [[ " ${test_types[*]} " =~ " integration " ]] || [[ " ${test_types[*]} " =~ " e2e " ]]; then
        wait_for_services
    fi
    
    # Run tests
    for test_type in "${test_types[@]}"; do
        if ! run_test_type "$test_type"; then
            failed_tests=$((failed_tests + 1))
        fi
    done
    
    # Additional analysis
    if [[ " ${test_types[*]} " =~ " unit " ]] || [[ " ${test_types[*]} " =~ " integration " ]]; then
        generate_coverage
    fi
    
    if [[ " ${test_types[*]} " =~ " e2e " ]]; then
        run_benchmarks
    fi
    
    # Validation and reporting
    validate_results
    generate_summary
    cleanup
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    log "🎉 Test execution completed in ${duration}s"
    
    if [ $failed_tests -eq 0 ]; then
        log "✅ All tests passed successfully!"
        exit 0
    else
        error "❌ $failed_tests test type(s) failed"
        exit 1
    fi
}

# Help function
show_help() {
    echo "Pipeline Test Runner"
    echo "==================="
    echo ""
    echo "Usage: $0 [test_types...]"
    echo ""
    echo "Test Types:"
    echo "  unit         - Run unit tests only"
    echo "  integration  - Run integration tests only"
    echo "  e2e          - Run end-to-end tests only"
    echo "  all          - Run all test types (default)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests"
    echo "  $0 unit               # Run unit tests only"
    echo "  $0 unit integration   # Run unit and integration tests"
    echo "  $0 e2e                # Run end-to-end tests only"
    echo ""
    echo "Environment Variables:"
    echo "  PYTHON_CMD     - Python command to use (default: python3)"
    echo "  WAIT_TIME      - Time to wait between service checks (default: 30s)"
    echo "  MAX_RETRIES    - Maximum retries for service checks (default: 3)"
    echo ""
    echo "Output:"
    echo "  - Test reports will be generated in: $REPORT_DIR"
    echo "  - Execution log will be saved to: $REPORT_DIR/test_execution.log"
}

# Parse command line arguments
case "${1:-}" in
    -h|--help|help)
        show_help
        exit 0
        ;;
    *)
        main "$@"
        ;;
esac

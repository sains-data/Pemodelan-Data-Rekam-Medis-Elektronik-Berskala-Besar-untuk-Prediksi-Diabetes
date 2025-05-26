# Makefile for Diabetes Prediction Data Engineering Pipeline
# Kelompok 8 RA - Big Data Project

# Load environment variables from .env file
include .env
export

# Variables from environment
PROJECT_NAME?=diabetes-prediction-pipeline
DOCKER_COMPOSE_FILE=docker-compose.yml
DOCKER_COMPOSE_CMD=docker compose

.PHONY: help init build up down clean logs restart status test backup validate-env dev setup-data train-model ingest-data health-check monitor config-check env-info setup health-check monitor config-check env-info

# Default target
help: ## Show this help message
	@echo "Diabetes Prediction Data Engineering Pipeline"
	@echo "============================================="
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

validate-env: ## Validate environment configuration
	@echo "🔍 Validating environment configuration..."
	@python scripts/validate_env.py

init: ## Initialize project directories and configurations
	@echo "🚀 Initializing project structure..."
	@mkdir -p data/{bronze,silver,gold,logs,backup}
	@mkdir -p scripts/{ingestion,transform,ml}
	@mkdir -p airflow/{dags,plugins,logs}
	@mkdir -p hive/ddl
	@mkdir -p dashboard/{grafana,prometheus}
	@mkdir -p models
	@mkdir -p docker/{spark,airflow,nifi,grafana}
	@echo "✅ Project structure initialized"

build: validate-env ## Build all Docker images
	@echo "🔨 Building Docker images..."
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) build
	@echo "✅ Build completed"

up: validate-env ## Start all services (with environment validation)
	@echo "🚀 Starting all services..."
	@echo "Environment: $(ENVIRONMENT)"
	@echo "Project: $(PROJECT_NAME) v$(PROJECT_VERSION)"
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 10
	@echo "✅ All services started successfully!"
	@echo ""
	@echo "🌐 Service URLs:"
	@echo "   - Airflow UI:     http://localhost:$(AIRFLOW_WEBSERVER_PORT)"
	@echo "   - Spark Master:   http://localhost:$(SPARK_MASTER_WEBUI_PORT)"
	@echo "   - Spark Worker:   http://localhost:$(SPARK_WORKER_WEBUI_PORT)"
	@echo "   - HDFS UI:        http://localhost:$(HDFS_NAMENODE_HTTP_PORT)"
	@echo "   - Grafana:        http://localhost:$(GRAFANA_PORT)"
	@echo "   - NiFi:           http://localhost:$(NIFI_WEB_HTTP_PORT)/nifi"
	@echo "   - Prometheus:     http://localhost:$(PROMETHEUS_PORT)"

dev: validate-env up setup-data ## Start development environment with data setup
	@echo "🔧 Development environment ready!"
	@echo "📊 Access the pipeline at: http://localhost:$(AIRFLOW_WEBSERVER_PORT)"

down: ## Stop all services
	@echo "🛑 Stopping all services..."
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) down
	@echo "✅ All services stopped"

restart: down up ## Restart all services

status: ## Check service status
	@echo "📊 Service Status:"
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) ps

logs: ## Show logs for all services
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) logs -f

logs-airflow: ## Show Airflow logs
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) logs -f airflow

logs-spark: ## Show Spark logs
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) logs -f spark-master spark-worker

logs-hdfs: ## Show HDFS logs
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) logs -f namenode datanode

clean: ## Clean up Docker containers, volumes, and networks
	@echo "🧹 Cleaning up..."
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) down -v --remove-orphans
	@docker system prune -f
	@echo "✅ Cleanup completed"

test: ## Run data quality tests
	@echo "🧪 Running data quality tests..."
	@$(DOCKER_COMPOSE_CMD) -f $(DOCKER_COMPOSE_FILE) exec airflow python /opt/airflow/scripts/test_pipeline.py
	@echo "✅ Tests completed"

test-local: ## Run tests locally (services must be running)
	@echo "🧪 Running local pipeline tests..."
	@python3 scripts/test_pipeline.py

ingest-data: ## Run data ingestion
	@echo "📥 Starting data ingestion..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec airflow airflow dags trigger diabetes_pipeline
	@echo "✅ Data ingestion triggered"

train-model: ## Train ML model
	@echo "🤖 Training ML model..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec spark-master python /opt/spark/scripts/train_diabetes_model.py
	@echo "✅ Model training completed"

backup: ## Backup data and models
	@echo "💾 Creating backup..."
	@mkdir -p data/backup/$(shell date +%Y%m%d_%H%M%S)
	@docker cp diabetes-prediction-pipeline_spark-master_1:/opt/spark/models data/backup/$(shell date +%Y%m%d_%H%M%S)/
	@echo "✅ Backup created"

setup-data: ## Setup initial data in HDFS
	@echo "📂 Setting up initial data in HDFS..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/bronze
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/silver
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/gold
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec namenode hdfs dfs -put /opt/data/diabetes.csv /data/bronze/
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec namenode hdfs dfs -put /opt/data/personal_health_data.csv /data/bronze/
	@echo "✅ Initial data setup completed"

install-deps: ## Install Python dependencies in Spark containers
	@echo "📦 Installing Python dependencies..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec spark-master pip install -r /opt/spark/requirements.txt
	@docker-compose -f $(DOCKER_COMPOSE_FILE) exec spark-worker pip install -r /opt/spark/requirements.txt
	@echo "✅ Dependencies installed"

dev-setup: build up setup-data install-deps ## Complete development setup
	@echo "🚀 Development environment ready!"
	@echo "Run 'make ingest-data' to start the pipeline"

complete-setup: ## Complete automated setup with testing
	@echo "🚀 Running complete automated setup..."
	@./setup.sh

setup: ## Complete environment setup (recommended for first-time setup)
	@echo "🚀 Running complete environment setup..."
	@./setup_env.sh

health-check: ## Check health of all services
	@echo "🏥 Checking service health..."
	@echo "Checking HDFS NameNode..."
	@curl -f http://localhost:$(HDFS_NAMENODE_HTTP_PORT) >/dev/null 2>&1 && echo "✅ HDFS NameNode: Healthy" || echo "❌ HDFS NameNode: Unhealthy"
	@echo "Checking Spark Master..."
	@curl -f http://localhost:$(SPARK_MASTER_WEBUI_PORT) >/dev/null 2>&1 && echo "✅ Spark Master: Healthy" || echo "❌ Spark Master: Unhealthy"
	@echo "Checking Airflow..."
	@curl -f http://localhost:$(AIRFLOW_WEBSERVER_PORT)/health >/dev/null 2>&1 && echo "✅ Airflow: Healthy" || echo "❌ Airflow: Unhealthy"
	@echo "Checking Grafana..."
	@curl -f http://localhost:$(GRAFANA_PORT)/api/health >/dev/null 2>&1 && echo "✅ Grafana: Healthy" || echo "❌ Grafana: Unhealthy"
	@echo "Checking Prometheus..."
	@curl -f http://localhost:$(PROMETHEUS_PORT)/-/healthy >/dev/null 2>&1 && echo "✅ Prometheus: Healthy" || echo "❌ Prometheus: Unhealthy"

monitor: ## Open monitoring dashboards
	@echo "📊 Opening monitoring dashboards..."
	@echo "Grafana: http://localhost:$(GRAFANA_PORT) (admin/$(GF_SECURITY_ADMIN_PASSWORD))"
	@echo "Prometheus: http://localhost:$(PROMETHEUS_PORT)"
	@echo "Airflow: http://localhost:$(AIRFLOW_WEBSERVER_PORT)"

config-check: ## Validate Docker Compose configuration
	@echo "🔧 Validating Docker Compose configuration..."
	@docker compose config --quiet && echo "✅ Docker Compose configuration is valid" || echo "❌ Docker Compose configuration has errors"

env-info: ## Display environment information
	@echo "📋 Environment Information"
	@echo "=========================="
	@echo "Project: $(PROJECT_NAME)"
	@echo "Version: $(PROJECT_VERSION)"
	@echo "Environment: $(ENVIRONMENT)"
	@echo "Network: $(NETWORK_NAME)"
	@echo "Timezone: $(TZ)"
	@echo ""
	@echo "🐳 Service Ports:"
	@echo "- Airflow:        $(AIRFLOW_WEBSERVER_PORT)"
	@echo "- Spark Master:   $(SPARK_MASTER_WEBUI_PORT)"
	@echo "- HDFS NameNode:  $(HDFS_NAMENODE_HTTP_PORT)"
	@echo "- Grafana:        $(GRAFANA_PORT)"
	@echo "- Prometheus:     $(PROMETHEUS_PORT)"
	@echo "- NiFi:           $(NIFI_WEB_HTTP_PORT)"

# Test commands
TEST_DIR := tests
REPORT_DIR := test_reports
PYTHON := python3

setup-test-env: ## Set up test environment and dependencies
	@echo "🧪 Setting up test environment..."
	$(PYTHON) -m pip install --quiet --upgrade pip
	$(PYTHON) -m pip install --quiet pytest pytest-html pytest-cov requests pandas docker
	@mkdir -p $(REPORT_DIR)
	@echo "✅ Test environment ready"

test-unit: setup-test-env ## Run unit tests only
	@echo "🧪 Running Unit Tests..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types unit --output-dir ../$(REPORT_DIR)

test-integration: setup-test-env ## Run integration tests only
	@echo "🔗 Running Integration Tests..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types integration --output-dir ../$(REPORT_DIR)

test-e2e: setup-test-env ## Run end-to-end tests only
	@echo "🎯 Running End-to-End Tests..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types e2e --output-dir ../$(REPORT_DIR)

test-all: setup-test-env ## Run all tests (unit, integration, e2e)
	@echo "🚀 Running All Tests..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types unit integration e2e --output-dir ../$(REPORT_DIR)

test-quick: setup-test-env ## Run quick test suite (unit tests only)
	@echo "⚡ Running Quick Test Suite..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types unit --output-dir ../$(REPORT_DIR)

# CI testing moved to advanced section below

test: test-all ## Alias for test-all

test-coverage: setup-test-env ## Run tests with coverage reporting
	@echo "📊 Running Tests with Coverage..."
	cd $(TEST_DIR) && $(PYTHON) -m pytest --cov=. --cov-report=html --cov-report=term-missing --cov-report=xml
	@echo "✅ Coverage report generated in $(TEST_DIR)/htmlcov/"

test-services: setup-test-env ## Test service health and connectivity
	@echo "🔍 Testing Service Health..."
	cd $(TEST_DIR) && $(PYTHON) -m unittest test_service_health.TestServiceHealth -v

# Data quality testing moved to advanced section below

test-ml-model: setup-test-env ## Test ML model validation
	@echo "🤖 Testing ML Model..."
	cd $(TEST_DIR) && $(PYTHON) -m unittest test_ml_model.TestMLModel -v

test-security: setup-test-env ## Test security and authentication
	@echo "🔒 Testing Security..."
	cd $(TEST_DIR) && $(PYTHON) -m unittest test_security.TestSecurity -v

# Performance testing moved to advanced section below

open-test-report: ## Open latest HTML test report in browser
	@echo "🌐 Opening test report..."
	@LATEST_REPORT=$$(ls -t $(REPORT_DIR)/test_report_*.html 2>/dev/null | head -n1); \
	if [ -n "$$LATEST_REPORT" ]; then \
		echo "Opening $$LATEST_REPORT"; \
		xdg-open "$$LATEST_REPORT" 2>/dev/null || open "$$LATEST_REPORT" 2>/dev/null || echo "Please open $$LATEST_REPORT manually"; \
	else \
		echo "❌ No test reports found. Run tests first."; \
	fi

clean-test-reports: ## Clean test reports directory
	@echo "🧹 Cleaning test reports..."
	rm -rf $(REPORT_DIR)/*
	@echo "✅ Test reports cleaned"

test-debug: setup-test-env ## Run tests in debug mode with verbose output
	@echo "🐛 Running Tests in Debug Mode..."
	cd $(TEST_DIR) && $(PYTHON) run_tests.py --types unit integration e2e --output-dir ../$(REPORT_DIR) --verbose

validate-test-env: ## Validate test environment setup
	@echo "✅ Validating Test Environment..."
	@echo "Python version: $$($(PYTHON) --version)"
	@echo "Working directory: $$(pwd)"
	@echo "Test directory exists: $$(test -d $(TEST_DIR) && echo 'Yes' || echo 'No')"
	@echo "Docker installed: $$(docker --version > /dev/null 2>&1 && echo 'Yes' || echo 'No')"
	@echo "Docker Compose installed: $$(docker-compose --version > /dev/null 2>&1 && echo 'Yes' || echo 'No')"

install-test-deps: setup-test-env ## Install test dependencies
	@echo "📦 Installing test dependencies..."
	@if [ -f requirements.txt ]; then \
		bash -c "source test_env/bin/activate && pip install -r requirements.txt"; \
	fi
	@if [ -f test-requirements.txt ]; then \
		bash -c "source test_env/bin/activate && pip install -r test-requirements.txt"; \
	fi
	@bash -c "source test_env/bin/activate && pip install pytest pytest-html pytest-cov pytest-mock pytest-benchmark requests psutil docker pyspark"
	@echo "✅ Test dependencies installed"

# Advanced Testing Targets
test-performance: install-test-deps ## Run performance and stress tests
	@echo "🚀 Running performance tests..."
	@source test_env/bin/activate && pytest -v -m performance tests/test_performance.py --html=test_reports/performance_report.html --self-contained-html
	@echo "📊 Performance test results: test_reports/performance_report.html"

test-stress: install-test-deps ## Run stress tests only
	@echo "💪 Running stress tests..."
	@source test_env/bin/activate && pytest -v -m stress tests/test_performance.py --html=test_reports/stress_report.html --self-contained-html

test-scheduling: install-test-deps ## Run scheduling and automation tests
	@echo "⏰ Running scheduling tests..."
	@source test_env/bin/activate && pytest -v -m scheduling tests/test_scheduling.py --html=test_reports/scheduling_report.html --self-contained-html

test-isolation: install-test-deps ## Run environment isolation tests
	@echo "🔒 Running isolation tests..."
	@source test_env/bin/activate && pytest -v -m isolation tests/test_isolation.py --html=test_reports/isolation_report.html --self-contained-html

test-comprehensive: install-test-deps ## Run all comprehensive test suites
	@echo "🧪 Running comprehensive test suite..."
	@bash -c "source test_env/bin/activate && python tests/run_tests.py --comprehensive --html-report"
	@echo "📈 Comprehensive test results: test_reports/comprehensive_report.html"

test-validate-suite: install-test-deps ## Validate test suite configuration and functionality
	@echo "✅ Validating test suite..."
	@source test_env/bin/activate && python tests/validate_test_suite.py
	@echo "📋 Validation results: test_reports/validation_results.json"

test-ci: test-validate-suite test-unit test-integration ## CI/CD test pipeline
	@echo "🔄 Running CI/CD test pipeline..."
	@echo "✅ All CI tests completed successfully!"

test-benchmark: install-test-deps ## Run performance benchmarks
	@echo "📊 Running performance benchmarks..."
	@source test_env/bin/activate && pytest -v -m performance tests/test_performance.py::TestPerformanceBenchmarks --benchmark-only
	@echo "📈 Benchmark results generated"

test-monitoring: install-test-deps ## Test monitoring and observability
	@echo "📈 Testing monitoring capabilities..."
	@source test_env/bin/activate && pytest -v tests/test_service_health.py tests/test_performance.py::TestPerformanceBenchmarks::test_resource_monitoring

test-full-pipeline: install-test-deps ## Run complete pipeline validation
	@echo "🔄 Running full pipeline test..."
	@bash scripts/run_pipeline_tests.sh --full-pipeline
	@echo "✅ Full pipeline test completed"

test-security-scan: install-test-deps ## Run security tests and scans
	@echo "🔒 Running security scans..."
	@source test_env/bin/activate && pytest -v tests/test_security.py --html=test_reports/security_report.html --self-contained-html

test-data-quality: install-test-deps ## Run data quality validation
	@echo "📊 Running data quality tests..."
	@source test_env/bin/activate && pytest -v tests/test_data_quality.py --html=test_reports/data_quality_report.html --self-contained-html

test-ml-model: install-test-deps ## Run ML model validation tests
	@echo "🤖 Running ML model validation tests..."
	@source test_env/bin/activate && pytest -v tests/test_ml_model.py --html=test_reports/ml_model_report.html --self-contained-html

test-service-health: install-test-deps ## Test service health and connectivity
	@echo "🔍 Testing service health..."
	@source test_env/bin/activate && pytest -v tests/test_service_health.py --html=test_reports/service_health_report.html --self-contained-html

test-performance-metrics: install-test-deps ## Test performance metrics collection
	@echo "📈 Testing performance metrics..."
	@source test_env/bin/activate && pytest -v tests/test_performance_metrics.py --html=test_reports/performance_metrics_report.html --self-contained-html

test-compliance: install-test-deps ## Run compliance and best practices checks
	@echo "📜 Running compliance checks..."
	@source test_env/bin/activate && pytest -v tests/test_compliance.py --html=test_reports/compliance_report.html --self-contained-html

test-security-best-practices: install-test-deps ## Test security best practices
	@echo "🔒 Testing security best practices..."
	@source test_env/bin/activate && pytest -v tests/test_security_best_practices.py --html=test_reports/security_best_practices_report.html --self-contained-html

test-data-schema: install-test-deps ## Test data schema validation
	@echo "📊 Testing data schema validation..."
	@source test_env/bin/activate && pytest -v tests/test_data_schema.py --html=test_reports/data_schema_report.html --self-contained-html

test-api-contract: install-test-deps ## Test API contract and integration
	@echo "🔌 Testing API contract..."
	@source test_env/bin/activate && pytest -v tests/test_api_contract.py --html=test_reports/api_contract_report.html --self-contained-html

test-ui-automation: install-test-deps ## Run UI automation tests
	@echo "🖥️ Running UI automation tests..."
	@source test_env/bin/activate && pytest -v tests/test_ui_automation.py --html=test_reports/ui_automation_report.html --self-contained-html

test-load: install-test-deps ## Run load testing
	@echo "⚙️ Running load tests..."
	@source test_env/bin/activate && locust -f tests/load_test.py --html=test_reports/load_test_report.html --headless -u 100 -r 10 --run-time 1m

test-stress: install-test-deps ## Run stress testing
	@echo "💥 Running stress tests..."
	@source test_env/bin/activate && locust -f tests/stress_test.py --html=test_reports/stress_test_report.html --headless -u 1000 -r 100 --run-time 2m

test-endpoint-security: install-test-deps ## Test endpoint security and authentication
	@echo "🔐 Testing endpoint security..."
	@source test_env/bin/activate && pytest -v tests/test_endpoint_security.py --html=test_reports/endpoint_security_report.html --self-contained-html

test-environment-variables: install-test-deps ## Test environment variable configuration
	@echo "🌐 Testing environment variables..."
	@source test_env/bin/activate && pytest -v tests/test_environment_variables.py --html=test_reports/environment_variables_report.html --self-contained-html

test-database-integration: install-test-deps ## Test database integration and connectivity
	@echo "🔗 Testing database integration..."
	@source test_env/bin/activate && pytest -v tests/test_database_integration.py --html=test_reports/database_integration_report.html --self-contained-html

test-cache-integration: install-test-deps ## Test cache integration and performance
	@echo "🗄️ Testing cache integration..."
	@source test_env/bin/activate && pytest -v tests/test_cache_integration.py --html=test_reports/cache_integration_report.html --self-contained-html

test-message-queue: install-test-deps ## Test message queue integration
	@echo "📬 Testing message queue integration..."
	@source test_env/bin/activate && pytest -v tests/test_message_queue.py --html=test_reports/message_queue_report.html --self-contained-html

test-async-task-queue: install-test-deps ## Test asynchronous task queue
	@echo "⏳ Testing asynchronous task queue..."
	@source test_env/bin/activate && pytest -v tests/test_async_task_queue.py --html=test_reports/async_task_queue_report.html --self-contained-html

test-data-pipeline: install-test-deps ## Test data pipeline end-to-end
	@echo "🔄 Testing data pipeline..."
	@source test_env/bin/activate && pytest -v tests/test_data_pipeline.py --html=test_reports/data_pipeline_report.html --self-contained-html

test-model-training-pipeline: install-test-deps ## Test model training pipeline
	@echo "📈 Testing model training pipeline..."
	@source test_env/bin/activate && pytest -v tests/test_model_training_pipeline.py --html=test_reports/model_training_pipeline_report.html --self-contained-html

test-predictive-model: install-test-deps ## Test predictive model accuracy and performance
	@echo "🔮 Testing predictive model..."
	@source test_env/bin/activate && pytest -v tests/test_predictive_model.py --html=test_reports/predictive_model_report.html --self-contained-html

test-notifications: install-test-deps ## Test notification system
	@echo "🔔 Testing notifications..."
	@source test_env/bin/activate && pytest -v tests/test_notifications.py --html=test_reports/notifications_report.html --self-contained-html

test-reporting: install-test-deps ## Test reporting and analytics
	@echo "📊 Testing reporting..."
	@source test_env/bin/activate && pytest -v tests/test_reporting.py --html=test_reports/reporting_report.html --self-contained-html

test-logging: install-test-deps ## Test logging configuration and output
	@echo "📜 Testing logging..."
	@source test_env/bin/activate && pytest -v tests/test_logging.py --html=test_reports/logging_report.html --self-contained-html

test-debugging: install-test-deps ## Test debugging and error handling
	@echo "🐞 Testing debugging..."
	@source test_env/bin/activate && pytest -v tests/test_debugging.py --html=test_reports/debugging_report.html --self-contained-html

test-cleanup: install-test-deps ## Test cleanup and resource deallocation
	@echo "🧹 Testing cleanup..."
	@source test_env/bin/activate && pytest -v tests/test_cleanup.py --html=test_reports/cleanup_report.html --self-contained-html

test-retry-mechanism: install-test-deps ## Test retry mechanism for failed tasks
	@echo "🔄 Testing retry mechanism..."
	@source test_env/bin/activate && pytest -v tests/test_retry_mechanism.py --html=test_reports/retry_mechanism_report.html --self-contained-html

test-rate-limiting: install-test-deps ## Test rate limiting on APIs
	@echo "⏱️ Testing rate limiting..."
	@source test_env/bin/activate && pytest -v tests/test_rate_limiting.py --html=test_reports/rate_limiting_report.html --self-contained-html

test-circuit-breaker: install-test-deps ## Test circuit breaker pattern
	@echo "⛓️ Testing circuit breaker..."
	@source test_env/bin/activate && pytest -v tests/test_circuit_breaker.py --html=test_reports/circuit_breaker_report.html --self-contained-html

test-service-discovery: install-test-deps ## Test service discovery and registry
	@echo "🔍 Testing service discovery..."
	@source test_env/bin/activate && pytest -v tests/test_service_discovery.py --html=test_reports/service_discovery_report.html --self-contained-html

test-api-gateway: install-test-deps ## Test API gateway functionality
	@echo "🚪 Testing API gateway..."
	@source test_env/bin/activate && pytest -v tests/test_api_gateway.py --html=test_reports/api_gateway_report.html --self-contained-html

test-backend-integration: install-test-deps ## Test backend service integration
	@echo "🔗 Testing backend integration..."
	@source test_env/bin/activate && pytest -v tests/test_backend_integration.py --html=test_reports/backend_integration_report.html --self-contained-html

test-frontend-integration: install-test-deps ## Test frontend and backend integration
	@echo "🌐 Testing frontend integration..."
	@source test_env/bin/activate && pytest -v tests/test_frontend_integration.py --html=test_reports/frontend_integration_report.html --self-contained-html

test-full-stack: install-test-deps ## Test full stack application
	@echo "🧪 Testing full stack..."
	@source test_env/bin/activate && pytest -v tests/test_full_stack.py --html=test_reports/full_stack_report.html --self-contained-html

test-api-load: install-test-deps ## Test API load handling
	@echo "⚙️ Testing API load..."
	@source test_env/bin/activate && locust -f tests/api_load_test.py --html=test_reports/api_load_test_report.html --headless -u 100 -r 10 --run-time 1m

test-api-stress: install-test-deps ## Test API stress handling
	@echo "💥 Testing API stress..."
	@source test_env/bin/activate && locust -f tests/api_stress_test.py --html=test_reports/api_stress_test_report.html --headless -u 1000 -r 100 --run-time 2m

test-api-security: install-test-deps ## Test API security vulnerabilities
	@echo "🔐 Testing API security..."
	@source test_env/bin/activate && pytest -v tests/test_api_security.py --html=test_reports/api_security_report.html --self-contained-html

test-api-performance: install-test-deps ## Test API performance metrics
	@echo "📈 Testing API performance..."
	@source test_env/bin/activate && pytest -v tests/test_api_performance.py --html=test_reports/api_performance_report.html --self-contained-html

test-api-compliance: install-test-deps ## Test API compliance with standards
	@echo "📜 Testing API compliance..."
	@source test_env/bin/activate && pytest -v tests/test_api_compliance.py --html=test_reports/api_compliance_report.html --self-contained-html

test-api-documentation: install-test-deps ## Test API documentation accuracy
	@echo "📚 Testing API documentation..."
	@source test_env/bin/activate && pytest -v tests/test_api_documentation.py --html=test_reports/api_documentation_report.html --self-contained-html

test-ui-performance: install-test-deps ## Test UI performance metrics
	@echo "📈 Testing UI performance..."
	@source test_env/bin/activate && pytest -v tests/test_ui_performance.py --html=test_reports/ui_performance_report.html --self-contained-html

test-ui-load: install-test-deps ## Test UI load handling
	@echo "⚙️ Testing UI load..."
	@source test_env/bin/activate && locust -f tests/ui_load_test.py --html=test_reports/ui_load_test_report.html --headless -u 100 -r 10 --run-time 1m

test-ui-stress: install-test-deps ## Test UI stress handling
	@echo "💥 Testing UI stress..."
	@source test_env/bin/activate && locust -f tests/ui_stress_test.py --html=test_reports/ui_stress_test_report.html --headless -u 1000 -r 100 --run-time 2m

test-ui-security: install-test-deps ## Test UI security vulnerabilities
	@echo "🔐 Testing UI security..."
	@source test_env/bin/activate && pytest -v tests/test_ui_security.py --html=test_reports/ui_security_report.html --self-contained-html

test-ui-compliance: install-test-deps ## Test UI compliance with standards
	@echo "📜 Testing UI compliance..."
	@source test_env/bin/activate && pytest -v tests/test_ui_compliance.py --html=test_reports/ui_compliance_report.html --self-contained-html

test-ui-documentation: install-test-deps ## Test UI documentation accuracy
	@echo "📚 Testing UI documentation..."
	@source test_env/bin/activate && pytest -v tests/test_ui_documentation.py --html=test_reports/ui_documentation_report.html --self-contained-html

test-endpoint-performance: install-test-deps ## Test endpoint performance metrics
	@echo "📈 Testing endpoint performance..."
	@source test_env/bin/activate && pytest -v tests/test_endpoint_performance.py --html=test_reports/endpoint_performance_report.html --self-contained-html

test-endpoint-load: install-test-deps ## Test endpoint load handling
	@echo "⚙️ Testing endpoint load..."
	@source test_env/bin/activate && locust -f tests/endpoint_load_test.py --html=test_reports/endpoint_load_test_report.html --headless -u 100 -r 10 --run-time 1m

test-endpoint-stress: install-test-deps ## Test endpoint stress handling
	@echo "💥 Testing endpoint stress..."
	@source test_env/bin/activate && locust -f tests/endpoint_stress_test.py --html=test_reports/endpoint_stress_test_report.html --headless -u 1000 -r 100 --run-time 2m

test-endpoint-security: install-test-deps ## Test endpoint security vulnerabilities
	@echo "🔐 Testing endpoint security..."
	@source test_env/bin/activate && pytest -v tests/test_endpoint_security.py --html=test_reports/endpoint_security_report.html --self-contained-html

test-endpoint-compliance: install-test-deps ## Test endpoint compliance with standards
	@echo "📜 Testing endpoint compliance..."
	@source test_env/bin/activate && pytest -v tests/test_endpoint_compliance.py --html=test_reports/endpoint_compliance_report.html --self-contained-html

test-endpoint-documentation: install-test-deps ## Test endpoint documentation accuracy
	@echo "📚 Testing endpoint documentation..."
	@source test_env/bin/activate && pytest -v tests/test_endpoint_documentation.py --html=test_reports/endpoint_documentation_report.html --self-contained-html

test-allure-report: ## Generate and open Allure report
	@echo "📊 Generating Allure report..."
	@allure generate --clean
	@echo "🌐 Opening Allure report..."
	@allure open

test-sarif-report: ## Generate and open SARIF report
	@echo "📊 Generating SARIF report..."
	@source test_env/bin/activate && pytest --sarif=test_reports/report.sarif.json
	@echo "🌐 Opening SARIF report..."
	@source test_env/bin/activate && code test_reports/report.sarif.json

test-html-report: ## Generate and open HTML report
	@echo "📊 Generating HTML report..."
	@source test_env/bin/activate && pytest --html=test_reports/report.html --self-contained-html
	@echo "🌐 Opening HTML report..."
	@source test_env/bin/activate && xdg-open test_reports/report.html || open test_reports/report.html

test-xml-report: ## Generate and open XML report
	@echo "📊 Generating XML report..."
	@source test_env/bin/activate && pytest --junitxml=test_reports/report.xml
	@echo "🌐 Opening XML report..."
	@source test_env/bin/activate && code test_reports/report.xml

test-json-report: ## Generate and open JSON report
	@echo "📊 Generating JSON report..."
	@source test_env/bin/activate && pytest --json=test_reports/report.json
	@echo "🌐 Opening JSON report..."
	@source test_env/bin/activate && code test_reports/report.json

test-summary-report: ## Generate and display summary report
	@echo "📊 Generating summary report..."
	@source test_env/bin/activate && pytest --tb=short -q > test_reports/summary_report.txt
	@echo "📋 Summary report:"
	@cat test_reports/summary_report.txt

test-metrics-report: ## Generate and display metrics report
	@echo "📊 Generating metrics report..."
	@source test_env/bin/activate && pytest --metrics=test_reports/metrics_report.json
	@echo "📋 Metrics report:"
	@cat test_reports/metrics_report.json

test-logs-report: ## Generate and display logs report
	@echo "📊 Generating logs report..."
	@source test_env/bin/activate && pytest --logs=test_reports/logs_report.json
	@echo "📋 Logs report:"
	@cat test_reports/logs_report.json

test-traceability-report: ## Generate and display traceability report
	@echo "📊 Generating traceability report..."
	@source test_env/bin/activate && pytest --traceability=test_reports/traceability_report.json
	@echo "📋 Traceability report:"
	@cat test_reports/traceability_report.json

test-dependency-report: ## Generate and display dependency report
	@echo "📊 Generating dependency report..."
	@source test_env/bin/activate && pytest --dependency=test_reports/dependency_report.json
	@echo "📋 Dependency report:"
	@cat test_reports/dependency_report.json

test-coverage-report: ## Generate and display coverage report
	@echo "📊 Generating coverage report..."
	@source test_env/bin/activate && pytest --cov-report=term-missing --cov-report=html:test_reports/htmlcov --cov-report=xml:test_reports/coverage.xml
	@echo "📋 Coverage report:"
	@cat test_reports/htmlcov/index.html

test-all-reports: ## Generate and open all reports
	@echo "📊 Generating all reports..."
	@make test-html-report
	@make test-xml-report
	@make test-json-report
	@make test-sarif-report
	@make test-allure-report
	@echo "🌐 Opening all reports..."
	@source test_env/bin/activate && xdg-open test_reports/report.html || open test_reports/report.html
	@source test_env/bin/activate && code test_reports/report.xml
	@source test_env/bin/activate && code test_reports/report.json
	@source test_env/bin/activate && code test_reports/report.sarif.json
	@allure open

test-clean: ## Clean test artifacts and reports
	@echo "🧹 Cleaning test artifacts..."
	rm -rf __pycache__ .pytest_cache .coverage htmlcov/ test_reports/
	@echo "✅ Test artifacts cleaned"

test-reset: clean-test-reports test-clean ## Reset test environment

test-restart: down up test-all ## Restart services and run all tests

test-restart-quick: down up test-quick ## Restart services and run quick tests

test-restart-full: down up test-allure-report test-html-report test-xml-report test-json-report test-sarif-report ## Restart services and generate all reports

test-restart-ci: down up test-ci ## Restart services and run CI tests

test-restart-all: down up test-allure-report test-html-report test-xml-report test-json-report test-sarif-report test-ci ## Restart services and generate all reports and run CI tests

test-debug: install-test-deps ## Debugging tests
	@echo "🐞 Debugging tests..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1

test-debug-all: install-test-deps ## Debugging all tests
	@echo "🐞 Debugging all tests..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 tests/

test-debug-last: install-test-deps ## Debugging last test run
	@echo "🐞 Debugging last test run..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --last-failed

test-debug-failed: install-test-deps ## Debugging failed tests
	@echo "🐞 Debugging failed tests..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --failed

test-debug-pdb: install-test-deps ## Debugging with pdb
	@echo "🐞 Debugging with pdb..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --pdb

test-debug-info: install-test-deps ## Debugging with info
	@echo "🐞 Debugging with info..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --info

test-debug-warnings: install-test-deps ## Debugging with warnings
	@echo "🐞 Debugging with warnings..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --warnings

test-debug-time: install-test-deps ## Debugging with time
	@echo "🐞 Debugging with time..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --time

test-debug-memory: install-test-deps ## Debugging with memory
	@echo "🐞 Debugging with memory..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --memory

test-debug-performance: install-test-deps ## Debugging performance
	@echo "🐞 Debugging performance..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --performance

test-debug-load: install-test-deps ## Debugging load
	@echo "🐞 Debugging load..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --load

test-debug-stress: install-test-deps ## Debugging stress
	@echo "🐞 Debugging stress..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --stress

test-debug-isolation: install-test-deps ## Debugging isolation
	@echo "🐞 Debugging isolation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --isolation

test-debug-comprehensive: install-test-deps ## Debugging comprehensive tests
	@echo "🐞 Debugging comprehensive tests..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --comprehensive

test-debug-validate-suite: install-test-deps ## Debugging test suite validation
	@echo "🐞 Debugging test suite validation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --validate-suite

test-debug-ci: install-test-deps ## Debugging CI tests
	@echo "🐞 Debugging CI tests..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --ci

test-debug-allure-report: ## Debugging Allure report generation
	@echo "🐞 Debugging Allure report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --allure

test-debug-sarif-report: ## Debugging SARIF report generation
	@echo "🐞 Debugging SARIF report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --sarif

test-debug-html-report: ## Debugging HTML report generation
	@echo "🐞 Debugging HTML report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --html

test-debug-xml-report: ## Debugging XML report generation
	@echo "🐞 Debugging XML report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --xml

test-debug-json-report: ## Debugging JSON report generation
	@echo "🐞 Debugging JSON report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --json

test-debug-summary-report: ## Debugging summary report generation
	@echo "🐞 Debugging summary report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --summary

test-debug-metrics-report: ## Debugging metrics report generation
	@echo "🐞 Debugging metrics report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --metrics

test-debug-logs-report: ## Debugging logs report generation
	@echo "🐞 Debugging logs report generation..."
	@source test_env/bin/activate && pytest -v --tb=short --disable-warnings --maxfail=1 --logs

test-debug-traceability-report: ## Debugging traceability report generation
	@echo "🐞 Debugging traceability report generation..."
	@source test_env/bin

# 🚀 Diabetes Prediction Pipeline - Step-by-Step Deployment Guide

## 📋 **DEPLOYMENT OVERVIEW**

This guide provides comprehensive instructions for deploying the diabetes prediction pipeline from initial setup to production-ready environment.

---

## 🎯 **PREREQUISITES**

### 📋 **System Requirements**
- **OS**: Linux/macOS (Ubuntu 20.04+ recommended)
- **RAM**: Minimum 8GB (16GB+ recommended)
- **Storage**: Minimum 20GB free space
- **CPU**: 4+ cores recommended
- **Network**: Internet connection for downloading dependencies

### 🛠️ **Required Software**
- **Docker**: Version 20.0+
- **Docker Compose**: Version 1.29+
- **Python**: Version 3.8+
- **Git**: Version 2.0+
- **Make**: GNU Make utility

### 📦 **Optional Tools**
- **curl**: For API testing
- **wget**: For downloading files
- **tree**: For directory visualization
- **htop**: For system monitoring

---

## 🚀 **STEP 1: INITIAL SETUP**

### 1.1 Clone and Navigate to Project
```bash
# Navigate to project directory
cd /mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes

# Verify project structure
ls -la
```

### 1.2 Verify Prerequisites
```bash
# Check Docker installation
docker --version
docker-compose --version

# Check Python installation
python3 --version

# Check Make installation
make --version

# Verify system resources
free -h
df -h
```

### 1.3 Set File Permissions
```bash
# Make scripts executable
chmod +x setup.sh
chmod +x setup_env.sh
chmod +x scripts/*.sh

# Set proper permissions for configuration files
chmod 644 .env docker-compose.yml Makefile
```

---

## 🔧 **STEP 2: ENVIRONMENT CONFIGURATION**

### 2.1 Environment Setup
```bash
# Run automated environment setup
./setup_env.sh

# Alternative: Manual setup
make init
```

### 2.2 Validate Environment Configuration
```bash
# Check environment variables
make env-info

# Validate configuration
make config-check

# Verify environment setup
make validate-env
```

### 2.3 Environment File Verification
```bash
# Verify .env file exists and contains required variables
cat .env | grep -E "(PROJECT_NAME|AIRFLOW|SPARK|HDFS)"
```

---

## 🧪 **STEP 3: TEST ENVIRONMENT SETUP**

### 3.1 Set Up Test Environment
```bash
# Initialize test environment
make setup-test-env

# Install test dependencies
make install-test-deps

# Validate test setup
make validate-test-env
```

### 3.2 Run Pre-deployment Validation
```bash
# Run comprehensive validation
python tests/comprehensive_validation.py

# Expected output: "🎉 TEST SUITE STATUS: READY"
```

---

## 🐳 **STEP 4: DOCKER INFRASTRUCTURE DEPLOYMENT**

### 4.1 Build Docker Images
```bash
# Build all Docker images
make build

# Verify images are built
docker images | grep -E "(diabetes|airflow|spark|hadoop)"
```

### 4.2 Initialize Project Structure
```bash
# Create required directories
make init

# Verify directory structure
tree -L 2 data/ airflow/ dashboard/
```

### 4.3 Start Core Services
```bash
# Start all services
make up

# Wait for services to initialize (2-3 minutes)
sleep 180

# Check service status
make status
```

---

## 🏥 **STEP 5: SERVICE HEALTH VERIFICATION**

### 5.1 Automated Health Check
```bash
# Run comprehensive health check
make health-check

# Expected output: All services should show "✅ Healthy"
```

### 5.2 Manual Service Verification
```bash
# Check individual service logs
make logs-airflow    # Airflow logs
make logs-spark      # Spark logs
make logs-hdfs       # HDFS logs

# Check all service logs
make logs
```

### 5.3 Web Interface Access Verification
```bash
# Test web interfaces (should return HTTP 200)
curl -I http://localhost:8080  # Airflow UI
curl -I http://localhost:8088  # Spark Master UI
curl -I http://localhost:9870  # HDFS NameNode UI
curl -I http://localhost:3000  # Grafana
curl -I http://localhost:9090  # Prometheus
```

---

## 📊 **STEP 6: DATA PIPELINE INITIALIZATION**

### 6.1 Set Up HDFS Data Structure
```bash
# Initialize HDFS directories and upload sample data
make setup-data

# Verify HDFS setup
docker-compose exec namenode hdfs dfs -ls /data/
```

### 6.2 Install Pipeline Dependencies
```bash
# Install Python dependencies in Spark containers
make install-deps

# Verify installation
docker-compose exec spark-master pip list | grep -E "(pandas|scikit|pyspark)"
```

### 6.3 Validate Data Pipeline
```bash
# Test data ingestion
make ingest-data

# Check Airflow DAG status
curl -X GET "http://localhost:8080/api/v1/dags" \
  -H "Authorization: Basic $(echo -n 'admin:admin' | base64)"
```

---

## 🤖 **STEP 7: MACHINE LEARNING MODEL DEPLOYMENT**

### 7.1 Train Initial Model
```bash
# Start model training
make train-model

# Monitor training progress
docker-compose logs -f spark-master | grep -i "model"
```

### 7.2 Validate Model Deployment
```bash
# Check if model files are created
docker-compose exec spark-master ls -la /opt/spark/models/

# Verify model artifacts
make test-ml-model
```

---

## 🔒 **STEP 8: SECURITY AND COMPLIANCE VALIDATION**

### 8.1 Run Security Tests
```bash
# Execute security validation
make test-security-scan

# Review security report
firefox test_reports/security_report.html
```

### 8.2 Validate Authentication
```bash
# Test service authentication
make test-security

# Verify SSL/TLS configuration (if applicable)
curl -k https://localhost:8080/health
```

---

## 📈 **STEP 9: PERFORMANCE VALIDATION**

### 9.1 Run Performance Tests
```bash
# Execute performance benchmarks
make test-performance

# Run stress tests
make test-stress

# Review performance reports
firefox test_reports/performance_report.html
```

### 9.2 Load Testing
```bash
# Run load testing
make test-load

# Monitor system resources during testing
htop
```

---

## 🔍 **STEP 10: COMPREHENSIVE TESTING**

### 10.1 Execute Full Test Suite
```bash
# Run all tests
make test-comprehensive

# Validate test results
make test-validate-suite
```

### 10.2 End-to-End Pipeline Testing
```bash
# Run complete pipeline test
make test-full-pipeline

# Test data quality
make test-data-quality

# Test integration
make test-integration
```

---

## 📊 **STEP 11: MONITORING SETUP**

### 11.1 Configure Dashboards
```bash
# Access monitoring dashboards
make monitor

# Verify Grafana dashboards
curl -X GET "http://admin:admin@localhost:3000/api/dashboards/home"
```

### 11.2 Set Up Alerting (Optional)
```bash
# Configure Prometheus alerts
docker-compose exec prometheus cat /etc/prometheus/prometheus.yml

# Test alerting rules
curl http://localhost:9090/api/v1/rules
```

---

## 🎯 **STEP 12: PRODUCTION READINESS VALIDATION**

### 12.1 Final Deployment Validation
```bash
# Run comprehensive deployment validation
python tests/comprehensive_validation.py

# Expected output: "Success Rate: 100.0%" and "Overall Status: READY"
```

### 12.2 Create Backup
```bash
# Create initial backup
make backup

# Verify backup creation
ls -la data/backup/
```

### 12.3 Performance Monitoring
```bash
# Start continuous monitoring
make test-monitoring

# Set up log aggregation
tail -f data/logs/*.log
```

---

## 🚦 **STEP 13: PRODUCTION DEPLOYMENT**

### 13.1 Production Environment Variables
```bash
# Update environment for production
cp .env .env.backup
sed -i 's/ENVIRONMENT=development/ENVIRONMENT=production/' .env

# Verify production settings
make env-info
```

### 13.2 Start Production Services
```bash
# Restart with production configuration
make restart

# Verify production deployment
make health-check
```

### 13.3 Final Validation
```bash
# Run production readiness tests
make test-all

# Verify all services
curl -f http://localhost:8080/health
curl -f http://localhost:8088
curl -f http://localhost:9870
curl -f http://localhost:3000
curl -f http://localhost:9090
```

---

## 📋 **STEP 14: POST-DEPLOYMENT VERIFICATION**

### 14.1 Service Monitoring
```bash
# Monitor service status
watch -n 30 'make status'

# Check resource usage
docker stats

# Monitor logs
make logs | tail -f
```

### 14.2 User Acceptance Testing
```bash
# Test complete user workflow
# 1. Access Airflow UI: http://localhost:8080
# 2. Trigger diabetes_pipeline DAG
# 3. Monitor execution in Spark UI: http://localhost:8088
# 4. View results in Grafana: http://localhost:3000
```

---

## 🔧 **TROUBLESHOOTING GUIDE**

### Common Issues and Solutions

#### Issue 1: Docker Services Not Starting
```bash
# Check Docker daemon
sudo systemctl status docker

# Check available resources
free -h && df -h

# Restart Docker
sudo systemctl restart docker
make restart
```

#### Issue 2: Port Conflicts
```bash
# Check port usage
netstat -tulpn | grep -E "(8080|8088|9870|3000|9090)"

# Stop conflicting services
sudo lsof -ti:8080 | xargs kill -9

# Restart pipeline
make restart
```

#### Issue 3: HDFS Connection Issues
```bash
# Check HDFS health
docker-compose exec namenode hdfs dfsadmin -report

# Restart HDFS services
docker-compose restart namenode datanode
```

#### Issue 4: Airflow DAG Issues
```bash
# Check DAG syntax
docker-compose exec airflow python -m py_compile /opt/airflow/dags/diabetes_etl_pipeline.py

# Refresh DAGs
docker-compose exec airflow airflow dags list
```

#### Issue 5: Spark Job Failures
```bash
# Check Spark logs
make logs-spark

# Verify Spark configuration
docker-compose exec spark-master spark-submit --version
```

---

## 📊 **MONITORING AND MAINTENANCE**

### Daily Monitoring
```bash
# Daily health check
make health-check

# Check disk usage
df -h

# Monitor logs for errors
grep -i error data/logs/*.log
```

### Weekly Maintenance
```bash
# Run comprehensive tests
make test-all

# Create backup
make backup

# Clean old logs
find data/logs -name "*.log" -mtime +7 -delete
```

### Monthly Maintenance
```bash
# Update dependencies
make install-deps

# Performance review
make test-performance

# Security audit
make test-security-scan
```

---

## 🎯 **SUCCESS CRITERIA**

### ✅ Deployment Successful When:
1. **All services show "✅ Healthy"** in health check
2. **All web interfaces accessible** (Airflow, Spark, HDFS, Grafana, Prometheus)
3. **Test suite shows "100% success rate"**
4. **Data pipeline executes successfully**
5. **ML model training completes**
6. **Monitoring dashboards display data**
7. **Security tests pass**
8. **Performance tests meet benchmarks**

### 📊 Expected Service URLs:
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark Master**: http://localhost:8088
- **Spark Worker**: http://localhost:8081
- **HDFS NameNode**: http://localhost:9870
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **NiFi**: http://localhost:8443/nifi

---

## 🆘 **SUPPORT AND RESOURCES**

### 📚 Documentation
- `README.md` - Project overview
- `DEPLOYMENT_SUMMARY.md` - Deployment status
- `PROJECT_COMPLETION.md` - Project details
- `tests/` - Testing documentation

### 🛠️ Useful Commands
```bash
# Quick status check
make status && make health-check

# Restart everything
make restart

# View all logs
make logs

# Run all tests
make test-all

# Emergency stop
make down

# Complete cleanup
make clean
```

### 🔍 Log Locations
- **Application logs**: `data/logs/`
- **Test reports**: `test_reports/`
- **Docker logs**: `docker-compose logs [service]`
- **System logs**: `/var/log/`

---

## 🎉 **DEPLOYMENT COMPLETION**

After successfully completing all steps, your diabetes prediction pipeline will be:

✅ **Fully operational** with all services running  
✅ **Monitored** with comprehensive dashboards  
✅ **Tested** with enterprise-grade test suite  
✅ **Secured** with vulnerability scanning  
✅ **Optimized** with performance benchmarking  
✅ **Production-ready** for real-world usage  

**🎊 Congratulations! Your diabetes prediction pipeline is now successfully deployed! 🎊**

---

## 📞 **GETTING HELP**

If you encounter issues:
1. Check the **Troubleshooting Guide** above
2. Review service **logs** using `make logs`
3. Run **health checks** using `make health-check`
4. Execute **diagnostic tests** using `make test-all`
5. Consult **documentation** in the `docs/` directory

For persistent issues, ensure all prerequisites are met and follow the steps in sequence.

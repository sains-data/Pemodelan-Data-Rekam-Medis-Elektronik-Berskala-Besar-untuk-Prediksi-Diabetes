# 🩺 Diabetes Prediction Data Engineering Pipeline

**Kelompok 8 RA - Big Data Project**

A comprehensive, production-ready data engineering pipeline for diabetes prediction using Apache Spark, Airflow, and modern big data technologies. This project implements a medallion architecture (Bronze → Silver → Gold) with end-to-end machine learning capabilities.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Data Engineering Pipeline                        │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│   Data Sources  │   Bronze Layer  │  Silver Layer   │   Gold Layer    │
│                 │                 │                 │                 │
│ • diabetes.csv  │ • Raw ingested │ • Cleaned data  │ • ML features   │
│ • health_data   │   data          │ • Normalized    │ • Predictions   │
│ • Streaming     │ • Schema        │ • Validated     │ • Aggregations  │
│   sources       │   validation    │ • Transformed   │ • Business KPIs │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
                           ↓                 ↓                 ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                          Technology Stack                              │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│   Ingestion     │   Processing    │   Orchestration │   Monitoring    │
│                 │                 │                 │                 │
│ • Apache NiFi   │ • Apache Spark  │ • Apache Airflow│ • Prometheus    │
│ • HDFS          │ • PySpark MLlib │ • Custom DAGs   │ • Grafana       │
│ • Batch/Stream  │ • Hive Tables   │ • Scheduling    │ • Custom metrics│
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker 20.10+ and Docker Compose 2.0+
- 8GB+ RAM available for containers
- 20GB+ free disk space

### 1. Clone and Setup

```bash
git clone <repository-url>
cd diabetes-prediction-pipeline
make validate-env  # Check prerequisites
```

### 2. Initialize and Start

```bash
make dev-setup     # Complete setup (build, start, configure)
```

This single command will:
- ✅ Build all Docker images
- ✅ Start all services
- ✅ Setup HDFS directories
- ✅ Install dependencies
- ✅ Upload initial datasets

### 3. Verify Installation

```bash
make test-local    # Run comprehensive tests
```

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8089 | admin/admin |
| **Spark Master** | http://localhost:8081 | - |
| **HDFS NameNode** | http://localhost:9870 | - |
| **Grafana** | http://localhost:3000 | admin/admin |
| **NiFi** | http://localhost:8080/nifi | admin/ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB |
| **Prometheus** | http://localhost:9090 | - |

## 📊 Pipeline Execution

### Trigger ETL Pipeline

```bash
# Start the complete diabetes prediction pipeline
make ingest-data

# Monitor progress in Airflow UI
# View real-time metrics in Grafana
# Check Spark job execution
```

### Manual Steps

```bash
# Train ML models manually
make train-model

# View detailed logs
make logs-airflow
make logs-spark

# Open service shells for debugging
make shell-spark
make shell-airflow
```

## 🏛️ Data Architecture

### Bronze Layer (Raw Data)
- **Location**: `/data/bronze/` in HDFS
- **Format**: CSV, JSON (as ingested)
- **Content**: Raw diabetes datasets, health monitoring data
- **Validation**: Schema validation, basic quality checks

### Silver Layer (Cleaned Data)
- **Location**: `/data/silver/` in HDFS  
- **Format**: Parquet (optimized)
- **Content**: Cleaned, normalized, validated data
- **Transformations**: 
  - Missing value imputation
  - Outlier handling (IQR method)
  - Data type standardization
  - Quality flags

### Gold Layer (Analytics Ready)
- **Location**: `/data/gold/` in HDFS
- **Format**: Parquet with partitioning
- **Content**: Feature-engineered data, ML predictions, business metrics
- **Features**:
  - BMI categories, age groups, risk scores
  - Normalized features for ML
  - Business KPIs and health insights

## 🤖 Machine Learning Pipeline

### Models Implemented
1. **Random Forest Classifier**
   - Best for feature importance analysis
   - Handles mixed data types well
   - Robust to outliers

2. **Logistic Regression**
   - Interpretable probability scores
   - Fast training and prediction
   - Good baseline model

3. **Gradient Boosting Classifier**
   - High accuracy potential
   - Handles complex patterns
   - Feature engineering benefits

### Model Evaluation
- **Metrics**: Accuracy, Precision, Recall, F1-score, AUC-ROC
- **Validation**: 5-fold cross-validation
- **Hyperparameter Tuning**: Grid search with cross-validation
- **Model Selection**: Automated best model selection

### Feature Engineering
- Health risk categories (Low/Medium/High)
- BMI classifications (Underweight/Normal/Overweight/Obese)
- Glucose categories (Normal/Prediabetic/Diabetic)
- Age groups and interaction features
- Normalized features for better ML performance

## 📈 Monitoring & Observability

### Prometheus Metrics
- **System Metrics**: CPU, memory, disk usage
- **HDFS Metrics**: Storage utilization, block health
- **Data Quality**: Missing values, validation rates
- **Model Performance**: Accuracy trends, prediction distributions
- **Pipeline Health**: Task success rates, execution times

### Grafana Dashboards
- **Executive Summary**: High-level KPIs and health status
- **ETL Monitoring**: Pipeline execution, data flow
- **Model Performance**: Accuracy trends, feature importance
- **System Health**: Infrastructure monitoring
- **Data Quality**: Validation metrics, anomaly detection

### Alerting
- Pipeline failures
- Data quality degradation
- Model accuracy drops
- System resource exhaustion

## 🔧 Development & Maintenance

### Available Commands

```bash
# Core operations
make up              # Start all services
make down            # Stop all services
make restart         # Restart all services
make status          # Check service status

# Development
make build           # Build Docker images
make clean           # Clean containers and volumes
make logs            # View all logs
make backup          # Backup models and data

# Testing
make test            # Run comprehensive tests
make validate-env    # Check prerequisites

# Monitoring
make monitoring      # Open monitoring dashboards
```

### Adding New Data Sources

1. **Update NiFi Flow**: Add processors for new data ingestion
2. **Modify ETL Scripts**: Update `scripts/ingestion/ingest_data.py`
3. **Schema Updates**: Add new tables in `hive/ddl/create_tables.sql`
4. **Airflow DAG**: Update task dependencies in DAG file

### Custom Model Development

1. **Add Model Script**: Create in `scripts/ml/`
2. **Update Training Pipeline**: Modify `train_model.py`
3. **Evaluation Metrics**: Update `evaluate_model.py`
4. **Airflow Integration**: Add to DAG tasks

## 🗂️ Project Structure

```
📦 diabetes-prediction-pipeline/
├── 🐳 docker-compose.yml              # Service orchestration
├── 📋 Makefile                        # Automation commands
├── 📚 README.md                       # This documentation
├── 
├── 📂 airflow/
│   └── 📂 dags/
│       └── 🌊 diabetes_etl_pipeline.py # Main ETL workflow
├── 
├── 📂 dashboard/
│   ├── 📂 grafana/                    # Visualization configs
│   └── 📂 prometheus/                 # Monitoring configs
├── 
├── 📂 data/
│   ├── 📊 diabetes.csv                # Primary dataset
│   ├── 📊 personal_health_data.csv    # Health monitoring data
│   ├── 📂 bronze/                     # Raw data storage
│   ├── 📂 silver/                     # Cleaned data storage
│   └── 📂 gold/                       # Analytics-ready data
├── 
├── 📂 docker/                         # Container configurations
│   ├── 📂 airflow/
│   ├── 📂 hadoop/
│   └── 📂 spark/
├── 
├── 📂 hive/
│   └── 📂 ddl/
│       └── 🗃️ create_tables.sql       # Database schema
├── 
├── 📂 models/                         # Trained ML models
├── 
└── 📂 scripts/
    ├── 📂 ingestion/                  # Data ingestion scripts
    ├── 📂 transform/                  # ETL transformations
    ├── 📂 ml/                         # Machine learning pipeline
    └── 📂 monitoring/                 # Metrics collection
```

## 🛠️ Technology Stack

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| **Orchestration** | Apache Airflow | 2.7.0 | Workflow management |
| **Processing** | Apache Spark | 3.3.0 | Distributed computing |
| **Storage** | Apache Hadoop HDFS | 3.2.1 | Distributed storage |
| **Data Warehouse** | Apache Hive | 2.3.2 | SQL on Hadoop |
| **Ingestion** | Apache NiFi | 1.23.2 | Data flow automation |
| **Monitoring** | Prometheus | 2.45.0 | Metrics collection |
| **Visualization** | Grafana | 10.0.0 | Dashboards & alerts |
| **ML Framework** | PySpark MLlib | 3.4.1 | Machine learning |
| **Container** | Docker | Latest | Containerization |
| **Language** | Python | 3.9 | Primary development |

## 📋 Data Quality Assurance

### Validation Rules
- **Schema Validation**: Required columns, data types
- **Range Checks**: Glucose (0-300), BMI (10-70), Age (0-120)
- **Missing Value Handling**: Strategic imputation vs. exclusion
- **Outlier Detection**: IQR method with configurable thresholds
- **Business Rules**: Medical impossibilities (e.g., 0 glucose)

### Quality Metrics
- **Completeness**: % of non-null values per column
- **Validity**: % of values within expected ranges
- **Consistency**: Cross-field validation rules
- **Uniqueness**: Duplicate record detection
- **Timeliness**: Data freshness indicators

## 🚨 Troubleshooting

### Common Issues

**Services won't start:**
```bash
make clean          # Clean everything
make validate-env   # Check prerequisites
make dev-setup      # Fresh setup
```

**Out of memory:**
```bash
# Reduce worker memory in docker-compose.yml
# SPARK_WORKER_MEMORY=1g (instead of 2g)
make restart
```

**Pipeline failures:**
```bash
make logs-airflow   # Check Airflow logs
make status         # Verify service health
# Check Airflow UI for task details
```

**Data not appearing:**
```bash
make setup-data     # Re-upload datasets
# Check HDFS UI for file presence
# Verify permissions and paths
```

### Performance Tuning

**Spark Optimization:**
- Adjust worker memory and cores
- Enable adaptive query execution
- Optimize shuffle partitions
- Use appropriate file formats (Parquet)

**HDFS Optimization:**
- Configure replication factor
- Optimize block size
- Monitor disk usage
- Regular maintenance

## 🤝 Contributing

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-algorithm`
3. **Follow coding standards**: PEP 8, documented functions
4. **Add tests**: Unit tests for new components
5. **Update documentation**: README and inline comments
6. **Submit pull request**: Detailed description of changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Team - Kelompok 8 RA

**Big Data Engineering Project**
- Advanced data pipeline architecture
- Production-ready monitoring and alerting
- Comprehensive ML model evaluation
- Industry-standard DevOps practices

---

**🎯 Quick Commands Reference:**

```bash
make dev-setup      # Complete setup
make ingest-data    # Run ETL pipeline  
make test-local     # Run tests
make monitoring     # Open dashboards
make clean          # Reset everything
```

For detailed command descriptions: `make help`

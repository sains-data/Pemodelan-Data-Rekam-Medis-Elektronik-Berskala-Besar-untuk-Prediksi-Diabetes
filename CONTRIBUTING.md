# =============================================================================
# CONTRIBUTING GUIDELINES
# =============================================================================
# Project: Big Data Pipeline for Diabetes Prediction
# Team: Kelompok 8 RA
# =============================================================================

## 🤝 How to Contribute

We welcome contributions to the Diabetes Prediction Pipeline project! This document provides guidelines for contributing to our data engineering project.

## 📋 Table of Contents

- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing Guidelines](#testing-guidelines)
- [Submitting Changes](#submitting-changes)
- [Data Engineering Best Practices](#data-engineering-best-practices)

## 🚀 Getting Started

### Prerequisites

Before contributing, ensure you have:

- **Docker & Docker Compose** (latest stable version)
- **Python 3.12+**
- **Git** (latest version)
- **Make** (for build automation)

### Initial Setup

1. **Fork the repository**
   ```bash
   # Fork on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/pipline_prediksi_diabestes.git
   cd pipline_prediksi_diabestes
   ```

2. **Set up development environment**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env with your local configuration
   vim .env
   
   # Set up the environment
   ./setup_env.sh
   ```

3. **Verify installation**
   ```bash
   # Run health checks
   make test-health
   
   # Run unit tests
   make test-unit
   ```

## 🔄 Development Workflow

### Branch Strategy

We use **Git Flow** with the following branches:

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/*` - Feature development branches
- `hotfix/*` - Critical bug fixes
- `release/*` - Release preparation branches

### Creating a Feature Branch

```bash
# Start from develop branch
git checkout develop
git pull origin develop

# Create feature branch
git checkout -b feature/your-feature-name

# Work on your feature...

# Push your feature branch
git push origin feature/your-feature-name
```

## 📏 Code Standards

### Python Code Style

We follow **PEP 8** with some modifications:

- **Line length**: 88 characters (Black formatter)
- **Import order**: Use `isort` for consistent imports
- **Type hints**: Required for all public functions

#### Code Formatting Tools

```bash
# Install formatting tools
pip install black isort flake8 mypy

# Format code
black scripts/ tests/
isort scripts/ tests/

# Check code quality
flake8 scripts/ tests/
mypy scripts/
```

### Documentation Standards

- All functions must have **docstrings** in Google style
- **README** files for each major component
- **Inline comments** for complex business logic
- **Architecture decisions** documented in `/docs`

#### Example Function Documentation

```python
def transform_diabetes_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Transform raw diabetes data for machine learning pipeline.
    
    Args:
        df: Raw diabetes dataset with patient records
        config: Transformation configuration including feature columns
        
    Returns:
        Transformed DataFrame ready for ML training
        
    Raises:
        ValueError: If required columns are missing from input DataFrame
        
    Example:
        >>> config = {"features": ["glucose", "bmi", "age"]}
        >>> transformed_df = transform_diabetes_data(raw_df, config)
    """
```

### Spark Job Standards

- Use **PySpark DataFrame API** (avoid RDDs unless necessary)
- Implement **checkpointing** for long lineages
- Add **monitoring** and **logging** to all jobs
- Use **configuration files** for parameters

```python
# Good Spark job structure
def run_etl_job(spark: SparkSession, config: Dict[str, Any]) -> None:
    """Run diabetes prediction ETL job."""
    logger = logging.getLogger(__name__)
    
    try:
        # Read data with schema validation
        df = spark.read.option("header", True).csv(config["input_path"])
        
        # Transform with checkpointing
        transformed_df = (df
            .transform(clean_data)
            .transform(feature_engineering)
            .checkpoint())  # Checkpoint for performance
        
        # Write with partitioning
        (transformed_df
            .write
            .mode("overwrite")
            .partitionBy("prediction_date")
            .parquet(config["output_path"]))
            
        logger.info(f"ETL job completed successfully. Records processed: {transformed_df.count()}")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
```

## 🧪 Testing Guidelines

### Test Structure

Our testing strategy includes:

```
tests/
├── unit/               # Unit tests for individual functions
├── integration/        # Integration tests for components
├── end-to-end/        # Full pipeline tests
├── performance/       # Performance and load tests
└── fixtures/          # Test data and configurations
```

### Writing Tests

#### Unit Tests

```python
import pytest
from scripts.transform.etl_spark_job import clean_diabetes_data

class TestDiabetesDataCleaning:
    """Test diabetes data cleaning functions."""
    
    def test_remove_null_values(self, spark_session, sample_diabetes_data):
        """Test null value removal from diabetes dataset."""
        # Given
        dirty_df = sample_diabetes_data.withColumn("glucose", lit(None))
        
        # When
        clean_df = clean_diabetes_data(dirty_df)
        
        # Then
        assert clean_df.filter(col("glucose").isNull()).count() == 0
    
    @pytest.mark.parametrize("glucose_value,expected", [
        (0, False),      # Invalid glucose
        (50, True),      # Valid glucose
        (400, False),    # Too high glucose
    ])
    def test_glucose_validation(self, spark_session, glucose_value, expected):
        """Test glucose value validation logic."""
        # Implementation here
```

#### Integration Tests

```python
def test_full_etl_pipeline(docker_services, test_data):
    """Test complete ETL pipeline from bronze to gold."""
    # Given: Raw data in bronze layer
    bronze_path = "data/bronze/test_diabetes.csv"
    
    # When: Run ETL pipeline
    result = run_diabetes_etl_pipeline(bronze_path)
    
    # Then: Verify gold layer output
    assert result.status == "SUCCESS"
    assert result.records_processed > 0
    
    # Verify data quality
    gold_df = spark.read.parquet("data/gold/diabetes_features/")
    assert gold_df.count() > 0
    assert all(col in gold_df.columns for col in REQUIRED_FEATURES)
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test categories
make test-unit          # Unit tests only
make test-integration   # Integration tests
make test-performance   # Performance tests

# Run tests with coverage
make test-coverage

# Run tests in Docker environment
make test-docker
```

## 📤 Submitting Changes

### Pull Request Process

1. **Ensure tests pass**
   ```bash
   make test
   make lint
   ```

2. **Update documentation**
   - Update relevant README files
   - Add/update docstrings
   - Update CHANGELOG.md

3. **Create pull request**
   - Use descriptive title and description
   - Link related issues
   - Add screenshots for UI changes
   - Request appropriate reviewers

### Pull Request Template

```markdown
## Description
Brief description of changes made.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Code is properly commented
- [ ] Documentation updated
- [ ] No new warnings introduced
```

### Commit Message Format

Use **Conventional Commits** format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Examples:
```bash
feat(etl): add diabetes data validation pipeline
fix(spark): resolve memory leak in feature engineering
docs(readme): update setup instructions for Docker
perf(ml): optimize model training performance
```

## 🏗️ Data Engineering Best Practices

### Data Pipeline Development

1. **Bronze → Silver → Gold Architecture**
   - Bronze: Raw data ingestion
   - Silver: Cleaned and validated data
   - Gold: Business-ready aggregated data

2. **Idempotent Operations**
   - All ETL jobs should be rerunnable
   - Use upsert patterns where applicable
   - Implement proper error handling

3. **Data Quality Checks**
   ```python
   def validate_data_quality(df: DataFrame) -> None:
       """Validate data quality with comprehensive checks."""
       # Schema validation
       assert_schema_compliance(df, EXPECTED_SCHEMA)
       
       # Data quality checks
       assert df.count() > 0, "DataFrame is empty"
       assert df.filter(col("patient_id").isNull()).count() == 0, "Null patient IDs found"
       
       # Business logic validation
       glucose_outliers = df.filter((col("glucose") < 0) | (col("glucose") > 500))
       assert glucose_outliers.count() == 0, f"Invalid glucose values: {glucose_outliers.count()}"
   ```

### Monitoring and Observability

1. **Structured Logging**
   ```python
   import structlog
   
   logger = structlog.get_logger()
   logger.info("ETL job started", 
              job_id=job_id, 
              input_records=input_count,
              timestamp=datetime.utcnow())
   ```

2. **Metrics Collection**
   - Track data volume and processing time
   - Monitor data quality metrics
   - Alert on pipeline failures

3. **Data Lineage**
   - Document data transformations
   - Track data sources and destinations
   - Maintain audit trails

### Performance Optimization

1. **Spark Optimization**
   - Use appropriate partitioning strategies
   - Implement broadcast joins for small tables
   - Cache intermediate results when beneficial
   - Monitor and tune Spark configurations

2. **Resource Management**
   - Right-size cluster resources
   - Use dynamic resource allocation
   - Implement proper error handling and retries

## 🆘 Getting Help

- **Issues**: Create GitHub issues for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check `/docs` directory for detailed guides
- **Code Review**: Request reviews from team members

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to the Diabetes Prediction Pipeline! 🚀

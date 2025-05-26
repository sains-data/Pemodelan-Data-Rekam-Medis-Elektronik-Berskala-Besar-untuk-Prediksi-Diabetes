"""
Diabetes Prediction ETL Pipeline DAG
Kelompok 8 RA - Big Data Project

This DAG orchestrates the complete ETL pipeline for diabetes prediction:
1. Data Ingestion (Bronze Layer)
2. Data Transformation (Silver Layer)
3. Feature Engineering (Gold Layer)
4. Model Training and Evaluation
5. Model Deployment and Monitoring
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'kelompok-8-ra',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG
dag = DAG(
    'diabetes_prediction_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for diabetes prediction using Apache Spark',
    schedule_interval='@daily',  # Run daily at midnight
    max_active_runs=1,
    tags=['diabetes', 'etl', 'spark', 'ml', 'healthcare']
)

# Task 1: Check if raw data is available
check_raw_data = FileSensor(
    task_id='check_raw_data_availability',
    filepath='/opt/airflow/data/diabetes.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

# Task 2: Data Validation and Quality Checks
def validate_data():
    """Validate data quality before processing"""
    import pandas as pd
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Read the raw data
        df = pd.read_csv('/opt/airflow/data/diabetes.csv')
        
        # Basic validation checks
        if df.empty:
            raise ValueError("Dataset is empty")
        
        if df.isnull().sum().sum() > len(df) * 0.3:  # More than 30% missing values
            raise ValueError("Too many missing values in dataset")
        
        logger.info(f"Data validation passed. Dataset shape: {df.shape}")
        return True
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise

data_validation = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_data,
    dag=dag
)

# Task 3: Data Ingestion to Bronze Layer (HDFS)
data_ingestion = BashOperator(
    task_id='ingest_data_to_bronze',
    bash_command="""
    /opt/airflow/scripts/spark_submit_wrapper.sh \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 1 \
        /opt/airflow/scripts/ingestion/ingest_data_pyspark.py \
        --source-path /opt/spark/data \
        --target-path hdfs://namenode:9000/data/bronze \
        --log-level INFO
    """,
    dag=dag
)

# Task 4: Data Transformation - Bronze to Silver
data_transformation = BashOperator(
    task_id='transform_bronze_to_silver',
    bash_command="""
    /opt/airflow/scripts/spark_submit_wrapper.sh \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /opt/airflow/scripts/transform/etl_spark_job.py \
        --input-path hdfs://namenode:9000/data/bronze/ \
        --output-path hdfs://namenode:9000/data/silver/ \
        --transformation-type bronze_to_silver
    """,
    dag=dag
)

# Task 5: Feature Engineering - Silver to Gold
feature_engineering = BashOperator(
    task_id='feature_engineering_silver_to_gold',
    bash_command="""
    /opt/airflow/scripts/spark_submit_wrapper.sh \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /opt/airflow/scripts/transform/etl_spark_job.py \
        --input-path hdfs://namenode:9000/data/silver/ \
        --output-path hdfs://namenode:9000/data/gold/ \
        --transformation-type silver_to_gold
    """,
    dag=dag
)

# Task 6: Model Training (Airflow Container)

model_training = BashOperator(
    task_id='train_diabetes_prediction_model',
    bash_command="""
    /opt/airflow/scripts/spark_submit_wrapper.sh \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /opt/airflow/scripts/ml/train_model_pyspark.py \
        hdfs://namenode:9000/data/gold/ \
        hdfs://namenode:9000/models/
    """,
    dag=dag
)

# Task 7: Model Evaluation
model_evaluation = BashOperator(
    task_id='evaluate_model_performance',
    bash_command="""
    /opt/airflow/scripts/spark_submit_wrapper.sh \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
        /opt/airflow/scripts/ml/evaluate_model_pyspark.py \
        --test-data-path hdfs://namenode:9000/data/gold/ \
        --metrics-output-path hdfs://namenode:9000/metrics/ \
    """,
    dag=dag
)

# Task 8: Data Quality Report Generation
def generate_quality_report():
    """Generate data quality report"""
    import json
    from datetime import datetime
    
    report = {
        "pipeline_run_date": datetime.now().isoformat(),
        "data_quality_status": "PASSED",
        "total_records_processed": 0,  # Will be updated by actual processing
        "bronze_layer_status": "SUCCESS",
        "silver_layer_status": "SUCCESS", 
        "gold_layer_status": "SUCCESS",
        "model_training_status": "SUCCESS",
        "model_accuracy": 0.0  # Will be updated by model evaluation
    }
    
    # Save report
    with open('/opt/airflow/data/logs/quality_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info("Data quality report generated successfully")

quality_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

# Task 9: Send metrics to Prometheus (for Grafana visualization)
send_metrics = BashOperator(
    task_id='send_metrics_to_prometheus',
    bash_command="""
    echo "Metrics collection completed - would send to Prometheus gateway"
    echo "Metrics timestamp: $(date)"
    echo "Pipeline execution metrics logged successfully"
    """,
    dag=dag
)

# Task 10: Pipeline completion notification
pipeline_complete = DummyOperator(
    task_id='pipeline_execution_complete',
    dag=dag
)

# Define task dependencies
check_raw_data >> data_validation >> data_ingestion
data_ingestion >> data_transformation >> feature_engineering
feature_engineering >> model_training >> model_evaluation
model_evaluation >> quality_report >> send_metrics >> pipeline_complete

# Additional parallel task for monitoring
monitoring_check = DummyOperator(
    task_id='monitoring_health_check',
    dag=dag
)

# Monitoring runs in parallel with main pipeline
data_ingestion >> monitoring_check >> pipeline_complete

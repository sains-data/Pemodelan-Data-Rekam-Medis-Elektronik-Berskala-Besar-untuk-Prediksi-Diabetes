"""
Enhanced PostgreSQL Diabetes ETL DAG
===================================

This DAG integrates the enhanced PostgreSQL ETL pipeline with comprehensive
feature engineering, risk scoring, and business intelligence features.
It orchestrates the enhanced_postgresql_etl.py script through Airflow.

Author: Kelompok 8 RA
Date: May 26, 2025
"""

from datetime import datetime, timedelta
import os
import sys
import logging
import subprocess
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Configuration
DEFAULT_ARGS = {
    'owner': 'kelompok_8_ra',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# PostgreSQL connection ID (configured in Airflow connections)
POSTGRES_CONN_ID = 'postgres_warehouse'

# Paths
ENHANCED_ETL_SCRIPT = '/opt/airflow/scripts/transform/enhanced_postgresql_etl.py'
DATA_PATH = '/opt/airflow/data'

# DAG definition
dag = DAG(
    'enhanced_postgresql_diabetes_etl',
    default_args=DEFAULT_ARGS,
    description='Enhanced diabetes prediction ETL with comprehensive feature engineering',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['diabetes', 'postgresql', 'enhanced-etl', 'feature-engineering', 'ml']
)

def check_prerequisites(**context):
    """
    Check all prerequisites before running the enhanced ETL
    """
    try:
        logging.info("Checking ETL prerequisites...")
        
        # Check if enhanced ETL script exists
        if not os.path.exists(ENHANCED_ETL_SCRIPT):
            raise FileNotFoundError(f"Enhanced ETL script not found: {ENHANCED_ETL_SCRIPT}")
        
        # Check PostgreSQL connection
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = postgres_hook.get_conn()
        
        # Verify schemas exist
        cursor = connection.cursor()
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics')
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        
        required_schemas = ['bronze', 'silver', 'gold', 'analytics']
        missing_schemas = set(required_schemas) - set(schemas)
        
        if missing_schemas:
            raise ValueError(f"Missing required schemas: {missing_schemas}")
        
        # Check if enhanced gold tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'diabetes_gold'
        """)
        
        if not cursor.fetchone():
            logging.warning("Enhanced gold table 'diabetes_gold' not found. Will be created by ETL.")
        
        cursor.close()
        connection.close()
        
        logging.info("All prerequisites check passed")
        return True
        
    except Exception as e:
        logging.error(f"Prerequisites check failed: {e}")
        raise

def run_enhanced_etl(**context):
    """
    Execute the enhanced PostgreSQL ETL script
    """
    try:
        logging.info("Starting enhanced PostgreSQL ETL pipeline...")
        
        # Set environment variables for the ETL script
        env = os.environ.copy()
        env.update({
            'POSTGRES_WAREHOUSE_HOST': 'postgres-warehouse',
            'POSTGRES_WAREHOUSE_PORT': '5432',
            'POSTGRES_WAREHOUSE_DB': 'diabetes_warehouse',
            'POSTGRES_WAREHOUSE_USER': 'warehouse',
            'POSTGRES_WAREHOUSE_PASSWORD': 'warehouse_secure_2025',
            'PYTHONPATH': str(project_root)
        })
        
        # Execute the enhanced ETL script
        result = subprocess.run(
            ['python3', ENHANCED_ETL_SCRIPT],
            env=env,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Log the output
        if result.stdout:
            logging.info(f"ETL Output:\n{result.stdout}")
        
        if result.stderr:
            logging.warning(f"ETL Warnings/Errors:\n{result.stderr}")
        
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, 
                ['python3', ENHANCED_ETL_SCRIPT],
                result.stdout,
                result.stderr
            )
        
        logging.info("Enhanced ETL pipeline completed successfully")
        
        # Extract metrics from output (simple parsing)
        records_processed = 0
        quality_score = 0.0
        
        for line in result.stdout.split('\n'):
            if 'records with average quality score' in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == 'Processed' and i+1 < len(parts):
                        try:
                            records_processed = int(parts[i+1])
                        except ValueError:
                            pass
                    if 'score:' in part and i+1 < len(parts):
                        try:
                            quality_score = float(parts[i+1])
                        except ValueError:
                            pass
        
        # Store metrics in XCom
        context['task_instance'].xcom_push(key='records_processed', value=records_processed)
        context['task_instance'].xcom_push(key='quality_score', value=quality_score)
        
        return {
            'status': 'success',
            'records_processed': records_processed,
            'quality_score': quality_score
        }
        
    except subprocess.TimeoutExpired:
        logging.error("Enhanced ETL script timed out after 1 hour")
        raise
    except subprocess.CalledProcessError as e:
        logging.error(f"Enhanced ETL script failed with return code {e.returncode}")
        logging.error(f"stdout: {e.stdout}")
        logging.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Failed to run enhanced ETL: {e}")
        raise

def validate_etl_results(**context):
    """
    Validate the results of the enhanced ETL pipeline
    """
    try:
        logging.info("Validating ETL results...")
        
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Check record counts in each layer
        validation_queries = {
            'bronze_diabetes': "SELECT COUNT(*) FROM bronze.diabetes_clinical",
            'silver_diabetes': "SELECT COUNT(*) FROM silver.diabetes_cleaned", 
            'gold_diabetes': "SELECT COUNT(*) FROM gold.diabetes_gold",
            'model_metadata': "SELECT COUNT(*) FROM gold.model_metadata_gold WHERE created_timestamp::date = CURRENT_DATE"
        }
        
        results = {}
        for key, query in validation_queries.items():
            try:
                count = postgres_hook.get_first(query)[0]
                results[key] = count
                logging.info(f"{key}: {count:,} records")
            except Exception as e:
                logging.warning(f"Failed to get count for {key}: {e}")
                results[key] = 0
        
        # Validate data quality
        quality_check_query = """
            SELECT 
                AVG(quality_score) as avg_quality,
                COUNT(*) as total_records,
                SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) as high_quality_records,
                COUNT(DISTINCT health_risk_level) as risk_levels
            FROM gold.diabetes_gold 
            WHERE processing_date = CURRENT_DATE
        """
        
        quality_result = postgres_hook.get_first(quality_check_query)
        
        if quality_result:
            avg_quality, total_records, high_quality_records, risk_levels = quality_result
            
            logging.info(f"Quality Validation:")
            logging.info(f"  - Average quality score: {avg_quality:.3f}")
            logging.info(f"  - Total records processed today: {total_records:,}")
            logging.info(f"  - High quality records (>=0.8): {high_quality_records:,}")
            logging.info(f"  - Number of risk levels: {risk_levels}")
            
            # Validation thresholds
            if avg_quality < 0.7:
                raise ValueError(f"Average quality score too low: {avg_quality:.3f}")
            
            if total_records == 0:
                raise ValueError("No records processed today")
            
            if risk_levels < 3:
                logging.warning(f"Only {risk_levels} risk levels found, expected at least 3")
        
        # Check feature engineering results
        feature_check_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN glucose_bmi_interaction IS NOT NULL THEN 1 END) as interaction_features,
                COUNT(CASE WHEN health_risk_level IS NOT NULL THEN 1 END) as risk_assessments,
                COUNT(CASE WHEN treatment_recommendation IS NOT NULL THEN 1 END) as recommendations
            FROM gold.diabetes_gold 
            WHERE processing_date = CURRENT_DATE
        """
        
        feature_result = postgres_hook.get_first(feature_check_query)
        
        if feature_result:
            total, interactions, risks, recommendations = feature_result
            logging.info(f"Feature Engineering Validation:")
            logging.info(f"  - Records with interaction features: {interactions:,}/{total:,}")
            logging.info(f"  - Records with risk assessments: {risks:,}/{total:,}")
            logging.info(f"  - Records with recommendations: {recommendations:,}/{total:,}")
        
        # Store validation results
        context['task_instance'].xcom_push(key='validation_results', value=results)
        
        logging.info("ETL results validation completed successfully")
        return results
        
    except Exception as e:
        logging.error(f"ETL results validation failed: {e}")
        raise

def refresh_analytics_views(**context):
    """
    Refresh analytics views and aggregations
    """
    try:
        logging.info("Refreshing analytics views and aggregations...")
        
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Call the analytics refresh function
        refresh_sql = "SELECT refresh_analytics_aggregations();"
        postgres_hook.run(refresh_sql)
        
        # Update daily statistics
        daily_stats_sql = """
            INSERT INTO gold.health_insights_gold (
                age_group, health_risk_category, bmi_category,
                total_patients, diabetic_patients, high_risk_patients,
                avg_bmi, avg_glucose, avg_age, avg_risk_score,
                high_risk_count, high_risk_percentage, report_date
            )
            SELECT 
                age_group,
                health_risk_level as health_risk_category,
                bmi_category,
                COUNT(*) as total_patients,
                SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic_patients,
                SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_patients,
                ROUND(AVG(bmi)::numeric, 2) as avg_bmi,
                ROUND(AVG(glucose)::numeric, 2) as avg_glucose,
                ROUND(AVG(age)::numeric, 2) as avg_age,
                ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
                SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_risk_count,
                ROUND((SUM(CASE WHEN health_risk_level IN ('High', 'Critical') THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric, 2) as high_risk_percentage,
                CURRENT_DATE as report_date
            FROM gold.diabetes_gold
            WHERE processing_date = CURRENT_DATE
            GROUP BY age_group, health_risk_level, bmi_category
            ON CONFLICT (age_group, health_risk_category, bmi_category, report_date) 
            DO UPDATE SET
                total_patients = EXCLUDED.total_patients,
                diabetic_patients = EXCLUDED.diabetic_patients,
                high_risk_patients = EXCLUDED.high_risk_patients,
                avg_bmi = EXCLUDED.avg_bmi,
                avg_glucose = EXCLUDED.avg_glucose,
                avg_age = EXCLUDED.avg_age,
                avg_risk_score = EXCLUDED.avg_risk_score,
                high_risk_count = EXCLUDED.high_risk_count,
                high_risk_percentage = EXCLUDED.high_risk_percentage
        """
        
        postgres_hook.run(daily_stats_sql)
        
        logging.info("Analytics views and aggregations refreshed successfully")
        
    except Exception as e:
        logging.error(f"Failed to refresh analytics views: {e}")
        raise

def send_success_notification(**context):
    """
    Send success notification with pipeline metrics
    """
    try:
        # Get metrics from previous tasks
        records_processed = context['task_instance'].xcom_pull(
            task_ids='run_enhanced_etl', key='records_processed'
        ) or 0
        
        quality_score = context['task_instance'].xcom_pull(
            task_ids='run_enhanced_etl', key='quality_score'
        ) or 0.0
        
        validation_results = context['task_instance'].xcom_pull(
            task_ids='validate_etl_results', key='validation_results'
        ) or {}
        
        # Create summary message
        message = f"""
Enhanced PostgreSQL Diabetes ETL Pipeline Completed Successfully!

Execution Details:
- Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Records Processed: {records_processed:,}
- Average Quality Score: {quality_score:.3f}

Data Layer Counts:
- Bronze Layer: {validation_results.get('bronze_diabetes', 0):,} records
- Silver Layer: {validation_results.get('silver_diabetes', 0):,} records  
- Gold Layer: {validation_results.get('gold_diabetes', 0):,} records
- Model Metadata: {validation_results.get('model_metadata', 0):,} entries

Features:
✓ Comprehensive feature engineering
✓ Risk scoring and predictions
✓ Business intelligence features
✓ Data quality validation
✓ Analytics aggregations updated

Dashboard: http://localhost:3000 (Grafana)
Database: postgres-warehouse:5432/diabetes_warehouse
        """
        
        logging.info(message)
        
        # Store notification in XCom for potential email operator
        context['task_instance'].xcom_push(key='success_message', value=message)
        
    except Exception as e:
        logging.error(f"Failed to send success notification: {e}")
        # Don't raise - this is just a notification

# Task definitions
start_task = DummyOperator(
    task_id='start_enhanced_etl_pipeline',
    dag=dag
)

check_prerequisites_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag
)

run_enhanced_etl_task = PythonOperator(
    task_id='run_enhanced_etl',
    python_callable=run_enhanced_etl,
    dag=dag,
    pool='postgres_pool',  # Limit concurrent PostgreSQL operations
    retries=1  # Reduce retries for long-running task
)

validate_results_task = PythonOperator(
    task_id='validate_etl_results',
    python_callable=validate_etl_results,
    dag=dag
)

refresh_analytics_task = PythonOperator(
    task_id='refresh_analytics_views',
    python_callable=refresh_analytics_views,
    dag=dag
)

send_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='all_done'  # Run even if previous tasks have warnings
)

end_task = DummyOperator(
    task_id='end_enhanced_etl_pipeline',
    dag=dag,
    trigger_rule='all_done'
)

# Task dependencies
start_task >> check_prerequisites_task >> run_enhanced_etl_task >> validate_results_task >> refresh_analytics_task >> send_notification_task >> end_task

# Add error handling branch
error_notification_task = PythonOperator(
    task_id='send_error_notification',
    python_callable=lambda **context: logging.error(
        f"Enhanced ETL Pipeline failed at {datetime.now()}"
    ),
    dag=dag,
    trigger_rule='one_failed'
)

# Connect error handling
[check_prerequisites_task, run_enhanced_etl_task, validate_results_task, refresh_analytics_task] >> error_notification_task

"""
PostgreSQL Data Warehouse ETL Pipeline
=====================================

Enhanced Airflow DAG for diabetes prediction pipeline using PostgreSQL as the data warehouse.
This replaces the Hive-based pipeline with PostgreSQL optimized operations.

Author: Kelompok 8 RA
Date: May 26, 2025
"""

from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

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

# Data paths
DATA_BRONZE_PATH = '/opt/airflow/data/bronze'
DATA_SILVER_PATH = '/opt/airflow/data/silver'
DATA_GOLD_PATH = '/opt/airflow/data/gold'

# DAG definition
dag = DAG(
    'postgresql_diabetes_prediction_etl',
    default_args=DEFAULT_ARGS,
    description='Diabetes prediction ETL pipeline using PostgreSQL data warehouse',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['diabetes', 'postgresql', 'etl', 'machine-learning']
)

def check_postgresql_connection(**context):
    """
    Check PostgreSQL connection and log database status
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Test connection
        connection = postgres_hook.get_conn()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Check database status
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as inserted_rows,
                n_tup_upd as updated_rows,
                n_tup_del as deleted_rows
            FROM pg_stat_user_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            ORDER BY schemaname, tablename;
        """)
        
        results = cursor.fetchall()
        
        logging.info("PostgreSQL Database Status:")
        for row in results:
            logging.info(f"  {row['schemaname']}.{row['tablename']}: "
                        f"Inserted={row['inserted_rows']}, "
                        f"Updated={row['updated_rows']}, "
                        f"Deleted={row['deleted_rows']}")
        
        cursor.close()
        connection.close()
        
        logging.info("PostgreSQL connection check completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"PostgreSQL connection check failed: {e}")
        raise

def ingest_csv_data_to_bronze(**context):
    """
    Ingest CSV data into PostgreSQL bronze layer
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Check for new CSV files
        csv_files = []
        for file_name in ['diabetes.csv', 'personal_health_data.csv']:
            file_path = f'/opt/airflow/data/{file_name}'
            if os.path.exists(file_path):
                csv_files.append((file_name, file_path))
        
        if not csv_files:
            logging.warning("No CSV files found for ingestion")
            return
        
        for file_name, file_path in csv_files:
            logging.info(f"Processing file: {file_name}")
            
            if 'diabetes' in file_name.lower():
                # Process diabetes data
                df = pd.read_csv(file_path)
                
                # Add metadata columns
                df['data_source'] = file_name
                df['ingestion_version'] = '1.0'
                df['file_name'] = file_name
                df['row_number'] = range(1, len(df) + 1)
                
                # Map columns to match PostgreSQL schema
                column_mapping = {
                    'Pregnancies': 'pregnancies',
                    'Glucose': 'glucose',
                    'BloodPressure': 'blood_pressure',
                    'SkinThickness': 'skin_thickness',
                    'Insulin': 'insulin',
                    'BMI': 'bmi',
                    'DiabetesPedigreeFunction': 'diabetes_pedigree_function',
                    'Age': 'age',
                    'Outcome': 'outcome'
                }
                
                df = df.rename(columns=column_mapping)
                
                # Insert into bronze.diabetes_clinical
                df.to_sql(
                    'diabetes_clinical',
                    engine,
                    schema='bronze',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                logging.info(f"Inserted {len(df)} records into bronze.diabetes_clinical")
                
            elif 'personal_health' in file_name.lower():
                # Process personal health data
                df = pd.read_csv(file_path)
                
                # Add metadata columns
                df['data_source'] = file_name
                df['ingestion_version'] = '1.0'
                
                # Map columns to match PostgreSQL schema
                column_mapping = {
                    'Person_ID': 'person_id',
                    'Age': 'age',
                    'Gender': 'gender',
                    'Height': 'height',
                    'Weight': 'weight',
                    'Heart_Rate': 'heart_rate',
                    'Blood_Pressure_Systolic': 'blood_pressure_systolic',
                    'Blood_Pressure_Diastolic': 'blood_pressure_diastolic',
                    'Sleep_Duration': 'sleep_duration',
                    'Deep_Sleep_Duration': 'deep_sleep_duration',
                    'REM_Sleep_Duration': 'rem_sleep_duration',
                    'Steps_Daily': 'steps_daily',
                    'Exercise_Minutes': 'exercise_minutes',
                    'Stress_Level': 'stress_level',
                    'Mood': 'mood',
                    'Medical_Condition': 'medical_condition',
                    'Health_Score': 'health_score',
                    'Sleep_Efficiency': 'sleep_efficiency'
                }
                
                df = df.rename(columns=column_mapping)
                
                # Insert into bronze.personal_health_data
                df.to_sql(
                    'personal_health_data',
                    engine,
                    schema='bronze',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                logging.info(f"Inserted {len(df)} records into bronze.personal_health_data")
        
        engine.dispose()
        logging.info("CSV data ingestion completed successfully")
        
    except Exception as e:
        logging.error(f"CSV data ingestion failed: {e}")
        raise

def process_bronze_to_silver(**context):
    """
    Process data from bronze to silver layer with data quality checks
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Call the PostgreSQL function to process bronze to silver
        sql = "SELECT process_bronze_to_silver() as processed_count;"
        
        result = postgres_hook.get_first(sql)
        processed_count = result[0] if result else 0
        
        logging.info(f"Processed {processed_count} records from bronze to silver layer")
        
        # Store processing metrics
        context['task_instance'].xcom_push(key='bronze_to_silver_count', value=processed_count)
        
        return processed_count
        
    except Exception as e:
        logging.error(f"Bronze to silver processing failed: {e}")
        raise

def process_silver_to_gold(**context):
    """
    Process data from silver to gold layer with feature engineering
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Call the PostgreSQL function to process silver to gold
        sql = "SELECT process_silver_to_gold() as processed_count;"
        
        result = postgres_hook.get_first(sql)
        processed_count = result[0] if result else 0
        
        logging.info(f"Processed {processed_count} records from silver to gold layer")
        
        # Store processing metrics
        context['task_instance'].xcom_push(key='silver_to_gold_count', value=processed_count)
        
        return processed_count
        
    except Exception as e:
        logging.error(f"Silver to gold processing failed: {e}")
        raise

def refresh_analytics_views(**context):
    """
    Refresh analytics aggregations and views
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Call the PostgreSQL function to refresh analytics
        sql = "SELECT refresh_analytics_aggregations();"
        
        postgres_hook.run(sql)
        
        logging.info("Analytics aggregations refreshed successfully")
        
        # Get latest metrics for monitoring
        metrics_sql = """
            SELECT 
                metric_name,
                metric_value,
                updated_at
            FROM analytics.latest_metrics
            ORDER BY metric_name;
        """
        
        metrics = postgres_hook.get_records(metrics_sql)
        
        logging.info("Latest Analytics Metrics:")
        for metric in metrics:
            logging.info(f"  {metric[0]}: {metric[1]} (updated: {metric[2]})")
        
        return metrics
        
    except Exception as e:
        logging.error(f"Analytics refresh failed: {e}")
        raise

def run_data_quality_checks(**context):
    """
    Run comprehensive data quality checks
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Data quality checks
        quality_checks = [
            {
                'name': 'Bronze Data Completeness',
                'sql': """
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN glucose IS NULL OR glucose = 0 THEN 1 ELSE 0 END) as missing_glucose,
                        SUM(CASE WHEN bmi IS NULL OR bmi = 0 THEN 1 ELSE 0 END) as missing_bmi,
                        SUM(CASE WHEN age IS NULL OR age = 0 THEN 1 ELSE 0 END) as missing_age
                    FROM bronze.diabetes_clinical;
                """
            },
            {
                'name': 'Silver Data Quality',
                'sql': """
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) as valid_records,
                        AVG(quality_score) as avg_quality_score,
                        MIN(quality_score) as min_quality_score
                    FROM silver.diabetes_cleaned;
                """
            },
            {
                'name': 'Gold Data Features',
                'sql': """
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT health_risk_level) as risk_levels,
                        COUNT(DISTINCT age_group) as age_groups,
                        COUNT(DISTINCT bmi_category) as bmi_categories,
                        AVG(risk_score) as avg_risk_score
                    FROM gold.diabetes_comprehensive;
                """
            }
        ]
        
        quality_results = {}
        
        for check in quality_checks:
            logging.info(f"Running quality check: {check['name']}")
            
            result = postgres_hook.get_first(check['sql'])
            quality_results[check['name']] = result
            
            logging.info(f"  Result: {result}")
        
        # Store quality results for monitoring
        context['task_instance'].xcom_push(key='quality_check_results', value=quality_results)
        
        return quality_results
        
    except Exception as e:
        logging.error(f"Data quality checks failed: {e}")
        raise

def generate_processing_report(**context):
    """
    Generate comprehensive processing report
    """
    try:
        # Get processing metrics from XCom
        ti = context['task_instance']
        
        bronze_to_silver_count = ti.xcom_pull(key='bronze_to_silver_count', task_ids='process_bronze_to_silver')
        silver_to_gold_count = ti.xcom_pull(key='silver_to_gold_count', task_ids='process_silver_to_gold')
        quality_results = ti.xcom_pull(key='quality_check_results', task_ids='run_data_quality_checks')
        
        # Generate report
        report = f"""
PostgreSQL ETL Pipeline Processing Report
========================================
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
DAG Run: {context['dag_run'].dag_id}

PROCESSING SUMMARY:
- Bronze to Silver: {bronze_to_silver_count or 0} records processed
- Silver to Gold: {silver_to_gold_count or 0} records processed

DATA QUALITY RESULTS:
"""
        
        if quality_results:
            for check_name, result in quality_results.items():
                report += f"\n{check_name}:\n"
                if isinstance(result, (list, tuple)):
                    for i, value in enumerate(result):
                        report += f"  Metric {i+1}: {value}\n"
                else:
                    report += f"  Result: {result}\n"
        
        report += f"""
STATUS: ✓ COMPLETED SUCCESSFULLY

Next scheduled run: {context['next_ds']}
"""
        
        logging.info(report)
        
        # Save report to file
        report_path = f"/opt/airflow/logs/processing_report_{context['ds']}.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        
        logging.info(f"Processing report saved to: {report_path}")
        
        return report_path
        
    except Exception as e:
        logging.error(f"Report generation failed: {e}")
        raise

# Task definitions
with dag:
    
    # Start task
    start_task = DummyOperator(
        task_id='start_postgresql_etl_pipeline'
    )
    
    # PostgreSQL connection check
    check_postgres_task = PythonOperator(
        task_id='check_postgresql_connection',
        python_callable=check_postgresql_connection,
        provide_context=True
    )
    
    # Data ingestion group
    with TaskGroup('data_ingestion') as ingestion_group:
        
        # Check for new data files
        check_data_files = FileSensor(
            task_id='check_for_csv_files',
            filepath='/opt/airflow/data/',
            fs_conn_id='fs_default',
            poke_interval=60,
            timeout=300,
            soft_fail=True
        )
        
        # Ingest CSV data to bronze
        ingest_csv_task = PythonOperator(
            task_id='ingest_csv_to_bronze',
            python_callable=ingest_csv_data_to_bronze,
            provide_context=True
        )
        
        check_data_files >> ingest_csv_task
    
    # Data processing group
    with TaskGroup('data_processing') as processing_group:
        
        # Bronze to Silver processing
        bronze_to_silver_task = PythonOperator(
            task_id='process_bronze_to_silver',
            python_callable=process_bronze_to_silver,
            provide_context=True
        )
        
        # Silver to Gold processing
        silver_to_gold_task = PythonOperator(
            task_id='process_silver_to_gold',
            python_callable=process_silver_to_gold,
            provide_context=True
        )
        
        bronze_to_silver_task >> silver_to_gold_task
    
    # Analytics and quality group
    with TaskGroup('analytics_and_quality') as analytics_group:
        
        # Refresh analytics views
        refresh_analytics_task = PythonOperator(
            task_id='refresh_analytics_views',
            python_callable=refresh_analytics_views,
            provide_context=True
        )
        
        # Data quality checks
        quality_checks_task = PythonOperator(
            task_id='run_data_quality_checks',
            python_callable=run_data_quality_checks,
            provide_context=True
        )
        
        # Run in parallel
        [refresh_analytics_task, quality_checks_task]
    
    # Generate processing report
    generate_report_task = PythonOperator(
        task_id='generate_processing_report',
        python_callable=generate_processing_report,
        provide_context=True
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_postgresql_etl_pipeline'
    )
    
    # Task dependencies
    start_task >> check_postgres_task >> ingestion_group >> processing_group >> analytics_group >> generate_report_task >> end_task

# Additional task for manual data refresh
manual_refresh_task = PostgresOperator(
    task_id='manual_refresh_all_analytics',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        -- Manual refresh of all analytics
        SELECT refresh_analytics_aggregations();
        
        -- Update processing statistics
        INSERT INTO analytics.model_performance (
            model_name, 
            model_version, 
            notes, 
            training_date
        ) VALUES (
            'etl_pipeline_run', 
            '1.0', 
            'Manual analytics refresh completed', 
            CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

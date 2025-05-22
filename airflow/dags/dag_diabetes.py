from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Parameterisasi untuk fleksibilitas
DATA_VALIDATION_THRESHOLD = 0.95  # Threshold validasi data (95%)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    
}

dag = DAG(
    'dag_diabetes_prediction', 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    description='End-to-end pipeline untuk prediksi diabetes'
)


# Task ingest data
ingest = BashOperator(
    task_id='ingest_data',
    bash_command='/usr/local/airflow/scripts/ingest_data.sh ',  # Note the space at the end!
    dag=dag,
)

def run_spark_job(script_path):
    from subprocess import run
    import sys
    result = run([
        'python3',
        script_path
    ])
    if result.returncode != 0:
        sys.exit(result.returncode)

# Task validasi data
validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=run_spark_job,
    op_args=['/usr/local/airflow/scripts/validate_data.py'],
    dag=dag
)

# Task transformasi data (ETL)
transform = PythonOperator(
    task_id='run_spark_etl',
    python_callable=run_spark_job,
    op_args=['/usr/local/airflow/scripts/etl_spark_job.py'],
    dag=dag
)

# Task prediksi diabetes
predict = PythonOperator(
    task_id='predict_diabetes',
    python_callable=run_spark_job,
    op_args=['/usr/local/airflow/scripts/predict_diabetes.py'],
    dag=dag
)

# Task menghasilkan laporan
generate_report = BashOperator(
    task_id='generate_report',
    bash_command='python3 /usr/local/airflow/scripts/generate_report.py',
    dag=dag
)

# Set dependency graph
ingest >> validate_data >> transform >> predict >> generate_report

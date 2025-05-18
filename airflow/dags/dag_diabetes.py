from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# Parameterisasi untuk fleksibilitas
EMAILS = ["admin@example.com"]  # Ganti dengan email yang sesuai
DATA_VALIDATION_THRESHOLD = 0.95  # Threshold validasi data (95%)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': EMAILS,
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

# Task untuk memastikan direktori data tersedia
check_dirs = BashOperator(
    task_id='check_directories',
    bash_command='mkdir -p /data/bronze /data/silver /data/gold /data/logs',
    dag=dag
)

# Task ingest data
ingest = BashOperator(
    task_id='ingest_data',
    bash_command='bash /scripts/ingest_data.sh',
    dag=dag
)

# Sensor untuk memastikan file data tersedia setelah ingest
check_data_available = FileSensor(
    task_id='check_data_available',
    filepath='/data/bronze/',
    poke_interval=60,  # cek setiap 60 detik
    timeout=600,       # timeout setelah 10 menit
    mode='poke',       # mode poke lebih efisien untuk interval pendek
    dag=dag
)

# Task validasi data
validate_data = BashOperator(
    task_id='validate_data',
    bash_command='spark-submit /scripts/validate_data.py',
    dag=dag
)

# Task transformasi data (ETL)
transform = BashOperator(
    task_id='run_spark_etl',
    bash_command='spark-submit /scripts/etl_spark_job.py',
    dag=dag
)

# Task prediksi diabetes
predict = BashOperator(
    task_id='predict_diabetes',
    bash_command='spark-submit /scripts/predict_diabetes.py',
    dag=dag
)

# Task menghasilkan laporan
generate_report = BashOperator(
    task_id='generate_report',
    bash_command='python3 /scripts/generate_report.py',
    dag=dag
)

# Task notifikasi berhasil
success_notification = EmailOperator(
    task_id='send_success_email',
    to=EMAILS,
    subject='Pipeline Prediksi Diabetes Berhasil',
    html_content="""
        <h2>Pipeline Prediksi Diabetes Berhasil Dijalankan</h2>
        <p>Pipeline prediksi diabetes berhasil dijalankan pada {{ ds }}.</p>
        <p>Silakan cek hasil prediksi dan laporan di direktori data/gold.</p>
    """,
    dag=dag
)

# Set dependency graph
check_dirs >> ingest >> check_data_available >> validate_data >> transform >> predict >> generate_report >> success_notification

#!/bin/bash
set -e

# Function to wait for services
wait_for_service() {
    local host=$1
    local port=$2
    echo "Waiting for $host:$port..."
    while ! nc -z $host $port; do
        sleep 1
    done
    echo "$host:$port is available"
}

# Wait for required services
wait_for_service airflow-postgres 5432
wait_for_service namenode 9870
wait_for_service spark-master 7077

# Initialize database if not already done
if [ ! -f /opt/airflow/airflow_db_initialized ]; then
    echo "Initializing Airflow database..."
    airflow db init
    
    # Create admin user
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@diabetes-pipeline.com \
        --password admin
    
    # Mark as initialized
    touch /opt/airflow/airflow_db_initialized
fi

# Start scheduler in background
echo "Starting Airflow scheduler..."
airflow scheduler &

# Start webserver
echo "Starting Airflow webserver..."
exec airflow webserver --port 8080

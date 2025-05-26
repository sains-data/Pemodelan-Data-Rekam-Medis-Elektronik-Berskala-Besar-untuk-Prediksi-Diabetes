#!/bin/bash

# Spark Submit Wrapper for Airflow
# This script executes spark-submit on the Spark master container from Airflow

set -e

# Default values
MASTER="spark://spark-master:7077"
DEPLOY_MODE="client"
DRIVER_MEMORY="1g"
EXECUTOR_MEMORY="2g"
EXECUTOR_CORES="2"

# Check for version or help flags first
if [[ "$1" == "--version" || "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Executing: docker exec spark-master /spark/bin/spark-submit $1"
    docker exec spark-master /spark/bin/spark-submit "$1"
    exit $?
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --master)
            MASTER="$2"
            shift 2
            ;;
        --deploy-mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        --driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --executor-cores)
            EXECUTOR_CORES="$2"
            shift 2
            ;;
        --conf)
            CONF_ARGS+=" --conf $2"
            shift 2
            ;;
        --packages)
            PACKAGES="$2"
            shift 2
            ;;
        *.py)
            PYTHON_FILE="$1"
            shift
            ;;
        --*)
            APP_ARGS+=" $1 $2"
            shift 2
            ;;
        *)
            APP_ARGS+=" $1"
            shift
            ;;
    esac
done

# Build the spark-submit command
SPARK_CMD="/spark/bin/spark-submit"
SPARK_CMD+=" --master $MASTER"
SPARK_CMD+=" --deploy-mode $DEPLOY_MODE"
SPARK_CMD+=" --driver-memory $DRIVER_MEMORY"
SPARK_CMD+=" --executor-memory $EXECUTOR_MEMORY"
SPARK_CMD+=" --executor-cores $EXECUTOR_CORES"

if [ ! -z "$CONF_ARGS" ]; then
    SPARK_CMD+="$CONF_ARGS"
fi

if [ ! -z "$PACKAGES" ]; then
    SPARK_CMD+=" --packages $PACKAGES"
fi

# If PYTHON_FILE is specified, convert the path to use shared volume
if [ ! -z "$PYTHON_FILE" ]; then
    # Convert Airflow path to Spark path using shared volume
    SPARK_FILE=$(echo "$PYTHON_FILE" | sed 's|/opt/airflow/scripts|/opt/spark/scripts|')
    echo "Using shared volume path: $SPARK_FILE"
    SPARK_CMD+=" $SPARK_FILE"
else
    echo "Error: No Python file specified"
    exit 1
fi
SPARK_CMD+="$APP_ARGS"

echo "Executing: docker exec spark-master $SPARK_CMD"

# Execute spark-submit on the Spark master container
docker exec spark-master $SPARK_CMD

#!/usr/bin/env python3
# filepath: /mnt/2A28ACA028AC6C8F/Programming/bigdata/Prediksi_Diabetes/scripts/validate_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, min, max, avg, stddev, lit
import logging
import sys
import os
import json
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/data/logs/validate_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('validate_data')

# Threshold untuk validasi data
MIN_ROW_COUNT = 5  # Data harus memiliki minimal X baris
MAX_MISSING_PERCENT = 20  # Maksimal X% missing values
VALIDATION_THRESHOLD = 0.95  # 95% data valid

def create_spark_session():
    """Inisialisasi SparkSession dengan konfigurasi yang optimal"""
    logger.info("Inisialisasi Spark Session")
    return (SparkSession.builder
            .appName("Diabetes-Data-Validation")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "1g")
            .getOrCreate())

def load_data(spark):
    """Load data dari bronze zone"""
    logger.info("Loading data dari bronze zone")
    
    try:
        # Load dataset utama
        files = os.listdir("/data/bronze")
        pima_files = [f for f in files if f.startswith("pima_")]
        
        if not pima_files:
            logger.error("No pima diabetes data files found in bronze zone")
            return None
            
        logger.info(f"Found files: {pima_files}")
        pima = spark.read.csv("/data/bronze/pima_*.csv", 
                            header=False, 
                            inferSchema=True)
        
        # Rename kolom untuk kemudahan
        columns = ["Pregnancies", "Glucose", "BloodPressure", "SkinThickness", 
                "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"]
        for i, col_name in enumerate(columns):
            pima = pima.withColumnRenamed(f"_c{i}", col_name)
        
        logger.info(f"Data loaded successfully. Row count: {pima.count()}")
        return pima
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def validate_data_quality(df):
    """Validasi kualitas data"""
    logger.info("Validating data quality")
    
    validation_results = {}
    is_valid = True
    
    # Checking 1: Row count
    row_count = df.count()
    validation_results["row_count"] = row_count
    validation_results["row_count_status"] = "PASS" if row_count >= MIN_ROW_COUNT else "FAIL"
    if row_count < MIN_ROW_COUNT:
        is_valid = False
        logger.warning(f"Row count ({row_count}) is less than minimum ({MIN_ROW_COUNT})")
    
    # Checking 2: Missing values
    missing_values = {}
    total_missing_percent = 0
    
    for column in df.columns:
        missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        missing_percent = (missing_count / row_count) * 100 if row_count > 0 else 0
        missing_values[column] = {
            "count": missing_count,
            "percent": missing_percent,
            "status": "PASS" if missing_percent <= MAX_MISSING_PERCENT else "FAIL"
        }
        
        if missing_percent > MAX_MISSING_PERCENT:
            is_valid = False
            logger.warning(f"Column {column} has {missing_percent:.2f}% missing values, exceeding threshold of {MAX_MISSING_PERCENT}%")
        
        total_missing_percent += missing_percent
    
    validation_results["missing_values"] = missing_values
    validation_results["avg_missing_percent"] = total_missing_percent / len(df.columns)
    
    # Checking 3: Basic statistics
    stats = {}
    for column in df.columns:
        try:
            # Statistik hanya untuk kolom numerik
            if df.schema[column].dataType.typeName() in ["integer", "long", "double", "float"]:
                stats_df = df.select(
                    min(col(column)).alias("min"),
                    max(col(column)).alias("max"),
                    avg(col(column)).alias("avg"),
                    stddev(col(column)).alias("stddev")
                ).collect()[0]
                
                stats[column] = {
                    "min": stats_df["min"],
                    "max": stats_df["max"],
                    "avg": stats_df["avg"],
                    "stddev": stats_df["stddev"]
                }
                
                # Validasi range (contoh: glucose tidak boleh negatif)
                if column in ["Glucose", "BloodPressure", "BMI", "Insulin"] and stats_df["min"] <= 0:
                    validation_results[f"{column}_range"] = "FAIL"
                    is_valid = False
                    logger.warning(f"Column {column} has negative or zero values which is invalid")
        except Exception as e:
            logger.warning(f"Couldn't compute statistics for column {column}: {str(e)}")
    
    validation_results["statistics"] = stats
    
    # Checking 4: Outcome distribution
    try:
        outcome_counts = df.groupby("Outcome").count().collect()
        outcome_dist = {str(row["Outcome"]): row["count"] for row in outcome_counts}
        validation_results["outcome_distribution"] = outcome_dist
        
        # Ensure we have both positive and negative cases
        if len(outcome_dist) < 2:
            validation_results["outcome_balance"] = "FAIL"
            is_valid = False
            logger.warning("Dataset doesn't have both positive and negative diabetes cases")
    except Exception as e:
        logger.warning(f"Couldn't analyze outcome distribution: {str(e)}")
    
    # Overall validation result
    validation_results["is_valid"] = is_valid
    
    if is_valid:
        logger.info("Data validation PASSED")
    else:
        logger.warning("Data validation FAILED")
    
    return validation_results

def save_validation_results(results):
    """Save validation results to logs directory"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as JSON
    with open(f"/data/logs/data_validation_{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Validation results saved to /data/logs/data_validation_{timestamp}.json")

def main():
    """Main validation process"""
    logger.info("Starting data validation process")
    
    spark = create_spark_session()
    
    try:
        # Load data
        df = load_data(spark)
        if df is None:
            logger.error("No data available for validation")
            sys.exit(1)
        
        # Validate data quality
        validation_results = validate_data_quality(df)
        
        # Save validation results
        save_validation_results(validation_results)
        
        # Return exit code based on validation results
        if not validation_results["is_valid"]:
            logger.error("Data validation failed")
            sys.exit(1)
        
        logger.info("Data validation process completed successfully")
        
    except Exception as e:
        logger.error(f"Data validation process failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()

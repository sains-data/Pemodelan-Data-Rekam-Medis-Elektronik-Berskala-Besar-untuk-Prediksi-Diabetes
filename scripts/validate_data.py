#!/usr/bin/env python3
# filepath: /mnt/2A28ACA028AC6C8F/Programming/bigdata/Prediksi_Diabetes/scripts/validate_data.py

import pandas as pd
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
        logging.FileHandler('/usr/local/airflow/data/logs/validate_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('validate_data')

# Threshold untuk validasi data
MIN_ROW_COUNT = 5  # Data harus memiliki minimal X baris
MAX_MISSING_PERCENT = 20  # Maksimal X% missing values
VALIDATION_THRESHOLD = 0.95  # 95% data valid

def load_data():
    """Load data dari bronze zone menggunakan pandas"""
    logger.info("Loading data dari bronze zone dengan pandas")
    try:
        files = os.listdir("/usr/local/airflow/data/bronze")
        pima_files = [f for f in files if f.startswith("pima_")]
        if not pima_files:
            logger.error("No pima diabetes data files found in bronze zone")
            return None
        logger.info(f"Found files: {pima_files}")
        # Load all matching CSVs and concatenate
        dfs = [pd.read_csv(f"/usr/local/airflow/data/bronze/{f}", header=None) for f in pima_files]
        pima = pd.concat(dfs, ignore_index=True)
        columns = ["Pregnancies", "Glucose", "BloodPressure", "SkinThickness", \
                   "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"]
        pima.columns = columns
        logger.info(f"Data loaded successfully. Row count: {len(pima)}")
        return pima
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def validate_data_quality(df):
    logger.info("Validating data quality (pandas)")
    validation_results = {}
    is_valid = True
    row_count = len(df)
    validation_results["row_count"] = row_count
    validation_results["row_count_status"] = "PASS" if row_count >= MIN_ROW_COUNT else "FAIL"
    if row_count < MIN_ROW_COUNT:
        is_valid = False
        logger.warning(f"Row count ({row_count}) is less than minimum ({MIN_ROW_COUNT})")
    missing_values = {}
    total_missing_percent = 0
    for column in df.columns:
        missing_count = df[column].isnull().sum()
        missing_percent = (missing_count / row_count) * 100 if row_count > 0 else 0
        missing_values[column] = {
            "count": int(missing_count),
            "percent": missing_percent,
            "status": "PASS" if missing_percent <= MAX_MISSING_PERCENT else "FAIL"
        }
        if missing_percent > MAX_MISSING_PERCENT:
            is_valid = False
            logger.warning(f"Column {column} has {missing_percent:.2f}% missing values, exceeding threshold of {MAX_MISSING_PERCENT}%")
        total_missing_percent += missing_percent
    validation_results["missing_values"] = missing_values
    validation_results["avg_missing_percent"] = total_missing_percent / len(df.columns)
    stats = {}
    for column in df.select_dtypes(include=["number"]):
        try:
            stats[column] = {
                "min": df[column].min(),
                "max": df[column].max(),
                "avg": df[column].mean(),
                "stddev": df[column].std()
            }
            if column in ["Glucose", "BloodPressure", "BMI", "Insulin"] and df[column].min() <= 0:
                validation_results[f"{column}_range"] = "FAIL"
                is_valid = False
                logger.warning(f"Column {column} has negative or zero values which is invalid")
        except Exception as e:
            logger.warning(f"Couldn't compute statistics for column {column}: {str(e)}")
    validation_results["statistics"] = stats
    try:
        outcome_dist = df["Outcome"].value_counts().to_dict()
        validation_results["outcome_distribution"] = outcome_dist
        if len(outcome_dist) < 2:
            validation_results["outcome_balance"] = "FAIL"
            is_valid = False
            logger.warning("Dataset doesn't have both positive and negative diabetes cases")
    except Exception as e:
        logger.warning(f"Couldn't analyze outcome distribution: {str(e)}")
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
    with open(f"/usr/local/airflow/data/logs/data_validation_{timestamp}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Validation results saved to /usr/local/airflow/data/logs/data_validation_{timestamp}.json")

def main():
    logger.info("Starting data validation process (pandas)")
    try:
        df = load_data()
        if df is None:
            logger.error("No data available for validation")
            sys.exit(1)
        validation_results = validate_data_quality(df)
        save_validation_results(validation_results)
        if not validation_results["is_valid"]:
            logger.error("Data validation failed")
            sys.exit(1)
        logger.info("Data validation process completed successfully")
    except Exception as e:
        logger.error(f"Data validation process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Data Ingestion Script for Diabetes Prediction Pipeline
Kelompok 8 RA - Big Data Project

This script handles data ingestion from various sources into HDFS Bronze layer using PySpark.
"""

import sys
import logging
from datetime import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    """
    Data Ingestion Pipeline for diabetes prediction project using PySpark
    """
    
    def __init__(self):
        """
        Initialize the ingestion pipeline with Spark session
        """
        try:
            self.spark = SparkSession.builder \
                .appName("DiabetesDataIngestion") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def validate_data(self, df, dataset_name):
        """
        Validate data quality before ingestion
        
        Args:
            df (pyspark.sql.DataFrame): Data to validate
            dataset_name (str): Name of the dataset
            
        Returns:
            bool: True if validation passes
        """
        logger.info(f"Validating {dataset_name} dataset...")
        
        # Check if dataframe is empty
        if df.count() == 0:
            logger.error(f"{dataset_name}: Dataset is empty")
            return False
        
        # Check for null values
        null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).collect()[0]
        
        total_nulls = sum(null_counts.asDict().values())
        if total_nulls > 0:
            logger.warning(f"{dataset_name}: Found {total_nulls} null values")
            for col_name, count_val in null_counts.asDict().items():
                if count_val > 0:
                    logger.warning(f"  - {col_name}: {count_val} nulls")
        
        # Dataset-specific validations
        if dataset_name == "diabetes":
            required_columns = ['Pregnancies', 'Glucose', 'BloodPressure', 'BMI', 'Age', 'Outcome']
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"{dataset_name}: Missing required columns: {missing_cols}")
                return False
                
            # Check glucose values are reasonable (0-500 mg/dL)
            glucose_stats = df.select('Glucose').describe().collect()
            logger.info(f"Glucose statistics: {glucose_stats}")
        
        elif dataset_name == "personal_health":
            required_columns = ['User_ID', 'Age', 'Heart_Rate', 'Health_Score']
            available_cols = [col for col in required_columns if col in df.columns]
            if len(available_cols) < len(required_columns):
                logger.warning(f"{dataset_name}: Some required columns missing, proceeding with available: {available_cols}")
        
        logger.info(f"{dataset_name}: Validation passed - {df.count()} records")
        return True
    
    def ingest_csv_to_hdfs(self, local_path, hdfs_path, dataset_name):
        """
        Ingest CSV file to HDFS
        
        Args:
            local_path (str): Local file path
            hdfs_path (str): HDFS destination path
            dataset_name (str): Name of the dataset
        """
        try:
            logger.info(f"Ingesting {dataset_name} from {local_path} to {hdfs_path}")
            
            # Read and validate data
            df = pd.read_csv(local_path)
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            
            if not self.validate_data(df, dataset_name):
                raise ValueError(f"Data validation failed for {dataset_name}")
            
            # Create bronze directory if not exists
            bronze_dir = os.path.dirname(hdfs_path)
            try:
                self.hdfs_client.makedirs(bronze_dir)
            except:
                pass  # Directory might already exist
            
            # Write to HDFS
            with self.hdfs_client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                df.to_csv(writer, index=False)
            
            # Add metadata
            metadata = {
                'dataset_name': dataset_name,
                'ingestion_timestamp': datetime.now().isoformat(),
                'rows': len(df),
                'columns': len(df.columns),
                'source_path': local_path,
                'hdfs_path': hdfs_path
            }
            
            metadata_path = hdfs_path.replace('.csv', '_metadata.json')
            with self.hdfs_client.write(metadata_path, overwrite=True, encoding='utf-8') as writer:
                import json
                json.dump(metadata, writer, indent=2)
            
            logger.info(f"Successfully ingested {dataset_name} to HDFS")
            
        except Exception as e:
            logger.error(f"Failed to ingest {dataset_name}: {e}")
            raise
    
    def ingest_diabetes_data(self):
        """Ingest diabetes dataset"""
        self.ingest_csv_to_hdfs(
            local_path='/opt/airflow/data/diabetes.csv',
            hdfs_path='/data/bronze/diabetes/diabetes.csv',
            dataset_name='diabetes'
        )
    
    def ingest_personal_health_data(self):
        """Ingest personal health dataset"""
        self.ingest_csv_to_hdfs(
            local_path='/opt/airflow/data/personal_health_data.csv',
            hdfs_path='/data/bronze/personal_health/personal_health_data.csv',
            dataset_name='personal_health'
        )
    
    def run_full_ingestion(self):
        """Run complete data ingestion pipeline"""
        logger.info("Starting full data ingestion pipeline...")
        
        try:
            # Ingest all datasets
            self.ingest_diabetes_data()
            self.ingest_personal_health_data()
            
            logger.info("Full ingestion pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Ingestion pipeline failed: {e}")
            sys.exit(1)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Data Ingestion Pipeline')
    parser.add_argument('--dataset', choices=['diabetes', 'personal_health', 'all'], 
                       default='all', help='Dataset to ingest')
    parser.add_argument('--hdfs-url', default='http://namenode:9870', 
                       help='HDFS URL')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = DataIngestionPipeline(hdfs_url=args.hdfs_url)
    
    # Run ingestion based on dataset selection
    if args.dataset == 'diabetes':
        pipeline.ingest_diabetes_data()
    elif args.dataset == 'personal_health':
        pipeline.ingest_personal_health_data()
    else:
        pipeline.run_full_ingestion()

if __name__ == "__main__":
    main()

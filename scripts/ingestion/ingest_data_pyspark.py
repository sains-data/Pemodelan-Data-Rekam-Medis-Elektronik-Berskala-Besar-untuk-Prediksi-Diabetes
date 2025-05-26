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
from pyspark.sql.functions import col, isnan, when, count, lit, current_timestamp

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
        row_count = df.count()
        if row_count == 0:
            logger.error(f"{dataset_name}: Dataset is empty")
            return False
        
        # Check for null values
        logger.info(f"Dataset info: {row_count} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {df.columns}")
        
        # Dataset-specific validations
        if dataset_name == "diabetes":
            expected_columns = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 
                              'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome']
            available_columns = df.columns
            
            # Check if we have most expected columns (flexible validation)
            if 'Glucose' not in available_columns or 'Outcome' not in available_columns:
                logger.error(f"{dataset_name}: Missing critical columns (Glucose or Outcome)")
                return False
                
        logger.info(f"{dataset_name}: Validation passed - {row_count} records")
        return True
    
    def ingest_csv_to_hdfs(self, source_path, target_path, dataset_name):
        """
        Ingest CSV file to HDFS using PySpark
        
        Args:
            source_path (str): Local path to CSV file
            target_path (str): HDFS target path
            dataset_name (str): Name of the dataset
        """
        try:
            logger.info(f"Ingesting {dataset_name} from {source_path} to {target_path}")
            
            # Read CSV file
            df = self.spark.read.csv(source_path, header=True, inferSchema=True)
            
            if not self.validate_data(df, dataset_name):
                raise ValueError(f"Data validation failed for {dataset_name}")
            
            # Add ingestion metadata
            df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp()) \
                                .withColumn("dataset_name", lit(dataset_name))
            
            # Write to HDFS in Parquet format for better performance
            target_parquet = target_path.replace('.csv', '.parquet')
            df_with_metadata.write.mode("overwrite").parquet(target_parquet)
            
            # Also write in CSV format for compatibility
            df_with_metadata.write.mode("overwrite").option("header", "true").csv(target_path)
            
            logger.info(f"Successfully ingested {dataset_name} to HDFS")
            logger.info(f"  - Parquet: {target_parquet}")
            logger.info(f"  - CSV: {target_path}")
            
        except Exception as e:
            logger.error(f"Failed to ingest {dataset_name}: {e}")
            raise
    
    def ingest_diabetes_data(self, source_path, target_path):
        """Ingest diabetes dataset"""
        self.ingest_csv_to_hdfs(
            source_path=f"{source_path}/diabetes.csv",
            target_path=f"{target_path}/diabetes",
            dataset_name='diabetes'
        )
    
    def ingest_personal_health_data(self, source_path, target_path):
        """Ingest personal health dataset"""
        try:
            self.ingest_csv_to_hdfs(
                source_path=f"{source_path}/personal_health_data.csv",
                target_path=f"{target_path}/personal_health",
                dataset_name='personal_health'
            )
        except Exception as e:
            logger.warning(f"Could not ingest personal health data: {e}")
    
    def run_full_ingestion(self, source_path, target_path):
        """Run complete data ingestion pipeline"""
        logger.info("Starting full data ingestion pipeline...")
        
        try:
            # Ingest diabetes dataset (primary)
            self.ingest_diabetes_data(source_path, target_path)
            
            # Ingest personal health dataset (optional)
            self.ingest_personal_health_data(source_path, target_path)
            
            logger.info("Full ingestion pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Ingestion pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Data Ingestion Pipeline')
    parser.add_argument('--source-path', required=True, help='Source data path')
    parser.add_argument('--target-path', required=True, help='Target HDFS path')
    parser.add_argument('--dataset', choices=['diabetes', 'personal_health', 'all'], 
                       default='all', help='Dataset to ingest')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Initialize pipeline
    pipeline = DataIngestionPipeline()
    
    # Run ingestion based on dataset selection
    try:
        if args.dataset == 'diabetes':
            pipeline.ingest_diabetes_data(args.source_path, args.target_path)
        elif args.dataset == 'personal_health':
            pipeline.ingest_personal_health_data(args.source_path, args.target_path)
        else:
            pipeline.run_full_ingestion(args.source_path, args.target_path)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)
    finally:
        if 'pipeline' in locals():
            pipeline.spark.stop()

if __name__ == "__main__":
    main()

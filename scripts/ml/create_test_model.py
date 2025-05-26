#!/usr/bin/env python3
"""
Create a test model in HDFS for evaluation testing
"""

import os
import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Create a test model in HDFS")
    parser.add_argument("--output-path", default="hdfs://namenode:9000/models/random_forest_latest",
                        help="Path to save the test model")
    return parser.parse_args()

def init_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("CreateTestModel") \
        .getOrCreate()

def create_dummy_data(spark):
    """Create dummy data for model training"""
    # Create a simple DataFrame with two features and binary label
    data = [(Vectors.dense([0.0, 0.0]), 0.0),
            (Vectors.dense([0.0, 1.0]), 0.0),
            (Vectors.dense([1.0, 0.0]), 1.0),
            (Vectors.dense([1.0, 1.0]), 1.0)]
    df = spark.createDataFrame(data, ["features", "diabetes"])
    return df

def create_and_save_model(spark, output_path):
    """Create and save a test model"""
    try:
        # Create dummy data
        df = create_dummy_data(spark)
        
        # Create and fit a logistic regression model
        lr = LogisticRegression(featuresCol="features", labelCol="diabetes", maxIter=10)
        model = lr.fit(df)
        
        # Save model
        logger.info(f"Saving model to {output_path}")
        model.write().overwrite().save(output_path)
        logger.info("Model saved successfully")
        
        # Verify model exists
        try:
            sc = spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            hdfs_path = sc._jvm.org.apache.hadoop.fs.Path(output_path)
            
            if fs.exists(hdfs_path):
                logger.info(f"Verified: Model path exists at {output_path}")
                
                # List files in model directory
                file_statuses = fs.listStatus(hdfs_path)
                logger.info(f"Model directory contains {len(file_statuses)} files/directories")
                for status in file_statuses:
                    path_str = str(status.getPath())
                    is_dir = status.isDirectory()
                    logger.info(f"  {'[DIR] ' if is_dir else '[FILE]'} {path_str}")
            else:
                logger.error(f"Model path verification failed: Path does not exist: {output_path}")
        except Exception as e:
            logger.error(f"Model path verification error: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating/saving model: {e}")
        return False

def main():
    """Main execution function"""
    args = parse_args()
    logger.info(f"Creating test model at path: {args.output_path}")
    
    # Initialize Spark session
    spark = init_spark_session()
    
    # Create and save model
    success = create_and_save_model(spark, args.output_path)
    
    # Also create a model with "logistic_regression" in the path
    if success:
        lr_path = args.output_path.replace("random_forest_latest", "logistic_regression_latest")
        logger.info(f"Creating additional logistic regression model at: {lr_path}")
        create_and_save_model(spark, lr_path)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()

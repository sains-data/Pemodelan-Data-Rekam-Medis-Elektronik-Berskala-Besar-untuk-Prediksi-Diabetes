#!/usr/bin/env python3
"""
Diabetes Prediction Model Training Script (PySpark Only)
Simplified version using only PySpark MLlib without external dependencies
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, avg, stddev
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiabetesModelTrainer:
    """
    Diabetes prediction model trainer using only Spark MLlib
    """
    
    def __init__(self, spark_session):
        """
        Initialize the model trainer
        
        Args:
            spark_session: Active Spark session
        """
        self.spark = spark_session
        self.trained_pipelines = {}
        
    def load_data(self, input_path):
        """
        Load processed data from Gold layer
        
        Args:
            input_path (str): Path to Gold layer data in HDFS
            
        Returns:
            DataFrame: Spark DataFrame with processed features
        """
        try:
            logger.info(f"Loading data from Gold layer: {input_path}")
            df = self.spark.read.parquet(input_path)
            
            logger.info(f"Data loaded successfully. Shape: {df.count()} x {len(df.columns)}")
            logger.info(f"Columns: {df.columns}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def data_quality_check(self, df):
        """
        Perform data quality checks on the dataset
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        logger.info("Performing data quality checks...")
        
        # Get numeric columns only for isnan check
        numeric_cols = [col_name for col_name, dtype in df.dtypes 
                       if dtype in ['int', 'double', 'float', 'long', 'short']]
        
        # Check for missing values - use isnan only for numeric columns
        missing_checks = []
        for c in df.columns:
            if c in numeric_cols:
                missing_checks.append(count(when(col(c).isNull() | isnan(col(c)), c)).alias(c))
            else:
                missing_checks.append(count(when(col(c).isNull(), c)).alias(c))
        
        missing_counts = df.select(missing_checks).collect()[0]
        
        total_rows = df.count()
        for column, missing_count in missing_counts.asDict().items():
            missing_pct = (missing_count / total_rows) * 100 if total_rows > 0 else 0
            logger.info(f"Column '{column}': {missing_count} missing values ({missing_pct:.2f}%)")
        
        # Remove rows with excessive missing values (>50% of features)
        threshold = len(df.columns) * 0.5
        df_clean = df.dropna(thresh=int(threshold))
        
        logger.info(f"Data quality check complete. Removed {total_rows - df_clean.count()} rows")
        
        return df_clean
    
    def prepare_features(self, df, target_column='diabetes'):
        """
        Prepare features for machine learning
        
        Args:
            df: Input DataFrame
            target_column: Name of target column
            
        Returns:
            DataFrame: DataFrame with features vector
        """
        logger.info("Preparing features...")
        
        # Get feature columns (exclude target and metadata columns including timestamp)
        exclude_cols = [target_column, 'patient_id', 'created_at', 'processed_at', 'processed_timestamp']
        
        # Only use numeric columns for VectorAssembler
        numeric_types = ['int', 'double', 'float', 'long', 'short', 'bigint']
        feature_cols = [col_name for col_name, dtype in df.dtypes 
                       if col_name not in exclude_cols and 
                       not col_name.endswith('_timestamp') and
                       dtype in numeric_types]
        
        logger.info(f"Feature columns: {feature_cols}")
        
        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Create pipeline for feature preparation
        feature_pipeline = Pipeline(stages=[assembler, scaler])
        
        # Fit and transform
        feature_model = feature_pipeline.fit(df)
        df_features = feature_model.transform(df)
        
        # Rename target column to label for MLlib
        df_features = df_features.withColumnRenamed(target_column, "label")
        
        logger.info("Feature preparation complete")
        
        return df_features, feature_model
    
    def train_models(self, df):
        """
        Train multiple models for diabetes prediction
        
        Args:
            df: DataFrame with features and labels
            
        Returns:
            dict: Dictionary of trained models
        """
        logger.info("Training machine learning models...")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        logger.info(f"Training set size: {train_df.count()}")
        logger.info(f"Test set size: {test_df.count()}")
        
        models = {}
        
        # 1. Random Forest
        logger.info("Training Random Forest...")
        rf = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="label",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        rf_model = rf.fit(train_df)
        models['random_forest'] = rf_model
        
        # 2. Logistic Regression
        logger.info("Training Logistic Regression...")
        lr = LogisticRegression(
            featuresCol="scaled_features",
            labelCol="label",
            maxIter=100,
            regParam=0.01
        )
        lr_model = lr.fit(train_df)
        models['logistic_regression'] = lr_model
        
        # Evaluate models
        self.evaluate_models(models, test_df)
        
        return models, train_df, test_df
    
    def evaluate_models(self, models, test_df):
        """
        Evaluate trained models
        
        Args:
            models: Dictionary of trained models
            test_df: Test DataFrame
        """
        logger.info("Evaluating models...")
        
        # Binary classification evaluator
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        # Multiclass evaluator
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        for model_name, model in models.items():
            logger.info(f"Evaluating {model_name}...")
            
            # Make predictions
            predictions = model.transform(test_df)
            
            # Calculate metrics
            auc = binary_evaluator.evaluate(predictions)
            accuracy = multiclass_evaluator.evaluate(predictions)
            
            logger.info(f"{model_name} - AUC: {auc:.4f}, Accuracy: {accuracy:.4f}")
    
    def save_models(self, models, feature_model, output_path):
        """
        Save trained models to HDFS
        
        Args:
            models: Dictionary of trained models
            feature_model: Feature preparation pipeline
            output_path: Path to save models
        """
        logger.info(f"Saving models to {output_path}...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Save feature preparation pipeline
            feature_path = f"{output_path}/feature_pipeline_{timestamp}"
            feature_model.write().overwrite().save(feature_path)
            logger.info(f"Feature pipeline saved to {feature_path}")
            
            # Save each model
            for model_name, model in models.items():
                model_path = f"{output_path}/{model_name}_{timestamp}"
                model.write().overwrite().save(model_path)
                logger.info(f"{model_name} saved to {model_path}")
                
        except Exception as e:
            logger.error(f"Failed to save models: {e}")
            raise

def main():
    """
    Main training pipeline
    """
    # Parse command line arguments
    if len(sys.argv) != 3:
        print("Usage: python train_model_pyspark.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DiabetesModelTraining") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        logger.info("Starting diabetes prediction model training...")
        
        # Initialize trainer
        trainer = DiabetesModelTrainer(spark)
        
        # Load data
        df = trainer.load_data(input_path)
        
        # Data quality checks
        df_clean = trainer.data_quality_check(df)
        
        # Prepare features
        df_features, feature_model = trainer.prepare_features(df_clean)
        
        # Train models
        models, train_df, test_df = trainer.train_models(df_features)
        
        # Save models
        trainer.save_models(models, feature_model, output_path)
        
        logger.info("Model training completed successfully!")
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

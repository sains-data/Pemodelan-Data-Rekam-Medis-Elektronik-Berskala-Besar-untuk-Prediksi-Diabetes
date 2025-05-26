#!/usr/bin/env python3
"""
Diabetes Prediction Model Training Script
Kelompok 8 RA - Big Data Project

This script trains machine learning models for diabetes prediction using Spark MLlib.
Implements the medallion architecture (Gold layer consumption) and saves models for deployment.
"""

import argparse
import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, avg, stddev
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiabetesModelTrainer:
    """
    Diabetes prediction model trainer using Spark MLlib
    """
    
    def __init__(self, spark_session):
        """
        Initialize the model trainer
        
        Args:
            spark_session: Active Spark session
        """
        self.spark = spark_session
        self.models = {}
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
        
        # Check for missing values
        missing_counts = df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) 
                                   for c in df.columns]).collect()[0]
        
        total_rows = df.count()
        for column, missing_count in missing_counts.asDict().items():
            missing_pct = (missing_count / total_rows) * 100
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
            df: Spark DataFrame
            target_column (str): Name of target column
            
        Returns:
            tuple: (features_df, feature_columns)
        """
        logger.info("Preparing features for machine learning...")
        
        # Identify feature columns (exclude target)
        feature_columns = [col for col in df.columns if col != target_column]
        
        # Cast all feature columns to double type
        for col_name in feature_columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Create features vector
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features_raw"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Create preprocessing pipeline
        preprocessing_pipeline = Pipeline(stages=[assembler, scaler])
        
        # Fit and transform
        preprocessing_model = preprocessing_pipeline.fit(df)
        features_df = preprocessing_model.transform(df)
        
        logger.info(f"Features prepared. Feature columns: {feature_columns}")
        
        return features_df, feature_columns, preprocessing_model
    
    def split_data(self, df, train_ratio=0.8, validation_ratio=0.1, test_ratio=0.1):
        """
        Split data into train, validation, and test sets
        
        Args:
            df: Spark DataFrame
            train_ratio (float): Training data ratio
            validation_ratio (float): Validation data ratio
            test_ratio (float): Test data ratio
            
        Returns:
            tuple: (train_df, val_df, test_df)
        """
        logger.info(f"Splitting data: train={train_ratio}, val={validation_ratio}, test={test_ratio}")
        
        # First split: train vs (validation + test)
        train_df, temp_df = df.randomSplit([train_ratio, validation_ratio + test_ratio], seed=42)
        
        # Second split: validation vs test
        val_ratio_adjusted = validation_ratio / (validation_ratio + test_ratio)
        val_df, test_df = temp_df.randomSplit([val_ratio_adjusted, 1 - val_ratio_adjusted], seed=42)
        
        logger.info(f"Data split complete:")
        logger.info(f"  Training: {train_df.count()} rows")
        logger.info(f"  Validation: {val_df.count()} rows")
        logger.info(f"  Test: {test_df.count()} rows")
        
        return train_df, val_df, test_df
    
    def train_random_forest(self, train_df, val_df):
        """
        Train Random Forest classifier with hyperparameter tuning
        
        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            
        Returns:
            CrossValidatorModel: Trained model with best parameters
        """
        logger.info("Training Random Forest classifier...")
        
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="diabetes",
            predictionCol="prediction",
            probabilityCol="probability",
            seed=42
        )
        
        # Parameter grid for hyperparameter tuning
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [50, 100, 200]) \
            .addGrid(rf.maxDepth, [5, 10, 15]) \
            .addGrid(rf.minInstancesPerNode, [1, 5, 10]) \
            .build()
        
        # Evaluator
        evaluator = BinaryClassificationEvaluator(
            labelCol="diabetes",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        # Cross validator
        cv = CrossValidator(
            estimator=rf,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        # Train model
        cv_model = cv.fit(train_df)
        
        # Get best model
        best_rf_model = cv_model.bestModel
        
        logger.info(f"Random Forest training complete.")
        logger.info(f"Best parameters: numTrees={best_rf_model.getNumTrees}, "
                   f"maxDepth={best_rf_model.getMaxDepth()}")
        
        return cv_model
    
    def train_logistic_regression(self, train_df, val_df):
        """
        Train Logistic Regression classifier
        
        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            
        Returns:
            LogisticRegressionModel: Trained model
        """
        logger.info("Training Logistic Regression classifier...")
        
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="diabetes",
            predictionCol="prediction",
            probabilityCol="probability",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.1
        )
        
        # Train model
        lr_model = lr.fit(train_df)
        
        logger.info("Logistic Regression training complete.")
        
        return lr_model
    
    def train_gradient_boosting(self, train_df, val_df):
        """
        Train Gradient Boosting classifier
        
        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            
        Returns:
            GBTClassificationModel: Trained model
        """
        logger.info("Training Gradient Boosting classifier...")
        
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="diabetes",
            predictionCol="prediction",
            maxIter=100,
            maxDepth=5,
            seed=42
        )
        
        # Train model
        gbt_model = gbt.fit(train_df)
        
        logger.info("Gradient Boosting training complete.")
        
        return gbt_model
    
    def evaluate_model(self, model, test_df, model_name):
        """
        Evaluate model performance
        
        Args:
            model: Trained model
            test_df: Test DataFrame
            model_name (str): Name of the model
            
        Returns:
            dict: Evaluation metrics
        """
        logger.info(f"Evaluating {model_name} model...")
        
        # Make predictions
        predictions = model.transform(test_df)
        
        # Binary classification metrics
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="diabetes",
            rawPredictionCol="rawPrediction"
        )
        
        # Multiclass classification metrics
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="diabetes",
            predictionCol="prediction"
        )
        
        # Calculate metrics
        auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
        accuracy = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "accuracy"})
        precision = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "weightedPrecision"})
        recall = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "weightedRecall"})
        f1 = multiclass_evaluator.evaluate(predictions, {multiclass_evaluator.metricName: "f1"})
        
        metrics = {
            "model_name": model_name,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "auc_roc": auc,
            "evaluation_timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"{model_name} Evaluation Results:")
        logger.info(f"  Accuracy: {accuracy:.4f}")
        logger.info(f"  Precision: {precision:.4f}")
        logger.info(f"  Recall: {recall:.4f}")
        logger.info(f"  F1-Score: {f1:.4f}")
        logger.info(f"  AUC-ROC: {auc:.4f}")
        
        return metrics, predictions
    
    def save_model(self, model, model_path, model_name):
        """
        Save trained model to HDFS
        
        Args:
            model: Trained model
            model_path (str): Base path for saving models
            model_name (str): Name of the model
        """
        full_path = f"{model_path}/{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            model.write().overwrite().save(full_path)
            logger.info(f"Model {model_name} saved to: {full_path}")
            return full_path
        except Exception as e:
            logger.error(f"Failed to save model {model_name}: {e}")
            raise
    
    def run_training_pipeline(self, input_path, model_output_path, model_types=['randomforest']):
        """
        Run the complete model training pipeline
        
        Args:
            input_path (str): Path to Gold layer data
            model_output_path (str): Path to save trained models
            model_types (list): List of model types to train
            
        Returns:
            dict: Training results and metrics
        """
        logger.info("Starting diabetes prediction model training pipeline...")
        
        # Load data
        df = self.load_data(input_path)
        
        # Data quality checks
        df_clean = self.data_quality_check(df)
        
        # Prepare features
        features_df, feature_columns, preprocessing_model = self.prepare_features(df_clean)
        
        # Split data
        train_df, val_df, test_df = self.split_data(features_df)
        
        # Train models
        results = {}
        
        for model_type in model_types:
            logger.info(f"Training {model_type} model...")
            
            if model_type == 'randomforest':
                model = self.train_random_forest(train_df, val_df)
                best_model = model.bestModel
            elif model_type == 'logistic':
                best_model = self.train_logistic_regression(train_df, val_df)
            elif model_type == 'gbt':
                best_model = self.train_gradient_boosting(train_df, val_df)
            else:
                logger.warning(f"Unknown model type: {model_type}")
                continue
            
            # Create full pipeline (preprocessing + model)
            full_pipeline = Pipeline(stages=[preprocessing_model, best_model])
            full_model = full_pipeline.fit(df_clean.select(*feature_columns + ['diabetes']))
            
            # Evaluate model
            metrics, predictions = self.evaluate_model(full_model, test_df, model_type)
            
            # Save model
            model_path = self.save_model(full_model, model_output_path, model_type)
            
            results[model_type] = {
                'model_path': model_path,
                'metrics': metrics,
                'feature_columns': feature_columns
            }
        
        logger.info("Model training pipeline completed successfully!")
        return results

def main():
    """Main function to run the training pipeline"""
    parser = argparse.ArgumentParser(description='Train diabetes prediction models')
    parser.add_argument('--input-path', required=True, help='Input path for Gold layer data')
    parser.add_argument('--model-output-path', required=True, help='Output path for trained models')
    parser.add_argument('--model-type', default='randomforest', 
                       choices=['randomforest', 'logistic', 'gbt', 'all'],
                       help='Type of model to train')
    parser.add_argument('--evaluation-metrics', default='accuracy,precision,recall,f1,auc',
                       help='Evaluation metrics to compute')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DiabetesModelTraining") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        # Initialize trainer
        trainer = DiabetesModelTrainer(spark)
        
        # Determine model types to train
        if args.model_type == 'all':
            model_types = ['randomforest', 'logistic', 'gbt']
        else:
            model_types = [args.model_type]
        
        # Run training pipeline
        results = trainer.run_training_pipeline(
            input_path=args.input_path,
            model_output_path=args.model_output_path,
            model_types=model_types
        )
        
        # Print summary
        logger.info("Training Summary:")
        for model_type, result in results.items():
            metrics = result['metrics']
            logger.info(f"  {model_type}:")
            logger.info(f"    Accuracy: {metrics['accuracy']:.4f}")
            logger.info(f"    F1-Score: {metrics['f1_score']:.4f}")
            logger.info(f"    AUC-ROC: {metrics['auc_roc']:.4f}")
            logger.info(f"    Model Path: {result['model_path']}")
        
    except Exception as e:
        logger.error(f"Training pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

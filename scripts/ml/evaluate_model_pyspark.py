#!/usr/bin/env python3
"""
PySpark-Only Model Evaluation Script for Diabetes Prediction Pipeline
Kelompok 8 RA - Big Data Project

This script evaluates trained models using only PySpark MLlib without external dependencies.
"""

import argparse
import logging
import sys
import json
import re
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, count, avg, stddev, desc, asc, lit, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import VectorUDT, DenseVector, SparseVector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PySparkModelEvaluator:
    """
    PySpark-only model evaluation and performance analysis
    """
    
    def __init__(self, spark_session):
        """
        Initialize the model evaluator
        
        Args:
            spark_session: Active Spark session
        """
        self.spark = spark_session
        
        # Define UDF to extract probability for positive class from vector
        def extract_positive_probability(probability_vector):
            """Extract probability for positive class (index 1) from ML vector"""
            if probability_vector is not None:
                if isinstance(probability_vector, (DenseVector, SparseVector)):
                    # For binary classification, positive class probability is at index 1
                    return float(probability_vector[1]) if len(probability_vector) > 1 else 0.0
                else:
                    return 0.0
            return 0.0
            
        self.extract_prob_udf = udf(extract_positive_probability, DoubleType())
    
    def find_latest_models(self, models_base_path="/models"):
        """
        Find the latest trained models in HDFS using PySpark
        
        Args:
            models_base_path (str): Base path where models are stored
            
        Returns:
            dict: Dictionary with latest model paths for each model type
        """
        try:
            logger.info(f"Searching for latest models in {models_base_path}")
            
            # Try to use Spark to list HDFS directories
            try:
                # Use Spark's Hadoop FileSystem API
                from pyspark import SparkContext
                from pyspark.sql import SparkSession
                
                sc = self.spark.sparkContext
                hadoop_conf = sc._jsc.hadoopConfiguration()
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                path = sc._jvm.org.apache.hadoop.fs.Path(models_base_path)
                
                if not fs.exists(path):
                    logger.warning(f"Models directory {models_base_path} does not exist")
                    return self._get_fallback_models(models_base_path)
                
                # List files in the models directory
                file_statuses = fs.listStatus(path)
                model_paths = {}
                latest_timestamp = None
                
                # Pattern to match model directories with timestamps
                timestamp_pattern = r'(\d{8}_\d{6})$'
                
                for file_status in file_statuses:
                    if file_status.isDirectory():
                        dir_path = str(file_status.getPath())
                        dir_name = dir_path.split('/')[-1]
                        
                        # Extract model type and timestamp
                        timestamp_match = re.search(timestamp_pattern, dir_name)
                        if timestamp_match:
                            timestamp = timestamp_match.group(1)
                            model_type = dir_name.replace(f'_{timestamp}', '')
                            
                            # Track latest timestamp
                            if latest_timestamp is None or timestamp > latest_timestamp:
                                latest_timestamp = timestamp
                            
                            # Store model info
                            if model_type not in model_paths or timestamp > model_paths[model_type]['timestamp']:
                                model_paths[model_type] = {
                                    'path': f"hdfs://namenode:9000{dir_path}",
                                    'timestamp': timestamp
                                }
                
                # Filter to only include models with the latest timestamp
                latest_models = {}
                for model_type, info in model_paths.items():
                    if info['timestamp'] == latest_timestamp:
                        latest_models[model_type] = info['path']
                
                if latest_models:
                    logger.info(f"Found latest models with timestamp {latest_timestamp}: {list(latest_models.keys())}")
                    return latest_models
                else:
                    logger.warning("No models found using Spark FileSystem API")
                    return self._get_fallback_models(models_base_path)
                    
            except Exception as spark_error:
                logger.warning(f"Spark FileSystem API failed: {spark_error}")
                return self._try_subprocess_approach(models_base_path)
                
        except Exception as e:
            logger.error(f"Failed to find latest models: {e}")
            return self._get_fallback_models(models_base_path)
    
    def _try_subprocess_approach(self, models_base_path):
        """
        Fallback to subprocess approach for listing HDFS directories
        
        Args:
            models_base_path (str): Base path where models are stored
            
        Returns:
            dict: Dictionary with latest model paths for each model type
        """
        try:
            import subprocess
            
            logger.info("Trying subprocess approach for HDFS listing...")
            
            # Use hdfs command to list models directory
            cmd = ["hdfs", "dfs", "-ls", models_base_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                logger.warning(f"HDFS command failed: {result.stderr}")
                return self._get_fallback_models(models_base_path)
            
            # Parse the output to find model directories
            model_paths = {}
            latest_timestamp = None
            
            # Pattern to match model directories with timestamps
            # More flexible pattern to handle HDFS ls output formats
            timestamp_pattern = r'(/models/(\w+)_(\d{8}_\d{6}))'
            latest_pattern = r'(/models/(\w+)_latest)'
            
            logger.info(f"HDFS output: {result.stdout[:500]}...")
            
            for line in result.stdout.split('\n'):
                logger.info(f"Processing line: {line}")
                
                # First try to match timestamp pattern
                match = re.search(timestamp_pattern, line)
                if match:
                    relative_path = match.group(1)
                    full_path = f"hdfs://namenode:9000{relative_path}"
                    model_type = match.group(2)
                    timestamp = match.group(3)
                    logger.info(f"Found timestamped model: {model_type} at {full_path} with timestamp {timestamp}")
                else:
                    # Try to match latest pattern
                    match = re.search(latest_pattern, line)
                    if match:
                        relative_path = match.group(1)
                        full_path = f"hdfs://namenode:9000{relative_path}"
                        model_type = match.group(2)
                        # Use a high timestamp to ensure latest models are prioritized if no timestamp models exist
                        timestamp = "99999999_999999"
                        
                        # Keep track of the latest timestamp
                        if latest_timestamp is None or timestamp > latest_timestamp:
                            latest_timestamp = timestamp
                        
                        # Store the path for each model type with this timestamp
                        if model_type not in model_paths or timestamp > model_paths[model_type]['timestamp']:
                            model_paths[model_type] = {
                                'path': full_path,
                                'timestamp': timestamp
                            }
                    continue
                
                # For timestamp models, track latest and store path
                if latest_timestamp is None or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                
                # Store the path for each model type with this timestamp
                if model_type not in model_paths or timestamp > model_paths[model_type]['timestamp']:
                    model_paths[model_type] = {
                        'path': full_path,
                        'timestamp': timestamp
                    }
            
            # Filter to only include models with the latest timestamp
            latest_models = {}
            for model_type, info in model_paths.items():
                if info['timestamp'] == latest_timestamp:
                    latest_models[model_type] = info['path']
            
            if latest_models:
                logger.info(f"Found latest models with timestamp {latest_timestamp}: {list(latest_models.keys())}")
                return latest_models
            else:
                logger.warning("No models found using subprocess approach")
                return self._get_fallback_models(models_base_path)
                
        except Exception as e:
            logger.warning(f"Subprocess approach failed: {e}")
            return self._get_fallback_models(models_base_path)
    
    def _get_fallback_models(self, models_base_path):
        """
        Fallback model paths when auto-detection fails
        
        Args:
            models_base_path (str): Base path where models are stored
            
        Returns:
            dict: Dictionary with fallback model paths
        """
        logger.info("Using fallback model paths")
        
        # Generate current timestamp-based fallback paths
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        fallback_models = {
            'random_forest': f"hdfs://namenode:9000{models_base_path}/random_forest_latest",
            'logistic_regression': f"hdfs://namenode:9000{models_base_path}/logistic_regression_latest",
            'feature_pipeline': f"hdfs://namenode:9000{models_base_path}/feature_pipeline_latest"
        }
        
        # Try to find any existing model directories
        try:
            import subprocess
            result = subprocess.run(["hdfs", "dfs", "-ls", models_base_path], 
                                  capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                # Parse available models
                for line in result.stdout.split('\n'):
                    if 'random_forest' in line and models_base_path in line:
                        path_match = re.search(r'(/models/random_forest_[\w\d_]+)', line)
                        if path_match:
                            fallback_models['random_forest'] = f"hdfs://namenode:9000{path_match.group(1)}"
                    
                    if 'logistic_regression' in line and models_base_path in line:
                        path_match = re.search(r'(/models/logistic_regression_[\w\d_]+)', line)
                        if path_match:
                            fallback_models['logistic_regression'] = f"hdfs://namenode:9000{path_match.group(1)}"
                            
                    if 'feature_pipeline' in line and models_base_path in line:
                        path_match = re.search(r'(/models/feature_pipeline_[\w\d_]+)', line)
                        if path_match:
                            fallback_models['feature_pipeline'] = f"hdfs://namenode:9000{path_match.group(1)}"
                            
        except Exception as e:
            logger.warning(f"Fallback model detection failed: {e}")
        
        logger.info(f"Using fallback models: {list(fallback_models.keys())}")
        return fallback_models
    
    def load_model(self, model_path):
        """
        Load trained model from HDFS with improved error handling
        
        Args:
            model_path (str): Path to saved model
            
        Returns:
            Model: Loaded model (can be PipelineModel or individual classifier)
        """
        try:
            logger.info(f"Loading model from {model_path}")
            
            # First try to load as PipelineModel
            try:
                model = PipelineModel.load(model_path)
                logger.info("Model loaded successfully as PipelineModel")
                return model
            except Exception as pipeline_error:
                logger.info(f"Failed to load as PipelineModel: {pipeline_error}")
                
                # Try to load as individual classifier models
                from pyspark.ml.classification import RandomForestClassificationModel, LogisticRegressionModel
                
                # Determine model type from path
                if 'random_forest' in model_path.lower():
                    try:
                        model = RandomForestClassificationModel.load(model_path)
                        logger.info("Model loaded successfully as RandomForestClassificationModel")
                        return model
                    except Exception as rf_error:
                        logger.error(f"Failed to load as RandomForest: {rf_error}")
                        
                elif 'logistic_regression' in model_path.lower():
                    try:
                        model = LogisticRegressionModel.load(model_path)
                        logger.info("Model loaded successfully as LogisticRegressionModel")
                        return model
                    except Exception as lr_error:
                        logger.error(f"Failed to load as LogisticRegression: {lr_error}")
                
                # If specific type loading fails, try both
                try:
                    model = RandomForestClassificationModel.load(model_path)
                    logger.info("Model loaded successfully as RandomForestClassificationModel (fallback)")
                    return model
                except Exception:
                    try:
                        model = LogisticRegressionModel.load(model_path)
                        logger.info("Model loaded successfully as LogisticRegressionModel (fallback)")
                        return model
                    except Exception as final_error:
                        raise Exception(f"Could not load model as any supported type. Final error: {final_error}")
                        
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def load_test_data(self, test_data_path):
        """
        Load test data from HDFS with improved error handling
        
        Args:
            test_data_path (str): Path to test data
            
        Returns:
            DataFrame: Test data
        """
        try:
            logger.info(f"Loading test data from {test_data_path}")
            
            # Try different possible data formats and paths
            possible_paths = [
                test_data_path,
                f"{test_data_path}/diabetes_gold.parquet",
                f"{test_data_path}/*.parquet",
                f"{test_data_path}/part-*.parquet"
            ]
            
            for path in possible_paths:
                try:
                    df = self.spark.read.parquet(path)
                    logger.info(f"Test data loaded from {path}: {df.count()} records")
                    return df
                except Exception as e:
                    logger.debug(f"Failed to load from {path}: {e}")
                    continue
            
            # If parquet fails, try other formats
            try:
                df = self.spark.read.format("delta").load(test_data_path)
                logger.info(f"Test data loaded as Delta: {df.count()} records")
                return df
            except Exception:
                pass
                
            raise Exception(f"Could not load test data from any attempted path")
            
        except Exception as e:
            logger.error(f"Failed to load test data: {e}")
            raise
    
    def make_predictions(self, model, test_data, model_path):
        """
        Generate predictions using the model with improved pipeline handling
        
        Args:
            model: Trained model (PipelineModel or individual classifier)
            test_data: Test dataset
            model_path: Original model path to extract timestamp
            
        Returns:
            DataFrame: Predictions
        """
        try:
            logger.info("Generating predictions...")
            
            # Check if this is a PipelineModel or individual classifier
            from pyspark.ml import PipelineModel
            from pyspark.ml.classification import RandomForestClassificationModel, LogisticRegressionModel
            
            if isinstance(model, PipelineModel):
                # For pipeline models, use transform directly
                predictions = model.transform(test_data)
                logger.info("Used PipelineModel for predictions")
            else:
                # For individual classifiers, we need to apply feature engineering first
                logger.info("Individual classifier detected - applying feature engineering...")
                
                # Try to find feature pipeline
                feature_pipeline = None
                
                # Extract timestamp from model path
                timestamp_match = re.search(r'(\d{8}_\d{6})', model_path)
                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    feature_pipeline_paths = [
                        f"hdfs://namenode:9000/models/feature_pipeline_{timestamp}",
                        f"hdfs://namenode:9000/models/feature_pipeline_latest"
                    ]
                    
                    for fp_path in feature_pipeline_paths:
                        try:
                            feature_pipeline = PipelineModel.load(fp_path)
                            logger.info(f"Loaded feature pipeline from {fp_path}")
                            break
                        except Exception as fp_error:
                            logger.debug(f"Could not load feature pipeline from {fp_path}: {fp_error}")
                            continue
                
                if feature_pipeline:
                    # Apply feature pipeline first
                    prepared_data = feature_pipeline.transform(test_data)
                    predictions = model.transform(prepared_data)
                    logger.info("Applied feature pipeline before prediction")
                else:
                    # Try direct prediction
                    logger.warning("No feature pipeline found - attempting direct prediction")
                    predictions = model.transform(test_data)
                    logger.info("Used direct prediction without feature pipeline")
            
            pred_count = predictions.count()
            logger.info(f"Predictions generated for {pred_count} records")
            
            # Verify required columns exist
            required_columns = ['diabetes', 'prediction']
            missing_columns = [col for col in required_columns if col not in predictions.columns]
            if missing_columns:
                raise Exception(f"Missing required columns in predictions: {missing_columns}")
            
            return predictions
            
        except Exception as e:
            logger.error(f"Failed to generate predictions: {e}")
            raise
    
    # ...existing code... (keep all other methods unchanged)
    def calculate_metrics(self, predictions):
        """
        Calculate comprehensive evaluation metrics using PySpark
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Comprehensive metrics
        """
        logger.info("Calculating evaluation metrics...")
        
        # Binary classification evaluator
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="diabetes",
            rawPredictionCol="rawPrediction"
        )
        
        # Multiclass classification evaluator
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="diabetes",
            predictionCol="prediction"
        )
        
        # Calculate metrics
        metrics = {}
        
        # AUC-ROC
        metrics['auc_roc'] = binary_evaluator.evaluate(
            predictions, {binary_evaluator.metricName: "areaUnderROC"}
        )
        
        # AUC-PR
        metrics['auc_pr'] = binary_evaluator.evaluate(
            predictions, {binary_evaluator.metricName: "areaUnderPR"}
        )
        
        # Accuracy
        metrics['accuracy'] = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "accuracy"}
        )
        
        # Precision
        metrics['precision'] = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "weightedPrecision"}
        )
        
        # Recall
        metrics['recall'] = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "weightedRecall"}
        )
        
        # F1-Score
        metrics['f1_score'] = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "f1"}
        )
        
        return metrics
    
    def calculate_confusion_matrix_pyspark(self, predictions):
        """
        Calculate confusion matrix using PySpark operations
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Confusion matrix metrics
        """
        logger.info("Calculating confusion matrix...")
        
        # Calculate confusion matrix components using PySpark
        total_count = predictions.count()
        
        # True Positives (TP): actual=1, predicted=1
        tp = predictions.filter((col("diabetes") == 1) & (col("prediction") == 1)).count()
        
        # True Negatives (TN): actual=0, predicted=0
        tn = predictions.filter((col("diabetes") == 0) & (col("prediction") == 0)).count()
        
        # False Positives (FP): actual=0, predicted=1
        fp = predictions.filter((col("diabetes") == 0) & (col("prediction") == 1)).count()
        
        # False Negatives (FN): actual=1, predicted=0
        fn = predictions.filter((col("diabetes") == 1) & (col("prediction") == 0)).count()
        
        # Calculate derived metrics
        sensitivity = tp / (tp + fn) if (tp + fn) > 0 else 0  # Recall/Sensitivity
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0  # Specificity
        ppv = tp / (tp + fp) if (tp + fp) > 0 else 0  # Positive Predictive Value (Precision)
        npv = tn / (tn + fn) if (tn + fn) > 0 else 0  # Negative Predictive Value
        
        cm_metrics = {
            'true_positives': tp,
            'true_negatives': tn,
            'false_positives': fp,
            'false_negatives': fn,
            'sensitivity': sensitivity,
            'specificity': specificity,
            'positive_predictive_value': ppv,
            'negative_predictive_value': npv,
            'total_samples': total_count
        }
        
        return cm_metrics
    
    def analyze_prediction_distribution(self, predictions):
        """
        Analyze prediction distribution using PySpark
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Distribution analysis
        """
        logger.info("Analyzing prediction distribution...")
        
        # Count predictions by class
        pred_distribution = predictions.groupBy("prediction").count().collect()
        actual_distribution = predictions.groupBy("diabetes").count().collect()
        
        # Calculate prediction confidence statistics using UDF to extract probabilities
        predictions_with_prob = predictions.withColumn("positive_prob", 
                                                      self.extract_prob_udf(col("probability")))
        
        confidence_stats = predictions_with_prob.select(
            avg(col("positive_prob")).alias("avg_positive_prob"),
            stddev(col("positive_prob")).alias("std_positive_prob")
        ).collect()[0]
        
        distribution_analysis = {
            'prediction_distribution': {str(row['prediction']): row['count'] for row in pred_distribution},
            'actual_distribution': {str(row['diabetes']): row['count'] for row in actual_distribution},
            'confidence_stats': {
                'avg_positive_probability': float(confidence_stats['avg_positive_prob']) if confidence_stats['avg_positive_prob'] else 0,
                'std_positive_probability': float(confidence_stats['std_positive_prob']) if confidence_stats['std_positive_prob'] else 0
            }
        }
        
        return distribution_analysis
    
    def calculate_business_metrics(self, cm_metrics, predictions_count):
        """
        Calculate business impact metrics
        
        Args:
            cm_metrics: Confusion matrix metrics
            predictions_count: Total number of predictions
            
        Returns:
            dict: Business metrics
        """
        logger.info("Calculating business impact metrics...")
        
        tp = cm_metrics['true_positives']
        tn = cm_metrics['true_negatives']
        fp = cm_metrics['false_positives']
        fn = cm_metrics['false_negatives']
        
        # Calculate rates
        false_positive_rate = (fp / (fp + tn)) * 100 if (fp + tn) > 0 else 0
        false_negative_rate = (fn / (fn + tp)) * 100 if (fn + tp) > 0 else 0
        
        business_metrics = {
            'false_positive_rate': false_positive_rate,
            'false_negative_rate': false_negative_rate,
            'early_detection_rate': (tp / (tp + fn)) * 100 if (tp + fn) > 0 else 0,
            'unnecessary_interventions': fp,
            'missed_cases': fn,
            'correctly_identified_healthy': tn,
            'correctly_identified_diabetes': tp
        }
        
        return business_metrics
    
    def generate_model_recommendation(self, metrics, cm_metrics, business_metrics):
        """
        Generate model deployment recommendation
        
        Args:
            metrics: Performance metrics
            cm_metrics: Confusion matrix metrics
            business_metrics: Business impact metrics
            
        Returns:
            dict: Model recommendation
        """
        logger.info("Generating model recommendation...")
        
        # Define thresholds for deployment
        min_accuracy = 0.75
        min_f1_score = 0.70
        min_auc_roc = 0.80
        max_false_negative_rate = 15.0  # Maximum acceptable missed diagnoses
        
        # Evaluate model quality
        accuracy_ok = metrics['accuracy'] >= min_accuracy
        f1_ok = metrics['f1_score'] >= min_f1_score
        auc_ok = metrics['auc_roc'] >= min_auc_roc
        fn_rate_ok = business_metrics['false_negative_rate'] <= max_false_negative_rate
        
        deployment_ready = accuracy_ok and f1_ok and auc_ok and fn_rate_ok
        
        # Determine model quality
        if metrics['accuracy'] >= 0.85 and metrics['f1_score'] >= 0.80:
            quality = "Excellent"
        elif metrics['accuracy'] >= 0.80 and metrics['f1_score'] >= 0.75:
            quality = "Good"
        elif metrics['accuracy'] >= 0.75 and metrics['f1_score'] >= 0.70:
            quality = "Acceptable"
        else:
            quality = "Needs Improvement"
        
        # Generate improvement suggestions
        suggestions = []
        if not accuracy_ok:
            suggestions.append("Improve model accuracy through feature engineering or algorithm tuning")
        if not f1_ok:
            suggestions.append("Balance precision and recall to improve F1-score")
        if not auc_ok:
            suggestions.append("Enhance model's ability to distinguish between classes")
        if not fn_rate_ok:
            suggestions.append("Reduce false negative rate to avoid missing diabetes cases")
        
        recommendation = {
            'model_quality': quality,
            'deployment_ready': deployment_ready,
            'accuracy_threshold_met': accuracy_ok,
            'f1_threshold_met': f1_ok,
            'auc_threshold_met': auc_ok,
            'false_negative_rate_acceptable': fn_rate_ok,
            'improvement_suggestions': suggestions
        }
        
        return recommendation
    
    def save_evaluation_report(self, report, output_path):
        """
        Save evaluation report to HDFS
        
        Args:
            report: Complete evaluation report
            output_path: Output path for the report
        """
        logger.info(f"Saving evaluation report to {output_path}")
        
        try:
            # Convert report to JSON string
            report_json = json.dumps(report, indent=2, default=str)
            
            # Create DataFrame with single row containing the JSON report
            report_df = self.spark.createDataFrame([(report_json,)], ["evaluation_report"])
            
            # Save as single text file
            report_df.coalesce(1).write.mode("overwrite").text(output_path)
            
            logger.info("Evaluation report saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save evaluation report: {e}")
            raise
    
    def generate_comprehensive_report(self, model_path, test_data_path, output_path):
        """
        Generate comprehensive evaluation report
        
        Args:
            model_path: Path to trained model
            test_data_path: Path to test data
            output_path: Output path for evaluation report
            
        Returns:
            dict: Complete evaluation report
        """
        logger.info("Starting comprehensive model evaluation...")
        
        # Load model and test data
        model = self.load_model(model_path)
        test_data = self.load_test_data(test_data_path)
        
        # Generate predictions
        predictions = self.make_predictions(model, test_data, model_path)
        
        # Calculate all metrics
        performance_metrics = self.calculate_metrics(predictions)
        cm_metrics = self.calculate_confusion_matrix_pyspark(predictions)
        distribution_analysis = self.analyze_prediction_distribution(predictions)
        business_metrics = self.calculate_business_metrics(cm_metrics, predictions.count())
        
        # Generate model recommendation
        recommendation = self.generate_model_recommendation(
            performance_metrics, cm_metrics, business_metrics
        )
        
        # Compile comprehensive report
        report = {
            'evaluation_timestamp': datetime.now().isoformat(),
            'model_path': model_path,
            'test_data_path': test_data_path,
            'test_data_size': test_data.count(),
            'performance_metrics': performance_metrics,
            'confusion_matrix_metrics': cm_metrics,
            'prediction_distribution': distribution_analysis,
            'business_impact_metrics': business_metrics,
            'model_recommendation': recommendation
        }
        
        # Save report
        self.save_evaluation_report(report, output_path)
        
        # Print summary
        self.print_evaluation_summary(report)
        
        return report
    
    def print_evaluation_summary(self, report):
        """
        Print evaluation summary to console
        
        Args:
            report: Complete evaluation report
        """
        logger.info("=== MODEL EVALUATION SUMMARY ===")
        
        metrics = report['performance_metrics']
        business = report['business_impact_metrics']
        recommendation = report['model_recommendation']
        
        logger.info(f"Model Quality: {recommendation['model_quality']}")
        logger.info(f"Deployment Ready: {recommendation['deployment_ready']}")
        logger.info(f"Test Data Size: {report['test_data_size']} records")
        logger.info("")
        logger.info("PERFORMANCE METRICS:")
        logger.info(f"  Accuracy: {metrics['accuracy']:.4f} ({metrics['accuracy']*100:.2f}%)")
        logger.info(f"  F1-Score: {metrics['f1_score']:.4f}")
        logger.info(f"  AUC-ROC: {metrics['auc_roc']:.4f}")
        logger.info(f"  AUC-PR: {metrics['auc_pr']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall: {metrics['recall']:.4f}")
        logger.info("")
        logger.info("BUSINESS IMPACT:")
        logger.info(f"  False Negative Rate: {business['false_negative_rate']:.2f}% (missed cases)")
        logger.info(f"  False Positive Rate: {business['false_positive_rate']:.2f}% (unnecessary alerts)")
        logger.info(f"  Early Detection Rate: {business['early_detection_rate']:.2f}%")
        logger.info("")
        
        if recommendation['improvement_suggestions']:
            logger.info("IMPROVEMENT SUGGESTIONS:")
            for suggestion in recommendation['improvement_suggestions']:
                logger.info(f"  - {suggestion}")

def main():
    """Main function to run model evaluation"""
    parser = argparse.ArgumentParser(description='Evaluate diabetes prediction models using PySpark only')
    parser.add_argument('--model-path', help='Path to trained model (optional - will auto-detect latest if not provided)')
    parser.add_argument('--test-data-path', required=True, help='Path to test data')
    parser.add_argument('--metrics-output-path', required=True, help='Output path for evaluation metrics')
    parser.add_argument('--model-type', choices=['random_forest', 'logistic_regression'], 
                       default='random_forest', help='Type of model to evaluate when auto-detecting')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DiabetesModelEvaluation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize evaluator
        evaluator = PySparkModelEvaluator(spark)
        
        # Determine model path
        if args.model_path:
            model_path = args.model_path
            logger.info(f"Using provided model path: {model_path}")
        else:
            # Auto-detect latest models
            logger.info("Auto-detecting latest models...")
            latest_models = evaluator.find_latest_models()
            
            if args.model_type in latest_models:
                model_path = latest_models[args.model_type]
                logger.info(f"Auto-detected latest {args.model_type} model: {model_path}")
            else:
                logger.error(f"Could not find latest {args.model_type} model")
                logger.info(f"Available models: {list(latest_models.keys())}")
                sys.exit(1)
        
        # Generate comprehensive evaluation report
        report = evaluator.generate_comprehensive_report(
            model_path=model_path,
            test_data_path=args.test_data_path,
            output_path=args.metrics_output_path
        )
        
        logger.info("Model evaluation completed successfully!")
        
    except Exception as e:
        logger.error(f"Model evaluation failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
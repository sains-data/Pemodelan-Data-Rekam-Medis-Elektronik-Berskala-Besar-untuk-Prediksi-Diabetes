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
        Find the latest trained models in HDFS
        
        Args:
            models_base_path (str): Base path where models are stored
            
        Returns:
            dict: Dictionary with latest model paths for each model type
        """
        try:
            import subprocess
            import re
            
            logger.info(f"Searching for latest models in {models_base_path}")
            
            # Use hdfs command to list models directory
            cmd = ["hdfs", "dfs", "-ls", models_base_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse the output to find model directories
            model_paths = {}
            latest_timestamp = None
            
            # Pattern to match model directories with timestamps
            pattern = r'(/models/(\w+)_(\d{8}_\d{6}))'
            
            for line in result.stdout.split('\n'):
                match = re.search(pattern, line)
                if match:
                    relative_path = match.group(1)
                    full_path = f"hdfs://namenode:9000{relative_path}"  # Add HDFS prefix
                    model_type = match.group(2)
                    timestamp = match.group(3)
                    
                    # Keep track of the latest timestamp
                    if latest_timestamp is None or timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                    
                    # Store the path for each model type with this timestamp
                    if timestamp == latest_timestamp or model_type not in model_paths:
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
            
            logger.info(f"Found latest models with timestamp {latest_timestamp}: {list(latest_models.keys())}")
            return latest_models
            
        except Exception as e:
            logger.error(f"Failed to find latest models: {e}")
            # Fallback to manual paths if automatic detection fails
            return {
                'random_forest': f"hdfs://namenode:9000{models_base_path}/random_forest_20250526_235657",
                'logistic_regression': f"hdfs://namenode:9000{models_base_path}/logistic_regression_20250526_235657",
                'feature_pipeline': f"hdfs://namenode:9000{models_base_path}/feature_pipeline_20250526_235657"
            }
    
    def load_model(self, model_path):
        """
        Load trained model from HDFS
        
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
                
                try:
                    model = RandomForestClassificationModel.load(model_path)
                    logger.info("Model loaded successfully as RandomForestClassificationModel")
                    return model
                except Exception as rf_error:
                    logger.info(f"Failed to load as RandomForest: {rf_error}")
                    
                    try:
                        model = LogisticRegressionModel.load(model_path)
                        logger.info("Model loaded successfully as LogisticRegressionModel")
                        return model
                    except Exception as lr_error:
                        logger.error(f"Failed to load as LogisticRegression: {lr_error}")
                        raise Exception(f"Could not load model as any supported type. Pipeline error: {pipeline_error}, RF error: {rf_error}, LR error: {lr_error}")
                        
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def load_test_data(self, test_data_path):
        """
        Load test data from HDFS
        
        Args:
            test_data_path (str): Path to test data
            
        Returns:
            DataFrame: Test data
        """
        try:
            logger.info(f"Loading test data from {test_data_path}")
            df = self.spark.read.parquet(test_data_path)
            logger.info(f"Test data loaded: {df.count()} records")
            return df
        except Exception as e:
            logger.error(f"Failed to load test data: {e}")
            raise
    
    def make_predictions(self, model, test_data, model_path):
        """
        Generate predictions using the model
        
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
            else:
                # For individual classifiers, we need to apply feature engineering first
                logger.info("Individual classifier detected - applying feature engineering...")
                
                # Extract timestamp from model path
                import re
                timestamp_match = re.search(r'(\d{8}_\d{6})$', model_path)
                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    feature_pipeline_path = f"hdfs://namenode:9000/models/feature_pipeline_{timestamp}"
                    
                    try:
                        feature_pipeline = PipelineModel.load(feature_pipeline_path)
                        logger.info(f"Loaded feature pipeline from {feature_pipeline_path}")
                        
                        # Apply feature pipeline first
                        prepared_data = feature_pipeline.transform(test_data)
                        
                        # Then apply the classifier
                        predictions = model.transform(prepared_data)
                    except Exception as fp_error:
                        logger.warning(f"Could not load feature pipeline: {fp_error}")
                        logger.info("Attempting direct prediction without feature pipeline...")
                        predictions = model.transform(test_data)
                else:
                    logger.warning("Could not extract timestamp from model path")
                    logger.info("Attempting direct prediction without feature pipeline...")
                    predictions = model.transform(test_data)
            
            logger.info(f"Predictions generated for {predictions.count()} records")
            return predictions
            
        except Exception as e:
            logger.error(f"Failed to generate predictions: {e}")
            raise
    
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

#!/usr/bin/env python3
"""
Model Evaluation Script for Diabetes Prediction Pipeline
Kelompok 8 RA - Big Data Project

This script evaluates trained models and generates comprehensive performance reports.
"""

import argparse
import logging
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, count, avg, stddev
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, classification_report
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ModelEvaluator:
    """
    Model evaluation and performance analysis
    """
    
    def __init__(self, spark_session):
        """
        Initialize the model evaluator
        
        Args:
            spark_session: Active Spark session
        """
        self.spark = spark_session
    
    def load_model(self, model_path):
        """
        Load trained model from HDFS
        
        Args:
            model_path (str): Path to saved model
            
        Returns:
            PipelineModel: Loaded model
        """
        try:
            logger.info(f"Loading model from: {model_path}")
            model = PipelineModel.load(model_path)
            logger.info("Model loaded successfully")
            return model
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def load_test_data(self, test_data_path):
        """
        Load test data for evaluation
        
        Args:
            test_data_path (str): Path to test data
            
        Returns:
            DataFrame: Test data DataFrame
        """
        try:
            logger.info(f"Loading test data from: {test_data_path}")
            df = self.spark.read.parquet(test_data_path)
            logger.info(f"Test data loaded. Shape: {df.count()} x {len(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to load test data: {e}")
            raise
    
    def make_predictions(self, model, test_df):
        """
        Make predictions on test data
        
        Args:
            model: Trained model
            test_df: Test DataFrame
            
        Returns:
            DataFrame: Predictions DataFrame
        """
        logger.info("Making predictions on test data...")
        predictions = model.transform(test_df)
        return predictions
    
    def calculate_metrics(self, predictions):
        """
        Calculate comprehensive evaluation metrics
        
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
    
    def generate_confusion_matrix(self, predictions):
        """
        Generate confusion matrix analysis
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Confusion matrix analysis
        """
        logger.info("Generating confusion matrix...")
        
        # Convert to Pandas for sklearn compatibility
        pred_pandas = predictions.select("diabetes", "prediction").toPandas()
        
        # Generate confusion matrix
        cm = confusion_matrix(pred_pandas["diabetes"], pred_pandas["prediction"])
        
        # Calculate derived metrics
        tn, fp, fn, tp = cm.ravel()
        
        cm_metrics = {
            'true_negatives': int(tn),
            'false_positives': int(fp),
            'false_negatives': int(fn),
            'true_positives': int(tp),
            'sensitivity': tp / (tp + fn) if (tp + fn) > 0 else 0,
            'specificity': tn / (tn + fp) if (tn + fp) > 0 else 0,
            'ppv': tp / (tp + fp) if (tp + fp) > 0 else 0,  # Positive Predictive Value
            'npv': tn / (tn + fn) if (tn + fn) > 0 else 0   # Negative Predictive Value
        }
        
        return cm_metrics, cm
    
    def analyze_feature_importance(self, model):
        """
        Analyze feature importance (for applicable models)
        
        Args:
            model: Trained model
            
        Returns:
            dict: Feature importance analysis
        """
        logger.info("Analyzing feature importance...")
        
        try:
            # Get the final stage of the pipeline (the classifier)
            stages = model.stages
            classifier = None
            
            for stage in stages:
                if hasattr(stage, 'featureImportances'):
                    classifier = stage
                    break
            
            if classifier is None:
                logger.warning("Model does not support feature importance")
                return {}
            
            # Get feature importances
            importances = classifier.featureImportances.toArray()
            
            # Create feature importance dictionary
            # Note: This assumes feature names are available
            # In practice, you'd need to map indices to feature names
            feature_importance = {
                f'feature_{i}': float(importance) 
                for i, importance in enumerate(importances)
            }
            
            # Sort by importance
            sorted_features = sorted(feature_importance.items(), 
                                   key=lambda x: x[1], reverse=True)
            
            return {
                'feature_importances': dict(sorted_features),
                'top_5_features': sorted_features[:5]
            }
            
        except Exception as e:
            logger.warning(f"Could not extract feature importance: {e}")
            return {}
    
    def generate_prediction_distribution(self, predictions):
        """
        Analyze prediction distribution
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Prediction distribution analysis
        """
        logger.info("Analyzing prediction distribution...")
        
        # Count predictions by class
        pred_counts = predictions.groupBy("prediction").count().collect()
        
        distribution = {}
        total_predictions = sum([row['count'] for row in pred_counts])
        
        for row in pred_counts:
            class_label = int(row['prediction'])
            count = row['count']
            percentage = (count / total_predictions) * 100
            
            distribution[f'class_{class_label}'] = {
                'count': count,
                'percentage': percentage
            }
        
        return distribution
    
    def calculate_business_metrics(self, predictions):
        """
        Calculate business-relevant metrics for diabetes prediction
        
        Args:
            predictions: Predictions DataFrame
            
        Returns:
            dict: Business metrics
        """
        logger.info("Calculating business metrics...")
        
        total_patients = predictions.count()
        high_risk_predictions = predictions.filter(col("prediction") == 1).count()
        actual_diabetic = predictions.filter(col("diabetes") == 1).count()
        
        # False positive rate (patients incorrectly flagged as high risk)
        false_positives = predictions.filter(
            (col("diabetes") == 0) & (col("prediction") == 1)
        ).count()
        
        # False negative rate (diabetic patients missed)
        false_negatives = predictions.filter(
            (col("diabetes") == 1) & (col("prediction") == 0)
        ).count()
        
        business_metrics = {
            'total_patients_evaluated': total_patients,
            'patients_flagged_high_risk': high_risk_predictions,
            'actual_diabetic_patients': actual_diabetic,
            'high_risk_identification_rate': (high_risk_predictions / total_patients * 100),
            'false_positive_rate': (false_positives / total_patients * 100),
            'false_negative_rate': (false_negatives / total_patients * 100),
            'cost_of_false_negatives': false_negatives * 1000,  # Estimated cost per missed case
            'cost_of_false_positives': false_positives * 100    # Estimated cost per false alarm
        }
        
        return business_metrics
    
    def generate_comprehensive_report(self, model_path, test_data_path, output_path):
        """
        Generate comprehensive evaluation report
        
        Args:
            model_path (str): Path to trained model
            test_data_path (str): Path to test data
            output_path (str): Path to save evaluation report
            
        Returns:
            dict: Complete evaluation report
        """
        logger.info("Generating comprehensive evaluation report...")
        
        # Load model and data
        model = self.load_model(model_path)
        test_df = self.load_test_data(test_data_path)
        
        # Make predictions
        predictions = self.make_predictions(model, test_df)
        
        # Calculate all metrics
        metrics = self.calculate_metrics(predictions)
        cm_metrics, confusion_matrix_array = self.generate_confusion_matrix(predictions)
        feature_importance = self.analyze_feature_importance(model)
        prediction_distribution = self.generate_prediction_distribution(predictions)
        business_metrics = self.calculate_business_metrics(predictions)
        
        # Compile comprehensive report
        report = {
            'evaluation_metadata': {
                'model_path': model_path,
                'test_data_path': test_data_path,
                'evaluation_timestamp': datetime.now().isoformat(),
                'total_test_samples': test_df.count()
            },
            'performance_metrics': metrics,
            'confusion_matrix_analysis': cm_metrics,
            'feature_importance_analysis': feature_importance,
            'prediction_distribution': prediction_distribution,
            'business_impact_metrics': business_metrics,
            'model_recommendation': self._generate_recommendation(metrics, business_metrics)
        }
        
        # Save report
        self._save_report(report, output_path)
        
        # Log summary
        self._log_summary(report)
        
        return report
    
    def _generate_recommendation(self, metrics, business_metrics):
        """
        Generate model recommendation based on performance
        
        Args:
            metrics (dict): Performance metrics
            business_metrics (dict): Business metrics
            
        Returns:
            dict: Recommendation analysis
        """
        # Define thresholds for good performance
        accuracy_threshold = 0.85
        f1_threshold = 0.80
        false_negative_threshold = 5.0  # Max 5% false negative rate
        
        recommendation = {
            'model_quality': 'UNKNOWN',
            'deployment_ready': False,
            'improvement_suggestions': []
        }
        
        # Assess model quality
        if (metrics['accuracy'] >= accuracy_threshold and 
            metrics['f1_score'] >= f1_threshold and
            business_metrics['false_negative_rate'] <= false_negative_threshold):
            recommendation['model_quality'] = 'EXCELLENT'
            recommendation['deployment_ready'] = True
        elif (metrics['accuracy'] >= 0.75 and 
              metrics['f1_score'] >= 0.70):
            recommendation['model_quality'] = 'GOOD'
            recommendation['deployment_ready'] = True
        elif (metrics['accuracy'] >= 0.65):
            recommendation['model_quality'] = 'FAIR'
            recommendation['deployment_ready'] = False
        else:
            recommendation['model_quality'] = 'POOR'
            recommendation['deployment_ready'] = False
        
        # Generate improvement suggestions
        if metrics['accuracy'] < accuracy_threshold:
            recommendation['improvement_suggestions'].append("Consider feature engineering or different algorithms")
        
        if business_metrics['false_negative_rate'] > false_negative_threshold:
            recommendation['improvement_suggestions'].append("Reduce false negative rate - adjust classification threshold")
        
        if business_metrics['false_positive_rate'] > 15.0:
            recommendation['improvement_suggestions'].append("High false positive rate - may need more training data")
        
        return recommendation
    
    def _save_report(self, report, output_path):
        """
        Save evaluation report to JSON file
        
        Args:
            report (dict): Evaluation report
            output_path (str): Output path
        """
        try:
            # Save to local filesystem (in container)
            local_path = f"/opt/spark/data/logs/evaluation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(local_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"Evaluation report saved to: {local_path}")
            
            # Also try to save to HDFS if available
            try:
                hdfs_path = f"{output_path}/evaluation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                # Convert to Spark DataFrame and save
                report_df = self.spark.createDataFrame([report])
                report_df.write.mode("overwrite").json(hdfs_path)
                logger.info(f"Evaluation report also saved to HDFS: {hdfs_path}")
            except:
                logger.warning("Could not save to HDFS, saved locally only")
                
        except Exception as e:
            logger.error(f"Failed to save evaluation report: {e}")
    
    def _log_summary(self, report):
        """
        Log evaluation summary
        
        Args:
            report (dict): Evaluation report
        """
        logger.info("=== EVALUATION SUMMARY ===")
        
        metrics = report['performance_metrics']
        business = report['business_impact_metrics']
        recommendation = report['model_recommendation']
        
        logger.info(f"Model Quality: {recommendation['model_quality']}")
        logger.info(f"Deployment Ready: {recommendation['deployment_ready']}")
        logger.info(f"Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"F1-Score: {metrics['f1_score']:.4f}")
        logger.info(f"AUC-ROC: {metrics['auc_roc']:.4f}")
        logger.info(f"False Negative Rate: {business['false_negative_rate']:.2f}%")
        logger.info(f"False Positive Rate: {business['false_positive_rate']:.2f}%")
        
        if recommendation['improvement_suggestions']:
            logger.info("Improvement Suggestions:")
            for suggestion in recommendation['improvement_suggestions']:
                logger.info(f"  - {suggestion}")

def main():
    """Main function to run model evaluation"""
    parser = argparse.ArgumentParser(description='Evaluate diabetes prediction models')
    parser.add_argument('--model-path', required=True, help='Path to trained model')
    parser.add_argument('--test-data-path', required=True, help='Path to test data')
    parser.add_argument('--metrics-output-path', required=True, help='Output path for evaluation metrics')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DiabetesModelEvaluation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize evaluator
        evaluator = ModelEvaluator(spark)
        
        # Generate comprehensive evaluation report
        report = evaluator.generate_comprehensive_report(
            model_path=args.model_path,
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

#!/usr/bin/env python3
"""
Monitoring and Metrics Collection Script for Diabetes Prediction Pipeline
Kelompok 8 RA - Big Data Project

This script collects metrics from various pipeline components and sends them to Prometheus.
"""

import os
import sys
import time
import json
import logging
import requests
from datetime import datetime, timedelta
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, max as spark_max, min as spark_min, stddev
import psutil
import hdfs3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiabetesMonitor:
    """
    Monitoring and metrics collection for diabetes prediction pipeline
    """
    
    def __init__(self, prometheus_gateway_url="http://localhost:9091"):
        """
        Initialize monitoring system
        
        Args:
            prometheus_gateway_url (str): Prometheus Pushgateway URL
        """
        self.prometheus_gateway = prometheus_gateway_url
        self.metrics = {}
        
        # Initialize Spark session for data quality monitoring
        try:
            self.spark = SparkSession.builder \
                .appName("DiabetesMonitoring") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .getOrCreate()
            logger.info("Spark session initialized for monitoring")
        except Exception as e:
            logger.warning(f"Failed to initialize Spark session: {e}")
            self.spark = None
    
    def collect_system_metrics(self):
        """
        Collect system-level metrics
        
        Returns:
            dict: System metrics
        """
        logger.info("Collecting system metrics...")
        
        try:
            # CPU and Memory metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            system_metrics = {
                'cpu_usage_percent': cpu_percent,
                'memory_usage_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'memory_total_gb': memory.total / (1024**3),
                'disk_usage_percent': disk.percent,
                'disk_free_gb': disk.free / (1024**3),
                'disk_total_gb': disk.total / (1024**3)
            }
            
            logger.info(f"System metrics collected: CPU {cpu_percent}%, Memory {memory.percent}%")
            return system_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return {}
    
    def collect_hdfs_metrics(self):
        """
        Collect HDFS storage metrics
        
        Returns:
            dict: HDFS metrics
        """
        logger.info("Collecting HDFS metrics...")
        
        try:
            # Connect to HDFS
            client = hdfs3.HDFileSystem(host='namenode', port=9000)
            
            # Check data directories
            data_paths = [
                '/data/bronze/diabetes',
                '/data/silver/diabetes',
                '/data/gold/diabetes',
                '/models'
            ]
            
            hdfs_metrics = {}
            
            for path in data_paths:
                try:
                    if client.exists(path):
                        info = client.du(path)
                        size_gb = sum(info.values()) / (1024**3) if info else 0
                        file_count = len(client.ls(path, detail=False))
                        
                        layer_name = path.split('/')[-1]
                        hdfs_metrics[f'{layer_name}_size_gb'] = size_gb
                        hdfs_metrics[f'{layer_name}_file_count'] = file_count
                    else:
                        layer_name = path.split('/')[-1]
                        hdfs_metrics[f'{layer_name}_size_gb'] = 0
                        hdfs_metrics[f'{layer_name}_file_count'] = 0
                        
                except Exception as e:
                    logger.warning(f"Failed to get metrics for {path}: {e}")
                    continue
            
            logger.info("HDFS metrics collected successfully")
            return hdfs_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect HDFS metrics: {e}")
            return {}
    
    def collect_data_quality_metrics(self):
        """
        Collect data quality metrics from different layers
        
        Returns:
            dict: Data quality metrics
        """
        if not self.spark:
            logger.warning("Spark session not available for data quality metrics")
            return {}
        
        logger.info("Collecting data quality metrics...")
        
        quality_metrics = {}
        
        # Check each data layer
        layers = {
            'bronze': 'hdfs://namenode:9000/data/bronze/diabetes',
            'silver': 'hdfs://namenode:9000/data/silver/diabetes',
            'gold': 'hdfs://namenode:9000/data/gold/diabetes'
        }
        
        for layer_name, layer_path in layers.items():
            try:
                # Try to read data
                if layer_name == 'bronze':
                    df = self.spark.read.option("header", "true").csv(layer_path, inferSchema=True)
                else:
                    df = self.spark.read.parquet(layer_path)
                
                if df:
                    # Basic metrics
                    total_records = df.count()
                    total_columns = len(df.columns)
                    
                    quality_metrics[f'{layer_name}_total_records'] = total_records
                    quality_metrics[f'{layer_name}_total_columns'] = total_columns
                    
                    # Calculate missing value percentages
                    missing_stats = {}
                    for column in df.columns:
                        if column not in ['processed_timestamp', 'data_source']:
                            missing_count = df.filter(col(column).isNull()).count()
                            missing_pct = (missing_count / total_records) * 100 if total_records > 0 else 0
                            missing_stats[column] = missing_pct
                    
                    # Average missing percentage across all columns
                    avg_missing_pct = sum(missing_stats.values()) / len(missing_stats) if missing_stats else 0
                    quality_metrics[f'{layer_name}_avg_missing_pct'] = avg_missing_pct
                    
                    # Data freshness (for layers with timestamp)
                    if 'processed_timestamp' in df.columns:
                        latest_timestamp = df.select(spark_max('processed_timestamp')).collect()[0][0]
                        if latest_timestamp:
                            age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
                            quality_metrics[f'{layer_name}_data_age_hours'] = age_hours
                    
                    logger.info(f"{layer_name} quality metrics: {total_records} records, {avg_missing_pct:.2f}% missing")
                
            except Exception as e:
                logger.warning(f"Failed to collect quality metrics for {layer_name}: {e}")
                quality_metrics[f'{layer_name}_total_records'] = 0
                quality_metrics[f'{layer_name}_error'] = 1
        
        return quality_metrics
    
    def collect_model_performance_metrics(self):
        """
        Collect model performance metrics from latest evaluation
        
        Returns:
            dict: Model performance metrics
        """
        logger.info("Collecting model performance metrics...")
        
        try:
            # Read latest model evaluation results
            model_metrics_path = '/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/logs/model_metrics.json'
            
            if os.path.exists(model_metrics_path):
                with open(model_metrics_path, 'r') as f:
                    model_data = json.load(f)
                
                # Extract key performance metrics
                performance_metrics = {
                    'model_accuracy': model_data.get('accuracy', 0),
                    'model_precision': model_data.get('precision', 0),
                    'model_recall': model_data.get('recall', 0),
                    'model_f1_score': model_data.get('f1_score', 0),
                    'model_auc': model_data.get('auc', 0),
                    'model_training_time_seconds': model_data.get('training_time_seconds', 0),
                    'model_test_samples': model_data.get('test_samples', 0)
                }
                
                logger.info(f"Model performance metrics collected: Accuracy {performance_metrics['model_accuracy']:.3f}")
                return performance_metrics
            else:
                logger.warning("Model metrics file not found")
                return {}
                
        except Exception as e:
            logger.error(f"Failed to collect model performance metrics: {e}")
            return {}
    
    def collect_pipeline_metrics(self):
        """
        Collect ETL pipeline execution metrics
        
        Returns:
            dict: Pipeline metrics
        """
        logger.info("Collecting pipeline metrics...")
        
        try:
            # Read pipeline execution logs
            pipeline_log_path = '/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/logs/pipeline_metrics.json'
            
            if os.path.exists(pipeline_log_path):
                with open(pipeline_log_path, 'r') as f:
                    pipeline_data = json.load(f)
                
                pipeline_metrics = {
                    'etl_execution_time_minutes': pipeline_data.get('total_execution_time_minutes', 0),
                    'bronze_to_silver_time_minutes': pipeline_data.get('bronze_to_silver_time_minutes', 0),
                    'silver_to_gold_time_minutes': pipeline_data.get('silver_to_gold_time_minutes', 0),
                    'data_validation_errors': pipeline_data.get('validation_errors', 0),
                    'pipeline_success_rate': pipeline_data.get('success_rate', 0),
                    'last_successful_run_hours_ago': pipeline_data.get('last_successful_run_hours_ago', 0)
                }
                
                logger.info("Pipeline metrics collected successfully")
                return pipeline_metrics
            else:
                logger.warning("Pipeline metrics file not found")
                return {'pipeline_status': 0}  # Indicate pipeline hasn't run
                
        except Exception as e:
            logger.error(f"Failed to collect pipeline metrics: {e}")
            return {'pipeline_error': 1}
    
    def collect_business_metrics(self):
        """
        Collect business-relevant metrics from gold layer
        
        Returns:
            dict: Business metrics
        """
        if not self.spark:
            logger.warning("Spark session not available for business metrics")
            return {}
        
        logger.info("Collecting business metrics...")
        
        try:
            # Read gold layer data
            gold_df = self.spark.read.parquet('hdfs://namenode:9000/data/gold/diabetes')
            
            # Calculate business metrics
            total_patients = gold_df.count()
            
            # Risk distribution
            risk_distribution = gold_df.groupBy('health_risk_level').count().collect()
            risk_counts = {row['health_risk_level']: row['count'] for row in risk_distribution}
            
            # Diabetes prediction distribution
            diabetes_pred_dist = gold_df.groupBy('diabetes').count().collect()
            diabetes_counts = {f'diabetes_{int(row["diabetes"])}': row['count'] for row in diabetes_pred_dist}
            
            # Average risk score
            avg_risk_score = gold_df.select(mean('risk_score')).collect()[0][0]
            
            # High-risk patients needing immediate intervention
            high_risk_patients = gold_df.filter(col('intervention_priority') == 'Immediate').count()
            high_risk_percentage = (high_risk_patients / total_patients) * 100 if total_patients > 0 else 0
            
            business_metrics = {
                'total_patients': total_patients,
                'avg_risk_score': avg_risk_score or 0,
                'high_risk_patients': high_risk_patients,
                'high_risk_percentage': high_risk_percentage,
                **risk_counts,
                **diabetes_counts
            }
            
            logger.info(f"Business metrics collected: {total_patients} patients, {high_risk_percentage:.1f}% high risk")
            return business_metrics
            
        except Exception as e:
            logger.error(f"Failed to collect business metrics: {e}")
            return {}
    
    def format_prometheus_metrics(self, metrics_dict, job_name="diabetes_pipeline"):
        """
        Format metrics for Prometheus
        
        Args:
            metrics_dict (dict): Metrics to format
            job_name (str): Prometheus job name
            
        Returns:
            str: Formatted metrics string
        """
        timestamp = int(time.time() * 1000)
        formatted_metrics = []
        
        for metric_name, value in metrics_dict.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                metric_line = f"{metric_name}{{job=\"{job_name}\"}} {value} {timestamp}"
                formatted_metrics.append(metric_line)
        
        return '\n'.join(formatted_metrics)
    
    def send_metrics_to_prometheus(self, metrics_dict, job_name="diabetes_pipeline"):
        """
        Send metrics to Prometheus Pushgateway
        
        Args:
            metrics_dict (dict): Metrics to send
            job_name (str): Prometheus job name
        """
        try:
            formatted_metrics = self.format_prometheus_metrics(metrics_dict, job_name)
            
            url = f"{self.prometheus_gateway}/metrics/job/{job_name}"
            headers = {'Content-Type': 'text/plain'}
            
            response = requests.post(url, data=formatted_metrics, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Successfully sent {len(metrics_dict)} metrics to Prometheus")
            else:
                logger.error(f"Failed to send metrics to Prometheus: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending metrics to Prometheus: {e}")
    
    def save_metrics_locally(self, metrics_dict, filename=None):
        """
        Save metrics to local file for debugging
        
        Args:
            metrics_dict (dict): Metrics to save
            filename (str): Output filename
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"diabetes_metrics_{timestamp}.json"
        
        metrics_dir = '/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/logs'
        os.makedirs(metrics_dir, exist_ok=True)
        
        filepath = os.path.join(metrics_dir, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(metrics_dict, f, indent=2, default=str)
            
            logger.info(f"Metrics saved to: {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save metrics locally: {e}")
    
    def collect_all_metrics(self):
        """
        Collect all types of metrics
        
        Returns:
            dict: Combined metrics dictionary
        """
        logger.info("Starting comprehensive metrics collection...")
        
        all_metrics = {
            'collection_timestamp': datetime.now().isoformat(),
            'pipeline_version': '1.0.0'
        }
        
        # Collect different types of metrics
        try:
            system_metrics = self.collect_system_metrics()
            all_metrics.update(system_metrics)
        except Exception as e:
            logger.error(f"System metrics collection failed: {e}")
        
        try:
            hdfs_metrics = self.collect_hdfs_metrics()
            all_metrics.update(hdfs_metrics)
        except Exception as e:
            logger.error(f"HDFS metrics collection failed: {e}")
        
        try:
            quality_metrics = self.collect_data_quality_metrics()
            all_metrics.update(quality_metrics)
        except Exception as e:
            logger.error(f"Data quality metrics collection failed: {e}")
        
        try:
            model_metrics = self.collect_model_performance_metrics()
            all_metrics.update(model_metrics)
        except Exception as e:
            logger.error(f"Model metrics collection failed: {e}")
        
        try:
            pipeline_metrics = self.collect_pipeline_metrics()
            all_metrics.update(pipeline_metrics)
        except Exception as e:
            logger.error(f"Pipeline metrics collection failed: {e}")
        
        try:
            business_metrics = self.collect_business_metrics()
            all_metrics.update(business_metrics)
        except Exception as e:
            logger.error(f"Business metrics collection failed: {e}")
        
        logger.info(f"Comprehensive metrics collection completed. Total metrics: {len(all_metrics)}")
        return all_metrics
    
    def run_monitoring_cycle(self, send_to_prometheus=True, save_locally=True):
        """
        Run a complete monitoring cycle
        
        Args:
            send_to_prometheus (bool): Whether to send metrics to Prometheus
            save_locally (bool): Whether to save metrics locally
        """
        try:
            logger.info("Starting monitoring cycle...")
            
            # Collect all metrics
            metrics = self.collect_all_metrics()
            
            # Send to Prometheus if enabled
            if send_to_prometheus:
                self.send_metrics_to_prometheus(metrics)
            
            # Save locally if enabled
            if save_locally:
                self.save_metrics_locally(metrics, 'latest_metrics.json')
            
            logger.info("Monitoring cycle completed successfully")
            return metrics
            
        except Exception as e:
            logger.error(f"Monitoring cycle failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main function to run monitoring"""
    parser = argparse.ArgumentParser(description='Monitor diabetes prediction pipeline')
    parser.add_argument('--prometheus-gateway', default='http://localhost:9091',
                       help='Prometheus Pushgateway URL')
    parser.add_argument('--no-prometheus', action='store_true',
                       help='Disable sending metrics to Prometheus')
    parser.add_argument('--no-local-save', action='store_true',
                       help='Disable saving metrics locally')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=300,
                       help='Monitoring interval in seconds (for continuous mode)')
    
    args = parser.parse_args()
    
    # Initialize monitor
    monitor = DiabetesMonitor(prometheus_gateway_url=args.prometheus_gateway)
    
    try:
        if args.continuous:
            logger.info(f"Starting continuous monitoring with {args.interval}s interval...")
            while True:
                monitor.run_monitoring_cycle(
                    send_to_prometheus=not args.no_prometheus,
                    save_locally=not args.no_local_save
                )
                logger.info(f"Sleeping for {args.interval} seconds...")
                time.sleep(args.interval)
        else:
            # Single run
            metrics = monitor.run_monitoring_cycle(
                send_to_prometheus=not args.no_prometheus,
                save_locally=not args.no_local_save
            )
            
            # Print summary
            print("\n=== DIABETES PIPELINE MONITORING SUMMARY ===")
            print(f"Collection Time: {metrics.get('collection_timestamp', 'Unknown')}")
            print(f"Total Metrics Collected: {len(metrics)}")
            
            if 'bronze_total_records' in metrics:
                print(f"Bronze Layer Records: {metrics['bronze_total_records']}")
            if 'silver_total_records' in metrics:
                print(f"Silver Layer Records: {metrics['silver_total_records']}")
            if 'gold_total_records' in metrics:
                print(f"Gold Layer Records: {metrics['gold_total_records']}")
            if 'model_accuracy' in metrics:
                print(f"Model Accuracy: {metrics['model_accuracy']:.3f}")
            if 'total_patients' in metrics:
                print(f"Total Patients: {metrics['total_patients']}")
            if 'high_risk_percentage' in metrics:
                print(f"High Risk Patients: {metrics['high_risk_percentage']:.1f}%")
            
            print("=== END SUMMARY ===\n")
            
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

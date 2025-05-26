#!/usr/bin/env python3
"""
Test Fixtures and Mock Data
Provides common test data, fixtures, and mock objects for unit testing
"""

import os
import json
import tempfile
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import unittest.mock as mock

class TestFixtures:
    """Centralized test fixtures for the diabetes prediction pipeline"""
    
    @staticmethod
    def get_sample_diabetes_data() -> pd.DataFrame:
        """Generate sample diabetes dataset for testing"""
        np.random.seed(42)  # For reproducible tests
        
        n_samples = 100
        data = {
            'Pregnancies': np.random.randint(0, 10, n_samples),
            'Glucose': np.random.normal(120, 30, n_samples).clip(70, 200),
            'BloodPressure': np.random.normal(80, 15, n_samples).clip(40, 120),
            'SkinThickness': np.random.normal(25, 10, n_samples).clip(10, 50),
            'Insulin': np.random.normal(100, 50, n_samples).clip(0, 300),
            'BMI': np.random.normal(30, 8, n_samples).clip(15, 50),
            'DiabetesPedigreeFunction': np.random.uniform(0.1, 2.0, n_samples),
            'Age': np.random.randint(20, 80, n_samples),
            'Outcome': np.random.binomial(1, 0.3, n_samples)
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def get_sample_personal_health_data() -> pd.DataFrame:
        """Generate sample personal health dataset for testing"""
        np.random.seed(42)
        
        n_samples = 50
        data = {
            'patient_id': [f'P{str(i).zfill(4)}' for i in range(1, n_samples + 1)],
            'age': np.random.randint(18, 85, n_samples),
            'gender': np.random.choice(['M', 'F'], n_samples),
            'weight': np.random.normal(70, 15, n_samples).clip(40, 150),
            'height': np.random.normal(170, 10, n_samples).clip(150, 200),
            'blood_pressure_systolic': np.random.normal(120, 20, n_samples).clip(90, 180),
            'blood_pressure_diastolic': np.random.normal(80, 10, n_samples).clip(60, 110),
            'cholesterol': np.random.normal(200, 40, n_samples).clip(150, 300),
            'smoking': np.random.choice(['Yes', 'No'], n_samples, p=[0.3, 0.7]),
            'family_history_diabetes': np.random.choice(['Yes', 'No'], n_samples, p=[0.4, 0.6]),
            'physical_activity': np.random.choice(['Low', 'Moderate', 'High'], n_samples),
            'last_checkup': [
                (datetime.now() - timedelta(days=np.random.randint(30, 365))).strftime('%Y-%m-%d')
                for _ in range(n_samples)
            ]
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def get_invalid_data_samples() -> List[pd.DataFrame]:
        """Generate invalid data samples for testing data validation"""
        samples = []
        
        # Sample with missing columns
        df_missing_cols = pd.DataFrame({
            'Glucose': [120, 130, 140],
            'Age': [25, 30, 35]
            # Missing other required columns
        })
        samples.append(df_missing_cols)
        
        # Sample with null values
        df_with_nulls = TestFixtures.get_sample_diabetes_data().copy()
        df_with_nulls.loc[0:5, 'Glucose'] = np.nan
        df_with_nulls.loc[10:15, 'BMI'] = np.nan
        samples.append(df_with_nulls)
        
        # Sample with invalid ranges
        df_invalid_ranges = TestFixtures.get_sample_diabetes_data().copy()
        df_invalid_ranges.loc[0, 'Glucose'] = -50  # Invalid negative glucose
        df_invalid_ranges.loc[1, 'Age'] = 200      # Invalid age
        df_invalid_ranges.loc[2, 'BMI'] = -10      # Invalid BMI
        samples.append(df_invalid_ranges)
        
        # Sample with wrong data types
        df_wrong_types = TestFixtures.get_sample_diabetes_data().copy()
        df_wrong_types['Glucose'] = df_wrong_types['Glucose'].astype(str)
        samples.append(df_wrong_types)
        
        return samples
    
    @staticmethod
    def create_temp_csv_files() -> Dict[str, str]:
        """Create temporary CSV files for testing"""
        temp_files = {}
        
        # Create diabetes data file
        diabetes_data = TestFixtures.get_sample_diabetes_data()
        temp_diabetes = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        diabetes_data.to_csv(temp_diabetes.name, index=False)
        temp_files['diabetes'] = temp_diabetes.name
        
        # Create personal health data file
        health_data = TestFixtures.get_sample_personal_health_data()
        temp_health = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        health_data.to_csv(temp_health.name, index=False)
        temp_files['personal_health'] = temp_health.name
        
        return temp_files
    
    @staticmethod
    def cleanup_temp_files(temp_files: Dict[str, str]):
        """Clean up temporary files"""
        for file_path in temp_files.values():
            try:
                os.unlink(file_path)
            except FileNotFoundError:
                pass
    
    @staticmethod
    def get_mock_api_responses() -> Dict[str, Any]:
        """Get mock API responses for testing service interactions"""
        return {
            'hdfs_status': {
                'status_code': 200,
                'json': {
                    'beans': [{
                        'name': 'Hadoop:service=NameNode,name=NameNodeStatus',
                        'State': 'active',
                        'HostAndPort': 'namenode:9000'
                    }]
                }
            },
            'spark_status': {
                'status_code': 200,
                'text': '<html><title>Spark Master</title></html>'
            },
            'airflow_health': {
                'status_code': 200,
                'json': {
                    'metadatabase': {'status': 'healthy'},
                    'scheduler': {'status': 'healthy'}
                }
            },
            'grafana_health': {
                'status_code': 200,
                'json': {
                    'database': 'ok',
                    'version': '8.0.0'
                }
            },
            'prometheus_health': {
                'status_code': 200,
                'text': 'Prometheus is Healthy.\n'
            },
            'nifi_status': {
                'status_code': 200,
                'json': {
                    'systemDiagnostics': {
                        'aggregateSnapshot': {
                            'availableProcessors': 4,
                            'totalThreads': 25
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def get_mock_ml_model_metrics() -> Dict[str, float]:
        """Get mock ML model metrics for testing"""
        return {
            'accuracy': 0.85,
            'precision': 0.82,
            'recall': 0.78,
            'f1_score': 0.80,
            'auc_roc': 0.88,
            'training_time': 120.5,
            'validation_loss': 0.34
        }
    
    @staticmethod
    def get_mock_environment_config() -> Dict[str, str]:
        """Get mock environment configuration for testing"""
        return {
            'ENVIRONMENT': 'test',
            'PROJECT_NAME': 'diabetes-prediction-test',
            'PROJECT_VERSION': '1.0.0-test',
            'HDFS_NAMENODE_HTTP_PORT': '9870',
            'SPARK_MASTER_WEBUI_PORT': '8081',
            'AIRFLOW_WEBSERVER_PORT': '8089',
            'GRAFANA_PORT': '3000',
            'PROMETHEUS_PORT': '9090',
            'NIFI_WEB_HTTP_PORT': '8080'
        }
    
    @staticmethod
    def get_mock_docker_compose_config() -> Dict[str, Any]:
        """Get mock Docker Compose configuration for testing"""
        return {
            'version': '3.8',
            'services': {
                'namenode': {
                    'image': 'apache/hadoop:3',
                    'ports': ['9870:9870'],
                    'environment': ['CLUSTER_NAME=test']
                },
                'spark-master': {
                    'image': 'apache/spark:latest',
                    'ports': ['8080:8080', '7077:7077'],
                    'environment': ['SPARK_MODE=master']
                },
                'airflow': {
                    'image': 'apache/airflow:2.5.0',
                    'ports': ['8080:8080'],
                    'environment': ['AIRFLOW__CORE__EXECUTOR=LocalExecutor']
                }
            }
        }


class MockServiceResponses:
    """Mock service responses for testing"""
    
    @staticmethod
    def mock_requests_get(url: str, **kwargs):
        """Mock requests.get responses based on URL"""
        mock_responses = TestFixtures.get_mock_api_responses()
        
        # Create mock response object
        mock_response = mock.Mock()
        
        if 'namenode' in url or '9870' in url:
            response_data = mock_responses['hdfs_status']
        elif 'spark' in url or '8081' in url:
            response_data = mock_responses['spark_status']
        elif 'airflow' in url or '8089' in url:
            response_data = mock_responses['airflow_health']
        elif 'grafana' in url or '3000' in url:
            response_data = mock_responses['grafana_health']
        elif 'prometheus' in url or '9090' in url:
            response_data = mock_responses['prometheus_health']
        elif 'nifi' in url or '8080' in url:
            response_data = mock_responses['nifi_status']
        else:
            # Default response for unknown URLs
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = Exception("Not Found")
            return mock_response
        
        # Set response attributes
        mock_response.status_code = response_data['status_code']
        if 'json' in response_data:
            mock_response.json.return_value = response_data['json']
        if 'text' in response_data:
            mock_response.text = response_data['text']
        
        # Mock successful response
        mock_response.raise_for_status.return_value = None
        
        return mock_response
    
    @staticmethod
    def mock_docker_client():
        """Mock Docker client for testing"""
        mock_client = mock.Mock()
        
        # Mock containers
        mock_container = mock.Mock()
        mock_container.name = 'test-container'
        mock_container.status = 'running'
        mock_container.attrs = {
            'State': {'Status': 'running', 'Health': {'Status': 'healthy'}},
            'Config': {'Image': 'test-image:latest'}
        }
        
        mock_client.containers.list.return_value = [mock_container]
        mock_client.containers.get.return_value = mock_container
        
        return mock_client


class TestDataGenerator:
    """Generate test data for various scenarios"""
    
    @staticmethod
    def generate_time_series_data(days: int = 30) -> pd.DataFrame:
        """Generate time series data for testing monitoring and alerts"""
        dates = pd.date_range(start=datetime.now() - timedelta(days=days), 
                             end=datetime.now(), freq='h')
        
        data = {
            'timestamp': dates,
            'cpu_usage': np.random.uniform(10, 90, len(dates)),
            'memory_usage': np.random.uniform(20, 80, len(dates)),
            'disk_usage': np.random.uniform(30, 70, len(dates)),
            'network_io': np.random.exponential(1000, len(dates)),
            'active_connections': np.random.poisson(50, len(dates)),
            'response_time': np.random.gamma(2, 0.5, len(dates))
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def generate_log_entries(count: int = 100) -> List[Dict[str, Any]]:
        """Generate sample log entries for testing log processing"""
        log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        services = ['airflow', 'spark', 'hdfs', 'nifi', 'grafana']
        
        logs = []
        for i in range(count):
            log_entry = {
                'timestamp': (datetime.now() - timedelta(
                    seconds=np.random.randint(0, 86400))).isoformat(),
                'level': np.random.choice(log_levels),
                'service': np.random.choice(services),
                'message': f'Sample log message {i}',
                'correlation_id': f'corr-{np.random.randint(1000, 9999)}',
                'host': f'host-{np.random.randint(1, 5)}'
            }
            logs.append(log_entry)
        
        return logs
    
    @staticmethod
    def generate_pipeline_metrics() -> Dict[str, Any]:
        """Generate sample pipeline execution metrics"""
        return {
            'execution_id': f'exec-{np.random.randint(10000, 99999)}',
            'start_time': (datetime.now() - timedelta(hours=2)).isoformat(),
            'end_time': datetime.now().isoformat(),
            'status': np.random.choice(['success', 'failed', 'running']),
            'stages': {
                'ingestion': {
                    'duration_seconds': np.random.randint(60, 300),
                    'records_processed': np.random.randint(1000, 10000),
                    'status': 'success'
                },
                'transformation': {
                    'duration_seconds': np.random.randint(120, 600),
                    'records_processed': np.random.randint(1000, 10000),
                    'status': 'success'
                },
                'model_training': {
                    'duration_seconds': np.random.randint(300, 1800),
                    'accuracy': np.random.uniform(0.7, 0.95),
                    'status': 'success'
                }
            },
            'resource_usage': {
                'max_cpu_percent': np.random.uniform(40, 90),
                'max_memory_mb': np.random.randint(1000, 8000),
                'disk_io_mb': np.random.randint(100, 1000)
            }
        }


# Export commonly used fixtures
def get_test_diabetes_data():
    """Convenience function to get test diabetes data"""
    return TestFixtures.get_sample_diabetes_data()

def get_test_personal_health_data():
    """Convenience function to get test personal health data"""
    return TestFixtures.get_sample_personal_health_data()

def get_mock_responses():
    """Convenience function to get mock API responses"""
    return TestFixtures.get_mock_api_responses()

def create_temp_test_files():
    """Convenience function to create temporary test files"""
    return TestFixtures.create_temp_csv_files()

def cleanup_test_files(temp_files):
    """Convenience function to cleanup test files"""
    TestFixtures.cleanup_temp_files(temp_files)

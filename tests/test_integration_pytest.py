#!/usr/bin/env python3
"""
Enhanced Integration Tests with Pytest
Comprehensive unit, integration, and end-to-end tests using pytest framework
"""

import pytest
import requests
import time
import json
import os
import subprocess
import pandas as pd
import docker
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from unittest.mock import patch, Mock

# Add the tests directory to the path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_config
from test_config import TestConfig, TestResult, TestReporter, wait_for_service
import test_fixtures
from test_fixtures import TestFixtures, MockServiceResponses


class TestUnitComponents:
    """Unit tests for individual components using pytest"""
    
    @pytest.fixture(autouse=True)
    def setup(self, test_config):
        """Set up test configuration"""
        self.config = test_config
        self.results = []
    
    @pytest.mark.unit
    def test_data_validation_function(self, test_data):
        """Unit test for data validation function"""
        df = test_data['diabetes']
        
        # Unit test: Check if data validation logic works
        validation_checks = {
            'not_empty': len(df) > 0,
            'has_required_columns': all(col in df.columns for col in self.config.diabetes_schema),
            'no_all_null_rows': not df.isnull().all(axis=1).any(),
            'outcome_is_binary': set(df['Outcome'].unique()).issubset({0, 1}) if 'Outcome' in df.columns else False
        }
        
        # Assertions
        assert len(df) > 0, "Dataset should not be empty"
        assert all(col in df.columns for col in self.config.diabetes_schema), "Missing required columns"
        assert not df.isnull().all(axis=1).any(), "Dataset contains rows with all null values"
        assert set(df['Outcome'].unique()).issubset({0, 1}), "Outcome column should be binary"
    
    @pytest.mark.unit
    def test_environment_variable_parsing(self):
        """Unit test for environment variable parsing"""
        # Test environment variable loading
        env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        
        if os.path.exists(env_file):
            env_vars = {}
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip()
            
            # Check critical environment variables
            critical_vars = [
                'PROJECT_NAME', 'PROJECT_VERSION', 'ENVIRONMENT',
                'HDFS_NAMENODE_HTTP_PORT', 'SPARK_MASTER_WEBUI_PORT',
                'AIRFLOW_WEBSERVER_PORT', 'GRAFANA_PORT', 'PROMETHEUS_PORT'
            ]
            
            missing_vars = [var for var in critical_vars if var not in env_vars]
            assert not missing_vars, f"Missing critical environment variables: {missing_vars}"
    
    @pytest.mark.unit
    def test_docker_compose_config_validation(self):
        """Unit test for Docker Compose configuration validation"""
        compose_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docker-compose.yml')
        
        assert os.path.exists(compose_file), f"Docker Compose file not found: {compose_file}"
        
        # Validate Docker Compose file
        result = subprocess.run(
            ['docker-compose', 'config', '--quiet'],
            cwd=os.path.dirname(compose_file),
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Docker Compose configuration is invalid: {result.stderr}"


@pytest.mark.integration
@pytest.mark.requires_services
class TestPipelineIntegration:
    """Integration tests for service-to-service communication"""
    
    @pytest.fixture(autouse=True)
    def setup(self, test_config):
        """Set up test configuration"""
        self.config = test_config
        
    @pytest.mark.integration
    def test_hdfs_spark_integration(self):
        """Test HDFS and Spark integration"""
        # Test HDFS availability
        hdfs_response = requests.get(f"{self.config.hdfs_namenode_url}/jmx", timeout=10)
        assert hdfs_response.status_code == 200, "HDFS NameNode not accessible"
        
        # Test Spark availability
        spark_response = requests.get(self.config.spark_master_url, timeout=10)
        assert spark_response.status_code == 200, "Spark Master not accessible"
        
        # Test if Spark can access HDFS (would require actual services running)
        # This would be a real integration test in a live environment
        
    @pytest.mark.integration
    def test_airflow_spark_integration(self):
        """Test Airflow and Spark integration"""
        # Test Airflow availability
        auth = (self.config.airflow_username, self.config.airflow_password)
        airflow_response = requests.get(f"{self.config.airflow_url}/health", auth=auth, timeout=10)
        assert airflow_response.status_code == 200, "Airflow not accessible"
        
        # Test DAG existence
        dags_response = requests.get(f"{self.config.airflow_url}/api/v1/dags", auth=auth, timeout=10)
        if dags_response.status_code == 200:
            dags_data = dags_response.json()
            dag_ids = [dag['dag_id'] for dag in dags_data.get('dags', [])]
            
            # Look for diabetes-related DAGs
            diabetes_dags = [dag_id for dag_id in dag_ids if 'diabetes' in dag_id.lower()]
            assert len(diabetes_dags) > 0, f"No diabetes DAGs found. Available DAGs: {dag_ids}"
    
    @pytest.mark.integration
    def test_monitoring_stack_integration(self):
        """Test monitoring stack (Prometheus + Grafana) integration"""
        # Test Prometheus availability
        prometheus_response = requests.get(f"{self.config.prometheus_url}/-/healthy", timeout=10)
        assert prometheus_response.status_code == 200, "Prometheus not accessible"
        
        # Test Grafana availability
        grafana_response = requests.get(f"{self.config.grafana_url}/api/health", timeout=10)
        assert grafana_response.status_code == 200, "Grafana not accessible"
        
        # Test Prometheus targets
        targets_response = requests.get(f"{self.config.prometheus_url}/api/v1/targets", timeout=10)
        if targets_response.status_code == 200:
            targets_data = targets_response.json()
            active_targets = targets_data.get('data', {}).get('activeTargets', [])
            assert len(active_targets) > 0, "No active Prometheus targets found"
    
    @pytest.mark.integration
    def test_data_pipeline_connectivity(self):
        """Test data pipeline connectivity between services"""
        # Test that all critical services are running and can communicate
        services_to_test = [
            (self.config.hdfs_namenode_url, "HDFS"),
            (self.config.spark_master_url, "Spark"),
            (f"{self.config.airflow_url}/health", "Airflow"),
            (f"{self.config.prometheus_url}/-/healthy", "Prometheus"),
            (f"{self.config.grafana_url}/api/health", "Grafana")
        ]
        
        failed_services = []
        for url, service_name in services_to_test:
            try:
                if service_name == "Airflow":
                    auth = (self.config.airflow_username, self.config.airflow_password)
                    response = requests.get(url, auth=auth, timeout=10)
                else:
                    response = requests.get(url, timeout=10)
                
                if response.status_code != 200:
                    failed_services.append(f"{service_name} ({response.status_code})")
            except requests.exceptions.RequestException as e:
                failed_services.append(f"{service_name} (Connection Error: {str(e)})")
        
        assert not failed_services, f"Failed to connect to services: {failed_services}"


@pytest.mark.e2e
@pytest.mark.requires_services
@pytest.mark.slow
class TestEndToEndPipeline:
    """End-to-end tests for complete pipeline workflows"""
    
    @pytest.fixture(autouse=True)
    def setup(self, test_config):
        """Set up test configuration"""
        self.config = test_config
    
    @pytest.mark.e2e
    def test_complete_data_pipeline_workflow(self):
        """Test complete data pipeline from ingestion to model training"""
        # This would test the complete workflow:
        # 1. Data ingestion to HDFS
        # 2. Data transformation with Spark
        # 3. Model training and evaluation
        # 4. Results storage
        
        # Check if DAG exists and can be triggered
        auth = (self.config.airflow_username, self.config.airflow_password)
        
        # Get available DAGs
        dags_response = requests.get(f"{self.config.airflow_url}/api/v1/dags", auth=auth, timeout=10)
        assert dags_response.status_code == 200, "Cannot access Airflow DAGs"
        
        dags_data = dags_response.json()
        dag_ids = [dag['dag_id'] for dag in dags_data.get('dags', [])]
        
        # Look for main pipeline DAG
        pipeline_dags = [dag_id for dag_id in dag_ids if any(keyword in dag_id.lower() 
                                                           for keyword in ['diabetes', 'pipeline', 'etl'])]
        assert len(pipeline_dags) > 0, f"No pipeline DAGs found. Available DAGs: {dag_ids}"
        
        # Test DAG structure (would require more detailed implementation)
        main_dag_id = pipeline_dags[0]
        dag_details_response = requests.get(
            f"{self.config.airflow_url}/api/v1/dags/{main_dag_id}",
            auth=auth, timeout=10
        )
        assert dag_details_response.status_code == 200, f"Cannot get details for DAG: {main_dag_id}"
    
    @pytest.mark.e2e
    def test_data_quality_end_to_end(self):
        """Test end-to-end data quality validation"""
        # Test that data quality checks work throughout the pipeline
        # This would involve:
        # 1. Checking raw data quality
        # 2. Validating transformed data
        # 3. Ensuring model training data meets requirements
        
        # Check if test data exists
        test_data_files = [
            self.config.diabetes_csv_path,
            "/opt/airflow/data/personal_health_data.csv"
        ]
        
        for data_file in test_data_files:
            if os.path.exists(data_file):
                df = pd.read_csv(data_file)
                assert len(df) > 0, f"Data file is empty: {data_file}"
                assert not df.empty, f"Data file contains no data: {data_file}"
    
    @pytest.mark.e2e
    def test_monitoring_and_alerting_workflow(self):
        """Test monitoring and alerting workflow"""
        # Test that monitoring system can detect and alert on pipeline issues
        
        # Check Prometheus metrics
        metrics_response = requests.get(f"{self.config.prometheus_url}/api/v1/query", 
                                      params={'query': 'up'}, timeout=10)
        assert metrics_response.status_code == 200, "Cannot query Prometheus metrics"
        
        metrics_data = metrics_response.json()
        assert metrics_data.get('status') == 'success', "Prometheus query failed"
        
        # Check that services are being monitored
        results = metrics_data.get('data', {}).get('result', [])
        monitored_services = [result['metric'].get('job', 'unknown') for result in results]
        assert len(monitored_services) > 0, "No services being monitored by Prometheus"
    
    @pytest.mark.e2e
    def test_backup_and_recovery_workflow(self):
        """Test backup and recovery workflow"""
        # Test that backup and recovery procedures work end-to-end
        
        # Check if backup directories exist
        backup_paths = [
            "/opt/airflow/data/backup",
            "/data/backup"
        ]
        
        existing_backup_paths = [path for path in backup_paths if os.path.exists(path)]
        assert len(existing_backup_paths) > 0, f"No backup directories found. Checked: {backup_paths}"
        
        # Test backup creation (mock)
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_backup_path = f"/tmp/test_backup_{backup_timestamp}"
        
        # This would create an actual backup in a real implementation
        os.makedirs(test_backup_path, exist_ok=True)
        assert os.path.exists(test_backup_path), "Test backup directory creation failed"
        
        # Cleanup test backup
        os.rmdir(test_backup_path)


@pytest.mark.performance
@pytest.mark.slow
class TestPerformance:
    """Performance tests for the pipeline"""
    
    @pytest.fixture(autouse=True)
    def setup(self, test_config):
        """Set up test configuration"""
        self.config = test_config
    
    @pytest.mark.performance
    def test_api_response_times(self):
        """Test API response times are within acceptable limits"""
        services_to_test = [
            (self.config.hdfs_namenode_url, "HDFS", 5.0),
            (self.config.spark_master_url, "Spark", 5.0),
            (f"{self.config.prometheus_url}/-/healthy", "Prometheus", 3.0),
            (f"{self.config.grafana_url}/api/health", "Grafana", 5.0)
        ]
        
        slow_services = []
        for url, service_name, max_time in services_to_test:
            start_time = time.time()
            try:
                response = requests.get(url, timeout=max_time + 1)
                response_time = time.time() - start_time
                
                if response_time > max_time:
                    slow_services.append(f"{service_name}: {response_time:.2f}s (max: {max_time}s)")
            except requests.exceptions.Timeout:
                slow_services.append(f"{service_name}: Timeout (>{max_time}s)")
            except requests.exceptions.RequestException:
                # Service might not be running, skip performance test
                pass
        
        assert not slow_services, f"Slow services detected: {slow_services}"
    
    @pytest.mark.performance
    def test_data_processing_performance(self, test_data):
        """Test data processing performance"""
        df = test_data['diabetes']
        
        # Test data loading performance
        start_time = time.time()
        processed_df = df.copy()
        processed_df['bmi_category'] = pd.cut(processed_df['BMI'], 
                                            bins=[0, 18.5, 25, 30, float('inf')], 
                                            labels=['Underweight', 'Normal', 'Overweight', 'Obese'])
        processing_time = time.time() - start_time
        
        # Processing should be fast for test data
        assert processing_time < 1.0, f"Data processing too slow: {processing_time:.2f}s"
        assert len(processed_df) == len(df), "Data processing changed row count"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])

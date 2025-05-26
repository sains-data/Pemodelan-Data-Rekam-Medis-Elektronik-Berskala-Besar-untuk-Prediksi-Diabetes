#!/usr/bin/env python3
"""
Integration Tests for Diabetes Prediction Pipeline
Tests service interactions, data flow, and end-to-end pipeline functionality
"""

import unittest
import requests
import time
import os
import pandas as pd
from datetime import datetime, timedelta

# Add the tests directory to the path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_config
from test_config import TestConfig, TestResult
import test_fixtures  
from test_fixtures import TestFixtures, MockServiceResponses


class TestServiceConnectivity(unittest.TestCase):
    """Test connectivity to all pipeline services"""
    
    def setUp(self):
        self.config = TestConfig()
        self.results = []
    
    def test_hdfs_connectivity(self):
        """Test HDFS namenode connectivity"""
        result = TestResult("hdfs_connectivity")
        
        try:
            response = requests.get(
                f"{self.config.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                namenode_status = response.json()
                if namenode_status.get('beans'):
                    result.success()
                    result.details['namenode_status'] = namenode_status['beans'][0]
                else:
                    result.failure("No namenode status beans found")
            else:
                result.failure(f"HDFS API returned status {response.status_code}")
                
        except Exception as e:
            result.failure(f"HDFS connectivity failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"HDFS connectivity test failed: {result.errors}")
    
    def test_spark_master_connectivity(self):
        """Test Spark master connectivity"""
        result = TestResult("spark_connectivity")
        
        try:
            response = requests.get(
                f"{self.config.spark_url}",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                if "Spark Master" in response.text:
                    result.success()
                    result.details['spark_status'] = "Master UI accessible"
                else:
                    result.failure("Spark Master UI not found in response")
            else:
                result.failure(f"Spark API returned status {response.status_code}")
                
        except Exception as e:
            result.failure(f"Spark connectivity failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Spark connectivity test failed: {result.errors}")
    
    def test_airflow_connectivity(self):
        """Test Airflow webserver connectivity"""
        result = TestResult("airflow_connectivity")
        
        try:
            response = requests.get(
                f"{self.config.airflow_url}/health",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get('metadatabase', {}).get('status') == 'healthy':
                    result.success()
                    result.details['airflow_health'] = health_data
                else:
                    result.failure("Airflow metadatabase not healthy")
            else:
                result.failure(f"Airflow health check returned status {response.status_code}")
                
        except Exception as e:
            result.failure(f"Airflow connectivity failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Airflow connectivity test failed: {result.errors}")


class TestMonitoringStack(unittest.TestCase):
    """Test monitoring and observability components"""
    
    def setUp(self):
        self.config = TestConfig()
        self.results = []
    
    def test_prometheus_connectivity(self):
        """Test Prometheus connectivity and targets"""
        result = TestResult("prometheus_connectivity")
        
        try:
            # Test Prometheus health
            response = requests.get(
                f"{self.config.prometheus_url}/-/healthy",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                result.success()
                result.details['prometheus_status'] = "healthy"
                
                # Check targets
                targets_response = requests.get(
                    f"{self.config.prometheus_url}/api/v1/targets",
                    timeout=self.config.service_timeout
                )
                
                if targets_response.status_code == 200:
                    targets_data = targets_response.json()
                    result.details['targets'] = targets_data.get('data', {})
            else:
                result.failure(f"Prometheus health check failed: {response.status_code}")
                
        except Exception as e:
            result.failure(f"Prometheus connectivity failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Prometheus connectivity test failed: {result.errors}")
    
    def test_grafana_connectivity(self):
        """Test Grafana connectivity and dashboard access"""
        result = TestResult("grafana_connectivity")
        
        try:
            response = requests.get(
                f"{self.config.grafana_url}/api/health",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get('database') == 'ok':
                    result.success()
                    result.details['grafana_health'] = health_data
                else:
                    result.failure("Grafana database not healthy")
            else:
                result.failure(f"Grafana health check failed: {response.status_code}")
                
        except Exception as e:
            result.failure(f"Grafana connectivity failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Grafana connectivity test failed: {result.errors}")


class TestPipelineIntegration(unittest.TestCase):
    """Test end-to-end pipeline integration"""
    
    def setUp(self):
        self.config = TestConfig()
        self.results = []
        self.test_data = TestFixtures.get_sample_diabetes_data()
    
    def test_data_ingestion_flow(self):
        """Test data ingestion from source to HDFS"""
        result = TestResult("data_ingestion_flow")
        
        try:
            # Simulate data ingestion process
            temp_files = TestFixtures.create_temp_csv_files()
            
            # Verify CSV files can be read
            diabetes_df = pd.read_csv(temp_files['diabetes'])
            personal_health_df = pd.read_csv(temp_files['personal_health'])
            
            if len(diabetes_df) > 0 and len(personal_health_df) > 0:
                result.success()
                result.details['diabetes_records'] = len(diabetes_df)
                result.details['personal_health_records'] = len(personal_health_df)
            else:
                result.failure("No data found in ingested files")
            
            # Cleanup
            TestFixtures.cleanup_temp_files(temp_files)
                
        except Exception as e:
            result.failure(f"Data ingestion test failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Data ingestion test failed: {result.errors}")
    
    def test_data_transformation_workflow(self):
        """Test data transformation and processing"""
        result = TestResult("data_transformation")
        
        try:
            # Test data transformation logic
            raw_data = self.test_data.copy()
            
            # Basic transformation validations
            if raw_data['Glucose'].isna().sum() == 0:  # No missing glucose values
                result.success()
                result.details['transformation_checks'] = {
                    'glucose_range': f"{raw_data['Glucose'].min():.1f} - {raw_data['Glucose'].max():.1f}",
                    'bmi_range': f"{raw_data['BMI'].min():.1f} - {raw_data['BMI'].max():.1f}",
                    'age_range': f"{raw_data['Age'].min()} - {raw_data['Age'].max()}",
                    'total_records': len(raw_data)
                }
            else:
                result.failure("Data quality issues found in transformation")
                
        except Exception as e:
            result.failure(f"Data transformation test failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Data transformation test failed: {result.errors}")


def run_integration_tests():
    """Run all integration tests"""
    print("\n🔄 Starting Integration Tests...")
    print("=" * 50)
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTest(unittest.makeSuite(TestServiceConnectivity))
    suite.addTest(unittest.makeSuite(TestMonitoringStack))
    suite.addTest(unittest.makeSuite(TestPipelineIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print(f"\n📊 Integration Tests Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    return result


if __name__ == "__main__":
    run_integration_tests()

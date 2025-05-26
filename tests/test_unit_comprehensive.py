#!/usr/bin/env python3
"""
Enhanced Unit Tests
Comprehensive unit tests for individual components using test fixtures
"""

import unittest
import os
import json
import tempfile
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from test_config import TestConfig, TestResult
from test_fixtures import (
    TestFixtures, MockServiceResponses, TestDataGenerator,
    get_test_diabetes_data, get_test_personal_health_data,
    create_temp_test_files, cleanup_test_files
)


class TestDataValidation(unittest.TestCase):
    """Unit tests for data validation functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = TestConfig()
        self.test_data = get_test_diabetes_data()
        self.temp_files = create_temp_test_files()
        
    def tearDown(self):
        """Clean up test fixtures"""
        cleanup_test_files(self.temp_files)
    
    def test_valid_data_structure(self):
        """Test validation of valid data structure"""
        # Test with valid diabetes data
        self.assertIsInstance(self.test_data, pd.DataFrame)
        self.assertGreater(len(self.test_data), 0)
        self.assertEqual(len(self.test_data.columns), 9)  # 8 features + 1 outcome
        
    def test_required_columns_present(self):
        """Test that all required columns are present"""
        required_columns = [
            'Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness',
            'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome'
        ]
        
        for column in required_columns:
            self.assertIn(column, self.test_data.columns, 
                         f"Required column '{column}' is missing")
    
    def test_data_types_validation(self):
        """Test data type validation"""
        numeric_columns = [
            'Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness',
            'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age'
        ]
        
        for column in numeric_columns:
            self.assertTrue(pd.api.types.is_numeric_dtype(self.test_data[column]),
                           f"Column '{column}' should be numeric")
        
        # Outcome should be binary (0 or 1)
        unique_outcomes = set(self.test_data['Outcome'].unique())
        self.assertTrue(unique_outcomes.issubset({0, 1}),
                       "Outcome column should contain only 0 and 1")
    
    def test_data_ranges_validation(self):
        """Test data range validation"""
        # Test reasonable ranges for each column
        self.assertTrue(self.test_data['Pregnancies'].min() >= 0)
        self.assertTrue(self.test_data['Pregnancies'].max() <= 20)
        
        self.assertTrue(self.test_data['Glucose'].min() >= 0)
        self.assertTrue(self.test_data['Glucose'].max() <= 300)
        
        self.assertTrue(self.test_data['BloodPressure'].min() >= 0)
        self.assertTrue(self.test_data['BloodPressure'].max() <= 200)
        
        self.assertTrue(self.test_data['BMI'].min() >= 0)
        self.assertTrue(self.test_data['BMI'].max() <= 100)
        
        self.assertTrue(self.test_data['Age'].min() >= 0)
        self.assertTrue(self.test_data['Age'].max() <= 150)
    
    def test_missing_values_detection(self):
        """Test missing values detection"""
        # Create data with missing values
        data_with_nulls = self.test_data.copy()
        data_with_nulls.loc[0:5, 'Glucose'] = np.nan
        
        # Check that missing values are detected
        missing_count = data_with_nulls['Glucose'].isnull().sum()
        self.assertEqual(missing_count, 6)
        
        # Test completeness calculation
        completeness = (len(data_with_nulls) - missing_count) / len(data_with_nulls)
        self.assertAlmostEqual(completeness, 0.94, places=2)
    
    def test_outlier_detection(self):
        """Test outlier detection using IQR method"""
        column = 'Glucose'
        Q1 = self.test_data[column].quantile(0.25)
        Q3 = self.test_data[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = self.test_data[
            (self.test_data[column] < lower_bound) | 
            (self.test_data[column] > upper_bound)
        ]
        
        # Outlier percentage should be reasonable
        outlier_percentage = len(outliers) / len(self.test_data) * 100
        self.assertLess(outlier_percentage, 20, "Too many outliers detected")
    
    def test_file_loading_validation(self):
        """Test file loading and validation"""
        # Test loading from temporary file
        df_loaded = pd.read_csv(self.temp_files['diabetes'])
        
        # Verify loaded data matches original
        self.assertEqual(len(df_loaded), len(self.test_data))
        self.assertEqual(list(df_loaded.columns), list(self.test_data.columns))
    
    def test_invalid_data_handling(self):
        """Test handling of invalid data"""
        invalid_samples = TestFixtures.get_invalid_data_samples()
        
        for i, invalid_data in enumerate(invalid_samples):
            with self.subTest(f"Invalid sample {i}"):
                # Test that validation properly identifies issues
                if len(invalid_data.columns) < 9:
                    # Missing columns test
                    required_cols = set(['Pregnancies', 'Glucose', 'BloodPressure', 
                                       'SkinThickness', 'Insulin', 'BMI', 
                                       'DiabetesPedigreeFunction', 'Age', 'Outcome'])
                    actual_cols = set(invalid_data.columns)
                    missing_cols = required_cols - actual_cols
                    self.assertGreater(len(missing_cols), 0)


class TestEnvironmentConfiguration(unittest.TestCase):
    """Unit tests for environment configuration"""
    
    def setUp(self):
        """Set up test configuration"""
        self.config = TestConfig()
        self.mock_env = TestFixtures.get_mock_environment_config()
    
    def test_default_configuration_values(self):
        """Test default configuration values"""
        self.assertEqual(self.config.hdfs_namenode_url, "http://localhost:9870")
        self.assertEqual(self.config.spark_master_url, "http://localhost:8081")
        self.assertEqual(self.config.airflow_url, "http://localhost:8089")
        self.assertEqual(self.config.service_timeout, 30)
        self.assertEqual(self.config.pipeline_timeout, 1800)
    
    def test_environment_variable_parsing(self):
        """Test environment variable parsing"""
        with patch.dict(os.environ, self.mock_env):
            # Test that environment variables would be read correctly
            self.assertEqual(os.getenv('ENVIRONMENT'), 'test')
            self.assertEqual(os.getenv('PROJECT_NAME'), 'diabetes-prediction-test')
            self.assertEqual(os.getenv('HDFS_NAMENODE_HTTP_PORT'), '9870')
    
    def test_url_construction(self):
        """Test URL construction from configuration"""
        # Test URL construction logic
        base_url = "http://localhost"
        port = "9870"
        full_url = f"{base_url}:{port}"
        
        self.assertEqual(full_url, "http://localhost:9870")
        self.assertTrue(full_url.startswith("http://"))
    
    def test_timeout_validation(self):
        """Test timeout validation"""
        # Service timeout should be positive
        self.assertGreater(self.config.service_timeout, 0)
        self.assertLess(self.config.service_timeout, 300)  # Should be reasonable
        
        # Pipeline timeout should be longer than service timeout
        self.assertGreater(self.config.pipeline_timeout, self.config.service_timeout)


class TestDockerConfiguration(unittest.TestCase):
    """Unit tests for Docker configuration validation"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_config = TestFixtures.get_mock_docker_compose_config()
    
    def test_docker_compose_structure(self):
        """Test Docker Compose configuration structure"""
        self.assertIn('version', self.mock_config)
        self.assertIn('services', self.mock_config)
        
        # Test version format
        version = self.mock_config['version']
        self.assertIsInstance(version, str)
        self.assertRegex(version, r'^\d+\.\d+$')
    
    def test_service_definitions(self):
        """Test service definitions in Docker Compose"""
        services = self.mock_config['services']
        
        # Test required services are defined
        required_services = ['namenode', 'spark-master', 'airflow']
        for service in required_services:
            self.assertIn(service, services, f"Service '{service}' is missing")
        
        # Test each service has required fields
        for service_name, service_config in services.items():
            self.assertIn('image', service_config, 
                         f"Service '{service_name}' missing 'image' field")
    
    def test_port_mappings(self):
        """Test port mapping validation"""
        services = self.mock_config['services']
        
        for service_name, service_config in services.items():
            if 'ports' in service_config:
                ports = service_config['ports']
                self.assertIsInstance(ports, list)
                
                for port_mapping in ports:
                    self.assertIsInstance(port_mapping, str)
                    self.assertRegex(port_mapping, r'^\d+:\d+$',
                                   f"Invalid port mapping in {service_name}: {port_mapping}")
    
    def test_environment_variables(self):
        """Test environment variable validation"""
        services = self.mock_config['services']
        
        for service_name, service_config in services.items():
            if 'environment' in service_config:
                env_vars = service_config['environment']
                self.assertIsInstance(env_vars, list)
                
                for env_var in env_vars:
                    self.assertIsInstance(env_var, str)
                    self.assertIn('=', env_var, 
                                f"Invalid environment variable format in {service_name}: {env_var}")


class TestUtilityFunctions(unittest.TestCase):
    """Unit tests for utility functions"""
    
    def test_timestamp_generation(self):
        """Test timestamp generation utilities"""
        # Test current timestamp
        now = datetime.now()
        timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Verify format
        self.assertRegex(timestamp_str, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
        
        # Test parsing back
        parsed_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        self.assertAlmostEqual(now.timestamp(), parsed_time.timestamp(), delta=1)
    
    def test_data_size_calculations(self):
        """Test data size calculation utilities"""
        # Test dataframe size calculation
        df = get_test_diabetes_data()
        
        # Memory usage calculation
        memory_usage = df.memory_usage(deep=True).sum()
        self.assertGreater(memory_usage, 0)
        
        # Row count validation
        row_count = len(df)
        self.assertGreater(row_count, 0)
        
        # Column count validation
        col_count = len(df.columns)
        self.assertEqual(col_count, 9)
    
    def test_percentage_calculations(self):
        """Test percentage calculation utilities"""
        # Test success rate calculation
        total_tests = 100
        passed_tests = 85
        success_rate = (passed_tests / total_tests) * 100
        
        self.assertEqual(success_rate, 85.0)
        self.assertGreaterEqual(success_rate, 0)
        self.assertLessEqual(success_rate, 100)
    
    def test_json_serialization(self):
        """Test JSON serialization of test results"""
        test_result = {
            'test_name': 'sample_test',
            'status': 'passed',
            'duration': 1.5,
            'timestamp': datetime.now().isoformat(),
            'details': {
                'assertions': 5,
                'failures': 0
            }
        }
        
        # Test serialization
        json_str = json.dumps(test_result, default=str)
        self.assertIsInstance(json_str, str)
        
        # Test deserialization
        parsed_result = json.loads(json_str)
        self.assertEqual(parsed_result['test_name'], 'sample_test')
        self.assertEqual(parsed_result['status'], 'passed')


class TestErrorHandling(unittest.TestCase):
    """Unit tests for error handling mechanisms"""
    
    def test_file_not_found_handling(self):
        """Test file not found error handling"""
        non_existent_file = '/path/that/does/not/exist.csv'
        
        with self.assertRaises(FileNotFoundError):
            pd.read_csv(non_existent_file)
    
    def test_invalid_url_handling(self):
        """Test invalid URL handling"""
        from urllib.parse import urlparse
        
        # Test URL validation
        valid_url = "http://localhost:9870"
        invalid_url = "not_a_url"
        
        valid_parsed = urlparse(valid_url)
        invalid_parsed = urlparse(invalid_url)
        
        self.assertTrue(valid_parsed.scheme in ['http', 'https'])
        self.assertFalse(invalid_parsed.scheme in ['http', 'https'])
    
    def test_data_type_conversion_errors(self):
        """Test data type conversion error handling"""
        # Test conversion of invalid string to numeric
        invalid_data = pd.Series(['1', '2', 'invalid', '4'])
        
        # Test that conversion with errors='coerce' handles invalid values
        converted_data = pd.to_numeric(invalid_data, errors='coerce')
        self.assertTrue(converted_data.isna().any())
        self.assertEqual(converted_data.dropna().sum(), 7.0)  # 1 + 2 + 4
    
    def test_timeout_error_simulation(self):
        """Test timeout error simulation"""
        import time
        
        # Simulate a function that might timeout
        def slow_function(duration):
            time.sleep(duration)
            return "completed"
        
        # Test quick execution
        start_time = time.time()
        result = slow_function(0.1)
        execution_time = time.time() - start_time
        
        self.assertEqual(result, "completed")
        self.assertLess(execution_time, 1.0)


class TestDataGenerators(unittest.TestCase):
    """Unit tests for test data generators"""
    
    def test_time_series_generation(self):
        """Test time series data generation"""
        days = 7
        ts_data = TestDataGenerator.generate_time_series_data(days)
        
        self.assertIsInstance(ts_data, pd.DataFrame)
        self.assertGreater(len(ts_data), 0)
        
        # Check expected columns
        expected_columns = [
            'timestamp', 'cpu_usage', 'memory_usage', 'disk_usage',
            'network_io', 'active_connections', 'response_time'
        ]
        for column in expected_columns:
            self.assertIn(column, ts_data.columns)
        
        # Check data ranges
        self.assertTrue((ts_data['cpu_usage'] >= 0).all())
        self.assertTrue((ts_data['cpu_usage'] <= 100).all())
        self.assertTrue((ts_data['memory_usage'] >= 0).all())
        self.assertTrue((ts_data['memory_usage'] <= 100).all())
    
    def test_log_entry_generation(self):
        """Test log entry generation"""
        count = 50
        log_entries = TestDataGenerator.generate_log_entries(count)
        
        self.assertEqual(len(log_entries), count)
        
        # Check each log entry structure
        for log_entry in log_entries:
            self.assertIn('timestamp', log_entry)
            self.assertIn('level', log_entry)
            self.assertIn('service', log_entry)
            self.assertIn('message', log_entry)
            self.assertIn('correlation_id', log_entry)
            self.assertIn('host', log_entry)
            
            # Validate log level
            self.assertIn(log_entry['level'], ['INFO', 'WARN', 'ERROR', 'DEBUG'])
    
    def test_pipeline_metrics_generation(self):
        """Test pipeline metrics generation"""
        metrics = TestDataGenerator.generate_pipeline_metrics()
        
        # Check required fields
        required_fields = [
            'execution_id', 'start_time', 'end_time', 'status', 'stages', 'resource_usage'
        ]
        for field in required_fields:
            self.assertIn(field, metrics)
        
        # Check stages
        stages = metrics['stages']
        self.assertIn('ingestion', stages)
        self.assertIn('transformation', stages)
        self.assertIn('model_training', stages)
        
        # Check resource usage
        resource_usage = metrics['resource_usage']
        self.assertIn('max_cpu_percent', resource_usage)
        self.assertIn('max_memory_mb', resource_usage)
        self.assertIn('disk_io_mb', resource_usage)


if __name__ == '__main__':
    # Configure test runner
    unittest.main(
        verbosity=2,
        buffer=True,
        warnings='ignore'
    )

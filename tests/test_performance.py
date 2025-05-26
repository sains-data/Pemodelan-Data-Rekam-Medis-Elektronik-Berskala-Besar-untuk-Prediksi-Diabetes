#!/usr/bin/env python3
"""
Performance and Load Testing for Diabetes Prediction Pipeline
Comprehensive performance benchmarking, stress testing, and resource monitoring
"""

import unittest
import requests
import time
import pytest
import psutil
import numpy as np
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Dict, List, Tuple
from unittest.mock import patch, Mock

# Add the tests directory to the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_config
from test_config import TestConfig, TestResult, TestReporter
import test_fixtures
from test_fixtures import TestFixtures, MockServiceResponses


class PerformanceMetrics:
    """Collect and manage performance metrics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.memory_usage = []
        self.cpu_usage = []
        self.response_times = []
        
    def start_monitoring(self):
        """Start performance monitoring"""
        self.start_time = time.time()
        self.memory_usage = []
        self.cpu_usage = []
        
    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.end_time = time.time()
        
    def record_metrics(self):
        """Record current system metrics"""
        self.memory_usage.append(psutil.virtual_memory().percent)
        self.cpu_usage.append(psutil.cpu_percent())
        
    def get_summary(self) -> Dict:
        """Get performance summary"""
        duration = self.end_time - self.start_time if self.end_time else 0
        return {
            'duration': duration,
            'avg_memory': np.mean(self.memory_usage) if self.memory_usage else 0,
            'max_memory': max(self.memory_usage) if self.memory_usage else 0,
            'avg_cpu': np.mean(self.cpu_usage) if self.cpu_usage else 0,
            'max_cpu': max(self.cpu_usage) if self.cpu_usage else 0,
            'avg_response_time': np.mean(self.response_times) if self.response_times else 0,
            'max_response_time': max(self.response_times) if self.response_times else 0
        }

class TestPerformance(unittest.TestCase):
    def setUp(self):
        self.config = TestConfig()
        self.results = []

    def test_airflow_api_load(self):
        result = TestResult("airflow_api_load")
        try:
            start = time.time()
            responses = []
            for _ in range(10):
                resp = requests.get(f"{self.config.airflow_url}/api/v1/dags", auth=(self.config.airflow_username, self.config.airflow_password), timeout=self.config.service_timeout)
                responses.append(resp.status_code)
            duration = time.time() - start
            result.details['responses'] = responses
            result.details['duration'] = duration
            if all(code == 200 for code in responses):
                result.success()
            else:
                result.failure(f"Non-200 responses: {responses}")
        except Exception as e:
            result.failure(f"Airflow API load test failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Airflow API load test failed: {result.errors}")

    def test_spark_api_load(self):
        result = TestResult("spark_api_load")
        try:
            start = time.time()
            responses = []
            for _ in range(10):
                resp = requests.get(f"{self.config.spark_master_url}/json/", timeout=self.config.service_timeout)
                responses.append(resp.status_code)
            duration = time.time() - start
            result.details['responses'] = responses
            result.details['duration'] = duration
            if all(code == 200 for code in responses):
                result.success()
            else:
                result.failure(f"Non-200 responses: {responses}")
        except Exception as e:
            result.failure(f"Spark API load test failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Spark API load test failed: {result.errors}")

    def tearDown(self):
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "performance_tests")
            print(f"\n⚡ Performance test report generated: {report_file}")

@pytest.mark.performance
class TestPerformanceBenchmarks:
    """Performance benchmarking test suite"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.config = TestConfig()
        self.fixtures = TestFixtures()
        self.metrics = PerformanceMetrics()
        
    def test_data_processing_performance(self):
        """Test data processing performance with large datasets"""
        # Generate large test dataset
        large_dataset = self.fixtures.generate_large_diabetes_dataset(10000)
        
        self.metrics.start_monitoring()
        monitoring_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        monitoring_thread.start()
        
        start_time = time.time()
        
        # Simulate data processing operations
        processed_data = large_dataset.copy()
        processed_data['risk_score'] = np.random.random(len(processed_data))
        processed_data['prediction'] = np.random.choice([0, 1], len(processed_data))
        
        # Data aggregations
        summary_stats = processed_data.groupby('prediction').agg({
            'glucose': ['mean', 'std'],
            'bmi': ['mean', 'std'],
            'age': ['mean', 'std']
        })
        
        end_time = time.time()
        self.metrics.stop_monitoring()
        
        processing_time = end_time - start_time
        
        # Performance assertions
        assert processing_time < 30, f"Data processing took too long: {processing_time:.2f}s"
        assert len(processed_data) == 10000, "Data processing corrupted dataset"
        
        # Log performance metrics
        metrics = self.metrics.get_summary()
        print(f"Data Processing Performance:")
        print(f"  Processing time: {processing_time:.2f}s")
        print(f"  Memory usage: {metrics['avg_memory']:.1f}% (max: {metrics['max_memory']:.1f}%)")
        print(f"  CPU usage: {metrics['avg_cpu']:.1f}% (max: {metrics['max_cpu']:.1f}%)")
        
    def test_ml_model_performance(self):
        """Test machine learning model training and prediction performance"""
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
        
        # Generate training data
        dataset = self.fixtures.generate_large_diabetes_dataset(5000)
        X = dataset[['glucose', 'bmi', 'age', 'blood_pressure']]
        y = np.random.choice([0, 1], len(dataset))
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        self.metrics.start_monitoring()
        monitoring_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        monitoring_thread.start()
        
        # Model training performance
        start_time = time.time()
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        training_time = time.time() - start_time
        
        # Model prediction performance
        start_time = time.time()
        predictions = model.predict(X_test)
        prediction_time = time.time() - start_time
        
        self.metrics.stop_monitoring()
        
        # Performance assertions
        assert training_time < 60, f"Model training took too long: {training_time:.2f}s"
        assert prediction_time < 5, f"Model prediction took too long: {prediction_time:.2f}s"
        
        accuracy = accuracy_score(y_test, predictions)
        assert accuracy > 0.4, f"Model accuracy too low: {accuracy:.3f}"
        
        # Log performance metrics
        metrics = self.metrics.get_summary()
        print(f"ML Model Performance:")
        print(f"  Training time: {training_time:.2f}s")
        print(f"  Prediction time: {prediction_time:.4f}s")
        print(f"  Model accuracy: {accuracy:.3f}")
        print(f"  Memory usage: {metrics['avg_memory']:.1f}% (max: {metrics['max_memory']:.1f}%)")
        
    def test_concurrent_requests_performance(self):
        """Test system performance under concurrent load"""
        self.metrics.start_monitoring()
        
        # Simulate concurrent API requests
        def simulate_api_request(request_id: int) -> float:
            start_time = time.time()
            
            # Simulate processing
            data = self.fixtures.generate_sample_diabetes_data()
            time.sleep(0.1)  # Simulate processing time
            
            # Simulate prediction
            prediction = np.random.choice([0, 1])
            
            return time.time() - start_time
        
        # Run concurrent requests
        num_concurrent = 20
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(simulate_api_request, i) for i in range(num_concurrent)]
            
            response_times = []
            for future in as_completed(futures):
                response_time = future.result()
                response_times.append(response_time)
                self.metrics.response_times.append(response_time)
        
        self.metrics.stop_monitoring()
        
        # Performance assertions
        avg_response_time = np.mean(response_times)
        max_response_time = max(response_times)
        
        assert avg_response_time < 1.0, f"Average response time too high: {avg_response_time:.3f}s"
        assert max_response_time < 2.0, f"Maximum response time too high: {max_response_time:.3f}s"
        
        # Log performance metrics
        metrics = self.metrics.get_summary()
        print(f"Concurrent Load Performance:")
        print(f"  Concurrent requests: {num_concurrent}")
        print(f"  Average response time: {avg_response_time:.3f}s")
        print(f"  Maximum response time: {max_response_time:.3f}s")
        print(f"  Memory usage: {metrics['avg_memory']:.1f}% (max: {metrics['max_memory']:.1f}%)")
        
    def _monitor_resources(self):
        """Monitor system resources in background"""
        while self.metrics.start_time and not self.metrics.end_time:
            self.metrics.record_metrics()
            time.sleep(0.5)


@pytest.mark.performance
@pytest.mark.stress
class TestStressTests:
    """Stress testing for system limits"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.config = TestConfig()
        self.fixtures = TestFixtures()
        
    def test_large_dataset_processing(self):
        """Test processing very large datasets"""
        # Generate large dataset (adjust size based on system capacity)
        large_size = 50000
        large_dataset = self.fixtures.generate_large_diabetes_dataset(large_size)
        
        start_time = time.time()
        
        # Perform complex operations
        result = large_dataset.groupby(['age_group', 'bmi_category']).agg({
            'glucose': ['mean', 'std', 'min', 'max'],
            'blood_pressure': ['mean', 'std'],
            'bmi': ['mean', 'std']
        })
        
        processing_time = time.time() - start_time
        
        assert processing_time < 120, f"Large dataset processing took too long: {processing_time:.2f}s"
        assert len(result) > 0, "Large dataset processing failed"
        
        print(f"Large Dataset Stress Test:")
        print(f"  Dataset size: {large_size:,} records")
        print(f"  Processing time: {processing_time:.2f}s")
        print(f"  Records per second: {large_size/processing_time:.0f}")

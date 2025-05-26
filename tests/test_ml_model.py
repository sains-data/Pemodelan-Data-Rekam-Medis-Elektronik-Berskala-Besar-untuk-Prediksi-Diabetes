#!/usr/bin/env python3
"""
ML Model Validation Tests
Tests for model file existence, metrics, and evaluation
"""

import unittest
import os
import json
from test_config import TestConfig, TestResult, TestReporter

class TestMLModel(unittest.TestCase):
    def setUp(self):
        self.config = TestConfig()
        self.results = []

    def test_model_file_existence(self):
        result = TestResult("model_file_existence")
        model_dir = self.config.models_path
        try:
            if os.path.exists(model_dir):
                files = os.listdir(model_dir)
                model_files = [f for f in files if f.endswith('.model') or f.endswith('.pkl') or f.endswith('.sav')]
                result.details['model_files'] = model_files
                if model_files:
                    result.success()
                else:
                    result.failure("No model files found in models directory")
            else:
                result.failure(f"Model directory does not exist: {model_dir}")
        except Exception as e:
            result.failure(f"Model file existence check failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Model file existence failed: {result.errors}")

    def test_model_metrics(self):
        result = TestResult("model_metrics")
        metrics_path = os.path.join(self.config.logs_path, 'quality_report.json')
        try:
            if os.path.exists(metrics_path):
                with open(metrics_path) as f:
                    metrics = json.load(f)
                result.details['metrics'] = metrics
                if metrics.get('model_accuracy', 0) > 0.6:
                    result.success()
                else:
                    result.failure(f"Model accuracy too low: {metrics.get('model_accuracy')}")
            else:
                result.failure(f"Metrics file not found: {metrics_path}")
        except Exception as e:
            result.failure(f"Model metrics check failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Model metrics failed: {result.errors}")

    def tearDown(self):
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "ml_model_tests")
            print(f"\n📊 ML model test report generated: {report_file}")

if __name__ == '__main__':
    print("🤖 Running ML Model Validation Tests")
    print("=" * 50)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMLModel)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\n📋 Test Summary")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

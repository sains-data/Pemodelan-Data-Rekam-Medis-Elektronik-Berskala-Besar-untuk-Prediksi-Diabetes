#!/usr/bin/env python3
"""
Test Runner - Orchestrates all tests for the diabetes prediction pipeline
Runs unit tests, integration tests, and end-to-end tests with comprehensive reporting
"""

import unittest
import sys
import os
import time
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# Add the tests directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_config
from test_config import TestConfig, TestResult, TestReporter
from test_service_health import TestServiceHealth
from test_data_quality import TestDataQuality
import test_integration_clean
from test_integration_clean import TestServiceConnectivity, TestMonitoringStack, TestPipelineIntegration
from test_ml_model import TestMLModel
from test_security import TestSecurity
from test_backup_recovery import TestBackupRecovery
from test_deployment import TestDeployment
from test_performance import TestPerformance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestSuiteRunner:
    """Comprehensive test suite runner for the diabetes prediction pipeline"""
    
    def __init__(self):
        self.config = TestConfig()
        self.reporter = TestReporter()
        self.test_results = []
        self.start_time = None
        self.end_time = None
        
    def run_unit_tests(self) -> Dict[str, Any]:
        """Run all unit tests"""
        logger.info("🧪 Running Unit Tests...")
        
        # Create test suite for unit tests
        suite = unittest.TestSuite()
        
        # Add unit test classes
        # For now, create a simple unit test placeholder since we have comprehensive unit tests elsewhere
        from test_unit_comprehensive import TestDataValidation
        suite.addTest(unittest.makeSuite(TestDataValidation))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=2, stream=open(os.devnull, 'w'))
        result = runner.run(suite)
        
        unit_results = {
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'skipped': len(result.skipped) if hasattr(result, 'skipped') else 0,
            'success_rate': ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0,
            'details': {
                'failures': [str(test) + ': ' + str(error) for test, error in result.failures],
                'errors': [str(test) + ': ' + str(error) for test, error in result.errors]
            }
        }
        
        logger.info(f"✅ Unit Tests completed: {unit_results['success_rate']:.1f}% success rate")
        return unit_results
        
    def run_integration_tests(self) -> Dict[str, Any]:
        """Run all integration tests"""
        logger.info("🔗 Running Integration Tests...")
        
        # Create test suite for integration tests
        suite = unittest.TestSuite()
        
        # Add integration test classes
        suite.addTest(unittest.makeSuite(TestPipelineIntegration))
        suite.addTest(unittest.makeSuite(TestServiceHealth))
        suite.addTest(unittest.makeSuite(TestDataQuality))
        suite.addTest(unittest.makeSuite(TestMLModel))
        suite.addTest(unittest.makeSuite(TestSecurity))
        suite.addTest(unittest.makeSuite(TestDeployment))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=2, stream=open(os.devnull, 'w'))
        result = runner.run(suite)
        
        integration_results = {
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'skipped': len(result.skipped) if hasattr(result, 'skipped') else 0,
            'success_rate': ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0,
            'details': {
                'failures': [str(test) + ': ' + str(error) for test, error in result.failures],
                'errors': [str(test) + ': ' + str(error) for test, error in result.errors]
            }
        }
        
        logger.info(f"✅ Integration Tests completed: {integration_results['success_rate']:.1f}% success rate")
        return integration_results
        
    def run_e2e_tests(self) -> Dict[str, Any]:
        """Run all end-to-end tests"""
        logger.info("🎯 Running End-to-End Tests...")
        
        # Create test suite for E2E tests
        suite = unittest.TestSuite()
        
        # Add E2E test classes
        suite.addTest(unittest.makeSuite(TestServiceConnectivity))
        suite.addTest(unittest.makeSuite(TestMonitoringStack))
        suite.addTest(unittest.makeSuite(TestPerformance))
        suite.addTest(unittest.makeSuite(TestBackupRecovery))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=2, stream=open(os.devnull, 'w'))
        result = runner.run(suite)
        
        e2e_results = {
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'skipped': len(result.skipped) if hasattr(result, 'skipped') else 0,
            'success_rate': ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0,
            'details': {
                'failures': [str(test) + ': ' + str(error) for test, error in result.failures],
                'errors': [str(test) + ': ' + str(error) for test, error in result.errors]
            }
        }
        
        logger.info(f"✅ End-to-End Tests completed: {e2e_results['success_rate']:.1f}% success rate")
        return e2e_results
        
    def run_all_tests(self, test_types: List[str] = None) -> Dict[str, Any]:
        """Run all test suites"""
        self.start_time = datetime.now()
        logger.info(f"🚀 Starting comprehensive test run at {self.start_time}")
        
        if test_types is None:
            test_types = ['unit', 'integration', 'e2e']
            
        results = {
            'start_time': self.start_time.isoformat(),
            'test_environment': {
                'python_version': sys.version,
                'working_directory': os.getcwd(),
                'test_config': self.config.__dict__
            }
        }
        
        # Run test suites based on specified types
        if 'unit' in test_types:
            results['unit_tests'] = self.run_unit_tests()
            
        if 'integration' in test_types:
            results['integration_tests'] = self.run_integration_tests()
            
        if 'e2e' in test_types:
            results['e2e_tests'] = self.run_e2e_tests()
            
        self.end_time = datetime.now()
        results['end_time'] = self.end_time.isoformat()
        results['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
        
        # Calculate overall statistics
        total_tests = sum([results.get(f'{test_type}_tests', {}).get('tests_run', 0) for test_type in test_types])
        total_failures = sum([results.get(f'{test_type}_tests', {}).get('failures', 0) for test_type in test_types])
        total_errors = sum([results.get(f'{test_type}_tests', {}).get('errors', 0) for test_type in test_types])
        
        results['summary'] = {
            'total_tests': total_tests,
            'total_failures': total_failures,
            'total_errors': total_errors,
            'overall_success_rate': ((total_tests - total_failures - total_errors) / total_tests * 100) if total_tests > 0 else 0,
            'test_types_run': test_types
        }
        
        logger.info(f"🎉 Test run completed in {results['duration_seconds']:.2f} seconds")
        logger.info(f"📊 Overall Success Rate: {results['summary']['overall_success_rate']:.1f}%")
        
        return results
        
    def generate_reports(self, results: Dict[str, Any], output_dir: str = "test_reports"):
        """Generate test reports in multiple formats"""
        logger.info(f"📝 Generating test reports in {output_dir}/")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate JSON report
        json_path = os.path.join(output_dir, f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(json_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"📄 JSON report saved to: {json_path}")
        
        # Generate HTML report
        html_path = os.path.join(output_dir, f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        self._generate_html_report(results, html_path)
        logger.info(f"🌐 HTML report saved to: {html_path}")
        
        # Generate summary report
        summary_path = os.path.join(output_dir, "test_summary.txt")
        self._generate_summary_report(results, summary_path)
        logger.info(f"📋 Summary report saved to: {summary_path}")
        
    def _generate_html_report(self, results: Dict[str, Any], output_path: str):
        """Generate HTML test report"""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Diabetes Prediction Pipeline - Test Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .test-section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .success {{ color: green; font-weight: bold; }}
                .failure {{ color: red; font-weight: bold; }}
                .error {{ color: orange; font-weight: bold; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .progress-bar {{ width: 100%; background-color: #f0f0f0; border-radius: 10px; overflow: hidden; }}
                .progress-fill {{ height: 20px; background-color: #4CAF50; text-align: center; color: white; line-height: 20px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🩺 Diabetes Prediction Pipeline - Test Report</h1>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Duration:</strong> {results.get('duration_seconds', 0):.2f} seconds</p>
            </div>
            
            <div class="summary">
                <h2>📊 Test Summary</h2>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {results['summary']['overall_success_rate']:.1f}%">
                        {results['summary']['overall_success_rate']:.1f}% Success Rate
                    </div>
                </div>
                <table>
                    <tr><th>Metric</th><th>Value</th></tr>
                    <tr><td>Total Tests</td><td>{results['summary']['total_tests']}</td></tr>
                    <tr><td>Failures</td><td class="failure">{results['summary']['total_failures']}</td></tr>
                    <tr><td>Errors</td><td class="error">{results['summary']['total_errors']}</td></tr>
                    <tr><td>Success Rate</td><td class="success">{results['summary']['overall_success_rate']:.1f}%</td></tr>
                </table>
            </div>
        """
        
        # Add sections for each test type
        for test_type in ['unit_tests', 'integration_tests', 'e2e_tests']:
            if test_type in results:
                test_data = results[test_type]
                html_content += f"""
                <div class="test-section">
                    <h3>🧪 {test_type.replace('_', ' ').title()}</h3>
                    <table>
                        <tr><th>Metric</th><th>Value</th></tr>
                        <tr><td>Tests Run</td><td>{test_data['tests_run']}</td></tr>
                        <tr><td>Failures</td><td class="failure">{test_data['failures']}</td></tr>
                        <tr><td>Errors</td><td class="error">{test_data['errors']}</td></tr>
                        <tr><td>Success Rate</td><td class="success">{test_data['success_rate']:.1f}%</td></tr>
                    </table>
                """
                
                if test_data['details']['failures']:
                    html_content += "<h4>Failures:</h4><ul>"
                    for failure in test_data['details']['failures']:
                        html_content += f"<li class='failure'>{failure}</li>"
                    html_content += "</ul>"
                    
                if test_data['details']['errors']:
                    html_content += "<h4>Errors:</h4><ul>"
                    for error in test_data['details']['errors']:
                        html_content += f"<li class='error'>{error}</li>"
                    html_content += "</ul>"
                    
                html_content += "</div>"
        
        html_content += """
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
            
    def _generate_summary_report(self, results: Dict[str, Any], output_path: str):
        """Generate text summary report"""
        summary = f"""
        DIABETES PREDICTION PIPELINE - TEST SUMMARY
        ==========================================
        
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Duration: {results.get('duration_seconds', 0):.2f} seconds
        
        OVERALL RESULTS
        ---------------
        Total Tests: {results['summary']['total_tests']}
        Failures: {results['summary']['total_failures']}
        Errors: {results['summary']['total_errors']}
        Success Rate: {results['summary']['overall_success_rate']:.1f}%
        
        """
        
        # Add details for each test type
        for test_type in ['unit_tests', 'integration_tests', 'e2e_tests']:
            if test_type in results:
                test_data = results[test_type]
                summary += f"""
        {test_type.replace('_', ' ').upper()}
        {'-' * len(test_type)}
        Tests Run: {test_data['tests_run']}
        Failures: {test_data['failures']}
        Errors: {test_data['errors']}
        Success Rate: {test_data['success_rate']:.1f}%
        
        """
        
        with open(output_path, 'w') as f:
            f.write(summary)


def main():
    """Main entry point for test runner"""
    parser = argparse.ArgumentParser(description='Run diabetes prediction pipeline tests')
    parser.add_argument('--types', nargs='+', choices=['unit', 'integration', 'e2e'], 
                       default=['unit', 'integration', 'e2e'],
                       help='Test types to run (default: all)')
    parser.add_argument('--output-dir', default='test_reports',
                       help='Output directory for test reports (default: test_reports)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create test runner
    runner = TestSuiteRunner()
    
    try:
        # Run tests
        results = runner.run_all_tests(args.types)
        
        # Generate reports
        runner.generate_reports(results, args.output_dir)
        
        # Exit with error code if tests failed
        if results['summary']['overall_success_rate'] < 100:
            logger.warning(f"⚠️  Some tests failed. Success rate: {results['summary']['overall_success_rate']:.1f}%")
            sys.exit(1)
        else:
            logger.info("🎉 All tests passed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"❌ Test run failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

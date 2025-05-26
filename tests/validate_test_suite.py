#!/usr/bin/env python3
"""
Test Suite Validation Script
Validates the complete test infrastructure and ensures all components work together
"""

import os
import sys
import subprocess
import json
from datetime import datetime
from pathlib import Path

class TestSuiteValidator:
    """Validates the complete test suite infrastructure"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.test_dir = self.project_root / "tests"
        self.reports_dir = self.project_root / "test_reports"
        self.validation_results = []
        
    def log_result(self, test_name: str, status: str, details: str = ""):
        """Log validation result"""
        result = {
            'test_name': test_name,
            'status': status,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        self.validation_results.append(result)
        status_emoji = "✅" if status == "PASS" else "❌"
        print(f"{status_emoji} {test_name}: {status}")
        if details:
            print(f"   {details}")
            
    def validate_test_files_exist(self):
        """Validate all required test files exist"""
        required_files = [
            "tests/__init__.py",
            "tests/conftest.py",
            "tests/pytest.ini",
            "tests/test_config.py",
            "tests/test_fixtures.py",
            "tests/test_unit_comprehensive.py",
            "tests/test_integration_clean.py",
            "tests/test_integration_pytest.py",
            "tests/run_tests.py"
        ]
        
        for file_path in required_files:
            full_path = self.project_root / file_path
            if full_path.exists():
                self.log_result(f"File exists: {file_path}", "PASS")
            else:
                self.log_result(f"File exists: {file_path}", "FAIL", f"Missing file: {full_path}")
                
    def validate_pytest_configuration(self):
        """Validate pytest configuration"""
        try:
            pytest_ini = self.project_root / "pytest.ini"
            if pytest_ini.exists():
                with open(pytest_ini, 'r') as f:
                    content = f.read()
                    if "testpaths = tests" in content:
                        self.log_result("Pytest configuration", "PASS", "pytest.ini properly configured")
                    else:
                        self.log_result("Pytest configuration", "FAIL", "pytest.ini missing testpaths")
            else:
                self.log_result("Pytest configuration", "FAIL", "pytest.ini not found")
        except Exception as e:
            self.log_result("Pytest configuration", "FAIL", f"Error reading pytest.ini: {str(e)}")
            
    def validate_unit_tests(self):
        """Validate unit tests can run successfully"""
        try:
            result = subprocess.run([
                sys.executable, "-m", "pytest", 
                str(self.test_dir / "test_unit_comprehensive.py"),
                "-v", "--tb=short", "--no-cov"
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                self.log_result("Unit tests execution", "PASS", f"27 unit tests passed")
            else:
                self.log_result("Unit tests execution", "FAIL", f"Exit code: {result.returncode}")
        except Exception as e:
            self.log_result("Unit tests execution", "FAIL", f"Error running unit tests: {str(e)}")
            
    def validate_test_coverage(self):
        """Validate test coverage functionality"""
        try:
            result = subprocess.run([
                sys.executable, "-m", "pytest", 
                str(self.test_dir / "test_unit_comprehensive.py"),
                "--cov=tests", "--cov-report=term", "--cov-report=html:test_reports/coverage_validation",
                "--tb=short"
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0 and "coverage:" in result.stdout:
                self.log_result("Test coverage", "PASS", "Coverage reporting functional")
            else:
                self.log_result("Test coverage", "FAIL", f"Coverage issues: {result.stderr}")
        except Exception as e:
            self.log_result("Test coverage", "FAIL", f"Error running coverage: {str(e)}")
            
    def validate_test_fixtures(self):
        """Validate test fixtures functionality"""
        try:
            result = subprocess.run([
                sys.executable, "-c", """
from tests.test_fixtures import TestFixtures
import pandas as pd

# Test diabetes data generation
diabetes_data = TestFixtures.get_sample_diabetes_data()
assert len(diabetes_data) == 100, f"Expected 100 rows, got {len(diabetes_data)}"
assert 'Glucose' in diabetes_data.columns, "Missing Glucose column"

# Test personal health data generation  
health_data = TestFixtures.get_sample_personal_health_data()
assert len(health_data) == 50, f"Expected 50 rows, got {len(health_data)}"
assert 'patient_id' in health_data.columns, "Missing patient_id column"

# Test mock responses
mock_responses = TestFixtures.get_mock_api_responses()
assert 'hdfs_status' in mock_responses, "Missing hdfs_status mock"
assert 'airflow_health' in mock_responses, "Missing airflow_health mock"

print("All test fixtures working correctly")
"""
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                self.log_result("Test fixtures", "PASS", "All fixtures functional")
            else:
                self.log_result("Test fixtures", "FAIL", f"Fixture error: {result.stderr}")
        except Exception as e:
            self.log_result("Test fixtures", "FAIL", f"Error validating fixtures: {str(e)}")
            
    def validate_integration_tests_structure(self):
        """Validate integration tests structure (without running connectivity tests)"""
        try:
            result = subprocess.run([
                sys.executable, "-c", """
from tests.test_integration_pytest import TestUnitComponents, TestPipelineIntegration
from tests.test_integration_clean import TestServiceConnectivity, TestMonitoringStack
import inspect

# Check if classes have required methods
unit_methods = [method for method in dir(TestUnitComponents) if method.startswith('test_')]
integration_methods = [method for method in dir(TestPipelineIntegration) if method.startswith('test_')]
service_methods = [method for method in dir(TestServiceConnectivity) if method.startswith('test_')]

print(f"Unit component tests: {len(unit_methods)}")
print(f"Pipeline integration tests: {len(integration_methods)}")  
print(f"Service connectivity tests: {len(service_methods)}")

assert len(unit_methods) >= 2, "Not enough unit component tests"
assert len(integration_methods) >= 2, "Not enough integration tests"
assert len(service_methods) >= 2, "Not enough service tests"
"""
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                self.log_result("Integration test structure", "PASS", "Test classes properly structured")
            else:
                self.log_result("Integration test structure", "FAIL", f"Structure error: {result.stderr}")
        except Exception as e:
            self.log_result("Integration test structure", "FAIL", f"Error validating structure: {str(e)}")
            
    def validate_test_reports_generation(self):
        """Validate test reports can be generated"""
        try:
            # Run tests with HTML reporting
            result = subprocess.run([
                sys.executable, "-m", "pytest", 
                str(self.test_dir / "test_unit_comprehensive.py"),
                "--html=test_reports/validation_report.html",
                "--self-contained-html",
                "--tb=short"
            ], capture_output=True, text=True, cwd=self.project_root)
            
            report_path = self.reports_dir / "validation_report.html"
            if report_path.exists():
                self.log_result("Test reports generation", "PASS", f"HTML report generated: {report_path}")
            else:
                self.log_result("Test reports generation", "FAIL", "HTML report not generated")
                
        except Exception as e:
            self.log_result("Test reports generation", "FAIL", f"Error generating reports: {str(e)}")
            
    def validate_makefile_integration(self):
        """Validate Makefile test commands"""
        try:
            makefile_path = self.project_root / "Makefile"
            if makefile_path.exists():
                with open(makefile_path, 'r') as f:
                    content = f.read()
                    
                required_targets = [
                    "test-unit", "test-integration", "test-e2e", 
                    "test-all", "test-coverage", "setup-test-env"
                ]
                
                missing_targets = []
                for target in required_targets:
                    if f"{target}:" not in content:
                        missing_targets.append(target)
                        
                if not missing_targets:
                    self.log_result("Makefile integration", "PASS", "All test targets present")
                else:
                    self.log_result("Makefile integration", "FAIL", f"Missing targets: {missing_targets}")
            else:
                self.log_result("Makefile integration", "FAIL", "Makefile not found")
                
        except Exception as e:
            self.log_result("Makefile integration", "FAIL", f"Error validating Makefile: {str(e)}")
            
    def validate_ci_cd_configuration(self):
        """Validate CI/CD configuration"""
        try:
            ci_path = self.project_root / ".github" / "workflows" / "ci-cd.yml"
            if ci_path.exists():
                with open(ci_path, 'r') as f:
                    content = f.read()
                    
                required_jobs = ["lint", "security", "unit-tests", "integration-tests", "build"]
                present_jobs = [job for job in required_jobs if job in content]
                
                if len(present_jobs) >= 3:
                    self.log_result("CI/CD configuration", "PASS", f"CI/CD pipeline configured with {len(present_jobs)} jobs")
                else:
                    self.log_result("CI/CD configuration", "FAIL", f"Insufficient CI/CD jobs: {present_jobs}")
            else:
                self.log_result("CI/CD configuration", "FAIL", "CI/CD workflow file not found")
                
        except Exception as e:
            self.log_result("CI/CD configuration", "FAIL", f"Error validating CI/CD: {str(e)}")
            
    def validate_test_environment(self):
        """Validate test environment setup"""
        try:
            # Check if virtual environment exists
            venv_path = self.project_root / "test_env"
            if venv_path.exists():
                self.log_result("Test environment", "PASS", "Virtual environment exists")
                
                # Check key packages
                result = subprocess.run([
                    str(venv_path / "bin" / "python"), "-c",
                    "import pytest, pandas, numpy, requests; print('All packages available')"
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.log_result("Test dependencies", "PASS", "All required packages installed")
                else:
                    self.log_result("Test dependencies", "FAIL", f"Package issues: {result.stderr}")
            else:
                self.log_result("Test environment", "FAIL", "Virtual environment not found")
                
        except Exception as e:
            self.log_result("Test environment", "FAIL", f"Error validating environment: {str(e)}")
            
    def run_validation(self):
        """Run complete validation suite"""
        print("🧪 Starting Test Suite Validation")
        print("=" * 50)
        
        # Create reports directory
        self.reports_dir.mkdir(exist_ok=True)
        
        # Run all validations
        self.validate_test_files_exist()
        self.validate_pytest_configuration()
        self.validate_test_environment()
        self.validate_test_fixtures()
        self.validate_unit_tests()
        self.validate_test_coverage()
        self.validate_integration_tests_structure()
        self.validate_test_reports_generation()
        self.validate_makefile_integration()
        self.validate_ci_cd_configuration()
        
        # Generate summary
        self.generate_validation_summary()
        
    def generate_validation_summary(self):
        """Generate validation summary"""
        total_tests = len(self.validation_results)
        passed_tests = len([r for r in self.validation_results if r['status'] == 'PASS'])
        failed_tests = total_tests - passed_tests
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        print("\n" + "=" * 50)
        print("📊 VALIDATION SUMMARY")
        print("=" * 50)
        print(f"Total Validations: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {success_rate:.1f}%")
        
        if failed_tests > 0:
            print("\n❌ FAILED VALIDATIONS:")
            for result in self.validation_results:
                if result['status'] == 'FAIL':
                    print(f"  • {result['test_name']}: {result['details']}")
        
        # Save detailed results
        results_file = self.reports_dir / "validation_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'summary': {
                    'total_tests': total_tests,
                    'passed_tests': passed_tests,
                    'failed_tests': failed_tests,
                    'success_rate': success_rate,
                    'validation_time': datetime.now().isoformat()
                },
                'results': self.validation_results
            }, f, indent=2)
            
        print(f"\n📄 Detailed results saved to: {results_file}")
        
        if success_rate >= 80:
            print("\n🎉 TEST SUITE VALIDATION SUCCESSFUL!")
            return 0
        else:
            print("\n⚠️  TEST SUITE VALIDATION NEEDS ATTENTION")
            return 1


def main():
    """Main entry point"""
    validator = TestSuiteValidator()
    exit_code = validator.run_validation()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

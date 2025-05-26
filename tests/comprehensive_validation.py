#!/usr/bin/env python3
"""
Final Comprehensive Test Suite Validation
Validates the complete testing infrastructure and ensures deployment readiness
"""

import sys
import os
import subprocess
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import yaml

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tests.test_config import TestConfig, TestResult, TestReporter
from tests.validate_test_suite import TestSuiteValidator


class ComprehensiveTestValidator:
    """Comprehensive validation of the entire test suite infrastructure"""
    
    def __init__(self):
        self.project_root = Path("/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes")
        self.reports_dir = self.project_root / "test_reports" / "comprehensive"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validations': {},
            'summary': {},
            'recommendations': []
        }
        
    def validate_complete_infrastructure(self) -> Dict:
        """Validate complete testing infrastructure"""
        print("🧪 COMPREHENSIVE TEST SUITE VALIDATION")
        print("=" * 60)
        
        validations = [
            ("test_file_structure", self._validate_test_file_structure),
            ("test_configuration", self._validate_test_configuration),
            ("test_dependencies", self._validate_test_dependencies),
            ("test_execution_capability", self._validate_test_execution_capability),
            ("ci_cd_integration", self._validate_ci_cd_integration),
            ("performance_testing", self._validate_performance_testing),
            ("security_testing", self._validate_security_testing),
            ("integration_testing", self._validate_integration_testing),
            ("monitoring_capabilities", self._validate_monitoring_capabilities),
            ("deployment_readiness", self._validate_deployment_readiness)
        ]
        
        total_validations = len(validations)
        passed_validations = 0
        
        for validation_name, validation_func in validations:
            print(f"\n📋 Validating: {validation_name.replace('_', ' ').title()}")
            print("-" * 40)
            
            try:
                result = validation_func()
                self.validation_results['validations'][validation_name] = result
                
                if result.get('status') == 'PASS':
                    passed_validations += 1
                    print(f"✅ {validation_name}: PASSED")
                else:
                    print(f"❌ {validation_name}: FAILED")
                    if 'error' in result:
                        print(f"   Error: {result['error']}")
                        
            except Exception as e:
                print(f"❌ {validation_name}: ERROR - {str(e)}")
                self.validation_results['validations'][validation_name] = {
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        # Calculate summary
        success_rate = (passed_validations / total_validations) * 100
        self.validation_results['summary'] = {
            'total_validations': total_validations,
            'passed_validations': passed_validations,
            'failed_validations': total_validations - passed_validations,
            'success_rate': success_rate,
            'overall_status': 'READY' if success_rate >= 90 else 'NEEDS_ATTENTION'
        }
        
        self._generate_recommendations()
        self._save_results()
        self._print_summary()
        
        return self.validation_results
        
    def _validate_test_file_structure(self) -> Dict:
        """Validate test file structure and organization"""
        required_test_files = [
            "tests/__init__.py",
            "tests/conftest.py", 
            "tests/pytest.ini",
            "tests/test_config.py",
            "tests/test_fixtures.py",
            "tests/test_unit_comprehensive.py",
            "tests/test_integration_clean.py",
            "tests/test_integration_pytest.py",
            "tests/test_performance.py",
            "tests/test_scheduling.py",
            "tests/test_isolation.py",
            "tests/test_service_health.py",
            "tests/test_security.py",
            "tests/test_data_quality.py",
            "tests/test_ml_model.py",
            "tests/run_tests.py",
            "tests/validate_test_suite.py"
        ]
        
        missing_files = []
        present_files = []
        
        for file_path in required_test_files:
            full_path = self.project_root / file_path
            if full_path.exists():
                present_files.append(file_path)
            else:
                missing_files.append(file_path)
                
        return {
            'status': 'PASS' if not missing_files else 'FAIL',
            'present_files': len(present_files),
            'missing_files': missing_files,
            'total_required': len(required_test_files),
            'coverage_percentage': (len(present_files) / len(required_test_files)) * 100
        }
        
    def _validate_test_configuration(self) -> Dict:
        """Validate test configuration files"""
        config_validations = {}
        
        # Validate pytest.ini
        pytest_ini = self.project_root / "pytest.ini"
        if pytest_ini.exists():
            with open(pytest_ini, 'r') as f:
                content = f.read()
                config_validations['pytest_ini'] = {
                    'exists': True,
                    'has_markers': 'markers =' in content,
                    'has_testpaths': 'testpaths =' in content,
                    'has_addopts': 'addopts =' in content
                }
        else:
            config_validations['pytest_ini'] = {'exists': False}
            
        # Validate conftest.py
        conftest = self.project_root / "tests" / "conftest.py"
        if conftest.exists():
            with open(conftest, 'r') as f:
                content = f.read()
                config_validations['conftest'] = {
                    'exists': True,
                    'has_fixtures': '@pytest.fixture' in content,
                    'has_markers': 'pytest.mark' in content
                }
        else:
            config_validations['conftest'] = {'exists': False}
            
        # Validate test_config.py
        test_config = self.project_root / "tests" / "test_config.py"
        if test_config.exists():
            config_validations['test_config'] = {'exists': True}
        else:
            config_validations['test_config'] = {'exists': False}
            
        all_valid = all(
            config.get('exists', False) 
            for config in config_validations.values()
        )
        
        return {
            'status': 'PASS' if all_valid else 'FAIL',
            'configurations': config_validations
        }
        
    def _validate_test_dependencies(self) -> Dict:
        """Validate test dependencies and environment"""
        try:
            # Check if test environment exists
            test_env = self.project_root / "test_env"
            
            if not test_env.exists():
                return {
                    'status': 'FAIL',
                    'error': 'Test environment not found'
                }
                
            # Check key dependencies
            result = subprocess.run(
                ["test_env/bin/python", "-c", "import pytest, pandas, numpy, sklearn, docker; print('OK')"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            dependencies_ok = result.returncode == 0 and 'OK' in result.stdout
            
            return {
                'status': 'PASS' if dependencies_ok else 'FAIL',
                'test_env_exists': test_env.exists(),
                'dependencies_available': dependencies_ok,
                'error': result.stderr if result.stderr else None
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_test_execution_capability(self) -> Dict:
        """Validate that tests can actually be executed"""
        try:
            # Run a simple test to verify execution capability
            result = subprocess.run(
                ["test_env/bin/python", "-m", "pytest", "--collect-only", "tests/test_unit_comprehensive.py"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            collection_successful = result.returncode == 0
            test_count = result.stdout.count('test session starts') > 0
            
            return {
                'status': 'PASS' if collection_successful else 'FAIL',
                'can_collect_tests': collection_successful,
                'test_discovery_working': test_count,
                'output': result.stdout[:500] if result.stdout else None,
                'error': result.stderr[:500] if result.stderr else None
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_ci_cd_integration(self) -> Dict:
        """Validate CI/CD integration"""
        ci_cd_file = self.project_root / ".github" / "workflows" / "ci-cd.yml"
        
        if not ci_cd_file.exists():
            return {
                'status': 'FAIL',
                'error': 'CI/CD configuration file not found'
            }
            
        try:
            with open(ci_cd_file, 'r') as f:
                ci_cd_config = yaml.safe_load(f)
                
            jobs = ci_cd_config.get('jobs', {})
            
            required_jobs = ['test-unit', 'test-integration', 'test-e2e']
            present_jobs = [job for job in required_jobs if job in jobs]
            
            return {
                'status': 'PASS' if len(present_jobs) >= 2 else 'FAIL',
                'ci_cd_file_exists': True,
                'total_jobs': len(jobs),
                'required_jobs_present': len(present_jobs),
                'job_names': list(jobs.keys())
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_performance_testing(self) -> Dict:
        """Validate performance testing capabilities"""
        perf_test_file = self.project_root / "tests" / "test_performance.py"
        
        if not perf_test_file.exists():
            return {
                'status': 'FAIL',
                'error': 'Performance test file not found'
            }
            
        try:
            with open(perf_test_file, 'r') as f:
                content = f.read()
                
            performance_features = {
                'has_performance_markers': '@pytest.mark.performance' in content,
                'has_stress_markers': '@pytest.mark.stress' in content,
                'has_benchmark_tests': 'benchmark' in content.lower(),
                'has_resource_monitoring': 'psutil' in content,
                'has_concurrent_testing': 'ThreadPoolExecutor' in content
            }
            
            feature_count = sum(performance_features.values())
            
            return {
                'status': 'PASS' if feature_count >= 3 else 'FAIL',
                'features': performance_features,
                'feature_count': feature_count
            }
            
        except Exception as e:
            return {
                'status': 'FAIL', 
                'error': str(e)
            }
            
    def _validate_security_testing(self) -> Dict:
        """Validate security testing capabilities"""
        security_test_file = self.project_root / "tests" / "test_security.py"
        
        if not security_test_file.exists():
            return {
                'status': 'FAIL',
                'error': 'Security test file not found'
            }
            
        try:
            with open(security_test_file, 'r') as f:
                content = f.read()
                
            security_features = {
                'has_security_markers': '@pytest.mark.security' in content,
                'has_vulnerability_tests': 'vulnerability' in content.lower(),
                'has_authentication_tests': 'auth' in content.lower(),
                'has_encryption_tests': 'encrypt' in content.lower() or 'decrypt' in content.lower()
            }
            
            feature_count = sum(security_features.values())
            
            return {
                'status': 'PASS' if feature_count >= 2 else 'FAIL',
                'features': security_features,
                'feature_count': feature_count
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_integration_testing(self) -> Dict:
        """Validate integration testing capabilities"""
        integration_files = [
            self.project_root / "tests" / "test_integration_clean.py",
            self.project_root / "tests" / "test_integration_pytest.py"
        ]
        
        existing_files = [f for f in integration_files if f.exists()]
        
        if not existing_files:
            return {
                'status': 'FAIL',
                'error': 'No integration test files found'
            }
            
        try:
            integration_features = {
                'service_connectivity_tests': False,
                'data_flow_tests': False,
                'api_integration_tests': False,
                'database_integration_tests': False
            }
            
            for file_path in existing_files:
                with open(file_path, 'r') as f:
                    content = f.read()
                    
                if 'connectivity' in content.lower():
                    integration_features['service_connectivity_tests'] = True
                if 'data' in content.lower() and 'flow' in content.lower():
                    integration_features['data_flow_tests'] = True
                if 'api' in content.lower():
                    integration_features['api_integration_tests'] = True
                if 'database' in content.lower() or 'db' in content.lower():
                    integration_features['database_integration_tests'] = True
                    
            feature_count = sum(integration_features.values())
            
            return {
                'status': 'PASS' if feature_count >= 2 else 'FAIL',
                'integration_files': len(existing_files),
                'features': integration_features,
                'feature_count': feature_count
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_monitoring_capabilities(self) -> Dict:
        """Validate monitoring and observability testing"""
        monitoring_files = [
            self.project_root / "tests" / "test_service_health.py",
            self.project_root / "tests" / "test_performance.py"
        ]
        
        existing_files = [f for f in monitoring_files if f.exists()]
        
        if not existing_files:
            return {
                'status': 'FAIL',
                'error': 'No monitoring test files found'
            }
            
        try:
            monitoring_features = {
                'health_checks': False,
                'metric_collection': False,
                'alert_testing': False,
                'dashboard_validation': False
            }
            
            for file_path in existing_files:
                with open(file_path, 'r') as f:
                    content = f.read()
                    
                if 'health' in content.lower():
                    monitoring_features['health_checks'] = True
                if 'metric' in content.lower() or 'monitor' in content.lower():
                    monitoring_features['metric_collection'] = True
                if 'alert' in content.lower():
                    monitoring_features['alert_testing'] = True
                if 'dashboard' in content.lower():
                    monitoring_features['dashboard_validation'] = True
                    
            feature_count = sum(monitoring_features.values())
            
            return {
                'status': 'PASS' if feature_count >= 2 else 'FAIL',
                'monitoring_files': len(existing_files),
                'features': monitoring_features,
                'feature_count': feature_count
            }
            
        except Exception as e:
            return {
                'status': 'FAIL',
                'error': str(e)
            }
            
    def _validate_deployment_readiness(self) -> Dict:
        """Validate deployment readiness"""
        deployment_indicators = {
            'makefile_targets': False,
            'docker_configuration': False,
            'environment_scripts': False,
            'documentation_complete': False,
            'test_automation': False
        }
        
        # Check Makefile targets
        makefile_path = self.project_root / "Makefile"
        if makefile_path.exists():
            with open(makefile_path, 'r') as f:
                makefile_content = f.read()
                if any(target in makefile_content for target in ['test-all', 'deploy', 'up']):
                    deployment_indicators['makefile_targets'] = True
                    
        # Check Docker configuration
        docker_compose = self.project_root / "docker-compose.yml"
        if docker_compose.exists():
            deployment_indicators['docker_configuration'] = True
            
        # Check environment scripts
        env_scripts = [
            self.project_root / "setup.sh",
            self.project_root / "setup_env.sh"
        ]
        if any(script.exists() for script in env_scripts):
            deployment_indicators['environment_scripts'] = True
            
        # Check documentation
        readme_file = self.project_root / "README.md"
        if readme_file.exists():
            with open(readme_file, 'r') as f:
                readme_content = f.read()
                if len(readme_content) > 1000:  # Substantial documentation
                    deployment_indicators['documentation_complete'] = True
                    
        # Check test automation
        ci_cd_file = self.project_root / ".github" / "workflows" / "ci-cd.yml"
        if ci_cd_file.exists():
            deployment_indicators['test_automation'] = True
            
        readiness_score = sum(deployment_indicators.values())
        
        return {
            'status': 'PASS' if readiness_score >= 4 else 'FAIL',
            'indicators': deployment_indicators,
            'readiness_score': readiness_score,
            'max_score': len(deployment_indicators)
        }
        
    def _generate_recommendations(self):
        """Generate recommendations based on validation results"""
        recommendations = []
        
        # Analyze results and generate recommendations
        for validation_name, result in self.validation_results['validations'].items():
            if result.get('status') == 'FAIL':
                if validation_name == 'test_file_structure':
                    if result.get('missing_files'):
                        recommendations.append(f"Create missing test files: {', '.join(result['missing_files'][:3])}")
                        
                elif validation_name == 'test_dependencies':
                    recommendations.append("Set up test environment with: make test-env-setup")
                    
                elif validation_name == 'ci_cd_integration':
                    recommendations.append("Complete CI/CD configuration with required test jobs")
                    
                elif validation_name == 'performance_testing':
                    recommendations.append("Enhance performance testing with more comprehensive benchmarks")
                    
                elif validation_name == 'security_testing':
                    recommendations.append("Add more security test coverage and vulnerability scans")
                    
        # Add general recommendations based on success rate
        success_rate = self.validation_results['summary']['success_rate']
        if success_rate < 70:
            recommendations.append("Test suite needs significant improvements before deployment")
        elif success_rate < 90:
            recommendations.append("Address remaining test validation issues for production readiness")
        else:
            recommendations.append("Test suite is deployment ready - consider adding additional edge case coverage")
            
        self.validation_results['recommendations'] = recommendations
        
    def _save_results(self):
        """Save validation results to file"""
        results_file = self.reports_dir / "comprehensive_validation.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
            
        print(f"\n📄 Detailed results saved to: {results_file}")
        
    def _print_summary(self):
        """Print validation summary"""
        summary = self.validation_results['summary']
        
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE VALIDATION SUMMARY")
        print("=" * 60)
        
        print(f"Total Validations: {summary['total_validations']}")
        print(f"Passed: {summary['passed_validations']}")
        print(f"Failed: {summary['failed_validations']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Overall Status: {summary['overall_status']}")
        
        if self.validation_results['recommendations']:
            print(f"\n📋 RECOMMENDATIONS:")
            for i, rec in enumerate(self.validation_results['recommendations'], 1):
                print(f"  {i}. {rec}")
                
        status_emoji = "🎉" if summary['overall_status'] == 'READY' else "⚠️"
        print(f"\n{status_emoji} TEST SUITE STATUS: {summary['overall_status']}")


def main():
    """Main execution function"""
    validator = ComprehensiveTestValidator()
    results = validator.validate_complete_infrastructure()
    
    # Exit with appropriate code
    exit_code = 0 if results['summary']['overall_status'] == 'READY' else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

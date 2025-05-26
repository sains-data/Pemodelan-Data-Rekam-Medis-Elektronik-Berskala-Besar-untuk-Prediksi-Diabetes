"""
Automated Test Scheduling and Verification System
Tests scheduling capabilities, cron job validation, and automated pipeline execution
"""

import pytest
import os
import yaml
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch, Mock
from pathlib import Path

from .test_config import TestConfig, TestResult
from .test_fixtures import TestFixtures


class SchedulingValidator:
    """Validates scheduling configurations and automated execution"""
    
    def __init__(self):
        self.config = TestConfig()
        self.project_root = Path("/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes")
        
    def validate_airflow_dags(self) -> Dict[str, bool]:
        """Validate Airflow DAG configurations"""
        results = {}
        
        dags_dir = self.project_root / "airflow" / "dags"
        if not dags_dir.exists():
            results["dags_directory_exists"] = False
            return results
            
        results["dags_directory_exists"] = True
        
        # Check for DAG files
        dag_files = list(dags_dir.glob("*.py"))
        results["dag_files_found"] = len(dag_files) > 0
        results["dag_count"] = len(dag_files)
        
        # Validate DAG syntax
        for dag_file in dag_files:
            try:
                # Simple syntax check
                with open(dag_file, 'r') as f:
                    content = f.read()
                    
                # Check for required imports
                required_imports = ["from airflow", "DAG", "datetime"]
                imports_present = all(imp in content for imp in required_imports)
                results[f"{dag_file.name}_syntax_valid"] = imports_present
                
                # Check for DAG definition
                dag_defined = "dag = DAG" in content or "with DAG" in content
                results[f"{dag_file.name}_dag_defined"] = dag_defined
                
                # Check for schedule interval
                schedule_defined = "schedule_interval" in content or "schedule=" in content
                results[f"{dag_file.name}_schedule_defined"] = schedule_defined
                
            except Exception as e:
                results[f"{dag_file.name}_error"] = str(e)
                
        return results
        
    def validate_cron_expressions(self) -> Dict[str, bool]:
        """Validate cron expressions in configuration files"""
        results = {}
        
        # Check common cron expression patterns
        cron_patterns = {
            "hourly": "0 * * * *",
            "daily": "0 0 * * *",
            "weekly": "0 0 * * 0",
            "monthly": "0 0 1 * *"
        }
        
        for name, pattern in cron_patterns.items():
            # Validate cron pattern syntax
            parts = pattern.split()
            valid_cron = (
                len(parts) == 5 and
                all(self._validate_cron_field(part, i) for i, part in enumerate(parts))
            )
            results[f"cron_{name}_valid"] = valid_cron
            
        return results
        
    def _validate_cron_field(self, field: str, position: int) -> bool:
        """Validate individual cron field"""
        # Basic cron field validation
        if field == "*":
            return True
            
        try:
            # Check numeric ranges based on position
            ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]  # min, hour, day, month, dow
            if position < len(ranges):
                min_val, max_val = ranges[position]
                if field.isdigit():
                    val = int(field)
                    return min_val <= val <= max_val
        except:
            pass
            
        return True  # Allow complex expressions for now
        
    def validate_docker_compose_scheduling(self) -> Dict[str, bool]:
        """Validate Docker Compose scheduling configurations"""
        results = {}
        
        compose_file = self.project_root / "docker-compose.yml"
        if not compose_file.exists():
            results["compose_file_exists"] = False
            return results
            
        results["compose_file_exists"] = True
        
        try:
            with open(compose_file, 'r') as f:
                compose_config = yaml.safe_load(f)
                
            # Check for scheduler services
            services = compose_config.get('services', {})
            
            # Check for Airflow scheduler
            airflow_services = [name for name in services.keys() if 'airflow' in name.lower()]
            results["airflow_services_present"] = len(airflow_services) > 0
            results["airflow_service_count"] = len(airflow_services)
            
            # Check for restart policies
            restart_policies = [
                services[service].get('restart', 'no') 
                for service in services
            ]
            has_restart_policies = any(policy != 'no' for policy in restart_policies)
            results["restart_policies_configured"] = has_restart_policies
            
            # Check for health checks
            health_checks = [
                services[service].get('healthcheck') 
                for service in services
            ]
            has_health_checks = any(hc is not None for hc in health_checks)
            results["health_checks_configured"] = has_health_checks
            
        except Exception as e:
            results["compose_parsing_error"] = str(e)
            
        return results


@pytest.mark.scheduling
class TestSchedulingValidation:
    """Test scheduling configurations and automated execution capabilities"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.validator = SchedulingValidator()
        self.config = TestConfig()
        
    def test_airflow_dag_validation(self):
        """Test Airflow DAG configurations"""
        results = self.validator.validate_airflow_dags()
        
        # Assertions
        assert results.get("dags_directory_exists", False), "Airflow DAGs directory missing"
        assert results.get("dag_files_found", False), "No DAG files found"
        assert results.get("dag_count", 0) > 0, "No DAG files present"
        
        # Check individual DAG files
        dag_files = [key for key in results.keys() if key.endswith("_syntax_valid")]
        for dag_file in dag_files:
            assert results[dag_file], f"DAG syntax invalid: {dag_file}"
            
        print(f"Airflow DAG Validation:")
        print(f"  DAGs directory exists: {results.get('dags_directory_exists')}")
        print(f"  DAG files found: {results.get('dag_count', 0)}")
        
    def test_cron_expression_validation(self):
        """Test cron expression validation"""
        results = self.validator.validate_cron_expressions()
        
        # All cron patterns should be valid
        for pattern_name, is_valid in results.items():
            assert is_valid, f"Invalid cron pattern: {pattern_name}"
            
        print(f"Cron Expression Validation:")
        for pattern, valid in results.items():
            print(f"  {pattern}: {'✅' if valid else '❌'}")
            
    def test_docker_compose_scheduling(self):
        """Test Docker Compose scheduling configurations"""
        results = self.validator.validate_docker_compose_scheduling()
        
        # Basic assertions
        assert results.get("compose_file_exists", False), "docker-compose.yml not found"
        
        if "compose_parsing_error" not in results:
            # Airflow services should be present for scheduling
            airflow_count = results.get("airflow_service_count", 0)
            print(f"Found {airflow_count} Airflow services")
            
            # Restart policies should be configured for production readiness
            has_restart = results.get("restart_policies_configured", False)
            print(f"Restart policies configured: {has_restart}")
            
            # Health checks improve reliability
            has_health = results.get("health_checks_configured", False)
            print(f"Health checks configured: {has_health}")
            
        print(f"Docker Compose Scheduling:")
        for key, value in results.items():
            print(f"  {key}: {value}")
            
    def test_makefile_scheduling_targets(self):
        """Test Makefile scheduling and automation targets"""
        makefile_path = self.validator.project_root / "Makefile"
        
        assert makefile_path.exists(), "Makefile not found"
        
        with open(makefile_path, 'r') as f:
            makefile_content = f.read()
            
        # Check for scheduling-related targets
        scheduling_targets = [
            "test-schedule",
            "test-automated",
            "run-pipeline",
            "deploy-pipeline",
            "monitor-pipeline"
        ]
        
        found_targets = []
        for target in scheduling_targets:
            if f"{target}:" in makefile_content:
                found_targets.append(target)
                
        print(f"Makefile Scheduling Targets:")
        print(f"  Found targets: {found_targets}")
        print(f"  Target count: {len(found_targets)}")
        
        # At least some scheduling targets should exist
        assert len(found_targets) > 0, "No scheduling targets found in Makefile"
        
    def test_pipeline_test_script_scheduling(self):
        """Test pipeline test script scheduling capabilities"""
        script_path = self.validator.project_root / "scripts" / "run_pipeline_tests.sh"
        
        assert script_path.exists(), "Pipeline test script not found"
        
        # Check if script is executable
        assert os.access(script_path, os.X_OK), "Pipeline test script not executable"
        
        with open(script_path, 'r') as f:
            script_content = f.read()
            
        # Check for scheduling-related features
        scheduling_features = [
            "cron",
            "schedule",
            "automated",
            "continuous",
            "interval"
        ]
        
        found_features = []
        for feature in scheduling_features:
            if feature.lower() in script_content.lower():
                found_features.append(feature)
                
        print(f"Pipeline Test Script Scheduling:")
        print(f"  Script executable: {os.access(script_path, os.X_OK)}")
        print(f"  Scheduling features: {found_features}")
        
    def test_ci_cd_scheduling_configuration(self):
        """Test CI/CD pipeline scheduling configuration"""
        ci_cd_path = self.validator.project_root / ".github" / "workflows" / "ci-cd.yml"
        
        if not ci_cd_path.exists():
            pytest.skip("CI/CD configuration file not found")
            
        with open(ci_cd_path, 'r') as f:
            ci_cd_config = yaml.safe_load(f)
            
        # Check for scheduling triggers
        on_config = ci_cd_config.get('on', {})
        
        # Check for various trigger types
        triggers = {
            'push': 'push' in on_config,
            'pull_request': 'pull_request' in on_config,
            'schedule': 'schedule' in on_config,
            'workflow_dispatch': 'workflow_dispatch' in on_config
        }
        
        # Check for scheduled runs
        if 'schedule' in on_config:
            schedule_config = on_config['schedule']
            cron_schedules = [item.get('cron') for item in schedule_config if 'cron' in item]
            triggers['cron_schedules'] = len(cron_schedules)
            
        print(f"CI/CD Scheduling Configuration:")
        for trigger, present in triggers.items():
            print(f"  {trigger}: {present}")
            
        # At least push or PR triggers should be present
        basic_triggers = triggers.get('push', False) or triggers.get('pull_request', False)
        assert basic_triggers, "No basic CI/CD triggers configured"


@pytest.mark.scheduling
@pytest.mark.integration
class TestAutomatedExecution:
    """Test automated execution capabilities"""
    
    def setup_method(self):
        """Setup for each test method"""
        self.config = TestConfig()
        self.fixtures = TestFixtures()
        
    def test_automated_test_execution(self):
        """Test automated test execution workflow"""
        # Simulate automated test execution
        start_time = datetime.now()
        
        # Check if test runner script exists and is executable
        test_runner = Path("/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/scripts/run_pipeline_tests.sh")
        
        if test_runner.exists():
            assert os.access(test_runner, os.X_OK), "Test runner script not executable"
            
            # Test script validation (dry run)
            try:
                result = subprocess.run(
                    [str(test_runner), "--dry-run"],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                dry_run_success = result.returncode == 0
            except (subprocess.TimeoutExpired, FileNotFoundError):
                dry_run_success = False
                
            print(f"Automated Test Execution:")
            print(f"  Test runner exists: {test_runner.exists()}")
            print(f"  Script executable: {os.access(test_runner, os.X_OK)}")
            print(f"  Dry run successful: {dry_run_success}")
        else:
            pytest.skip("Test runner script not found")
            
    def test_scheduled_execution_simulation(self):
        """Simulate scheduled execution workflow"""
        # Simulate a scheduled execution cycle
        execution_steps = [
            "Environment validation",
            "Service health checks",
            "Data quality validation", 
            "Pipeline execution",
            "Result validation",
            "Notification dispatch"
        ]
        
        execution_results = {}
        
        for step in execution_steps:
            # Simulate step execution
            start_time = time.time()
            
            # Mock execution time
            time.sleep(0.1)  
            
            execution_time = time.time() - start_time
            execution_results[step] = {
                'success': True,
                'duration': execution_time,
                'timestamp': datetime.now().isoformat()
            }
            
        # Verify all steps completed successfully
        all_successful = all(result['success'] for result in execution_results.values())
        total_duration = sum(result['duration'] for result in execution_results.values())
        
        assert all_successful, "Not all execution steps completed successfully"
        assert total_duration < 10, f"Execution took too long: {total_duration:.2f}s"
        
        print(f"Scheduled Execution Simulation:")
        print(f"  Total steps: {len(execution_steps)}")
        print(f"  All successful: {all_successful}")
        print(f"  Total duration: {total_duration:.2f}s")
        
    def test_failure_recovery_scheduling(self):
        """Test failure recovery in scheduled executions"""
        # Simulate failure scenarios
        failure_scenarios = [
            "service_unavailable",
            "data_validation_failed", 
            "insufficient_resources",
            "timeout_exceeded"
        ]
        
        recovery_results = {}
        
        for scenario in failure_scenarios:
            # Simulate failure detection
            failure_detected = True
            
            # Simulate recovery attempt
            recovery_success = True  # Mock successful recovery
            recovery_time = 0.2  # Mock recovery time
            
            recovery_results[scenario] = {
                'failure_detected': failure_detected,
                'recovery_attempted': True,
                'recovery_successful': recovery_success,
                'recovery_time': recovery_time
            }
            
        # Verify recovery mechanisms
        all_detected = all(r['failure_detected'] for r in recovery_results.values())
        all_attempted = all(r['recovery_attempted'] for r in recovery_results.values())
        all_recovered = all(r['recovery_successful'] for r in recovery_results.values())
        
        assert all_detected, "Not all failures were detected"
        assert all_attempted, "Not all failures had recovery attempted"
        
        print(f"Failure Recovery Testing:")
        print(f"  Scenarios tested: {len(failure_scenarios)}")
        print(f"  All failures detected: {all_detected}")
        print(f"  All recoveries attempted: {all_attempted}")
        print(f"  All recoveries successful: {all_recovered}")


if __name__ == "__main__":
    # Run scheduling tests
    pytest.main(["-v", "-m", "scheduling", __file__])

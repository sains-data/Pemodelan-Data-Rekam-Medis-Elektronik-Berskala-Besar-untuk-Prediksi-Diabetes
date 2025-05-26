#!/usr/bin/env python3
"""
Test Configuration and Utilities
Common configuration, fixtures, and utilities for all tests
"""

import os
import json
import requests
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@dataclass
class TestConfig:
    """Test configuration class"""
    # Service URLs
    hdfs_namenode_url: str = "http://localhost:9870"
    spark_master_url: str = "http://localhost:8081"
    airflow_url: str = "http://localhost:8089"
    grafana_url: str = "http://localhost:3000" 
    nifi_url: str = "http://localhost:8080/nifi"
    prometheus_url: str = "http://localhost:9090"
    
    # Authentication
    airflow_username: str = "admin"
    airflow_password: str = "admin"
    grafana_username: str = "admin"
    grafana_password: str = "admin"
    
    # Timeouts
    service_timeout: int = 30
    pipeline_timeout: int = 1800  # 30 minutes
    
    # Data paths
    bronze_data_path: str = "/data/bronze"
    silver_data_path: str = "/data/silver"
    gold_data_path: str = "/data/gold"
    models_path: str = "/models"
    logs_path: str = "/opt/airflow/data/logs"
    
    # Test data
    diabetes_csv_path: str = "/opt/airflow/data/diabetes.csv"
    personal_health_csv_path: str = "/opt/airflow/data/personal_health_data.csv"
    
    # Expected schema
    diabetes_schema: List[str] = None
    
    def __post_init__(self):
        if self.diabetes_schema is None:
            self.diabetes_schema = [
                'Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness',
                'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome'
            ]

class TestResult:
    """Test result wrapper"""
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.status = "PENDING"
        self.start_time = datetime.now()
        self.end_time = None
        self.duration = None
        self.details = {}
        self.errors = []
        
    def success(self, details: Dict[str, Any] = None):
        """Mark test as successful"""
        self.status = "PASSED"
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        if details:
            self.details.update(details)
            
    def failure(self, error: str, details: Dict[str, Any] = None):
        """Mark test as failed"""
        self.status = "FAILED"
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.errors.append(error)
        if details:
            self.details.update(details)
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "test_name": self.test_name,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "details": self.details,
            "errors": self.errors
        }

class ServiceHealthChecker:
    """Utility class for checking service health"""
    
    def __init__(self, config: TestConfig):
        self.config = config
        
    def check_service(self, service_name: str, url: str, 
                     auth: Optional[tuple] = None, 
                     expected_status: int = 200) -> TestResult:
        """Check if a service is healthy"""
        result = TestResult(f"service_health_{service_name.lower().replace(' ', '_')}")
        
        try:
            response = requests.get(
                url, 
                auth=auth, 
                timeout=self.config.service_timeout
            )
            
            if response.status_code == expected_status:
                result.success({
                    "url": url,
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds()
                })
            else:
                result.failure(
                    f"Unexpected status code: {response.status_code}",
                    {"url": url, "status_code": response.status_code}
                )
                
        except requests.exceptions.RequestException as e:
            result.failure(f"Connection error: {str(e)}", {"url": url})
            
        return result
        
    def check_all_services(self) -> List[TestResult]:
        """Check all configured services"""
        services = [
            ("HDFS NameNode", self.config.hdfs_namenode_url),
            ("Spark Master", self.config.spark_master_url),
            ("Airflow", self.config.airflow_url, (self.config.airflow_username, self.config.airflow_password)),
            ("Grafana", self.config.grafana_url),
            ("NiFi", self.config.nifi_url),
            ("Prometheus", self.config.prometheus_url)
        ]
        
        results = []
        for service_data in services:
            if len(service_data) == 3:
                name, url, auth = service_data
                result = self.check_service(name, url, auth)
            else:
                name, url = service_data
                result = self.check_service(name, url)
            results.append(result)
            
        return results

class TestReporter:
    """Test reporting utility"""
    
    def __init__(self, output_dir: str = "./test_reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def generate_report(self, test_results: List[TestResult], 
                       suite_name: str = "comprehensive_test_suite") -> str:
        """Generate a comprehensive test report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Calculate summary statistics
        total_tests = len(test_results)
        passed_tests = len([r for r in test_results if r.status == "PASSED"])
        failed_tests = len([r for r in test_results if r.status == "FAILED"])
        pending_tests = len([r for r in test_results if r.status == "PENDING"])
        
        total_duration = sum([r.duration for r in test_results if r.duration])
        
        # Create detailed report
        report = {
            "test_suite": suite_name,
            "timestamp": timestamp,
            "summary": {
                "total_tests": total_tests,
                "passed": passed_tests,
                "failed": failed_tests,
                "pending": pending_tests,
                "success_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                "total_duration": total_duration
            },
            "test_results": [result.to_dict() for result in test_results]
        }
        
        # Save JSON report
        report_file = os.path.join(self.output_dir, f"{suite_name}_{timestamp}.json")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        # Generate HTML report
        html_report = self._generate_html_report(report)
        html_file = os.path.join(self.output_dir, f"{suite_name}_{timestamp}.html")
        with open(html_file, 'w') as f:
            f.write(html_report)
            
        return report_file
        
    def _generate_html_report(self, report: Dict[str, Any]) -> str:
        """Generate HTML test report"""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Test Report - {report['test_suite']}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .metric {{ background-color: #e9ecef; padding: 10px; border-radius: 5px; text-align: center; }}
        .passed {{ background-color: #d4edda; }}
        .failed {{ background-color: #f8d7da; }}
        .pending {{ background-color: #fff3cd; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .status-PASSED {{ color: green; font-weight: bold; }}
        .status-FAILED {{ color: red; font-weight: bold; }}
        .status-PENDING {{ color: orange; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Test Report: {report['test_suite']}</h1>
        <p>Generated: {report['timestamp']}</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>Total Tests</h3>
            <p>{report['summary']['total_tests']}</p>
        </div>
        <div class="metric passed">
            <h3>Passed</h3>
            <p>{report['summary']['passed']}</p>
        </div>
        <div class="metric failed">
            <h3>Failed</h3>
            <p>{report['summary']['failed']}</p>
        </div>
        <div class="metric pending">
            <h3>Pending</h3>
            <p>{report['summary']['pending']}</p>
        </div>
        <div class="metric">
            <h3>Success Rate</h3>
            <p>{report['summary']['success_rate']:.1f}%</p>
        </div>
        <div class="metric">
            <h3>Total Duration</h3>
            <p>{report['summary']['total_duration']:.2f}s</p>
        </div>
    </div>
    
    <h2>Test Results</h2>
    <table>
        <tr>
            <th>Test Name</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Details</th>
            <th>Errors</th>
        </tr>
"""
        
        for test in report['test_results']:
            html += f"""
        <tr>
            <td>{test['test_name']}</td>
            <td class="status-{test['status']}">{test['status']}</td>
            <td>{test['duration']:.2f}s</td>
            <td>{json.dumps(test['details'], indent=2) if test['details'] else ''}</td>
            <td>{'; '.join(test['errors']) if test['errors'] else ''}</td>
        </tr>
"""
        
        html += """
    </table>
</body>
</html>
"""
        return html

def wait_for_service(url: str, timeout: int = 300, auth: Optional[tuple] = None) -> bool:
    """Wait for a service to become available"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, auth=auth, timeout=10)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
            
        time.sleep(5)
        
    return False

def load_env_variables() -> Dict[str, str]:
    """Load environment variables from .env file"""
    env_vars = {}
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
                    
    return env_vars

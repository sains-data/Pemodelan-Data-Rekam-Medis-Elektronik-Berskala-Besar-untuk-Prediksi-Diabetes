#!/usr/bin/env python3
"""
Service Health Tests
Tests for checking the health and availability of all pipeline services
"""

import unittest
import requests
import json
from typing import List, Dict, Any

from test_config import TestConfig, TestResult, ServiceHealthChecker, TestReporter


class TestServiceHealth(unittest.TestCase):
    """Test suite for service health checks"""
    
    def setUp(self):
        """Set up test configuration"""
        self.config = TestConfig()
        self.health_checker = ServiceHealthChecker(self.config)
        self.results = []
        
    def test_hdfs_namenode_health(self):
        """Test HDFS NameNode health"""
        result = self.health_checker.check_service(
            "HDFS NameNode", 
            self.config.hdfs_namenode_url
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            # Additional HDFS checks
            try:
                # Check HDFS web UI endpoints
                response = requests.get(
                    f"{self.config.hdfs_namenode_url}/jmx",
                    timeout=self.config.service_timeout
                )
                self.assertEqual(response.status_code, 200)
                
                # Check if HDFS is not in safe mode
                jmx_data = response.json()
                beans = jmx_data.get('beans', [])
                namenode_info = next((bean for bean in beans if bean.get('name') == 'Hadoop:service=NameNode,name=NameNodeInfo'), None)
                
                if namenode_info:
                    safe_mode = namenode_info.get('Safemode', '')
                    self.assertEqual(safe_mode, '', "HDFS should not be in safe mode")
                    result.details['safe_mode'] = safe_mode
                    result.details['total_files'] = namenode_info.get('TotalFiles', 0)
                    result.details['total_blocks'] = namenode_info.get('TotalBlocks', 0)
                    
            except Exception as e:
                result.failure(f"Additional HDFS checks failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"HDFS NameNode health check failed: {result.errors}")
        
    def test_spark_master_health(self):
        """Test Spark Master health"""
        result = self.health_checker.check_service(
            "Spark Master",
            self.config.spark_master_url
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            try:
                # Check Spark Master JSON API
                response = requests.get(
                    f"{self.config.spark_master_url}/json/",
                    timeout=self.config.service_timeout
                )
                self.assertEqual(response.status_code, 200)
                
                spark_info = response.json()
                result.details.update({
                    'status': spark_info.get('status', 'Unknown'),
                    'workers': len(spark_info.get('workers', [])),
                    'cores': spark_info.get('cores', 0),
                    'memory': spark_info.get('memory', '0 MB'),
                    'active_apps': len(spark_info.get('activeapps', [])),
                    'completed_apps': len(spark_info.get('completedapps', []))
                })
                
            except Exception as e:
                result.failure(f"Spark Master API check failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"Spark Master health check failed: {result.errors}")
        
    def test_airflow_health(self):
        """Test Airflow health"""
        result = self.health_checker.check_service(
            "Airflow",
            self.config.airflow_url,
            (self.config.airflow_username, self.config.airflow_password)
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            try:
                # Check Airflow API health
                response = requests.get(
                    f"{self.config.airflow_url}/health",
                    auth=(self.config.airflow_username, self.config.airflow_password),
                    timeout=self.config.service_timeout
                )
                
                if response.status_code == 200:
                    health_data = response.json()
                    result.details['health_status'] = health_data
                
                # Check DAGs
                response = requests.get(
                    f"{self.config.airflow_url}/api/v1/dags",
                    auth=(self.config.airflow_username, self.config.airflow_password),
                    timeout=self.config.service_timeout
                )
                
                if response.status_code == 200:
                    dags_data = response.json()
                    total_dags = len(dags_data.get('dags', []))
                    diabetes_dag_found = any(
                        dag['dag_id'] in ['diabetes_prediction_etl_pipeline', 'diabetes_etl_pipeline']
                        for dag in dags_data.get('dags', [])
                    )
                    
                    result.details.update({
                        'total_dags': total_dags,
                        'diabetes_dag_found': diabetes_dag_found
                    })
                    
            except Exception as e:
                result.failure(f"Airflow API checks failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"Airflow health check failed: {result.errors}")
        
    def test_prometheus_health(self):
        """Test Prometheus health"""
        result = self.health_checker.check_service(
            "Prometheus",
            self.config.prometheus_url
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            try:
                # Check Prometheus targets
                response = requests.get(
                    f"{self.config.prometheus_url}/api/v1/targets",
                    timeout=self.config.service_timeout
                )
                self.assertEqual(response.status_code, 200)
                
                targets_data = response.json()
                active_targets = targets_data.get('data', {}).get('activeTargets', [])
                
                result.details.update({
                    'active_targets': len(active_targets),
                    'targets_health': [
                        {
                            'job': target.get('labels', {}).get('job', 'unknown'),
                            'health': target.get('health', 'unknown')
                        }
                        for target in active_targets
                    ]
                })
                
                # Check if metrics are being collected
                response = requests.get(
                    f"{self.config.prometheus_url}/api/v1/query?query=up",
                    timeout=self.config.service_timeout
                )
                
                if response.status_code == 200:
                    metrics_data = response.json()
                    metrics_count = len(metrics_data.get('data', {}).get('result', []))
                    result.details['available_metrics'] = metrics_count
                    
            except Exception as e:
                result.failure(f"Prometheus API checks failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"Prometheus health check failed: {result.errors}")
        
    def test_grafana_health(self):
        """Test Grafana health"""
        result = self.health_checker.check_service(
            "Grafana",
            self.config.grafana_url
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            try:
                # Check Grafana API health
                response = requests.get(
                    f"{self.config.grafana_url}/api/health",
                    timeout=self.config.service_timeout
                )
                
                if response.status_code == 200:
                    health_data = response.json()
                    result.details['grafana_health'] = health_data
                
                # Check dashboards (if accessible without auth)
                response = requests.get(
                    f"{self.config.grafana_url}/api/search",
                    timeout=self.config.service_timeout
                )
                
                if response.status_code in [200, 401]:  # 401 is expected without auth
                    if response.status_code == 200:
                        dashboards = response.json()
                        result.details['dashboards_count'] = len(dashboards)
                    else:
                        result.details['auth_required'] = True
                        
            except Exception as e:
                result.failure(f"Grafana API checks failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"Grafana health check failed: {result.errors}")
        
    def test_nifi_health(self):
        """Test NiFi health"""
        result = self.health_checker.check_service(
            "NiFi",
            self.config.nifi_url
        )
        self.results.append(result)
        
        if result.status == "PASSED":
            try:
                # Check NiFi API
                response = requests.get(
                    f"{self.config.nifi_url}/nifi-api/system-diagnostics",
                    timeout=self.config.service_timeout
                )
                
                if response.status_code == 200:
                    diagnostics = response.json()
                    system_diagnostics = diagnostics.get('systemDiagnostics', {})
                    
                    result.details.update({
                        'available_processors': system_diagnostics.get('availableProcessors', 0),
                        'processor_load_average': system_diagnostics.get('processorLoadAverage', 0),
                        'total_non_heap': system_diagnostics.get('totalNonHeap', ''),
                        'used_non_heap': system_diagnostics.get('usedNonHeap', '')
                    })
                    
            except Exception as e:
                result.failure(f"NiFi API checks failed: {str(e)}")
        
        self.assertEqual(result.status, "PASSED", f"NiFi health check failed: {result.errors}")
        
    def tearDown(self):
        """Generate test report"""
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "service_health_tests")
            print(f"\n📊 Service health test report generated: {report_file}")


class TestServiceConnectivity(unittest.TestCase):
    """Test inter-service connectivity"""
    
    def setUp(self):
        """Set up test configuration"""
        self.config = TestConfig()
        self.results = []
        
    def test_spark_to_hdfs_connectivity(self):
        """Test Spark to HDFS connectivity"""
        result = TestResult("spark_hdfs_connectivity")
        
        try:
            # Check if Spark can connect to HDFS
            response = requests.get(
                f"{self.config.spark_master_url}/json/",
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                spark_info = response.json()
                
                # Check if any applications are using HDFS
                active_apps = spark_info.get('activeapps', [])
                completed_apps = spark_info.get('completedapps', [])
                
                result.details.update({
                    'spark_status': spark_info.get('status', 'Unknown'),
                    'active_applications': len(active_apps),
                    'completed_applications': len(completed_apps)
                })
                
                result.success()
            else:
                result.failure(f"Cannot access Spark Master: {response.status_code}")
                
        except Exception as e:
            result.failure(f"Spark-HDFS connectivity test failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Spark-HDFS connectivity failed: {result.errors}")
        
    def test_airflow_to_spark_connectivity(self):
        """Test Airflow to Spark connectivity"""
        result = TestResult("airflow_spark_connectivity")
        
        try:
            # Check if Airflow can see Spark connection
            response = requests.get(
                f"{self.config.airflow_url}/api/v1/connections",
                auth=(self.config.airflow_username, self.config.airflow_password),
                timeout=self.config.service_timeout
            )
            
            if response.status_code == 200:
                connections = response.json()
                spark_connections = [
                    conn for conn in connections.get('connections', [])
                    if 'spark' in conn.get('conn_id', '').lower()
                ]
                
                result.details.update({
                    'total_connections': len(connections.get('connections', [])),
                    'spark_connections': len(spark_connections),
                    'spark_connection_details': spark_connections
                })
                
                result.success()
            else:
                result.failure(f"Cannot access Airflow connections: {response.status_code}")
                
        except Exception as e:
            result.failure(f"Airflow-Spark connectivity test failed: {str(e)}")
            
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Airflow-Spark connectivity failed: {result.errors}")
        
    def tearDown(self):
        """Generate test report"""
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "service_connectivity_tests")
            print(f"\n📊 Service connectivity test report generated: {report_file}")


if __name__ == '__main__':
    # Run service health tests
    print("🏥 Running Service Health Tests")
    print("=" * 50)
    
    health_suite = unittest.TestLoader().loadTestsFromTestCase(TestServiceHealth)
    connectivity_suite = unittest.TestLoader().loadTestsFromTestCase(TestServiceConnectivity)
    
    # Combine test suites
    combined_suite = unittest.TestSuite([health_suite, connectivity_suite])
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(combined_suite)
    
    # Print summary
    print(f"\n📋 Test Summary")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

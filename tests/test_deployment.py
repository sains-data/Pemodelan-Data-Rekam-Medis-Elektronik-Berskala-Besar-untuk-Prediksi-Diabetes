#!/usr/bin/env python3
"""
Automated Deployment Validation Tests
Check if all containers are up and healthy via Docker Compose
"""

import unittest
import subprocess
import json
from test_config import TestResult, TestReporter

class TestDeployment(unittest.TestCase):
    def setUp(self):
        self.results = []

    def test_docker_compose_services(self):
        result = TestResult("docker_compose_services")
        try:
            cmd = ["docker", "compose", "ps", "--format", "json"]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            services = json.loads(proc.stdout)
            unhealthy = [s for s in services if s.get('State') != 'running']
            result.details['services'] = services
            if not unhealthy:
                result.success()
            else:
                result.failure(f"Unhealthy containers: {[s.get('Name') for s in unhealthy]}")
        except Exception as e:
            result.failure(f"Docker Compose check failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Docker Compose services failed: {result.errors}")

    def tearDown(self):
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "deployment_tests")
            print(f"\n🚀 Deployment test report generated: {report_file}")

if __name__ == '__main__':
    print("🚀 Running Deployment Validation Tests")
    print("=" * 50)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDeployment)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\n📋 Test Summary")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

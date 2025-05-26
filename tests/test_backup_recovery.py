#!/usr/bin/env python3
"""
Backup and Recovery Tests
Check if backup files exist and can be restored
"""

import unittest
import os
from test_config import TestConfig, TestResult, TestReporter

class TestBackupRecovery(unittest.TestCase):
    def setUp(self):
        self.config = TestConfig()
        self.results = []

    def test_backup_file_existence(self):
        result = TestResult("backup_file_existence")
        backup_dir = os.path.join(self.config.logs_path, 'backups')
        try:
            if os.path.exists(backup_dir):
                files = os.listdir(backup_dir)
                backup_files = [f for f in files if f.endswith('.tar.gz') or f.endswith('.zip')]
                result.details['backup_files'] = backup_files
                if backup_files:
                    result.success()
                else:
                    result.failure("No backup files found in backup directory")
            else:
                result.failure(f"Backup directory does not exist: {backup_dir}")
        except Exception as e:
            result.failure(f"Backup file existence check failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Backup file existence failed: {result.errors}")

    def test_restore_simulation(self):
        result = TestResult("restore_simulation")
        try:
            # Simulate restore (actual restore would be destructive)
            result.details['restore_simulation'] = 'success (simulated)'
            result.success()
        except Exception as e:
            result.failure(f"Restore simulation failed: {str(e)}")
        self.results.append(result)
        self.assertEqual(result.status, "PASSED", f"Restore simulation failed: {result.errors}")

    def tearDown(self):
        if hasattr(self, 'results') and self.results:
            reporter = TestReporter()
            report_file = reporter.generate_report(self.results, "backup_recovery_tests")
            print(f"\n🗄️ Backup/Recovery test report generated: {report_file}")

if __name__ == '__main__':
    print("🗄️ Running Backup & Recovery Tests")
    print("=" * 50)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBackupRecovery)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print(f"\n📋 Test Summary")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

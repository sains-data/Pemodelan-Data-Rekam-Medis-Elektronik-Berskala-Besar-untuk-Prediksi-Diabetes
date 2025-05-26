#!/usr/bin/env python3
"""
pytest configuration and test discovery
Configures pytest for the diabetes prediction pipeline test suite
"""

import pytest
import os
import sys
from datetime import datetime

# Add the tests directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def pytest_configure(config):
    """Configure pytest with custom settings"""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_services: marks tests that require running services"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically"""
    for item in items:
        # Add markers based on test file names
        if "unit" in item.nodeid:
            item.add_marker(pytest.mark.unit)
        elif "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        elif "e2e" in item.nodeid or "end_to_end" in item.nodeid:
            item.add_marker(pytest.mark.e2e)
        
        # Add slow marker for tests that might take longer
        if any(keyword in item.nodeid.lower() for keyword in ["performance", "load", "stress"]):
            item.add_marker(pytest.mark.slow)
        
        # Add requires_services marker for tests that need running services
        if any(keyword in item.nodeid.lower() for keyword in ["service", "health", "deployment"]):
            item.add_marker(pytest.mark.requires_services)

@pytest.fixture(scope="session")
def test_config():
    """Provide test configuration for all tests"""
    from test_config import TestConfig
    return TestConfig()

@pytest.fixture(scope="session") 
def test_data():
    """Provide test data fixtures for all tests"""
    from test_fixtures import get_test_diabetes_data, get_test_personal_health_data
    return {
        'diabetes': get_test_diabetes_data(),
        'personal_health': get_test_personal_health_data()
    }

@pytest.fixture(scope="function")
def temp_files():
    """Provide temporary test files"""
    from test_fixtures import create_temp_test_files, cleanup_test_files
    temp_files = create_temp_test_files()
    yield temp_files
    cleanup_test_files(temp_files)

@pytest.fixture(scope="session")
def mock_responses():
    """Provide mock service responses"""
    from test_fixtures import get_mock_responses
    return get_mock_responses()

def pytest_html_report_title(report):
    """Customize HTML report title"""
    report.title = "Diabetes Prediction Pipeline - Test Report"

def pytest_html_results_summary(prefix, summary, postfix):
    """Customize HTML report summary"""
    prefix.extend([
        "<h2>🩺 Diabetes Prediction Pipeline Test Results</h2>",
        f"<p><strong>Test Run Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
        "<p><strong>Test Environment:</strong> CI/CD Pipeline</p>"
    ])

def pytest_runtest_setup(item):
    """Setup for each test"""
    # Skip integration tests if services are not available
    if "requires_services" in [marker.name for marker in item.iter_markers()]:
        # Check if we're in CI environment without services
        if os.getenv('CI') and not os.getenv('SERVICES_RUNNING'):
            pytest.skip("Skipping service-dependent test in CI without services")

def pytest_sessionstart(session):
    """Called after the Session object has been created"""
    print(f"\n🚀 Starting Diabetes Prediction Pipeline Test Suite")
    print(f"📅 Test Session: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🐍 Python Version: {sys.version}")
    print(f"📁 Working Directory: {os.getcwd()}")

def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished"""
    print(f"\n✅ Test Session Completed with exit status: {exitstatus}")
    if exitstatus == 0:
        print("🎉 All tests passed successfully!")
    else:
        print("⚠️  Some tests failed or encountered errors")
    print(f"📊 Test reports available in: {os.path.join(os.getcwd(), 'test_reports')}")

# Pytest configuration options
pytest_plugins = [
    "pytest_html",
    "pytest_cov"
]

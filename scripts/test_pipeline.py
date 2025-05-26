#!/usr/bin/env python3
"""
Data Quality and Pipeline Test Script
Tests the entire diabetes prediction pipeline from end to end
"""

import sys
import os
import requests
import json
import time
from datetime import datetime
import pandas as pd

def test_service_health():
    """Test if all services are healthy"""
    services = {
        'HDFS NameNode': 'http://localhost:9870',
        'Spark Master': 'http://localhost:8081',
        'Airflow': 'http://localhost:8089',
        'Grafana': 'http://localhost:3000',
        'NiFi': 'http://localhost:8080/nifi',
        'Prometheus': 'http://localhost:9090'
    }
    
    print("🔍 Testing service health...")
    
    for service_name, url in services.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"✅ {service_name}: Healthy")
            else:
                print(f"⚠️  {service_name}: Status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ {service_name}: Not accessible ({str(e)})")
    
    print()

def test_data_quality():
    """Test data quality of input datasets"""
    print("🧪 Testing data quality...")
    
    # Test diabetes.csv
    try:
        df_diabetes = pd.read_csv('/opt/airflow/data/diabetes.csv')
        print(f"✅ Diabetes dataset loaded: {len(df_diabetes)} rows, {len(df_diabetes.columns)} columns")
        
        # Check for missing values
        missing_values = df_diabetes.isnull().sum()
        if missing_values.sum() > 0:
            print(f"⚠️  Missing values found: {missing_values.sum()} total")
        else:
            print("✅ No missing values in diabetes dataset")
        
        # Check data types
        expected_columns = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 
                          'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome']
        
        if all(col in df_diabetes.columns for col in expected_columns):
            print("✅ All expected columns present in diabetes dataset")
        else:
            print("❌ Missing expected columns in diabetes dataset")
        
    except Exception as e:
        print(f"❌ Error loading diabetes dataset: {str(e)}")
    
    # Test personal health data
    try:
        df_health = pd.read_csv('/opt/airflow/data/personal_health_data.csv')
        print(f"✅ Personal health dataset loaded: {len(df_health)} rows, {len(df_health.columns)} columns")
        
    except Exception as e:
        print(f"❌ Error loading personal health dataset: {str(e)}")
    
    print()

def test_airflow_connection():
    """Test Airflow connection and DAG status"""
    print("🌊 Testing Airflow connection...")
    
    try:
        # Test API connection
        response = requests.get('http://localhost:8089/api/v1/dags', 
                              auth=('admin', 'admin'), timeout=10)
        
        if response.status_code == 200:
            dags = response.json()
            print(f"✅ Airflow API accessible, found {len(dags.get('dags', []))} DAGs")
            
            # Check for our specific DAG
            diabetes_dag_found = False
            for dag in dags.get('dags', []):
                if dag['dag_id'] == 'diabetes_etl_pipeline':
                    diabetes_dag_found = True
                    print(f"✅ Diabetes ETL DAG found: {dag['dag_id']}")
                    print(f"   Is Paused: {dag['is_paused']}")
                    break
            
            if not diabetes_dag_found:
                print("⚠️  Diabetes ETL DAG not found")
        else:
            print(f"❌ Airflow API not accessible: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error connecting to Airflow: {str(e)}")
    
    print()

def test_spark_connection():
    """Test Spark Master connection"""
    print("⚡ Testing Spark connection...")
    
    try:
        response = requests.get('http://localhost:8081/json/', timeout=10)
        
        if response.status_code == 200:
            spark_info = response.json()
            print(f"✅ Spark Master accessible")
            print(f"   Status: {spark_info.get('status', 'Unknown')}")
            print(f"   Workers: {len(spark_info.get('workers', []))}")
            print(f"   Cores: {spark_info.get('cores', 'Unknown')}")
            print(f"   Memory: {spark_info.get('memory', 'Unknown')}")
        else:
            print(f"❌ Spark Master not accessible: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error connecting to Spark: {str(e)}")
    
    print()

def test_prometheus_metrics():
    """Test Prometheus metrics availability"""
    print("📊 Testing Prometheus metrics...")
    
    try:
        response = requests.get('http://localhost:9090/api/v1/query?query=up', timeout=10)
        
        if response.status_code == 200:
            metrics = response.json()
            print(f"✅ Prometheus API accessible")
            
            # Check if we have metrics
            if metrics.get('data', {}).get('result'):
                active_targets = len(metrics['data']['result'])
                print(f"   Active targets: {active_targets}")
            else:
                print("⚠️  No metrics found")
        else:
            print(f"❌ Prometheus not accessible: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error connecting to Prometheus: {str(e)}")
    
    print()

def run_end_to_end_test():
    """Run a complete end-to-end test"""
    print("🚀 Running End-to-End Pipeline Test")
    print("=" * 50)
    
    # Record start time
    start_time = datetime.now()
    print(f"Test started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Run all tests
    test_service_health()
    test_data_quality()
    test_airflow_connection()
    test_spark_connection()
    test_prometheus_metrics()
    
    # Calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("📋 Test Summary")
    print("=" * 50)
    print(f"Test completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {duration.total_seconds():.2f} seconds")
    print()
    print("🎯 Next Steps:")
    print("1. Run 'make ingest-data' to trigger the ETL pipeline")
    print("2. Monitor progress in Airflow UI: http://localhost:8089")
    print("3. View results in Grafana: http://localhost:3000")
    print("4. Check Spark jobs: http://localhost:8081")

if __name__ == "__main__":
    run_end_to_end_test()

#!/usr/bin/env python3
"""
Environment Configuration Validation Script
Validates that all required environment variables are properly set
"""

import os
import sys
from typing import List, Dict

def load_env_file(file_path: str) -> Dict[str, str]:
    """Load environment variables from .env file"""
    env_vars = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return {}
    return env_vars

def validate_required_variables() -> bool:
    """Validate that all required environment variables are set"""
    
    # Load environment variables from .env file
    env_vars = load_env_file('.env')
    
    # Required variables grouped by service
    required_vars = {
        "Project": [
            "PROJECT_NAME", "PROJECT_VERSION", "ENVIRONMENT", "COMPOSE_PROJECT_NAME"
        ],
        "Network": [
            "NETWORK_NAME", "NETWORK_DRIVER"
        ],
        "Hadoop/HDFS": [
            "HADOOP_VERSION", "HDFS_NAMENODE_HOST", "HDFS_NAMENODE_PORT", 
            "HDFS_NAMENODE_HTTP_PORT", "HDFS_REPLICATION_FACTOR", "CLUSTER_NAME"
        ],
        "Spark": [
            "SPARK_VERSION", "SPARK_MASTER_HOST", "SPARK_MASTER_PORT",
            "SPARK_WORKER_CORES", "SPARK_WORKER_MEMORY"
        ],
        "Hive": [
            "HIVE_VERSION", "HIVE_METASTORE_PORT", "HIVE_POSTGRES_USER",
            "HIVE_POSTGRES_PASSWORD", "HIVE_POSTGRES_DB"
        ],
        "Airflow": [
            "AIRFLOW_VERSION", "AIRFLOW_WEBSERVER_PORT", "AIRFLOW_POSTGRES_USER",
            "AIRFLOW_POSTGRES_PASSWORD", "AIRFLOW_POSTGRES_DB"
        ],
        "NiFi": [
            "NIFI_VERSION", "NIFI_WEB_HTTP_PORT", "SINGLE_USER_CREDENTIALS_USERNAME",
            "SINGLE_USER_CREDENTIALS_PASSWORD"
        ],
        "Monitoring": [
            "PROMETHEUS_VERSION", "PROMETHEUS_PORT", "GRAFANA_VERSION", "GRAFANA_PORT",
            "GF_SECURITY_ADMIN_USER", "GF_SECURITY_ADMIN_PASSWORD"
        ],
        "ML Pipeline": [
            "ML_MODEL_VERSION", "ML_ACCURACY_THRESHOLD", "ML_PRECISION_THRESHOLD"
        ]
    }
    
    all_valid = True
    print("🔍 Environment Configuration Validation")
    print("=" * 50)
    
    for category, variables in required_vars.items():
        print(f"\n📋 {category} Configuration:")
        category_valid = True
        
        for var in variables:
            if var in env_vars and env_vars[var]:
                print(f"  ✅ {var}: {env_vars[var]}")
            else:
                print(f"  ❌ {var}: NOT SET or EMPTY")
                category_valid = False
                all_valid = False
        
        if category_valid:
            print(f"  ✅ {category} configuration is valid")
        else:
            print(f"  ❌ {category} configuration has missing variables")
    
    return all_valid

def validate_service_connectivity() -> bool:
    """Validate service connectivity configuration"""
    print("\n🔗 Service Connectivity Validation")
    print("=" * 50)
    
    env_vars = load_env_file('.env')
    
    # Check port conflicts
    used_ports = {}
    port_vars = [
        ("HDFS NameNode HTTP", "HDFS_NAMENODE_HTTP_PORT"),
        ("HDFS NameNode", "HDFS_NAMENODE_PORT"),
        ("Spark Master Web UI", "SPARK_MASTER_WEBUI_PORT"),
        ("Spark Master", "SPARK_MASTER_PORT"),
        ("Spark Worker Web UI", "SPARK_WORKER_WEBUI_PORT"),
        ("Hive Metastore", "HIVE_METASTORE_PORT"),
        ("Airflow Webserver", "AIRFLOW_WEBSERVER_PORT"),
        ("NiFi", "NIFI_WEB_HTTP_PORT"),
        ("Prometheus", "PROMETHEUS_PORT"),
        ("Grafana", "GRAFANA_PORT"),
        ("Node Exporter", "NODE_EXPORTER_PORT")
    ]
    
    port_conflicts = False
    for service_name, port_var in port_vars:
        if port_var in env_vars:
            port = env_vars[port_var]
            if port in used_ports:
                print(f"  ❌ Port conflict: {port} used by both {used_ports[port]} and {service_name}")
                port_conflicts = True
            else:
                used_ports[port] = service_name
                print(f"  ✅ {service_name}: Port {port}")
    
    if not port_conflicts:
        print("  ✅ No port conflicts detected")
    
    return not port_conflicts

def main():
    """Main validation function"""
    print("🚀 Diabetes Prediction Pipeline - Environment Validation")
    print("=" * 60)
    
    # Change to project directory if script is run from scripts folder
    if os.path.basename(os.getcwd()) == 'scripts':
        os.chdir('..')
    
    env_valid = validate_required_variables()
    connectivity_valid = validate_service_connectivity()
    
    print("\n📊 Validation Summary")
    print("=" * 50)
    
    if env_valid and connectivity_valid:
        print("✅ All environment configurations are valid!")
        print("🎉 Ready to deploy the pipeline!")
        return 0
    else:
        print("❌ Environment configuration issues detected")
        print("🔧 Please fix the issues above before deployment")
        return 1

if __name__ == "__main__":
    sys.exit(main())

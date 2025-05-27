#!/usr/bin/env python3
"""
Enhanced Dashboard Upload Script
Uploads multiple dashboards to Grafana
"""

import requests
import json
import sys
import os
from pathlib import Path

class GrafanaDashboardManager:
    def __init__(self, grafana_url="http://localhost:3000", username="admin", password="grafana_admin_2025"):
        self.grafana_url = grafana_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = (username, password)
        
    def test_connection(self):
        """Test connection to Grafana"""
        try:
            response = self.session.get(f"{self.grafana_url}/api/health")
            if response.status_code == 200:
                print("✅ Successfully connected to Grafana")
                return True
            else:
                print(f"❌ Failed to connect to Grafana: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Error connecting to Grafana: {e}")
            return False
    
    def upload_dashboard(self, dashboard_json, title="Dashboard"):
        """Upload dashboard to Grafana"""
        try:
            # Prepare dashboard for upload
            dashboard_data = {
                "dashboard": dashboard_json,
                "overwrite": True,
                "message": f"Uploaded {title} via API"
            }
            
            response = self.session.post(f"{self.grafana_url}/api/dashboards/db", 
                                       json=dashboard_data)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ {title} uploaded successfully!")
                print(f"   Dashboard URL: {self.grafana_url}/d/{result['uid']}")
                return True, result['uid']
            else:
                print(f"❌ Failed to upload {title}: {response.status_code}")
                print(f"   Response: {response.text}")
                return False, None
                
        except Exception as e:
            print(f"❌ Error uploading {title}: {e}")
            return False, None

    def create_datasources(self):
        """Create all required datasources"""
        print("\n📊 Creating PostgreSQL datasources...")
        
        datasources = [
            {
                "name": "PostgreSQL Warehouse",
                "type": "postgres",
                "access": "proxy",
                "url": "postgres-warehouse:5432",
                "database": "diabetes_warehouse",
                "user": "warehouse",
                "secureJsonData": {
                    "password": "warehouse_secure_2025"
                },
                "isDefault": True,
                "jsonData": {
                    "sslmode": "disable",
                    "maxOpenConns": 10,
                    "maxIdleConns": 2,
                    "connMaxLifetime": 14400,
                    "postgresVersion": 1300,
                    "timescaledb": False
                }
            },
            {
                "name": "PostgreSQL Bronze Layer",
                "type": "postgres",
                "access": "proxy", 
                "url": "postgres-warehouse:5432",
                "database": "diabetes_warehouse",
                "user": "warehouse",
                "secureJsonData": {
                    "password": "warehouse_secure_2025"
                },
                "isDefault": False,
                "jsonData": {
                    "sslmode": "disable",
                    "maxOpenConns": 5,
                    "maxIdleConns": 1,
                    "connMaxLifetime": 14400,
                    "postgresVersion": 1300,
                    "timescaledb": False
                }
            },
            {
                "name": "PostgreSQL Silver Layer",
                "type": "postgres",
                "access": "proxy",
                "url": "postgres-warehouse:5432", 
                "database": "diabetes_warehouse",
                "user": "warehouse",
                "secureJsonData": {
                    "password": "warehouse_secure_2025"
                },
                "isDefault": False,
                "jsonData": {
                    "sslmode": "disable",
                    "maxOpenConns": 5,
                    "maxIdleConns": 1,
                    "connMaxLifetime": 14400,
                    "postgresVersion": 1300,
                    "timescaledb": False
                }
            },
            {
                "name": "PostgreSQL Gold Layer",
                "type": "postgres",
                "access": "proxy",
                "url": "postgres-warehouse:5432",
                "database": "diabetes_warehouse", 
                "user": "warehouse",
                "secureJsonData": {
                    "password": "warehouse_secure_2025"
                },
                "isDefault": False,
                "jsonData": {
                    "sslmode": "disable",
                    "maxOpenConns": 5,
                    "maxIdleConns": 1,
                    "connMaxLifetime": 14400,
                    "postgresVersion": 1300,
                    "timescaledb": False
                }
            }
        ]
        
        success_count = 0
        for datasource in datasources:
            if self.create_datasource(datasource):
                success_count += 1
                
        return success_count == len(datasources)
    
    def create_datasource(self, datasource_config):
        """Create or update datasource in Grafana"""
        try:
            # Check if datasource exists
            response = self.session.get(f"{self.grafana_url}/api/datasources/name/{datasource_config['name']}")
            
            if response.status_code == 200:
                # Update existing datasource
                datasource_id = response.json()['id']
                datasource_config['id'] = datasource_id
                response = self.session.put(f"{self.grafana_url}/api/datasources/{datasource_id}", 
                                          json=datasource_config)
                if response.status_code == 200:
                    print(f"✅ Updated datasource: {datasource_config['name']}")
                    return True
            else:
                # Create new datasource
                response = self.session.post(f"{self.grafana_url}/api/datasources", 
                                           json=datasource_config)
                if response.status_code == 200:
                    print(f"✅ Created datasource: {datasource_config['name']}")
                    return True
                    
            print(f"❌ Failed to create/update datasource {datasource_config['name']}: {response.text}")
            return False
            
        except Exception as e:
            print(f"❌ Error with datasource {datasource_config['name']}: {e}")
            return False

def main():
    # Initialize manager
    manager = GrafanaDashboardManager()
    
    # Test connection
    if not manager.test_connection():
        sys.exit(1)
    
    # Create datasources
    if not manager.create_datasources():
        print("❌ Failed to create some datasources")
        sys.exit(1)
    
    print("\n📈 Uploading dashboards...")
    
    # Define dashboard files to upload
    dashboard_files = [
        {
            "path": "/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/dashboard/grafana/dashboards/diabetes_postgresql_dashboard.json",
            "name": "Main Diabetes Analytics Dashboard"
        },
        {
            "path": "/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/dashboard/grafana/dashboards/deep_analysis_dashboard.json", 
            "name": "Deep Analysis Dashboard"
        },
        {
            "path": "/mnt/2A28ACA028AC6C8F/Programming/bigdata/pipline_prediksi_diabestes/dashboard/grafana/dashboards/diabetes_realtime_monitoring.json",
            "name": "Real-time Monitoring Dashboard"
        }
    ]
    
    uploaded_dashboards = []
    failed_uploads = []
    
    for dashboard_file in dashboard_files:
        try:
            if os.path.exists(dashboard_file["path"]):
                print(f"\n📊 Uploading {dashboard_file['name']}...")
                with open(dashboard_file["path"], 'r') as f:
                    dashboard_data = json.load(f)
                    
                # Extract dashboard from wrapper if present
                if "dashboard" in dashboard_data:
                    dashboard_json = dashboard_data["dashboard"]
                else:
                    dashboard_json = dashboard_data
                    
                success, uid = manager.upload_dashboard(dashboard_json, dashboard_file['name'])
                if success:
                    uploaded_dashboards.append({
                        'name': dashboard_file['name'],
                        'uid': uid,
                        'url': f"http://localhost:3000/d/{uid}"
                    })
                else:
                    failed_uploads.append(dashboard_file['name'])
            else:
                print(f"⚠️  Dashboard file not found: {dashboard_file['path']}")
                failed_uploads.append(dashboard_file['name'])
        except Exception as e:
            print(f"❌ Error uploading {dashboard_file['name']}: {e}")
            failed_uploads.append(dashboard_file['name'])
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"📊 DASHBOARD UPLOAD SUMMARY")
    print(f"{'='*60}")
    
    if uploaded_dashboards:
        print(f"\n✅ Successfully uploaded {len(uploaded_dashboards)} dashboard(s):")
        for dashboard in uploaded_dashboards:
            print(f"   • {dashboard['name']}")
            print(f"     URL: {dashboard['url']}")
        
        print(f"\n🌐 Grafana Access Information:")
        print(f"   URL: http://localhost:3000")
        print(f"   Username: admin")
        print(f"   Password: grafana_admin_2025")
        
    if failed_uploads:
        print(f"\n❌ Failed to upload {len(failed_uploads)} dashboard(s):")
        for name in failed_uploads:
            print(f"   • {name}")
    
    if uploaded_dashboards and not failed_uploads:
        print(f"\n🎉 All dashboards uploaded successfully!")
        return 0
    elif uploaded_dashboards:
        print(f"\n⚠️  Some dashboards uploaded with issues")
        return 1
    else:
        print(f"\n❌ No dashboards were uploaded successfully!")
        return 2

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

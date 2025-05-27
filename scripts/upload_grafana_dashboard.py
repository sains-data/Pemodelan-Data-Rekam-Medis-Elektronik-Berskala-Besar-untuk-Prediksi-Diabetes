#!/usr/bin/env python3
"""
Upload Grafana Dashboard Script
Uploads the diabetes prediction dashboard to Grafana via API
"""

import requests
import json
import sys
import os
from pathlib import Path

class GrafanaDashboardUploader:
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
    
    def upload_dashboard(self, dashboard_json):
        """Upload dashboard to Grafana"""
        try:
            # Prepare dashboard for upload
            dashboard_data = {
                "dashboard": dashboard_json,
                "overwrite": True,
                "message": "Uploaded via API"
            }
            
            response = self.session.post(f"{self.grafana_url}/api/dashboards/db", 
                                       json=dashboard_data)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Dashboard uploaded successfully!")
                print(f"   Dashboard URL: {self.grafana_url}/d/{result['uid']}")
                return True
            else:
                print(f"❌ Failed to upload dashboard: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error uploading dashboard: {e}")
            return False

def main():
    # Initialize uploader
    uploader = GrafanaDashboardUploader()
    
    # Test connection
    if not uploader.test_connection():
        sys.exit(1)
    
    print("\n📊 Creating PostgreSQL datasources...")
    
    # Define datasources
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
    
    # Create datasources
    for ds in datasources:
        uploader.create_datasource(ds)
    
    print("\n📈 Creating comprehensive diabetes prediction dashboard...")
    
    # Define comprehensive dashboard
    dashboard = {
        "id": None,
        "uid": "diabetes-prediction-comprehensive",
        "title": "🩺 Diabetes Prediction Analytics - Comprehensive Dashboard",
        "tags": ["diabetes", "postgresql", "warehouse", "health", "prediction"],
        "timezone": "browser",
        "schemaVersion": 16,
        "version": 1,
        "refresh": "30s",
        "time": {
            "from": "now-24h",
            "to": "now"
        },
        "panels": [
            # Row 1: Key Metrics
            {
                "id": 1,
                "title": "Total Patients",
                "type": "stat",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT COUNT(*) as value FROM gold.diabetes_gold;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "palette-classic"
                        },
                        "custom": {
                            "displayMode": "basic",
                            "orientation": "horizontal"
                        },
                        "unit": "short",
                        "decimals": 0
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 0,
                    "y": 0
                }
            },
            {
                "id": 2,
                "title": "Diabetic Cases",
                "type": "stat",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT COUNT(*) as value FROM gold.diabetes_gold WHERE diabetes = 1;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 100},
                                {"color": "red", "value": 200}
                            ]
                        },
                        "unit": "short",
                        "decimals": 0
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 4,
                    "y": 0
                }
            },
            {
                "id": 3,
                "title": "Diabetes Rate",
                "type": "stat",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT ROUND(AVG(diabetes::DECIMAL) * 100, 2) as value FROM gold.diabetes_gold;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 25},
                                {"color": "red", "value": 40}
                            ]
                        },
                        "unit": "percent",
                        "decimals": 1
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 8,
                    "y": 0
                }
            },
            {
                "id": 4,
                "title": "High Risk Patients",
                "type": "stat",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table", 
                        "rawSql": "SELECT COUNT(*) as value FROM gold.diabetes_gold WHERE health_risk_level = 'High';",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 50},
                                {"color": "red", "value": 100}
                            ]
                        },
                        "unit": "short",
                        "decimals": 0
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 12,
                    "y": 0
                }
            },
            {
                "id": 5,
                "title": "Average Glucose",
                "type": "stat", 
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT ROUND(AVG(glucose), 1) as value FROM gold.diabetes_gold;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 120},
                                {"color": "red", "value": 140}
                            ]
                        },
                        "unit": "short",
                        "decimals": 1
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 16,
                    "y": 0
                }
            },
            {
                "id": 6,
                "title": "Average BMI",
                "type": "stat",
                "datasource": "PostgreSQL Warehouse", 
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT ROUND(AVG(bmi), 1) as value FROM gold.diabetes_gold;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 25},
                                {"color": "red", "value": 30}
                            ]
                        },
                        "unit": "short",
                        "decimals": 1
                    }
                },
                "gridPos": {
                    "h": 4,
                    "w": 4,
                    "x": 20,
                    "y": 0
                }
            },
            # Row 2: Distribution Charts
            {
                "id": 7,
                "title": "Diabetes Distribution",
                "type": "piechart",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT CASE WHEN diabetes = 1 THEN 'Diabetic' ELSE 'Non-Diabetic' END as category, COUNT(*) as value FROM gold.diabetes_gold GROUP BY diabetes ORDER BY diabetes;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "palette-classic"
                        },
                        "custom": {
                            "displayLabels": ["name", "percent"]
                        }
                    }
                },
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 0,
                    "y": 4
                }
            },
            {
                "id": 8,
                "title": "BMI Category Distribution",
                "type": "barchart",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT bmi_category as category, COUNT(*) as value FROM gold.diabetes_gold GROUP BY bmi_category ORDER BY COUNT(*) DESC;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "palette-classic"
                        }
                    }
                },
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 8,
                    "y": 4
                }
            },
            {
                "id": 9,
                "title": "Age Group Analysis",
                "type": "barchart",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT age_group as category, COUNT(*) as total, SUM(CASE WHEN diabetes = 1 THEN 1 ELSE 0 END) as diabetic FROM gold.diabetes_gold GROUP BY age_group ORDER BY age_group;",
                        "refId": "A"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "palette-classic"
                        }
                    }
                },
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 16,
                    "y": 4
                }
            },
            # Row 3: Health Metrics Analysis
            {
                "id": 10,
                "title": "Risk Level Distribution",
                "type": "piechart",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT health_risk_level as category, COUNT(*) as value FROM gold.diabetes_gold GROUP BY health_risk_level ORDER BY health_risk_level;",
                        "refId": "A"
                    }
                ],
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 0,
                    "y": 12
                }
            },
            {
                "id": 11,
                "title": "Glucose vs BMI Scatter Plot",
                "type": "scatterplot",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT glucose, bmi, CASE WHEN diabetes = 1 THEN 'Diabetic' ELSE 'Non-Diabetic' END as series FROM gold.diabetes_gold LIMIT 500;",
                        "refId": "A"
                    }
                ],
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 8,
                    "y": 12
                }
            },
            {
                "id": 12,
                "title": "Data Quality Metrics",
                "type": "table",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT 'Gold Layer' as layer, COUNT(*) as total_records, ROUND(AVG(quality_score), 3) as avg_quality_score FROM gold.diabetes_gold UNION ALL SELECT 'Silver Layer' as layer, COUNT(*) as total_records, 1.0 as avg_quality_score FROM silver.diabetes_cleaned UNION ALL SELECT 'Bronze Layer' as layer, COUNT(*) as total_records, ROUND(AVG(data_quality_score), 3) as avg_quality_score FROM bronze.diabetes_raw;",
                        "refId": "A"
                    }
                ],
                "gridPos": {
                    "h": 8,
                    "w": 8,
                    "x": 16,
                    "y": 12
                }
            },
            # Row 4: Advanced Analytics
            {
                "id": 13,
                "title": "Prediction Distribution",
                "type": "histogram",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT prediction_probability as value FROM gold.diabetes_gold WHERE prediction_probability IS NOT NULL;",
                        "refId": "A"
                    }
                ],
                "gridPos": {
                    "h": 8,
                    "w": 12,
                    "x": 0,
                    "y": 20
                }
            },
            {
                "id": 14,
                "title": "Feature Importance",
                "type": "table",
                "datasource": "PostgreSQL Warehouse",
                "targets": [
                    {
                        "expr": "",
                        "format": "table",
                        "rawSql": "SELECT 'Glucose' as feature, ROUND(CORR(glucose, diabetes), 3) as correlation FROM gold.diabetes_gold UNION ALL SELECT 'BMI' as feature, ROUND(CORR(bmi, diabetes), 3) as correlation FROM gold.diabetes_gold UNION ALL SELECT 'Age' as feature, ROUND(CORR(age, diabetes), 3) as correlation FROM gold.diabetes_gold ORDER BY correlation DESC;",
                        "refId": "A"
                    }
                ],
                "gridPos": {
                    "h": 8,
                    "w": 12,
                    "x": 12,
                    "y": 20
                }
            }
        ]
    }
    
    # Upload dashboard
    if uploader.upload_dashboard(dashboard):
        print("\n🎉 Dashboard upload completed successfully!")
        print(f"   Access your dashboard at: http://localhost:3000")
        print(f"   Login: admin / admin")
    else:
        print("\n❌ Dashboard upload failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

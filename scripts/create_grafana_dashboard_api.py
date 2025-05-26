#!/usr/bin/env python3
# =============================================================================
# GRAFANA DASHBOARD API CREATION SCRIPT
# =============================================================================
# Script to create Grafana dashboard using REST API
# Created: May 27, 2025
# =============================================================================

import json
import requests
import time
import sys
import os
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings for development
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

class GrafanaDashboardCreator:
    def __init__(self, grafana_url="http://localhost:3000", username="admin", password="grafana_admin_2025"):
        self.grafana_url = grafana_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def check_grafana_health(self):
        """Check if Grafana is accessible"""
        try:
            response = self.session.get(f"{self.grafana_url}/api/health", timeout=10)
            if response.status_code == 200:
                print("✅ Grafana is accessible")
                return True
            else:
                print(f"❌ Grafana health check failed: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot connect to Grafana: {e}")
            return False

    def create_datasource_if_not_exists(self):
        """Create PostgreSQL datasource if it doesn't exist"""
        print("\n🔗 Setting up PostgreSQL datasource...")
        
        # Check if datasource exists
        try:
            response = self.session.get(f"{self.grafana_url}/api/datasources/name/PostgreSQL%20Warehouse")
            if response.status_code == 200:
                print("✅ PostgreSQL datasource already exists")
                return True
        except:
            pass

        # Create datasource
        datasource_config = {
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
                "postgresVersion": 1500,
                "timescaledb": False
            }
        }

        try:
            response = self.session.post(
                f"{self.grafana_url}/api/datasources",
                json=datasource_config
            )
            if response.status_code in [200, 201]:
                print("✅ PostgreSQL datasource created successfully")
                return True
            else:
                print(f"❌ Failed to create datasource: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error creating datasource: {e}")
            return False

    def create_folder(self, folder_name="Diabetes Analytics"):
        """Create folder for organizing dashboards"""
        print(f"\n📁 Creating folder: {folder_name}")
        
        folder_config = {
            "title": folder_name
        }

        try:
            response = self.session.post(
                f"{self.grafana_url}/api/folders",
                json=folder_config
            )
            if response.status_code in [200, 201]:
                folder_data = response.json()
                print(f"✅ Folder '{folder_name}' created successfully")
                return folder_data.get('uid')
            elif response.status_code == 412:
                # Folder already exists
                print(f"✅ Folder '{folder_name}' already exists")
                # Get existing folder
                response = self.session.get(f"{self.grafana_url}/api/folders")
                if response.status_code == 200:
                    folders = response.json()
                    for folder in folders:
                        if folder.get('title') == folder_name:
                            return folder.get('uid')
                return None
            else:
                print(f"❌ Failed to create folder: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating folder: {e}")
            return None

    def create_deep_analysis_dashboard(self, folder_uid=None):
        """Create the comprehensive deep analysis dashboard"""
        print("\n🎯 Creating Deep Analysis Dashboard...")

        dashboard_config = {
            "dashboard": {
                "id": None,
                "title": "Diabetes Deep Analysis Dashboard",
                "tags": ["diabetes", "analytics", "prediction", "deep-analysis"],
                "timezone": "browser",
                "refresh": "30s",
                "time": {
                    "from": "now-7d",
                    "to": "now"
                },
                "timepicker": {
                    "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
                },
                "templating": {
                    "list": [
                        {
                            "name": "time_range",
                            "type": "custom",
                            "label": "Time Range",
                            "query": "1d,7d,30d,90d",
                            "current": {"value": "7d", "text": "7 days"},
                            "options": [
                                {"value": "1d", "text": "1 day"},
                                {"value": "7d", "text": "7 days"},
                                {"value": "30d", "text": "30 days"},
                                {"value": "90d", "text": "90 days"}
                            ]
                        },
                        {
                            "name": "risk_level",
                            "type": "custom",
                            "label": "Risk Level",
                            "query": "all,high,medium,low",
                            "current": {"value": "all", "text": "All Levels"},
                            "options": [
                                {"value": "all", "text": "All Levels"},
                                {"value": "high", "text": "High Risk"},
                                {"value": "medium", "text": "Medium Risk"},
                                {"value": "low", "text": "Low Risk"}
                            ]
                        }
                    ]
                },
                "panels": [
                    # Executive KPI Overview Panel
                    {
                        "id": 1,
                        "title": "Executive KPI Overview",
                        "type": "stat",
                        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                'Total Patients' as metric,
                                total_patients as value,
                                current_timestamp as time
                            FROM analytics.executive_kpi_dashboard
                            ORDER BY time DESC LIMIT 1;
                            """,
                            "format": "table"
                        }],
                        "options": {
                            "reduceOptions": {
                                "values": False,
                                "calcs": ["lastNotNull"],
                                "fields": ""
                            },
                            "orientation": "auto",
                            "textMode": "auto",
                            "colorMode": "value",
                            "graphMode": "area",
                            "justifyMode": "auto"
                        },
                        "fieldConfig": {
                            "defaults": {
                                "color": {"mode": "thresholds"},
                                "thresholds": {
                                    "steps": [
                                        {"color": "green", "value": None},
                                        {"color": "red", "value": 80}
                                    ]
                                }
                            }
                        }
                    },
                    # Temporal Health Trends
                    {
                        "id": 2,
                        "title": "Temporal Health Trends",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                week_start as time,
                                avg_glucose_level,
                                avg_bmi,
                                high_risk_percentage
                            FROM analytics.temporal_health_trends
                            WHERE week_start >= NOW() - INTERVAL '$time_range'
                            ORDER BY week_start;
                            """,
                            "format": "time_series"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "color": {"mode": "palette-classic"},
                                "custom": {
                                    "axisLabel": "",
                                    "axisPlacement": "auto",
                                    "barAlignment": 0,
                                    "drawStyle": "line",
                                    "fillOpacity": 10,
                                    "gradientMode": "none",
                                    "hideFrom": {"legend": False, "tooltip": False, "vis": False},
                                    "lineInterpolation": "linear",
                                    "lineWidth": 1,
                                    "pointSize": 5,
                                    "scaleDistribution": {"type": "linear"},
                                    "showPoints": "never",
                                    "spanNulls": False,
                                    "stacking": {"group": "A", "mode": "none"},
                                    "thresholdsStyle": {"mode": "off"}
                                }
                            }
                        }
                    },
                    # Risk Distribution
                    {
                        "id": 3,
                        "title": "Risk Distribution Analysis",
                        "type": "piechart",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                risk_category,
                                COUNT(*) as patient_count
                            FROM analytics.demographic_health_profile
                            GROUP BY risk_category;
                            """,
                            "format": "table"
                        }],
                        "options": {
                            "reduceOptions": {
                                "values": False,
                                "calcs": ["lastNotNull"],
                                "fields": ""
                            },
                            "pieType": "pie",
                            "tooltip": {"mode": "single"},
                            "legend": {
                                "displayMode": "visible",
                                "placement": "bottom"
                            }
                        }
                    },
                    # Model Performance Metrics
                    {
                        "id": 4,
                        "title": "Model Performance Tracking",
                        "type": "stat",
                        "gridPos": {"h": 6, "w": 8, "x": 0, "y": 16},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                'Accuracy' as metric,
                                avg_accuracy as value
                            FROM analytics.model_performance_deep_analysis
                            ORDER BY evaluation_date DESC LIMIT 1;
                            """,
                            "format": "table"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "color": {"mode": "thresholds"},
                                "thresholds": {
                                    "steps": [
                                        {"color": "red", "value": None},
                                        {"color": "yellow", "value": 0.7},
                                        {"color": "green", "value": 0.8}
                                    ]
                                },
                                "unit": "percentunit"
                            }
                        }
                    },
                    # Data Quality Assessment
                    {
                        "id": 5,
                        "title": "Data Quality Score",
                        "type": "gauge",
                        "gridPos": {"h": 6, "w": 8, "x": 8, "y": 16},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                overall_quality_score
                            FROM analytics.data_quality_comprehensive
                            ORDER BY assessment_timestamp DESC LIMIT 1;
                            """,
                            "format": "table"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "color": {"mode": "thresholds"},
                                "thresholds": {
                                    "steps": [
                                        {"color": "red", "value": None},
                                        {"color": "yellow", "value": 0.7},
                                        {"color": "green", "value": 0.9}
                                    ]
                                },
                                "min": 0,
                                "max": 1,
                                "unit": "percentunit"
                            }
                        },
                        "options": {
                            "orientation": "auto",
                            "reduceOptions": {
                                "values": False,
                                "calcs": ["lastNotNull"],
                                "fields": ""
                            },
                            "showThresholdLabels": False,
                            "showThresholdMarkers": True
                        }
                    },
                    # Clinical Feature Insights
                    {
                        "id": 6,
                        "title": "Clinical Feature Correlations",
                        "type": "table",
                        "gridPos": {"h": 6, "w": 8, "x": 16, "y": 16},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                feature_name,
                                correlation_with_diabetes,
                                clinical_significance
                            FROM analytics.clinical_feature_insights
                            ORDER BY ABS(correlation_with_diabetes) DESC
                            LIMIT 10;
                            """,
                            "format": "table"
                        }],
                        "fieldConfig": {
                            "defaults": {
                                "custom": {
                                    "align": "auto",
                                    "displayMode": "auto"
                                }
                            },
                            "overrides": [
                                {
                                    "matcher": {"id": "byName", "options": "correlation_with_diabetes"},
                                    "properties": [
                                        {
                                            "id": "custom.displayMode",
                                            "value": "color-background"
                                        },
                                        {
                                            "id": "thresholds",
                                            "value": {
                                                "steps": [
                                                    {"color": "red", "value": -1},
                                                    {"color": "yellow", "value": -0.3},
                                                    {"color": "green", "value": 0.3},
                                                    {"color": "red", "value": 0.7}
                                                ]
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            "folderId": folder_uid,
            "overwrite": True,
            "message": "Created diabetes deep analysis dashboard via API"
        }

        try:
            response = self.session.post(
                f"{self.grafana_url}/api/dashboards/db",
                json=dashboard_config
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                print("✅ Deep Analysis Dashboard created successfully!")
                print(f"   Dashboard URL: {self.grafana_url}/d/{result.get('uid')}")
                return result
            else:
                print(f"❌ Failed to create dashboard: {response.status_code}")
                print(f"   Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error creating dashboard: {e}")
            return None

    def create_monitoring_dashboard(self, folder_uid=None):
        """Create monitoring and alerting dashboard"""
        print("\n📊 Creating Monitoring Dashboard...")

        dashboard_config = {
            "dashboard": {
                "id": None,
                "title": "Diabetes Pipeline Monitoring",
                "tags": ["monitoring", "pipeline", "diabetes", "alerts"],
                "timezone": "browser",
                "refresh": "15s",
                "time": {
                    "from": "now-1h",
                    "to": "now"
                },
                "panels": [
                    # System Health Panel
                    {
                        "id": 1,
                        "title": "Real-time System Health",
                        "type": "stat",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                system_status,
                                active_connections,
                                data_processing_rate
                            FROM analytics.real_time_system_health
                            ORDER BY timestamp DESC LIMIT 1;
                            """,
                            "format": "table"
                        }]
                    },
                    # ETL Performance Panel
                    {
                        "id": 2,
                        "title": "ETL Pipeline Performance",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                pipeline_run_time as time,
                                avg_processing_time,
                                records_processed_per_hour,
                                error_rate
                            FROM analytics.etl_pipeline_performance
                            WHERE pipeline_run_time >= NOW() - INTERVAL '1 hour'
                            ORDER BY pipeline_run_time;
                            """,
                            "format": "time_series"
                        }]
                    },
                    # Alert Monitoring Panel
                    {
                        "id": 3,
                        "title": "Active Alerts",
                        "type": "table",
                        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8},
                        "targets": [{
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": """
                            SELECT 
                                alert_type,
                                current_value,
                                threshold_value,
                                alert_status,
                                last_triggered
                            FROM analytics.alert_threshold_monitoring
                            WHERE alert_status = 'ACTIVE'
                            ORDER BY last_triggered DESC;
                            """,
                            "format": "table"
                        }]
                    }
                ]
            },
            "folderId": folder_uid,
            "overwrite": True,
            "message": "Created monitoring dashboard via API"
        }

        try:
            response = self.session.post(
                f"{self.grafana_url}/api/dashboards/db",
                json=dashboard_config
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                print("✅ Monitoring Dashboard created successfully!")
                print(f"   Dashboard URL: {self.grafana_url}/d/{result.get('uid')}")
                return result
            else:
                print(f"❌ Failed to create monitoring dashboard: {response.status_code}")
                print(f"   Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error creating monitoring dashboard: {e}")
            return None

    def setup_alerts(self):
        """Setup Grafana alerting rules"""
        print("\n🚨 Setting up alerting rules...")
        
        # Note: Grafana alerting API varies by version
        # This is a basic example for Grafana 8+
        alert_rules = [
            {
                "title": "High Diabetes Risk Alert",
                "condition": "B",
                "data": [
                    {
                        "refId": "A",
                        "queryType": "",
                        "relativeTimeRange": {
                            "from": 600,
                            "to": 0
                        },
                        "model": {
                            "datasource": {"type": "postgres", "uid": "postgres-warehouse"},
                            "rawSql": "SELECT COUNT(*) FROM analytics.executive_kpi_dashboard WHERE high_risk_patients > 100"
                        }
                    }
                ],
                "intervalSeconds": 60,
                "noDataState": "NoData",
                "execErrState": "Alerting"
            }
        ]
        
        # Implement alert creation based on Grafana version
        print("⚠️  Alert setup requires manual configuration in current Grafana version")
        print("   Navigate to Alerting > Alert Rules in Grafana UI")

def main():
    print("🚀 Grafana Dashboard API Creator")
    print("=" * 50)
    
    # Check environment variables
    grafana_url = os.getenv('GRAFANA_URL', 'http://localhost:3000')
    grafana_user = "admin"
    grafana_password = "grafana_admin_2025"
    print(f"🔗 Connecting to Grafana at: {grafana_url}")
    
    # Initialize creator
    creator = GrafanaDashboardCreator(grafana_url, grafana_user, grafana_password)
    
    # Wait for Grafana to be ready
    print("⏳ Waiting for Grafana to be ready...")
    for attempt in range(10):
        if creator.check_grafana_health():
            break
        print(f"   Attempt {attempt + 1}/10: Waiting for Grafana...")
        time.sleep(5)
    else:
        print("❌ Grafana is not accessible after 10 attempts")
        sys.exit(1)
    
    # Create datasource
    if not creator.create_datasource_if_not_exists():
        print("❌ Failed to setup datasource")
        sys.exit(1)
    
    # Create folder
    folder_uid = creator.create_folder("Diabetes Analytics")
    
    # Create dashboards
    deep_analysis_result = creator.create_deep_analysis_dashboard(folder_uid)
    monitoring_result = creator.create_monitoring_dashboard(folder_uid)
    
    # Setup alerts
    creator.setup_alerts()
    
    # Summary
    print("\n🎊 DASHBOARD CREATION COMPLETE!")
    print("=" * 50)
    print("✅ Datasource configured: PostgreSQL Warehouse")
    print("✅ Folder created: Diabetes Analytics")
    
    if deep_analysis_result:
        print(f"✅ Deep Analysis Dashboard: {grafana_url}/d/{deep_analysis_result.get('uid')}")
    
    if monitoring_result:
        print(f"✅ Monitoring Dashboard: {grafana_url}/d/{monitoring_result.get('uid')}")
    
    print(f"\n🔗 Access Grafana: {grafana_url}")
    print(f"   Username: {grafana_user}")
    print(f"   Password: {grafana_password}")
    print("\n📊 Features:")
    print("   • Executive KPI overview with real-time metrics")
    print("   • Temporal health trend analysis")
    print("   • Risk distribution visualization")
    print("   • Model performance tracking")
    print("   • Data quality monitoring")
    print("   • Clinical feature correlation analysis")
    print("   • Real-time system health monitoring")
    print("   • ETL pipeline performance tracking")
    print("   • Active alert monitoring")

if __name__ == "__main__":
    main()

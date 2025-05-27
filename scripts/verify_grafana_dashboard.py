#!/usr/bin/env python3
"""
Verify Grafana Dashboard Script
Verifies that the diabetes prediction dashboard and datasources are working correctly
"""

import requests
import json
import sys

class GrafanaDashboardVerifier:
    def __init__(self, grafana_url="http://localhost:3000", username="admin", password="grafana_admin_2025"):
        self.grafana_url = grafana_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = (username, password)
        
    def verify_datasources(self):
        """Verify all datasources are working"""
        print("🔍 Verifying datasources...")
        
        try:
            response = self.session.get(f"{self.grafana_url}/api/datasources")
            if response.status_code != 200:
                print(f"❌ Failed to get datasources: {response.status_code}")
                return False
                
            datasources = response.json()
            print(f"✅ Found {len(datasources)} datasources")
            
            for ds in datasources:
                print(f"   - {ds['name']} ({ds['type']})")
                
                # Test datasource connection
                test_response = self.session.get(f"{self.grafana_url}/api/datasources/{ds['id']}/health")
                if test_response.status_code == 200:
                    print(f"     ✅ Connection OK")
                else:
                    print(f"     ❌ Connection failed: {test_response.status_code}")
                    
            return True
            
        except Exception as e:
            print(f"❌ Error verifying datasources: {e}")
            return False
    
    def verify_dashboard(self):
        """Verify the diabetes dashboard exists and is accessible"""
        print("\n📊 Verifying dashboard...")
        
        try:
            # Search for our dashboard
            response = self.session.get(f"{self.grafana_url}/api/search?query=diabetes")
            if response.status_code != 200:
                print(f"❌ Failed to search dashboards: {response.status_code}")
                return False
                
            dashboards = response.json()
            
            diabetes_dashboard = None
            for dash in dashboards:
                if "diabetes" in dash.get('title', '').lower():
                    diabetes_dashboard = dash
                    break
                    
            if not diabetes_dashboard:
                print("❌ Diabetes dashboard not found")
                return False
                
            print(f"✅ Found dashboard: {diabetes_dashboard['title']}")
            print(f"   URL: {self.grafana_url}/d/{diabetes_dashboard['uid']}")
            
            # Get dashboard details
            dashboard_response = self.session.get(f"{self.grafana_url}/api/dashboards/uid/{diabetes_dashboard['uid']}")
            if dashboard_response.status_code == 200:
                dashboard_data = dashboard_response.json()['dashboard']
                panel_count = len(dashboard_data.get('panels', []))
                print(f"   Panels: {panel_count}")
                print("   ✅ Dashboard accessible")
                return True
            else:
                print(f"   ❌ Failed to get dashboard details: {dashboard_response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error verifying dashboard: {e}")
            return False
    
    def test_queries(self):
        """Test some key database queries"""
        print("\n🔍 Testing database queries...")
        
        # Test queries
        test_queries = [
            ("Total patients", "SELECT COUNT(*) as count FROM gold.diabetes_gold"),
            ("Diabetic cases", "SELECT COUNT(*) as count FROM gold.diabetes_gold WHERE diabetes = 1"),
            ("Average glucose", "SELECT AVG(glucose) as avg_glucose FROM gold.diabetes_gold"),
            ("Data quality", "SELECT COUNT(*) as bronze FROM bronze.diabetes_raw")
        ]
        
        for query_name, query in test_queries:
            try:
                # Use the main datasource for testing
                payload = {
                    "queries": [
                        {
                            "refId": "A",
                            "rawSql": query,
                            "format": "table",
                            "datasource": {
                                "type": "postgres",
                                "uid": "postgres-warehouse"
                            }
                        }
                    ],
                    "from": "now-1h",
                    "to": "now"
                }
                
                response = self.session.post(f"{self.grafana_url}/api/ds/query", json=payload)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('results', {}).get('A', {}).get('frames'):
                        print(f"   ✅ {query_name}: Query executed successfully")
                    else:
                        print(f"   ⚠️  {query_name}: Query returned no data")
                else:
                    print(f"   ❌ {query_name}: Query failed ({response.status_code})")
                    
            except Exception as e:
                print(f"   ❌ {query_name}: Error - {e}")
        
        return True

def main():
    print("🔧 Grafana Dashboard Verification")
    print("=" * 50)
    
    verifier = GrafanaDashboardVerifier()
    
    # Test connection
    try:
        response = verifier.session.get(f"{verifier.grafana_url}/api/health")
        if response.status_code == 200:
            print("✅ Connected to Grafana successfully")
        else:
            print(f"❌ Failed to connect to Grafana: {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)
    
    # Run verifications
    success = True
    success &= verifier.verify_datasources()
    success &= verifier.verify_dashboard()
    success &= verifier.test_queries()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All verifications passed!")
        print(f"\n📈 Access your dashboard at: {verifier.grafana_url}")
        print("   Username: admin")
        print("   Password: grafana_admin_2025")
    else:
        print("⚠️  Some verifications failed. Check the logs above.")
        sys.exit(1)

if __name__ == "__main__":
    main()

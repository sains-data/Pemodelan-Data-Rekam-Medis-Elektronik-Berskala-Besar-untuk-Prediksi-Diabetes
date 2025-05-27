#!/usr/bin/env python3
"""
Test Grafana Authentication
"""

import requests
import json

def test_grafana_login():
    """Test different authentication methods for Grafana"""
    
    session = requests.Session()
    
    # Method 1: Try basic auth
    print("🔐 Testing Basic Authentication...")
    try:
        response = session.get("http://localhost:3000/api/datasources", 
                             auth=("admin", "grafana_admin_2025"))
        print(f"Basic Auth Status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Basic Auth Success!")
            return session
        else:
            print(f"❌ Basic Auth Failed: {response.text}")
    except Exception as e:
        print(f"❌ Basic Auth Error: {e}")
    
    # Method 2: Try session login
    print("\n🔐 Testing Session Login...")
    try:
        login_data = {
            "user": "admin",
            "password": "grafana_admin_2025"
        }
        response = session.post("http://localhost:3000/login", json=login_data)
        print(f"Session Login Status: {response.status_code}")
        
        if response.status_code == 200:
            # Try to access datasources
            response = session.get("http://localhost:3000/api/datasources")
            print(f"Datasources Access Status: {response.status_code}")
            if response.status_code == 200:
                print("✅ Session Login Success!")
                return session
            else:
                print(f"❌ Session Login Failed: {response.text}")
        else:
            print(f"❌ Login Failed: {response.text}")
    except Exception as e:
        print(f"❌ Session Login Error: {e}")
    
    # Method 3: Try default credentials
    print("\n🔐 Testing Default Credentials...")
    try:
        response = session.get("http://localhost:3000/api/datasources", 
                             auth=("admin", "admin"))
        print(f"Default Auth Status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Default Auth Success!")
            return session
        else:
            print(f"❌ Default Auth Failed: {response.text}")
    except Exception as e:
        print(f"❌ Default Auth Error: {e}")
    
    return None

def test_grafana_endpoints():
    """Test various Grafana endpoints"""
    print("\n🔍 Testing Grafana Endpoints...")
    
    # Test health endpoint
    try:
        response = requests.get("http://localhost:3000/api/health")
        print(f"Health endpoint: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"Health endpoint error: {e}")
    
    # Test org endpoint (doesn't require auth)
    try:
        response = requests.get("http://localhost:3000/api/org")
        print(f"Org endpoint: {response.status_code}")
        if response.status_code != 200:
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Org endpoint error: {e}")
    
    # Test admin endpoint
    try:
        response = requests.get("http://localhost:3000/api/admin/settings", 
                              auth=("admin", "grafana_admin_2025"))
        print(f"Admin endpoint: {response.status_code}")
        if response.status_code != 200:
            print(f"Response: {response.text[:200]}")
    except Exception as e:
        print(f"Admin endpoint error: {e}")

if __name__ == "__main__":
    print("🚀 Testing Grafana Authentication...")
    test_grafana_endpoints()
    
    session = test_grafana_login()
    if session:
        print("\n✅ Successfully authenticated to Grafana!")
    else:
        print("\n❌ Failed to authenticate to Grafana!")
        print("\n💡 Troubleshooting suggestions:")
        print("   1. Check if Grafana container is fully initialized")
        print("   2. Restart Grafana container: docker restart grafana")
        print("   3. Check Grafana logs: docker logs grafana")
        print("   4. Try accessing web interface at http://localhost:3000")

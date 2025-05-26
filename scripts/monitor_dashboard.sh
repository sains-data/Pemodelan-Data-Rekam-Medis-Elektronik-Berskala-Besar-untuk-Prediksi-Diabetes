#!/bin/bash

# Dashboard Health Monitor
GRAFANA_URL="http://localhost:3000"

check_grafana_health() {
    if curl -s -f "$GRAFANA_URL/api/health" >/dev/null 2>&1; then
        echo "✅ Grafana is healthy"
        return 0
    else
        echo "❌ Grafana is not responding"
        return 1
    fi
}

check_dashboard_data() {
    response=$(curl -s -u "admin:admin" "$GRAFANA_URL/api/search?query=Diabetes" 2>/dev/null || echo "")
    if [[ "$response" == *"Diabetes"* ]]; then
        echo "✅ Dashboard is accessible"
        return 0
    else
        echo "❌ Dashboard not found"
        return 1
    fi
}

echo "=== Dashboard Health Check ==="
check_grafana_health
check_dashboard_data
echo "=== Check Complete ==="

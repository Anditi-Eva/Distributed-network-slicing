from flask import Flask, jsonify
import requests
from node_manager import load_cluster_config

app = Flask(__name__)

@app.route('/cloud/analytics', methods=['GET'])
def aggregate_telemetry():
    """Cloud Aggregator: Pulls Edge metrics and computes system-wide stats."""
    config = load_cluster_config()
    edge_telemetry = []

    for node in config.get("nodes", []):
        if node["role"] == "worker":
            metrics_url = f"http://{node['ip_address']}:{node['agent_port']}/metrics"
            try:
                res = requests.get(metrics_url, timeout=2)
                if res.status_code == 200:
                    edge_telemetry.append(res.json())
            except requests.exceptions.RequestException:
                edge_telemetry.append({"node_id": node["node_id"], "status": "OFFLINE"})

    return jsonify({
        "cloud_service": "Global Telemetry & Analytics Engine",
        "active_edge_nodes": len(edge_telemetry),
        "telemetry_stream": edge_telemetry
    })

if __name__ == '__main__':
    print("Cloud Telemetry Aggregator Service listening on port 5002...")
    app.run(host='127.0.0.1', port=5002)

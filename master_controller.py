from flask import Flask, request, jsonify
import requests
from node_manager import load_cluster_config

app = Flask(__name__)

def schedule_process(cpu_req, mem_req):
    config = load_cluster_config()
    best_candidate = None
    max_free_mem = -1

    for node in config.get("nodes", []):
        if node["role"] == "worker":
            worker_url = f"http://{node['ip_address']}:{node['agent_port']}/health"
            try:
                res = requests.get(worker_url, timeout=2)
                if res.status_code == 200:
                    health_data = res.json()
                    if (health_data["available_cpu"] >= cpu_req and 
                        health_data["available_memory"] >= mem_req):
                        if health_data["available_memory"] > max_free_mem:
                            max_free_mem = health_data["available_memory"]
                            best_candidate = node
            except requests.exceptions.RequestException:
                continue

    return best_candidate

@app.route('/')
def index():
    return """
    <h1>Network Slicing Master Controller Active</h1>
    <p>Available Web Endpoints:</p>
    <ul>
        <li><a href="/api/nodes">/api/nodes</a> - View Cluster Topology</li>
        <li><a href="/metrics">/metrics</a> - View Real-Time Telemetry (CPU, Memory, Latency, Jitter)</li>
        <li><a href="/api/slice/request?slice_type=URLLC&cpu=1.5&memory=1024">/api/slice/request</a> - Trigger Browser Slice Creation</li>
    </ul>
    """

@app.route('/api/nodes', methods=['GET'])
def list_nodes():
    return jsonify(load_cluster_config())

@app.route('/metrics', methods=['GET'])
def aggregated_metrics():
    """Proxies and returns real-time worker node telemetry to the browser."""
    config = load_cluster_config()
    telemetry = []

    for node in config.get("nodes", []):
        if node["role"] == "worker":
            metrics_url = f"http://{node['ip_address']}:{node['agent_port']}/metrics"
            try:
                res = requests.get(metrics_url, timeout=2)
                if res.status_code == 200:
                    telemetry.append(res.json())
            except requests.exceptions.RequestException:
                telemetry.append({"node_id": node["node_id"], "status": "UNREACHABLE"})

    return jsonify({"cluster_telemetry": telemetry})

@app.route('/api/slice/request', methods=['GET', 'POST'])
def handle_slice_request():
    if request.method == 'GET':
        slice_type = request.args.get("slice_type", "URLLC")
        cpu_req = float(request.args.get("cpu", 1.5))
        mem_req = int(request.args.get("memory", 1024))
    else:
        data = request.get_json() or {}
        slice_type = data.get("slice_type", "eMBB")
        cpu_req = float(data.get("cpu", 1.0))
        mem_req = int(data.get("memory", 512))

    target_node = schedule_process(cpu_req, mem_req)
    if not target_node:
        return jsonify({
            "status": "REJECTED",
            "reason": "Scheduling failed: No worker nodes have sufficient resource capacity."
        }), 503

    dispatch_url = f"http://{target_node['ip_address']}:{target_node['agent_port']}/process/create"
    try:
        response = requests.post(dispatch_url, json={
            "slice_type": slice_type,
            "cpu": cpu_req,
            "memory": mem_req
        }, timeout=3)
        
        if response.status_code == 201:
            return jsonify({
                "status": "SUCCESS",
                "scheduled_node": target_node["node_id"],
                "execution_details": response.json()
            }), 201
        else:
            return jsonify({"status": "FAILED", "node_response": response.json()}), 500
            
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "ERROR", "message": f"Worker communication error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)

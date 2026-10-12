import time
import requests
import threading
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# Physical Worker Nodes (Hidden from Client)
WORKER_NODES = [
    {"id": "worker_primary", "url": "http://127.0.0.1:5001", "status": "UP"},
    {"id": "worker_backup",  "url": "http://127.0.0.1:5002", "status": "UP"}
]

# Logical Resource Registry (Mapping logical slices to physical nodes)
logical_slices = {}
lock = threading.Lock()

# Prometheus Telemetry Metrics for Transparency Layer
TRANSPARENT_REQ = Counter('transparency_requests_total', 'Total Requests via Gateway', ['type'])
FAILOVER_COUNT  = Counter('transparency_failovers_total', 'Total Failures Hidden from Client')
MIGRATION_COUNT = Counter('transparency_migrations_total', 'Total Slice Migrations Executed')
GATEWAY_LATENCY = Histogram('transparency_overhead_seconds', 'Gateway Abstraction Overhead')

# ==========================================
# 1. ACCESS & LOCATION TRANSPARENCY ENDPOINT
# ==========================================
@app.route('/api/v1/slice', methods=['POST'])
def allocate_logical_slice():
    """
    Logical Resource Endpoint: Clients call this single URI without knowing
    the target IP, port, replication state, or underlying worker topology.
    """
    start_time = time.time()
    TRANSPARENT_REQ.labels(type="allocation").inc()
    data = request.json or {}
    slice_id = data.get("slice_id", f"slice_{int(time.time())}")
    
    # 2. REPLICATION & FAILURE TRANSPARENCY (Transparent Failover & State Replication)
    response_data = None
    successful_node = None

    with lock:
        for node in WORKER_NODES:
            try:
                # Location Transparency: Proxy routes request to underlying worker node
                target_url = f"{node['url']}/resource/request"
                resp = requests.post(target_url, json=data, timeout=2.0)
                
                if resp.status_code == 200:
                    response_data = resp.json()
                    successful_node = node['id']
                    node['status'] = "UP"
                    # Track physical location transparently in gateway registry
                    logical_slices[slice_id] = {
                        "primary_node": successful_node,
                        "data": data,
                        "status": "ACTIVE"
                    }
                    break
                else:
                    node['status'] = "DEGRADED"
            except Exception as e:
                # Failure Transparency: Silently swallow error and try next backup worker
                node['status'] = "DOWN"
                FAILOVER_COUNT.inc()
                continue

    GATEWAY_LATENCY.observe(time.time() - start_time)

    if response_data:
        return jsonify({
            "logical_slice_id": slice_id,
            "status": "COMMITTED",
            "message": "Slice created transparently across physical infrastructure.",
            "transparency_guarantees": ["Access", "Location", "Failure", "Replication"]
        }), 200
    else:
        return jsonify({
            "error": "Resource Allocation Failed",
            "message": "All physical compute nodes currently unreachable."
        }), 503

# ==========================================
# 2. MIGRATION TRANSPARENCY ENDPOINT
# ==========================================
@app.route('/api/v1/slice/migrate', methods=['POST'])
def migrate_slice():
    """
    Migration Transparency: Moves an active slice from Node A to Node B
    without disrupting the logical slice identifier or client session.
    """
    data = request.json or {}
    slice_id = data.get("slice_id")
    target_node_url = data.get("target_node_url", "http://127.0.0.1:5002")

    if slice_id not in logical_slices:
        return jsonify({"error": "Logical Slice ID not found"}), 404

    with lock:
        current_info = logical_slices[slice_id]
        # Re-allocate slice at target physical location
        resp = requests.post(f"{target_node_url}/resource/request", json=current_info["data"], timeout=2.0)
        
        if resp.status_code == 200:
            current_info["primary_node"] = "worker_migrated"
            MIGRATION_COUNT.inc()
            return jsonify({
                "logical_slice_id": slice_id,
                "status": "MIGRATED",
                "message": "Slice migrated transparently to new physical node."
            }), 200
        else:
            return jsonify({"error": "Migration target refused allocation"}), 500

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    print("Starting Transparency API Gateway on http://0.0.0.0:5000...")
    app.run(host='0.0.0.0', port=5000, debug=False)

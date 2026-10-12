import time
import json
import threading
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# ==========================================
# 1. DISTRIBUTED SHARED MEMORY (DSM) STATE
# ==========================================
# Memory pages shared logically across godfather (Host) & spoiltbrat (VM)
DSM_SHARED_PAGES = {
    "page_0x01": {"slice_id": "slice_urllc_01", "allocated_cpu": 2.0, "version": 1, "owner": "godfather"},
    "page_0x02": {"slice_id": "slice_embb_01",  "allocated_ram": 2048, "version": 1, "owner": "spoiltbrat"}
}

dsm_lock = threading.Lock()

# Metrics
RPC_CALLS = Counter('rpc_calls_total', 'Total Remote Procedure Calls Executed', ['method', 'status'])
RPC_LATENCY = Histogram('rpc_execution_latency_seconds', 'RPC Round-Trip Execution Time')
DSM_SYNC_TIME = Histogram('dsm_synchronization_seconds', 'DSM State Replication Overhead')
DSM_PAGES_GAUGE = Gauge('dsm_active_pages_total', 'Active Distributed Shared Memory Pages')

DSM_PAGES_GAUGE.set(len(DSM_SHARED_PAGES))

# ==========================================
# 2. RPC CLIENT & SERVER STUB MOCKING ENGINE
# ==========================================
# Classic RPC Pipeline: Client -> Client Stub -> Network -> Server Stub -> Server Process

class SliceServiceServerStub:
    """Server Stub: Unmarshals request bytes, invokes server procedure, marshals response."""
    @staticmethod
    def execute_allocate_slice(payload_bytes):
        # 1. Unmarshal Request (Server Stub)
        request_data = json.loads(payload_bytes.decode('utf-8'))
        proc_id = request_data.get("process_id")
        req_cpu = float(request_data.get("cpu", 0.1))
        
        # 2. Execute Server Core Logic
        status = "SUCCESS" if req_cpu <= 4.0 else "EXHAUSTED"
        
        # 3. Marshal Response (Server Stub)
        response = {
            "status": status,
            "allocated_process": proc_id,
            "timestamp": time.time(),
            "protocol": "gRPC/Protobuf-over-TCP"
        }
        return json.dumps(response).encode('utf-8')

# ==========================================
# 3. RPC ENDPOINTS
# ==========================================
@app.route('/rpc/AllocateSlice', methods=['POST'])
def rpc_allocate_slice():
    """
    Simulates gRPC binary call via client/server stub marshaling pipeline.
    """
    start_time = time.time()
    try:
        # Received raw byte stream from Network
        raw_bytes = request.data
        
        # Dispatch to Server Stub
        response_bytes = SliceServiceServerStub.execute_allocate_slice(raw_bytes)
        
        RPC_LATENCY.observe(time.time() - start_time)
        RPC_CALLS.labels(method="AllocateSlice", status="SUCCESS").inc()
        
        return response_bytes, 200, {'Content-Type': 'application/octet-stream'}
    except Exception as e:
        RPC_CALLS.labels(method="AllocateSlice", status="FAILED").inc()
        return jsonify({"error": str(e)}), 500

# ==========================================
# 4. DISTRIBUTED SHARED MEMORY (DSM) ENDPOINTS
# ==========================================
@app.route('/dsm/read', methods=['POST'])
def dsm_read():
    """Read-Only access to Distributed Shared Memory Page."""
    data = request.json or {}
    page_id = data.get("page_id", "page_0x01")
    
    with dsm_lock:
        if page_id in DSM_SHARED_PAGES:
            return jsonify({"page_id": page_id, "data": DSM_SHARED_PAGES[page_id]}), 200
        return jsonify({"error": "Page Fault: Page ID not found in DSM space"}), 404

@app.route('/dsm/write', methods=['POST'])
def dsm_write():
    """
    Write-Update Sequential Consistency Protocol:
    Invalidates/Updates shared pages across node boundaries atomically.
    """
    start_sync = time.time()
    data = request.json or {}
    page_id = data.get("page_id", "page_0x01")
    new_val = data.get("value")

    with dsm_lock:
        if page_id not in DSM_SHARED_PAGES:
            DSM_SHARED_PAGES[page_id] = {"version": 0, "owner": "coordinator"}

        # Write-Update Protocol: Increment version and broadcast memory update
        DSM_SHARED_PAGES[page_id]["version"] += 1
        DSM_SHARED_PAGES[page_id]["data"] = new_val
        DSM_SHARED_PAGES[page_id]["last_updated"] = time.time()
        
        DSM_PAGES_GAUGE.set(len(DSM_SHARED_PAGES))
        DSM_SYNC_TIME.observe(time.time() - start_sync)

        return jsonify({
            "message": "DSM Write-Update Committed",
            "page_id": page_id,
            "current_version": DSM_SHARED_PAGES[page_id]["version"],
            "consistency_model": "Sequential Consistency (Write-Update)"
        }), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    print("Starting RPC & DSM Engine on http://0.0.0.0:5004...")
    app.run(host='0.0.0.0', port=5004, debug=False)

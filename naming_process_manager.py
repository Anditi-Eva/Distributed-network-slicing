import time
import uuid
import requests
import threading
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# ==========================================
# 1. NAMING SUBSYSTEM STATE (Prefix Tables & Name Server)
# ==========================================
# Domain Prefix Routing Table (DNS/Prefix Table Mechanism)
PREFIX_TABLE = {
    "urllc.slice": "http://127.0.0.1:5001",  # Edge Worker (Ultra-Reliable Low-Latency)
    "embb.slice":  "http://127.0.0.1:5002",  # Core Worker (Enhanced Mobile Broadband)
    "mmtc.slice":  "http://127.0.0.1:5001"   # IoT Worker (Massive Machine Type)
}

# Dynamic Name Server Registry (Logical Name -> Resource Location Mapping)
NAME_REGISTRY = {
    "urllc.slice.telecom.local": {"location": "http://127.0.0.1:5001", "type": "PRIMARY", "status": "ACTIVE"},
    "embb.slice.telecom.local":  {"location": "http://127.0.0.1:5002", "type": "PRIMARY", "status": "ACTIVE"},
    "backup.slice.telecom.local":{"location": "http://127.0.0.1:5002", "type": "BACKUP",  "status": "ACTIVE"}
}

# ==========================================
# 2. PROCESS MANAGEMENT STATE
# ==========================================
# Process Lifecycle Registry: Tracking Create -> Locate -> Schedule -> Coordinate -> Terminate
PROCESS_TABLE = {}
lock = threading.Lock()

# Prometheus Metrics
NAME_LOOKUPS = Counter('naming_resolution_lookups_total', 'Total Name Resolution Lookups', ['status'])
RESOLUTION_LATENCY = Histogram('naming_resolution_latency_seconds', 'Name Resolution Latency')
PROCESS_LIFECYCLE_COUNTER = Counter('process_lifecycle_events_total', 'Process Management State Transitions', ['stage'])

# ==========================================
# 3. NAMING SUBSYSTEM ENDPOINTS
# ==========================================
@app.route('/dns/resolve', methods=['POST'])
def resolve_name():
    """
    Model: Name -> Name Resolution -> Resource Location
    Resolves logical names using Prefix Table or Direct Lookup.
    """
    start_time = time.time()
    data = request.json or {}
    logical_name = data.get("name", "")

    # 1. Direct Name Server Lookup
    if logical_name in NAME_REGISTRY:
        location = NAME_REGISTRY[logical_name]["location"]
        RESOLUTION_LATENCY.observe(time.time() - start_time)
        NAME_LOOKUPS.labels(status="SUCCESS_DIRECT").inc()
        return jsonify({
            "logical_name": logical_name,
            "resolved_location": location,
            "resolution_mechanism": "Direct Name Server Registry",
            "resolution_time_ms": round((time.time() - start_time) * 1000, 3)
        }), 200

    # 2. Prefix Table Search
    for prefix, target_location in PREFIX_TABLE.items():
        if logical_name.startswith(prefix):
            RESOLUTION_LATENCY.observe(time.time() - start_time)
            NAME_LOOKUPS.labels(status="SUCCESS_PREFIX").inc()
            return jsonify({
                "logical_name": logical_name,
                "resolved_location": target_location,
                "resolution_mechanism": f"Prefix Table Rule [{prefix}]",
                "resolution_time_ms": round((time.time() - start_time) * 1000, 3)
            }), 200

    NAME_LOOKUPS.labels(status="FAILED").inc()
    return jsonify({"error": "Name Resolution Failed", "message": f"Name '{logical_name}' not found"}), 404

# ==========================================
# 4. PROCESS MANAGEMENT LIFECYCLE ENDPOINTS
# ==========================================
@app.route('/process/orchestrate', methods=['POST'])
def orchestrate_process():
    """
    Executes Process Lifecycle:
    CREATE -> LOCATE -> SCHEDULE -> COORDINATE -> TERMINATE
    """
    data = request.json or {}
    logical_name = data.get("slice_name", "urllc.slice.telecom.local")
    req_cpu = data.get("cpu", 0.2)
    req_ram = data.get("ram", 128)

    pid = f"proc_{uuid.uuid4().hex[:8]}"

    # STAGE 1: CREATE
    PROCESS_LIFECYCLE_COUNTER.labels(stage="CREATE").inc()
    with lock:
        PROCESS_TABLE[pid] = {"pid": pid, "name": logical_name, "stage": "CREATED", "timestamp": time.time()}

    # STAGE 2: LOCATE (Name Resolution)
    PROCESS_LIFECYCLE_COUNTER.labels(stage="LOCATE").inc()
    target_location = None
    if logical_name in NAME_REGISTRY:
        target_location = NAME_REGISTRY[logical_name]["location"]
    else:
        for prefix, loc in PREFIX_TABLE.items():
            if logical_name.startswith(prefix):
                target_location = loc
                break

    if not target_location:
        PROCESS_TABLE[pid]["stage"] = "LOCATION_FAILED"
        return jsonify({"error": "Failed to locate resource for process", "pid": pid}), 400

    PROCESS_TABLE[pid]["location"] = target_location
    PROCESS_TABLE[pid]["stage"] = "LOCATED"

    # STAGE 3 & 4: SCHEDULE & COORDINATE (Remote Resource Allocation)
    PROCESS_LIFECYCLE_COUNTER.labels(stage="SCHEDULE").inc()
    PROCESS_LIFECYCLE_COUNTER.labels(stage="COORDINATE").inc()
    
    try:
        alloc_resp = requests.post(
            f"{target_location}/resource/request",
            json={"process_id": pid, "cpu": req_cpu, "ram": req_ram},
            timeout=3.0
        )
        if alloc_resp.status_code == 200:
            PROCESS_TABLE[pid]["stage"] = "RUNNING_COORDINATED"
        else:
            PROCESS_TABLE[pid]["stage"] = "SCHEDULE_REJECTED"
            return jsonify({"error": "Node scheduling rejected process", "pid": pid}), 500
    except Exception as e:
        PROCESS_TABLE[pid]["stage"] = "COORDINATION_FAILED"
        return jsonify({"error": "Remote node coordination failed", "pid": pid}), 500

    # STAGE 5: TERMINATE (Clean Release)
    PROCESS_LIFECYCLE_COUNTER.labels(stage="TERMINATE").inc()
    PROCESS_TABLE[pid]["stage"] = "TERMINATED_SUCCESS"

    return jsonify({
        "process_id": pid,
        "logical_name": logical_name,
        "assigned_location": target_location,
        "lifecycle_states": ["CREATED", "LOCATED", "SCHEDULED", "COORDINATED", "TERMINATED"],
        "final_status": "COMPLETED"
    }), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    print("Starting Naming & Process Orchestration Engine on http://0.0.0.0:5003...")
    app.run(host='0.0.0.0', port=5003, debug=False)

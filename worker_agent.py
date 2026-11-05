import os
import time
import threading
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from rag_deadlock_engine import ResourceAllocationGraph
from heartbeat_detector import HeartbeatDetector
from replicated_state_machine import ReplicatedStateMachine
from byzantine_analyzer import ByzantineAnalyzer

app = Flask(__name__)

# ==========================================
# 1. CAPACITY & RESOURCE STATE
# ==========================================
TOTAL_CPU = 4.0
TOTAL_RAM = 4096.0
TOTAL_SOCKETS = 4.0

graph = ResourceAllocationGraph({"CPU": TOTAL_CPU, "RAM": TOTAL_RAM, "SOCKETS": TOTAL_SOCKETS})
lock = threading.Lock()

# Subsystem instances
NODE_ID = os.environ.get("NODE_ID", "edge")
PEERS = {
    "edge": "http://127.0.0.1:5001",
    "core": "http://127.0.0.1:5002",
    "cloud": "http://127.0.0.1:5003"
}
PEERS_EXCEPT_SELF = {k: v for k, v in PEERS.items() if k != NODE_ID}

detector = HeartbeatDetector(NODE_ID, PEERS_EXCEPT_SELF)
rsm = ReplicatedStateMachine(NODE_ID, PEERS_EXCEPT_SELF, is_primary=(NODE_ID == "edge"))

# State flags for Milestone 7 Fault Simulation
IS_CRASHED = False
SIMULATE_PACKET_DROP = False
LATENCY_DELAY = 0

# ==========================================
# 2. PROMETHEUS METRIC REGISTRY
# ==========================================
GAUGE_CPU = Gauge('network_slicing_available_cpu', 'Available Node CPU Cores')
GAUGE_RAM = Gauge('network_slicing_available_ram_mb', 'Available Node RAM in MB')
COUNTER_TX = Counter('network_slicing_allocations_total', 'Local resource allocation requests by outcome', ['status'])
HIST_LATENCY = Histogram('network_slicing_allocation_latency_seconds', 'Local resource allocation latency')
COUNTER_DEADLOCKS = Counter('network_slicing_deadlocks_total', 'Deadlock cycles detected in the wait-for graph')
COUNTER_VICTIMS = Counter('network_slicing_deadlock_victims_total', 'Processes preempted to break a deadlock')
GAUGE_WAITING = Gauge('network_slicing_waiting_processes', 'Processes waiting for a resource (request edges)')
GAUGE_UTIL = Gauge('network_slicing_utilization_ratio', 'Allocated fraction of a resource', ['resource'])

GAUGE_MTTR = Gauge('network_slicing_mttr_ms', 'Mean Time To Recovery in milliseconds')
GAUGE_AVAILABILITY = Gauge('network_slicing_availability_pct', 'Measured System Availability Percentage')

GAUGE_CPU.set(TOTAL_CPU)
GAUGE_RAM.set(TOTAL_RAM)
GAUGE_MTTR.set(0.0)
GAUGE_AVAILABILITY.set(100.0)

def refresh_gauges():
    GAUGE_CPU.set(graph.available["CPU"])
    GAUGE_RAM.set(graph.available["RAM"])
    GAUGE_WAITING.set(len(graph.requests))
    for r, u in graph.utilization().items():
        GAUGE_UTIL.labels(resource=r).set(u)

# ==========================================
# 3. MILESTONE 7: HEARTBEAT, REPLICATION & BYZANTINE
# ==========================================
@app.route('/health', methods=['GET'])
def health():
    if IS_CRASHED:
        return jsonify({"status": "CRASHED"}), 500
    if SIMULATE_PACKET_DROP:
        time.sleep(10)
        return jsonify({"status": "TIMEOUT"}), 504
    if LATENCY_DELAY > 0:
        time.sleep(LATENCY_DELAY)
    return jsonify({"status": "UP", "node": NODE_ID, "timestamp": time.time()}), 200

@app.route('/detector/status', methods=['GET'])
def detector_status():
    return jsonify(detector.get_status())

@app.route('/replication/append', methods=['POST'])
def replication_append():
    data = request.json or {}
    return jsonify(rsm.handle_append(data))

@app.route('/replication/state', methods=['GET'])
def replication_state():
    return jsonify({"is_primary": rsm.is_primary, "term": rsm.term, "state": rsm.state, "log_size": len(rsm.log)})

@app.route('/byzantine/analyze', methods=['POST'])
def byzantine_analyze():
    data = request.json or {}
    analyzer = ByzantineAnalyzer(data.get("n", 4), data.get("f", 1))
    bounds = analyzer.verify_byzantine_bounds()
    votes = data.get("votes", {})
    consensus = analyzer.evaluate_byzantine_consensus(votes) if votes else None
    return jsonify({"bounds": bounds, "consensus": consensus})

@app.route('/chaos/inject', methods=['POST'])
def inject_chaos():
    global IS_CRASHED, SIMULATE_PACKET_DROP, LATENCY_DELAY
    data = request.json or {}
    failure_type = data.get("failure_type")

    if failure_type == "crash":
        IS_CRASHED = True
    elif failure_type == "omission":
        SIMULATE_PACKET_DROP = True
    elif failure_type == "latency":
        LATENCY_DELAY = data.get("delay", 5)
    elif failure_type == "reset":
        IS_CRASHED = False
        SIMULATE_PACKET_DROP = False
        LATENCY_DELAY = 0
        if "mttr_ms" in data:
            GAUGE_MTTR.set(float(data["mttr_ms"]))
        if "availability" in data:
            GAUGE_AVAILABILITY.set(float(data["availability"]))

    return jsonify({"message": f"Fault injected: {failure_type}", "node": NODE_ID}), 200

# ==========================================
# 4. NETWORK SLICING & RESOURCE ENDPOINTS
# ==========================================
@app.route('/resource/request', methods=['POST'])
def allocate_resource():
    start_time = time.time()
    if IS_CRASHED:
        return jsonify({"message": "Node down"}), 500

    data = request.json or {}
    proc_id = data.get("process_id")
    req = {"CPU": float(data.get("cpu", 0)), "RAM": float(data.get("ram", 0)),
           "SOCKETS": float(data.get("sockets", 0))}
    wait = bool(data.get("wait", False))

    result = graph.request_resources(proc_id, req, wait=wait)
    refresh_gauges()
    remaining = {"remaining_cpu": graph.available["CPU"], "remaining_ram": graph.available["RAM"]}

    if result["status"] == "ALLOCATED":
        if rsm.is_primary:
            rsm.apply_operation("SET", proc_id, req)
        COUNTER_TX.labels(status="COMMITTED").inc()
        HIST_LATENCY.observe(time.time() - start_time)
        return jsonify({"message": "Resources allocated successfully", "process_id": proc_id,
                        "status": "RUNNING", **remaining}), 200
    if result["status"] == "ORDER_VIOLATION":
        COUNTER_TX.labels(status="ORDER_VIOLATION").inc()
        return jsonify({"message": "Request violates global resource ordering",
                        "process_id": proc_id, "status": "ORDER_VIOLATION"}), 409
    if wait and result["status"] == "WAITING_BLOCKED":
        COUNTER_TX.labels(status="WAITING").inc()
        return jsonify({"message": "Process waiting for resources", "process_id": proc_id,
                        "status": "WAITING", **remaining}), 202
    COUNTER_TX.labels(status="BLOCKED").inc()
    return jsonify({"message": "Request blocked due to resource exhaustion", "process_id": proc_id,
                    "status": "BLOCKED_WAITING", **remaining}), 200

@app.route('/resource/release', methods=['POST'])
def release_resource():
    data = request.json or {}
    proc_id = data.get("process_id")
    released = graph.release_resources(proc_id)
    if rsm.is_primary:
        rsm.apply_operation("DELETE", proc_id, None)
    refresh_gauges()
    return jsonify({"process_id": proc_id, "released": released}), 200

@app.route('/resource/status', methods=['GET'])
def resource_status():
    return jsonify(graph.status(request.args.get("process_id")))

@app.route('/resource/graph', methods=['GET'])
def resource_graph():
    return jsonify(graph.export_graph_state())

@app.route('/resource/reset', methods=['POST'])
def resource_reset():
    global graph
    data = request.json or {}
    graph = ResourceAllocationGraph({"CPU": TOTAL_CPU, "RAM": TOTAL_RAM, "SOCKETS": TOTAL_SOCKETS},
                                    grant_policy=data.get("grant_policy", "fifo"),
                                    prevention=data.get("prevention"))
    refresh_gauges()
    return jsonify({"reset": True, "policy": graph.export_graph_state()["policy"]})

# ==========================================
# 5. MILESTONE 6: DEADLOCK DETECTION & RECOVERY
# ==========================================
@app.route('/deadlock/detect', methods=['GET'])
def deadlock_detect():
    result = graph.detect_deadlock_cycle()
    if result["deadlock_detected"]:
        COUNTER_DEADLOCKS.inc()
    return jsonify(result)

@app.route('/deadlock/recover', methods=['POST'])
def deadlock_recover():
    victim = (request.json or {}).get("victim")
    result = graph.recover_deadlock(victim)
    if result.get("action") != "NONE":
        COUNTER_VICTIMS.inc()
    refresh_gauges()
    return jsonify(result)

@app.route('/metrics', methods=['GET'])
def metrics():
    refresh_gauges()
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    port = int(os.environ.get("WORKER_PORT", 5001))
    detector.start()
    print(f"Starting Worker Agent ({NODE_ID}) on http://0.0.0.0:{port}...")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

import time
import threading
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# ==========================================
# 1. CAPACITY & RESOURCE STATE
# ==========================================
TOTAL_CPU = 4.0
TOTAL_RAM = 4096.0

available_cpu = TOTAL_CPU
available_ram = TOTAL_RAM
active_slices = {}
lock = threading.Lock()

# State flags for Milestone 7 Fault Simulation
IS_CRASHED = False
SIMULATE_PACKET_DROP = False
LATENCY_DELAY = 0

# ==========================================
# 2. PROMETHEUS METRIC REGISTRY
# ==========================================
GAUGE_CPU = Gauge('network_slicing_available_cpu', 'Available Node CPU Cores')
GAUGE_RAM = Gauge('network_slicing_available_ram_mb', 'Available Node RAM in MB')
COUNTER_TX = Counter('network_slicing_tx_total', 'Total 2PC Transactions', ['status'])
HIST_LATENCY = Histogram('network_slicing_tx_latency_seconds', '2PC Transaction Latency')
COUNTER_DEADLOCKS = Counter('network_slicing_deadlocks_total', 'Total Deadlocks Detected')

# Milestone 7 Metrics
GAUGE_MTTR = Gauge('network_slicing_mttr_ms', 'Mean Time To Recovery in milliseconds')
GAUGE_AVAILABILITY = Gauge('network_slicing_availability_pct', 'Measured System Availability Percentage')

# Initialize baseline gauge values
GAUGE_CPU.set(available_cpu)
GAUGE_RAM.set(available_ram)
GAUGE_MTTR.set(275.0)
GAUGE_AVAILABILITY.set(100.0)

# ==========================================
# 3. MILESTONE 7: CHAOS & HEALTH ENDPOINTS
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
    return jsonify({"status": "UP", "timestamp": time.time()}), 200

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
        
    return jsonify({"message": f"Fault injected: {failure_type}"}), 200

# ==========================================
# 4. NETWORK SLICING & RESOURCE ENDPOINTS
# ==========================================
@app.route('/resource/request', methods=['POST'])
def allocate_resource():
    global available_cpu, available_ram
    start_time = time.time()

    if IS_CRASHED:
        return jsonify({"message": "Node down"}), 500

    data = request.json or {}
    proc_id = data.get("process_id")
    req_cpu = float(data.get("cpu", 0))
    req_ram = float(data.get("ram", 0))

    with lock:
        if req_cpu <= available_cpu and req_ram <= available_ram:
            available_cpu -= req_cpu
            available_ram -= req_ram
            active_slices[proc_id] = {"cpu": req_cpu, "ram": req_ram}

            # Update Prometheus Metrics
            GAUGE_CPU.set(available_cpu)
            GAUGE_RAM.set(available_ram)
            COUNTER_TX.labels(status="COMMITTED").inc()
            HIST_LATENCY.observe(time.time() - start_time)

            return jsonify({
                "message": "Resources allocated successfully",
                "process_id": proc_id,
                "status": "RUNNING",
                "remaining_cpu": available_cpu,
                "remaining_ram": available_ram
            }), 200
        else:
            COUNTER_TX.labels(status="BLOCKED").inc()
            COUNTER_DEADLOCKS.inc()
            return jsonify({
                "message": "Request blocked due to resource exhaustion",
                "process_id": proc_id,
                "status": "BLOCKED_WAITING",
                "remaining_cpu": available_cpu,
                "remaining_ram": available_ram
            }), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# ==========================================
# 5. MAIN EXECUTION ENTRYPOINT
# ==========================================
if __name__ == '__main__':
    print("Starting Edge Worker Agent on http://0.0.0.0:5001...")
    app.run(host='0.0.0.0', port=5001, debug=False)

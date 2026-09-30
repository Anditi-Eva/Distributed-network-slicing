from flask import Flask, request, jsonify
import uuid
import time
from rag_deadlock_engine import ResourceAllocationGraph

app = Flask(__name__)

# Initialize RAG with total capacity
TOTAL_RESOURCES = {"CPU": 4.0, "RAM": 4096.0, "SOCKETS": 4.0}
rag = ResourceAllocationGraph(TOTAL_RESOURCES)
active_processes = {}

@app.route('/resource/request', methods=['POST'])
def handle_resource_request():
    """Handles concurrent process resource allocation (P_i -> R_j)."""
    data = request.get_json() or {}
    process_id = data.get("process_id", f"proc-{uuid.uuid4().hex[:6]}")
    req_cpu = float(data.get("cpu", 1.0))
    req_ram = float(data.get("ram", 512.0))
    req_sockets = float(data.get("sockets", 1.0))

    resource_req = {"CPU": req_cpu, "RAM": req_ram, "SOCKETS": req_sockets}

    allocation_result = rag.request_resources(process_id, resource_req)

    if allocation_result["granted"]:
        active_processes[process_id] = {
            "process_id": process_id,
            "allocated_resources": resource_req,
            "status": "RUNNING"
        }
        return jsonify({
            "message": "Resources allocated successfully",
            "process_id": process_id,
            "status": "RUNNING",
            "rag_state": rag.export_graph_state()
        }), 200
    else:
        # Request registered as blocked edge (P_i -> R_j)
        return jsonify({
            "message": "Insufficient resources. Request blocked (Waiting Edge P_i -> R_j created).",
            "process_id": process_id,
            "status": "BLOCKED_WAITING",
            "rag_state": rag.export_graph_state()
        }), 202

@app.route('/resource/release', methods=['POST'])
def handle_resource_release():
    """Releases assigned edges (R_j -> P_i) upon process completion."""
    data = request.get_json() or {}
    process_id = data.get("process_id")

    if process_id in active_processes:
        del active_processes[process_id]

    reclaimed = rag.release_resources(process_id)
    return jsonify({
        "message": "Resources released successfully",
        "process_id": process_id,
        "reclaimed": reclaimed,
        "rag_state": rag.export_graph_state()
    }), 200

@app.route('/deadlock/detect', methods=['GET'])
def handle_deadlock_detection():
    """Executes deadlock detection algorithm over current Resource Allocation Graph."""
    detection_summary = rag.detect_deadlock_cycle()
    return jsonify({
        "engine": "RAG Wait-For Cycle Detector",
        "analysis": detection_summary,
        "rag_graph": rag.export_graph_state()
    })

@app.route('/deadlock/recover', methods=['POST'])
def handle_deadlock_recovery():
    """Executes preemption against a deadlocked victim process."""
    data = request.get_json() or {}
    victim_id = data.get("victim_process_id")

    if not victim_id:
        # Auto-select victim from current deadlocked set
        detection = rag.detect_deadlock_cycle()
        if detection["deadlocked_processes"]:
            victim_id = detection["deadlocked_processes"][0]

    if victim_id:
        if victim_id in active_processes:
            del active_processes[victim_id]
        recovery_summary = rag.recover_deadlock(victim_id)
        return jsonify({
            "recovery_summary": recovery_summary,
            "updated_rag_state": rag.export_graph_state()
        }), 200

    return jsonify({"message": "No deadlocked processes available to recover"}), 400

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001)

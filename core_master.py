from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
WORKER_URL = "http://127.0.0.1:5001"

@app.route('/')
def index():
    return jsonify({
        "system": "Distributed Network Slicing System",
        "subsystem": "Concurrent Resource Allocation & Deadlock Engine",
        "status": "OPERATIONAL"
    })

@app.route('/api/resource/request', methods=['GET', 'POST'])
def proxy_resource_request():
    if request.method == 'GET':
        pid = request.args.get("process_id", "proc-01")
        cpu = float(request.args.get("cpu", 2.0))
        ram = float(request.args.get("ram", 2048.0))
        sockets = float(request.args.get("sockets", 2.0))
    else:
        data = request.get_json() or {}
        pid = data.get("process_id", "proc-01")
        cpu = float(data.get("cpu", 2.0))
        ram = float(data.get("ram", 2048.0))
        sockets = float(data.get("sockets", 2.0))

    payload = {"process_id": pid, "cpu": cpu, "ram": ram, "sockets": sockets}
    res = requests.post(f"{WORKER_URL}/resource/request", json=payload)
    return (res.text, res.status_code, res.headers.items())

@app.route('/api/deadlock/detect', methods=['GET'])
def proxy_deadlock_detect():
    res = requests.get(f"{WORKER_URL}/deadlock/detect")
    return (res.text, res.status_code, res.headers.items())

@app.route('/api/deadlock/recover', methods=['POST'])
def proxy_deadlock_recover():
    res = requests.post(f"{WORKER_URL}/deadlock/recover", json=request.get_json() or {})
    return (res.text, res.status_code, res.headers.items())

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)

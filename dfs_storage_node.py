import os
import time
import json
import hashlib
from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

STORAGE_DIR = "./dfs_block_store"
os.makedirs(STORAGE_DIR, exist_ok=True)

# Metrics
DFS_BYTES_STORED = Gauge('dfs_bytes_stored_total', 'Total Bytes Stored in Distributed File System')
DFS_BLOCKS_COUNT = Gauge('dfs_blocks_total', 'Total Data Blocks Stored')
DFS_OPS_COUNTER  = Counter('dfs_operations_total', 'Total DFS Operations', ['type', 'status'])
DFS_LATENCY      = Histogram('dfs_operation_latency_seconds', 'DFS I/O Operation Latency')

# ==========================================
# 1. DISTRIBUTED FILE SYSTEM (DFS) ENDPOINTS
# ==========================================
@app.route('/dfs/write', methods=['POST'])
def write_block():
    """
    Simulates NFS/HDFS Block Write:
    Calculates block hash, writes chunk data to disk, updates WAL.
    """
    start_time = time.time()
    data = request.json or {}
    filename = data.get("filename", f"log_{int(time.time())}.txt")
    content = data.get("content", "")

    block_id = hashlib.sha256(content.encode('utf-8')).hexdigest()[:12]
    file_path = os.path.join(STORAGE_DIR, f"{filename}.block")

    try:
        # Write Block Content
        with open(file_path, "a") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] BLOCK_HASH={block_id} | CONTENT={content}\n")

        # Update Storage Metrics
        total_bytes = sum(os.path.getsize(os.path.join(STORAGE_DIR, f)) for f in os.listdir(STORAGE_DIR))
        block_count = len(os.listdir(STORAGE_DIR))

        DFS_BYTES_STORED.set(total_bytes)
        DFS_BLOCKS_COUNT.set(block_count)
        DFS_OPS_COUNTER.labels(type="WRITE", status="SUCCESS").inc()
        DFS_LATENCY.observe(time.time() - start_time)

        return jsonify({
            "status": "STORED",
            "filename": filename,
            "block_id": block_id,
            "bytes_written": len(content),
            "total_dfs_blocks": block_count,
            "storage_path": file_path
        }), 200
    except Exception as e:
        DFS_OPS_COUNTER.labels(type="WRITE", status="FAILED").inc()
        return jsonify({"error": str(e)}), 500

@app.route('/dfs/read', methods=['POST'])
def read_block():
    """Reads stored block log entries from disk."""
    data = request.json or {}
    filename = data.get("filename", "")
    file_path = os.path.join(STORAGE_DIR, f"{filename}.block")

    if not os.path.exists(file_path):
        return jsonify({"error": "File/Block Not Found"}), 404

    with open(file_path, "r") as f:
        lines = f.readlines()

    DFS_OPS_COUNTER.labels(type="READ", status="SUCCESS").inc()
    return jsonify({"filename": filename, "block_entries": lines}), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    print("Starting Distributed Storage Node (DFS) on http://0.0.0.0:5005...")
    app.run(host='0.0.0.0', port=5005, debug=False)

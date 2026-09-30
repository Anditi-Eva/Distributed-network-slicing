from flask import Flask, jsonify
from node_manager import load_cluster_config, get_node

app = Flask(__name__)

@app.route('/')
def home():
    return "<h1>Master Node Controller Active</h1>"

@app.route('/api/nodes', methods=['GET'])
def list_nodes():
    return jsonify(load_cluster_config())

@app.route('/api/nodes/<node_id>', methods=['GET'])
def node_detail(node_id):
    node = get_node(node_id)
    if node:
        return jsonify(node)
    return jsonify({"error": "Node not found"}), 404

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)

import json
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'nodes.json')

def load_cluster_config():
    """Reads the node registry configuration."""
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Config file missing: {CONFIG_FILE}")
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_node(node_id):
    """Finds a specific node by ID."""
    config = load_cluster_config()
    for node in config.get("nodes", []):
        if node["node_id"] == node_id:
            return node
    return None

if __name__ == '__main__':
    config = load_cluster_config()
    print(f"=== Cluster: {config['cluster_id']} ===")
    for n in config['nodes']:
        print(f"[{n['role'].upper()}] ID: {n['node_id']} | IP: {n['ip_address']} | Max Slices: {n['resource_quotas']['max_slices']}")

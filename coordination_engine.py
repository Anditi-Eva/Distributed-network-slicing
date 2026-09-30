import time
import threading
import requests
from node_manager import load_cluster_config

class DistributedCoordinator:
    def __init__(self, node_id, priority):
        self.node_id = node_id
        self.priority = priority
        self.leader_id = None
        self.is_leader = False
        self.logical_clock = 0
        self.lock = threading.Lock()
        
    def tick_clock(self, incoming_clock=0):
        with self.lock:
            self.logical_clock = max(self.logical_clock, incoming_clock) + 1
            return self.logical_clock

    def start_election(self):
        """Executes the Bully Algorithm to declare a Leader based on highest priority."""
        print(f"[{self.node_id}] Triggering Leader Election (Priority: {self.priority})...")
        config = load_cluster_config()
        higher_nodes = []

        for node in config.get("nodes", []):
            if node.get("priority", 0) > self.priority:
                higher_nodes.append(node)

        # If no higher priority node exists, claim leadership
        if not higher_nodes:
            self.claim_leadership()
            return

        # Challenge higher priority nodes
        any_response = False
        for node in higher_nodes:
            port = node.get("agent_port") or node.get("port", 5001)
            url = f"http://{node['ip_address']}:{port}/election/challenge"
            try:
                res = requests.post(url, json={"challenger_id": self.node_id, "priority": self.priority}, timeout=1)
                if res.status_code == 200:
                    any_response = True
            except requests.exceptions.RequestException:
                pass

        if not any_response:
            self.claim_leadership()

    def claim_leadership(self):
        with self.lock:
            self.is_leader = True
            self.leader_id = self.node_id
            self.logical_clock += 1
            print(f" SUCCESS: Node [{self.node_id}] elected as LEADER at Logical Time L={self.logical_clock}")

        # Broadcast leadership claim to cluster
        config = load_cluster_config()
        for node in config.get("nodes", []):
            if node["node_id"] != self.node_id:
                port = node.get("agent_port") or node.get("port", 5001)
                url = f"http://{node['ip_address']}:{port}/election/victory"
                try:
                    requests.post(url, json={
                        "leader_id": self.node_id,
                        "logical_clock": self.logical_clock
                    }, timeout=1)
                except requests.exceptions.RequestException:
                    pass

coordinator = None

def init_coordinator(node_id, priority):
    global coordinator
    coordinator = DistributedCoordinator(node_id, priority)
    return coordinator

#!/usr/bin/env python3
"""
ICS 2403 - Milestone 7: Primary-Backup State Machine Replication
Manages synchronized state replication across nodes with leader failover.
"""
import time
import threading
import requests

class ReplicatedStateMachine:
    def __init__(self, node_id, peers, is_primary=False):
        self.node_id = node_id
        self.peers = dict(peers)
        self.is_primary = is_primary
        self.primary_id = node_id if is_primary else None
        self.state = {} # Key-value state store
        self.log = []   # Replicated WAL entries [(term, index, op, key, val)]
        self.term = 1
        self.lock = threading.RLock()

    def apply_operation(self, op, key, val):
        """
        Executed on Primary. Replicates to backups synchronously before committing.
        """
        with self.lock:
            if not self.is_primary:
                return {"success": False, "reason": "Not primary", "primary_id": self.primary_id}

            entry = {"term": self.term, "index": len(self.log) + 1, "op": op, "key": key, "val": val}
            
            # Replicate to backups
            acks = 1 # Primary self-ack
            for peer, url in self.peers.items():
                try:
                    r = requests.post(f"{url}/replication/append", json=entry, timeout=0.2)
                    if r.status_code == 200 and r.json().get("ack"):
                        acks += 1
                except Exception:
                    pass

            quorum = (len(self.peers) + 1) // 2 + 1
            if acks >= quorum:
                self._commit_local(entry)
                return {"success": True, "index": entry["index"], "quorum_achieved": acks}
            else:
                return {"success": False, "reason": "Quorum lost during replication", "acks": acks}

    def handle_append(self, entry):
        """
        Backup handler for state replication.
        """
        with self.lock:
            self._commit_local(entry)
            return {"ack": True, "index": entry["index"]}

    def _commit_local(self, entry):
        self.log.append(entry)
        op, k, v = entry["op"], entry["key"], entry["val"]
        if op == "SET":
            self.state[k] = v
        elif op == "DELETE" and k in self.state:
            del self.state[k]

    def promote_to_primary(self, new_term):
        with self.lock:
            self.is_primary = True
            self.primary_id = self.node_id
            self.term = new_term
            return {"promoted": self.node_id, "term": self.term}

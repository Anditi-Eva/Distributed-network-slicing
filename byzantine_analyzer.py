#!/usr/bin/env python3
"""
ICS 2403 - Milestone 7: Byzantine Fault Analyzer
Evaluates 3f+1 node bounds, arbitrary payload corruption/bit-flips, and signed state consensus.
"""
import hashlib
import json

class ByzantineAnalyzer:
    def __init__(self, total_nodes, max_faulty):
        self.n = total_nodes
        self.f = max_faulty

    def verify_byzantine_bounds(self):
        """
        Verifies if N >= 3f + 1 holds for Byzantine agreement.
        """
        required = 3 * self.f + 1
        return {
            "n": self.n,
            "f": self.f,
            "required_n": required,
            "is_safe": self.n >= required
        }

    @staticmethod
    def sign_message(node_id, data, secret="ics2403_key"):
        payload = json.dumps(data, sort_keys=True)
        signature = hashlib.sha256(f"{node_id}:{payload}:{secret}".encode()).hexdigest()
        return {"node_id": node_id, "data": data, "signature": signature}

    @staticmethod
    def verify_message(signed_msg, secret="ics2403_key"):
        node_id = signed_msg.get("node_id")
        data = signed_msg.get("data")
        sig = signed_msg.get("signature")
        payload = json.dumps(data, sort_keys=True)
        expected = hashlib.sha256(f"{node_id}:{payload}:{secret}".encode()).hexdigest()
        return sig == expected

    @staticmethod
    def evaluate_byzantine_consensus(votes):
        """
        votes: dict of {node_id: {"vote": "COMMIT"|"ABORT", "signed": dict}}
        Filters out invalid signatures / corrupt messages before tallying.
        """
        valid_votes = {}
        byzantine_nodes = []
        for nid, v in votes.items():
            if v.get("corrupted") or not ByzantineAnalyzer.verify_message(v.get("signed", {})):
                byzantine_nodes.append(nid)
            else:
                valid_votes[nid] = v["vote"]

        # Tally valid votes
        commits = sum(1 for v in valid_votes.values() if v == "COMMIT")
        aborts = sum(1 for v in valid_votes.values() if v == "ABORT")
        
        return {
            "valid_votes": valid_votes,
            "byzantine_nodes": byzantine_nodes,
            "commits": commits,
            "aborts": aborts,
            "consensus": "COMMIT" if commits > aborts else "ABORT" if aborts > 0 else "NO_CONSENSUS"
        }

import json
import os
import threading

WAL_FILE = "transaction_wal.log"
wal_lock = threading.Lock()

class TransactionWAL:
    def __init__(self, log_path=WAL_FILE):
        self.log_path = log_path
        if not os.path.exists(self.log_path):
            with open(self.log_path, "w") as f:
                f.write("")

    def log_event(self, tx_id, state, participants, details=None):
        """Appends a write-ahead log record to disk before executing state changes."""
        entry = {
            "tx_id": tx_id,
            "state": state,  # PREPARE, COMMIT, ABORT, COMMITTED, ABORTED
            "participants": participants,
            "details": details or {}
        }
        with wal_lock:
            with open(self.log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def recover_pending_transactions(self):
        """Scans the WAL on node startup to resolve uncommitted/in-flight transactions."""
        if not os.path.exists(self.log_path):
            return {}

        tx_history = {}
        with wal_lock:
            with open(self.log_path, "r") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line.strip())
                        tx_history[record["tx_id"]] = record

        pending_recovery = {}
        for tx_id, record in tx_history.items():
            # If transaction logged PREPARE but never completed COMMIT/ABORT
            if record["state"] in ["PREPARE", "COMMIT", "ABORT"]:
                pending_recovery[tx_id] = record

        return pending_recovery

#!/usr/bin/env python3
"""
Coordinator write-ahead log (Milestone 5).

Every protocol step is appended to disk BEFORE the corresponding messages are sent, so a
restarted coordinator can finish what a crashed one started.

Record states (coordinator side):
    BEGIN        transaction started, participant list recorded
    PRECOMMIT    3PC only: all voted YES, PRECOMMIT about to be sent
    COMMIT       decision = commit   (forced to disk before phase 2)
    ABORT        decision = abort    (forced to disk before phase 2)
    END          every participant acknowledged the decision; nothing left to do

A transaction is "pending" while its last record is not END.
Presumed abort: a pending transaction with no COMMIT decision is aborted on recovery.
"""
import json
import os
import threading
import time

WAL_FILE = "transaction_wal.log"
COMPLETE_STATES = ("END", "COMMITTED", "ABORTED")   # the last two: legacy state names


class TransactionWAL:
    def __init__(self, log_path=WAL_FILE, fsync=True):
        self.log_path = log_path
        self.fsync = fsync
        self._lock = threading.Lock()
        directory = os.path.dirname(os.path.abspath(log_path))
        os.makedirs(directory, exist_ok=True)
        self._file = open(self.log_path, "a")

    def log_event(self, tx_id, state, participants, details=None, force=True, protocol=None):
        """Append one record. force=True makes it durable (fsync) before returning."""
        entry = {"tx_id": tx_id, "state": state, "participants": participants,
                 "protocol": protocol, "details": details or {}, "t": time.time()}
        with self._lock:
            self._file.write(json.dumps(entry) + "\n")
            self._file.flush()
            if force and self.fsync:
                os.fsync(self._file.fileno())

    def _replay(self):
        history = {}
        with self._lock:
            self._file.flush()
        with open(self.log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:          # torn last line after a crash
                    continue
                tx = history.setdefault(rec["tx_id"], {"participants": None, "protocol": None,
                                                       "state": None, "details": {}})
                tx["state"] = rec["state"]
                if rec.get("participants"):
                    tx["participants"] = rec["participants"]
                if rec.get("protocol"):
                    tx["protocol"] = rec["protocol"]
                if rec.get("details"):
                    tx["details"].update(rec["details"])
        return history

    def recover_pending_transactions(self):
        """Return {tx_id: record} for every transaction whose last record is not END.
        TwoPhaseCommitCoordinator.recover() uses this to finish them."""
        return {tx: rec for tx, rec in self._replay().items()
                if rec["state"] not in COMPLETE_STATES}

    def history(self):
        return self._replay()

    def close(self):
        with self._lock:
            self._file.close()

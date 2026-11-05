#!/usr/bin/env python3
"""
Milestone 5 - Transaction coordinator: Two-Phase Commit (2PC) and Three-Phase Commit (3PC).
"""
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests
from prometheus_client import Counter, Gauge, Histogram

from transaction_wal import TransactionWAL
from tx_common import decide_from_states

TX_TOTAL = Counter('tx_coordinator_transactions_total', 'Transactions finished',
                   ['protocol', 'status'])
TX_LATENCY = Histogram('tx_coordinator_latency_seconds', 'End-to-end transaction latency',
                       ['protocol'])
TX_PENDING = Gauge('tx_coordinator_pending_decisions',
                   'Decisions not yet acknowledged by every participant')


class TwoPhaseCommitCoordinator:
    protocol = "2PC"

    def __init__(self, participants, wal=None, protocol=None, rpc_timeout=1.0,
                 retry_interval=0.5, crash_at=None):
        self.participants = dict(participants)
        self.wal = wal or TransactionWAL()
        self.protocol = protocol or self.protocol
        self.rpc_timeout = rpc_timeout
        self.retry_interval = retry_interval
        self.crash_at = crash_at
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=64)
        self.active = set()
        self.decisions = {}
        self.pending = {}
        self.recovering = False
        self.recovered = []

    def _call(self, pid, path, body, msgs=None):
        if msgs is not None:
            with self.lock:
                msgs["sent"] += 1
        try:
            r = requests.post(self.participants[pid] + path, json=body, timeout=self.rpc_timeout)
            try:
                reply = r.json()
            except ValueError:
                return None
            if msgs is not None:
                with self.lock:
                    msgs["recv"] += 1
            return reply
        except requests.exceptions.RequestException:
            return None

    def _broadcast(self, ids, path, body_for, msgs=None):
        futures = {pid: self.pool.submit(self._call, pid, path, body_for(pid), msgs) for pid in ids}
        return {pid: f.result() for pid, f in futures.items()}

    def _crash_if(self, point):
        if self.crash_at == point:
            os._exit(1)

    def execute(self, allocations, slice_id=None, protocol=None):
        protocol = protocol or self.protocol
        tx_id = f"tx-{uuid.uuid4().hex[:8]}"
        ids = list(allocations)
        msgs = {"sent": 0, "recv": 0}
        t0 = time.perf_counter()
        with self.lock:
            self.active.add(tx_id)

        self.wal.log_event(tx_id, "BEGIN", ids,
                           {"allocations": allocations, "slice_id": slice_id}, protocol=protocol)

        replies = self._broadcast(ids, "/tx/prepare", lambda pid: {
            "tx_id": tx_id, "alloc": allocations[pid], "participants": ids,
            "protocol": protocol}, msgs)
        votes = {pid: ("VOTE_COMMIT" if r and r.get("vote") == "VOTE_COMMIT" else "VOTE_ABORT")
                 for pid, r in replies.items()}
        all_yes = all(v == "VOTE_COMMIT" for v in votes.values())
        t1 = time.perf_counter()
        self._crash_if("after_votes")

        if protocol == "3PC" and all_yes:
            self.wal.log_event(tx_id, "PRECOMMIT", ids)
            self._broadcast(ids, "/tx/precommit", lambda pid: {"tx_id": tx_id}, msgs)
        t2 = time.perf_counter()

        decision = "COMMIT" if all_yes else "ABORT"
        self.wal.log_event(tx_id, decision, ids, {"decision": decision})
        with self.lock:
            self.decisions[tx_id] = decision
        self._crash_if("after_decision")

        path = "/tx/commit" if decision == "COMMIT" else "/tx/abort"
        acks = self._broadcast(ids, path, lambda pid: {"tx_id": tx_id}, msgs)
        unacked = {pid for pid, r in acks.items() if not (r and r.get("ack"))}
        if unacked:
            with self.lock:
                self.pending[tx_id] = {"decision": decision, "targets": unacked}
            TX_PENDING.set(len(self.pending))
        else:
            self.wal.log_event(tx_id, "END", ids, {"decision": decision})
        with self.lock:
            self.active.discard(tx_id)
        t3 = time.perf_counter()

        status = "COMMITTED" if decision == "COMMIT" else "ABORTED"
        TX_TOTAL.labels(protocol=protocol, status=status).inc()
        TX_LATENCY.labels(protocol=protocol).observe(t3 - t0)
        return {"tx_id": tx_id, "protocol": protocol, "status": status, "votes": votes,
                "latency_ms": round((t3 - t0) * 1000, 2),
                "prepare_ms": round((t1 - t0) * 1000, 2),
                "precommit_ms": round((t2 - t1) * 1000, 2),
                "decide_ms": round((t3 - t2) * 1000, 2),
                "messages": msgs["sent"] + msgs["recv"],
                "unacknowledged": sorted(unacked)}

    def start_background(self):
        threading.Thread(target=self._resend_loop, daemon=True).start()

    def _resend_loop(self):
        while True:
            time.sleep(self.retry_interval)
            if self.recovering:
                continue
            with self.lock:
                work = [(t, dict(i, targets=set(i["targets"]))) for t, i in self.pending.items()]
            for tx_id, info in work:
                path = "/tx/commit" if info["decision"] == "COMMIT" else "/tx/abort"
                for pid in info["targets"]:
                    r = self._call(pid, path, {"tx_id": tx_id})
                    if r and r.get("ack"):
                        with self.lock:
                            if tx_id in self.pending:
                                self.pending[tx_id]["targets"].discard(pid)
                with self.lock:
                    done = tx_id in self.pending and not self.pending[tx_id]["targets"]
                    if done:
                        self.pending.pop(tx_id)
                if done:
                    self.wal.log_event(tx_id, "END", [], {"decision": info["decision"]})
                TX_PENDING.set(len(self.pending))

    def decision_of(self, tx_id):
        with self.lock:
            if self.recovering:
                return "PENDING"
            if tx_id in self.decisions:
                return self.decisions[tx_id]
            if tx_id in self.active:
                return "PENDING"
        return "ABORT"

    def recover(self):
        self.recovering = True
        try:
            for tx_id, rec in self.wal.history().items():
                if rec["details"].get("decision"):
                    self.decisions[tx_id] = rec["details"]["decision"]
            for tx_id, rec in self.wal.recover_pending_transactions().items():
                ids = rec["participants"] or list(self.participants)
                protocol = rec["protocol"] or "2PC"
                decision = self.decisions.get(tx_id)
                if decision is None:
                    decision = self._decide_unfinished(tx_id, ids, protocol,
                                                        precommitted=(rec["state"] == "PRECOMMIT"))
                    self.wal.log_event(tx_id, decision, ids,
                                        {"decision": decision, "recovered": True})
                    self.decisions[tx_id] = decision
                self.pending[tx_id] = {"decision": decision, "targets": set(ids)}
                self.recovered.append({"tx_id": tx_id, "decision": decision, "protocol": protocol})
            TX_PENDING.set(len(self.pending))
        finally:
            self.recovering = False
        return list(self.recovered)

    def _decide_unfinished(self, tx_id, ids, protocol, precommitted, wait=10.0):
        if protocol != "3PC":
            return "ABORT"
        deadline = time.time() + wait
        while True:
            states = []
            for pid in ids:
                try:
                    r = requests.get(f"{self.participants[pid]}/tx/status/{tx_id}",
                                     params={"resolve": "1"}, timeout=self.rpc_timeout)
                    states.append(r.json().get("state"))
                except (requests.exceptions.RequestException, ValueError, KeyError):
                    states.append(None)
            decision = decide_from_states("3PC", states)
            if decision:
                return decision
            if time.time() > deadline:
                return "COMMIT" if precommitted else "ABORT"
            time.sleep(0.3)

    def state(self):
        with self.lock:
            return {"protocol": self.protocol, "active": sorted(self.active),
                    "pending": {t: {"decision": i["decision"], "targets": sorted(i["targets"])}
                                for t, i in self.pending.items()},
                    "recovering": self.recovering, "recovered": list(self.recovered),
                    "crash_at": self.crash_at}


class ThreePhaseCommitCoordinator(TwoPhaseCommitCoordinator):
    protocol = "3PC"

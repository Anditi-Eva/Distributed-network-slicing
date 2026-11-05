#!/usr/bin/env python3
"""
Milestone 5 - Transaction participant (a resource manager such as the Edge, Core or Cloud node).

Implements the participant side of Two-Phase Commit and Three-Phase Commit for one
network-slice allocation (CPU + RAM) and gives the node its ACID properties:

  Atomicity   all-or-nothing through the 2PC/3PC decision (the coordinator never commits unless
              every participant voted YES; an ABORT releases the reservation everywhere)
  Consistency reservations + allocations never exceed capacity (checked by /tx/audit)
  Isolation   phase 1 takes a no-wait reservation under a lock: a concurrent transaction that
              does not fit votes NO instead of waiting, so no deadlock and no oversubscription
  Durability  every state change is forced to the write-ahead log (fsync) BEFORE it is
              acknowledged; a restarted node rebuilds its state by replaying the log

Endpoints                 phase
  POST /tx/prepare         1  vote (READY = YES, reservation made durable first)
  POST /tx/precommit       2  3PC only
  POST /tx/commit          3  turn reservation into allocation
  POST /tx/abort           3  release reservation
  GET  /tx/status/<tx_id>     state query used by the termination protocol
  GET  /tx/audit            invariants + lists for the consistency analysis
  POST /chaos/tx            fault injection (vote_abort, crash_before_vote,
                            crash_after_prepare, delay)

Termination protocol (the recovery mechanism for an in-doubt participant):
  A participant that is READY/PRECOMMITTED for longer than --termination-timeout asks the
  coordinator for the decision. If the coordinator is unreachable it asks the other participants
  and applies tx_common.decide_from_states(): under 2PC it may stay BLOCKED, under 3PC it can
  decide on its own. A restarted participant does the same after replaying its log.
"""
import argparse
import logging
import os
import threading
import time

import requests
from flask import Flask, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from transaction_wal import TransactionWAL
from tx_common import ABORTED, COMMITTED, IN_DOUBT, INIT, PRECOMMITTED, READY, decide_from_states

VOTES = Counter('tx_participant_votes_total', 'Phase-1 votes cast', ['vote'])
OUTCOMES = Counter('tx_participant_outcomes_total', 'Final outcomes applied', ['outcome', 'via'])
IN_DOUBT_GAUGE = Gauge('tx_participant_in_doubt', 'Transactions prepared but not yet resolved')
BLOCKED_SECONDS = Histogram('tx_participant_in_doubt_seconds',
                            'Time a transaction stayed in doubt before it was resolved',
                            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60))


class Participant:
    def __init__(self, node_id, peers, coordinator_url, cpu, ram, wal_path,
                 fsync=True, termination_timeout=1.0, rpc_timeout=0.5):
        self.node_id = node_id
        self.peers = {k: v for k, v in peers.items() if k != node_id}
        self.coordinator_url = coordinator_url
        self.total_cpu, self.total_ram = float(cpu), float(ram)
        self.termination_timeout = termination_timeout
        self.rpc_timeout = rpc_timeout
        self.lock = threading.RLock()
        self.chaos = {"mode": None, "value": 0}
        self.anomalies = 0                      # an outcome that contradicted a final state
        self.resolved_log = []                  # [{tx_id, outcome, via, in_doubt_seconds}]
        self.wal = TransactionWAL(wal_path, fsync=fsync)
        self.tx = {}
        self._recover()
        IN_DOUBT_GAUGE.set(len(self._ids(IN_DOUBT)))

    # ------------------------------------------------------------------ helpers
    def _recover(self):
        """Durability: rebuild the in-memory state by replaying the write-ahead log."""
        now = time.time()
        for tx_id, rec in self.wal.history().items():
            self.tx[tx_id] = {"state": rec["state"],
                              "alloc": rec["details"].get("alloc", {}),
                              "participants": rec["participants"] or [],
                              "protocol": rec["protocol"] or "2PC",
                              "since": now}              # in-doubt clock restarts on recovery

    def _ids(self, states):
        return [t for t, v in self.tx.items() if v["state"] in states]

    def _used(self, states):
        cpu = sum(self.tx[t]["alloc"].get("cpu", 0) for t in self._ids(states))
        ram = sum(self.tx[t]["alloc"].get("ram", 0) for t in self._ids(states))
        return cpu, ram

    def _fits(self, alloc):
        r_cpu, r_ram = self._used(IN_DOUBT)
        a_cpu, a_ram = self._used((COMMITTED,))
        return (alloc.get("cpu", 0) <= self.total_cpu - r_cpu - a_cpu + 1e-9 and
                alloc.get("ram", 0) <= self.total_ram - r_ram - a_ram + 1e-9)

    def _log(self, tx_id, state, participants=None, details=None, protocol=None):
        self.wal.log_event(tx_id, state, participants or [], details, protocol=protocol)

    # ------------------------------------------------------------------ protocol steps
    def prepare(self, data):
        """Phase 1. Returns (reply, crash_after_reply)."""
        tx_id, alloc = data["tx_id"], data.get("alloc", {})
        mode = self.chaos.get("mode")
        if mode == "delay":
            time.sleep(self.chaos.get("value", 0) / 1000.0)
        if mode == "crash_before_vote":
            os._exit(1)
        with self.lock:
            known = self.tx.get(tx_id)
            if known:                                           # retransmitted PREPARE
                yes = known["state"] != ABORTED
                return {"vote": "VOTE_COMMIT" if yes else "VOTE_ABORT",
                        "state": known["state"]}, False
            if mode == "vote_abort" or not self._fits(alloc):
                self._log(tx_id, ABORTED, data.get("participants"), protocol=data.get("protocol"))
                self.tx[tx_id] = {"state": ABORTED, "alloc": {}, "since": time.time(),
                                  "participants": data.get("participants", []),
                                  "protocol": data.get("protocol", "2PC")}
                VOTES.labels(vote="abort").inc()
                return {"vote": "VOTE_ABORT",
                        "reason": "forced" if mode == "vote_abort" else "insufficient capacity"}, False
            # force the READY record to disk BEFORE voting YES (the participant's promise)
            self._log(tx_id, READY, data.get("participants"), {"alloc": alloc},
                      protocol=data.get("protocol", "2PC"))
            self.tx[tx_id] = {"state": READY, "alloc": alloc, "since": time.time(),
                              "participants": data.get("participants", []),
                              "protocol": data.get("protocol", "2PC")}
            IN_DOUBT_GAUGE.set(len(self._ids(IN_DOUBT)))
            VOTES.labels(vote="commit").inc()
        return {"vote": "VOTE_COMMIT", "state": READY}, mode == "crash_after_prepare"

    def precommit(self, data):
        """3PC phase 2."""
        with self.lock:
            t = self.tx.get(data["tx_id"])
            if not t:
                return {"ack": False, "state": INIT}
            if t["state"] == READY:
                self._log(data["tx_id"], PRECOMMITTED)
                t["state"] = PRECOMMITTED
            return {"ack": t["state"] in (PRECOMMITTED, COMMITTED), "state": t["state"]}

    def apply(self, tx_id, outcome, via="coordinator"):
        """Final step: outcome is 'COMMIT' or 'ABORT'. Idempotent. Returns (ok, state)."""
        with self.lock:
            t = self.tx.get(tx_id)
            if outcome == "COMMIT":
                if not t:
                    return False, INIT
                if t["state"] == COMMITTED:
                    return True, COMMITTED
                if t["state"] == ABORTED:
                    self.anomalies += 1
                    return False, ABORTED
                self._log(tx_id, COMMITTED)
                final = COMMITTED
            else:
                if not t:                                       # poison: refuse a late PREPARE
                    self._log(tx_id, ABORTED)
                    self.tx[tx_id] = {"state": ABORTED, "alloc": {}, "since": time.time(),
                                      "participants": [], "protocol": "2PC"}
                    return True, ABORTED
                if t["state"] == ABORTED:
                    return True, ABORTED
                if t["state"] == COMMITTED:
                    self.anomalies += 1
                    return False, COMMITTED
                self._log(tx_id, ABORTED)
                final = ABORTED
            in_doubt_for = time.time() - t["since"]
            t["state"] = final
            self.resolved_log.append({"tx_id": tx_id, "outcome": outcome, "via": via,
                                      "in_doubt_seconds": round(in_doubt_for, 3)})
            OUTCOMES.labels(outcome=final, via=via).inc()
            BLOCKED_SECONDS.observe(in_doubt_for)
            IN_DOUBT_GAUGE.set(len(self._ids(IN_DOUBT)))
            return True, final

    def status(self, tx_id, resolve=False):
        with self.lock:
            t = self.tx.get(tx_id)
            if t:
                return t["state"]
            if resolve:                                         # never voted: make that permanent
                self.apply(tx_id, "ABORT", via="poison")
            return INIT

    # ------------------------------------------------------------------ termination protocol
    def start_termination_thread(self):
        threading.Thread(target=self._termination_loop, daemon=True).start()

    def _termination_loop(self):
        while True:
            time.sleep(0.2)
            if self.termination_timeout <= 0:
                continue
            now = time.time()
            with self.lock:
                due = [(t, dict(v)) for t, v in self.tx.items()
                       if v["state"] in IN_DOUBT and now - v["since"] >= self.termination_timeout]
            for tx_id, info in due:
                try:
                    self._resolve(tx_id, info)
                except Exception:               # never let the recovery thread die
                    pass

    def _resolve(self, tx_id, info):
        # 1) ask the coordinator
        try:
            r = requests.get(f"{self.coordinator_url}/tx/decision/{tx_id}", timeout=self.rpc_timeout)
            if r.status_code == 200:
                d = r.json().get("decision")
                if d in ("COMMIT", "ABORT"):
                    self.apply(tx_id, d, via="coordinator")
                    return
                if d == "PENDING":              # coordinator is alive and still working on it
                    return
        except requests.exceptions.RequestException:
            pass
        # 2) coordinator unreachable: cooperative termination with the other participants
        states = [info["state"]]
        for pid in info.get("participants", []):
            if pid == self.node_id or pid not in self.peers:
                continue
            try:
                r = requests.get(f"{self.peers[pid]}/tx/status/{tx_id}",
                                 params={"resolve": "1"}, timeout=self.rpc_timeout)
                states.append(r.json().get("state") if r.status_code == 200 else None)
            except (requests.exceptions.RequestException, ValueError):
                states.append(None)
        decision = decide_from_states(info.get("protocol", "2PC"), states)
        if decision:
            self.apply(tx_id, decision, via="termination")
        # else: BLOCKED - keep the reservation and retry on the next tick

    # ------------------------------------------------------------------ audit
    def audit(self):
        with self.lock:
            r_cpu, r_ram = self._used(IN_DOUBT)
            a_cpu, a_ram = self._used((COMMITTED,))
            wal_states = {t: v["state"] for t, v in self.wal.history().items()}
            mem_states = {t: v["state"] for t, v in self.tx.items()}
            return {
                "node": self.node_id,
                "total_cpu": self.total_cpu, "total_ram": self.total_ram,
                "allocated_cpu": a_cpu, "allocated_ram": a_ram,
                "reserved_cpu": r_cpu, "reserved_ram": r_ram,
                "no_oversubscription": (a_cpu + r_cpu <= self.total_cpu + 1e-9 and
                                        a_ram + r_ram <= self.total_ram + 1e-9),
                "wal_consistent": wal_states == mem_states,
                "committed": self._ids((COMMITTED,)),
                "aborted": self._ids((ABORTED,)),
                "in_doubt": self._ids(IN_DOUBT),
                "anomalies": self.anomalies,
                "resolved_log": list(self.resolved_log),
            }


def create_app(p):
    app = Flask(__name__)

    @app.route("/tx/prepare", methods=["POST"])
    def prepare():
        reply, crash_after = p.prepare(request.get_json(force=True))
        resp = jsonify(reply)
        if crash_after:
            resp.call_on_close(lambda: os._exit(1))     # real crash right after voting YES
        return resp

    @app.route("/tx/precommit", methods=["POST"])
    def precommit():
        return jsonify(p.precommit(request.get_json(force=True)))

    @app.route("/tx/commit", methods=["POST"])
    def commit():
        ok, state = p.apply(request.get_json(force=True)["tx_id"], "COMMIT")
        return jsonify({"ack": ok, "state": state}), (200 if ok else 409)

    @app.route("/tx/abort", methods=["POST"])
    def abort():
        ok, state = p.apply(request.get_json(force=True)["tx_id"], "ABORT")
        return jsonify({"ack": ok, "state": state}), (200 if ok else 409)

    @app.route("/tx/status/<tx_id>")
    def status(tx_id):
        return jsonify({"state": p.status(tx_id, resolve=request.args.get("resolve") == "1")})

    @app.route("/tx/audit")
    def audit():
        return jsonify(p.audit())

    @app.route("/chaos/tx", methods=["POST"])
    def chaos():
        d = request.get_json(silent=True) or {}
        mode = d.get("mode")
        p.chaos = {"mode": None if mode in (None, "reset") else mode, "value": d.get("value", 0)}
        return jsonify(p.chaos)

    @app.route("/health")
    def health():
        return jsonify({"status": "UP", "node": p.node_id})

    @app.route("/metrics")
    def metrics():
        return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    return app


def parse_cluster(spec):
    """'edge:127.0.0.1:6201,core:127.0.0.1:6202' -> {'edge': 'http://127.0.0.1:6201', ...}"""
    out = {}
    for item in spec.split(","):
        node_id, host, port = item.strip().split(":")
        out[node_id] = f"http://{host}:{port}"
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Milestone 5 transaction participant")
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--cluster", required=True, help="id:host:port,... for ALL participants")
    ap.add_argument("--coordinator", required=True, help="e.g. http://127.0.0.1:6200")
    ap.add_argument("--cpu", type=float, default=4.0)
    ap.add_argument("--ram", type=float, default=4096.0)
    ap.add_argument("--wal", default=None, help="write-ahead log path (default ./<id>_participant.wal)")
    ap.add_argument("--no-fsync", action="store_true")
    ap.add_argument("--termination-timeout", type=float, default=1.0,
                    help="seconds in doubt before the termination protocol starts (0 = off)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    if a.quiet:
        logging.getLogger("werkzeug").setLevel(logging.ERROR)
    part = Participant(a.id, parse_cluster(a.cluster), a.coordinator, a.cpu, a.ram,
                       a.wal or f"{a.id}_participant.wal", fsync=not a.no_fsync,
                       termination_timeout=a.termination_timeout)
    part.start_termination_thread()
    print(f"Participant {a.id} on port {a.port} (cpu={a.cpu}, ram={a.ram}, "
          f"recovered {len(part.tx)} transactions, {len(part._ids(IN_DOUBT))} in doubt)")
    create_app(part).run(host="0.0.0.0", port=a.port, debug=False, threaded=True)

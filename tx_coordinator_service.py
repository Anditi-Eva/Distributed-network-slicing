#!/usr/bin/env python3
"""
HTTP service around the transaction coordinator (Milestone 5).

  POST /tx/begin               {"allocations": {"edge": {"cpu": 1, "ram": 256}, ...},
                                "slice_id": "urllc-01", "protocol": "2PC" | "3PC"}
  GET  /tx/decision/<tx_id>   COMMIT | ABORT | PENDING   (asked by in-doubt participants)
  GET  /coordinator/state     pending decisions, recovery status
  POST /coordinator/chaos     {"crash_at": "after_votes" | "after_decision" | null}
  GET  /health, /metrics

On every start the coordinator replays its write-ahead log and finishes unfinished transactions.

Example (3 participants on one machine):
  python3 tx_coordinator_service.py --port 6200 \
      --participants edge:127.0.0.1:6201,core:127.0.0.1:6202,cloud:127.0.0.1:6203
"""
import argparse
import logging
import threading

from flask import Flask, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from transaction_participant import parse_cluster
from transaction_wal import TransactionWAL
from two_phase_coordinator import TwoPhaseCommitCoordinator


def create_app(co):
    app = Flask(__name__)

    @app.route("/tx/begin", methods=["POST"])
    def begin():
        d = request.get_json(force=True)
        return jsonify(co.execute(d["allocations"], d.get("slice_id"), d.get("protocol")))

    @app.route("/tx/decision/<tx_id>")
    def decision(tx_id):
        return jsonify({"tx_id": tx_id, "decision": co.decision_of(tx_id)})

    @app.route("/coordinator/state")
    def state():
        return jsonify(co.state())

    @app.route("/coordinator/chaos", methods=["POST"])
    def chaos():
        co.crash_at = (request.get_json(silent=True) or {}).get("crash_at")
        return jsonify({"crash_at": co.crash_at})

    @app.route("/health")
    def health():
        return jsonify({"status": "UP", "recovering": co.recovering})

    @app.route("/metrics")
    def metrics():
        return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    return app


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Milestone 5 transaction coordinator")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--participants", required=True, help="id:host:port,...")
    ap.add_argument("--wal", default="coordinator.wal")
    ap.add_argument("--protocol", default="2PC", choices=["2PC", "3PC"])
    ap.add_argument("--no-fsync", action="store_true")
    ap.add_argument("--rpc-timeout", type=float, default=1.0)
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    if a.quiet:
        logging.getLogger("werkzeug").setLevel(logging.ERROR)

    coordinator = TwoPhaseCommitCoordinator(parse_cluster(a.participants),
                                            TransactionWAL(a.wal, fsync=not a.no_fsync),
                                            protocol=a.protocol, rpc_timeout=a.rpc_timeout)
    coordinator.start_background()
    # recover in the background so the service answers /health (participants see PENDING meanwhile)
    coordinator.recovering = True
    threading.Thread(target=coordinator.recover, daemon=True).start()
    print(f"Coordinator on port {a.port}, default protocol {a.protocol}")
    create_app(coordinator).run(host="0.0.0.0", port=a.port, debug=False, threaded=True)

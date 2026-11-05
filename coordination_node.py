"""
Standalone coordination node (Milestone 4).

One process = one node N_i of the coordination cluster.

Example: a 3-node cluster on one machine (run each in its own terminal)

  python3 coordination_node.py --id N1 --port 6101 --priority 1 --cluster N1:6101:1,N2:6102:2,N3:6103:3
  python3 coordination_node.py --id N2 --port 6102 --priority 2 --cluster N1:6101:1,N2:6102:2,N3:6103:3
  python3 coordination_node.py --id N3 --port 6103 --priority 3 --cluster N1:6101:1,N2:6102:2,N3:6103:3

then:  curl -X POST localhost:6101/election/trigger     (N3 becomes leader)
       curl localhost:6101/coordination/state
"""
import argparse
import logging

from flask import Flask
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from coordination_engine import register_coordination


def parse_cluster(spec, host):
    """'N1:6101:1,N2:6102:2' -> [{'node_id','url','priority'}]"""
    peers = []
    for item in spec.split(","):
        node_id, port, priority = item.strip().split(":")
        peers.append({"node_id": node_id, "url": f"http://{host}:{port}",
                      "priority": float(priority)})
    return peers


def create_app(node_id, priority, peers, **kwargs):
    app = Flask(__name__)
    co = register_coordination(app, node_id, priority, peers, **kwargs)

    @app.route("/metrics")
    def metrics():
        return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    @app.route("/health")
    def health():
        return {"status": "UP", "node": node_id}

    return app, co


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Milestone 4 coordination node")
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--priority", type=float, required=True)
    ap.add_argument("--cluster", required=True, help="comma list of id:port:priority")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--monitor-interval", type=float, default=0.0,
                    help="leader heartbeat period in seconds (0 = failure detector off)")
    ap.add_argument("--monitor-misses", type=int, default=3)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.quiet:
        logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app, _ = create_app(args.id, args.priority, parse_cluster(args.cluster, args.host),
                        monitor_interval=args.monitor_interval,
                        monitor_misses=args.monitor_misses)
    print(f"Coordination node {args.id} (priority {args.priority}) on port {args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False, threaded=True)

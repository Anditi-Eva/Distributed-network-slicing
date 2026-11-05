"""
Milestone 4 - Distributed Algorithms and Coordination
=====================================================
Implements, on top of plain HTTP/JSON messaging:

  1. Leader election       : Bully algorithm (highest priority alive node wins)
  2. Logical clocks        : Lamport clock, piggybacked on EVERY inter-node message
  3. Event ordering        : happens-before via Lamport timestamps, total order
                             broken by (timestamp, node_id)
  4. Failure detection     : heartbeat monitor that triggers re-election when the
                             leader stops answering
  5. Instrumentation       : per-type message counters, bytes sent, election
                             duration, Prometheus metrics

Model:  N1..Nn  ->  Coordination Protocol (Bully + Lamport)  ->  Consistent State
        (consistent state = every live node agrees on the same single leader)

The module can be used in two ways:
  * standalone, via coordination_node.py (one process per node, used by the
    experiment suite), or
  * mounted into an existing Flask service (e.g. worker_agent.py) with
    register_coordination(app, node_id, priority, peers).

Message types counted for complexity analysis:
    ELECTION    challenge sent to every higher-priority node
    OK          answer from a higher-priority node (the HTTP reply to ELECTION)
    COORDINATOR victory announcement sent to every other node
HEARTBEAT and APP (application / event-ordering) messages are counted separately
so they never pollute the election complexity figures.
"""
import json
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Blueprint, jsonify, request
from prometheus_client import Counter, Gauge, Histogram

# ----------------------------------------------------------------------------
# Prometheus metrics (default registry, so they appear on the service /metrics)
# ----------------------------------------------------------------------------
COORD_MESSAGES = Counter('coordination_messages_total',
                         'Coordination messages sent', ['type'])
COORD_CLOCK = Gauge('coordination_lamport_clock', 'Current Lamport clock value')
COORD_IS_LEADER = Gauge('coordination_is_leader', '1 if this node is the leader')
COORD_ELECTIONS = Counter('coordination_elections_total',
                          'Elections started by this node', ['reason'])
COORD_ELECTION_SECONDS = Histogram('coordination_election_duration_seconds',
                                   'Time from election start to leader known')

ELECTION_TYPES = ("ELECTION", "OK", "COORDINATOR")


class DistributedCoordinator:
    def __init__(self, node_id, priority, peers, rpc_timeout=0.5,
                 coord_timeout=1.5, cooldown=1.0, monitor_interval=0.0,
                 monitor_misses=3, max_retries=3):
        """
        peers: list of {"node_id": str, "url": str, "priority": number}
               (self may be included; it is filtered out)
        monitor_interval: seconds between leader heartbeats (0 = monitor off)
        monitor_misses:   consecutive missed heartbeats before the leader is
                         declared dead (mirrors the 3-miss rule of Milestone 7)
        cooldown:         seconds during which a freshly announced leader is
                         considered authoritative, so late/duplicate challenges
                         do not trigger redundant elections
        """
        self.node_id = node_id
        self.priority = priority
        self.peers = [p for p in peers if p["node_id"] != node_id]
        self.peer_by_id = {p["node_id"]: p for p in self.peers}
        self.rpc_timeout = rpc_timeout
        self.coord_timeout = coord_timeout
        self.cooldown = cooldown
        self.monitor_interval = monitor_interval
        self.monitor_misses = monitor_misses
        self.max_retries = max_retries

        self.lock = threading.RLock()
        self.logical_clock = 0
        self.leader_id = None
        self.leader_priority = None
        self.leader_since = 0.0
        self.is_leader = False
        self.in_election = False
        self.crashed = False
        self._leader_event = threading.Event()

        self.counts = {t: 0 for t in ("ELECTION", "OK", "COORDINATOR", "HEARTBEAT", "APP")}
        self.bytes_sent = 0
        self.election_started_at = None
        self.last_election_seconds = None
        self.last_election_reason = None
        self.failure_detected_at = None
        self.elections_started = 0
        self.event_log = []                 # application events (event ordering demo)
        self.trace = deque(maxlen=2000)     # coordination message trace
        self._monitor_thread = None

    # ------------------------------------------------------------------
    # Lamport logical clock
    # ------------------------------------------------------------------
    def tick_clock(self, incoming=0):
        """Receive/local event rule: L = max(L, L_msg) + 1."""
        with self.lock:
            self.logical_clock = max(self.logical_clock, incoming) + 1
            COORD_CLOCK.set(self.logical_clock)
            return self.logical_clock

    def _stamp_send(self):
        """Send event rule: L = L + 1; the new value is the message timestamp."""
        with self.lock:
            self.logical_clock += 1
            COORD_CLOCK.set(self.logical_clock)
            return self.logical_clock

    # ------------------------------------------------------------------
    # Messaging helpers
    # ------------------------------------------------------------------
    def _count(self, msg_type, nbytes):
        with self.lock:
            self.counts[msg_type] += 1
            self.bytes_sent += nbytes
        COORD_MESSAGES.labels(type=msg_type).inc()

    def _trace(self, direction, msg_type, peer, ts):
        self.trace.append({"node": self.node_id, "dir": direction, "type": msg_type,
                           "peer": peer, "lamport": ts})

    def _post(self, peer, path, body, msg_type, stamp=None, merge_reply=True):
        """Send one message carrying the Lamport timestamp. Returns reply JSON or None."""
        body = dict(body)
        body["from"] = self.node_id
        body["priority"] = self.priority
        body["lamport"] = stamp if stamp is not None else self._stamp_send()
        payload = json.dumps(body)
        self._count(msg_type, len(payload))
        self._trace("send", msg_type, peer["node_id"], body["lamport"])
        try:
            r = requests.post(peer["url"] + path, data=payload,
                              headers={"Content-Type": "application/json"},
                              timeout=self.rpc_timeout)
            if r.status_code != 200:
                return None
            reply = r.json()
            if merge_reply and "lamport" in reply:
                self.tick_clock(reply["lamport"])
            return reply
        except (requests.exceptions.RequestException, ValueError):
            return None

    # ------------------------------------------------------------------
    # Bully election
    # ------------------------------------------------------------------
    def _claim_election(self, reason, skip_if_resolved=False):
        """Atomically claim the right to run an election (check-and-set under the lock).
        Prevents two concurrent challenges from both launching an election."""
        with self.lock:
            if self.in_election or self.crashed:
                return False
            if skip_if_resolved and self.leader_id is not None and \
                    time.time() - self.leader_since < self.cooldown:
                return False                     # a fresh leader is already known
            self.in_election = True
            self.election_started_at = time.time()
            self.last_election_reason = reason
            self.elections_started += 1
            self.leader_id = None
            self.leader_priority = None
            self.is_leader = False
            self._leader_event.clear()
        COORD_ELECTIONS.labels(reason=reason).inc()
        COORD_IS_LEADER.set(0)
        return True

    def start_election(self, reason="manual"):
        """Run the Bully algorithm. Returns True if this call ran an election."""
        if not self._claim_election(reason):
            return False
        self._run_election()
        return True

    def _run_election(self):
        """Body of the Bully algorithm; the caller has already claimed the election."""
        for _ in range(self.max_retries):
            higher = [p for p in self.peers if p["priority"] > self.priority]
            if not higher:
                self._declare_victory()
                return

            with ThreadPoolExecutor(max_workers=len(higher)) as pool:
                replies = list(pool.map(
                    lambda p: self._post(p, "/election/challenge", {}, "ELECTION"), higher))
            got_ok = any(r and r.get("answer") == "OK" for r in replies)

            if not got_ok:                       # nobody higher is alive
                self._declare_victory()
                return

            # A higher node answered OK and runs its own election: wait for it
            if self._leader_event.wait(self.coord_timeout):
                return
            # Higher node died before announcing: try again
        # give up gracefully
        with self.lock:
            self.in_election = False

    def _declare_victory(self):
        with self.lock:
            self.is_leader = True
            self.leader_id = self.node_id
            self.leader_priority = self.priority
            self.leader_since = time.time()
            self.in_election = False
            if self.election_started_at:
                self.last_election_seconds = time.time() - self.election_started_at
                COORD_ELECTION_SECONDS.observe(self.last_election_seconds)
            self._leader_event.set()
        COORD_IS_LEADER.set(1)
        stamp = self.tick_clock()
        targets = list(self.peers)
        if targets:
            with ThreadPoolExecutor(max_workers=len(targets)) as pool:
                list(pool.map(lambda p: self._post(
                    p, "/election/victory", {"leader_id": self.node_id},
                    "COORDINATOR"), targets))
        return stamp

    # --- handlers (called by the HTTP routes) --------------------------
    def on_challenge(self, data):
        ts = self.tick_clock(data.get("lamport", 0))
        self._trace("recv", "ELECTION", data.get("from"), ts)
        sender_priority = data.get("priority", 0)
        if sender_priority >= self.priority:
            return {"answer": "IGNORE", "lamport": self.logical_clock}

        # Claim atomically here (not inside the spawned thread) so that concurrent
        # challenges cannot each launch their own election.
        if self._claim_election("challenged", skip_if_resolved=True):
            threading.Thread(target=self._run_election, daemon=True).start()
        stamp = self._stamp_send()
        self._count("OK", 0)
        self._trace("send", "OK", data.get("from"), stamp)
        return {"answer": "OK", "lamport": stamp}

    def on_victory(self, data):
        ts = self.tick_clock(data.get("lamport", 0))
        self._trace("recv", "COORDINATOR", data.get("from"), ts)
        sender_priority = data.get("priority", 0)
        if sender_priority < self.priority:
            # Bully rule: a lower-priority node cannot be leader while I am alive
            threading.Thread(target=self.start_election, args=("usurper",),
                             daemon=True).start()
            return {"accepted": False, "lamport": self.logical_clock}
        with self.lock:
            self.leader_id = data.get("leader_id", data.get("from"))
            self.leader_priority = sender_priority
            self.leader_since = time.time()
            self.is_leader = (self.leader_id == self.node_id)
            if self.election_started_at and self.in_election:
                self.last_election_seconds = time.time() - self.election_started_at
                COORD_ELECTION_SECONDS.observe(self.last_election_seconds)
            self.in_election = False
            self._leader_event.set()
        COORD_IS_LEADER.set(1 if self.is_leader else 0)
        return {"accepted": True, "lamport": self.logical_clock}

    # ------------------------------------------------------------------
    # Failure detector (heartbeat monitor)
    # ------------------------------------------------------------------
    def start_monitor(self):
        if self.monitor_interval <= 0 or self._monitor_thread:
            return
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _monitor_loop(self):
        misses = 0
        while True:
            time.sleep(self.monitor_interval)
            with self.lock:
                leader, busy, down = self.leader_id, self.in_election, self.crashed
            if down or busy or leader is None or leader == self.node_id:
                misses = 0
                continue
            peer = self.peer_by_id.get(leader)
            reply = self._post(peer, "/coordination/ping", {}, "HEARTBEAT") if peer else None
            if reply:
                misses = 0
                continue
            misses += 1
            if misses >= self.monitor_misses:
                misses = 0
                self.failure_detected_at = time.time()
                self.start_election("leader_failure")

    # ------------------------------------------------------------------
    # Event ordering with Lamport timestamps (application messages)
    # ------------------------------------------------------------------
    def _log_event(self, kind, ts, peer=None, msg_id=None, label=""):
        with self.lock:
            self.event_log.append({"seq": len(self.event_log), "node": self.node_id,
                                   "kind": kind, "lamport": ts, "peer": peer,
                                   "msg_id": msg_id, "label": label,
                                   "wall": time.time()})

    def local_event(self, label=""):
        ts = self.tick_clock()
        self._log_event("local", ts, label=label)
        return ts

    def app_send(self, to, label=""):
        peer = self.peer_by_id.get(to)
        if not peer:
            return {"error": f"unknown peer {to}"}
        msg_id = uuid.uuid4().hex[:10]
        ts = self._stamp_send()
        self._log_event("send", ts, peer=to, msg_id=msg_id, label=label)
        reply = self._post(peer, "/message", {"msg_id": msg_id, "label": label},
                           "APP", stamp=ts, merge_reply=False)
        return {"msg_id": msg_id, "send_ts": ts,
                "recv_ts": reply.get("recv_ts") if reply else None,
                "delivered": reply is not None}

    def on_message(self, data):
        ts = self.tick_clock(data.get("lamport", 0))
        self._log_event("recv", ts, peer=data.get("from"),
                        msg_id=data.get("msg_id"), label=data.get("label", ""))
        return {"recv_ts": ts, "lamport": ts}

    # ------------------------------------------------------------------
    # State / control
    # ------------------------------------------------------------------
    def state(self, with_log=False):
        with self.lock:
            s = {"node_id": self.node_id, "priority": self.priority,
                 "leader_id": self.leader_id, "is_leader": self.is_leader,
                 "leader_since": self.leader_since,
                 "in_election": self.in_election, "lamport_clock": self.logical_clock,
                 "crashed": self.crashed, "counts": dict(self.counts),
                 "election_messages": sum(self.counts[t] for t in ELECTION_TYPES),
                 "bytes_sent": self.bytes_sent,
                 "elections_started": self.elections_started,
                 "last_election_seconds": self.last_election_seconds,
                 "last_election_reason": self.last_election_reason,
                 "failure_detected_at": self.failure_detected_at}
            if with_log:
                s["event_log"] = list(self.event_log)
                s["trace"] = list(self.trace)
            return s

    def reset(self):
        with self.lock:
            self.logical_clock = 0
            self.leader_id = self.leader_priority = None
            self.leader_since = 0.0
            self.is_leader = self.in_election = self.crashed = False
            self.counts = {k: 0 for k in self.counts}
            self.bytes_sent = 0
            self.election_started_at = self.last_election_seconds = None
            self.last_election_reason = self.failure_detected_at = None
            self.elections_started = 0
            self.event_log = []
            self.trace.clear()
            self._leader_event.clear()
        COORD_CLOCK.set(0)
        COORD_IS_LEADER.set(0)


# ----------------------------------------------------------------------------
# Flask integration
# ----------------------------------------------------------------------------
def build_blueprint(co):
    bp = Blueprint("coordination", __name__)
    always_allowed = ("/coordination/crash", "/coordination/reset", "/coordination/state")

    @bp.before_request
    def _simulate_crash():
        if co.crashed and request.path not in always_allowed:
            return jsonify({"status": "CRASHED"}), 503

    @bp.route("/election/challenge", methods=["POST"])
    def challenge():
        return jsonify(co.on_challenge(request.get_json(force=True) or {}))

    @bp.route("/election/victory", methods=["POST"])
    def victory():
        return jsonify(co.on_victory(request.get_json(force=True) or {}))

    @bp.route("/election/trigger", methods=["POST"])
    def trigger():
        data = request.get_json(silent=True) or {}
        if data.get("wait"):
            started = co.start_election("manual")
        else:
            threading.Thread(target=co.start_election, args=("manual",), daemon=True).start()
            started = True
        return jsonify({"triggered": started})

    @bp.route("/coordination/ping", methods=["POST"])
    def ping():
        data = request.get_json(silent=True) or {}
        co.tick_clock(data.get("lamport", 0))
        return jsonify({"status": "UP", "lamport": co.logical_clock})

    @bp.route("/coordination/state", methods=["GET"])
    def state():
        return jsonify(co.state(with_log=request.args.get("log") == "1"))

    @bp.route("/coordination/reset", methods=["POST"])
    def reset():
        co.reset()
        return jsonify({"reset": True})

    @bp.route("/coordination/crash", methods=["POST"])
    def crash():
        data = request.get_json(silent=True) or {}
        co.crashed = bool(data.get("crashed", True))
        return jsonify({"crashed": co.crashed})

    @bp.route("/event/local", methods=["POST"])
    def event_local():
        data = request.get_json(silent=True) or {}
        return jsonify({"node": co.node_id, "lamport": co.local_event(data.get("label", ""))})

    @bp.route("/event/send", methods=["POST"])
    def event_send():
        data = request.get_json(force=True) or {}
        return jsonify(co.app_send(data.get("to"), data.get("label", "")))

    @bp.route("/message", methods=["POST"])
    def message():
        return jsonify(co.on_message(request.get_json(force=True) or {}))

    return bp


def register_coordination(app, node_id, priority, peers, **kwargs):
    """Mount the coordination endpoints into an existing Flask app."""
    co = DistributedCoordinator(node_id, priority, peers, **kwargs)
    app.register_blueprint(build_blueprint(co))
    co.start_monitor()
    return co


# ----------------------------------------------------------------------------
# Backwards-compatible helpers (older code used load_cluster_config)
# ----------------------------------------------------------------------------
def peers_from_cluster_config(self_id):
    from node_manager import load_cluster_config
    peers = []
    for n in load_cluster_config().get("nodes", []):
        if n["node_id"] == self_id:
            continue
        port = n.get("agent_port") or n.get("port", 5001)
        peers.append({"node_id": n["node_id"],
                      "url": f"http://{n['ip_address']}:{port}",
                      "priority": n.get("priority", 0)})
    return peers


coordinator = None


def init_coordinator(node_id, priority, peers=None, **kwargs):
    global coordinator
    coordinator = DistributedCoordinator(
        node_id, priority, peers if peers is not None else peers_from_cluster_config(node_id),
        **kwargs)
    return coordinator

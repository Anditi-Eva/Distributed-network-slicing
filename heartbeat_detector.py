#!/usr/bin/env python3
"""
ICS 2403 - Milestone 7: Heartbeat Monitor & Failure Detector
Implements active heartbeating with sliding-window suspicion scores (Phi-Accrual style).
"""
import time
import threading
import math
import requests

class HeartbeatDetector:
    def __init__(self, node_id, peers, heartbeat_interval=0.2, suspicion_threshold=3.0):
        """
        peers: dict of {peer_id: base_url}
        """
        self.node_id = node_id
        self.peers = dict(peers)
        self.interval = heartbeat_interval
        self.threshold = suspicion_threshold
        self.lock = threading.RLock()
        
        # State tracking per peer
        self.last_seen = {p: time.time() for p in self.peers}
        self.history = {p: [] for p in self.peers}  # sliding window of intervals
        self.status = {p: "UP" for p in self.peers} # UP, SUSPECT, DOWN
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(self.interval)
            now = time.time()
            for peer, url in list(self.peers.items()):
                self._ping_peer(peer, url, now)

    def _ping_peer(self, peer, url, now):
        try:
            r = requests.get(f"{url}/health", timeout=0.15)
            if r.status_code == 200:
                with self.lock:
                    delta = now - self.last_seen[peer]
                    self.last_seen[peer] = now
                    self.history[peer].append(delta)
                    if len(self.history[peer]) > 20:
                        self.history[peer].pop(0)
                    self.status[peer] = "UP"
                return
        except Exception:
            pass

        with self.lock:
            elapsed = now - self.last_seen[peer]
            if elapsed > self.interval * 8:
                self.status[peer] = "DOWN"
            elif elapsed > self.interval * 3:
                self.status[peer] = "SUSPECT"

    def get_status(self):
        with self.lock:
            now = time.time()
            return {
                p: {
                    "status": self.status[p],
                    "seconds_since_heartbeat": round(now - self.last_seen[p], 3),
                    "mean_interval_ms": round(
                        (sum(self.history[p]) / len(self.history[p]) * 1000) if self.history[p] else 0, 2
                    )
                }
                for p in self.peers
            }

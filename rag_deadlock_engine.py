#!/usr/bin/env python3
"""
ICS 2403 - Milestone 6: Resource Allocation Graph (RAG) & Deadlock Engine

Features:
  - Multi-resource capacity tracking (CPU, RAM, SOCKETS)
  - Allocation & Request edge tracking with FIFO / non-FIFO granting
  - Deadlock Detection via cycle finding in the Wait-For Graph (WFG)
  - Deadlock Recovery via minimum-cost victim selection and preemption
  - Deadlock Prevention via Havender's global resource ordering (CPU -> RAM -> SOCKETS)
"""
import copy
import threading

RESOURCE_ORDER = ["CPU", "RAM", "SOCKETS"]


class ResourceAllocationGraph:
    def __init__(self, capacity, grant_policy="fifo", prevention=None):
        """
        capacity     : dict e.g. {"CPU": 4.0, "RAM": 4096.0, "SOCKETS": 4.0}
        grant_policy : "fifo" (default) or "unfair"
        prevention   : "resource_ordering" or None
        """
        self.capacity = {r: float(v) for r, v in capacity.items()}
        self.available = {r: float(v) for r, v in capacity.items()}
        self.grant_policy = grant_policy
        self.prevention = prevention
        self.lock = threading.RLock()

        # RAG state tracking
        self.allocations = {}  # proc_id -> {res: float}
        self.requests = {}     # proc_id -> {res: float}
        self.waiting_queue = []  # List of proc_ids waiting (for FIFO tracking)

    def _fits(self, req):
        return all(req.get(r, 0.0) <= self.available.get(r, 0.0) + 1e-9 for r in req)

    def _check_resource_ordering(self, proc_id, req):
        """
        Havender's prevention rule: a process holding resource R_i may only request R_j if order(R_j) > order(R_i).
        """
        if self.prevention != "resource_ordering":
            return True

        held = self.allocations.get(proc_id, {})
        max_held_idx = -1
        for r, amt in held.items():
            if amt > 1e-9 and r in RESOURCE_ORDER:
                max_held_idx = max(max_held_idx, RESOURCE_ORDER.index(r))

        for r, amt in req.items():
            if amt > 1e-9 and r in RESOURCE_ORDER:
                if RESOURCE_ORDER.index(r) < max_held_idx:
                    return False
        return True

    def _grant(self, proc_id, req):
        for r, amt in req.items():
            if amt > 0:
                self.available[r] -= amt
                if proc_id not in self.allocations:
                    self.allocations[proc_id] = {res: 0.0 for res in self.capacity}
                self.allocations[proc_id][r] = self.allocations[proc_id].get(r, 0.0) + amt

    def request_resources(self, proc_id, req, wait=False):
        """
        Processes a resource request from proc_id.
        Returns dict with status ("ALLOCATED", "WAITING_BLOCKED", "BLOCKED", or "ORDER_VIOLATION").
        """
        with self.lock:
            # 1. Prevention check
            if not self._check_resource_ordering(proc_id, req):
                return {"status": "ORDER_VIOLATION"}

            # 2. Check if fits immediately
            if self._fits(req):
                self._grant(proc_id, req)
                if proc_id in self.requests:
                    del self.requests[proc_id]
                if proc_id in self.waiting_queue:
                    self.waiting_queue.remove(proc_id)
                return {"status": "ALLOCATED"}

            # 3. Handle wait vs no-wait
            if wait:
                self.requests[proc_id] = {r: float(req.get(r, 0.0)) for r in req}
                if proc_id not in self.waiting_queue:
                    self.waiting_queue.append(proc_id)
                return {"status": "WAITING_BLOCKED"}

            return {"status": "BLOCKED"}

    def release_resources(self, proc_id):
        """
        Releases all resources allocated to proc_id, removes request edges, and attempts to satisfy waiting requests.
        """
        with self.lock:
            released = self.allocations.pop(proc_id, {})
            for r, amt in released.items():
                self.available[r] += amt

            if proc_id in self.requests:
                del self.requests[proc_id]
            if proc_id in self.waiting_queue:
                self.waiting_queue.remove(proc_id)

            self._process_waiting_requests()
            return released

    def _process_waiting_requests(self):
        """
        Processes pending request edges according to the configured grant_policy.
        """
        if self.grant_policy == "fifo":
            for p in list(self.waiting_queue):
                req = self.requests.get(p)
                if req and self._fits(req):
                    self._grant(p, req)
                    del self.requests[p]
                    self.waiting_queue.remove(p)
                else:
                    # Strict FIFO: stop at the first request that cannot be satisfied
                    break
        else:  # Unfair / greedy grant policy
            for p in list(self.waiting_queue):
                req = self.requests.get(p)
                if req and self._fits(req):
                    self._grant(p, req)
                    del self.requests[p]
                    self.waiting_queue.remove(p)

    def status(self, proc_id=None):
        with self.lock:
            if proc_id:
                return {
                    "allocated": self.allocations.get(proc_id, {}),
                    "requested": self.requests.get(proc_id, {})
                }
            return {
                "allocations": self.allocations,
                "requests": self.requests,
                "available": self.available
            }

    def utilization(self):
        with self.lock:
            return {
                r: round(1.0 - (self.available[r] / self.capacity[r]), 3)
                for r in self.capacity
            }

    def export_graph_state(self):
        with self.lock:
            return {
                "policy": {
                    "grant_policy": self.grant_policy,
                    "prevention": self.prevention
                },
                "capacity": copy.deepcopy(self.capacity),
                "available": copy.deepcopy(self.available),
                "allocations": copy.deepcopy(self.allocations),
                "requests": copy.deepcopy(self.requests)
            }

    def detect_deadlock_cycle(self):
        """
        Builds the Wait-For Graph (WFG) from request and allocation edges and detects cycles via Tarjan's / DFS traversal.
        """
        with self.lock:
            wfg = {}
            for p1, reqs in self.requests.items():
                wfg[p1] = set()
                for r, amt in reqs.items():
                    if amt > 1e-9:
                        # Find processes holding resource r
                        for p2, allocs in self.allocations.items():
                            if p1 != p2 and allocs.get(r, 0.0) > 1e-9:
                                wfg[p1].add(p2)

            visited = set()
            rec_stack = set()
            cycle = []

            def dfs(v, path):
                visited.add(v)
                rec_stack.add(v)
                path.append(v)

                for neighbor in wfg.get(v, []):
                    if neighbor not in visited:
                        if dfs(neighbor, path):
                            return True
                    elif neighbor in rec_stack:
                        idx = path.index(neighbor)
                        cycle.extend(path[idx:])
                        return True

                path.pop()
                rec_stack.remove(v)
                return False

            for node in list(wfg.keys()):
                if node not in visited:
                    if dfs(node, []):
                        return {"deadlock_detected": True, "cycle": cycle}

            return {"deadlock_detected": False, "cycle": []}

    def recover_deadlock(self, victim=None):
        """
        Preempts a victim process to break a detected cycle.
        If no victim is specified, selects the process holding the minimum total allocated resources.
        """
        with self.lock:
            detection = self.detect_deadlock_cycle()
            if not detection["deadlock_detected"]:
                return {"action": "NONE", "message": "No deadlock detected"}

            cycle = detection["cycle"]
            if victim and victim in cycle:
                chosen_victim = victim
            else:
                # Minimum total resources held cost metric
                def cost(p):
                    return sum(self.allocations.get(p, {}).values())
                chosen_victim = min(cycle, key=cost)

            released = self.release_resources(chosen_victim)
            return {
                "action": "PREEMPT",
                "preempted_process": chosen_victim,
                "released": released
            }

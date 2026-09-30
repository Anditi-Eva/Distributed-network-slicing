import threading
from collections import defaultdict

class ResourceAllocationGraph:
    def __init__(self, available_resources):
        self.available = available_resources.copy()  # e.g., {"CPU": 4.0, "RAM": 4096, "SOCKETS": 4}
        self.total = available_resources.copy()
        
        # Graph adjacency structures
        self.allocated = defaultdict(lambda: defaultdict(float))  # R_j -> P_i -> quantity
        self.requests = defaultdict(lambda: defaultdict(float))   # P_i -> R_j -> quantity
        self.lock = threading.Lock()

    def request_resources(self, process_id, resource_requirements):
        """Adds request edges (P_i -> R_j) and attempts immediate allocation."""
        with self.lock:
            # 1. Register Request Edges (P_i -> R_j)
            for res_type, amount in resource_requirements.items():
                self.requests[process_id][res_type] = amount

            # 2. Check if request can be satisfied
            can_allocate = True
            for res_type, amount in resource_requirements.items():
                if self.available.get(res_type, 0) < amount:
                    can_allocate = False
                    break

            if can_allocate:
                # Convert Request Edges to Assignment Edges (R_j -> P_i)
                for res_type, amount in resource_requirements.items():
                    self.available[res_type] -= amount
                    self.allocated[res_type][process_id] += amount
                del self.requests[process_id]
                return {"status": "ALLOCATED", "granted": True}
            else:
                return {"status": "WAITING_BLOCKED", "granted": False}

    def release_resources(self, process_id):
        """Releases assignment edges (R_j -> P_i) and frees resources."""
        with self.lock:
            released = {}
            for res_type in list(self.allocated.keys()):
                if process_id in self.allocated[res_type]:
                    amt = self.allocated[res_type].pop(process_id)
                    self.available[res_type] += amt
                    released[res_type] = amt
            if process_id in self.requests:
                del self.requests[process_id]
            return released

    def detect_deadlock_cycle(self):
        """
        Constructs explicit dependency graph between Processes (Wait-For Graph)
        and detects circular wait dependencies via DFS traversal.
        """
        with self.lock:
            # Build Wait-For Graph (P_i -> P_k where P_i waits for resource held by P_k)
            wait_for_graph = defaultdict(set)
            
            for p_waiting, req_dict in self.requests.items():
                for res_type, amt_needed in req_dict.items():
                    if self.available.get(res_type, 0) < amt_needed:
                        # Find processes holding this resource
                        for p_holding, amt_held in self.allocated[res_type].items():
                            if amt_held > 0 and p_waiting != p_holding:
                                wait_for_graph[p_waiting].add(p_holding)

            # Cycle Detection Algorithm (DFS)
            visited = set()
            rec_stack = set()
            cycle_nodes = []

            def dfs(node, path):
                visited.add(node)
                rec_stack.add(node)
                path.append(node)

                for neighbor in wait_for_graph.get(node, []):
                    if neighbor not in visited:
                        if dfs(neighbor, path):
                            return True
                    elif neighbor in rec_stack:
                        # Cycle found
                        cycle_start = path.index(neighbor)
                        cycle_nodes.extend(path[cycle_start:])
                        return True

                path.pop()
                rec_stack.remove(node)
                return False

            for process in list(wait_for_graph.keys()):
                if process not in visited:
                    if dfs(process, []):
                        return {
                            "deadlock_detected": True,
                            "wait_for_graph": {k: list(v) for k, v in wait_for_graph.items()},
                            "deadlocked_processes": list(set(cycle_nodes))
                        }

            return {
                "deadlock_detected": False,
                "wait_for_graph": {k: list(v) for k, v in wait_for_graph.items()},
                "deadlocked_processes": []
            }

    def recover_deadlock(self, victim_process_id):
        """Preempts resources from a victim process to break the circular dependency."""
        released = self.release_resources(victim_process_id)
        return {
            "action": "VICTIM_PREEMPTED_AND_ROLLED_BACK",
            "preempted_process": victim_process_id,
            "reclaimed_resources": released
        }

    def export_graph_state(self):
        """Exports JSON structure representing current RAG assignments and requests."""
        with self.lock:
            return {
                "available_resources": self.available,
                "assignment_edges": {r: dict(p) for r, p in self.allocated.items()},
                "request_edges": {p: dict(r) for p, r in self.requests.items()}
            }

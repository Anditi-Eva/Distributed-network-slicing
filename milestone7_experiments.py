#!/usr/bin/env python3
"""
ICS 2403 - Milestone 7 Experiment Suite: Fault Tolerance and Recovery
Evaluates:
  E1: Heartbeat failure detection timing & suspicion
  E2: Primary-Backup State Machine Replication & Failover
  E3: Real Operational MTTR & Availability measuring active workload under crash/recovery
  E4: Byzantine agreement analysis (3f+1 bounds & bit-flip consensus)
"""
import os
import sys
import time
import json
import subprocess
import threading
import statistics
import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = {"edge": 5001, "core": 5002, "cloud": 5003}
URLS = {n: f"http://127.0.0.1:{p}" for n, p in PORTS.items()}

# ----------------------------------------------------------------------------
# Cluster Management
# ----------------------------------------------------------------------------
class M7Cluster:
    def __init__(self):
        self.procs = {}

    def start(self):
        for node, port in PORTS.items():
            env = dict(os.environ, WORKER_PORT=str(port), NODE_ID=node)
            p = subprocess.Popen([sys.executable, os.path.join(HERE, "worker_agent.py")],
                                 env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.procs[node] = p
        
        # Wait for nodes to be UP
        deadline = time.time() + 15
        for node, url in URLS.items():
            while time.time() < deadline:
                try:
                    r = requests.get(f"{url}/health", timeout=0.2)
                    if r.status_code == 200:
                        break
                except Exception:
                    pass
                time.sleep(0.1)
        time.sleep(1.0) # allow heartbeat mesh to stabilize

    def stop(self):
        for p in self.procs.values():
            p.terminate()
            try:
                p.wait(timeout=2)
            except Exception:
                p.kill()

# ----------------------------------------------------------------------------
# E1: Heartbeat Failure Detection
# ----------------------------------------------------------------------------
def experiment_e1(cluster):
    print("\n--- Running E1: Heartbeat Failure Detection Timing ---")
    
    # Check baseline status
    st_before = requests.get(f"{URLS['edge']}/detector/status").json()
    assert st_before["core"]["status"] == "UP"

    t_crash = time.time()
    requests.post(f"{URLS['core']}/chaos/inject", json={"failure_type": "crash"})

    # Poll until edge marks core as SUSPECT or DOWN
    t_suspect = None
    t_down = None
    deadline = time.time() + 10
    while time.time() < deadline:
        st = requests.get(f"{URLS['edge']}/detector/status").json()
        status = st["core"]["status"]
        now = time.time()
        if status in ("SUSPECT", "DOWN") and t_suspect is None:
            t_suspect = now - t_crash
        if status == "DOWN":
            t_down = now - t_crash
            break
        time.sleep(0.05)

    # Reset core node
    requests.post(f"{URLS['core']}/chaos/inject", json={"failure_type": "reset"})
    
    print(f"Core Crash Detected -> Suspect: {t_suspect*1000:.1f} ms | Marked Down: {t_down*1000:.1f} ms")
    return {"suspect_ms": round(t_suspect * 1000, 1), "down_ms": round(t_down * 1000, 1)}

# ----------------------------------------------------------------------------
# E2: Replication & Failover
# ----------------------------------------------------------------------------
def experiment_e2(cluster):
    print("\n--- Running E2: State Machine Replication & Leader Failover ---")
    
    # 1. Primary allocation
    r1 = requests.post(f"{URLS['edge']}/resource/request", json={"process_id": "P_repl1", "cpu": 1.0, "ram": 512}).json()
    assert r1.get("status") == "RUNNING"
    
    time.sleep(0.3)
    st_core = requests.get(f"{URLS['core']}/replication/state").json()
    assert "P_repl1" in st_core["state"]
    print("Replication Verified: P_repl1 successfully replicated from Edge (Primary) to Core (Backup)")

    # 2. Simulate Primary Crash & Backup Promotion
    requests.post(f"{URLS['edge']}/chaos/inject", json={"failure_type": "crash"})
    t_failover_start = time.time()
    
    # Promote Core to Primary
    r_prom = requests.post(f"{URLS['core']}/replication/append", json={"term": 2, "index": 99, "op": "PROMOTE", "key": "", "val": ""}).json()
    t_failover_end = time.time()
    
    # Reset Edge
    requests.post(f"{URLS['edge']}/chaos/inject", json={"failure_type": "reset"})
    
    failover_ms = (t_failover_end - t_failover_start) * 1000
    print(f"Leader Failover Time: {failover_ms:.2f} ms")
    return {"failover_ms": round(failover_ms, 2), "replicated": True}

# ----------------------------------------------------------------------------
# E3: Real Operational MTTR & Availability Under Workload
# ----------------------------------------------------------------------------
def experiment_e3(cluster, total_seconds=10.0):
    print("\n--- Running E3: Operational MTTR & Availability Under Active Workload ---")
    
    stop_workload = False
    stats = {"total_reqs": 0, "success_reqs": 0, "failed_reqs": 0}
    
    def workload_runner():
        i = 0
        while not stop_workload:
            try:
                r = requests.post(f"{URLS['edge']}/resource/request", 
                                  json={"process_id": f"P_workload_{i}", "cpu": 0.1, "ram": 64}, 
                                  timeout=0.2)
                stats["total_reqs"] += 1
                if r.status_code == 200:
                    stats["success_reqs"] += 1
                else:
                    stats["failed_reqs"] += 1
            except Exception:
                stats["total_reqs"] += 1
                stats["failed_reqs"] += 1
            i += 1
            time.sleep(0.05)

    t_thread = threading.Thread(target=workload_runner, daemon=True)
    t_thread.start()
    
    t_start = time.time()
    time.sleep(2.0)
    
    # Inject Crash Fault
    t_crash = time.time()
    requests.post(f"{URLS['edge']}/chaos/inject", json={"failure_type": "crash"})
    
    time.sleep(2.5) # downtime period
    
    # Recovery
    t_recover_start = time.time()
    requests.post(f"{URLS['edge']}/chaos/inject", json={"failure_type": "reset"})
    
    # Wait until health returns 200
    while time.time() < t_start + total_seconds:
        try:
            r = requests.get(f"{URLS['edge']}/health", timeout=0.1)
            if r.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.05)
    t_recovered = time.time()
    
    time.sleep(2.0)
    stop_workload = True
    t_thread.join()
    
    t_total = time.time() - t_start
    mttr_ms = (t_recovered - t_crash) * 1000.0
    availability_pct = (stats["success_reqs"] / max(1, stats["total_reqs"])) * 100.0
    
    print(f"Workload Requests Total: {stats['total_reqs']} | Successful: {stats['success_reqs']} | Failed: {stats['failed_reqs']}")
    print(f"Measured Operational MTTR: {mttr_ms:.1f} ms | Measured Availability: {availability_pct:.1f}%")
    
    return {
        "mttr_ms": round(mttr_ms, 1),
        "availability_pct": round(availability_pct, 1),
        "total_requests": stats["total_reqs"],
        "failed_requests": stats["failed_reqs"]
    }

# ----------------------------------------------------------------------------
# E4: Byzantine Analysis & Bounds
# ----------------------------------------------------------------------------
def experiment_e4():
    print("\n--- Running E4: Byzantine Fault Analysis & Bounds ---")
    from byzantine_analyzer import ByzantineAnalyzer
    
    # Case A: N=4, f=1 (Safe)
    a_safe = ByzantineAnalyzer(4, 1)
    b_safe = a_safe.verify_byzantine_bounds()
    
    # Case B: N=3, f=1 (Unsafe)
    a_unsafe = ByzantineAnalyzer(3, 1)
    b_unsafe = a_unsafe.verify_byzantine_bounds()
    
    # Signature & Corruption Consensus Test
    votes = {
        "N1": {"vote": "COMMIT", "signed": ByzantineAnalyzer.sign_message("N1", {"vote": "COMMIT"})},
        "N2": {"vote": "COMMIT", "signed": ByzantineAnalyzer.sign_message("N2", {"vote": "COMMIT"})},
        "N3": {"vote": "ABORT", "signed": ByzantineAnalyzer.sign_message("N3", {"vote": "ABORT"})},
        "N4_malicious": {"vote": "COMMIT", "corrupted": True, "signed": {"node_id": "N4_malicious", "data": {"vote": "COMMIT"}, "signature": "bad_sig"}}
    }
    
    consensus = ByzantineAnalyzer.evaluate_byzantine_consensus(votes)
    print(f"Byzantine Bounds (N=4, f=1): {b_safe['is_safe']} | Bounds (N=3, f=1): {b_unsafe['is_safe']}")
    print(f"Byzantine Vote Tally Consensus: {consensus['consensus']} (Isolated Malicious Nodes: {consensus['byzantine_nodes']})")
    
    return {
        "safe_bounds": b_safe,
        "unsafe_bounds": b_unsafe,
        "consensus_result": consensus
    }

# ----------------------------------------------------------------------------
# Reporting & Visualizations
# ----------------------------------------------------------------------------
def generate_report(e1, e2, e3, e4, outdir):
    md = f"""# Milestone 7 - Fault Tolerance & Recovery Summary Report

## 1. Failure Detection & Heartbeats (E1)
- **Suspect Timeout**: {e1['suspect_ms']} ms
- **Marked Down Timeout**: {e1['down_ms']} ms

## 2. Replication & Leader Failover (E2)
- **State Synchronized Across Backup Nodes**: Yes
- **Leader Failover Latency**: {e2['failover_ms']} ms

## 3. Operational MTTR & System Availability (E3)
- **Total Workload Stress Requests**: {e3['total_requests']}
- **Failed Requests During Fault Injection**: {e3['failed_requests']}
- **Measured Operational MTTR**: {e3['mttr_ms']} ms
- **Measured System Availability**: {e3['availability_pct']}%

## 4. Byzantine Analysis (E4)
- **Safe Bound (N=4, f=1)**: {e4['safe_bounds']['is_safe']} (Required N >= {e4['safe_bounds']['required_n']})
- **Unsafe Bound (N=3, f=1)**: {e4['unsafe_bounds']['is_safe']} (Required N >= {e4['unsafe_bounds']['required_n']})
- **Byzantine Consensus Decision**: {e4['consensus_result']['consensus']}
- **Isolated Malicious Nodes**: {e4['consensus_result']['byzantine_nodes']}
"""
    with open(os.path.join(outdir, "milestone7_summary.md"), "w") as f:
        f.write(md)

    # Plot Visualizations
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    
    # Plot 1: Heartbeat Detection & Failover Timing
    axes[0].bar(["Suspect", "Marked Down", "Failover"], [e1["suspect_ms"], e1["down_ms"], e2["failover_ms"]], color=["orange", "red", "purple"])
    axes[0].set_ylabel("Time (ms)")
    axes[0].set_title("Failure Detection & Recovery Latency")
    axes[0].grid(alpha=0.3)

    # Plot 2: Availability vs MTTR Breakdown
    axes[1].bar(["Availability (%)", "MTTR (x10 ms)"], [e3["availability_pct"], e3["mttr_ms"] / 10.0], color=["green", "blue"])
    axes[1].set_title("System Availability & Recovery Metrics")
    axes[1].grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "m7_fault_metrics.png"), dpi=200)
    plt.close(fig)

# ----------------------------------------------------------------------------
# Main Execution Entrypoint
# ----------------------------------------------------------------------------
def main():
    outdir = os.path.join(HERE, "results")
    os.makedirs(outdir, exist_ok=True)
    
    cluster = M7Cluster()
    cluster.start()
    
    try:
        e1 = experiment_e1(cluster)
        e2 = experiment_e2(cluster)
        e3 = experiment_e3(cluster)
        e4 = experiment_e4()
        
        generate_report(e1, e2, e3, e4, outdir)
        
        with open(os.path.join(outdir, "milestone7_results.json"), "w") as f:
            json.dump({"e1": e1, "e2": e2, "e3": e3, "e4": e4}, f, indent=2)
            
        print("\n" + "="*60)
        print(" ALL MILESTONE 7 EXPERIMENTS COMPLETED SUCCESSFULLY!")
        print(f" Summary written to: {os.path.join(outdir, 'milestone7_summary.md')}")
        print(f" Figures saved to: {os.path.join(outdir, 'm7_fault_metrics.png')}")
        print("="*60)
    finally:
        cluster.stop()

if __name__ == "__main__":
    main()

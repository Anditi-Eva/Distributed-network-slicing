#!/usr/bin/env python3
"""
ICS 2403 - Milestone 6: Deadlock & Resource Allocation Experiments
"""
import os
import sys
import time
import json
import random
import tempfile
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
HOST = "127.0.0.1"
PORT = 5005
BASE_URL = f"http://{HOST}:{PORT}"

# ----------------------------------------------------------------------------
# Helper Utilities
# ----------------------------------------------------------------------------
def post(endpoint, data=None):
    return requests.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=5).json()

def get(endpoint, params=None):
    return requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=5).json()

def get_text(endpoint):
    return requests.get(f"{BASE_URL}{endpoint}", timeout=5).text

def reset_engine(policy="fifo", prevention=None):
    return post("/resource/reset", {"grant_policy": policy, "prevention": prevention})

# ----------------------------------------------------------------------------
# Demo 1: Race Condition & Mutual Exclusion
# ----------------------------------------------------------------------------
def run_race_demo():
    print("\n--- Running Demo 1: Race Condition & Mutual Exclusion ---")
    reset_engine()
    
    def request_task(proc_id):
        return post("/resource/request", {"process_id": f"P{proc_id}", "cpu": 1.0, "ram": 512.0})

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(request_task, range(20)))

    allocated = [r for r in results if r.get("status") == "RUNNING"]
    blocked = [r for r in results if r.get("status") == "BLOCKED_WAITING"]
    
    graph_state = get("/resource/graph")
    avail_cpu = graph_state["available"]["CPU"]
    
    print(f"Total Requests: 20 | Allocated: {len(allocated)} | Blocked: {len(blocked)}")
    print(f"Remaining CPU: {avail_cpu} (Expected: 0.0, No oversubscription)")
    return {"allocated": len(allocated), "blocked": len(blocked), "passed": len(allocated) == 4}

# ----------------------------------------------------------------------------
# Demo 2: Starvation Demo
# ----------------------------------------------------------------------------
def run_starvation_demo():
    print("\n--- Running Demo 2: Starvation Demo (Unfair vs FIFO) ---")
    
    # 1. Unfair policy
    reset_engine(policy="unfair")
    post("/resource/request", {"process_id": "P_holder", "cpu": 3.0, "ram": 1024, "wait": False})
    post("/resource/request", {"process_id": "P_starve", "cpu": 2.0, "ram": 512, "wait": True})
    
    res_unfair = post("/resource/request", {"process_id": "P_small", "cpu": 1.0, "ram": 256, "wait": True})
    unfair_bypassed = res_unfair.get("status") == "RUNNING"
    print(f"Unfair Policy: P_small allocated while P_starve waits? -> {unfair_bypassed}")

    # 2. FIFO policy
    reset_engine(policy="fifo")
    post("/resource/request", {"process_id": "P_holder", "cpu": 3.0, "ram": 1024, "wait": False})
    post("/resource/request", {"process_id": "P_starve", "cpu": 2.0, "ram": 512, "wait": True})
    
    res_fifo = post("/resource/request", {"process_id": "P_small", "cpu": 1.0, "ram": 256, "wait": True})
    fifo_blocked = res_fifo.get("status") in ("WAITING", "WAITING_BLOCKED", "BLOCKED_WAITING")
    print(f"FIFO Policy  : P_small queued behind P_starve? -> {fifo_blocked}")

    return {"unfair_bypassed": unfair_bypassed, "fifo_queued": fifo_blocked}

# ----------------------------------------------------------------------------
# Demo 3: Live Deadlock & RAG Diagram Generation
# ----------------------------------------------------------------------------
def run_deadlock_demo(outdir):
    print("\n--- Running Demo 3: Live Deadlock & RAG Graph ---")
    reset_engine()

    post("/resource/request", {"process_id": "P1", "cpu": 3.0, "sockets": 1.0, "wait": True})
    post("/resource/request", {"process_id": "P2", "ram": 3072.0, "sockets": 1.0, "wait": True})

    post("/resource/request", {"process_id": "P1", "ram": 2048.0, "wait": True})
    post("/resource/request", {"process_id": "P2", "cpu": 2.0, "wait": True})

    detection = get("/deadlock/detect")
    print(f"Deadlock Detected: {detection['deadlock_detected']} | Cycle: {detection['cycle']}")

    graph_data = get("/resource/graph")
    plot_rag_graph(graph_data, detection["cycle"], outdir)

    recovery = post("/deadlock/recover")
    print(f"Recovery Action: Preempted {recovery.get('preempted_process')}")

    return {"detected": detection["deadlock_detected"], "preempted": recovery.get("preempted_process")}

def plot_rag_graph(graph_data, cycle, outdir):
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.set_title("Resource Allocation Graph (Deadlock Cycle Highlighted)", fontsize=12, fontweight='bold')
    
    procs = list(set(list(graph_data["allocations"].keys()) + list(graph_data["requests"].keys())))
    resources = list(graph_data["capacity"].keys())
    
    y_p = [0.7, 0.3]
    y_r = [0.8, 0.5, 0.2]
    
    p_pos = {}
    for i, p in enumerate(procs):
        p_pos[p] = (0.2, y_p[i % len(y_p)])
        ax.plot(p_pos[p][0], p_pos[p][1], 'o', ms=30, color='lightblue', mec='black', zorder=3)
        ax.text(p_pos[p][0], p_pos[p][1], p, ha='center', va='center', fontweight='bold', zorder=4)

    r_pos = {}
    for i, r in enumerate(resources):
        r_pos[r] = (0.8, y_r[i % len(y_r)])
        ax.plot(r_pos[r][0], r_pos[r][1], 's', ms=35, color='lightgray', mec='black', zorder=3)
        ax.text(r_pos[r][0], r_pos[r][1], r, ha='center', va='center', fontweight='bold', zorder=4)

    for p, res_map in graph_data["allocations"].items():
        for r, amt in res_map.items():
            if amt > 0 and p in p_pos and r in r_pos:
                in_cycle = p in cycle
                color = 'red' if in_cycle else 'gray'
                lw = 2 if in_cycle else 1
                ax.annotate("", xy=p_pos[p], xytext=r_pos[r],
                            arrowprops=dict(arrowstyle="->", color=color, lw=lw, shrinkA=18, shrinkB=18))

    for p, res_map in graph_data["requests"].items():
        for r, amt in res_map.items():
            if amt > 0 and p in p_pos and r in r_pos:
                in_cycle = p in cycle
                color = 'red' if in_cycle else 'gray'
                lw = 2 if in_cycle else 1
                ax.annotate("", xy=r_pos[r], xytext=p_pos[p],
                            arrowprops=dict(arrowstyle="->", color=color, lw=lw, ls="--", shrinkA=18, shrinkB=18))

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "m6_rag_deadlock.png"), dpi=200)
    plt.close(fig)

# ----------------------------------------------------------------------------
# Demo 4: Utilization Comparison of Three Policies
# ----------------------------------------------------------------------------
def run_utilization_comparison(outdir):
    print("\n--- Running Demo 4: Utilization Comparison of 3 Policies ---")
    
    policies = [
        ("Detection & Recovery", {"policy": "fifo", "prevention": None, "auto": True}),
        ("Havender Ordering", {"policy": "fifo", "prevention": "resource_ordering", "auto": False}),
        ("Immediate Abort (No-Wait)", {"policy": "fifo", "prevention": None, "auto": False})
    ]
    
    results = {}
    
    for label, cfg in policies:
        reset_engine(policy=cfg["policy"], prevention=cfg["prevention"])
        if cfg["auto"]:
            post("/deadlock/auto", {"enabled": True, "interval": 0.1})
        else:
            post("/deadlock/auto", {"enabled": False})
            
        committed, rejected = 0, 0
        wait_flag = (label != "Immediate Abort (No-Wait)")
        
        rng = random.Random(42)
        for i in range(60):
            p_id = f"P_{i%5}"
            cpu = rng.choice([0.5, 1.0, 2.0])
            ram = rng.choice([256.0, 512.0, 1024.0])
            sockets = rng.choice([0.0, 1.0])
            
            req_data = {"process_id": p_id, "cpu": cpu, "ram": ram, "sockets": sockets, "wait": wait_flag}
            res = post("/resource/request", req_data)
            
            if res.get("status") in ("RUNNING", "ALLOCATED"):
                committed += 1
            elif res.get("status") in ("BLOCKED_WAITING", "ORDER_VIOLATION"):
                rejected += 1
                
            if i % 10 == 0:
                post("/resource/release", {"process_id": p_id})
                
        # Use get_text for /metrics endpoint since Prometheus outputs plain text
        raw_metrics = get_text("/metrics")
        util = get("/resource/graph")["available"]
        cpu_util = (4.0 - util["CPU"]) / 4.0 * 100.0
        ram_util = (4096.0 - util["RAM"]) / 4096.0 * 100.0
        
        results[label] = {
            "committed": committed,
            "rejected": rejected,
            "cpu_util_pct": cpu_util,
            "ram_util_pct": ram_util
        }
        print(f"Policy: {label:<25} | Committed: {committed:<2} | Rejected: {rejected:<2} | Avg CPU Util: {cpu_util:.1f}%")

    fig, ax = plt.subplots(figsize=(8, 4.5))
    labels = list(results.keys())
    cpu_utils = [results[l]["cpu_util_pct"] for l in labels]
    commits = [results[l]["committed"] for l in labels]
    
    x = range(len(labels))
    width = 0.35
    
    ax.bar([i - width/2 for i in x], cpu_utils, width, label='CPU Utilization (%)', color='skyblue', edgecolor='black')
    ax.bar([i + width/2 for i in x], commits, width, label='Successful Allocations', color='lightgreen', edgecolor='black')
    
    ax.set_ylabel('Metrics')
    ax.set_title('Policy Comparison: Resource Utilization & Throughput')
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(alpha=0.3)
    
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "m6_policy_comparison.png"), dpi=200)
    plt.close(fig)
    
    return results

# ----------------------------------------------------------------------------
# Main Script Execution
# ----------------------------------------------------------------------------
def main():
    outdir = os.path.join(HERE, "results")
    os.makedirs(outdir, exist_ok=True)
    
    env = dict(os.environ, WORKER_PORT=str(PORT))
    proc = subprocess.Popen([sys.executable, os.path.join(HERE, "worker_agent.py")],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)
    
    try:
        r1 = run_race_demo()
        r2 = run_starvation_demo()
        r3 = run_deadlock_demo(outdir)
        r4 = run_utilization_comparison(outdir)
        
        print("\n" + "="*60)
        print(" ALL MILESTONE 6 EXPERIMENTS COMPLETED SUCCESSFULLY!")
        print(f" Figures generated in: {outdir}")
        print("="*60)
    finally:
        proc.terminate()
        proc.wait()

if __name__ == "__main__":
    main()

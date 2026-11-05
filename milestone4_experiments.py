#!/usr/bin/env python3
"""
ICS 2403 - Milestone 4 experiment suite
Distributed Algorithms and Coordination (Bully election + Lamport clocks)

Spawns real node processes (coordination_node.py), drives them over HTTP and
measures what the milestone asks for:

  E1  Message complexity & scalability  (best / typical / worst-case initiator, N = 3..13)
  E2  Synchronization delay & failure recovery (leader crash -> heartbeat detection -> re-election)
  E3  Event ordering & consistency      (Lamport clock condition over a random message workload)

Every run is checked automatically; the script exits non-zero if a property fails.

Usage:  python3 milestone4_experiments.py            # full run
        python3 milestone4_experiments.py --quick    # fast smoke run
"""
import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_PORT = 6100
HOST = "127.0.0.1"
ELECTION_TYPES = ("ELECTION", "OK", "COORDINATOR")


# ----------------------------------------------------------------------------
# Cluster management
# ----------------------------------------------------------------------------
class Cluster:
    def __init__(self, n, monitor_interval=0.0, monitor_misses=3):
        self.n = n
        self.ids = [f"N{i}" for i in range(1, n + 1)]          # priority i for N_i
        self.ports = {nid: BASE_PORT + i for i, nid in enumerate(self.ids, start=1)}
        self.procs = []
        self.monitor_interval = monitor_interval
        self.monitor_misses = monitor_misses

    def url(self, nid):
        return f"http://{HOST}:{self.ports[nid]}"

    def start(self):
        spec = ",".join(f"{nid}:{self.ports[nid]}:{i}" for i, nid in enumerate(self.ids, start=1))
        for i, nid in enumerate(self.ids, start=1):
            cmd = [sys.executable, os.path.join(HERE, "coordination_node.py"),
                   "--id", nid, "--port", str(self.ports[nid]), "--priority", str(i),
                   "--cluster", spec, "--quiet",
                   "--monitor-interval", str(self.monitor_interval),
                   "--monitor-misses", str(self.monitor_misses)]
            env = dict(os.environ, PYTHONPATH=HERE)
            self.procs.append(subprocess.Popen(cmd, cwd=HERE, env=env,
                                               stdout=subprocess.DEVNULL,
                                               stderr=subprocess.DEVNULL))
        deadline = time.time() + 30
        for nid in self.ids:
            while True:
                try:
                    requests.get(self.url(nid) + "/coordination/state", timeout=0.5)
                    break
                except requests.exceptions.RequestException:
                    if time.time() > deadline:
                        raise RuntimeError(f"node {nid} did not start")
                    time.sleep(0.1)

    def stop(self):
        for p in self.procs:
            p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        self.procs = []

    # --- helpers ---------------------------------------------------------
    def state(self, nid, log=False):
        r = requests.get(self.url(nid) + "/coordination/state",
                         params={"log": "1"} if log else None, timeout=2)
        return r.json()

    def states(self, ids=None, log=False):
        ids = ids or self.ids
        with ThreadPoolExecutor(max_workers=len(ids)) as pool:
            return list(pool.map(lambda x: self.state(x, log), ids))

    def post(self, nid, path, body=None):
        return requests.post(self.url(nid) + path, json=body or {}, timeout=3).json()

    def reset_all(self):
        for nid in self.ids:
            self.post(nid, "/coordination/reset")

    def wait_agreement(self, ids, expected_leader=None, timeout=10.0):
        """Poll until every node in ids reports the same non-null leader and is idle."""
        end = time.time() + timeout
        while time.time() < end:
            sts = self.states(ids)
            leaders = {s["leader_id"] for s in sts}
            if (len(leaders) == 1 and None not in leaders
                    and not any(s["in_election"] for s in sts)
                    and (expected_leader is None or leaders == {expected_leader})):
                return sts
            time.sleep(0.01)
        return None


def summarize(values):
    return {"mean": statistics.mean(values), "min": min(values), "max": max(values),
            "stdev": statistics.pstdev(values) if len(values) > 1 else 0.0, "n": len(values)}


# ----------------------------------------------------------------------------
# E1 - message complexity / scalability
# ----------------------------------------------------------------------------
def run_one_election(cluster, initiator):
    cluster.reset_all()
    t0 = time.time()
    cluster.post(initiator, "/election/trigger")
    top = cluster.ids[-1]
    sts = cluster.wait_agreement(cluster.ids, expected_leader=top, timeout=15)
    if sts is None:
        return {"ok": False}
    time.sleep(0.3)                                     # let in-flight messages drain
    sts = cluster.states()
    counts = {t: sum(s["counts"][t] for s in sts) for t in ELECTION_TYPES}
    leaders = {s["leader_id"] for s in sts}
    return {"ok": True,
            "messages": sum(counts.values()), "counts": counts,
            "bytes": sum(s["bytes_sent"] for s in sts),
            "elections_started": sum(s["elections_started"] for s in sts),
            "delay_ms": (max(s["leader_since"] for s in sts) - t0) * 1000.0,
            "consistent": len(leaders) == 1 and leaders == {top}}


def experiment_e1(sizes, reps, rng):
    results = {}
    for n in sizes:
        print(f"  [E1] N = {n}")
        cl = Cluster(n)
        cl.start()
        try:
            res = {"best": [], "typical": [], "worst": []}
            for rep in range(reps):
                res["best"].append(run_one_election(cl, cl.ids[-1]))             # highest initiates
                res["typical"].append(run_one_election(cl, rng.choice(cl.ids)))  # random initiator
                res["worst"].append(run_one_election(cl, cl.ids[0]))             # lowest initiates
            results[n] = res
        finally:
            cl.stop()
    return results


# ----------------------------------------------------------------------------
# E2 - failure detection and recovery
# ----------------------------------------------------------------------------
def experiment_e2(sizes, reps, interval=0.2, misses=3):
    results = {}
    for n in sizes:
        print(f"  [E2] N = {n}  (heartbeat {interval}s, {misses} misses)")
        cl = Cluster(n, monitor_interval=interval, monitor_misses=misses)
        cl.start()
        runs = []
        try:
            for rep in range(reps):
                cl.reset_all()
                cl.post(cl.ids[0], "/election/trigger")
                if cl.wait_agreement(cl.ids, expected_leader=cl.ids[-1]) is None:
                    runs.append({"ok": False}); continue
                time.sleep(1.3)                                  # exceed the dedup cooldown
                before = {s["node_id"]: dict(s["counts"]) for s in cl.states()}
                alive = cl.ids[:-1]
                t_crash = time.time()
                cl.post(cl.ids[-1], "/coordination/crash", {"crashed": True})
                sts = cl.wait_agreement(alive, expected_leader=cl.ids[-2], timeout=20)
                if sts is None:
                    runs.append({"ok": False}); continue
                time.sleep(0.4)
                sts = cl.states(alive)
                detect = min(s["failure_detected_at"] for s in sts if s["failure_detected_at"])
                recovered = max(s["leader_since"] for s in sts)
                msgs = sum(sum(s["counts"][t] - before[s["node_id"]][t] for t in ELECTION_TYPES)
                           for s in sts)
                runs.append({"ok": True,
                             "detection_ms": (detect - t_crash) * 1000.0,
                             "election_ms": (recovered - detect) * 1000.0,
                             "recovery_ms": (recovered - t_crash) * 1000.0,
                             "messages": msgs,
                             "consistent": len({s["leader_id"] for s in sts}) == 1})
                cl.post(cl.ids[-1], "/coordination/crash", {"crashed": False})
        finally:
            cl.stop()
        results[n] = runs
    return results


# ----------------------------------------------------------------------------
# E3 - event ordering with Lamport clocks
# ----------------------------------------------------------------------------
def experiment_e3(n, n_messages, rng):
    print(f"  [E3] N = {n}, {n_messages} random messages (8 concurrent senders)")
    cl = Cluster(n)
    cl.start()
    try:
        cl.reset_all()
        # ---- deterministic causal chain (used as the worked example) ----
        chain = []
        def local(node, label):
            r = cl.post(node, "/event/local", {"label": label})
            chain.append((label, node, "local", r["lamport"]))
        def send(a, b, label):
            r = cl.post(a, "/event/send", {"to": b, "label": label})
            chain.append((f"{label} send", a, "send", r["send_ts"]))
            chain.append((f"{label} recv", b, "recv", r["recv_ts"]))
        local("N1", "a1")
        send("N1", "N2", "m1")
        local("N2", "b1")
        send("N2", "N3", "m2")
        send("N3", "N1", "m3")
        local("N4", "c1 (concurrent)")

        cl.reset_all()
        # ---- random workload ----
        pairs = [tuple(rng.sample(cl.ids, 2)) for _ in range(n_messages)]
        latencies = []
        lat_lock = threading.Lock()
        def fire(pair):
            t = time.perf_counter()
            r = requests.post(cl.url(pair[0]) + "/event/send",
                              json={"to": pair[1], "label": "w"}, timeout=3).json()
            with lat_lock:
                latencies.append((time.perf_counter() - t) * 1000.0)
            return r
        t_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=8) as pool:
            replies = list(pool.map(fire, pairs))
        wall = time.perf_counter() - t_start

        sts = cl.states(log=True)
        events = [e for s in sts for e in s["event_log"]]
        sends = {e["msg_id"]: e for e in events if e["kind"] == "send"}
        recvs = {e["msg_id"]: e for e in events if e["kind"] == "recv"}

        delivered = sum(1 for r in replies if r.get("delivered"))
        violations = [m for m, s in sends.items()
                      if m not in recvs or not s["lamport"] < recvs[m]["lamport"]]
        dup_ts = 0
        for s in sts:
            tss = [e["lamport"] for e in s["event_log"]]
            dup_ts += len(tss) - len(set(tss))

        total_order = sorted(events, key=lambda e: (e["lamport"], e["node"]))
        position = {id(e): i for i, e in enumerate(total_order)}
        order_viol = sum(1 for m, s in sends.items()
                         if m in recvs and position[id(s)] > position[id(recvs[m])])

        ts_field_bytes = len(', "lamport": 1000')
        sample_bytes = len(json.dumps({"msg_id": "0123456789", "label": "w", "from": "N1",
                                       "priority": 1, "lamport": 1000}))
        return {"chain": chain, "messages": n_messages, "delivered": delivered,
                "clock_condition_violations": len(violations),
                "duplicate_timestamps_per_node": dup_ts,
                "total_order_violations": order_viol,
                "max_clock": max(s["lamport_clock"] for s in sts),
                "mean_latency_ms": statistics.mean(latencies),
                "p95_latency_ms": sorted(latencies)[int(0.95 * len(latencies)) - 1],
                "throughput_msgs_s": n_messages / wall,
                "timestamp_overhead_bytes": ts_field_bytes,
                "message_bytes": sample_bytes,
                "overhead_pct": 100.0 * ts_field_bytes / sample_bytes}
    finally:
        cl.stop()


# ----------------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------------
def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def make_report(e1, e2, e3, outdir):
    lines = []
    checks = []

    # ---- E1 tables ----
    rows, scal = [], []
    for n, res in e1.items():
        row = [n]
        for scen in ("best", "typical", "worst"):
            runs = [r for r in res[scen] if r["ok"]]
            m = summarize([r["messages"] for r in runs])
            row.append(f"{m['mean']:.1f} ({m['min']}-{m['max']})")
        row += [n - 1, n * n - 1]
        rows.append(row)
        best = [r for r in res["best"] if r["ok"]]
        worst = [r for r in res["worst"] if r["ok"]]
        typ = [r for r in res["typical"] if r["ok"]]
        d = {s: summarize([r["delay_ms"] for r in runs]) for s, runs in
             (("best", best), ("typical", typ), ("worst", worst))}
        b = summarize([r["bytes"] for r in worst])
        scal.append([n, f"{d['best']['mean']:.1f}", f"{d['typical']['mean']:.1f}",
                     f"{d['worst']['mean']:.1f}", f"{b['mean']:.0f}"])
        n_all = sum(len(res[s]) for s in res)
        n_ok = sum(1 for s in res for r in res[s] if r["ok"] and r["consistent"])
        checks.append((f"E1 N={n}: all {n_all} elections consistent, leader = highest priority",
                       n_ok == n_all))
        checks.append((f"E1 N={n}: best-case messages == N-1",
                       all(r["messages"] == n - 1 for r in best)))
        checks.append((f"E1 N={n}: worst-case messages <= N^2-1",
                       all(r["messages"] <= n * n - 1 for r in worst + typ)))
    lines.append("## E1 Message complexity (mean, min-max over runs) versus theory\n")
    lines.append(md_table(["N", "Best case (highest starts)", "Typical (random starts)",
                           "Worst case (lowest starts)", "Theory best N-1",
                           "Theory worst N^2-1"], rows))
    lines.append("\n## E1 Synchronization delay and overhead\n")
    lines.append(md_table(["N", "Delay best (ms)", "Delay typical (ms)", "Delay worst (ms)",
                           "Bytes sent, worst case"], scal))

    # ---- E2 table ----
    lines.append("\n## E2 Leader crash: detection, election and recovery (mean over runs)\n")
    rows = []
    for n, runs in e2.items():
        ok = [r for r in runs if r["ok"]]
        checks.append((f"E2 N={n}: recovered and consistent in all {len(runs)} runs",
                       len(ok) == len(runs) and all(r["consistent"] for r in ok)))
        rows.append([n, len(ok),
                     f"{summarize([r['detection_ms'] for r in ok])['mean']:.0f}",
                     f"{summarize([r['election_ms'] for r in ok])['mean']:.0f}",
                     f"{summarize([r['recovery_ms'] for r in ok])['mean']:.0f}",
                     f"{summarize([r['messages'] for r in ok])['mean']:.1f}"])
    lines.append(md_table(["N", "Runs", "Detection (ms)", "Election (ms)",
                           "Total recovery (ms)", "Election messages"], rows))

    # ---- E3 ----
    lines.append("\n## E3 Worked example: Lamport timestamps on a causal chain\n")
    lines.append(md_table(["Event", "Node", "Type", "Lamport time"],
                          [[a, b, c, d] for a, b, c, d in e3["chain"]]))
    lines.append("\n## E3 Event-ordering check over a random workload\n")
    lines.append(md_table(["Metric", "Value"], [
        ["Messages sent / delivered", f"{e3['messages']} / {e3['delivered']}"],
        ["Clock-condition violations (send ts >= receive ts)", e3["clock_condition_violations"]],
        ["Duplicate timestamps within a node", e3["duplicate_timestamps_per_node"]],
        ["Total-order violations (by timestamp, then node id)", e3["total_order_violations"]],
        ["Largest clock value reached", e3["max_clock"]],
        ["Mean / p95 message latency (ms)",
         f"{e3['mean_latency_ms']:.1f} / {e3['p95_latency_ms']:.1f}"],
        ["Throughput (messages/s)", f"{e3['throughput_msgs_s']:.0f}"],
        ["Timestamp overhead per message", f"{e3['timestamp_overhead_bytes']} bytes "
                                           f"({e3['overhead_pct']:.0f}% of a {e3['message_bytes']}-byte message)"]]))
    checks.append(("E3: every message delivered", e3["delivered"] == e3["messages"]))
    checks.append(("E3: zero clock-condition violations", e3["clock_condition_violations"] == 0))
    checks.append(("E3: unique timestamps per node", e3["duplicate_timestamps_per_node"] == 0))
    checks.append(("E3: total order respects happens-before", e3["total_order_violations"] == 0))

    lines.append("\n## Automatic checks\n")
    lines.append(md_table(["Check", "Result"], [[c, "PASS" if ok else "FAIL"] for c, ok in checks]))
    text = "\n".join(lines)
    with open(os.path.join(outdir, "milestone4_summary.md"), "w") as f:
        f.write("# Milestone 4 - Experimental results (generated)\n\n" + text + "\n")
    return text, all(ok for _, ok in checks)


def make_charts(e1, e2, outdir):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "serif", "font.size": 10})
    sizes = list(e1.keys())

    def series(scen, key):
        return [statistics.mean([r[key] for r in e1[n][scen] if r["ok"]]) for n in sizes]

    # Figure 1: message complexity
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(sizes, series("worst", "messages"), "k-o", label="Measured, worst case")
    ax.plot(sizes, series("typical", "messages"), "k--s", label="Measured, typical")
    ax.plot(sizes, series("best", "messages"), "k:^", label="Measured, best case")
    ax.plot(sizes, [n * n - 1 for n in sizes], color="0.5", lw=1, label=r"Theory $N^2-1$")
    ax.plot(sizes, [n - 1 for n in sizes], color="0.5", lw=1, ls="--", label=r"Theory $N-1$")
    ax.set_xlabel("Number of nodes N"); ax.set_ylabel("Election messages")
    ax.set_title("Bully election: message complexity"); ax.legend(frameon=False); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "m4_message_complexity.png"), dpi=200)
    plt.close(fig)

    # Figure 2: synchronization delay
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(sizes, series("worst", "delay_ms"), "k-o", label="Worst case")
    ax.plot(sizes, series("typical", "delay_ms"), "k--s", label="Typical")
    ax.plot(sizes, series("best", "delay_ms"), "k:^", label="Best case")
    ax.set_xlabel("Number of nodes N"); ax.set_ylabel("Time until all nodes agree (ms)")
    ax.set_title("Synchronization delay vs cluster size"); ax.legend(frameon=False); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "m4_sync_delay.png"), dpi=200)
    plt.close(fig)

    # Figure 3: recovery breakdown
    if e2:
        fig, ax = plt.subplots(figsize=(6, 4))
        ns = list(e2.keys()); w = 0.35
        det = [statistics.mean([r["detection_ms"] for r in e2[n] if r["ok"]]) for n in ns]
        ele = [statistics.mean([r["election_ms"] for r in e2[n] if r["ok"]]) for n in ns]
        x = range(len(ns))
        ax.bar([i for i in x], det, w * 2, color="white", edgecolor="black", hatch="//",
               label="Failure detection")
        ax.bar([i for i in x], ele, w * 2, bottom=det, color="0.75", edgecolor="black",
               label="Re-election")
        ax.set_xticks(list(x)); ax.set_xticklabels([f"N = {n}" for n in ns])
        ax.set_ylabel("Time after leader crash (ms)"); ax.set_title("Leader failure recovery")
        ax.legend(frameon=False); fig.tight_layout()
        fig.savefig(os.path.join(outdir, "m4_recovery.png"), dpi=200); plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--sizes", default=None, help="comma list, e.g. 3,5,7")
    ap.add_argument("--reps", type=int, default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    args = ap.parse_args()

    sizes = [int(x) for x in args.sizes.split(",")] if args.sizes else \
        ([3, 5, 7] if args.quick else [3, 5, 7, 9, 11, 13])
    reps = args.reps or (3 if args.quick else 5)
    e2_sizes = [5] if args.quick else [5, 9]
    n_msgs = 100 if args.quick else 300
    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)

    print("=" * 66)
    print(" MILESTONE 4 - DISTRIBUTED COORDINATION EXPERIMENTS")
    print("=" * 66)
    e1 = experiment_e1(sizes, reps, rng)
    e2 = experiment_e2(e2_sizes, reps)
    e3 = experiment_e3(5, n_msgs, rng)

    text, passed = make_report(e1, e2, e3, args.out)
    make_charts(e1, e2, args.out)
    with open(os.path.join(args.out, "milestone4_results.json"), "w") as f:
        json.dump({"e1": e1, "e2": e2, "e3": e3}, f, indent=2, default=str)
    print("\n" + text)
    print("\nFiles written to", args.out)
    print("OVERALL:", "PASS" if passed else "FAIL")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

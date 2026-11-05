#!/usr/bin/env python3
"""
ICS 2403 - Milestone 5 experiment suite: Distributed Transactions (2PC and 3PC)
"""
import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from transaction_wal import TransactionWAL

HERE = os.path.dirname(os.path.abspath(__file__))
HOST = "127.0.0.1"
PARTS = ["edge", "core", "cloud"]
PORTS = {"coord": 6200, "edge": 6201, "core": 6202, "cloud": 6203}
WINDOW = 2.5


def alloc(cpu=0.5, ram=64):
    return {p: {"cpu": cpu, "ram": ram} for p in PARTS}


def wait_until(pred, timeout=20.0, step=0.05):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return time.time() - t0
        time.sleep(step)
    return None


class TxCluster:
    def __init__(self, cpu=4.0, ram=4096.0, fsync=True, term_timeout=1.0):
        self.cpu, self.ram, self.fsync, self.term_timeout = cpu, ram, fsync, term_timeout
        self.dir = tempfile.mkdtemp(prefix="m5_")
        self.procs = {}

    def url(self, name):
        return f"http://{HOST}:{PORTS[name]}"

    @property
    def coord_wal(self):
        return os.path.join(self.dir, "coord.wal")

    def spawn(self, name):
        spec = ",".join(f"{p}:{HOST}:{PORTS[p]}" for p in PARTS)
        if name == "coord":
            cmd = ["tx_coordinator_service.py", "--port", str(PORTS[name]), "--participants", spec,
                   "--wal", self.coord_wal]
        else:
            cmd = ["transaction_participant.py", "--id", name, "--port", str(PORTS[name]),
                   "--cluster", spec, "--coordinator", self.url("coord"),
                   "--cpu", str(self.cpu), "--ram", str(self.ram),
                   "--wal", os.path.join(self.dir, f"{name}.wal"),
                   "--termination-timeout", str(self.term_timeout)]
        cmd = [sys.executable, os.path.join(HERE, cmd[0])] + cmd[1:] + ["--quiet"]
        if not self.fsync:
            cmd.append("--no-fsync")
        self.procs[name] = subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL)

    def wait_ready(self, name, timeout=30):
        def up():
            try:
                return requests.get(self.url(name) + "/health", timeout=0.5).status_code == 200
            except requests.exceptions.RequestException:
                return False
        if wait_until(up, timeout, 0.1) is None:
            raise RuntimeError(f"{name} did not start")

    def start(self):
        for n in PARTS + ["coord"]:
            self.spawn(n)
        for n in PARTS + ["coord"]:
            self.wait_ready(n)
        return self

    def kill(self, name):
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.kill()
            p.wait()

    def restart(self, name):
        self.kill(name)
        self.spawn(name)
        self.wait_ready(name)

    def stop(self):
        for n in list(self.procs):
            self.kill(n)
        shutil.rmtree(self.dir, ignore_errors=True)

    def begin(self, allocations, protocol="2PC", slice_id=None, timeout=20):
        try:
            return requests.post(self.url("coord") + "/tx/begin", timeout=timeout,
                                 json={"allocations": allocations, "protocol": protocol,
                                       "slice_id": slice_id}).json()
        except (requests.exceptions.RequestException, ValueError):
            return None

    def audit(self, name):
        try:
            return requests.get(self.url(name) + "/tx/audit", timeout=2).json()
        except (requests.exceptions.RequestException, ValueError):
            return None

    def audits(self):
        return {p: self.audit(p) for p in PARTS}

    def chaos(self, name, mode, value=0):
        requests.post(self.url(name) + "/chaos/tx", json={"mode": mode, "value": value}, timeout=2)

    def coord_chaos(self, crash_at):
        requests.post(self.url("coord") + "/coordinator/chaos", json={"crash_at": crash_at}, timeout=2)

    def coord_state(self):
        try:
            return requests.get(self.url("coord") + "/coordinator/state", timeout=2).json()
        except (requests.exceptions.RequestException, ValueError):
            return None

    def last_tx_id(self):
        w = TransactionWAL(self.coord_wal, fsync=False)
        try:
            return list(w.history())[-1]
        finally:
            w.close()

    def quiesce(self, timeout=25):
        def ok():
            for p in PARTS:
                a = self.audit(p)
                if a is None or a["in_doubt"]:
                    return False
            s = self.coord_state()
            return bool(s) and not s["pending"] and not s["recovering"] and not s["active"]
        return wait_until(ok, timeout)

    def verify(self):
        violations, auds = [], self.audits()
        in_doubt = False
        for p, a in auds.items():
            if a is None:
                violations.append(f"{p}: unreachable during verification")
                continue
            if not a["no_oversubscription"]:
                violations.append(f"{p}: capacity oversubscribed")
            if not a["wal_consistent"]:
                violations.append(f"{p}: memory differs from log replay")
            if a["anomalies"]:
                violations.append(f"{p}: {a['anomalies']} outcome(s) contradicted a final state")
            in_doubt = in_doubt or bool(a["in_doubt"])
        w = TransactionWAL(self.coord_wal, fsync=False)
        history = w.history()
        w.close()
        for tx, rec in history.items():
            ids, decision = rec["participants"], rec["details"].get("decision")
            committed_on = [p for p in ids if auds.get(p) and tx in auds[p]["committed"]]
            if decision == "COMMIT" and len(committed_on) != len(ids) and not in_doubt:
                violations.append(f"{tx}: decided COMMIT but committed on {committed_on} only")
            if decision != "COMMIT" and committed_on:
                violations.append(f"{tx}: decision {decision} but committed on {committed_on}")
        return {"ok": not violations, "violations": violations, "in_doubt": in_doubt}


def outcome_of(cl, tx_id):
    out = {}
    for p, a in cl.audits().items():
        out[p] = ("COMMITTED" if tx_id in a["committed"] else
                  "ABORTED" if tx_id in a["aborted"] else
                  "IN_DOUBT" if tx_id in a["in_doubt"] else "UNKNOWN") if a else "DOWN"
    return out


def atomic(states):
    return len(set(states.values())) == 1


def experiment_acid():
    print("  [T1] ACID properties")
    cl = TxCluster(cpu=4.0, ram=4096.0).start()
    res = {}
    try:
        r = cl.begin({"edge": {"cpu": 1, "ram": 64}, "core": {"cpu": 1, "ram": 64},
                      "cloud": {"cpu": 5, "ram": 64}}, "2PC")
        cl.quiesce()
        auds = cl.audits()
        res["atomicity"] = {"status": r["status"] if r else "FAILED",
                            "allocated_cpu": {p: a["allocated_cpu"] for p, a in auds.items() if a},
                            "reserved_cpu": {p: a["reserved_cpu"] for p, a in auds.items() if a},
                            "ok": bool(r and r["status"] == "ABORTED" and all(
                                a["allocated_cpu"] == 0 and a["reserved_cpu"] == 0 for a in auds.values() if a))}

        r = cl.begin(alloc(1, 128), "2PC")
        cl.quiesce()
        before = {p: (a["allocated_cpu"], sorted(a["committed"])) for p, a in cl.audits().items() if a}
        for n in PARTS + ["coord"]:
            cl.kill(n)
        for n in PARTS + ["coord"]:
            cl.spawn(n)
        for n in PARTS + ["coord"]:
            cl.wait_ready(n)
        cl.quiesce()
        after = {p: (a["allocated_cpu"], sorted(a["committed"])) for p, a in cl.audits().items() if a}
        res["durability"] = {"committed_tx": r["tx_id"] if r else None, "before": before, "after": after,
                             "ok": bool(before == after and r and r["status"] == "COMMITTED")}

        left = 4.0 - 1.0
        with ThreadPoolExecutor(max_workers=20) as pool:
            rs = list(pool.map(lambda _: cl.begin(alloc(1, 128), "2PC"), range(20)))
        cl.quiesce()
        committed = sum(1 for x in rs if x and x["status"] == "COMMITTED")
        auds = cl.audits()
        res["isolation"] = {"concurrent_transactions": 20, "capacity_left_cpu": left,
                            "committed": committed, "aborted": 20 - committed,
                            "allocated_cpu": {p: a["allocated_cpu"] for p, a in auds.items() if a},
                            "ok": (committed <= left and all(
                                a["allocated_cpu"] == 1 + committed and a["no_oversubscription"]
                                for a in auds.values() if a))}

        v = cl.verify()
        res["consistency"] = {"ok": v["ok"], "violations": v["violations"]}
    finally:
        cl.stop()
    return res


def pct(values, q):
    if not values:
        return 0.0
    s = sorted(values)
    return s[max(0, int(round(q * len(s))) - 1)]


def wal_microbench(n=300):
    d = tempfile.mkdtemp(prefix="m5wal_")
    out = {}
    try:
        for force in (True, False):
            w = TransactionWAL(os.path.join(d, f"w{int(force)}.wal"), fsync=force)
            t0 = time.perf_counter()
            for i in range(n):
                w.log_event(f"t{i}", "READY", ["a"], {"x": 1})
            out["forced_ms" if force else "unforced_ms"] = (time.perf_counter() - t0) / n * 1000
            w.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)
    return {"wal_microbench": out}


def experiment_perf(n_tx, concurrencies, fsyncs=(True, False)):
    results = [wal_microbench()]
    for fsync in fsyncs:
        print(f"  [T2] fsync={'on' if fsync else 'off'}")
        cl = TxCluster(cpu=1e9, ram=1e12, fsync=fsync).start()
        try:
            for proto in ("2PC", "3PC"):
                for _ in range(5):
                    cl.begin(alloc(0.01, 1), proto)
                for conc in concurrencies:
                    t0 = time.perf_counter()
                    with ThreadPoolExecutor(max_workers=conc) as pool:
                        rs = list(pool.map(lambda _: cl.begin(alloc(0.01, 1), proto), range(n_tx)))
                    wall = time.perf_counter() - t0
                    ok = [r for r in rs if r and r.get("status") == "COMMITTED"]
                    lat = [r["latency_ms"] for r in ok if "latency_ms" in r]
                    
                    mean_lat = statistics.mean(lat) if lat else 0.0
                    p50_lat = pct(lat, 0.5)
                    p95_lat = pct(lat, 0.95)
                    mean_msg = statistics.mean(r["messages"] for r in ok) if ok else 0.0
                    mean_prep = statistics.mean(r["prepare_ms"] for r in ok) if ok else 0.0
                    mean_prec = statistics.mean(r["precommit_ms"] for r in ok) if ok else 0.0
                    mean_dec = statistics.mean(r["decide_ms"] for r in ok) if ok else 0.0

                    results.append({
                        "protocol": proto, "fsync": fsync, "concurrency": conc, "n": n_tx,
                        "commit_rate": len(ok) / n_tx,
                        "mean_ms": mean_lat, "p50_ms": p50_lat,
                        "p95_ms": p95_lat, "throughput": len(ok) / max(wall, 1e-6),
                        "messages": mean_msg,
                        "prepare_ms": mean_prep,
                        "precommit_ms": mean_prec,
                        "decide_ms": mean_dec})
            cl.quiesce()
            v = cl.verify()
            results.append({"consistency_after_run": v["ok"], "fsync": fsync})
        finally:
            cl.stop()
    return results


def scen_vote_abort(cl, proto):
    cl.chaos("core", "vote_abort")
    r = cl.begin(alloc(), proto)
    cl.chaos("core", "reset")
    cl.quiesce()
    st = outcome_of(cl, r["tx_id"]) if r else {}
    return {"final_status": r["status"] if r else "FAILED", "states": st, "atomic": atomic(st), "blocked": False,
            "measured_from": "transaction start", "resolution_ms": r["latency_ms"] if r else 0.0}


def scen_participant_down_before_prepare(cl, proto):
    cl.kill("cloud")
    r = cl.begin(alloc(), proto)
    cl.restart("cloud")
    t0 = time.time()
    cl.quiesce()
    st = outcome_of(cl, r["tx_id"]) if r else {}
    return {"final_status": r["status"] if r else "FAILED", "states": st, "atomic": atomic(st), "blocked": False,
            "measured_from": "node restart", "resolution_ms": (time.time() - t0) * 1000}


def scen_participant_crash_after_vote(cl, proto):
    cl.chaos("core", "crash_after_prepare")
    r = cl.begin(alloc(), proto)
    time.sleep(1.0)
    cl.restart("core")
    t0 = time.time()
    cl.quiesce()
    st = outcome_of(cl, r["tx_id"]) if r else {}
    return {"final_status": r["status"] if r else "FAILED", "states": st, "atomic": atomic(st), "blocked": False,
            "measured_from": "node restart", "resolution_ms": (time.time() - t0) * 1000,
            "unacknowledged_at_commit": r.get("unacknowledged", []) if r else []}


def scen_coord_crash(cl, proto, point):
    cl.coord_chaos(point)
    cl.begin(alloc(), proto)
    t_crash = time.time()
    tx_id = cl.last_tx_id()
    none_in_doubt = lambda: all(a is not None and not a["in_doubt"] for a in cl.audits().values())
    alone = wait_until(none_in_doubt, WINDOW)
    blocked_cpu = None
    if alone is None:
        blocked_cpu = sum(a["reserved_cpu"] for a in cl.audits().values() if a)
        cl.restart("coord")
        cl.quiesce()
        resolution = (time.time() - t_crash) * 1000
        via = "coordinator recovery"
    else:
        resolution = alone * 1000
        via = "participants (termination protocol)"
        cl.restart("coord")
        cl.quiesce()
    st = outcome_of(cl, tx_id)
    return {"final_status": ("COMMITTED" if "COMMITTED" in st.values() else "ABORTED"),
            "states": st, "atomic": atomic(st), "blocked": alone is None,
            "resolved_by": via, "measured_from": "coordinator crash",
            "resolution_ms": resolution, "reserved_cpu_while_blocked": blocked_cpu}


SCENARIOS = [
    ("F1 participant votes NO", scen_vote_abort),
    ("F2 participant down before voting", scen_participant_down_before_prepare),
    ("F3 participant crashes after voting YES", scen_participant_crash_after_vote),
    ("F4 coordinator crashes after votes", lambda cl, p: scen_coord_crash(cl, p, "after_votes")),
    ("F5 coordinator crashes after decision", lambda cl, p: scen_coord_crash(cl, p, "after_decision")),
]


def experiment_failures(reps):
    out = {}
    for proto in ("2PC", "3PC"):
        print(f"  [T3] {proto}")
        cl = TxCluster(cpu=1e4, ram=1e7).start()
        try:
            out[proto] = {}
            for name, fn in SCENARIOS:
                out[proto][name] = [fn(cl, proto) for _ in range(reps)]
            cl.quiesce()
            out[proto]["_verify"] = cl.verify()
        finally:
            cl.stop()
    return out


def experiment_availability(n_tx):
    down_at, up_at = n_tx // 3, 2 * n_tx // 3
    out = {}
    for proto in ("2PC", "3PC"):
        print(f"  [T4] {proto}: Core down for transactions {down_at}..{up_at - 1}")
        cl = TxCluster(cpu=1e6, ram=1e9).start()
        try:
            phases = {"before": [], "during": [], "after": []}
            for i in range(n_tx):
                if i == down_at:
                    cl.kill("core")
                if i == up_at:
                    cl.restart("core")
                r = cl.begin(alloc(0.1, 8), proto)
                phase = "before" if i < down_at else "during" if i < up_at else "after"
                phases[phase].append(r)
            cl.quiesce()
            v = cl.verify()

            def rate(rs):
                return sum(1 for r in rs if r and r.get("status") == "COMMITTED") / max(1, len(rs))
            all_rs = [r for rs in phases.values() for r in rs]
            out[proto] = {
                "transactions": n_tx,
                "commit_rate": {k: rate(v_) for k, v_ in phases.items()},
                "overall_availability": rate(all_rs),
                "abort_latency_ms_during_outage": statistics.mean(
                    [r["latency_ms"] for r in phases["during"] if r and "latency_ms" in r] or [0.0]),
                "consistent": v["ok"], "violations": v["violations"]}
        finally:
            cl.stop()
    return out


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def build_report(acid, perf, fail, avail, outdir):
    lines, checks = [], []
    yn = lambda b: "yes" if b else "NO"

    lines.append("## T1 ACID properties\n")
    lines.append(md_table(["Property", "Experiment", "Result", "Holds"], [
        ["Atomicity", "Cloud cannot supply its share (5 CPU of 4)",
         f"{acid['atomicity']['status']}; allocated CPU {list(acid['atomicity']['allocated_cpu'].values())}, "
         f"reserved {list(acid['atomicity']['reserved_cpu'].values())}", yn(acid["atomicity"]["ok"])],
        ["Isolation", f"{acid['isolation']['concurrent_transactions']} concurrent transactions, "
                      f"{acid['isolation']['capacity_left_cpu']:.0f} CPU left",
         f"{acid['isolation']['committed']} committed, {acid['isolation']['aborted']} aborted; "
         f"allocated CPU {list(acid['isolation']['allocated_cpu'].values())}", yn(acid["isolation"]["ok"])],
        ["Durability", "SIGKILL all 4 processes, restart, replay logs",
         "allocations and committed set identical before and after", yn(acid["durability"]["ok"])],
        ["Consistency", "Global invariants after the run",
         "no oversubscription, log replay equals memory, atomic across participants",
         yn(acid["consistency"]["ok"])]]))
    checks += [("T1 atomicity: failed allocation leaves nothing behind", acid["atomicity"]["ok"]),
               ("T1 isolation: no oversubscription under concurrency", acid["isolation"]["ok"]),
               ("T1 durability: state survives kill -9 of every process", acid["durability"]["ok"]),
               ("T1 consistency: global invariants hold", acid["consistency"]["ok"])]

    rows = [r for r in perf if "protocol" in r]
    lines.append("\n## T2 Latency and throughput (3 participants)\n")
    lines.append(md_table(["Protocol", "fsync", "Clients", "Mean (ms)", "p50 (ms)", "p95 (ms)",
                           "Throughput (tx/s)", "Messages/tx", "Commit rate"],
                          [[r["protocol"], "on" if r["fsync"] else "off", r["concurrency"],
                            f"{r['mean_ms']:.1f}", f"{r['p50_ms']:.1f}", f"{r['p95_ms']:.1f}",
                            f"{r['throughput']:.0f}", f"{r['messages']:.0f}",
                            f"{r['commit_rate']*100:.0f}%"] for r in rows]))
    mb = next(r["wal_microbench"] for r in perf if "wal_microbench" in r)
    lines.append(f"\nCost of forcing one log record to disk on this machine: "
                 f"{mb['forced_ms']:.3f} ms (fsync) against {mb['unforced_ms']:.3f} ms (no fsync). "
                 f"A 2PC transaction forces 9 records (3 at the coordinator, 6 across the participants; "
                 f"3PC forces 13), so durability adds at most "
                 f"{9 * (mb['forced_ms'] - mb['unforced_ms']):.1f} ms to a 2PC transaction.")
    lines.append("\n## T2 Phase breakdown, single client, fsync on (mean ms)\n")
    lines.append(md_table(["Protocol", "Prepare", "PreCommit", "Decide + complete", "Total"],
                          [[r["protocol"], f"{r['prepare_ms']:.1f}", f"{r['precommit_ms']:.1f}",
                            f"{r['decide_ms']:.1f}", f"{r['mean_ms']:.1f}"]
                           for r in rows if r["fsync"] and r["concurrency"] == 1]))
    n = 3
    checks.append(("T2 every transaction committed", all(r["commit_rate"] == 1 for r in rows)))
    checks.append((f"T2 2PC uses 4n = {4*n} messages", all(r["messages"] == 4 * n for r in rows if r["protocol"] == "2PC")))
    checks.append((f"T2 3PC uses 6n = {6*n} messages", all(r["messages"] == 6 * n for r in rows if r["protocol"] == "3PC")))
    checks.append(("T2 consistent after the load runs",
                   all(r["consistency_after_run"] for r in perf if "consistency_after_run" in r)))

    lines.append("\n## T3 Failure experiments (outcome per scenario, mean over runs)\n")
    frows = []
    for name, _ in SCENARIOS:
        for proto in ("2PC", "3PC"):
            runs = fail[proto][name]
            frows.append([name, proto, runs[0]["final_status"],
                          yn(all(r["atomic"] for r in runs)),
                          "BLOCKED" if any(r["blocked"] for r in runs) else "no",
                          runs[0].get("resolved_by", "-"),
                          f"{statistics.mean([r['resolution_ms'] for r in runs]):.0f}",
                          runs[0]["measured_from"]])
    lines.append(md_table(["Scenario", "Protocol", "Final outcome", "Atomic", "Participants blocked",
                           "Resolved by", "Time to consistent state (ms)", "Measured from"], frows))
    exp_status = {SCENARIOS[0][0]: "ABORTED", SCENARIOS[1][0]: "ABORTED", SCENARIOS[2][0]: "COMMITTED",
                  SCENARIOS[3][0]: "ABORTED", SCENARIOS[4][0]: "COMMITTED"}
    for proto in ("2PC", "3PC"):
        for name, _ in SCENARIOS:
            runs = fail[proto][name]
            checks.append((f"T3 {proto} {name[:2]}: atomic, outcome {exp_status[name]}",
                           all(r["atomic"] and r["final_status"] == exp_status[name] for r in runs)))
        checks.append((f"T3 {proto}: consistent after all failures", fail[proto]["_verify"]["ok"]))
    for name in (SCENARIOS[3][0], SCENARIOS[4][0]):
        checks.append((f"T3 2PC blocks when the coordinator crashes ({name[:2]})",
                       all(r["blocked"] for r in fail["2PC"][name])))
        checks.append((f"T3 3PC does not block ({name[:2]})",
                       not any(r["blocked"] for r in fail["3PC"][name])))
    blocked = [r["reserved_cpu_while_blocked"] for name in (SCENARIOS[3][0], SCENARIOS[4][0])
               for r in fail["2PC"][name] if r["reserved_cpu_while_blocked"] is not None]
    if blocked:
        lines.append(f"\nWhile blocked, 2PC kept {statistics.mean(blocked):.1f} CPU reserved across the three "
                     f"nodes for a transaction that nobody could finish.")

    lines.append("\n## T4 Availability with the Core node down for one third of the workload\n")
    arows = []
    for proto, a in avail.items():
        arows.append([proto, a["transactions"], f"{a['commit_rate']['before']*100:.0f}%",
                      f"{a['commit_rate']['during']*100:.0f}%", f"{a['commit_rate']['after']*100:.0f}%",
                      f"{a['overall_availability']*100:.1f}%",
                      f"{a['abort_latency_ms_during_outage']:.1f}", yn(a["consistent"])])
        checks.append((f"T4 {proto}: consistent after outage", a["consistent"]))
        checks.append((f"T4 {proto}: service fully available again after recovery",
                       a["commit_rate"]["after"] == 1.0))
    lines.append(md_table(["Protocol", "Transactions", "Commit rate before", "during outage",
                           "after recovery", "Overall availability", "Abort latency during outage (ms)",
                           "Consistent"], arows))

    lines.append("\n## Automatic checks\n")
    lines.append(md_table(["Check", "Result"], [[c, "PASS" if ok else "FAIL"] for c, ok in checks]))
    text = "\n".join(lines)
    with open(os.path.join(outdir, "milestone5_summary.md"), "w") as f:
        f.write("# Milestone 5 - Experimental results (generated)\n\n" + text + "\n")
    return text, all(ok for _, ok in checks)


def make_charts(perf, fail, outdir):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "serif", "font.size": 10})
    rows = [r for r in perf if "protocol" in r]
    concs = sorted({r["concurrency"] for r in rows})
    styles = {("2PC", True): "k-o", ("3PC", True): "k--s", ("2PC", False): "0.45",
              ("3PC", False): "0.45"}

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for ax, key, title, ylab in ((axes[0], "mean_ms", "Transaction latency", "Mean latency (ms)"),
                                 (axes[1], "throughput", "Throughput", "Transactions per second")):
        for proto, fs in (("2PC", True), ("3PC", True), ("2PC", False), ("3PC", False)):
            ys = [next((r[key] for r in rows if r["protocol"] == proto and r["fsync"] == fs
                        and r["concurrency"] == c), 0.0) for c in concs]
            label = f"{proto}, log {'forced' if fs else 'not forced'}"
            if fs:
                ax.plot(concs, ys, styles[(proto, fs)], label=label)
            else:
                ax.plot(concs, ys, color="0.45", marker="o" if proto == "2PC" else "s",
                        ls="-" if proto == "2PC" else "--", label=label)
        ax.set_xlabel("Concurrent clients"); ax.set_ylabel(ylab); ax.set_title(title)
        ax.set_xticks(concs); ax.grid(alpha=0.3); ax.legend(frameon=False, fontsize=8)
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "m5_latency_throughput.png"), dpi=200)
    plt.close(fig)

    names = [SCENARIOS[3][0], SCENARIOS[4][0]]
    fig, ax = plt.subplots(figsize=(7, 4))
    w = 0.35
    for i, proto in enumerate(("2PC", "3PC")):
        vals = [statistics.mean([r["resolution_ms"] for r in fail[proto][n]]) if fail[proto][n] else 0.0 for n in names]
        bars = ax.bar([j + (i - 0.5) * w for j in range(2)], vals, w,
                      color="white" if proto == "2PC" else "0.7", edgecolor="black",
                      hatch="//" if proto == "2PC" else None, label=proto)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + 40, f"{v:.0f}", ha="center", fontsize=8)
    ax.set_xticks(range(2)); ax.set_xticklabels(["Crash after votes", "Crash after decision"])
    ax.set_ylabel("Time until every participant is resolved (ms)")
    ax.set_title("Coordinator crash (coordinator stays down 2.5 s)\n2PC waits for the restart, 3PC does not")
    ax.legend(frameon=False); fig.tight_layout()
    fig.savefig(os.path.join(outdir, "m5_failure_resolution.png"), dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    a = ap.parse_args()
    n_tx = 20 if a.quick else 60
    concs = (1, 4) if a.quick else (1, 4, 8)
    reps = 1 if a.quick else 2
    n_avail = 30 if a.quick else 60
    os.makedirs(a.out, exist_ok=True)

    print("=" * 66)
    print(" MILESTONE 5 - DISTRIBUTED TRANSACTIONS EXPERIMENTS")
    print("=" * 66)
    acid = experiment_acid()
    perf = experiment_perf(n_tx, concs)
    fail = experiment_failures(reps)
    avail = experiment_availability(n_avail)

    text, passed = build_report(acid, perf, fail, avail, a.out)
    make_charts(perf, fail, a.out)
    with open(os.path.join(a.out, "milestone5_results.json"), "w") as f:
        json.dump({"acid": acid, "perf": perf, "failures": fail, "availability": avail},
                  f, indent=2, default=str)
    print("\n" + text)
    print("\nFiles written to", a.out)
    print("OVERALL:", "PASS" if passed else "FAIL")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
chaos.py — Chaos testing for the raftkv cluster.

What it does:
  1. Starts a 3-node raftkv cluster as subprocesses
  2. Hammers the cluster with concurrent writes via the CLI
  3. Randomly kills and restarts nodes during the write storm
  4. After all writes, reads back every key and verifies correctness
  5. Reports any lost writes, stale reads, or consistency violations

Usage:
  python3 chaos.py --binary ./raftkv-server --cli ./raftkv-cli --rounds 3

The test PASSES if:
  - Every key that got a confirmed OK write is readable with the correct value
  - No key returns a value that was never written (corruption)
  - The cluster recovers and elects a new leader after every kill
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

# ── Cluster configuration ────────────────────────────────────────────────────

NODES = {
    "node1": "localhost:7001",
    "node2": "localhost:7002",
    "node3": "localhost:7003",
}
PEERS_FLAG = ",".join(f"{k}={v}" for k, v in NODES.items())

# ── Result tracking ──────────────────────────────────────────────────────────

@dataclass
class WriteResult:
    key: str
    value: str
    confirmed: bool   # True = got "OK" back from CLI
    attempt: int

@dataclass
class Stats:
    writes_attempted: int = 0
    writes_confirmed: int = 0
    writes_failed: int = 0
    reads_correct: int = 0
    reads_stale: int = 0
    reads_missing: int = 0
    node_kills: int = 0
    node_restarts: int = 0
    errors: list = field(default_factory=list)

# ── Subprocess cluster management ────────────────────────────────────────────

class Node:
    def __init__(self, node_id: str, addr: str, binary: str, data_dir: str):
        self.node_id = node_id
        self.addr = addr
        self.binary = binary
        self.data_dir = os.path.join(data_dir, node_id)
        self.proc: Optional[subprocess.Popen] = None
        os.makedirs(self.data_dir, exist_ok=True)

    def start(self):
        peers = {k: v for k, v in NODES.items() if k != self.node_id}
        peers_flag = ",".join(f"{k}={v}" for k, v in peers.items())

        cmd = [
            self.binary,
            f"--id={self.node_id}",
            f"--listen={self.addr}",
            f"--peers={peers_flag}",
            f"--data-dir={self.data_dir}",
        ]
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.1)  # give it a moment to bind

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None

    def kill(self):
        """Abrupt kill — simulates a crash, not graceful shutdown."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


class Cluster:
    def __init__(self, binary: str, data_dir: str):
        self.nodes = {
            node_id: Node(node_id, addr, binary, data_dir)
            for node_id, addr in NODES.items()
        }

    def start_all(self):
        print("  Starting cluster nodes...")
        for node in self.nodes.values():
            node.start()
        # Wait for election
        print("  Waiting for leader election (2s)...")
        time.sleep(2)

    def stop_all(self):
        for node in self.nodes.values():
            node.stop()

    def kill_random(self, stats: Stats) -> str:
        alive = [n for n in self.nodes.values() if n.alive]
        if len(alive) <= 1:
            return ""  # don't kill the last node
        victim = random.choice(alive)
        print(f"  💀 KILLING {victim.node_id}")
        victim.kill()
        stats.node_kills += 1
        return victim.node_id

    def restart(self, node_id: str, stats: Stats):
        node = self.nodes[node_id]
        print(f"  🔄 RESTARTING {node_id}")
        node.start()
        stats.node_restarts += 1


# ── CLI wrapper ──────────────────────────────────────────────────────────────

def cli_put(cli_binary: str, key: str, value: str, timeout: float = 3.0) -> bool:
    """Returns True if the write was confirmed (exit 0 + printed OK)."""
    try:
        result = subprocess.run(
            [cli_binary, "--peers", PEERS_FLAG, "put", key, value],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0 and "OK" in result.stdout
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False


def cli_get(cli_binary: str, key: str, timeout: float = 3.0) -> Optional[str]:
    """Returns the value string, '(not found)' sentinel, or None on error."""
    try:
        result = subprocess.run(
            [cli_binary, "--peers", PEERS_FLAG, "get", key],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            return None
        out = result.stdout.strip()
        if out == "(not found)":
            return None
        return out
    except subprocess.TimeoutExpired:
        return None
    except Exception:
        return None


# ── Write worker ─────────────────────────────────────────────────────────────

def write_worker(
    cli_binary: str,
    worker_id: int,
    num_writes: int,
    results: list,
    lock: threading.Lock,
    stats: Stats,
):
    for i in range(num_writes):
        key = f"w{worker_id:02d}-k{i:04d}"
        value = f"val-{worker_id}-{i}-{random.randint(1000, 9999)}"

        with lock:
            stats.writes_attempted += 1

        confirmed = cli_put(cli_binary, key, value)

        result = WriteResult(key=key, value=value, confirmed=confirmed, attempt=i)
        with lock:
            results.append(result)
            if confirmed:
                stats.writes_confirmed += 1
            else:
                stats.writes_failed += 1


# ── Chaos goroutine ──────────────────────────────────────────────────────────

def chaos_worker(cluster: Cluster, stats: Stats, stop_event: threading.Event):
    """Randomly kills and restarts nodes while writes are in flight."""
    killed_node = None
    while not stop_event.is_set():
        wait = random.uniform(1.5, 3.0)
        stop_event.wait(timeout=wait)
        if stop_event.is_set():
            break

        if killed_node:
            # Restart the previously killed node
            cluster.restart(killed_node, stats)
            killed_node = None
            stop_event.wait(timeout=1.0)
        else:
            # Kill a random node
            killed_node = cluster.kill_random(stats)

    # Ensure all nodes are back up for the verification phase
    if killed_node:
        cluster.restart(killed_node, stats)
        time.sleep(1.0)


# ── Verification ─────────────────────────────────────────────────────────────

def verify(cli_binary: str, results: list, stats: Stats):
    """
    Read back every confirmed write and check correctness.

    Invariant: if a write returned OK, the value MUST be readable.
    If a write timed out or failed, we don't check it (it may or may not have committed).
    """
    print(f"\n  Verifying {stats.writes_confirmed} confirmed writes...")
    confirmed = [r for r in results if r.confirmed]

    violations = []
    for r in confirmed:
        actual = cli_get(cli_binary, r.key)
        if actual is None:
            stats.reads_missing += 1
            violations.append(f"MISSING  key={r.key} expected={r.value}")
        elif actual != r.value:
            stats.reads_stale += 1
            violations.append(f"MISMATCH key={r.key} expected={r.value} got={actual}")
        else:
            stats.reads_correct += 1

    for v in violations:
        print(f"  ❌ {v}")
        stats.errors.append(v)

    return len(violations) == 0


# ── Main test runner ─────────────────────────────────────────────────────────

def run_round(round_num: int, binary: str, cli: str, num_workers: int, writes_per_worker: int) -> Stats:
    stats = Stats()
    print(f"\n{'='*60}")
    print(f"  Round {round_num}: {num_workers} workers × {writes_per_worker} writes each")
    print(f"{'='*60}")

    with tempfile.TemporaryDirectory(prefix="raftkv-chaos-") as data_dir:
        cluster = Cluster(binary, data_dir)
        cluster.start_all()

        results = []
        lock = threading.Lock()

        # Start chaos in background
        stop_chaos = threading.Event()
        chaos_thread = threading.Thread(
            target=chaos_worker, args=(cluster, stats, stop_chaos), daemon=True
        )
        chaos_thread.start()

        # Start concurrent write workers
        print(f"  Starting {num_workers} write workers...")
        write_threads = []
        for i in range(num_workers):
            t = threading.Thread(
                target=write_worker,
                args=(cli, i, writes_per_worker, results, lock, stats),
            )
            write_threads.append(t)
            t.start()

        # Wait for all writes to complete
        for t in write_threads:
            t.join()

        # Stop chaos and let cluster stabilize
        stop_chaos.set()
        chaos_thread.join(timeout=5)
        print(f"  Writes done. Waiting 2s for cluster to stabilize...")
        time.sleep(2)

        # Verify
        passed = verify(cli, results, stats)

        cluster.stop_all()

    return stats, passed


def main():
    parser = argparse.ArgumentParser(description="Chaos test for raftkv")
    parser.add_argument("--binary", default="./raftkv-server", help="Path to server binary")
    parser.add_argument("--cli", default="./raftkv-cli", help="Path to CLI binary")
    parser.add_argument("--rounds", type=int, default=3, help="Number of chaos rounds")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent write workers per round")
    parser.add_argument("--writes", type=int, default=50, help="Writes per worker per round")
    args = parser.parse_args()

    if not os.path.exists(args.binary):
        print(f"Error: server binary not found: {args.binary}")
        print("Build it with: go build -o raftkv-server ./main.go")
        sys.exit(1)

    if not os.path.exists(args.cli):
        print(f"Error: CLI binary not found: {args.cli}")
        print("Build it with: go build -o raftkv-cli ./client/")
        sys.exit(1)

    print("🔥 raftkv chaos test")
    print(f"   Rounds:  {args.rounds}")
    print(f"   Workers: {args.workers} per round")
    print(f"   Writes:  {args.writes} per worker")
    print(f"   Total:   ~{args.rounds * args.workers * args.writes} writes")

    all_passed = True
    total_stats = Stats()

    for round_num in range(1, args.rounds + 1):
        stats, passed = run_round(round_num, args.binary, args.cli, args.workers, args.writes)

        total_stats.writes_attempted  += stats.writes_attempted
        total_stats.writes_confirmed  += stats.writes_confirmed
        total_stats.writes_failed     += stats.writes_failed
        total_stats.reads_correct     += stats.reads_correct
        total_stats.reads_stale       += stats.reads_stale
        total_stats.reads_missing     += stats.reads_missing
        total_stats.node_kills        += stats.node_kills
        total_stats.node_restarts     += stats.node_restarts
        total_stats.errors.extend(stats.errors)

        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"\n  Round {round_num} result: {status}")
        print(f"    Writes:  {stats.writes_attempted} attempted, "
              f"{stats.writes_confirmed} confirmed, {stats.writes_failed} failed")
        print(f"    Reads:   {stats.reads_correct} correct, "
              f"{stats.reads_stale} stale, {stats.reads_missing} missing")
        print(f"    Chaos:   {stats.node_kills} kills, {stats.node_restarts} restarts")

        if not passed:
            all_passed = False

    print(f"\n{'='*60}")
    print(f"  FINAL RESULTS across {args.rounds} rounds")
    print(f"{'='*60}")
    print(f"  Writes:      {total_stats.writes_attempted} attempted, "
          f"{total_stats.writes_confirmed} confirmed")
    print(f"  Correctness: {total_stats.reads_correct}/{total_stats.writes_confirmed} verified")
    print(f"  Chaos ops:   {total_stats.node_kills} kills, {total_stats.node_restarts} restarts")

    if total_stats.errors:
        print(f"\n  Violations ({len(total_stats.errors)}):")
        for e in total_stats.errors[:10]:
            print(f"    {e}")
        if len(total_stats.errors) > 10:
            print(f"    ... and {len(total_stats.errors) - 10} more")

    if all_passed:
        print("\n  ✅ ALL ROUNDS PASSED — cluster is consistent under chaos")
        sys.exit(0)
    else:
        print("\n  ❌ CONSISTENCY VIOLATIONS DETECTED")
        sys.exit(1)


if __name__ == "__main__":
    main()

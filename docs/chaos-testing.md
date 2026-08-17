# Chaos Testing

## Why chaos tests

Unit tests prove that individual components behave correctly in isolation. Integration tests prove that components work together under normal conditions. Neither proves that the system is correct when things go wrong at the worst possible moment.

Chaos testing fills this gap. It deliberately injects failures - random node kills, during active writes - and then verifies that the system's safety guarantees held throughout.

For a distributed system, the safety guarantee is: **every write that received an OK response must remain readable with the correct value, even if nodes crash during or after the write.**

Source: [chaos/chaos.py](../chaos/chaos.py)

---

## How it works

```
┌─────────────────────────────────────────────────────────────────┐
│  Chaos round                                                    │
│                                                                 │
│  1. Start 3-node cluster as subprocesses                        │
│  2. Wait 2s for leader election                                 │
│                                                                 │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐ │
│  │  Write workers (4×)      │  │  Chaos worker                │ │
│  │                          │  │                              │ │
│  │  for 50 writes each:     │  │  every 1.5-3s:               │ │
│  │    PUT w00-k0001 → val   │  │    KILL a random node        │ │
│  │    record (key, val,     │  │    wait 1-2s                 │ │
│  │      confirmed=OK?)      │  │    RESTART that node         │ │
│  │    ...                   │  │                              │ │
│  └──────────────────────────┘  └──────────────────────────────┘ │
│                     both run concurrently                       │
│                                                                 │
│  3. Wait for all writes to complete                             │
│  4. Stop chaos worker                                           │
│  5. Wait 2s for cluster to stabilise                            │
│                                                                 │
│  6. Verification:                                               │
│     for each confirmed write:                                   │
│       actual = GET key                                          │
│       assert actual == expected_value                           │
│                                                                 │
│  7. Stop cluster, wipe data directory                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## What "confirmed" means

A write is **confirmed** if the CLI process exited with code 0 and printed `OK`. This means:

- The entry was committed on a majority of nodes
- The leader applied it to its state machine
- The leader responded to the client

An unconfirmed write (timeout, connection refused, non-zero exit) may or may not have committed. The test does not verify unconfirmed writes - they might be in the log on some nodes or they might not. Both outcomes are correct.

Only confirmed writes are checked in the verification phase. This is the right invariant: **an OK response is a durability promise.**

---

## Failure scenarios exercised

During a typical 3-round chaos test with 4 workers and 50 writes each:

| Scenario | How it arises |
|----------|--------------|
| Leader crash mid-replication | Kill fires while leader is sending AppendEntries |
| Follower crash | Kill fires on a non-leader; write proceeds on remaining 2/3 nodes |
| Leader crash after commit, before response | Write committed on majority, leader dies before client sees OK |
| Node restart and log replay | Killed node restarts, replays WAL, catches up via AppendEntries |
| Election during active writes | New leader elected; pending writes either committed or retried |
| Minority partition | If we kill 2/3 nodes, writes start failing (expected - no majority) |

The chaos worker deliberately avoids killing the last alive node (`len(alive) <= 1 → skip`), since a 1-node partition can never commit - writes would hang indefinitely and the test would timeout rather than catching bugs.

---

## What the test proves

**Safety:** For every write that returned OK, the value must still be correct after all the chaos. The verification checks both:
- `MISSING`: a confirmed write whose key returns not-found
- `MISMATCH`: a confirmed write whose key returns a different value

Either is a consistency violation. A MISSING key means a committed entry was somehow lost. A MISMATCH means a later write (that was never committed as far as the client knows) somehow overrode a committed entry.

**Liveness:** The cluster must recover and accept new writes after every kill-restart cycle. If the cluster gets permanently stuck (split-brain, deadlocked goroutine, etc.), the write workers timeout and the test eventually fails. This is not an explicit liveness check but an implicit one.

---

## Why it catches bugs that unit tests miss

The unit test `TestNoCommitWithMinorityPartition` checks that an isolated leader doesn't commit. But it doesn't check what happens when:
- The leader commits entry 5, then crashes before sending the commit to followers
- A new leader is elected with a log that includes entry 5 (from the WAL on a follower)
- The new leader must not re-commit entry 5 with a different value

This scenario requires real timing and real process crashes to reproduce reliably. The chaos test creates these conditions by randomly killing processes while writes are in flight - which hits timing windows that are impossible to exercise in a deterministic unit test.

Similarly, the WAL truncation path (`TruncateSuffix`) is rarely hit in unit tests but gets exercised every time a node restarts after being killed mid-replication and discovers its log conflicts with the new leader.

---

## Running the test

```bash
# Build first
make build

# Run with defaults (3 rounds, 4 workers, 50 writes each)
make chaos

# Or directly with custom parameters
python3 chaos/chaos.py \
  --binary ./raftkv-server \
  --cli    ./raftkv-cli \
  --rounds 5 \
  --workers 8 \
  --writes 100
```

### Expected output

```
🔥 raftkv chaos test
   Rounds:  3
   Workers: 4 per round
   Writes:  50 per worker
   Total:   ~600 writes

============================================================
  Round 1: 4 workers × 50 writes each
============================================================
  Starting cluster nodes...
  Waiting for leader election (2s)...
  Starting 4 write workers...
  💀 KILLING node2
  🔄 RESTARTING node2
  💀 KILLING node1
  🔄 RESTARTING node1
  Writes done. Waiting 2s for cluster to stabilize...

  Verifying 187 confirmed writes...

  Round 1 result: ✅ PASS
    Writes:  200 attempted, 187 confirmed, 13 failed
    Reads:   187 correct, 0 stale, 0 missing
    Chaos:   3 kills, 3 restarts
```

A pass means: all 187 writes that returned OK are still readable with their original values.

The 13 "failed" writes timed out or hit errors during the kill window - those may or may not have committed (the client never got an OK), so they are not checked.

---

## Interpreting failures

**MISSING key:** A confirmed write is not found on read. This means either:
- A committed entry was lost during WAL replay (data corruption)
- The state machine's apply loop skipped an entry incorrectly
- The storage engine lost data during a flush/compaction cycle

**MISMATCH key:** A confirmed write has the wrong value. This means either:
- An uncommitted write was applied (state machine safety violation)
- A tombstone incorrectly marked a live value as deleted (storage bug)
- Two nodes served reads with different views of the same key (split-brain)

Both are serious correctness violations. If you see them, check:
1. WAL replay: does `ReadAll` return the correct entries in order?
2. `TruncateSuffix`: are entries being correctly removed or preserved?
3. `maybeAdvanceCommit`: is quorum counting correct?
4. `apply`: is `lastApplied` preventing re-application?

---

## Cluster management in the test

The chaos harness starts each node as a subprocess:

```python
cmd = [
    binary,
    f"--id={node_id}",
    f"--listen={addr}",
    f"--peers={peers_flag}",
    f"--data-dir={data_dir}",
]
proc = subprocess.Popen(cmd, stdout=DEVNULL, stderr=DEVNULL)
```

Each round uses a fresh temporary directory (`tempfile.TemporaryDirectory`), so there is no leftover state between rounds. This ensures each round starts from a clean slate.

Node kills use `SIGKILL` (not `SIGTERM`) to simulate a hard crash - no graceful shutdown, no final WAL flush, no cleanup. This is the most adversarial scenario.

Node restarts reuse the same data directory, so the WAL from before the kill is available for replay. This exercises the recovery path on every restart.

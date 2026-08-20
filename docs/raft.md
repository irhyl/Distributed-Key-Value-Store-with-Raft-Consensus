# Raft Consensus

## What Raft solves

Multiple machines need to agree on a sequence of operations. If they apply the same operations in the same order, they arrive at the same state - that's the invariant. The challenge is reaching that agreement when machines can crash, network partitions can isolate nodes, and messages can arrive out of order or be lost entirely.

Raft breaks this problem into three mostly-independent sub-problems:

1. **Leader election** - elect exactly one leader per term
2. **Log replication** - the leader accepts entries and replicates them
3. **Safety** - once an entry is committed, no future leader will overwrite it

---

## Node roles and state transitions

```
          ┌─────────────────────────────┐
          │         Candidate           │
          │   Increments term,          │◄──────────────┐
          │   votes for self,           │               │
          │   requests votes from peers │  Split vote   │
          └──────────┬──────────────────┘  or timeout   │
                     │                                  │
        Majority     │                  Election        │
        vote         │                  timeout         │
                     ▼                                  │
          ┌─────────────────────────────┐               │
     ┌───►│           Leader            │               │
     │    │   Sends AppendEntries,      │               │
     │    │   heartbeats every 50ms,    │               │
     │    │   commits on majority ACK   │               │
     │    └──────────┬──────────────────┘               │
     │               │                                  │
     │   Higher term │                                  │
     │   seen        ▼                                  │
     │    ┌─────────────────────────────┐               │
start│    │          Follower           │               │
─────┴───►│   Resets election timer     ├───────────────┘
          │   when it hears from leader │  Election timeout
          └─────────────────────────────┘  (no heartbeat received)
```

Every node starts as a **Follower**. A follower that hears no heartbeat within its election timeout becomes a **Candidate** and starts an election. A candidate that receives votes from a majority becomes the **Leader**. Any node that sees a message with a higher term immediately steps back to Follower - this is the universal rule that prevents split-brain.

---

## Terms

A **term** is a logical clock. It's a monotonically increasing integer that advances every time a new election starts. Terms serve two purposes:

1. They let nodes detect stale messages. If a message arrives with a lower term than the receiver's current term, it's rejected outright.
2. They identify which "era" a log entry belongs to. The safety proof depends on tracking which term each entry was written in.

When a node sees a message with a higher term than its own, it immediately sets its term to that higher value and converts to Follower. This happens unconditionally, in every RPC handler, before any other logic.

---

## Leader election

### Starting an election

When a follower's election timer fires with no heartbeat received:

```
1. Increment currentTerm
2. Transition to Candidate
3. Vote for self (votedFor = self.ID)
4. Persist (currentTerm, votedFor) to WAL hard state
5. Reset election timer (to retry if this election times out)
6. Send RequestVote to all peers in parallel
```

### Granting a vote

A voter grants its vote only if all three conditions hold:

| Condition | Rule |
|-----------|------|
| Fresh term | `req.Term >= currentTerm` |
| Not already voted | `votedFor == ""` or `votedFor == req.CandidateID` |
| Candidate log is up-to-date | `req.LastLogTerm > lastLogTerm` or (`req.LastLogTerm == lastLogTerm` and `req.LastLogIndex >= lastLogIndex`) |

The third condition is the **election restriction**. It prevents a candidate with a stale log from winning an election. Without it, a new leader could be missing committed entries and overwrite them.

### Winning the election

A candidate wins when it accumulates votes from a majority (including its own). The majority threshold is `⌊N/2⌋ + 1` where N is the total cluster size. For a 3-node cluster, majority = 2.

A single-node cluster has majority = 1. After voting for itself, the node wins immediately - the election loop recognises this before contacting any peers.

### Randomised timeouts

Every node picks a random election timeout in the range `[150ms, 300ms]`. This is critical. Without randomisation, all followers would time out simultaneously, all become candidates, all vote for themselves, and nobody reaches majority. Randomisation ensures one node almost always fires before others and wins uncontested.

---

## Log replication

### AppendEntries

The leader replicates entries to followers using `AppendEntries`. The same RPC serves as a heartbeat when sent with empty entries - followers reset their election timers whenever they see it.

The request includes:

| Field | Purpose |
|-------|---------|
| `term` | Leader's current term |
| `leaderID` | So followers can tell clients where to redirect |
| `prevLogIndex` | Index of the entry immediately before the new ones |
| `prevLogTerm` | Term of that entry |
| `entries` | The new entries to append (empty = heartbeat) |
| `leaderCommit` | Leader's current commitIndex |

### Log consistency check

Before appending entries, the follower checks the **log consistency invariant**: the entry at `prevLogIndex` in the follower's log must have term `prevLogTerm`. If not, the follower rejects the RPC and tells the leader where the conflict starts.

This guarantees that if two logs agree at index `i`, they agree at all indices `j ≤ i`. It's the **Log Matching property** from the Raft paper.

### Fast rollback

The naive conflict resolution is to back up `nextIndex` by one and retry. For a lagging follower this takes O(log length) round trips. The optimisation (implemented here) returns:

- `ConflictTerm`: the term of the conflicting entry at prevLogIndex
- `ConflictIndex`: the first index in the follower's log with that term

The leader scans backwards to find its last entry with `ConflictTerm`. If it has none, it jumps straight to `ConflictIndex`. This resolves conflicts in one round trip rather than one per entry.

### Tracking replication progress

The leader maintains two arrays, indexed by peer ID:

- `nextIndex[peer]` - the next log index to send to that peer (optimistic)
- `matchIndex[peer]` - the highest index confirmed replicated on that peer (pessimistic)

On a successful `AppendEntries`, `matchIndex` advances to `prevLogIndex + len(entries)` and `nextIndex` to `matchIndex + 1`.

On failure, `nextIndex` is reduced using the conflict hint and the RPC is retried.

---

## Commitment

An entry is **committed** when it has been written to a majority of nodes' logs. Once committed, it will never be lost - any future leader will have it.

### Advancing commitIndex

After each successful replication, the leader calls `maybeAdvanceCommit`:

```go
for idx := lastLogIndex; idx > commitIndex; idx-- {
    // Safety rule: only commit entries from the current term
    if log[idx].Term != currentTerm {
        continue
    }
    // Count: self (1) + each peer where matchIndex >= idx
    count := 1
    for peer, match := range matchIndex {
        if match >= idx {
            count++
        }
    }
    if count >= quorum {
        advanceCommitTo(idx)
        break
    }
}
```

### The current-term rule (Figure 8)

The `if log[idx].Term != currentTerm` check is the most subtle correctness requirement in Raft. Here is why it exists.

**Scenario:**
```
Term 1:  S1 is leader. Appends entry at index 2. Replicates to S2 but not S3.
         S1 crashes.

Term 2:  S3 becomes leader (it was eligible: its log is a prefix, term 2 > term 1).
         S3 overwrites index 2 with a different entry.

Now:  Entry at index 2 from term 1 was on S1 and S2 (majority).
      If a new leader could commit it based on replication count alone,
      S3 would apply a different entry at index 2.
      Two nodes would apply different commands at the same index: safety violation.
```

**Fix:** A leader can only directly commit entries from its **own** term. When it commits a current-term entry, all preceding entries are committed implicitly by the log prefix invariant. The dangerous term-1 entry would get committed only as a side effect of committing a term-3 entry that precedes it - and at that point we know term-3's leader had it in its log, meaning S3 accepted it rather than overwriting it.

In code: `if log[idx].Term != currentTerm { continue }`.

---

## Safety guarantees

Raft guarantees these properties at all times:

| Property | Guarantee |
|----------|-----------|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites or deletes entries in its log |
| **Log Matching** | If two logs contain an entry with the same index and term, the logs are identical up to that index |
| **Leader Completeness** | If an entry is committed in term T, every leader for term T' > T has that entry in its log |
| **State Machine Safety** | If a server applies log entry i, no other server applies a different entry at i |

The first four are maintained by the algorithm; the last follows from the first four.

---

## Heartbeats and liveness

The leader sends heartbeats (empty `AppendEntries`) every `50ms`. Followers have election timeouts between `150ms` and `300ms`. This gives the leader 3-6 heartbeat intervals before a follower decides the leader is dead and starts an election.

If a leader fails, the fastest follower times out first, starts an election, and wins before the others wake up. Total downtime is typically one election timeout (150-300ms) plus one round trip to collect votes (~10ms on LAN).

This node's election timer has no PreVote check: it fires and bumps `currentTerm` unconditionally, even while completely partitioned from the rest of the cluster. That's fine for a normal leader failure, but it means a node that stays disconnected for a while can come back with a term far ahead of everyone else's, which is disruptive on reconnection even though it can't actually win. See "No PreVote" in [design-decisions.md](design-decisions.md).

---

## Persistent state

Raft requires three pieces of state to survive crashes:

| State | What | Why |
|-------|------|-----|
| `currentTerm` | The highest term seen | Prevents re-using a term number |
| `votedFor` | Who we voted for in currentTerm | Prevents voting twice in the same term |
| `log` | Log entries since the last snapshot | Preserves committed entries across restarts |

`currentTerm` and `votedFor` are stored as a tiny JSON file (`hardstate.json`) written atomically via temp-file rename. Log entries go to the WAL. A fourth piece, the most recent snapshot (if any) and the index/term it covers, is stored the same way and lets a restart skip replaying anything the snapshot already covers - see "Log snapshotting" in [design-decisions.md](design-decisions.md).

---

## Implementation files

| File | What it contains |
|------|-----------------|
| [raft/node.go](../raft/node.go) | Everything: state machine, RPC handlers, election, replication |
| [raft/memstate.go](../raft/memstate.go) | In-memory `PersistentState` used in tests |
| [raft/memtransport.go](../raft/memtransport.go) | In-memory `Transport` with controllable partitions |
| [raft/raft_test.go](../raft/raft_test.go) | 12 tests covering election safety, log replication, safety under partition, and snapshotting |
| [server/walstate.go](../server/walstate.go) | Production `PersistentState`: delegates to WAL + hardstate.json |
| [server/transport.go](../server/transport.go) | Production `Transport`: gRPC connections to peers |

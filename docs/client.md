# CLI Client

## Overview

`raftkv-cli` is the command-line interface for interacting with a raftkv cluster. It connects directly to nodes via gRPC and handles the complexity of finding the current leader transparently - the user never needs to know which node is the leader.

Source: [client/main.go](../client/main.go)

---

## Usage

```bash
raftkv-cli --peers <peer-list> <command> [args...]
```

**Peer list format:** `nodeID=address` pairs, comma-separated.

```bash
PEERS="node1=localhost:7001,node2=localhost:7002,node3=localhost:7003"

# Write a key
raftkv-cli --peers $PEERS put greeting "hello from raft"

# Read a key
raftkv-cli --peers $PEERS get greeting

# Delete a key
raftkv-cli --peers $PEERS delete greeting
```

**Exit codes:**
- `0` - success
- `1` - error (operation failed, cluster unreachable, or bad arguments)

**Output:**
- `put` / `delete`: prints `OK` on success, nothing otherwise
- `get`: prints the value string on success, `(not found)` if the key doesn't exist

---

## Leader redirection

Raft clusters have exactly one leader at any given time. Only the leader can commit writes (and serve consistent reads). If a client sends a request to a follower, the follower rejects it with a `LeaderHint` field pointing to the current leader's address.

The CLI handles this automatically:

```
1. Try peer[0] (first address in the list)
2. If response.LeaderHint != "":
     a. Move the hint address to the front of the retry list
     b. Retry immediately on that address
3. If the RPC failed (node unreachable, network error):
     a. Wait 300ms
     b. Try the next address in the round-robin order
4. Repeat up to 10 attempts total
```

The `redirectFirst` function moves the hint address to the front without discarding the rest of the list, so if the hint node is also unreachable, the next attempt falls through to the other known addresses.

### Example trace

```
Attempt 1: Connect to localhost:7001 (node1 - follower)
           → Response: LeaderHint = "localhost:7002"

Attempt 2: Connect to localhost:7002 (node2 - leader)
           → Response: Success=true, "OK" printed
```

If node2 then fails mid-test:

```
Attempt 3: Connect to localhost:7002 (dead) → connection error, wait 300ms

Attempt 4: Connect to localhost:7003 (node3 - new leader after election)
           → Response: Success=true
```

---

## Connection lifecycle

Each command creates a fresh gRPC connection, uses it for one RPC, and immediately closes it. This simplifies the client significantly - no connection pooling, no stale connection detection, no health-check goroutines.

The trade-off is latency: TLS handshake + TCP connection setup adds ~1-5ms per command. For an interactive CLI that humans use, this is fine. For a library used in application code, you would maintain a persistent connection.

```go
func dial(addr string) (pb.KVServiceClient, *grpc.ClientConn, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    conn, err := grpc.DialContext(ctx, addr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock(),
    )
    return pb.NewKVServiceClient(conn), conn, err
}
```

The 2-second dial timeout means the CLI will wait up to 2 seconds for a TCP connection before giving up and trying the next peer. The per-RPC timeout is 5 seconds.

---

## Chaos test integration

The chaos harness (`chaos/chaos.py`) uses the CLI as the interface to the cluster. It calls the CLI as a subprocess and checks the exit code and stdout:

```python
def cli_put(cli_binary: str, key: str, value: str) -> bool:
    result = subprocess.run(
        [cli_binary, "--peers", PEERS_FLAG, "put", key, value],
        capture_output=True, text=True, timeout=3.0,
    )
    return result.returncode == 0 and "OK" in result.stdout

def cli_get(cli_binary: str, key: str) -> Optional[str]:
    result = subprocess.run(
        [cli_binary, "--peers", PEERS_FLAG, "get", key],
        ...
    )
    return result.stdout.strip() if result.returncode == 0 else None
```

The CLI's retry logic (up to 10 attempts, 300ms between) is important here: during a chaos kill/restart cycle, some individual attempts will hit dead nodes or nodes mid-election. The retries ensure confirmed writes get through even under churn.

---

## Adding client IDs and sequence numbers

For deduplication to work, the CLI would need to attach a `ClientID` and `SeqNum` to each write. The current implementation sends empty values for these fields, which means deduplication is effectively disabled at the CLI level - the state machine can't distinguish two CLI invocations.

This is intentional for a CLI: each CLI invocation is typically a distinct intended operation. The deduplication feature is more relevant for library clients (e.g., a Go service using raftkv) that retry internally and need at-most-once semantics across retries.

To add it to the CLI, generate a UUID as the `ClientID` at process start and increment a local counter for each write:

```go
clientID := uuid.New().String()
var seqNum uint64

// On each put/delete:
seqNum++
cmd := Command{..., ClientID: clientID, SeqNum: seqNum}
```

---

## Implementation reference

| Function | Description |
|----------|-------------|
| `main` | Parse flags, dispatch to doGet/doPut/doDelete |
| `doGet` | Get with retry + leader redirect loop |
| `doPut` | Put with retry + leader redirect loop |
| `doDelete` | Delete with retry + leader redirect loop |
| `dial` | Open one gRPC connection with timeout |
| `parsePeers` | Parse `"node1=addr1,node2=addr2"` → map |
| `addrList` | Extract addresses from peer map (for round-robin) |
| `redirectFirst` | Move hint address to front of retry list |

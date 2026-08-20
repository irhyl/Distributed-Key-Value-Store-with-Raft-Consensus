// client/main.go - CLI for the raftkv cluster.
//
// Usage:
//
//	raftkv-cli --peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003 get  <key>
//	raftkv-cli --peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003 put  <key> <value>
//	raftkv-cli --peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003 delete <key>
//
// Leader redirection: when a follower rejects a write or read, it returns a
// LeaderHint address. The CLI retries on that address automatically, then
// falls back to trying all peers in turn if the hint is also unreachable.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/raftkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dialTimeout = 2 * time.Second
	rpcTimeout  = 5 * time.Second
	maxRetries  = 10
)

func main() {
	peersFlag := flag.String("peers", "", "comma-separated nodeID=addr pairs (e.g. node1=localhost:7001,...)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 || *peersFlag == "" {
		fmt.Fprintf(os.Stderr, "Usage: raftkv-cli --peers <peers> <get|put|delete> [args...]\n")
		os.Exit(1)
	}

	peers := parsePeers(*peersFlag)
	if len(peers) == 0 {
		fmt.Fprintf(os.Stderr, "error: no valid peers in %q\n", *peersFlag)
		os.Exit(1)
	}

	cmd := args[0]
	switch cmd {
	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: get <key>\n")
			os.Exit(1)
		}
		doGet(peers, args[1])
	case "put":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "usage: put <key> <value>\n")
			os.Exit(1)
		}
		doPut(peers, args[1], []byte(args[2]))
	case "delete":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: delete <key>\n")
			os.Exit(1)
		}
		doDelete(peers, args[1])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (valid: get, put, delete)\n", cmd)
		os.Exit(1)
	}
}

// doGet reads a key, following leader hints and retrying across all peers.
func doGet(peers map[string]string, key string) {
	addrs := addrList(peers)
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		addr := addrs[attempt%len(addrs)]
		client, conn, err := dial(addr)
		if err != nil {
			lastErr = err
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
		cancel()
		conn.Close()

		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if resp.LeaderHint != "" {
			// Not the leader - redirect and retry immediately on the hint
			addrs = redirectFirst(addrs, resp.LeaderHint)
			continue
		}

		if !resp.Found {
			fmt.Println("(not found)")
			return
		}
		fmt.Println(string(resp.Value))
		return
	}

	fmt.Fprintf(os.Stderr, "get failed after %d attempts: %v\n", maxRetries, lastErr)
	os.Exit(1)
}

// doPut writes a key-value pair, following leader hints.
func doPut(peers map[string]string, key string, value []byte) {
	addrs := addrList(peers)
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		addr := addrs[attempt%len(addrs)]
		client, conn, err := dial(addr)
		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		cancel()
		conn.Close()

		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if resp.LeaderHint != "" {
			addrs = redirectFirst(addrs, resp.LeaderHint)
			continue
		}

		if resp.Success {
			fmt.Println("OK")
			return
		}
	}

	fmt.Fprintf(os.Stderr, "put failed after %d attempts: %v\n", maxRetries, lastErr)
	os.Exit(1)
}

// doDelete removes a key, following leader hints.
func doDelete(peers map[string]string, key string) {
	addrs := addrList(peers)
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		addr := addrs[attempt%len(addrs)]
		client, conn, err := dial(addr)
		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
		cancel()
		conn.Close()

		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if resp.LeaderHint != "" {
			addrs = redirectFirst(addrs, resp.LeaderHint)
			continue
		}

		if resp.Success {
			fmt.Println("OK")
			return
		}
	}

	fmt.Fprintf(os.Stderr, "delete failed after %d attempts: %v\n", maxRetries, lastErr)
	os.Exit(1)
}

// dial opens a gRPC connection and returns a KVServiceClient.
func dial(addr string) (pb.KVServiceClient, *grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	// DialContext+WithBlock is deprecated in favor of NewClient, but NewClient
	// connects lazily on first RPC - we want dial itself to fail within
	// dialTimeout so callers get an immediate, actionable error.
	conn, err := grpc.DialContext(ctx, addr, //nolint:staticcheck
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), //nolint:staticcheck
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return pb.NewKVServiceClient(conn), conn, nil
}

// parsePeers converts "node1=addr1,node2=addr2" into a map.
func parsePeers(s string) map[string]string {
	m := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return m
}

// addrList returns peer addresses in a stable order.
func addrList(peers map[string]string) []string {
	addrs := make([]string, 0, len(peers))
	for _, addr := range peers {
		addrs = append(addrs, addr)
	}
	return addrs
}

// redirectFirst moves the hint address to the front of the retry list.
// If the hint isn't in the list (it's a raw address, not a node ID),
// prepend it directly so the next attempt uses it.
func redirectFirst(addrs []string, hint string) []string {
	for i, a := range addrs {
		if a == hint {
			out := make([]string, 0, len(addrs))
			out = append(out, addrs[i])
			out = append(out, addrs[:i]...)
			out = append(out, addrs[i+1:]...)
			return out
		}
	}
	// hint is a new address not in our list - prepend it
	return append([]string{hint}, addrs...)
}

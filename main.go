// main.go - entry point for the raftkv server binary.
//
// Usage:
//   raftkv-server \
//     --id=node1 \
//     --listen=localhost:7001 \
//     --peers=node2=localhost:7002,node3=localhost:7003 \
//     --data-dir=/var/lib/raftkv/node1

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/raftkv/server"
)

func main() {
	id            := flag.String("id",             "",  "unique node ID (e.g. node1)")
	listen        := flag.String("listen",         "",  "gRPC listen address (e.g. localhost:7001)")
	peersStr      := flag.String("peers",          "",  "comma-separated peerID=addr pairs for OTHER nodes")
	dataDir       := flag.String("data-dir",       "",  "directory for WAL and SSTable storage")
	metricsListen := flag.String("metrics-listen", "",  "if set, serve Prometheus metrics at http://<addr>/metrics (e.g. localhost:9091)")
	flag.Parse()

	if *id == "" || *listen == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	peers := parsePeers(*peersStr)
	log.Printf("starting node %s on %s (peers: %v)", *id, *listen, peers)

	cfg := server.Config{
		NodeID:      *id,
		ListenAddr:  *listen,
		Peers:       peers,
		DataDir:     *dataDir,
		MetricsAddr: *metricsListen,
	}

	srv, err := server.NewKVServer(cfg)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	if err := srv.Start(*listen); err != nil {
		log.Fatalf("start server: %v", err)
	}

	log.Printf("node %s ready", *id)

	// Block until SIGTERM or SIGINT, then shut down gracefully.
	// This is what lets the chaos script use SIGTERM for clean stops
	// and SIGKILL for abrupt crash simulation.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Printf("node %s shutting down...", *id)
	srv.Stop()
	log.Printf("node %s stopped", *id)
}

func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	if s == "" {
		return peers
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			peers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return peers
}

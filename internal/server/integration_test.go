package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

func TestFullLoopReplicaCommands(t *testing.T) {
	t.Parallel()
	// Create a listener on any port
	replicaListener, addr := createListener(t)
	kv := store.New()

	// Run accept loop on listener
	go acceptLoop(
		replicaListener,
		func(conn net.Conn) { handleReplicaConnection(conn, kv) },
	)

	// Dial the replica listener
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.Close()

	// Assert responses
	reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "REPLICA_SET foo 500 node-a bar\n")
	response, _ := reader.ReadString('\n')
	if response != "OK\n" {
		t.Errorf("REPLICA_SET: got %q, want %q", response, "OK\n")
	}

	fmt.Fprintf(conn, "REPLICA_GET foo\n")
	response, _ = reader.ReadString('\n')
	if response != "VALUE 500 node-a bar\n" {
		t.Errorf("REPLICA_GET: got %q, want %q", response, "VALUE 500 node-a bar\n")
	}
}

func TestQuorumReplication(t *testing.T) {
	t.Parallel()

	clientListenerA, clientAddrA := createListener(t)

	replicaListenerA, replicaAddrA := createListener(t)
	replicaListenerB, replicaAddrB := createListener(t)
	replicaListenerC, replicaAddrC := createListener(t)

	cfg := cluster.Config{
		ClientListenAddress: clientAddrA,
		ReplicaListenAddress: replicaAddrA,
		Nodes: []cluster.NodeConfig{
			{ID: "node-a", ReplicaAddress: replicaAddrA},
			{ID: "node-b", ReplicaAddress: replicaAddrB},
			{ID: "node-c", ReplicaAddress: replicaAddrC},
		},
	}

	nodeA, err := cluster.NewNodeFromConfig("node-a", cfg, hashring.New(256))
	if err != nil {
		t.Fatalf("create node-a: %v", err)
	}

	storeA := store.New()
	storeB := store.New()
	storeC := store.New()

	poolA := NewPool(8)

	go acceptLoop(
		clientListenerA,
		func (conn net.Conn) { handleClientConnection(conn, nodeA, storeA, poolA) },
	)

	go acceptLoop(
		replicaListenerA,
		func (conn net.Conn) { handleReplicaConnection(conn, storeA) },
	)

	go acceptLoop(
		replicaListenerB,
		func (conn net.Conn) { handleReplicaConnection(conn, storeB) },
	)
	go acceptLoop(
		replicaListenerC,
		func (conn net.Conn) { handleReplicaConnection(conn, storeC) },
	)

	conn, err := net.Dial("tcp", clientAddrA)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.Close()

	reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "SET foo bar\n")
	response, _ := reader.ReadString('\n')
	if response != "OK\n" {
		t.Errorf("SET: got %q, want %q", response, "OK\n")
	}

	stores := map[string]*store.Store{
		"node-a": storeA,
		"node-b": storeB,
		"node-c": storeC,
	}

	successCount := 0
	for _, kv := range stores {
		got, ok := kv.Get("foo")
		if ok && !got.Tombstone && got.Value == "bar" {
			successCount++
		}
	}
	if successCount < writeQuorum {
		t.Errorf("inspected all 3 nodes, writeQuorum not met after %q", "SET foo bar")
	}

	fmt.Fprintf(conn, "GET foo\n")
	response, _ = reader.ReadString('\n')
	if response != "bar\n" {
		t.Errorf("GET: got %q, want %q", response, "bar\n")
	}

	fmt.Fprintf(conn, "DELETE foo\n")
	response, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read DELETE response: %v", err)
	}
	if response != "OK\n" {
		t.Fatalf("DELETE: got %q, want %q", response, "OK\n")
	}

	successCount = 0
	for _, kv := range stores {
		value, ok := kv.Get("foo")
		if ok && value.Tombstone {
			successCount++
		}
	}
	if successCount < writeQuorum {
		t.Errorf("inspected all 3 nodes, writeQuorum not met after %q", "DELETE foo")
	}
}

func createListener(t *testing.T) (net.Listener, string) {
	t.Helper()

	network := "tcp"
	port := ":0"
	listener, err := net.Listen(network, port)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	t.Cleanup(func() {
		listener.Close()
	})

	addr := listener.Addr().String()
	return listener, addr
}

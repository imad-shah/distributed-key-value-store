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
	listener, addr := createListener(t)
	// Run accept loop on listener
	node, err := cluster.New("test-node", addr, "", hashring.New(256))
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	go acceptLoop(listener, node, store.New(), NewPool(8))

	// Dial in as client
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
	listenerA, addrA := createListener(t)
	listenerB, addrB := createListener(t)
	listenerC, addrC := createListener(t)

	nodeA, err := cluster.New("node-a", addrA, fmt.Sprintf("node-b=%s,node-c=%s", addrB, addrC), hashring.New(256))
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}

	nodeB, err := cluster.New("node-b", addrB, fmt.Sprintf("node-a=%s,node-c=%s", addrA, addrC), hashring.New(256))
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}

	nodeC, err := cluster.New("node-c", addrC, fmt.Sprintf("node-a=%s,node-b=%s", addrA, addrB), hashring.New(256))
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}

	storeA := store.New()
	storeB := store.New()
	storeC := store.New()

	go acceptLoop(listenerA, nodeA, storeA, NewPool(8))
	go acceptLoop(listenerB, nodeB, storeB, NewPool(8))
	go acceptLoop(listenerC, nodeC, storeC, NewPool(8))

	conn, err := net.Dial("tcp", addrA)
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
	for nodeID, kv := range stores {
		got, ok := kv.Get("foo")
		if !ok || got.Tombstone || got.Value != "bar" {
			t.Errorf(
				"%s Get(%q) = %+v, %v; want live value %q",
				nodeID,
				"foo",
				got,
				ok,
				"bar",
			)
		}
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
	for nodeID, kv := range stores {
		value, ok := kv.Get("foo")

		if !ok {
			t.Errorf(
				"%s missing tombstone for %q after DELETE",
				nodeID,
				"foo",
			)
			continue
		}

		if !value.Tombstone {
			t.Errorf(
				"%s Get(%q) = %+v; want tombstone",
				nodeID,
				"foo",
				value,
			)
		}
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

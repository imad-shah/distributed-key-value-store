package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

type testCluster struct {
	stores       map[string]*store.Store
	clientAddrs  map[string]string
	replicaAddrs map[string]string
}

func TestSetSucceedsWithTwoOfThreeAcks(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a", "node-b"})

	response := sendCommand(t, tc.clientAddrs["node-a"], "SET foo bar")
	if response != "OK\n" {
		t.Errorf("SET: got %q, want %q", response, "OK\n")
	}

	val, ok := tc.stores["node-a"].Get("foo")
	if !ok {
		t.Fatalf("Get(%q) on serverA does not contain %q", "foo", "bar")
	}
	if val.Value != "bar" {
		t.Fatalf("val %v, does not have %q stored", val, "bar")
	}

	val, ok = tc.stores["node-b"].Get("foo")
	if !ok {
		t.Fatalf("Get(%q) on serverB does not contain %q", "foo", "bar")
	}
	if val.Value != "bar" {
		t.Fatalf("val %v, does not have %q stored", val, "bar")
	}
}

func TestSetFailsWithOneOfThreeAcks(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a"})

	response := sendCommand(t, tc.clientAddrs["node-a"], "SET foo bar")
	want := "error write quorum not reached: got 1 acks, wanted 2\n"
	if response != want {
		t.Errorf("SET response = %q, want %q", response, want)
	}

	val, ok := tc.stores["node-a"].Get("foo")
	if !ok {
		t.Fatalf("Get(%q) on serverA does not contain %q", "foo", "bar")
	}
	if val.Value != "bar" {
		t.Fatalf("val %v, does not have %q stored", val, "bar")
	}
	if val.Tombstone {
		t.Fatalf("val %v, has tombstone set to true", val)
	}
}

func TestGetSucceedsWithTwoOfThreeAvailable(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a", "node-b"})
	val := store.VersionedValue{
		Value: "bar",
		Version: store.Version{
			Timestamp: 500,
			NodeID:    "node-a",
		},
		Tombstone: false,
	}

	ok := tc.stores["node-a"].Put("foo", val)
	if !ok {
		t.Fatalf("PUT(%q, %v) on node %q unsuccessful", "foo", val, "node-a")
	}

	ok = tc.stores["node-b"].Put("foo", val)
	if !ok {
		t.Fatalf("PUT(%q, %v) on node %q unsuccessful", "foo", val, "node-b")
	}

	response := sendCommand(t, tc.clientAddrs["node-a"], "GET foo")
	if response != "bar\n" {
		t.Fatalf("Get(%q) got %q, want %q", "foo", response, "bar")
	}

}

func TestGetFailsWithOneOfThreeAvailable(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a"})
	val := store.VersionedValue{
		Value: "bar",
		Version: store.Version{
			Timestamp: 500,
			NodeID:    "node-a",
		},
	}
	tc.stores["node-a"].Put("foo", val)

	response := sendCommand(t, tc.clientAddrs["node-a"], "GET foo")
	want := "error read quorum not reached: got 1 responses, want 2\n"
	if response != want {
		t.Fatalf("Get(%q) got %v, want %v", "foo", response, want)
	}
}

func TestDeleteSucceedsWithTwoOfThreeAcks(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a", "node-b"})

	response := sendCommand(t, tc.clientAddrs["node-a"], "DELETE foo")
	if response != "OK\n" {
		t.Fatalf("DELETE foo = %q, want %q", response, "OK\n")
	}

	for _, nodeID := range []string{"node-a", "node-b"} {
		got, found := tc.stores[nodeID].Get("foo")
		if !found {
			t.Fatalf("%s did not store a tombstone for %q", nodeID, "foo")
		}
		if !got.Tombstone {
			t.Fatalf("%s value = %+v, want tombstone", nodeID, got)
		}
	}
}

func TestDeleteFailsWithOneOfThreeAcks(t *testing.T) {
	tc := startTestCluster(t, []string{"node-a"})

	response := sendCommand(t, tc.clientAddrs["node-a"], "DELETE foo")

	want := "error delete quorum not reached: got 1 acks, want 2\n"
	if response != want {
		t.Fatalf("Delete(%q) got %q, want %q", "foo", response, want)
	}
}

func TestForwardTimesOutWaitingForResponse(t *testing.T) {
	listener, addr := createListener(t)
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		bufio.NewReader(conn).ReadString('\n')
		defer conn.Close()
		<-release
	}()
	_, err := forward(NewPool(8), addr, "REPLICA_GET foo")
	if err == nil {
		t.Fatalf("got no error, error timeout was expected")
	}

	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected ErrDeadlineExceeded, got: %v", err)
	}
}

func startTestCluster(t *testing.T, liveNodeIDs []string) *testCluster {
	t.Helper()

	tc := &testCluster{
		stores:       make(map[string]*store.Store),
		clientAddrs:  make(map[string]string),
		replicaAddrs: make(map[string]string),
	}

	clientListenerA, clientAddrA := createListener(t)

	replicaListenerA, replicaAddrA := createListener(t)
	replicaListenerB, replicaAddrB := createListener(t)
	replicaListenerC, replicaAddrC := createListener(t)

	tc.clientAddrs["node-a"] = clientAddrA
	tc.replicaAddrs["node-a"] = replicaAddrA
	tc.replicaAddrs["node-b"] = replicaAddrB
	tc.replicaAddrs["node-c"] = replicaAddrC

	clientListeners := map[string]net.Listener{
		"node-a": clientListenerA,
	}

	replicaListeners := map[string]net.Listener{
		"node-a": replicaListenerA,
		"node-b": replicaListenerB,
		"node-c": replicaListenerC,
	}

	for _, node := range liveNodeIDs {
		if _, ok := replicaListeners[node]; !ok {
			t.Fatalf("node: %v is not in test cluster", node)
		}
	}

	cfg := cluster.Config{
		ClientListenAddress:  clientAddrA,
		ReplicaListenAddress: replicaAddrA,
		Nodes: []cluster.NodeConfig{
			{ID: "node-a", ReplicaAddress: replicaAddrA},
			{ID: "node-b", ReplicaAddress: replicaAddrB},
			{ID: "node-c", ReplicaAddress: replicaAddrC},
		},
	}

	live := make(map[string]struct{})
	for _, node := range liveNodeIDs {
		live[node] = struct{}{}
	}

	for nodeID, replicaListener := range replicaListeners {
		if _, ok := live[nodeID]; !ok {
			replicaListener.Close()
			continue
		}

		nodeX, err := cluster.NewNodeFromConfig(nodeID, cfg, hashring.New(256))
		if err != nil {
			t.Fatalf("creating node %q: %v", nodeID, err)
		}
		storeX := store.New()
		tc.stores[nodeID] = storeX

		go acceptLoop(
			replicaListener,
			func(conn net.Conn) { handleReplicaConnection(conn, storeX) },
		)

		if clientListener, ok := clientListeners[nodeID]; ok {
			poolX := NewPool(8)

			go acceptLoop(
				clientListener,
				func(conn net.Conn) {
					handleClientConnection(conn, nodeX, storeX, poolX)
				},
			)

		}
	}
	return tc
}

func sendCommand(t *testing.T, address, command string) string {
	t.Helper()

	network := "tcp"
	conn, err := net.Dial(network, address)
	if err != nil {
		t.Fatalf("conn: %v failed to dial: %v", conn, err)
	}

	err = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		t.Fatalf("conn: %v ran into error setting read deadline: %v", conn, err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	_, err = fmt.Fprintln(conn, command)
	if err != nil {
		t.Fatalf("conn: %v ran into error writing to connection: %v", conn, err)
	}

	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("error reading response: %v", err)
	}

	return response
}

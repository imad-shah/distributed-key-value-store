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

func TestGetRepairsReplica(t *testing.T) {
	tests := []struct {
		name         string
		winner       store.VersionedValue
		nodeCInitial *store.VersionedValue
		wantResponse string
	}{
		{
			name: "missing replica with live winner",
			winner: store.VersionedValue{
				Value: "green",
				Version: store.Version{
					Timestamp: 200,
					NodeID:    "node-a",
				},
			},
			nodeCInitial: nil,
			wantResponse: "green\n",
		},
		{
			name: "stale live replica with newer live winner",
			winner: store.VersionedValue{
				Value: "green",
				Version: store.Version{
					Timestamp: 200,
					NodeID:    "node-a",
				},
			},
			nodeCInitial: &store.VersionedValue{
				Value: "blue",
				Version: store.Version{
					Timestamp: 199,
					NodeID:    "node-a",
				},
			},
			wantResponse: "green\n",
		},
		{
			name: "stale live replica with newer tombstone",
			winner: store.VersionedValue{
				Version: store.Version{
					Timestamp: 300,
					NodeID:    "node-a",
				},
				Tombstone: true,
			},
			nodeCInitial: &store.VersionedValue{
				Value: "green",
				Version: store.Version{
					Timestamp: 200,
					NodeID:    "node-a",
				},
			},
			wantResponse: "NOT_FOUND\n",
		},
		{
			name: "stale tombstone with newer live winner",
			winner: store.VersionedValue{
				Value: "green",
				Version: store.Version{
					Timestamp: 201,
					NodeID:    "node-a",
				},
			},
			nodeCInitial: &store.VersionedValue{
				Version: store.Version{
					Timestamp: 200,
					NodeID:    "node-a",
				},
				Tombstone: true,
			},
			wantResponse: "green\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tc := startTestCluster(
				t,
				[]string{"node-a", "node-b", "node-c"},
			)

			if ok := tc.stores["node-a"].Put("color", test.winner); !ok {
				t.Fatal("failed to seed node-a")
			}
			if ok := tc.stores["node-b"].Put("color", test.winner); !ok {
				t.Fatal("failed to seed node-b")
			}

			if test.nodeCInitial != nil {
				if ok := tc.stores["node-c"].Put(
					"color",
					*test.nodeCInitial,
				); !ok {
					t.Fatal("failed to seed node-c")
				}
			}

			response := sendCommand(
				t,
				tc.clientAddrs["node-a"],
				"GET color",
			)

			if response != test.wantResponse {
				t.Fatalf(
					"GET color = %q, want %q",
					response,
					test.wantResponse,
				)
			}

			deadline := time.Now().Add(2 * time.Second)
			for {
				got, found := tc.stores["node-c"].Get("color")
				if found && got == test.winner {
					break
				}

				if time.Now().After(deadline) {
					if !found {
						t.Fatal("node-c did not contain the repaired record before timeout")
					}
					t.Fatalf(
						"node-c value = %+v, want %+v before timeout",
						got,
						test.winner,
					)
				}
				time.Sleep(10 * time.Millisecond)
			}

		})
	}
}

func TestLateNewerReplicaResponseRepairsInitialQuorum(t *testing.T) {
	t.Parallel()

	clientListenerA, clientAddrA := createListener(t)

	replicaListenerA, replicaAddrA := createListener(t)
	replicaListenerB, replicaAddrB := createListener(t)
	replicaListenerC, replicaAddrC := createListener(t)

	cfg := cluster.Config{
		ClientListenAddress:  clientAddrA,
		ReplicaListenAddress: replicaAddrA,
		Nodes: []cluster.NodeConfig{
			{ID: "node-a", ReplicaAddress: replicaAddrA},
			{ID: "node-b", ReplicaAddress: replicaAddrB},
			{ID: "node-c", ReplicaAddress: replicaAddrC},
		},
	}

	nodeA, err := cluster.NewNodeFromConfig(
		"node-a",
		cfg,
		hashring.New(256),
	)
	if err != nil {
		t.Fatalf("create node-a: %v", err)
	}

	storeA := store.New()
	storeB := store.New()
	poolA := NewPool(8)

	oldValue := store.VersionedValue{
		Value: "old",
		Version: store.Version{
			Timestamp: 199,
			NodeID:    "node-a",
		},
	}

	newValue := store.VersionedValue{
		Value: "new",
		Version: store.Version{
			Timestamp: 200,
			NodeID:    "node-c",
		},
	}

	if ok := storeA.Put("foo", oldValue); !ok {
		t.Fatal("failed to seed node-a")
	}

	if ok := storeB.Put("foo", oldValue); !ok {
		t.Fatal("failed to seed node-b")
	}

	go acceptLoop(
		clientListenerA,
		func(conn net.Conn) {
			handleClientConnection(conn, nodeA, storeA, poolA)
		},
	)

	go acceptLoop(
		replicaListenerA,
		func(conn net.Conn) {
			handleReplicaConnection(conn, storeA)
		},
	)

	go acceptLoop(
		replicaListenerB,
		func(conn net.Conn) {
			handleReplicaConnection(conn, storeB)
		},
	)

	releaseNodeC := make(chan struct{})
	go acceptLoop(
		replicaListenerC,
		func(conn net.Conn) {
			defer conn.Close()

			scanner := bufio.NewScanner(conn)
			for scanner.Scan() {
				<-releaseNodeC
				fmt.Fprintln(conn, "VALUE 200 node-c new")
			}
		},
	)

	conn, err := net.Dial("tcp", clientAddrA)
	if err != nil {
		t.Fatalf("failed to dial client listener: %v", err)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(
		time.Now().Add(3 * time.Second),
	); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	reader := bufio.NewReader(conn)

	fmt.Fprintln(conn, "GET foo")

	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read GET response: %v", err)
	}

	if response != "old\n" {
		t.Fatalf("GET foo = %q, want %q", response, "old\n")
	}

	gotA, foundA := storeA.Get("foo")
	if !foundA {
		t.Fatal("node-a missing foo before late response")
	}
	if gotA != oldValue {
		t.Fatalf(
			"node-a value before late response = %+v, want %+v",
			gotA,
			oldValue,
		)
	}

	gotB, foundB := storeB.Get("foo")
	if !foundB {
		t.Fatal("node-b missing foo before late response")
	}
	if gotB != oldValue {
		t.Fatalf(
			"node-b value before late response = %+v, want %+v",
			gotB,
			oldValue,
		)
	}

	close(releaseNodeC)

	deadline := time.Now().Add(2 * time.Second)

	for {
		gotA, foundA = storeA.Get("foo")
		gotB, foundB = storeB.Get("foo")

		if foundA &&
			foundB &&
			gotA == newValue &&
			gotB == newValue {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"background repair did not complete: node-a=%+v found=%v, node-b=%+v found=%v",
				gotA,
				foundA,
				gotB,
				foundB,
			)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

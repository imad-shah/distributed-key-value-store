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
			if err := scanner.Err(); err != nil {
				t.Errorf("scan replica connection: %v", err)
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

func TestRepairFailureDoesNotFailSuccessfulRead(t *testing.T) {
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

	winner := store.VersionedValue{
		Value: "bar",
		Version: store.Version{
			Timestamp: 200,
			NodeID:    "node-a",
		},
	}

	if ok := storeA.Put("foo", winner); !ok {
		t.Fatal("failed to seed node-a")
	}

	if ok := storeB.Put("foo", winner); !ok {
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

	repairAttempted := make(chan string, 1)

	go acceptLoop(
		replicaListenerC,
		func(conn net.Conn) {
			defer conn.Close()

			scanner := bufio.NewScanner(conn)
			if !scanner.Scan() {
				return
			}
			readCommand := scanner.Text()
			// somehow the first request was not REPLICA_GET foo
			if readCommand != "REPLICA_GET foo" {
				t.Errorf(
					"node-c first command = %q, want %q",
					readCommand,
					"REPLICA_GET foo",
				)
				return
			}
			// at first, it node-c responds with NOT_FOUND, this is a valid response
			fmt.Fprintln(conn, "NOT_FOUND")

			// now we start the second request
			if !scanner.Scan() {
				return
			}

			repairCommand := scanner.Text()
			repairAttempted <- repairCommand
			// this time, node-c is taken offline before it can respond,
			// guarenteeing a timeout
			replicaListenerC.Close()
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
		t.Fatalf("set client read deadline: %v", err)
	}

	reader := bufio.NewReader(conn)

	if _, err := fmt.Fprintln(conn, "GET foo"); err != nil {
		t.Fatalf("write GET command: %v", err)
	}

	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read GET response: %v", err)
	}

	// on this first pass, 2/3 nodes respond with `bar`
	if response != "bar\n" {
		t.Fatalf("GET foo = %q, want %q", response, "bar\n")
	}

	select {
	// node-c reports the repair command it received, which should be the winner from {a,b}
	// **proves the asynchronous read repair happened
	case command := <-repairAttempted:
		want := "REPLICA_SET foo 200 node-a bar"
		if command != want {
			t.Fatalf(
				"node-c repair command = %q, want %q",
				command,
				want,
			)
		}
	// test shouldnt hang for longer than 3 seconds
	case <-time.After(3 * time.Second):
		t.Fatal("background repair was not attempted before timeout")
	}

}

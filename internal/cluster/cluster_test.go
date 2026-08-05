package cluster

import (
	"errors"
	"testing"

	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

const (
	vNodes = 256
	id     = "node-a"
	key    = "some-key"
)

func TestOwnerAddr(t *testing.T) {
	t.Parallel()

	ring := hashring.New(vNodes)
	cfg := newTestConfig()

	node, err := NewNodeFromConfig(id, cfg, ring)

	if err != nil {
		t.Fatalf("error creating node from config: %v", err)
	}
	tests := []struct {
		name       string
		key        string
		wantAddr   string
		wantIsSelf bool
	}{
		{"self", "foo", "node-a:9090", true},
		{"peer", "bar", "node-b:9090", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr, isSelf, err := node.OwnerAddr(test.key)
			if err != nil {
				t.Fatalf("OwnerAddr(%q): unexpected error: %v", test.key, err)
			}
			if addr != test.wantAddr || isSelf != test.wantIsSelf {
				t.Errorf("OwnerAddr(%q) = %q, %v; want %q, %v",
					test.key, addr, isSelf, test.wantAddr, test.wantIsSelf)
			}
		})
	}
}

func TestReplicas(t *testing.T) {
	t.Parallel()

	ring := hashring.New(vNodes)
	cfg := newTestConfig()

	node, err := NewNodeFromConfig(id, cfg, ring)

	if err != nil {
		t.Fatalf("error creating node from config: %v", err)
	}

	n := 3

	replicas, err := node.Replicas(key, n)
	if err != nil {
		t.Fatalf("Replicas(%q, %d) returned error: %v", key, n, err)
	}
	if len(replicas) != n {
		t.Fatalf("Replicas(%q, %d) returned %d replicas, want %d", key, n, len(replicas), n)
	}

	for _, replica := range replicas {
		wantAddr, ok := node.addrBook[replica.ID]
		if !ok {
			t.Fatalf("replica %q missing from address book", replica.ID)
		}

		if replica.Addr != wantAddr {
			t.Errorf(
				"replica %q address = %q, want %q",
				replica.ID,
				replica.Addr,
				wantAddr,
			)
		}
	}

	servers, err := ring.GetNServers(key, n)
	if err != nil {
		t.Fatalf("GetNServers(%q, %d) threw err: %v", key, n, err)
	}
	for idx := range servers {
		if servers[idx] != replicas[idx].ID {
			t.Errorf("server: %v does not match replica: %v", servers[idx], replicas[idx].ID)
		}
	}

	selfCount := 0
	for _, replica := range replicas {
		wantIsSelf := replica.ID == node.id
		if replica.IsSelf != wantIsSelf {
			t.Errorf(
				"replica %q IsSelf = %v, want %v",
				replica.ID,
				replica.IsSelf,
				wantIsSelf,
			)
		}

		if replica.IsSelf {
			selfCount++
		}
	}
	if selfCount != 1 {
		t.Errorf("got %d self replicas, want exactly 1", selfCount)
	}
}

func TestReplicasTooMany(t *testing.T) {
	t.Parallel()

	ring := hashring.New(vNodes)
	cfg := newTestConfig()

	node, err := NewNodeFromConfig(id, cfg, ring)

	if err != nil {
		t.Fatalf("error creating node from config: %v", err)
	}

	_, err = node.Replicas(key, 4)
	// only 3 nodes present in cluster
	// trying to create 4 replicas needs to throw ErrTooManyReplicas
	if !errors.Is(err, hashring.ErrTooManyReplicas) {
		t.Errorf(
			"Replicas with too many replicas error = %v, want %v",
			err,
			hashring.ErrTooManyReplicas,
		)
	}
}

func TestNewFromConfig(t *testing.T) {
	cfg := Config{
		ClientListenAddress:  ":8080",
		ReplicaListenAddress: ":9090",
		Nodes: []NodeConfig{
			{
				ID:             "node-a",
				ReplicaAddress: "node-a:9090",
			},
			{
				ID:             "node-b",
				ReplicaAddress: "node-b:9090",
			},
			{
				ID:             "node-c",
				ReplicaAddress: "node-c:9090",
			},
		},
	}

	ring := hashring.New(vNodes)

	// specifically creating node-a
	node, err := NewNodeFromConfig(id, cfg, ring)
	if err != nil {
		t.Fatalf("node construction failed: %v", err)
	}
	if node.ring != ring {
		t.Fatal("node ring does not match provided ring")
	}
	if node.id != id {
		t.Fatalf("node id = %q, want %q", node.id, id)
	}

	wantAddress := map[string]string{
		"node-a": "node-a:9090",
		"node-b": "node-b:9090",
		"node-c": "node-c:9090",
	}

	for nodeID, wantAddr := range wantAddress {
		gotAddr, ok := node.addrBook[nodeID]
		if !ok {
			t.Fatalf("addrBook missing node %q", nodeID)
		}

		if gotAddr != wantAddr {
			t.Errorf(
				"addrBook[%q] = %q, want %q",
				nodeID,
				gotAddr,
				wantAddr,
			)
		}
	}
	for nodeID := range wantAddress {
		if _, ok := node.members[nodeID]; !ok {
			t.Errorf("members missing node %q", nodeID)
		}
	}
	if len(node.addrBook) != len(wantAddress) {
		t.Fatalf(
			"addrBook contains %d entries, want %d",
			len(node.addrBook),
			len(wantAddress),
		)
	}
	if len(node.members) != len(wantAddress) {
		t.Fatalf(
			"members contains %d entries, want %d",
			len(node.members),
			len(wantAddress),
		)
	}

}

func TestNewNodeFromConfigMissingID(t *testing.T) {
	cfg := Config{
		ClientListenAddress:  ":8080",
		ReplicaListenAddress: ":9090",
		Nodes: []NodeConfig{
			{
				ID:             "node-a",
				ReplicaAddress: "node-a:9090",
			},
		},
	}

	// attempt to create node-z, but config only specifies node-a
	_, err := NewNodeFromConfig(
		"node-z",
		cfg,
		hashring.New(vNodes),
	)

	if !errors.Is(err, ErrNodeIDNotFound) {
		t.Fatalf(
			"NewNodeFromConfig() error = %v, want %v",
			err,
			ErrNodeIDNotFound,
		)
	}
}

func newTestConfig() Config {
	return Config{
		ClientListenAddress:  ":8080",
		ReplicaListenAddress: ":9090",
		Nodes: []NodeConfig{
			{ID: "node-a", ReplicaAddress: "node-a:9090"},
			{ID: "node-b", ReplicaAddress: "node-b:9090"},
			{ID: "node-c", ReplicaAddress: "node-c:9090"},
		},
	}
}

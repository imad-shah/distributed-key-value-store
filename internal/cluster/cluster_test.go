package cluster

import (
	"errors"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"testing"
)

const (
	vNodes   = 256
	id       = "node-a"
	addr     = ":8080"
	peersRaw = "node-b=:8081,node-c=:8082"
	key      = "some-key"
)

func TestOwnerAddr(t *testing.T) {
	t.Parallel()

	ring := hashring.New(vNodes)

	node, err := New(id, addr, peersRaw, ring)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tests := []struct {
		name       string
		key        string
		wantAddr   string
		wantIsSelf bool
	}{
		{"self", "foo", ":8080", true},
		{"peer", "bar", ":8081", false},
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
	node, err := New(id, addr, peersRaw, ring)
	if err != nil {
		t.Fatalf("New: %v", err)
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
	node, err := New(id, addr, peersRaw, ring)
	if err != nil {
		t.Fatalf("New: %v", err)
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

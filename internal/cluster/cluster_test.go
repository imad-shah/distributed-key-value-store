package cluster

import (
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"testing"
)

const (
	vNodes = 256
)

func TestOwnerAddr(t *testing.T) {
	node, err := New("node-a", ":8080", "node-b=:8081,node-c=:8082", hashring.New(vNodes))
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

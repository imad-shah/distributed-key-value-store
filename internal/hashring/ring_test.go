package hashring

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const (
	testVNodes     = 256
	testNumKeys    = 100_000
	testNumServers = 8
)

// TestKeyConsistency verifies that GetServer returns a stable server
// for a given key across repeated lookups.
func TestKeyConsistency(t *testing.T) {
	t.Parallel()
	ring, _ := newTestRing(t, testNumServers)

	keys := []string{
		"i",
		"Key-1",
		"user:954:session",
		"",
		"long-key-" + strings.Repeat("x", 500),
		"12345",
		"wizardCity",
		"🔑",
	}

	want := make(map[string]string, len(keys))
	for _, key := range keys {
		server, err := ring.GetServer(key)
		if err != nil {
			t.Fatalf("GetServer(%q) unexpected error: %v", key, err)
		}
		want[key] = server
	}

	for range 5 {
		for _, key := range keys {
			got, err := ring.GetServer(key)
			if err != nil {
				t.Fatalf("GetServer(%q) unexpected error: %v", key, err)
			}
			if got != want[key] {
				t.Errorf("GetServer(%q) = %s, want %s", key, got, want[key])
			}
		}
	}

}

// TestRebalanceMinimalDisruption tests that upon removing a server
// only keys affected by that dead server must reroute
// TODO: add t.Logf percentange
func TestRebalanceMinimalDisruption(t *testing.T) {
	t.Parallel()
	ring, servers := newTestRing(t, testNumServers)

	// generating testNumKeys keys, adding to a slice
	keys := make([]string, 0, testNumKeys)
	for i := range testNumKeys {
		k := fmt.Sprintf("Key-%d", i)
		keys = append(keys, k)
	}

	// taking each key, routing it to its initial server
	keyMap := make(map[string]string, testNumKeys)
	for _, key := range keys {
		server, err := ring.GetServer(key)
		if err != nil {
			t.Fatalf("GetServer(%q) unexpected error: %v", key, err)
		}
		keyMap[key] = server
	}

	for _, deadServer := range servers {
		t.Run("Remove_"+deadServer, func(t *testing.T) {
			err := ring.RemoveServer(deadServer)
			if err != nil {
				t.Fatalf("RemoveServer led to an unexpected error: %v", err)
			}

			// defer because subtests must be sequential
			// because they rely on a shared mutable ring object
			defer ring.AddServer(deadServer)

			for _, k := range keys {
				oldServer := keyMap[k]
				newServer, err := ring.GetServer(k)
				if err != nil {
					t.Fatalf("New server for key cannot be found: %v", err)
				}

				if oldServer == deadServer {
					// key needs to be moved off dead server
					if newServer == deadServer {
						t.Errorf("GetServer(%q) = %s, expected to move away from dead server", k, newServer)
					}
				} else {
					// key was not on dead server, should NOT move
					if oldServer != newServer {
						t.Errorf("GetServer(%q) = %s, want %s", k, newServer, oldServer)
					}
				}
			}
		})
	}
}

// TODO: Cover ErrEmptyRing
// empty ring instantiation > GetServer

// TODO: Cover ErrServerNotFound
// populated ring > RemoveServer on a server that doesn't exist

// ^ use errors.Is, both errors already exist in ring.go


// TODO: TestDistribution()
// no server should hold <0.7x or >1.4x the mean
// either 100k or 1m keys

func newTestRing(t *testing.T, numServers int) (*Ring, []string) {
	t.Helper()
	ring := New(testVNodes)
	servers := make([]string, 0, numServers)
	for range numServers {
		s := uuid.New().String()
		servers = append(servers, s)
		ring.AddServer(s)
	}
	return ring, servers
}

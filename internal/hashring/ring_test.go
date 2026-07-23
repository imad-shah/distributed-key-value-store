package hashring

import (
	"errors"
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
			if err := ring.RemoveServer(deadServer); err != nil {
				t.Fatalf("RemoveServer led to an unexpected error: %v", err)
			}

			// defer because subtests must be sequential
			// because they rely on a shared mutable ring object
			defer ring.AddServer(deadServer)

			rotatedKeys := 0
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
					rotatedKeys++
				} else {
					// key was not on dead server, should NOT move
					if oldServer != newServer {
						t.Errorf("GetServer(%q) = %s, want %s", k, newServer, oldServer)
					}
				}
			}
			t.Logf("%d of %d keys moved (%.1f%%)", rotatedKeys, len(keys), 100*float64(rotatedKeys)/float64(len(keys)))
		})
	}
}

func TestGetServerEmptyRing(t *testing.T) {
	t.Parallel()
	ring := New(1)
	key := "Key-1"

	if _, err := ring.GetServer(key); !errors.Is(err, ErrEmptyRing) {
		t.Fatalf("GetServer(%q) got %v, wants %v", key, err, ErrEmptyRing)
	}
}

func TestRemoveServerNotFound(t *testing.T) {
	t.Parallel()
	ring, _ := newTestRing(t, testNumServers)
	fakeServer := "this-server-cannot-exist-because-i-just-made-it-up"

	if err := ring.RemoveServer(fakeServer); !errors.Is(err, ErrServerNotFound) {
		t.Fatalf("RemoveServer(%q) got %v, wants %v", fakeServer, err, ErrServerNotFound)
	}
}

func TestDistribution(t *testing.T) {
	t.Parallel()
	ring, servers := newTestRing(t, testNumServers)
	counts := keySpread(t, ring, testNumKeys)

	mean := float64(testNumKeys) / float64(testNumServers)
	for _, s := range servers {
		if float64(counts[s]) < 0.7*mean || float64(counts[s]) > 1.4*mean {
			t.Errorf("%s received %d keys; expected between %.0f and %.0f",
				s, counts[s], 0.7*mean, 1.4*mean)
		}
	}
}

func TestNewPanicsOnZeroVNodes(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("hashring: zero vnodes did not lead to a panic")
		}
	}()

	New(0)
}

// Helper function to spread testNumKeys on a given Ring
func keySpread(t *testing.T, ring *Ring, numKeys int) map[string]int {
	t.Helper()

	counts := make(map[string]int)
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)

		server, err := ring.GetServer(key)
		if err != nil {
			t.Fatalf("GetServer(%q) got %v", key, err)
		}
		counts[server]++
	}
	return counts
}

// Helper to construct a new Ring object
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

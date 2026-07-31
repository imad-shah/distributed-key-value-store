package store

import (
	"fmt"
	"sync"
	"testing"
)

const (
	missingKey = "there is no way this key exists in the map because i just made it up230-9dfj31290fn3109nf0138nfd0813bnfd0813bnf-8012bnr0-8`21br0821"
	key        = "foo"
	operations = 100
)

func TestStorePut(t *testing.T) {
	t.Run("stores missing key", func(t *testing.T) {
		kv := New()
		value := versionValue("bar", 100, "node-a", false)

		if ok := kv.Put(key, value); !ok {
			t.Fatalf("Put(%q, %+v) = false, want true", key, value)
		}
		requireStoredValue(t, kv, key, value)
	})

	t.Run("replaces with newer version", func(t *testing.T) {
		kv := New()
		current := versionValue("bar", 100, "node-a", false)
		incoming := versionValue("baz", 200, "node-a", false)

		if ok := kv.Put(key, current); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, current)
		}

		if ok := kv.Put(key, incoming); !ok {
			t.Fatalf("newer Put(%q, %+v) returned false", key, incoming)
		}

		requireStoredValue(t, kv, key, incoming)
	})
	t.Run("rejects old version", func(t *testing.T) {
		kv := New()
		current := versionValue("qux", 100, "node-a", false)
		incoming := versionValue("corge", 99, "node-a", false)

		if ok := kv.Put(key, current); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, current)
		}

		if ok := kv.Put(key, incoming); ok {
			t.Fatalf("older Put(%q, %+v) returned true", key, incoming)
		}

		requireStoredValue(t, kv, key, current)
	})

	t.Run("stores newer tombstone over live value", func(t *testing.T) {
		kv := New()
		live := versionValue("bar", 100, "node-a", false)
		tombstone := versionValue("", 200, "node-a", true)

		if ok := kv.Put(key, live); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, live)
		}

		if ok := kv.Put(key, tombstone); !ok {
			t.Fatalf("newer Put(%q, %+v) returned false", key, tombstone)
		}

		requireStoredValue(t, kv, key, tombstone)
	})

	t.Run("newer live value replaces tombstone", func(t *testing.T) {
		kv := New()
		tombstone := versionValue("", 100, "node-a", true)
		live := versionValue("bar", 101, "node-a", false)

		if ok := kv.Put(key, tombstone); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, tombstone)
		}

		if ok := kv.Put(key, live); !ok {
			t.Fatalf("newer live-value Put(%q, %+v) returned false", key, live)
		}

		requireStoredValue(t, kv, key, live)
	})
	t.Run("rejects different value with identical version", func(t *testing.T) {
		kv := New()
		current := versionValue("bar", 100, "node-a", false)
		incoming := versionValue("baz", 100, "node-a", false)

		if ok := kv.Put(key, current); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, current)
		}

		if ok := kv.Put(key, incoming); ok {
			t.Fatalf("Put(%q, %+v) accepted different data with identical version", key, incoming)
		}

		requireStoredValue(t, kv, key, current)
	})

	t.Run("rejects older live value after newer tombstone", func(t *testing.T) {
		kv := New()
		tombstone := versionValue("", 200, "node-a", true)
		staleLive := versionValue("bar", 100, "node-a", false)

		if ok := kv.Put(key, tombstone); !ok {
			t.Fatalf("initial tombstone Put(%q, %+v) returned false", key, tombstone)
		}

		if ok := kv.Put(key, staleLive); ok {
			t.Fatalf(
				"Put(%q, %+v) replaced newer tombstone with stale live value",
				key,
				staleLive,
			)
		}

		requireStoredValue(t, kv, key, tombstone)
	})
	t.Run("accepts identical retry", func(t *testing.T) {
		kv := New()
		value := versionValue("bar", 100, "node-a", false)

		if ok := kv.Put(key, value); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, value)
		}

		if ok := kv.Put(key, value); !ok {
			t.Fatalf("identical retry Put(%q, %+v) returned false", key, value)
		}

		requireStoredValue(t, kv, key, value)
	})

}
func TestStoreGet(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		kv := New()
		if _, ok := kv.Get(missingKey); ok {
			t.Fatalf("Get(%q) found missing key", missingKey)
		}
	})
	t.Run("stored tombstone", func(t *testing.T) {
		kv := New()
		tombstone := versionValue("", 100, "node-a", true)

		if ok := kv.Put(key, tombstone); !ok {
			t.Fatalf("initial Put(%q, %+v) returned false", key, tombstone)
		}

		val, ok := kv.Get(key)
		if !ok {
			t.Fatalf("Get(%q): tombstone was treated as missing key", key)
		}

		if !val.Tombstone {
			t.Fatalf("Get(%q) = %+v, want tombstone", key, val)
		}

	})
	t.Run("returns complete live value", func(t *testing.T) {
		kv := New()
		want := versionValue("bar", 100, "node-a", false)

		kv.Put(key, want)

		got, ok := kv.Get(key)
		if !ok {
			t.Fatalf("Get(%q): key not found", key)
		}
		if got != want {
			t.Fatalf("Get(%q) = %+v, want %+v", key, got, want)
		}
	})

}
func TestVersionAfter(t *testing.T) {
	tests := []struct {
		name  string
		left  Version
		right Version
		want  bool
	}{
		{
			name:  "newer timestamp",
			left:  Version{Timestamp: 200, NodeID: "node-a"},
			right: Version{Timestamp: 100, NodeID: "node-z"},
			want:  true,
		},
		{
			name:  "older timestamp",
			left:  Version{Timestamp: 100, NodeID: "node-z"},
			right: Version{Timestamp: 200, NodeID: "node-a"},
			want:  false,
		},
		{
			name:  "higher node ID breaks tie",
			left:  Version{Timestamp: 100, NodeID: "node-z"},
			right: Version{Timestamp: 100, NodeID: "node-a"},
			want:  true,
		},
		{
			name:  "lower node ID loses tie",
			left:  Version{Timestamp: 100, NodeID: "node-a"},
			right: Version{Timestamp: 100, NodeID: "node-z"},
			want:  false,
		},
		{
			name:  "same version is not after itself",
			left:  Version{Timestamp: 100, NodeID: "node-a"},
			right: Version{Timestamp: 100, NodeID: "node-a"},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.left.After(test.right)
			if got != test.want {
				t.Errorf(
					"%+v.After(%+v) = %v, want %v",
					test.left,
					test.right,
					got,
					test.want,
				)
			}
		})
	}
}

func TestVersionEqual(t *testing.T) {
	tests := []struct {
		name  string
		left  Version
		right Version
		want  bool
	}{
		{
			name:  "both equal",
			left:  Version{Timestamp: 200, NodeID: "node-a"},
			right: Version{Timestamp: 200, NodeID: "node-a"},
			want:  true,
		},
		{
			name:  "left smaller",
			left:  Version{Timestamp: 199, NodeID: "node-a"},
			right: Version{Timestamp: 200, NodeID: "node-a"},
			want:  false,
		},
		{
			name:  "left bigger",
			left:  Version{Timestamp: 201, NodeID: "node-a"},
			right: Version{Timestamp: 200, NodeID: "node-a"},
			want:  false,
		},
		{
			name:  "equal ts, different nodes",
			left:  Version{Timestamp: 200, NodeID: "node-a"},
			right: Version{Timestamp: 200, NodeID: "node-b"},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.left.Equal(test.right)
			if got != test.want {
				t.Errorf(
					"%+v.Equal(%+v) = %v, want %v",
					test.left,
					test.right,
					got,
					test.want,
				)
			}
		})
	}
}

func versionValue(value string, timestamp int64, nodeID string, tombstone bool) VersionedValue {
	return VersionedValue{
		Value: value,
		Version: Version{
			Timestamp: timestamp,
			NodeID:    nodeID,
		},
		Tombstone: tombstone,
	}
}

func requireStoredValue(t *testing.T, kv *Store, key string, want VersionedValue) {
	t.Helper()

	got, ok := kv.Get(key)
	if !ok {
		t.Fatalf("Get(%q): key not found", key)
	}

	if got != want {
		t.Fatalf("Get(%q) = %+v, want %+v", key, got, want)
	}
}

func TestStoreConcurrentGetAndPut(t *testing.T) {
	kv := New()

	var wg sync.WaitGroup

	for i := range operations {
		wg.Add(2)

		go func(i int) {
			defer wg.Done()

			kv.Put(
				key,
				versionValue(
					fmt.Sprintf("value-%d", i),
					int64(i),
					"node-a",
					false,
				),
			)
		}(i)

		go func() {
			defer wg.Done()
			kv.Get(key)
		}()
	}

	wg.Wait()

	want := versionValue("value-99", 99, "node-a", false)
	requireStoredValue(t, kv, key, want)
}

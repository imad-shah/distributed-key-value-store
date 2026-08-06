package server

import (
	"testing"
	"time"

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

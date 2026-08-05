package server

import (
	"bytes"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
	"strings"
	"testing"
)

func TestServeReplicaCommands(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "replica get missing key",
			input: "REPLICA_GET food29md921d12d129n\n",
			want:  "NOT_FOUND\n",
		},
		{
			name:  "replica set + replica get",
			input: "REPLICA_SET foo 500 node-a bar\nREPLICA_GET foo\n",
			want:  "OK\nVALUE 500 node-a bar\n",
		},
		{
			name:  "replica delete + replica get",
			input: "REPLICA_SET foo 500 node-a bar\nREPLICA_DELETE foo 600 node-a\nREPLICA_GET foo\n",
			want:  "OK\nOK\nTOMBSTONE 600 node-a\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kv := store.New()
			input := strings.NewReader(test.input)
			var output bytes.Buffer

			serve(
				input,
				&output,
				func(cmd Command) string {
					return handleReplicaCommand(cmd, kv)
				},
			)

			if got := output.String(); got != test.want {
				t.Errorf(
					"serve(%q) = %q, want %q",
					test.input,
					got,
					test.want,
				)
			}
		})
	}
}
func TestServeClientRejectsReplicaCommands(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "reject replica get",
			input: "REPLICA_GET foo\n",
		},
		{
			name:  "reject replica set",
			input: "REPLICA_SET foo 500 node-a bar\n",
		},
		{
			name:  "reject replica delete",
			input: "REPLICA_DELETE foo 600 node-a\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kv := store.New()
			pool := NewPool(8)
			input := strings.NewReader(test.input)
			var output bytes.Buffer

			ring := hashring.New(256)
			cfg := cluster.Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes: []cluster.NodeConfig{
					{ID: "test-node", ReplicaAddress: "test-node:8080"},
				},
			}

			node, err := cluster.NewNodeFromConfig("test-node", cfg, ring)
			if err != nil {
				t.Fatalf("error creating node from config: %v", err)
			}

			serve(
				input,
				&output,
				func(cmd Command) string { return handleClientCommand(cmd, node, kv, pool) },
			)

			want := "error command not allowed on client interface\n"
			if got := output.String(); got != want {
				t.Errorf(
					"serve(%q) = %q, want %q",
					test.input,
					got,
					want,
				)
			}
		})
	}
}
func TestReplicaInterfaceRejectsClientCommands(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "reject get",
			input: "GET foo\n",
		},
		{
			name:  "reject set",
			input: "SET foo bar\n",
		},
		{
			name:  "reject delete",
			input: "DELETE foo\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kv := store.New()
			var output bytes.Buffer

			serve(
				strings.NewReader(test.input),
				&output,
				func(cmd Command) string {
					return handleReplicaCommand(cmd, kv)
				},
			)

			want := "error command not allowed on replica interface\n"
			if got := output.String(); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}
func TestClassifyRepair(t *testing.T) {
	tests := []struct {
		name   string
		result replicaReadResult
		winner store.VersionedValue
		want   repairState
	}{
		{
			name: "replica missing",
			result: replicaReadResult{
				Found: false,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairMissing,
		},
		{
			name: "replica exactly equals winner",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "bar",
					Version: store.Version{
						Timestamp: 500,
						NodeID:    "node-a",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairNotNeeded,
		},
		{
			name: "replica older than winner",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "bar",
					Version: store.Version{
						Timestamp: 499,
						NodeID:    "node-a",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairStale,
		},
		{
			name: "same version with different contents",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "baz",
					Version: store.Version{
						Timestamp: 500,
						NodeID:    "node-a",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairConflict,
		},
		{
			name: "replica newer than winner",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "bar",
					Version: store.Version{
						Timestamp: 501,
						NodeID:    "node-a",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairInvalidWinner,
		},
		{
			name: "same timestamp with replica lower NodeID",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "bar",
					Version: store.Version{
						Timestamp: 500,
						NodeID:    "node-a",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-b",
				},
			},
			want: repairStale,
		},
		{
			name: "same timestamp with replica higher NodeID",
			result: replicaReadResult{
				Value: store.VersionedValue{
					Value: "bar",
					Version: store.Version{
						Timestamp: 500,
						NodeID:    "node-b",
					},
				},
				Found: true,
			},
			winner: store.VersionedValue{
				Value: "bar",
				Version: store.Version{
					Timestamp: 500,
					NodeID:    "node-a",
				},
			},
			want: repairInvalidWinner,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyRepair(test.result, test.winner)

			if got != test.want {
				t.Errorf("classifyRepair() = %v, want %v", got, test.want)
			}
		})
	}
}

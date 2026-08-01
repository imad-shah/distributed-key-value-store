package server

import (
	"bytes"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
	"strings"
	"testing"
)

func TestServe(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "getNothing",
			input: "REPLICA_GET foo\n",
			want:  "NOT_FOUND\n",
		},
		{
			name: "get",
			input: "REPLICA_SET foo 500 node-a bar\n" +
				"REPLICA_GET foo\n",
			want: "OK\n" +
				"VALUE 500 node-a bar\n",
		},
		{
			name: "delete",
			input: "REPLICA_SET foo 500 node-a bar\n" +
				"REPLICA_GET foo\n" +
				"REPLICA_DELETE foo 600 node-a\n" +
				"REPLICA_GET foo\n",
			want: "OK\n" +
				"VALUE 500 node-a bar\n" +
				"OK\n" +
				"TOMBSTONE 600 node-a\n",
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
				ListenAddress: ":8080",
				Nodes: []cluster.NodeConfig{
					{ID: "test-node", Address: "test-node:8080"},
				},
			}

			node, err := cluster.NewNodeFromConfig("test-node", cfg, ring)
			if err != nil {
				t.Fatalf("error creating node from config: %v", err)
			}

			serve(input, &output, node, kv, pool)

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

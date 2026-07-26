package server

import (
	"bytes"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"strings"
	"testing"
)

func TestServe(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"getNothing", "GET foo\n", "NOT_FOUND\n"},
		{"get", "SET foo bar\nGET foo\n", "OK\nbar\n"},
		{"delete", "SET foo bar\nGET foo\nDELETE foo\nGET foo\n", "OK\nbar\nOK\nNOT_FOUND\n"},
		{"unknown", "PING\nSET foo bar\nGET foo\n", "error unknown command: \"PING\"\nOK\nbar\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kv := store.New()
			pool := NewPool(8)
			input := strings.NewReader(test.input)
			var output bytes.Buffer
			node, _ := cluster.New("test-node", ":0", "", hashring.New(256))
			serve(input, &output, node, kv, pool)
			if got := output.String(); got != test.want {
				t.Errorf("serve(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

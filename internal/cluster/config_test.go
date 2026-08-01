package cluster

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		ListenAddress: ":8080",
		Nodes: []NodeConfig{
			{ID: "node-a", Address: "node-a:8080"},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned unexpected error: %v", err)
	}
}

func TestConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "missing listen address",
			cfg: Config{
				ListenAddress: "",
				Nodes: []NodeConfig{
					{ID: "node-a", Address: "node-a:8080"},
				},
			},
			want: ErrEmptyListenAddress,
		},
		{
			name: "no nodes",
			cfg: Config{
				ListenAddress: ":8080",
				Nodes:         []NodeConfig{},
			},
			want: ErrEmptyNodes,
		},
		{
			name: "empty node ID",
			cfg: Config{
				ListenAddress: ":8080",
				Nodes: []NodeConfig{
					{ID: "", Address: "node-a:8080"},
				},
			},
			want: ErrEmptyNodeID,
		},
		{
			name: "empty node Address",
			cfg: Config{
				ListenAddress: ":8080",
				Nodes: []NodeConfig{
					{ID: "node-a", Address: ""},
				},
			},
			want: ErrEmptyNodeAddress,
		},
		{
			name: "duplicate node ID",
			cfg: Config{
				ListenAddress: ":8080",
				Nodes: []NodeConfig{
					{ID: "node-a", Address: "node-a:8080"},
					{ID: "node-a", Address: "node-a:8082"},
				},
			},
			want: ErrDuplicateNodeID,
		},
		{
			name: "duplicate node address",
			cfg: Config{
				ListenAddress: ":8080",
				Nodes: []NodeConfig{
					{ID: "node-a", Address: "node-a:8080"},
					{ID: "node-b", Address: "node-a:8080"},
				},
			},
			want: ErrDuplicateNodeAddress,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.cfg.Validate()

			if !errors.Is(err, test.want) {
				t.Fatalf(
					"Validate() error = %v, want %v",
					err,
					test.want,
				)
			}
		})
	}
}

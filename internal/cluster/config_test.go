package cluster

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		ClientListenAddress:  ":8080",
		ReplicaListenAddress: ":9090",
		Nodes: []NodeConfig{
			{ID: "node-a", ReplicaAddress: "node-a:9090"},
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
			name: "missing client listen address",
			cfg: Config{
				ClientListenAddress:  "",
				ReplicaListenAddress: ":9090",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: "node-a:9090"},
				},
			},
			want: ErrEmptyClientListenAddress,
		},
		{
			name: "missing replica listen address",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: "",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: "node-a:9090"},
				},
			},
			want: ErrEmptyReplicaListenAddress,
		},
		{
			name: "client and replica listen addresses are identical",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":8080",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: "node-a:9090"},
				},
			},
			want: ErrClientAndReplicaListenAddressEqual,
		},
		{
			name: "no nodes given",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes:                []NodeConfig{},
			},
			want: ErrEmptyNodes,
		},
		{
			name: "empty node ID",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes: []NodeConfig{
					{ID: "", ReplicaAddress: "node-a:9090"},
				},
			},
			want: ErrEmptyNodeID,
		},
		{
			name: "empty replica node Address",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: ""},
				},
			},
			want: ErrEmptyReplicaAddress,
		},
		{
			name: "duplicate node ID",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: "node-a:9090"},
					{ID: "node-a", ReplicaAddress: "node-a:9091"},
				},
			},
			want: ErrDuplicateNodeID,
		},
		{
			name: "duplicate repica node address",
			cfg: Config{
				ClientListenAddress:  ":8080",
				ReplicaListenAddress: ":9090",
				Nodes: []NodeConfig{
					{ID: "node-a", ReplicaAddress: "node-a:9090"},
					{ID: "node-b", ReplicaAddress: "node-a:9090"},
				},
			},
			want: ErrDuplicateReplicaAddress,
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

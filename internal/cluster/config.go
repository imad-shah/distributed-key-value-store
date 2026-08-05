package cluster

import (
	"errors"
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

var (
	ErrEmptyClientListenAddress  = errors.New("cluster: client_listen_address is required")
	ErrEmptyReplicaListenAddress = errors.New("cluster: replica_listen_address is required")
	ErrEmptyNodes                = errors.New("cluster: at least one node is required")
	ErrEmptyNodeID               = errors.New("cluster: node has no ID")
	ErrEmptyReplicaAddress       = errors.New("cluster: node has no Address")

	ErrReadClusterConfig                  = errors.New("cluster: reading cluster config")
	ErrParseClusterConfig                 = errors.New("cluster: parsing cluster config")
	ErrValidateClusterConfig              = errors.New("cluster: validating cluster config")
	ErrClientAndReplicaListenAddressEqual = errors.New("cluster: client and replica listen must differ")
	ErrNodeIDNotFound                     = errors.New("cluster: node ID not found")

	ErrDuplicateNodeID         = errors.New("cluster: duplicate node ID detected")
	ErrDuplicateReplicaAddress = errors.New("cluster: duplicate node Address detected")
)

type Config struct {
	ClientListenAddress  string       `yaml:"client_listen_address"`
	ReplicaListenAddress string       `yaml:"replica_listen_address"`
	Nodes                []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID             string `yaml:"id"`
	ReplicaAddress string `yaml:"replica_address"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrReadClusterConfig, err)
	}

	var cfg Config
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrParseClusterConfig, err)
	}

	if err = cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrValidateClusterConfig, err)
	}

	return cfg, nil
}

func (cfg Config) Validate() error {
	if cfg.ClientListenAddress == "" {
		return ErrEmptyClientListenAddress
	}
	if cfg.ReplicaListenAddress == "" {
		return ErrEmptyReplicaListenAddress
	}

	if cfg.ClientListenAddress == cfg.ReplicaListenAddress {
		return ErrClientAndReplicaListenAddressEqual
	}

	if len(cfg.Nodes) == 0 {
		return ErrEmptyNodes
	}

	seenIDs := make(map[string]struct{}, len(cfg.Nodes))
	seenReplicaAddresses := make(map[string]struct{}, len(cfg.Nodes))

	for idx, node := range cfg.Nodes {

		if node.ID == "" {
			return fmt.Errorf("%w: node at index %d has no ID", ErrEmptyNodeID, idx)
		}

		if node.ReplicaAddress == "" {
			return fmt.Errorf("%w: node %q at index %d", ErrEmptyReplicaAddress, node.ID, idx)
		}

		if _, ok := seenIDs[node.ID]; ok {
			return fmt.Errorf("%w: %q", ErrDuplicateNodeID, node.ID)
		}
		seenIDs[node.ID] = struct{}{}

		if _, ok := seenReplicaAddresses[node.ReplicaAddress]; ok {
			return fmt.Errorf(
				"%w: %q",
				ErrDuplicateReplicaAddress,
				node.ReplicaAddress,
			)
		}
		seenReplicaAddresses[node.ReplicaAddress] = struct{}{}
	}
	return nil
}

package cluster

import (
	"errors"
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

var ErrReadClusterConfig = errors.New("cluster: reading cluster config")
var ErrParseClusterConfig = errors.New("cluster: parsing cluster config")
var ErrValidateClusterConfig = errors.New("cluster: validating cluster config")
var ErrEmptyListenAddress = errors.New("cluster: listen_address is required")
var ErrEmptyNodes = errors.New("cluster: at least one node is required")
var ErrEmptyNodeID = errors.New("cluster: node has no ID")
var ErrEmptyNodeAddress = errors.New("cluster: node has no Address")
var ErrDuplicateNodeID = errors.New("cluster: duplicate node ID detected")
var ErrDuplicateNodeAddress = errors.New("cluster: duplicate node Address detected")
var ErrNodeIDNotFound = errors.New("cluster: node ID not found")

type Config struct {
	ClientListenAddress  string       `yaml:"client_listen_address"`
	ReplicaListenAddress string       `yaml:"replica_listen_address"`
	Nodes                []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID      string `yaml:"id"`
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
	if cfg.ListenAddress == "" {
		return ErrEmptyListenAddress
	}

	if len(cfg.Nodes) == 0 {
		return ErrEmptyNodes
	}

	seenIDs := make(map[string]struct{}, len(cfg.Nodes))
	seenAddresses := make(map[string]struct{}, len(cfg.Nodes))

	for idx, node := range cfg.Nodes {

		if node.ID == "" {
			return fmt.Errorf("%w: node at index %d has no ID", ErrEmptyNodeID, idx)
		}

		if node.Address == "" {
			return fmt.Errorf("%w: node %q at index %d", ErrEmptyNodeAddress, node.ID, idx)
		}

		if _, ok := seenIDs[node.ID]; ok {
			return fmt.Errorf("%w: %q", ErrDuplicateNodeID, node.ID)
		}
		seenIDs[node.ID] = struct{}{}

		if _, ok := seenAddresses[node.Address]; ok {
			return ErrDuplicateNodeAddress
		}
		seenAddresses[node.Address] = struct{}{}
	}
	return nil
}

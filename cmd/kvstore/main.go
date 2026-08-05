package main

import (
	"flag"
	"log"

	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/server"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

const (
	vNodes = 256
)

func main() {
	// TCP server entry point
	id := flag.String("id", "", "this node's ID")
	configPath := flag.String(
		"config",
		"./config/cluster.yaml",
		"path to cluster configuration",
	)
	flag.Parse()
	if *id == "" {
		log.Fatal("node id is required")
	}

	cfg, err := cluster.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load cluster config: %v", err)
	}

	node, err := cluster.NewNodeFromConfig(*id, cfg, hashring.New(vNodes))
	if err != nil {
		log.Fatalf("error creating cluster: %v", err)
	}
	kv := store.New()
	pool := server.NewPool(8)
	server.StartServers(
		cfg.ClientListenAddress,
		cfg.ReplicaListenAddress,
		node,
		kv,
		pool,
	)
}

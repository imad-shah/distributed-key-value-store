package main

import (
	"flag"
	"log"

	"github.com/imad-shah/distributed-key-value-store/internal/server"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

const (
	vNodes = 256
)

func main() {
	// TCP server entry point
	id := flag.String("id", "", "this node's ID")
	addr := flag.String("addr", ":8080", "address to listen on")
	peersRaw := flag.String("peers", "", "comma-separated pairs of id=addr")
	flag.Parse()
	
	node, err := cluster.New(*id, *addr, *peersRaw, hashring.New(vNodes))
	if err != nil {
		log.Fatalf("error creating cluster: %v", err)
	}
	kv := store.New()
	server.StartServer(*addr, node, kv)
}

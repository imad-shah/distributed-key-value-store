package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

const (
	vnodes = 256
)

func keySpread(numServers int, numKeys int) {
	ring := hashring.New(vnodes)

	for range numServers {
		if err := ring.AddServer(uuid.New().String()); err != nil {
			log.Fatalf("AddServer: %v", err)
		}
	}

	counts := make(map[string]int)

	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		server, err := ring.GetServer(key)
		if err != nil {
			fmt.Println("Error: ", err)
			continue
		}
		counts[server]++
	}
	for server, count := range counts {
		fmt.Printf("%s: %d keys\n", server, count)
	}
}

func main() {
	numServers := 8
	numKeys := 100_000
	keySpread(numServers, numKeys)
}

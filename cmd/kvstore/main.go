package main

import (
	"fmt"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
	"github.com/google/uuid"
)

func main() {
	ring := hashring.New(1)
	numServers := 8
	var servers []string
	for i := 0; i < numServers; i++ {
		servers = append(servers, uuid.New().String())
	}
	
	for _, server := range servers {
		ring.AddServer(server)
	}


	counts := make(map[string]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
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

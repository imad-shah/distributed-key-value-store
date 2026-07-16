package main

import (
	"fmt"
	"hash/fnv"
	"slices"
)


// func getServer() {}   <- implement the ability to take a string, and find what server it mapped to. 
// Use binary search on the sorted hashes slice, wrap end to beginning to emulate hash ring

func hashString(s string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return hash.Sum64()
}

func main() {
	servers := []string{"A", "B", "C"}
	hashes := make([]uint64, 0)
	ringMap := make(map[uint64]string, 0)

	for _, server := range servers {
		serverHash := hashString(server)
		hashes = append(hashes, serverHash)
		ringMap[serverHash] = server
	}
	slices.Sort(hashes)

	for hash, server := range ringMap {
		fmt.Printf("%d -> %s\n", hash, server)
	}

}
package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
)

var ErrEmptyRing = errors.New("hashring: ring is empty")

func addServer(s string, hashes *[]uint64, ringMap map[uint64]string) {
	serverHash := hashString(s)
	*hashes = append(*hashes, serverHash)
	ringMap[serverHash] = s
	slices.Sort(*hashes)
}

func getServer(key string, hashes *[]uint64, ringMap map[uint64]string) (uint64, error) {
	if len(*hashes) == 0 {
		return 0, ErrEmptyRing
	}
	keyHash := hashString(key)

	idx, _ := slices.BinarySearch(*hashes, keyHash)
	if idx >= len(*hashes) {
		fmt.Println("WRAPAROUND")
		return (*hashes)[0], nil
	}

	targetHash := (*hashes)[idx]
	return targetHash, nil

}

func hashString(s string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return hash.Sum64()
}

func main() {
	servers := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	hashes := make([]uint64, 0)
	ringMap := make(map[uint64]string, 0)

	for _, server := range servers {
		addServer(server, &hashes, ringMap)
	}

	testKeys := []string{"(1, 10)", "hello", "foo", "zzzzzz", "user:42", "user:1234567890", "session:abc-def-ghi", "cart:xyz:999"}
	for _, key := range testKeys {
		server, err := getServer(key, &hashes, ringMap)
		if err != nil {
		fmt.Println("Could not find server: ", err)
		return
		}
		fmt.Printf("key %q hash=%d -> server %d\n", key, hashString(key), server)
	}

}

package main

import (
	"fmt"
	"hash/fnv"
)

func hashString(s string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return hash.Sum32()
}

func main() {
	serverOne := "A"
	serverTwo := "B"
	serverThree := "C"
	fmt.Printf("Server: %s | Hash: %d\n", serverOne, hashString(serverOne))
	fmt.Printf("Server: %s | Hash: %d\n", serverTwo, hashString(serverTwo))
	fmt.Printf("Server: %s | Hash: %d\n", serverThree, hashString(serverThree))
}

package main

import (
	"github.com/imad-shah/distributed-key-value-store/internal/server"
)

func main() {
	// TCP server entry point
	server.StartServer()
}

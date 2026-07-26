package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/imad-shah/distributed-key-value-store/internal/store"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

func TestFullLoop(t *testing.T) {
	// Create a listener on any port
	network := "tcp"
	port := ":0"
	listener, err := net.Listen(network, port)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	// Run accept loop on listener
	node, err := cluster.New("test-node", ":0", "", hashring.New(256))
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	go acceptLoop(listener, node, store.New())

	// Dial in as client
	conn, err := net.Dial(network, addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.Close()

	// Assert responses
	reader := bufio.NewReader(conn)
	fmt.Fprintf(conn, "SET foo bar\n")
	response, _ := reader.ReadString('\n')
	if response != "OK\n" {
		t.Errorf("SET: got %q, want %q", response, "OK\n")
	}

	fmt.Fprintf(conn, "GET foo\n")
	response, _ = reader.ReadString('\n')
	if response != "bar\n" {
		t.Errorf("GET: got %q, want %q", response, "bar\n")
	}

}

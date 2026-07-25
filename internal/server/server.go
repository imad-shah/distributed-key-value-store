package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

func handleConnection(conn net.Conn, kv *store.Store) {
	defer conn.Close()
	serve(conn, conn, kv)
}

func serve(r io.Reader, w io.Writer, kv *store.Store) {

	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		input, err := Parse(scanner.Text())
		if err != nil {
			writeLine(w, fmt.Sprintf("error %v", err))
			continue
		}

		var response string
		switch input.Type {
		case CmdGet:
			if val, ok := kv.Get(input.Key); ok {
				response = val
			} else {
				response = "NOT_FOUND"
			}
		case CmdSet:
			kv.Set(input.Key, input.Value)
			response = "OK"
		case CmdDelete:
			if kv.Delete(input.Key) {
				response = "OK"
			} else {
				response = "NOT_FOUND"
			}
		}
		writeLine(w, response)

	}
	if err := scanner.Err(); err != nil {
		log.Printf("error reading lines: %v", err)
	}

}

func writeLine(w io.Writer, s string) {
	if _, err := w.Write([]byte(s + "\n")); err != nil {
		log.Printf("write error: %v", err)
	}
}

func StartServer() {
	network := "tcp"
	port := ":8080"
	listener, err := net.Listen(network, port)
	if err != nil {
		log.Printf("error connecting to server: %v", err)
		return
	}

	defer listener.Close()
	log.Printf("Listening on %s", listener.Addr())
	acceptLoop(listener, store.New())

}
func acceptLoop(listener net.Listener, kv *store.Store) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, kv)
	}
}

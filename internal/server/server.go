package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/imad-shah/distributed-key-value-store/internal/store"
	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
)

func handleConnection(conn net.Conn, node *cluster.Node, kv *store.Store, pool *Pool) {
	defer conn.Close()
	serve(conn, conn, node, kv, pool)
}

func serve(r io.Reader, w io.Writer, node *cluster.Node, kv *store.Store, pool *Pool) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		input, err := Parse(line)
		if err != nil {
			writeLine(w, fmt.Sprintf("error %v", err))
			continue
		}

		var response string
		addr, isSelf, err := node.OwnerAddr(input.Key)
		if err != nil {
			writeLine(w, fmt.Sprintf("error %v", err))
			continue
		}
		if !isSelf{
			log.Printf("forwarding %s for key %q to %s", input.Type, input.Key, addr)
			response, err := forward(pool, addr, line)
			if err != nil {
				writeLine(w, fmt.Sprintf("ERR forward failed: %v", err))
				continue
			}
			writeLine(w, response)
			continue
		}
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

func forward(pool *Pool, addr, line string) (string, error) {
	conn, err := pool.Get(addr)
	if err != nil {
		return "", err
	}

	if _, err := fmt.Fprintf(conn, "%s\n", line); err != nil {
		conn.Close() // broke mid write
		return "", err
	}
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close() // broke mid read
		return "", err
	}
	pool.Put(addr, conn)
	return strings.TrimRight(response, "\n"), nil
}

func writeLine(w io.Writer, s string) {
	if _, err := w.Write([]byte(s + "\n")); err != nil {
		log.Printf("write error: %v", err)
	}
}

func StartServer(addr string, node *cluster.Node, kv *store.Store, pool *Pool) {
	network := "tcp"
	listener, err := net.Listen(network, addr)
	if err != nil {
		log.Printf("error connecting to server: %v", err)
		return
	}

	defer listener.Close()
	log.Printf("Listening on %s", listener.Addr())
	acceptLoop(listener, node, kv, pool)

}
func acceptLoop(listener net.Listener, node *cluster.Node, kv *store.Store, pool *Pool) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, node, kv, pool)
	}
}

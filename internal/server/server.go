package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

const (
	replicationFactor = 3
	readQuorum        = 2
	writeQuorum       = 2
)

func handleConnection(conn net.Conn, node *cluster.Node, kv *store.Store, pool *Pool) {
	defer conn.Close()
	serve(conn, conn, node, kv, pool)
}

func serve(r io.Reader, w io.Writer, node *cluster.Node, kv *store.Store, pool *Pool) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		cmd, err := Parse(line)
		if err != nil {
			writeLine(w, fmt.Sprintf("parse error %v", err))
			continue
		}
		response := handleCommand(cmd, node, kv, pool)
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

func handleCommand(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	if isReplicaCommand(cmd.Type) {
		return executeLocal(cmd, kv)
	}

	switch cmd.Type {
	case CmdSet:
		return coordinateSet(cmd, node, kv, pool)

	case CmdGet:
		return coordinateGet(cmd, node, kv, pool)

	case CmdDelete:
		return coordinateDelete(cmd, node, kv, pool)

	default:
		return "error unsupported command"
	}
}

func coordinateSet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	successCount := 0
	for _, replica := range replicas {
		if err := setOnReplica(replica, cmd, kv, pool); err != nil {
			log.Printf("write for key %q to replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		successCount++
	}

	if successCount >= writeQuorum {
		return "OK"
	}

	return fmt.Sprintf("error write quorum not reached: got %d acks, wanted 2", successCount)
}

func coordinateGet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	replicas, err := node.Replicas(cmd.Key, 3)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}
	responses := make([]string, 0, 2)

	for _, replica := range replicas {
		response, err := getFromReplica(replica, cmd, kv, pool)
		if err != nil {
			log.Printf("read key %q from replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		responses = append(responses, response)
		if len(responses) == 2 {
			break
		}
	}

	if len(responses) < 2 {
		return fmt.Sprintf("error read quorum not reached: got %d responses, wanted 2", len(responses))
	}

	if responses[0] != responses[1] {
		return "error replicas returned conflicting values"
	}

	return responses[0]

}

func coordinateDelete(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	replicas, err := node.Replicas(cmd.Key, 3)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	successCount := 0
	for _, replica := range replicas {
		if err := deleteFromReplica(replica, cmd, kv, pool); err != nil {
			log.Printf("delete for key %q to replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		successCount++
	}

	if successCount >= 2 {
		return "OK"
	}

	return fmt.Sprintf("error delete quorum not reached: got %d acks, wanted 2", successCount)
}

func executeLocal(cmd Command, kv *store.Store) string {
	switch cmd.Type {
	case CmdReplicaGet:
		if val, ok := kv.Get(cmd.Key); ok {
			return val
		}
		return "NOT_FOUND"

	case CmdReplicaSet:
		kv.Set(cmd.Key, cmd.Value)
		return "OK"

	case CmdReplicaDelete:
		if kv.Delete(cmd.Key) {
			return "OK"
		}
		return "NOT_FOUND"
	default:
		return "error not a replica command"
	}
}

func isReplicaCommand(cmdType CommandType) bool {
	switch cmdType {
	case CmdReplicaGet, CmdReplicaSet, CmdReplicaDelete:
		return true
	default:
		return false
	}
}

func setOnReplica(replica cluster.Replica, cmd Command, kv *store.Store, pool *Pool) error {
	if replica.IsSelf {
		kv.Set(cmd.Key, cmd.Value)
		return nil
	}

	line := fmt.Sprintf("REPLICA_SET %s %s", cmd.Key, cmd.Value)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return err
	}

	if response != "OK" {
		return fmt.Errorf("replica returned %q", response)
	}
	return nil
}

func getFromReplica(replica cluster.Replica, cmd Command, kv *store.Store, pool *Pool) (string, error) {
	if replica.IsSelf {
		if value, ok := kv.Get(cmd.Key); ok {
			return value, nil
		}
		return "NOT_FOUND", nil
	}

	line := fmt.Sprintf("REPLICA_GET %s", cmd.Key)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return "", err
	}

	return response, nil

}

func deleteFromReplica(replica cluster.Replica, cmd Command, kv *store.Store, pool *Pool) error {
	if replica.IsSelf {
		kv.Delete(cmd.Key)
		return nil
	}

	line := fmt.Sprintf("REPLICA_DELETE %s", cmd.Key)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return err
	}

	if response != "OK" && response != "NOT_FOUND" {
		return fmt.Errorf("replica returned %q", response)
	}
	return nil
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
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, node, kv, pool)
	}
}

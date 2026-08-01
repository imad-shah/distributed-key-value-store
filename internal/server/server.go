package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/imad-shah/distributed-key-value-store/internal/cluster"
	"github.com/imad-shah/distributed-key-value-store/internal/store"
)

const (
	replicationFactor = 3
	readQuorum        = 2
	writeQuorum       = 2
)
// replicaReadResult is one successful replica read
// Found=false means the replica has no record, ie a tombstone has Found=true.
type replicaReadResult struct {
	Value store.VersionedValue
	Found bool
}

// replicaReadResponse connects a read result with its replica
type replicaReadResponse struct {
	Replica cluster.Replica
	Result  replicaReadResult
	Err     error
}

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
	case CmdGet:
		return coordinateGet(cmd, node, kv, pool)

	case CmdSet:
		return coordinateSet(cmd, node, kv, pool)

	case CmdDelete:
		return coordinateDelete(cmd, node, kv, pool)

	default:
		return "error unsupported command"
	}
}

func coordinateSet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	record := newRecord(cmd, node)

	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	successCount := 0
	for _, replica := range replicas {
		if err := setOnReplica(replica, cmd.Key, record, kv, pool); err != nil {
			log.Printf("write for key %q to replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		successCount++
	}

	if successCount >= writeQuorum {
		return "OK"
	}

	return fmt.Sprintf("error write quorum not reached: got %d acks, wanted %d", successCount, writeQuorum)
}

func coordinateGet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}
	responses := make([]replicaReadResult, 0, readQuorum)

	for _, replica := range replicas {
		value, found, err := getFromReplica(replica, cmd, kv, pool)
		if err != nil {
			log.Printf("read key %q from replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		responses = append(responses, replicaReadResult{
			Value: value,
			Found: found,
		})

		if len(responses) == readQuorum {
			break
		}
	}

	if len(responses) < readQuorum {
		return fmt.Sprintf(
			"error read quorum not reached: got %d responses, want %d",
			len(responses),
			readQuorum,
		)
	}

	// iterate through the replica responses, choose the newest one
	winner, found := chooseNewest(responses)
	if !found {
		return "NOT_FOUND"
	}

	if winner.Tombstone {
		return "NOT_FOUND"
	}
	return winner.Value

}

func chooseNewest(responses []replicaReadResult) (store.VersionedValue, bool) {
	var winner store.VersionedValue
	foundWinner := false

	for _, response := range responses {
		if !response.Found {
			continue
		}
		if !foundWinner || response.Value.Version.After(winner.Version) {
			winner = response.Value
			foundWinner = true
		}
	}
	return winner, foundWinner
}

func coordinateDelete(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	record := newRecord(cmd, node)

	replicas, err := node.Replicas(cmd.Key, 3)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	successCount := 0
	for _, replica := range replicas {
		if err := deleteFromReplica(replica, cmd.Key, record, kv, pool); err != nil {
			log.Printf("delete for key %q to replica %q failed: %v", cmd.Key, replica.ID, err)
			continue
		}
		successCount++
	}

	if successCount >= writeQuorum {
		return "OK"
	}

	return fmt.Sprintf("error delete quorum not reached: got %d acks, wanted %d", successCount, writeQuorum)
}

func executeLocal(cmd Command, kv *store.Store) string {
	switch cmd.Type {
	case CmdReplicaGet:
		val, found := kv.Get(cmd.Key)
		if !found {
			return "NOT_FOUND"
		}
		return formatReplicaGetResponse(val)

	case CmdReplicaSet:
		record, err := recordFromCommand(cmd)
		if err != nil {
			return fmt.Sprintf("error %v", err)
		}

		if ok := kv.Put(cmd.Key, record); !ok {
			return "STALE"
		}
		return "OK"

	case CmdReplicaDelete:
		record, err := recordFromCommand(cmd)
		if err != nil {
			return fmt.Sprintf("error %v", err)
		}

		if ok := kv.Put(cmd.Key, record); !ok {
			return "STALE"
		}
		return "OK"

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

// for external SET & DELETE, Tombstone is based off which command is used
func newRecord(cmd Command, node *cluster.Node) store.VersionedValue {
	return store.VersionedValue{
		Value: cmd.Value,
		Version: store.Version{
			Timestamp: time.Now().UnixNano(),
			NodeID:    node.ID(),
		},
		Tombstone: cmd.Type == CmdDelete,
	}
}

// for replicas, record creation, takes inputs from the cmd (cmd.Timestamp, cmd.NodeID, etc)
func recordFromCommand(cmd Command) (store.VersionedValue, error) {
	switch cmd.Type {
	case CmdReplicaSet, CmdReplicaDelete:
		return store.VersionedValue{
			Value: cmd.Value,
			Version: store.Version{
				Timestamp: cmd.Timestamp,
				NodeID:    cmd.NodeID,
			},
			Tombstone: cmd.Type == CmdReplicaDelete,
		}, nil

	default:
		return store.VersionedValue{},
			fmt.Errorf("cannot reconstruct record from %s", cmd.Type)
	}
}

func setOnReplica(replica cluster.Replica, key string, record store.VersionedValue, kv *store.Store, pool *Pool) error {
	if replica.IsSelf {
		if ok := kv.Put(key, record); !ok {
			return fmt.Errorf("local replica rejected stale or conflicted write")
		}
		return nil
	}

	line := fmt.Sprintf("REPLICA_SET %s %d %s %s", key, record.Version.Timestamp, record.Version.NodeID, record.Value)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return err
	}

	if response != "OK" {
		return fmt.Errorf("replica returned %q", response)
	}
	return nil
}

func deleteFromReplica(replica cluster.Replica, key string, record store.VersionedValue, kv *store.Store, pool *Pool) error {
	if replica.IsSelf {
		if ok := kv.Put(key, record); !ok {
			return fmt.Errorf("local replica rejected stale or conflicted write")
		}
		return nil
	}

	line := fmt.Sprintf("REPLICA_DELETE %s %d %s", key, record.Version.Timestamp, record.Version.NodeID)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return err
	}

	if response != "OK" {
		return fmt.Errorf("replica returned %q", response)
	}
	return nil
}

func getFromReplica(replica cluster.Replica, cmd Command, kv *store.Store, pool *Pool) (store.VersionedValue, bool, error) {
	if replica.IsSelf {
		value, ok := kv.Get(cmd.Key)
		return value, ok, nil
	}

	line := fmt.Sprintf("REPLICA_GET %s", cmd.Key)
	response, err := forward(pool, replica.Addr, line)
	if err != nil {
		return store.VersionedValue{}, false, err
	}

	return parseReplicaGetResponse(response)

}

func formatReplicaGetResponse(value store.VersionedValue) string {
	if value.Tombstone {
		return fmt.Sprintf(
			"TOMBSTONE %d %s",
			value.Version.Timestamp,
			value.Version.NodeID,
		)
	}

	return fmt.Sprintf(
		"VALUE %d %s %s",
		value.Version.Timestamp,
		value.Version.NodeID,
		value.Value,
	)
}

func parseReplicaGetResponse(response string) (store.VersionedValue, bool, error) {
	if response == "NOT_FOUND" {
		return store.VersionedValue{}, false, nil
	}

	kind, rest := splitFirst(response)
	timestampStr, rest := splitFirst(rest)
	nodeID, value := splitFirst(rest)

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return store.VersionedValue{}, false, fmt.Errorf("invalid timestamp %q: %w", timestampStr, err)
	}

	switch kind {
	case "VALUE":
		return store.VersionedValue{
			Value: value,
			Version: store.Version{
				Timestamp: timestamp,
				NodeID:    nodeID,
			},
			Tombstone: false,
		}, true, nil
	case "TOMBSTONE":
		return store.VersionedValue{
			Version: store.Version{
				Timestamp: timestamp,
				NodeID:    nodeID,
			},
			Tombstone: true,
		}, true, nil
	default:
		return store.VersionedValue{}, false, fmt.Errorf("unknown replica response %q", response)
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
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, node, kv, pool)
	}
}

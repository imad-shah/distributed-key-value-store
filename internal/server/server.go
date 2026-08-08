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

type repairState int

const (
	replicationFactor = 3

	readQuorum  = 2
	writeQuorum = 2

	writeTimeout = 2 * time.Second
	readTimeout  = 2 * time.Second

	repairNotNeeded repairState = iota
	repairMissing
	repairStale
	repairConflict
	repairInvalidWinner
)

// replicaReadResult is one replica read
// Found=false means the replica has no record/doesnt exist
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

func handleClientConnection(conn net.Conn, node *cluster.Node, kv *store.Store, pool *Pool) {
	defer conn.Close()

	serve(conn, conn, func(cmd Command) string {
		return handleClientCommand(cmd, node, kv, pool)
	})
}

func handleReplicaConnection(conn net.Conn, kv *store.Store) {
	defer conn.Close()

	serve(conn, conn, func(cmd Command) string {
		return handleReplicaCommand(cmd, kv)
	})
}

type commandHandler func(Command) string

func serve(r io.Reader, w io.Writer, handler commandHandler) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()

		cmd, err := Parse(line)
		if err != nil {
			writeLine(w, fmt.Sprintf("parse error %v", err))
			continue
		}
		response := handler(cmd)
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
	response, err := forwardOnce(conn, line)
	if err == nil {
		pool.Put(addr, conn)
		return response, nil
	}

	// retrying, first connection failed
	// so abandon it
	conn.Close()

	// retry again with a fresh connection
	conn, err = pool.DialAlwaysFresh(addr)
	if err != nil {
		return "", err
	}

	response, err = forwardOnce(conn, line)
	if err != nil {
		conn.Close()
		return "", err
	}

	pool.Put(addr, conn)
	return response, nil
}

func forwardOnce(conn net.Conn, line string) (string, error) {
	// bounds how long writing can block
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return "", err
	}
	if _, err := fmt.Fprintf(conn, "%s\n", line); err != nil {
		return "", err
	}

	// bounds how long reading can block
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return "", err
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}

	// reset deadlines
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return "", err
	}
	return strings.TrimRight(response, "\n"), nil
}

func writeLine(w io.Writer, s string) {
	if _, err := w.Write([]byte(s + "\n")); err != nil {
		log.Printf("write error: %v", err)
	}
}

func handleClientCommand(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	switch cmd.Type {
	case CmdGet:
		return coordinateGet(cmd, node, kv, pool)

	case CmdSet:
		return coordinateSet(cmd, node, kv, pool)

	case CmdDelete:
		return coordinateDelete(cmd, node, kv, pool)

	default:
		return "error command not allowed on client interface"
	}
}

func handleReplicaCommand(cmd Command, kv *store.Store) string {
	if !isReplicaCommand(cmd.Type) {
		return "error command not allowed on replica interface"
	}
	return executeLocal(cmd, kv)
}

func coordinateSet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	record := newRecord(cmd, node)

	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	results := make(chan error, len(replicas))
	for _, replica := range replicas {
		go func(r cluster.Replica) {
			err := setOnReplica(r, cmd.Key, record, kv, pool)
			if err != nil {
				log.Printf("write for key %q to replica %q failed: %v", cmd.Key, r.ID, err)
			}
			results <- err
		}(replica)
	}
	successCount := 0
	for range replicas {
		err := <-results
		if err == nil {
			successCount++
			if successCount >= writeQuorum {
				return "OK"
			}
		}
	}

	return fmt.Sprintf("error write quorum not reached: got %d acks, want %d", successCount, writeQuorum)
}

// fan out reads to all replicas
// wait until read quorum valid responses come back (or all replicas respond)
// return the newest quorum value
// keep collecting responses and repairing replicas in the background

func coordinateGet(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	responses := fetchReplicaResponses(replicas, cmd, kv, pool)
	validResponses := make([]replicaReadResponse, 0, len(replicas))
	got := 0

	for got < len(replicas) && len(validResponses) < readQuorum {
		response := <-responses
		got++

		if response.Err != nil {
			continue
		}

		validResponses = append(validResponses, response)
	}

	if len(validResponses) < readQuorum {
		return fmt.Sprintf(
			"error read quorum not reached: got %d responses, want %d",
			len(validResponses),
			readQuorum,
		)
	}
	winner, found := chooseNewest(validResponses)
	initialResponses := append([]replicaReadResponse(nil), validResponses...)

	go finishReadRepair(cmd.Key, responses, len(replicas)-got, initialResponses, kv, pool)

	if !found || winner.Tombstone {
		return "NOT_FOUND"
	}
	return winner.Value

}

// collect the replica outcomes that were not consumed before quorum
// add every valid result to the initial quorum responses
// select the final winner using all valid replica responses
// repair replicas that are missing or contain an older record
func finishReadRepair(key string, responses <-chan replicaReadResponse, remaining int, validResponses []replicaReadResponse, kv *store.Store, pool *Pool) {
	for range remaining {
		response := <-responses

		if response.Err != nil {
			continue
		}
		validResponses = append(validResponses, response)
	}

	winner, found := chooseNewest(validResponses)
	if !found {
		return
	}
	repairAllOutdatedReplicas(key, validResponses, winner, kv, pool)
}

// create a channel
// start a concurrent read for each replica
// return the channel back immediately to coordinateGet()
func fetchReplicaResponses(replicas []cluster.Replica, cmd Command, kv *store.Store, pool *Pool) <-chan replicaReadResponse {
	responses := make(chan replicaReadResponse, len(replicas))

	for _, replica := range replicas {
		go fetchSingleReplicaResponse(replica, cmd, kv, pool, responses)
	}
	return responses
}

func coordinateDelete(cmd Command, node *cluster.Node, kv *store.Store, pool *Pool) string {
	record := newRecord(cmd, node)

	replicas, err := node.Replicas(cmd.Key, replicationFactor)
	if err != nil {
		return fmt.Sprintf("err finding replicas: %v", err)
	}

	buffer := make(chan error, len(replicas))
	for _, replica := range replicas {
		go func(r cluster.Replica) {
			err := deleteFromReplica(r, cmd.Key, record, kv, pool)
			if err != nil {
				log.Printf("delete for key %q to replica %q failed: %v", cmd.Key, r.ID, err)
			}
			buffer <- err
		}(replica)
	}

	successCount := 0
	for range replicas {
		err := <-buffer
		if err == nil {
			successCount++
			if successCount >= writeQuorum {
				return "OK"
			}
		}
	}

	return fmt.Sprintf("error delete quorum not reached: got %d acks, want %d", successCount, writeQuorum)
}

// compare versions of all found records
// **ignore replicas that don't have a record
// return the newest record and whether there was even one found
func chooseNewest(responses []replicaReadResponse) (store.VersionedValue, bool) {
	var winner store.VersionedValue
	foundWinner := false

	for _, response := range responses {
		if !response.Result.Found {
			continue
		}
		if !foundWinner || response.Result.Value.Version.After(winner.Version) {
			winner = response.Result.Value
			foundWinner = true
		}
	}
	return winner, foundWinner
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
func parseReplicaGetResponse(
	response string,
) (store.VersionedValue, bool, error) {
	if response == "NOT_FOUND" {
		return store.VersionedValue{}, false, nil
	}

	kind, rest := splitFirst(response)
	switch kind {
	case "VALUE":
		version, value, err := parseReplicaVersion(rest, response)
		if err != nil {
			return store.VersionedValue{}, false, err
		}
		if value == "" {
			return store.VersionedValue{}, false,
				fmt.Errorf("malformed VALUE response %q", response)
		}
		return store.VersionedValue{
			Value:   value,
			Version: version,
		}, true, nil

	case "TOMBSTONE":
		version, extra, err := parseReplicaVersion(rest, response)
		if err != nil {
			return store.VersionedValue{}, false, err
		}
		if extra != "" {
			return store.VersionedValue{}, false,
				fmt.Errorf(
					"malformed TOMBSTONE response %q",
					response,
				)
		}
		return store.VersionedValue{
			Version:   version,
			Tombstone: true,
		}, true, nil

	default:
		return store.VersionedValue{}, false,
			fmt.Errorf("unknown replica response %q", response)
	}
}

func parseReplicaVersion(
	rest string,
	response string,
) (store.Version, string, error) {
	timestampStr, rest := splitFirst(rest)
	nodeID, remaining := splitFirst(rest)

	if timestampStr == "" || nodeID == "" {
		return store.Version{}, "",
			fmt.Errorf("malformed replica response %q", response)
	}

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return store.Version{}, "",
			fmt.Errorf(
				"invalid timestamp %q: %w",
				timestampStr,
				err,
			)
	}

	return store.Version{
		Timestamp: timestamp,
		NodeID:    nodeID,
	}, remaining, nil
}

// read the requested key from one local or remote replica
// log any network or protocol failure
// send the replica identity, stored value, and error to the response channel
func fetchSingleReplicaResponse(replica cluster.Replica, cmd Command, kv *store.Store, pool *Pool, responses chan<- replicaReadResponse) {
	value, found, err := getFromReplica(replica, cmd, kv, pool)
	if err != nil {
		log.Printf(
			"read key %q from replica %q failed: %v",
			cmd.Key,
			replica.ID,
			err,
		)
	}
	responses <- replicaReadResponse{
		Replica: replica,
		Result: replicaReadResult{
			Value: value,
			Found: found,
		},
		Err: err,
	}

}

// compare every valid replica response with the final winner
// **skip replicas that already contain the winner
// apply the winner to missing or stale replicas
func repairAllOutdatedReplicas(key string, responses []replicaReadResponse, winner store.VersionedValue, kv *store.Store, pool *Pool) {
	for _, response := range responses {
		state := classifyRepair(response.Result, winner)

		switch state {
		case repairNotNeeded:
			continue
		case repairMissing, repairStale:
			if err := applyWinnerToReplica(
				response.Replica,
				key,
				winner,
				kv,
				pool,
			); err != nil {
				log.Printf(
					"read repair for key %q on replica %q failed: %v",
					key,
					response.Replica.ID,
					err,
				)
			}
		case repairConflict:
			log.Printf(
				"read repair conflict for key %q on replica %q: same version, different data",
				key,
				response.Replica.ID,
			)
		case repairInvalidWinner:
			log.Printf(
				"read repair invariant violation for key %q: replica %q has newer value than selected winner",
				key,
				response.Replica.ID,
			)
		}
	}
}

func applyWinnerToReplica(replica cluster.Replica, key string, winner store.VersionedValue, kv *store.Store, pool *Pool) error {
	if winner.Tombstone {
		return deleteFromReplica(replica, key, winner, kv, pool)
	}
	return setOnReplica(replica, key, winner, kv, pool)
}

func classifyRepair(result replicaReadResult, winner store.VersionedValue) repairState {
	if !result.Found {
		return repairMissing
	}

	if result.Value == winner {
		return repairNotNeeded
	}

	if winner.Version.After(result.Value.Version) {
		return repairStale
	}

	if winner.Version.Equal(result.Value.Version) {
		return repairConflict
	}
	return repairInvalidWinner
}

func StartServers(clientAddr, replicaAddr string, node *cluster.Node, kv *store.Store, pool *Pool) error {
	network := "tcp"

	clientListener, err := net.Listen(network, clientAddr)
	if err != nil {
		log.Printf("error starting client listener: %v", err)
		return err
	}

	replicaListener, err := net.Listen(network, replicaAddr)
	if err != nil {
		clientListener.Close()
		log.Printf("error starting replica listener: %v", err)
		return err
	}
	defer clientListener.Close()
	defer replicaListener.Close()

	clientHandler := func(conn net.Conn) { handleClientConnection(conn, node, kv, pool) }
	replicaHandler := func(conn net.Conn) { handleReplicaConnection(conn, kv) }

	log.Printf("client listener active on %s", clientListener.Addr())
	log.Printf("replica listener active on %s", replicaListener.Addr())
	errCh := make(chan error, 2)

	go func() {
		errCh <- fmt.Errorf("client listener: %w", acceptLoop(clientListener, clientHandler))
	}()
	go func() {
		errCh <- fmt.Errorf("replica listener: %w", acceptLoop(replicaListener, replicaHandler))
	}()
	return <-errCh
}

type connectionHandler func(net.Conn)

func acceptLoop(listener net.Listener, handler connectionHandler) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handler(conn)
	}
}

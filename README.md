# distributed-key-value-store

An in-memory key-value store in Go, served over TCP, being built toward a
distributed, partitioned, and replicated system.

## Current Progress
The store currently supports:

- concurrent TCP clients
- GET, SET, and DELETE commands
- consistent hashing with virtual nodes
- replication across 3 nodes
- read and write quorums
- versioned values
- tombstones for deletes
- connection pooling between nodes

A client can connect to any node. That node acts as the coordinator for the
request, finds the replicas responsible for the key, and communicates with
the other nodes when needed.

The current quorum configuration is:

    replication factor (N) = 3
    read quorum (R)        = 2
    write quorum (W)       = 2

**Guarantees: R + W > N**

## Design

The store is an in-memory `map[string]VersionedValue` guarded by a `sync.RWMutex`,
shared across all client connections. 

Each stored record contains:

```go
type VersionedValue struct {
    Value     string
    Version   Version
    Tombstone bool
}
```

A version contains a timestamp and the ID of the node that coordinated the
write:

```go
type Version struct {
    Timestamp int64
    NodeID    string
}
```

The timestamp is used to choose the newest value during quorum reads. The node
ID is used as a deterministic tie-breaker when two versions have the same
timestamp.

The server accepts each TCP connection in its own goroutine and speaks a
simple line-based client protocol:
```
SET key value    -> OK
GET key          -> value | NOT_FOUND
DELETE key       -> OK
<bad input>      -> error <reason>
```

Client commands do not contain replication metadata. The node receiving the
request creates the version and then sends internal commands to the replicas:

```
REPLICA_SET key timestamp nodeID value
REPLICA_GET key
REPLICA_DELETE key timestamp nodeID
```

Internal `GET` responses include one of these three stored versions:
```
VALUE timestamp nodeID value
TOMBSTONE timestamp nodeID
NOT_FOUND
```

## Writes

For a `SET` request, the coordinator creates one `VersionedValue` and sends the
same record to all 3 replicas.

The write succeeds after at least 2 replicas acknowledge it.
```
node-a -> foo = bar @ time X
node-b -> foo = bar @ time X
node-c -> foo = bar @ time X
```

All replicas receive the same timestamp and coordinator node ID. 

## Reads

For a `GET` request, the coordinator reads from replicas until it receives 2
successful responses.

Each replica returns its local value and version. The coordinator compares the
versions, chooses the newest record, and returns that to the client.
```
node-a -> bar @ time 100
node-b -> baz @ time 200
```

`GET foo` returns `baz` because it has the more recent time

Note: A missing value and a tombstone are different 
```
missing key:
    no record exists

tombstone:
    a versioned delete record exists
```

while both return `NOT_FOUND` to client, the tombstone prevents an
older value from being restored later (zombie data).

## Deletes

`DELETE` does not physically remove the key from the store.

The coordinator creates a new versioned tombstone and replicates it using the
same write quorum as `SET`.

`DELETE foo`
```
node-a -> tombstone @ version X
node-b -> tombstone @ version X
node-c -> tombstone @ version X
```

Keeping the tombstone allows replicas to distinguish an intentional delete from
a node that does not have the key.

## Partitioning

Keys are assigned to nodes using consistent hashing with virtual nodes.

Each physical node is placed on the 64-bit hash ring at 256 virtual positions.
A key is assigned to the first node found clockwise from the key's hash.

For replication, the key is assigned to the first 3 distinct physical nodes
found clockwise on the ring.

Consistent hashing keeps rebalancing proportional. Removing one of N nodes
remaps roughly 1/N of the keys instead of remapping almost everything as
`hash(key) % N` would.

## Connection Pooling

Nodes reuse TCP connections when communicating with peers.

The pool keeps a bounded list of idle connections for each peer. A request
borrows one connection, sends one command, reads one response, and returns the
connection to the pool.

If no idle connection is available, the node opens a new one.

## Layout

    cmd/kvstore/         server entry point

    config/              shared cluster configuration
    docker/              Dockerfile and Compose configuration

    internal/cluster/    cluster membership and replica lookup
    internal/server/     TCP server, protocol parser, quorum coordination
    internal/store/      versioned in-memory key-value store
    internal/hashring/   consistent hashing ring

    benchmarks/          distribution measurement script
    decisions/           architecture decision records

## Cluster Configuration

All nodes load the same cluster definition from `config/cluster.yaml`.

Example:
```
listen_address: ":8080"

nodes:
  - id: node-a
    address: node-a:8080
  - id: node-b
    address: node-b:8080
  - id: node-c
    address: node-c:8080
```
`listen_address` is the local address each server binds to inside its container.

Each address under `nodes` is the advertised address other nodes use to reach that server over the Docker Compose network.

Every container receives the same configuration file but starts with a different `--id` value.

## Decisions

- [001: Virtual nodes and FNV-1a prefix ordering](decisions/001-virtual-nodes.md)
- [002: Connection pool for forwarding](decisions/002-connection-pool.md)
- [003: Quorum semantics](decisions/003-quorum-semantics.md)
- [004: Replica protocol trust boundary](decisions/004-replica-protocol-boundary.md)

## Running the tests

```bash
go test ./...                      # all packages
go test -race ./...                # with the race detector
go test -v ./internal/hashring/    # includes rebalance move percentages
```

## Distribution benchmark

```bash
go run ./benchmarks/distribution.go
```

## Running a Three-Node Cluster

The cluster uses one shared configuration file and one Docker image. Docker Compose starts three instances of the same server with different node IDs.

To start, run:
```bash
docker compose -f docker/compose.yaml up --build
```

The debugging port mappings are:
```
localhost:8080 -> node-a:8080
localhost:8081 -> node-b:8080
localhost:8082 -> node-c:8080
```
All three nodes listen on port 8080 inside their own containers. The different host ports allow each node to be contacted directly during development.

Connect to any node:
```bash 
nc localhost 8080 # node-a
```
You can also connect directly to other nodes:
```bash
nc localhost 8081 # node-b
nc localhost 8082 # node-c
```
Example commands:
```
SET foo bar
GET foo
DELETE foo
GET foo
```
The node receiving the client command acts as the coordinator. It finds the three replicas for the key, sends the operation to those replicas, and returns success after the required quorum is reached.

### Running in the background
Use detached mode to start the cluster without keeping the logs attached to the current terminal.
```bash
docker compose -f docker/compose.yaml up --build -d
```

View logs from the full cluster:
```bash
docker compose -f docker/compose.yaml logs -f
```

View logs from one node:
```bash
docker compose -f docker/compose.yaml logs -f node-a
```

Stop and remove the cluster containers:
```bash
docker compose -f docker/compose.yaml down
```
The store is currently in memory, so stopping the containers removes all stored records.

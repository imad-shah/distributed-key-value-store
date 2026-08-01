# distributed-key-value-store

An in-memory key-value store in Go, served over TCP, being built toward a
distributed (partitioned, replicated) system.

## Current Progress

Single-node server works. A TCP server accepts concurrent clients and
serves GET/SET/DELETE against a shared in-memory store.

## Design

The store is an in-memory `map[string]string` guarded by a `sync.RWMutex`,
shared across all client connections. The server accepts each connection
in its own goroutine and speaks a simple line-based text protocol:

    SET key value    -> OK
    GET key          -> value | NOT_FOUND
    DELETE key       -> OK | NOT_FOUND
    <bad input>      -> error <reason>

Keys will be partitioned across nodes using consistent hashing with
virtual nodes, each node sits on a 64-bit hash ring at 256 positions, and
a key routes to the first node position at or after the key's hash. 
This keeps rebalancing proportional, removing one of N nodes remaps roughly
1/N of keys and leaves the rest in place, rather than almost 
everything as `hash % N` would.

## Layout

    cmd/kvstore/         server entry point

    internal/server/     TCP server, protocol parser, request dispatch
    internal/store/      in-memory key-value store
    internal/hashring/   consistent hashing ring

    benchmarks/          distribution measurement script
    decisions/           architecture decision records

## Decisions

- [001: Virtual nodes and FNV-1a prefix ordering](decisions/001-virtual-nodes.md)
- [002: Connection pool for forwarding](decisions/002-connection-pool.md)

## Running the Server

```bash
go run ./cmd/kvstore/
```

Then connect with any TCP client:

```bash
nc localhost 8080
SET foo bar
GET foo
```

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

## Testing multiple servers with pools

Terminal 1:
```bash
go run ./cmd/kvstore/ --id node-a --addr :8080 --peers node-b=:8081,node-c=:8082
```
Terminal 2:
```bash
go run ./cmd/kvstore/ --id node-b --addr :8081 --peers node-a=:8080,node-c=:8082
```
Terminal 3:
```bash
go run ./cmd/kvstore/ --id node-c --addr :8082 --peers node-a=:8080,node-b=:8081
```

Terminal 3:
```bash
nc localhost 8080 # only connected to node-a ever
SET foo bar # routes to node-a (local)
GET foo # returns bar | pulls from node-a
SET bar baz # routes to node-b (forwarded)
GET bar # returns baz | pulls from node-b 
```
Even though we are connected to **only** node-a (8080), we still are able to: \
`set` 'bar' -> 'baz'\
`get` 'bar' \
and which can only happen on node-b, proving that node-a is able to forward the request to node-b
and return the response
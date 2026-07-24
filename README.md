# distributed-key-value-store

A distributed key-value store written in Go.

## Design

Keys are partitioned across servers using consistent hashing with
virtual nodes. Each server is placed on a 64-bit hash ring at 256
positions, and a key routes to the first server position at or after
the key's own hash. This keeps rebalancing proportional. Removing one
of N servers remaps roughly 1/N of keys and leaves the rest in place,
rather than remapping nearly everything as `hash % N` would.

## Layout

    internal/hashring/   hashing ring
    cmd/kvstore/         server entry point (WIP)
    benchmarks/          distribution measurement script
    decisions/           architecture decision records

## Decisions

- [001 — Virtual nodes and FNV-1a prefix ordering](decisions/001-virtual-nodes.md)

## Running the tests
```bash
go test ./...
go test -v ./internal/hashring/   # includes rebalance percentages
```

## Running distribution test

```bash
go run benchmarks/distribution.go  
```

## Testing Key-Val Store

```bash
go test -race ./internal/store/
```
# 006: Asynchronous Read Repair After Read Quorum

## Context

The original read repair design waited for all `N = 3` replica outcomes before
choosing a winner, repairing stale replicas, and returning to the client

This made the repair logic very simple, but client read latency was tied to the slowest
replica

The store already requires only `R = 2` valid responses for a successful read,
so waiting for all 3 outcomes unnecessarily delayed reads

## Decision

For a `GET`:

1. Send read requests to all `N = 3` replicas concurrently
2. Collect responses until `R = 2` valid responses are available, or all
   replicas have responded
3. If fewer than `R` valid responses arrive, fail the read
4. Choose the newest record from the initial quorum responses
5. Return that value, or `NOT_FOUND` for a tombstone, immediately to the client
6. Continue collecting the remaining replica outcomes in a background goroutine
7. Recompute the winner using every valid response
8. Repair replicas that are missing the final winner or contain an older record

A network error, timeout, or malformed response **does not** count toward read quorum

A successful `NOT_FOUND` response **does** count toward quorum

Replicas that failed to provide a valid read response are not repaired during
that request (they have an unknown state if they didn't respond)

## Consequences

Pros:

- client latency depends on the fastest `R` valid responses rather than waiting for
  the slowest replica
- slow replicas do not unnecessarily delay reads
- read repair still converges stale replicas in the background

Cons:

- the first `R` responses may not contain the newest value stored in the full
  replica set (sacrifices consistency)
- introduces more complexity in implementation, such as goroutine and channel lifecycle being bounded by network timeouts

## Example

Suppose:

```bash
node-a -> old @ version 100
node-b -> old @ version 100

node-c -> new @ version 200 # but responds too slow
```

The coordinator may receive node-a and node-b first, reach `R = 2`, and return
`old` to the client.

When node-c later returns `new`, the background repair selects version 200 as the
final winner and repairs node-a and node-b

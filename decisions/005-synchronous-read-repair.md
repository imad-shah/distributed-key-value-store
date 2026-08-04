# 005: Synchronous Read Repair After All Replica Responses

## Context

The store uses replication factor `N = 3` and read quorum `R = 2`.

A distributed `GET` may receive different states from the replicas responsible
for a key:

- the current live value
- an older live value
- a tombstone
- no record
- a network or protocol error

After reading from the replicas, the coordinator selects the newest
`VersionedValue` as the winner. Replicas that are missing the winner or contain
an older version should then be repaired so that the cluster has a

The main design question is if the coordinator should immediately return the winning value
to the client and asynchronously update the old replicas later, or update them first and then
reply to the client.

## Options Considered

### 1. Return after `R` valid responses

The coordinator could return as soon as it receives the read quorum.

The remaining replica responses could be collected in the background, and stale
replicas could then be repaired asynchronously.

Advantages:

- lower client latency
- the client does not wait for the slowest replica
- repair work does not delay the response

Disadvantages:

- the first `R` responses may not contain the newest value stored anywhere in
  the replica set
- the selected winner may change when a later response arrives
- the coordinator must keep background work alive after the client response
- channel and goroutine lifecycle management becomes more complex
- asynchronous repair requires additional testing and observability

### 2. Wait for all `N` replica outcomes

The coordinator can send all replica reads concurrently but wait until every
replica returns a value, returns `NOT_FOUND`, or fails due to a timeout or other
error.

It can then select the newest value from every valid response and synchronously
repair stale replicas before returning to the client.

Advantages:

- winner selection considers every replica that responded successfully
- the coordinator knows which replicas are missing, stale, current, or
  conflicted
- repair behavior is deterministic and easier to reason about
- no background goroutine lifecycle is required
- integration tests can inspect repaired replicas immediately after the client
  response

Disadvantages:

- client latency includes the slowest replica read
- client latency also includes synchronous repair attempts
- a slow replica delays a read even after `R` valid responses are available

## Decision

The initial read-repair implementation (#2) will:

1. Find all `N` replicas responsible for the key.
2. Send read requests to all replicas concurrently.
3. Wait for all `N` outcomes.
4. Keep responses where the read completed without an error.
5. Fail the read if fewer than `R` valid responses were received.
6. Select the newest `VersionedValue` from the valid responses.
7. Compare each valid replica response with the winner.
8. Repair replicas that are missing the record or contain an older version.
9. Wait for the repair attempts to complete.
10. Return the winning value, or `NOT_FOUND` when the winner is a tombstone or
    no record exists.

A successful `NOT_FOUND` response counts toward read quorum. A network error,
timeout, or malformed response does not.

Repair failures are logged but do not turn an otherwise successful quorum read
into a client error.

Replicas that failed to provide a valid read response are not repaired during
that request because their current state is unknown.

## Consequences

The implementation favors simplicity and complete winner selection over minimum
read latency.

For the current configuration, the coordinator waits for all three replica
outcomes even though only two valid responses are required for quorum.

The network deadlines prevent an unavailable or unresponsive replica from
blocking forever, but the client may still wait for the full timeout.

Because repair is synchronous, the replica state can be checked immediately
after the `GET` returns. This also makes the initial integration tests simpler
and deterministic.

## Future Change

A future implementation should return after `R` valid responses and continue
processing the remaining replica responses asynchronously.

That version should:

- preserve the buffered response channel after the client response
- continue collecting the remaining outcomes
- reconsider the winner if a newer response arrives
- repair stale replicas in the background
- ensure background goroutines cannot block or leak
- add tests for asynchronous completion and eventual convergence
# 002: Connection Pool for Forwarding

## The Problem

When a node gets a command for a key it doesn't own, it forwards that command
to the node that does. Forwarding means opening a network connection to the
other node, sending the command, and reading the reply.

The first version opened a **brand-new connection every single time**:

```text
dial the peer  ->  send command  ->  read reply  ->  close connection
```

Opening a TCP connection at scale will inevitably be expensive. It's a back-and-forth handshake over the
network (three messages before you can send anything), plus the operating system
has to set up a new socket. Doing that for *every* forwarded request is wasteful
since a node usually forwards to the same few peers over and over.

We're paying a setup cost on every request that we could pay once and reuse

## The Decision

Keep a small stash of already-open connections to each peer, and reuse them
instead of dialing fresh every time. This stash is the **connection pool**.

### How the pool actually works

**Borrow (`Get`)**

- Check the idle connections for the peer.
- Close and discard expired entries.
- Return the first non-expired connection.
- Dial a fresh connection if no reusable entry remains.

**Return (`Put`)**

- Record the time the connection became idle.
- Put it back if the per-peer pool has room.
- Close it if the pool is full.

### Handling broken connections

A pooled connection may become unusable while sitting idle, for example if the
peer restarts or closes the socket.

A connection only returns to the pool when a request completes successfully.
If writing, reading, or resetting deadlines fails, that connection is closed.

When the first connection attempt fails during writing, reading, or deadline handling, the coordinator closes that connection and retries the request once
using a guaranteed-fresh TCP connection.

The retry is bounded to one additional attempt. If the fresh connection also
fails, the error is returned to the caller.

### Idle connection expiration

Each idle connection stores the time it was returned to the pool.

When `Get` examines an idle connection, it discards and closes it if it has been
idle longer than 30 seconds. It continues checking the remaining pooled
connections and dials a fresh connection if no reusable connection remains.

Expiration is lazy: idle connections are checked when borrowed rather than by a
background cleanup goroutine.

## Alternatives

**No pooling (Original implementation).** Simple, but pays the connection-setup
cost on every forward. Bad under load.

**One shared connection per peer, with many requests multiplexed over it.**
This is what HTTP/2 and gRPC do. It's more efficient (a single connection
carries everything), but it requires *tagging* every request with an ID and
matching replies back to the right request, because multiple requests share one
byte stream and their responses can come back interleaved. To create this
I would've needed to build a whole protocol. Invites too much complexity.

**A pool of separate connections per peer (what I chose).** Each borrowed
connection carries exactly one request at a time, so there's no interleaving and
no need for request IDs. We get connection reuse without building a multiplexing protocol.
This is also how most database drivers manage connections, so it's an existing pattern.

## Trade-offs and known limits

- A failed request is retried only once. The pool does not implement unlimited
  retry or exponential backoff.

- Idle expiration is lazy. A connection may remain open beyond 30 seconds if
  that address is never accessed again.

- There are no proactive health checks. TCP connection health is determined by
  attempting to use the connection.

- The pool has no global shutdown method yet, so idle connections are not
  explicitly closed during graceful server shutdown.

- Pool behavior is configured in code rather than through cluster
  configuration.

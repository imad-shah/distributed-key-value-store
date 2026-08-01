# 002: Connection Pool for Forwarding

## The Problem

When a node gets a command for a key it doesn't own, it forwards that command
to the node that does. Forwarding means opening a network connection to the
other node, sending the command, and reading the reply.

The first version opened a **brand-new connection every single time**:

```
dial the peer  ->  send command  ->  read reply  ->  close connection
```

Opening a TCP connection at scale will inevitably be expensive. It's a back-and-forth handshake over the
network (three messages before you can send anything), plus the operating system
has to set up a new socket. Doing that for *every* forwarded request is wasteful
since a node usually forwards to the same few peers over and over.

We're paying a setup cost on every request that we could pay once and reuse


## The Decision

Keep a small stash of already-open connections to each peer, and reuse them
instead of dialing fresh every time. This stash is the **connection pool**

### How the pool actually works

The pool keeps, for each peer address, a small set of connections that are open
but not currently in use ("idle"). The set is capped (8 per peer).

Two operations:

**Borrow (`Get`)** -> "I need a connection to this peer."
- If there's an idle one sitting in the pool, take it and use it.
- If the pool is empty, dial a fresh one.

**Return (`Put`)** -> "I'm done with this connection."
- If the pool has room (under the cap of 8), put it back so it can be reused.
- If the pool is already full, close it.


### Handling broken connections

A pooled connection is reused across many requests. If a request leaves a
connection in a bad state, like an error while sending, an error while reading, or
the peer hung up, we must not put that connection back in the pool. The next
request to borrow it would inherit the mess and fail.

A connection only goes back in the pool if its request finished
cleanly. Any error along the way will close the connection, not return it.
The next borrower just dials a fresh one.


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

- **A dead pooled connection surfaces as a failed request.** If a connection has
  been sitting in the pool and the peer closed it in the meantime, the next
  request to borrow it will fail when it tries to send. Right now that failure is
  passed back to the client as an error. A better version would notice the dead
  connection, quietly throw it away, and retry once with a fresh dial. Haven't
  built that retry yet.

- **No idle timeout or health checks.** Pooled connections stick around until
  they're either reused or pushed out by the cap. Real pools close
  connections that have been idle too long and periodically check they're still
  alive. Not needed yet.


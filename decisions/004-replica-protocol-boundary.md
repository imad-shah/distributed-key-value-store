# 004: Replica Protocol Trust Boundary

## Context

The server currently accepts client commands and internal replication commands through the same TCP listener.

Client commands are:

* `GET`
* `SET`
* `DELETE`

Internal commands are:

* `REPLICA_GET`
* `REPLICA_SET`
* `REPLICA_DELETE`

Because both protocols share one listener, any client that can connect to a node and submit internal replica commands directly.

## Decision

For now, the cluster is considered a trusted environment. 

Direct client access to `REPLICA_*` commands is a known limitation, not an intended public API.

Separating the client and replica protocols are planned next steps to ensure reliability.

## Consequences

Until the protocols are separated or authenticated, a client can bypass normal coordination and quorum handling and submit replication metadata directly to an individual node.

Future solutions may include:

* separate client and replica ports
* network-level access restrictions
* authenticated internal requests
* mutual TLS between nodes
* a dedicated internal RPC protocol

Most likely implementation fix for this scope will be to separate client and replica ports

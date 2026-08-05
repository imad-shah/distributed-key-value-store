# 004: Separate Client and Replica Protocol Listeners

## Context

The server originally accepted client commands and internal replica commands through the same TCP listener.

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

Each node now runs two separate TCP listeners:

- a client listener for `GET`, `SET`, and `DELETE`
- a replica listener for `REPLICA_GET`, `REPLICA_SET`, and `REPLICA_DELETE`

Now, the `client listener` rejects replica commands, and the `replica listener` rejects client commands.

Every node can act as both a coordinator and a replica. These are per-request roles rather than permanent node types.

The node that receives a client request becomes the coordinator. It communicates with the replica listeners of the nodes selected by the hash ring.

Only client ports are published to the host through Docker Compose.

Cluster configuration now separates the two interfaces:

- `client_listen_address` : `":8080"`
- `replica_listen_address` : `":9090"`

Replica ports remain available inside the Docker Compose network so nodes can communicate with one another.

## Consequences

Pros:

- clients cannot submit replica commands through the client interface
- client and replica command handling now have explicit boundaries
- replica traffic can remain internal to the Docker network
- any node can still coordinate client requests
- internal replication metadata is no longer accepted through the client protocol

Cons:

- every node must run two listeners
- configuration must include client and replica addresses
- tests must create separate client and replica listeners
- startup and shutdown must manage both listeners
- deployment configuration must allow nodes to reach each other's replica ports

## Security Limitations

This change creates a protocol and network boundary, but it does not authenticate replica requests.

Any process with access to the internal network could still submit replica commands.

To actually handle security, future protections may include:

- mutual TLS
- signed internal requests
- service identity
- Docker or Kubernetes network policies
- private subnets
- an authenticated internal RPC protocol
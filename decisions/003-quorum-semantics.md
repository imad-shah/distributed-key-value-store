# 003: Quorum Semantics

## Context

The cluster currently uses:

* Replication factor: `N = 3`
* Read quorum: `R = 2`
* Write quorum: `W = 2`

A coordinator sends operations to the replicas that are responsible for a key and decides whether the client operation was successful or not based on the number of valid responses.

## Set & Delete

A `SET` or `DELETE` succeeds after at least two replicas return a valid `OK` acknowledgement.

Network errors, timeouts, malformed responses, and rejected writes do not count toward the write quorum.

## Get

A `GET` succeeds after at least two replicas return valid read responses. Each of the following counts as a valid response:

* `VALUE`
* `TOMBSTONE`
* `NOT_FOUND`

Network errors and timeouts do not count toward the read quorum.

After reaching the read quorum, the coordinator selects the newest returned record using its version.
Tombstones participate in version comparison like live values but are returned to the client as `NOT_FOUND`.

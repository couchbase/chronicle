Chronicle is an implementation of Raft consensus protocol that Couchbase
Server uses to manage its internal metadata.

Notable features:

 * Supports voting and non-voting participants.
 * Implements pre-vote protocol to prevent individual nodes from starting
   elections unnecessarily.
 * Supports multiple pluggable state machines that run on a single replicated
   log.
 * Reads are served by the local replica by default, providing sequential
   consistency. Linearizable reads are also supported.
 * Supports exactly-once writes.
 * Extends Raft to support quorum-loss disaster recovery: through manual
   intervention, it's possible to make the cluster available for writes again
   even when the majority of nodes are unavailable. The surviving nodes may
   not have all updates (violating linearizability), but all updates that made
   it to them will be preserved. The surviving nodes will be insulated from
   the failed-over nodes so no new updates from them may propagate.

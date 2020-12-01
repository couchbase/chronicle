# Example

The chronicle library has very few dependencies, which is good for reuse but
can mean it's hard to get started with. This example allows you to start a
REST API server on a collection of nodes that drives a chronicle process.

## Build

You build all examples as follows (at the top-level directory):

`rebar3 as examples compile`

There are instructions to get rebar3 on your system at: https://github.com/erlang/rebar3.
Given that you'll need Erlang on your system to run chronicle, the easiest
thing to do is probably to build it and install it locally via:

```
$ git clone https://github.com/erlang/rebar3.git
$ cd rebar3
$ ./bootstrap
$ rebar3 local install
```

## Start a cluster of example nodes

Run:

`start_cluster --app chronicled --num-nodes N --hostname 127.0.0.1`

This will start a cluster of N example nodes listening on the loopback
interface. The `--app chronicled` argument instructs the script to start the
example application named `chronicled` - the script can also be used to nodes
running only chronicle.

The i-th node in the cluster is:
- named `chronicle_i@127.0.0.1`
- listens on port `(8080+i)`

## Provision one node

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/provision`

This will "provision" node 0, that is, turns node 0 from an uninitialized node
to an initialized one node cluster running chronicle. One replicated state
machine is provisioned with name `kv`.

## Check the configuration

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/info`

You should see something like:

```
HTTP/1.1 200 OK
content-length: 60
content-type: application/json
date: Fri, 25 Sep 2020 05:41:28 GMT
server: Cowboy
{"voters":["chronicle_0@127.0.0.1"]}.
```

## Add a key-value pair

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key -X PUT -d '1'`


## Get the value associated with a key

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key`

You should see something like this:

```
HTTP/1.1 200 OK
content-length: 81
content-type: application/json
date: Fri, 25 Sep 2020 04:10:16 GMT
server: Cowboy
{"rev":{"history_id":"6e4d2640cbe41b818bb5af4407142be9","seqno":2},"value":1}
```

## Get the key with varying read consistency levels

Chronicle supports 3 read consistency levels:

1. local - read from the local replicated state machine (RSM) copy (this is
   the default)
2. leader - reads from the leader's RSM copy
3. quorum - performs a quorum read

Local reads are performed against the local replicated state machine and don't
involve any network round trips. They are fast and for clients that always
read against a single chronicle node, give sequential consistency.

Quorum reads are fully linearizable and generally incur (1) one network round
trip to the leader (2) a "quorum sync" (wherein the leader must confirm from a
quorum of nodes that it is currently the leader) plus the time take to sync the
replicated log to the local node (so that read-your-own-writes semantics are
preserved).

Leader reads involve a network round-trip to the chronicle leader but skip the
quorum sync. They're faster than quorum reads and under stable leadership
provide linearizable semantics and even in changing leadership conditions 
provide sequentially consistent semantics as long as the client always reads
against the same chronicle node. However, if the client performs leader reads
against different chronicle nodes, reads may be seen to go "back in time". This
non-linearizable behavior of leader reads in systems like chronicle (and etcd)
is well described in this [etcd issue](https://github.com/etcd-io/etcd/issues/741).

Local reads are the default but can be explicitly run as follows:
 
    curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key?consistency=local

For quorum reads, run:

    curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key?consistency=quorum

For leader reads, run:

    curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key?consistency=leader

## Update a value

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/kv/key -X POST -d '{"value": 1}'`

PUTs are used to add key-value pairs; POSTs are used to update the value. Note
that the value can be set to arbitrary JSON.


## Add nodes

To add one node, run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/addnode -d '"chronicle_1@127.0.0.1"'`

To add two, run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/addnode
         -d '["chronicle_1@127.0.0.1", "chronicle_2@127.0.0.1"]'`

Check the configuration from the newly added node:

```
$ curl -i -H "Content-Type: application/json" localhost:8081/config/info
HTTP/1.1 200 OK
content-length: 60
content-type: application/json
date: Fri, 25 Sep 2020 05:41:28 GMT
server: Cowboy
{"voters":["chronicle_0@127.0.0.1","chronicle_1@127.0.0.1"]}.
```

Verify that the new nodes also return the value associated with the key. Run:


`curl -i -H "Content-Type: application/json" 127.0.0.1:8081/kv/key`

Again you should see something like:

```
HTTP/1.1 200 OK
content-length: 81
content-type: application/json
date: Fri, 25 Sep 2020 04:42:46 GMT
server: Cowboy
{"rev":{"history_id":"6e4d2640cbe41b818bb5af4407142be9","seqno":6},"value":{"value":1}
```

## Remove nodes

To remove a node, run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/removenode
         -d '"chronicle_0@127.0.0.1"'`

## Delete a key

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8081/kv/key -X DELETE`

# Example

The chronicle library has very few dependencies, which is good for reuse but 
can mean it's hard to get started with. This example allows you to start a 
REST API server on a collection of nodes that drives a chronicle process. 

## Build

You build the example as follows:

`rebar3 as example compile`

## Start a cluster of example nodes

Run:

`start_cluster --profile example --num-nodes N --hostname 127.0.0.1`
 
This will start a cluster of N example nodes listening on the loopback
interface. The `--profile example` argument instructs the script to start
the example server - the script can also be used to nodes running only 
chronicle. 

The i-th node in the cluster is:
- named `chronicle_i@127.0.0.1`
- listens on port `(8080+i)`

## Provision one node

Run:

`curl -i -H "Content-Type: application/json" 127.0.0.1:8080/config/provision`

This will "provision" node 0, that is, turns node 0 from an uninitialized node
to an initialized one node cluster running chronicle. One replicated state
machine is provisioned with name `kv`. 

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

Once the nodes are added you can verify that the new nodes also return the value
associated with the key. Run:


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
         -d '"chronicle_0@127.0.0.1"'
         
`

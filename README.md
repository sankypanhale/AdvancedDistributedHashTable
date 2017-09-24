## Distributed Hash Table with Concurrent Join and Remove Nodes
One of the biggest problem with the distributed system is to locate/lookup particular file on various nodes in distributed system. We try to solve this problem is quicker way by defining chord.

This service provides functionality of concurrent join and remove of nodes from distributed systems. This uses **eventual consistency** model.

## What is chord?

- It a peer to peer lookup system.
- Given a key, it maps key to one of the node
- Each node is assigned at key using consistent hashing function
- So, this consistent hashing function will assign each node a key which is m bit identifier
- A nodes's IP address and port number is used to calculate key or identifier
- So, in a m bit identifier space, there will be 2^m identifiers
- So, all these identifiers are ordered on identifier circle of modulo 2^m.
- This identifier ring is called chord ring
- So, the key k is assigned to first node on the in the ring whose identifier equal to or follows key
- Let's imagine m =5 , so we have identifier space starting from  0 to 31. We can imagine that as chord ring. Let's assume there are 3 nodes having identifier 1, 4 and 9. Now, if request to read a file whose identifier is 6 comes to node with identifier 1. 1 will see if it has key, if not it will go to 4 and so on. So, the system will visit all nodes on chord on linear order as there is no other rounting info other than successor of node. 
- To accelerate the lookup, chord node maintains a structure called finger table which is of size m
- The ith entry of in the fingertable at node p is successor of (p + 2 ^ i-1) key
- So, the first entry on fingertable is a successor of current node
- Since each node has finger entries at power of two intervals around the identifier circle, each node can forward a query at least halfway along the remaining distance between the node and the target identifier. 


## Implementation Details:

Project includes total 4 packages and few supporting files and folders. Here are the details of each.

Package: thriftRemoteCall.thriftClient
implemented test code

Package: thriftRemoteCall.thriftServer
implemented client code			

Package: thriftRemoteCall.thriftServiceHandler
implemented service handler code	

Package: thriftRemoteCall.thriftUtil
auto generated files by thrift

Folder: files
data storage for server side

Folder: src
contains all packages in it

**Files:**

stable_chord.thrift

server, bash script to run the server; also cleans and builds the project

test, bash script to run the client

build.xml, the file defines attributes for ANT

README.txt, the present text file

## Server:
chmod 755 server

./server 9090 1

./server port interval

## Test:
chmod 755 test
./test 9090 1 3

./test portNo interval totalNoOfNodes

The test compares program's fingertable output with cmp_fingertable file

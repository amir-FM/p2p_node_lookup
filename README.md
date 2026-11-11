# P2P Node Lookup Implementation

## Overview

This project implements a Peer-to-Peer (P2P) Node Lookup system utilizing the Message Passing Interface (MPI). It simulates a Distributed Hash Table (DHT) ring architecture, focusing on efficient message routing, chronological response synchronization, and deadlock-free graceful termination across distributed nodes.

## System Architecture & Specifications

### Message Handling and Optimization

* **Packet Processing:** Incoming network messages are routed through a streamlined `switch` statement architecture based on message tags, ensuring clean, scalable, and predictable parsing without deep conditional chains.
* **Passive Routing Mode:** To prevent system deadlocks—where nodes without local lookups might indefinitely block on an `MPI_Recv`—the system implements an early-standby optimization. If a node has no keys to look up, it immediately enters a "passive mode" where it acts purely as a routing relay to assist other nodes' in-flight requests.

### Synchronized Output Buffer

Due to the asynchronous nature of P2P routing, responses often arrive out of order (e.g., a lookup requiring one hop may return before a lookup initiated earlier that requires multiple hops). The system guarantees chronological output matching the initial request order using a dual-buffer mechanism:

1. **Response Buffer:** Temporarily stores early-arriving `TAG_LOOKUP_REP` packets.
2. **State Array:** A binary array (1/0) that tracks the receipt status of the *i-th* expected lookup.
3. **Resolution Protocol:** When a response is received, the system checks if it matches the current expected index *i*. If it does, the system prints the result and flushes any subsequently completed lookups from the buffer as long as there is an unbroken chain of `1`s in the state array. If it does not match, the response is buffered until its predecessors arrive.

### Graceful Termination Protocol

A node completing its own local lookups cannot safely shut down, as it may still be required to route messages for other nodes. To prevent dropped packets and broken routing paths, the system implements a strict global shutdown protocol:

* **Broadcast on Completion:** Once a node receives all its requested lookup responses, it broadcasts a `TAG_DONE` message to all nodes in the network (including itself) and transitions to passive routing mode.
* **Safe Shutdown Condition:** A node will only terminate its execution process after it has received exactly *N* `TAG_DONE` messages (where *N* is the total number of active nodes in the cluster). This guarantees that all nodes have finished their tasks and the network is entirely quiet.

### Edge Case Handling: Circular Ring Routing

The system explicitly handles identifier space wraparound (the circular nature of the DHT ring).

* **`closest_preceding_finger` Refinement:** In standard implementations, a node near the upper identifier limit (e.g., Node 16) looking for a numerically small key (e.g., Key 1) can fail to find a valid `biggest_finger` if the logic only strictly checks for values greater than the key. The lookup function is adjusted to account for modular arithmetic, ensuring accurate finger table routing across the ring boundary (the 0-index threshold).

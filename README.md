# Distributed Fault-Tolerant Key-Value Store

A distributed, sharded, and fault-tolerant key-value storage system implemented in C++ using gRPC and the Raft Consensus Algorithm.

## Project Description

This project provides a robust key-value store capable of handling concurrent requests, distributing data across multiple nodes using consistent hashing, and ensuring data consistency and availability through the Raft consensus protocol.

## Core Features

- Strong Consistency: Uses the Raft consensus algorithm to replicate logs across a majority of nodes.
- Fault Tolerance: Automatic leader election and recovery from node failures.
- Scalability: Sharding mechanism using consistent hashing with virtual nodes.
- High Performance: Internal thread-safety using Readers-Writer locks (std::shared_mutex).
- Communication: Defined service interface using Protocol Buffers and gRPC.

## Architecture

- KV Store: Core in-memory storage engine.
- Raft Node: Standalone state machine managing consensus logic.
- Server: gRPC server wrapping the KV store and Raft node.
- Coordinator: Proxy service that routes client requests to the appropriate shard.
- Client: CLI tool for interacting with the system.

## Requirements

- Linux or WSL2
- C++17 compiler (GCC or Clang)
- CMake 3.14 or higher
- gRPC and Protocol Buffers development libraries

### Dependencies Installation (Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y cmake build-essential libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev
```

## Build Instructions

1. Create a build directory:
   mkdir build && cd build

2. Configure the project:
   cmake ..

3. Compile:
   make

## Usage Guide

### Running a 3-Node Raft Cluster

Start three server instances in different terminal windows:

Node 1:
./kv_server localhost:50051 localhost:50052 localhost:50053

Node 2:
./kv_server localhost:50052 localhost:50051 localhost:50053

Node 3:
./kv_server localhost:50053 localhost:50051 localhost:50052

### Running the Coordinator

./kv_coordinator

### Interacting with the Client

./kv_client localhost:50051 put key1 value1
./kv_client localhost:50051 get key1

Note: If a node is not the leader, the client will report a leader hint for redirection.

## Testing

- Unit Tests: verify core storage logic.
  make run (from build directory)
- Concurrency Tests: stress test thread safety.
  ./concurrent_test
- Raft Verification: automated script for leader election and failover.
  bash verify_raft.sh

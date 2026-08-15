# Kafka-Inspired Message Broker - Development Plan

## Project Goal

Build a Kafka-inspired persistent event streaming/message broker in C++.

The system should demonstrate:

- TCP networking
- message-oriented protocols
- persistent append-only logs
- topics and partitions
- offsets
- producers and consumers
- consumer groups
- concurrency
- crash recovery
- optional replication
- benchmarking

The project is intentionally smaller than Apache Kafka.

---

# Phase 0 - Project Setup

Status: IN PROGRESS

- [ ] Initialize repository
- [ ] Configure CMake
- [ ] Create source structure
- [ ] Add basic build
- [ ] Add AGENTS.md
- [ ] Add initial README
- [ ] Verify C++ compiler

---

# Phase 1 - TCP Broker

Status: NOT STARTED

Goal:

Create a TCP server capable of accepting multiple client connections.

Requirements:

- Create listening socket
- bind()
- listen()
- accept()
- receive client data
- send response
- graceful connection handling

Success criteria:

A simple client can connect and exchange a request/response.

---

# Phase 2 - Application Protocol

Status: NOT STARTED

Define a simple request/response protocol.

Initial commands:

CREATE_TOPIC
PUBLISH
FETCH
LIST_TOPICS

Example:

CREATE_TOPIC sports

PUBLISH sports "India won"

FETCH sports 0

The protocol should be simple and deterministic.

---

# Phase 3 - Topics and Partitions

Status: NOT STARTED

Implement:

- topics
- partitions
- partition ownership
- partition metadata

Initial storage structure:

data/
└── topics/
    └── <topic>/
        ├── partition-0/
        ├── partition-1/
        └── ...

---

# Phase 4 - Persistent Append-Only Log

Status: NOT STARTED

Messages should survive process termination.

Implement:

- append-only log
- message serialization
- offsets
- reading by offset
- corruption/error handling

Investigate:

- record format
- length prefixes
- offsets
- fsync behavior

---

# Phase 5 - Producer and Consumer

Status: NOT STARTED

Implement basic clients.

Producer:

publish messages to topics.

Consumer:

fetch messages from a partition beginning at an offset.

---

# Phase 6 - Consumer Groups

Status: NOT STARTED

Implement:

- consumer registration
- consumer offsets
- partition assignment
- basic rebalancing

Study delivery semantics:

- at-most-once
- at-least-once

---

# Phase 7 - Concurrency

Status: NOT STARTED

Investigate:

- multiple producers
- multiple consumers
- thread safety
- locking
- contention
- thread pool vs thread-per-client

Add benchmarks.

---

# Phase 8 - Crash Recovery

Status: NOT STARTED

Kill the broker during operation.

Restart it.

Verify:

- persisted messages remain
- offsets remain valid
- metadata can be reconstructed
- corrupted/incomplete records are handled

---

# Phase 9 - Replication

Status: OPTIONAL

Only implement if the core broker is stable.

Potential design:

Leader
    |
    +---- Replica
    |
    +---- Replica

Investigate:

- replication log
- acknowledgements
- leader/follower
- replica failure
- recovery

---

# Phase 10 - Fault Tolerance

Status: OPTIONAL

Potential experiments:

- kill leader
- kill consumer
- restart broker
- network interruption
- replica unavailable

Document observed behavior.

---

# Phase 11 - Benchmarking

Status: NOT STARTED

Measure:

- throughput
- latency
- concurrent producers
- concurrent consumers
- disk throughput
- memory usage
- recovery time

Report:

- average latency
- p50
- p95
- p99
- messages/sec

---

# Phase 12 - Finalization

Status: NOT STARTED

- [ ] Clean README
- [ ] Architecture diagram
- [ ] Benchmark results
- [ ] Testing documentation
- [ ] Limitations
- [ ] Design decisions
- [ ] Final Git history
- [ ] Resume description
- [ ] B.Tech presentation material
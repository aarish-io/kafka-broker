# Kafka Broker Plan Instructions

Last updated: 2026-09-09

This file is the detailed companion to `plan.md`.

Use it for:
- stage intent
- scope boundaries
- implementation notes
- completion criteria
- deviations from the original roadmap
- new additions we decide to keep

The goal is to preserve both:
- the original learning roadmap
- what we actually built in this repository

## Working Rules

For each stage we should do this:
1. Understand the concept first.
2. Define the exact scope before implementation.
3. Identify important edge cases.
4. Implement only the approved scope.
5. Build and test after meaningful changes.
6. Mark the stage complete in `plan.md` when the stage goal is actually met.
7. Record any deviation or extra feature in the notes for that stage.

## Change Log Convention

When a stage changes, append short notes like:

```text
2026-08-23 - Stage 1
- Completed socket lifecycle (1.1), basic request/response (1.2), thread-per-client model (1.3), length-prefixed framing (1.4), and persistent connections (1.5).
- Simple thread-per-client model retained; non-blocking I/O and epoll deferred to later stages.
- Manually tested back-to-back requests on single connection and split frame scenarios.
- Status: COMPLETED.

2026-09-09 - Stage 4
- Completed Stage 4 (Partitions and Partition-Aware Reads/Writes).
- Implemented 3 default partitions per topic (0, 1, 2) in-memory and on disk.
- Updated log storage layout to data/<topic>/partition-<id>.log.
- Updated request syntax to partition-aware PRODUCE (<topic> <partition> <payload>) and FETCH (<topic> <partition> <offset>).
- Implemented offset-based FETCH reading based on zero-indexed message position within each partition.
- Fixed broker restart partition parsing off-by-one bug (substr(10) for "partition-").
- Comprehensive integration tests (test_stage4.py) and unit tests verified.
- Status: COMPLETED.

2026-09-09 - Stage 5
- Completed Stage 5 (Producer and Consumer Clients).
- Implemented BrokerClient abstraction in src/client.hpp with RAII socket management, 4-byte length-prefixed framing, MSG_NOSIGNAL, EINTR retry, move semantics.
- Implemented producer CLI in clients/producer.cpp for PRODUCE command with client-side validation.
- Implemented consumer CLI in clients/consumer.cpp for FETCH command with persistent TCP connection and sequential offset advancement.
- Consumer maintains single connection and issues multiple FETCH requests, advancing offset by counting newline-separated messages in each response.
- Both clients validate arguments (topic non-empty, partition 0-2, offset non-negative, message non-empty).
- Manual testing verified: producer→broker communication, consumer FETCH from different offsets, sequential offset progression, partition isolation, invalid argument handling, persistence across broker restart.
- Status: COMPLETED.
```

## Stage 0 - System Understanding

### Purpose

Understand the core ideas before building broker code:
- producer
- consumer
- broker
- topic
- partition
- message
- offset
- consumer group
- persistence
- replication

### What "done" means

We can explain:
- why a broker exists
- why Kafka uses append-only logs
- why offsets matter
- why partitions trade global ordering for scalability

### Repository status

Treat this stage as complete enough for implementation to begin.

### Notes

- Existing repo scaffolding already reflects early architectural planning.
- If we later discover conceptual gaps, we can add notes here without reopening the whole stage.

## Stage 1 - Linux TCP Foundation

### Purpose

Build the networking base for the broker.

### Scope

- create a listening TCP server
- use the socket lifecycle: `socket()`, `bind()`, `listen()`, `accept()`
- support clients with `connect()`, `send()`, `recv()`, `close()`
- handle multiple clients, initially with thread-per-connection
- add message framing
- support basic request/response behavior
- handle disconnects, partial reads, invalid input, and multiple requests per connection

### What not to do yet

- no persistence yet
- no partitions yet
- no consumer groups yet
- no replication yet
- no premature Kafka-compatible wire protocol

### Suggested deliverable

A small broker that can:
- accept multiple client connections
- receive framed messages
- return deterministic responses
- shut down cleanly enough for local testing

### Completion criteria

Stage 1 is done when we have:
- a working TCP server
- basic multi-client handling
- framed request parsing
- a simple protocol such as `PING -> PONG` or equivalent health request/response
- meaningful tests for framing and connection behavior

### Edge cases to keep in mind

- partial reads
- partial writes
- oversized payloads
- malformed frame lengths
- client disconnect during request
- multiple requests sent on one connection

### Stage notes

- 2026-08-22: Mini-stage 1.1 implemented (`socket()`, `setsockopt(SO_REUSEADDR)`, `bind()`, `listen()`, `accept()`, `close()`).
- 2026-08-23: Mini-stage 1.2 implemented (`PING` -> `PONG`, invalid request -> `ERROR`).
- 2026-08-23: Mini-stage 1.3 implemented (main thread accept loop with detached `std::thread` per client).
- 2026-08-23: Mini-stage 1.4 implemented (4-byte network-order `uint32_t` length prefix framing, `read_exactly()` / `write_exactly()` helper functions for TCP partial I/O, 64 KiB max payload validation, `MSG_NOSIGNAL` send flag).
- 2026-08-23: Persistent connection support implemented (client loop in worker thread handles multiple framed requests over a single TCP connection until disconnect/error).
- Architectural decision: Simple thread-per-client model intentionally chosen for Stage 1. Advanced concurrency models (`epoll`, non-blocking sockets, thread pools, connection registries) are explicitly deferred to later stages.
- Verification: Manually tested using Python client scripts and netcat, validating multiple requests per connection, split header/payload TCP reads, and broker isolation against client disconnects.
- Status: COMPLETED.

### Mini-Stage 2.1 - Define the Broker Protocol (2026-08-26)

**Purpose**: Establish a structured protocol layer above the existing TCP framing, separating network framing from protocol semantics.

**What was implemented**:
- Added `RequestType` enum class: `PING`, `PRODUCE`, `FETCH`, `INVALID`
- Added `Request` struct with `type`, `topic`, and `payload` fields
- Added `parse_request(const std::string&)` function that:
  - Parses `PING` (no topic, no payload)
  - Parses `FETCH <topic>` (topic required, no payload)
  - Parses `PRODUCE <topic> <payload>` (topic and non-empty payload required; payload preserves spaces)
  - Returns `INVALID` for malformed requests (missing topic, missing payload, unknown command)
- Updated `handle_client()` to use `parse_request()` and switch on `RequestType`
- Protocol remains text-based for easy debugging (as specified in Stage 2 roadmap)

**Validation rules implemented**:
- `PING` - valid (exactly one command)
- `FETCH orders` - valid (command + topic)
- `PRODUCE orders hello` - valid (command + topic + payload)
- `FETCH` - invalid (missing topic)
- `PRODUCE` - invalid (missing topic)
- `PRODUCE orders` - invalid (missing payload)
- `UNKNOWN orders hello` - invalid (unknown command)
- `PRODUCE orders hello world` - valid (payload = "hello world" - remainder preserved)

**Files changed**: `src/main.cpp` only (kept in single file per Stage 2 roadmap guidance)

**Testing**: Created `tests/test_protocol.py` - all 8 test cases pass:
- PING → PONG
- PRODUCE orders hello → OK
- PRODUCE orders hello world → OK (multi-word payload preserved)
- FETCH orders → OK
- FETCH → ERROR
- PRODUCE → ERROR
- PRODUCE orders → ERROR
- UNKNOWN cmd → ERROR

**Architectural significance**: Established the protocol boundary (NETWORK → FRAME → PARSE → BROKER OPERATION) that future mini-stages will build upon. The existing TCP framing (`read_frame`/`write_frame`) remains completely untouched.

## Stage 2 - First Broker Protocol

### Purpose

Move from generic TCP server to broker behavior.

### Scope

- define a broker message/request shape (`PING`, `PRODUCE <topic> <payload>`, `FETCH <topic>`)
- add in-memory topic storage and thread-safe operations
- support topic auto-creation on `PRODUCE`
- support edge-case handling for malformed requests and unknown topics

### Completion criteria

Stage 2 is COMPLETED.

### Stage notes

- 2026-08-28 - Stage 2 (Mini-Stages 2.1–2.8) COMPLETED.
  - **Protocol Parsing**: Structured protocol layer with `RequestType` (`PING`, `PRODUCE`, `FETCH`, `INVALID`) and `parse_request()`.
  - **In-Memory Topic Storage**: Thread-safe shared state using `std::unordered_map<std::string, std::vector<std::string>>` protected by `std::mutex`.
  - **PRODUCE Command**: Appends payload to topic list. Auto-creates topic on first `PRODUCE`. Preserves payload spaces.
  - **FETCH Command**: Returns newline-separated messages for existing topic. Returns `ERROR` for unknown topic without creating it.
  - **Edge-Case Handling**: Malformed requests (missing topic/payload, unknown command) map to `RequestType::INVALID` and respond with `ERROR`.
  - **End-to-End Verification**: Preserved framing layer (`read_frame`/`write_frame`) and multi-client threading model. Verified with integration tests.

## Stage 3 - Append-Only Log and Persistence (DONE)

### Purpose

Persist messages to disk so broker restarts do not lose accepted data.

### Scope

- Binary length-prefixed `Record` framing (`[4-byte uint32 big-endian length][payload]`)
- Per-topic append-only storage files stored under `data/<topic>.log`
- `TopicLog` class managing persistent disk append and full record readback (`read_all()`)
- Startup recovery discovering existing `.log` files in `data/` and populating in-memory `topics` state prior to accepting network traffic
- Direct Linux/POSIX file descriptor system calls (`open`, `write`, `fsync`, `close`) for durable disk flushes
- Comprehensive restart persistence verification test suite (`tests/test_restart_persistence.py`)

### Completion criteria

Stage 3 is done when:
- produced messages are stored on disk
- broker restart preserves readable messages
- writes are forced to physical storage via `fsync` before acknowledging success

### Notes section

- 2026-08-28 - Stage 3 Completed:
  - **Mini-Stage 3.1**: Implemented `kafka::Record` struct with 4-byte big-endian framing (`htonl`/`ntohl`) in `src/record.hpp` and unit tests in `tests/test_record.cpp`.
  - **Mini-Stage 3.2**: Created `kafka::TopicLog` storage manager (`src/topic_log.hpp`) handling binary appending to `data/<topic>.log`, directory auto-creation, and unit tests in `tests/test_topic_log.cpp`.
  - **Mini-Stage 3.3**: Integrated `TopicLog` with `PRODUCE` handling in `src/main.cpp`. Performed lock-free disk appends outside `topics_mutex`, updating in-memory state only upon disk success.
  - **Mini-Stage 3.4**: Added startup recovery `recover_topics_from_disk()`, parsing existing `.log` files and restoring the in-memory `topics` state prior to opening network listener sockets.
  - **Mini-Stage 3.5**: Created end-to-end restart persistence verification script (`tests/test_restart_persistence.py`), confirming cross-restart message integrity over TCP.
  - **Mini-Stage 3.6**: Replaced `std::ofstream` with POSIX `open()`, `write()`, `fsync()`, and `close()` in `TopicLog::append()`, ensuring durable physical flushes, partial write handling, `EINTR` signal retry loops, and RAII file descriptor management.
  - **Mini-Stage 3.7**: Added incomplete final record recovery in `TopicLog::read_all()`. Automatically detects unparsed trailing bytes at the end of a log file resulting from crash mid-write, recovers valid earlier records, and truncates the file back to the last valid record boundary using `std::filesystem::resize_file()`.
  - Status: COMPLETED.

## Stage 4 - Partitions and Partition-Aware Reads/Writes

### Purpose

Introduce topic partitions, explicit partition targeting for writes, offset-based reads, and partition-aware log storage.

### Status

`COMPLETED`

### Summary of Completed Work

- **4.1 Topic + Partition model**:
  - Each auto-created topic initializes with 3 default partitions (0, 1, 2).
  - `Topic` owns `std::vector<Partition>`, and each `Partition` stores its integer ID and in-memory message history.
- **4.2 Partition-aware persistent storage**:
  - Storage path updated to `data/<topic>/partition-<id>.log`.
  - `kafka::TopicLog` is instantiated per partition (`TopicLog(topic, partition_id, data_dir)`).
  - Preserved append-only payload serialization, length prefixing, CRC32 checksum validation, and incomplete trailing record truncation from Stage 3.
- **4.3 Partition-aware PRODUCE**:
  - Protocol updated to: `PRODUCE <topic> <partition> <payload>`.
  - Validates partition index (0, 1, 2). Messages are written to `data/<topic>/partition-<id>.log` and stored in memory.
  - Rejects invalid or missing partition IDs with `ERROR`.
- **4.4 Partition-aware FETCH**:
  - Protocol updated to: `FETCH <topic> <partition> <offset>`.
  - Reads exclusively from the specified partition.
- **4.5 Offsets**:
  - Offsets represent the zero-based message index within a specific partition's message history.
  - `FETCH <topic> <partition> <offset>` returns all messages starting from the given offset, separated by newlines.
  - Out-of-range offsets return an empty response `""`.
  - Negative or non-numeric offsets are rejected with `ERROR`.
  - Offsets are currently in-memory vector indices and are not yet saved as separate committed offset metadata (deferred to Stage 6 consumer groups).
- **4.6 Final integration testing and cleanup**:
  - Fixed startup recovery off-by-one bug when parsing `partition-<id>.log` filenames (`substr(10)` vs `substr(9)`).
  - Comprehensive integration test suite (`tests/test_stage4.py`) verifies all 9 Stage 4 test groups: Partition Model, Produce Routing, Partition Isolation, Offset Fetching, Multi-Partition Sequences, Restart Persistence, Invalid Partitions, Invalid Offsets, and Storage Layout & Recovery.

### Completion criteria

Stage 4 is complete:
- Topics auto-create with default 3 partitions (0, 1, 2).
- PRODUCE routes payloads to targeted partition log files and memory.
- FETCH reads partition-isolated messages starting from specified offset.
- System recovers multi-partition logs correctly across broker restarts.
- Comprehensive integration tests pass.

### Notes section

- **Partitioning Strategy**: Auto-created topics default to 3 partitions (IDs 0, 1, 2). Partition selection is specified explicitly by the producer client in the wire protocol request.
- **Ordering Guarantees**: Messages within a single partition are strictly ordered by sequential append offset. No global ordering guarantee exists across different partitions.
- **Storage Layout**: Disk persistence follows the `data/<topic>/partition-<id>.log` directory structure. Recovery scans topic subdirectories and parses partition IDs.

## Stage 5 - Producer and Consumer Clients

### Purpose

Make the system usable through dedicated client programs.

### Scope

- producer CLI
- consumer CLI
- configurable topic input
- configurable starting offset
- optional long-lived consumer connection

### Completion criteria

Stage 5 is done when:
- a producer binary can publish to the broker
- a consumer binary can fetch from the broker
- the flow is easy to demo locally

## Stage 6 - Consumer Groups

### Purpose

Allow multiple consumers to share partition work.

### Scope

- group membership
- assignment of partitions to consumers
- tracking committed or current offsets per group
- simple rebalance behavior

### Completion criteria

Stage 6 is done when:
- consumers in the same group do not all process the same partition copy
- assignment changes can be explained and demonstrated

## Stage 7 - Concurrency and Benchmarking

### Purpose

Turn the broker from a functional toy into a more robust concurrent system.

### Scope

- shared broker state protection
- concurrent producers and consumers
- race-condition testing
- throughput and latency measurements

### Completion criteria

Stage 7 is done when:
- shared state is safely synchronized
- we have measurable benchmark output
- we understand at least one real bottleneck

## Stage 8 - Linux Performance and epoll

### Purpose

Compare event-driven networking with thread-per-connection.

### Scope

- non-blocking sockets
- `fcntl()`
- `epoll_create()`, `epoll_ctl()`, `epoll_wait()`
- equivalent broker path for comparison

### Completion criteria

Stage 8 is done when:
- we have an `epoll`-based path or variant
- we can compare it against the threaded version with data

## Stage 9 - Failure Recovery

### Purpose

Handle broken states and recovery more intentionally.

### Scope

- restart recovery from logs
- incomplete record handling
- consumer offset recovery behavior
- delivery semantics discussion

### Completion criteria

Stage 9 is done when:
- restart recovery is tested
- duplicate delivery and failure cases are documented
- the chosen semantics are explicit

## Stage 10 - Replication

### Purpose

Add a simplified leader/follower replication model.

### Scope

- leader/follower roles
- log replication
- follower catch-up
- leadership failover experiments

### Completion criteria

Stage 10 is done when:
- one broker can replicate to another in a controlled setup
- replication lag or progress can be observed

## Stage 11 - Observability and Serious Benchmarking

### Purpose

Produce evidence, not guesses, about system behavior.

### Scope

- broker logs
- metrics
- throughput measurement
- latency measurement
- consumer lag measurement
- graphs or result tables

### Completion criteria

Stage 11 is done when:
- we can show benchmark numbers
- we can explain tradeoffs with measured results

## Stage 12 - Polish

### Purpose

Make the project easy to present, explain, and maintain.

### Scope

- README cleanup
- architecture notes
- test documentation
- CI
- static analysis
- sanitizer support
- final project summary and resume-ready wording

### Completion criteria

Stage 12 is done when:
- the repo is easy to build and understand
- the major design choices are documented
- the project can be presented confidently in interviews

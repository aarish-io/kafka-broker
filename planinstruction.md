# Kafka Broker Plan Instructions

Last updated: 2026-09-14

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

2026-09-12 - Stage 6
- Completed Stage 6 (Consumer Groups).
- Implemented Stage 6.1 through Stage 6.6 and tested the final integrated behavior successfully.
- Added the consumer group model and protocol commands: JOIN, LEAVE, GROUP_POLL, and COMMIT.
- Implemented broker-side group membership, duplicate consumer ID rejection within a group, and cleanup when a TCP connection disconnects.
- Implemented deterministic round-robin assignment across the existing 3 partitions per topic, with rebalancing after JOIN, LEAVE, and disconnect cleanup.
- Implemented in-memory group-owned committed offsets stored by group, topic, and partition.
- GROUP_POLL returns the current partition assignments plus the committed offset for each assigned partition.
- Updated the consumer client flow to JOIN -> GROUP_POLL -> FETCH from committed offset -> COMMIT progress -> repeat -> LEAVE.
- Status: COMPLETED.

2026-09-13 - Stage 8
- Completed Stage 8 (Linux non-blocking I/O and epoll) through mini-stages 8.1-8.7.
- Added an alternative event-driven server path: `EpollServer` with nonblocking sockets (`fcntl(O_NONBLOCK)`), `epoll_create1()`/`epoll_ctl()`/`epoll_wait()`, nonblocking accept/read/write, per-client `read_buffer`/`write_buffer`/`write_offset`, and incremental length-prefixed frame handling.
- `EpollServer` uses the existing `parse_request()` and `Broker::handle_request()`; real broker responses replace the temporary "FRAME_RECEIVED" response.
- Consumer-group JOIN/LEAVE membership tracking and disconnect cleanup mirror `TcpServer`.
- `TcpServer`, `main.cpp`, protocol implementation, `Broker` implementation, framing format, and storage format were not redesigned.
- Validated against the real Linux build in WSL Ubuntu: TcpServer regression (sequential PRODUCE, multiple sequential clients, 10 concurrent producers), EpollServer complete/split/multi-frame scenarios, frame plus partial next frame, multiple simultaneous clients, and persistence sanity (recovery from disk and PRODUCE persistence).
- Status: COMPLETED.

2026-09-14 - Stage 9
- Completed Stage 9 (Failure Recovery).
- Persistent records now use `[4-byte big-endian payload length][payload bytes][4-byte big-endian CRC32]`.
- CRC32 covers the serialized length bytes plus payload bytes.
- Invalid or incomplete trailing records are discarded by truncating the log to the last valid record boundary.
- Current consumer delivery behavior is at-least-once-style: FETCH does not commit progress, so messages may be delivered again before COMMIT.
- Consumer-group committed offsets are group/topic/partition-owned and survive consumer reassignment while the broker process remains alive.
- Consumer-group committed offsets are not persisted and are lost on broker restart; groups without recovered committed offsets begin from offset 0.
- No offset persistence, migration/versioning, exactly-once semantics, or Kafka-level guarantee system was introduced because it is outside Stage 9 scope.
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

Stage 6 is COMPLETED.

Completed mini-stages:
- `6.1` Consumer group model and protocol
- `6.2` Consumer group membership
- `6.3` Partition assignment and rebalancing
- `6.4` Consumer group offset tracking
- `6.5` Consumer/consumer-group client integration
- `6.6` Final integration and edge-case testing

Implemented behavior:
- Consumers join and leave groups through the existing request/response networking model.
- Duplicate `consumer_id` membership is rejected within the same group.
- Consumer group membership is cleaned up when the owning TCP connection disconnects.
- Empty groups are removed when appropriate.
- Group state is protected by synchronization because the broker still uses multiple client threads.
- Topic partitions are assigned deterministically with simple round-robin assignment.
- Rebalancing happens after successful JOIN, LEAVE, and disconnect cleanup.
- Committed offsets are stored in memory by group, topic, and partition, not by individual consumer.
- GROUP_POLL returns the current assignments and committed offsets for a valid group member.
- The consumer client now performs JOIN -> GROUP_POLL -> FETCH from committed offset -> COMMIT progress -> repeat -> LEAVE.

### Stage notes

- 2026-09-12 - Stage 6 completed through mini-stage 6.6.
- Final integration and edge-case testing verified the completed consumer-group flow.
- Offsets remain in-memory only.
- Heartbeats, session timeouts, persistent committed offsets, advanced assignment strategies, replication, and delivery semantics are intentionally deferred to later stages.
- Next implementation target after Stage 6 was Stage 8 (Linux non-blocking I/O and epoll). Stage 8 is now complete; see the Stage 8 section below.

## Stage 7 - Concurrency and Thread Safety

### Purpose

Audit and verify the safety of the existing concurrent broker without prematurely redesigning it.

### Scope

- shared broker state and synchronization boundaries
- concurrent producer, consumer, and consumer-group behavior
- race detection with ThreadSanitizer
- lock-scope and contention review
- final combined concurrency stress testing

### Completion criteria

Stage 7 is COMPLETED.

### Completed mini-stages

- **7.1 - Concurrency model audit**:
  - Reviewed the existing thread-per-client `TcpServer` and shared `Broker` model.
  - Confirmed that topic, partition, and message state is protected by `topics_mutex_`.
  - Confirmed that consumer-group state is protected by `consumer_groups_mutex_`.
  - Recorded the only current multiple-lock order: `consumer_groups_mutex_` -> `topics_mutex_` in COMMIT.
  - No production code changes were required.
- **7.2 - Concurrent producer safety**:
  - Added and ran `tests/test_concurrent_producers.py`.
  - Verified concurrent writes to the same partition and different partitions, concurrent topic creation, successful PRODUCE responses, expected counts and contents, and no message loss or duplication.
- **7.3 - Producer and consumer concurrency**:
  - Added and ran `tests/test_producer_consumer_concurrency.py`.
  - Verified concurrent PRODUCE and FETCH activity while GROUP_POLL and COMMIT operated, with consistent consumer-group progress and no missing or duplicate messages.
- **7.4 - Consumer-group concurrency**:
  - Added and ran `tests/test_consumer_group_concurrency.py`.
  - Verified concurrent JOIN, GROUP_POLL, FETCH, COMMIT, and LEAVE activity.
  - Verified that exactly one of two concurrent duplicate JOIN requests succeeds and the other fails, while group state remains consistent.
- **7.5 - Race detection**:
  - Built the `kafka-broker` target separately with `-fsanitize=thread`, `-fno-omit-frame-pointer`, and `-g`.
  - Ran the concurrency workloads against the ThreadSanitizer-instrumented broker; no ThreadSanitizer data-race warnings were observed.
  - The normal build was not replaced or modified.
  - A pre-existing, unrelated `tests/test_record.cpp` compilation issue prevented the entire CMake test target from building under this configuration. Only the `kafka-broker` target is claimed as successfully built under TSan.
- **7.6 - Lock scope and contention review**:
  - Confirmed that current synchronization is correct but coarse-grained.
  - `PRODUCE` holds `topics_mutex_` during in-memory mutation and synchronous `TopicLog` persistence, including disk I/O and `fsync`.
  - `FETCH` holds `topics_mutex_` while constructing its response.
  - Different partitions therefore contend on the same global topic mutex.
  - This behavior was intentionally retained because changing it safely requires decisions about ordering, persistence failure, and recovery semantics.
- **7.7 - Final concurrency stress test**:
  - Added and ran `tests/test_stage7_final_stress.py`.
  - Verified a combined workload with 6 producers, 3 consumers, 3 partitions, and 300 total messages.
  - Verified concurrent producer, consumer, and group activity, correct per-partition counts, consistent state, and no message loss or duplication.

### Scope decisions

- Stage 7 did not introduce `epoll`, non-blocking I/O, thread pools, per-partition mutexes, lock-free structures, or another concurrency redesign.
- The thread-per-client model and the two existing Broker mutexes remain the production architecture.
- Coarse-grained contention is a known architectural tradeoff, not evidence that the current implementation is unsafe.
- Serious throughput, latency, observability, and benchmark reporting were moved to Stage 11.
- Next planned stage after Stage 7 was Stage 8 (Linux non-blocking I/O and epoll). It is now COMPLETED; see the Stage 8 section below.

## Stage 8 - Linux Performance and epoll

### Purpose

Compare event-driven networking with thread-per-connection.

### Scope

- non-blocking sockets
- `fcntl()`
- `epoll_create()`, `epoll_ctl()`, `epoll_wait()`
- equivalent broker path for comparison

### Completion criteria

Stage 8 is COMPLETED.

Completed mini-stages:
- **8.1 - EpollServer design boundary**: Added a concrete `EpollServer` class as an alternative event-driven server path without premature abstraction, interfaces, or factories. Kept the existing threaded `TcpServer` unchanged as the known-good baseline.
- **8.2 - Non-blocking sockets and epoll foundation**: Listening socket made nonblocking with `fcntl(F_GETFL/F_SETFL | O_NONBLOCK)`; `epoll_create1()`; listening FD registered via `epoll_ctl(... EPOLL_CTL_ADD ... EPOLLIN)`; `epoll_wait()` event loop.
- **8.3 - Accept and manage multiple connections**: Nonblocking `accept()` loop with `EINTR` and `EAGAIN`/`EWOULDBLOCK` handling. Accepted client sockets are nonblocking and registered for `EPOLLIN`. Per-client `ClientState` mapping with `EPOLLERR`/`EPOLLHUP` handling and client cleanup.
- **8.4 - Nonblocking frame reading**: `EpollServer` does not use the blocking `read_frame()`/`read_exactly()` helpers. Per-client `read_buffer` accumulates arbitrary TCP chunks; complete length-prefixed frames are extracted incrementally. Handles partial headers, partial payloads, multiple complete frames, and a complete frame followed by a partial next frame. The existing 4-byte network-order framing and maximum payload limit are unchanged.
- **8.5 - Nonblocking response writing**: Per-client `write_buffer` and `write_offset`; responses are length-framed and queued instead of written through blocking helpers. `EPOLLOUT` is enabled when response data is pending and disabled after the buffer drains. `send()` may partially write; `EAGAIN`/`EWOULDBLOCK` leaves the remaining data queued for the next `EPOLLOUT`. `MSG_NOSIGNAL` is used for safe socket sends.
- **8.6 - Complete Epoll broker path**: Complete frames go through the existing `parse_request()` parser and `Broker::handle_request()`, replacing the temporary "FRAME_RECEIVED" response. No duplicated protocol parsing or broker business logic. Responses use the same 4-byte network-order framing as `TcpServer`. JOIN/LEAVE connection membership tracking and disconnect cleanup mirror `TcpServer`.
- **8.7 - Hardening and threaded-vs-epoll validation**: Ran the validation below against the real Linux build in WSL Ubuntu.

### Validation results (recorded in mini-stage 8.7)

These tests validate correctness of the current event-driven networking path, not serious performance benchmarking. All were run against the real Linux build.

1. Existing threaded TcpServer regression:
   - `kafka-broker` target built successfully.
   - Sequential PRODUCE requests returned OK.
   - Multiple sequential clients returned OK.
   - 10 concurrently launched producer processes all returned OK.
   - Existing TcpServer remained functional.
2. EpollServer basic complete frame (temporary standalone EpollServer driver on port 9093):
   - PING -> PONG succeeded.
3. EpollServer split frame:
   - The first part of a frame was sent; the server did not process it until the remaining bytes arrived.
   - Final PING -> PONG succeeded.
4. Multiple complete frames in one TCP send:
   - Three complete PING frames sent in one send; three PONG responses returned in order.
5. Multiple complete frames plus a partial next frame:
   - Two complete PING frames plus a partial third frame sent together.
   - The first two were processed; the third was not processed until its remaining bytes arrived.
   - All three PONG responses were eventually received.
6. Multiple simultaneous clients:
   - Client A sent a complete PING.
   - Client B sent a partial PING and later completed it.
   - Client C sent three complete PING frames.
   - Client D sent a real PRODUCE request.
   - All clients received correct responses; client-specific state stayed independent.
   - Event ordering was naturally nondeterministic and is not treated as a failure.
7. Persistence sanity:
   - EpollServer recovered existing topics/messages from disk.
   - The PRODUCE request from the multi-client test persisted successfully.

### Scope decisions

- Stage 8 added an alternative event-driven networking path (`EpollServer`) while preserving the threaded `TcpServer` as the comparison baseline.
- Keep the level-triggered epoll model (`EPOLLET` was not introduced). The event loop can wake frequently for non-blocking readiness notifications; this is a level-triggered design property and is expected behavior.
- Early epoll work added a `get_ready_payload()` breakpoint so the loop processes at most one connection per wake to preserve observer parity with the threaded version (byte-stream readiness, not application-message semantics).
- Protocol parsing, `Broker` logic, the wire framing format, the storage format, `main.cpp`, and `TcpServer` were not redesigned; `EpollServer` reuses the existing protocol and `Broker` path.
- No claim that epoll is universally faster, no measured throughput/latency improvement claim, and no production-scale scalability claim are made.
- Serious quantitative benchmarking (throughput, latency, concurrency comparisons of the two paths) is deferred to Stage 11.
- Next planned stage: Stage 9 (Failure Recovery).

## Stage 9 - Failure Recovery

### Purpose

Handle broken states and recovery more intentionally.

### Scope

- restart recovery from logs
- incomplete record handling
- consumer offset recovery behavior
- delivery semantics discussion

### Completion criteria

Stage 9 is COMPLETED.

Stage 9 is done when:
- restart recovery is tested
- duplicate delivery and failure cases are documented
- the chosen semantics are explicit

### Stage notes

- 2026-09-14 - Stage 9 completed and manually verified.
- Existing topic/partition logs survive broker restart and records are recovered from disk.
- Persistent records include CRC32 after the payload; CRC covers `[length bytes][payload bytes]`.
- `TopicLog::read_all()` recovery accepts valid records until `deserialize_record()` fails, then truncates the log to the last valid boundary.
- Incomplete final records and CRC-corrupt records are treated as invalid trailing records; recovery does not attempt reconstruction or scanning past the first invalid record.
- Delivery behavior is at-least-once-style: FETCH alone does not advance committed group progress, and duplicate delivery is possible if a consumer receives messages but fails or disconnects before COMMIT.
- COMMIT advances the committed offset for the consumer group and topic/partition, not for an individual connection.
- Consumer-group offsets remain available across reassignment while the broker process stays alive.
- Consumer-group offsets are in-memory only and are lost on broker restart. After restart, a group with no recovered committed offset begins at offset 0.
- Persistent offset storage and migration/versioning were not implemented in Stage 9.

2026-09-14 - Stage 10
- Completed Stage 10 (simplified replication, follower catch-up, and explicit failover) through mini-stages 10.3.1-10.5.4, followed by final validation in 10.6.
- Added leader/follower roles with independent data directories. `TopicLog` remains the persistence layer, including its existing durable append and `fsync` behavior.
- Added internal `REPLICATE <topic> <partition> <offset> <payload>` handling. It uses the existing partition/message index model, where `messages[index]` is the logical offset, and follower progress is tracked separately for each `TopicPartition`.
- Leader `PRODUCE` persists locally first, sends `REPLICATE` through the existing framed TCP request/response mechanism, waits for follower `OK`, and only then returns `OK` when a follower is configured.
- Added `REPLICATION_PROGRESS` queries so a leader can obtain the follower's recovered progress. `get_missing_records()` identifies records after that progress, and `catch_up_follower()` replays them in logical offset order, waiting for an acknowledgement after each record.
- Follower progress is reconstructed from recovered persisted records; no metadata file or second persistence subsystem was introduced.
- Added explicit/manual `Broker::promote_to_leader()`. Promotion retains recovered topics, partitions, data, and progress, clears the old follower configuration, and enables the existing local `PRODUCE` path without contacting the failed old leader.
- `TcpServer` remains the runtime server and `EpollServer` remains an alternative networking path sharing the same Broker logic; neither was redesigned or removed.
- Final validation passed: native CMake build, record, TopicLog, protocol, broker replication, two-broker replication, synchronous ACK, follower lag, restart catch-up, already-caught-up behavior, progress recovery, manual promotion, post-promotion PRODUCE and persistence, data-directory isolation, and `git diff --check`.
- Validation used temporary isolated broker directories and temporary promotion harnesses; those artifacts were removed afterward.
- Intentionally excluded: automatic failure detection, automatic leader election, heartbeats, retries, quorum/ISR, Raft/KRaft, a controller, client redirection, consumer-offset replication, and a background synchronization manager. Catch-up is triggered by the next leader `PRODUCE`.
- Status: COMPLETED.

## Stage 10 - Replication

### Purpose

Add a simplified leader/follower replication model.

### Scope

- leader/follower roles
- log replication
- follower catch-up
- leadership failover experiments

### Implemented design

- A leader and follower use independent data directories and the existing `TopicLog` append-only persistence layer.
- `REPLICATE` is an internal broker-to-broker operation. Normal leader `PRODUCE` waits for the follower's `OK` response after durable persistence before returning `OK`.
- Offsets remain zero-based partition/message indexes. Replication progress is tracked independently by `TopicPartition` and recovered from the follower's persisted records.
- The leader queries follower progress, calls `get_missing_records()`, and uses `catch_up_follower()` to replay missing records in order with one synchronous acknowledgement per record.
- Explicit promotion changes a follower to a standalone leader without rewriting its logs. The promoted broker uses the existing local `PRODUCE` path.

### Completion criteria

Stage 10 is COMPLETED. Final validation confirmed controlled two-broker replication, synchronous acknowledgement behavior, follower lag, restart recovery and catch-up, already-caught-up behavior, replication progress recovery, manual promotion, post-promotion persistence, failure behavior, and independent data directories.

### Intentional boundaries

Automatic failure detection, automatic leader election, heartbeats, retry policies, quorum/ISR, Raft/KRaft, controller behavior, client redirection, consumer-offset replication, and background synchronization are outside this simplified educational stage. There is no runtime TCP promotion command; promotion is explicit/manual, and catch-up begins on the next leader `PRODUCE`.

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

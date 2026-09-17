# Stage History

This document records the completed evolution of the Kafka-inspired C++ broker from Stage 0 through Stage 11. It is grounded in the current repository state, `plan.md`, `planinstruction.md`, `current_architecture.md`, tests, benchmark artifacts, and recent git history.

Known repository discrepancy: `plan.md` still labels Stage 11 as `IN PROGRESS`, but the repository contains Stage 11 implementation, documentation, benchmark scripts, raw benchmark results, and analysis artifacts. This document treats Stage 11 as completed for the purpose of Stage 12.1 documentation.

## Stage 0: Kafka And Broker Concepts

### Purpose

Stage 0 established the conceptual target before implementation: producers send records to a broker, consumers fetch records, topics organize records, partitions isolate ordering and scale, offsets identify records, consumer groups share partition work, persistence survives restart, and replication copies records to another broker.

### Before This Stage

There was no broker implementation. The project needed a shared mental model before choosing C++ classes, protocol shape, storage layout, or tests.

### Concepts Introduced

- Broker as the server-side owner of messages and coordination state.
- Topic as a named stream.
- Partition as the ordered append-only unit within a topic.
- Offset as the zero-based position in one partition.
- Consumer group as a set of consumers sharing partition ownership.
- Append-only log as the persistence primitive.
- Replication as leader-to-follower copying.

### What Was Actually Implemented

Stage 0 is documentation and planning rather than code. The implementation that followed reflects these concepts in `Broker`, `Topic`, `Partition`, `TopicLog`, `Record`, `TopicPartition`, `ConsumerGroup`, and `GroupMember`.

### How It Works

Stage 0 provided the vocabulary used by later stages. The current code maps that vocabulary directly:

- `Broker` owns topics and consumer groups.
- `Topic` owns partitions.
- `Partition` owns in-memory payload strings.
- `TopicLog` owns append-only file persistence for one topic partition.
- `TopicPartition` is the key type for group offsets and replication progress.

### Important Design Decisions

The roadmap chose an educational Kafka-inspired broker rather than Kafka protocol compatibility. That decision allowed a simple text command protocol over custom length-prefixed TCP frames.

### Failure/Edge Cases

No runtime failure behavior existed yet.

### Testing/Validation

No direct tests belong to Stage 0.

### What This Enabled

Stage 0 made it possible to build the broker in layers: TCP first, then protocol, storage, partitions, clients, groups, concurrency, epoll, recovery, replication, and observability.

### Limitations

Stage 0 did not implement code or lock in production-grade Kafka semantics.

### Future/Interview Explanation

"I started by modeling the core Kafka concepts: producers and consumers talk to a broker, topics are split into ordered partitions, offsets track position, consumer groups share partitions, append-only logs provide persistence, and replication copies partition logs from a leader to a follower. The project is Kafka-inspired, not Kafka-compatible."

## Stage 1: Linux TCP Foundation

### Purpose

Stage 1 created the network foundation needed for any broker behavior: listening sockets, client connections, request/response handling, length-prefixed framing, and persistent connections.

### Before This Stage

The project had concepts but no server that clients could connect to.

### Concepts Introduced

- POSIX socket lifecycle: `socket()`, `setsockopt(SO_REUSEADDR)`, `bind()`, `listen()`, `accept()`, `recv()`, `send()`, `close()`.
- TCP as a byte stream, not a message stream.
- Application framing with a fixed-size length header.
- Thread-per-client concurrency.

### What Was Actually Implemented

The current `TcpServer` in `src/tcp_server.cpp` preserves the Stage 1 model:

- Creates an IPv4 TCP listening socket.
- Binds to the configured port.
- Listens with a configurable backlog.
- Accepts clients in a loop.
- Starts one detached `std::thread` per accepted client.
- Reads length-prefixed frames with `read_frame()`.
- Writes length-prefixed responses with `write_frame()`.

The shared framing layer in `src/framing.hpp` implements:

- `[4-byte network-order uint32 length][payload bytes]`.
- Maximum payload size of 64 KiB.
- `read_exactly()` and `write_exactly()` loops for partial I/O.
- `EINTR` retry.
- `MSG_NOSIGNAL` on sends.

### How It Works

1. `TcpServer::start()` creates, binds, and listens on the socket.
2. The accept loop waits for client connections.
3. Each accepted connection is handed to `TcpServer::handle_client()` in a detached thread.
4. The client thread repeatedly reads one framed request.
5. The request is parsed and passed into `Broker::handle_request()`.
6. The response is written back as another frame.
7. Disconnect or error closes the client socket and cleans up group memberships.

### Important Design Decisions

The thread-per-client model was intentionally simple and retained as the known-good baseline even after `EpollServer` was added. This makes correctness easier to reason about, but it does not scale as well as an event loop under many connections.

### Failure/Edge Cases

- Partial TCP reads and writes are handled by helper loops.
- Oversized frames are rejected.
- Client disconnect while reading the header returns cleanly.
- Client disconnect while reading payload or writing response is treated as an error.
- `TcpServer` logs errors and closes the client connection.

### Testing/Validation

`planinstruction.md` records manual validation of multiple requests on one connection, split header/payload reads, and disconnect isolation. Later integration tests continue to exercise the same framing path.

### What This Enabled

Stage 1 gave later stages a stable transport: protocol parsing, broker operations, clients, consumer groups, replication, and metrics all reuse framed TCP.

### Limitations

No topics, persistence, partitions, consumer groups, replication, or benchmark claims existed yet.

### Future/Interview Explanation

"I began with a real Linux TCP server and a simple length-prefixed framing layer. TCP only gives bytes, so every request is sent as a 4-byte big-endian length followed by payload bytes. The initial concurrency model is one thread per client, with broker synchronization handled below the server layer."

## Stage 2: Broker Protocol And In-Memory Topics

### Purpose

Stage 2 turned the generic TCP server into a message broker by adding a text protocol, request parsing, in-memory topic storage, and simple `PRODUCE`/`FETCH` behavior.

### Before This Stage

The server could accept framed requests, but it had no broker semantics.

### Concepts Introduced

- Protocol parsing as a separate layer from network framing.
- Topic auto-creation.
- In-memory message lists.
- Malformed request handling.

### What Was Actually Implemented

The current parser lives in `src/protocol.hpp` and `src/protocol.cpp`. The protocol has grown since Stage 2, but the original ideas remain:

```text
PING
PRODUCE <topic> <partition> <payload>
FETCH <topic> <partition> <offset>
JOIN <group> <consumer_id> <topic>
LEAVE <group> <consumer_id>
GROUP_POLL <group> <consumer_id>
COMMIT <group> <consumer_id> <topic> <partition> <offset>
REPLICATE <topic> <partition> <offset> <payload>
REPLICATION_PROGRESS <topic> <partition>
METRICS
```

The initial Stage 2 shape was `PING`, `PRODUCE`, and `FETCH`; later stages added partitions, groups, replication, and metrics.

### How It Works

1. The server receives a frame payload as text.
2. `parse_request()` tokenizes and validates it.
3. It returns a `Request` with `RequestType`, topic, partition, offset, group fields, and payload.
4. Invalid syntax returns `RequestType::INVALID`.
5. `Broker::handle_request()` maps invalid requests to `ERROR`.

### Important Design Decisions

The protocol is intentionally text-based for readability and testing. Framing stays binary and protocol semantics stay textual.

### Failure/Edge Cases

- Unknown commands become invalid requests.
- Missing required fields become invalid requests.
- Empty produce payloads are rejected.
- Extra tokens are rejected for commands that should not have them.

### Testing/Validation

`tests/test_protocol.py` and `tests/test_protocol_parse.cpp` cover parsing and request/response behavior.

### What This Enabled

Stage 2 created the stable broker boundary used by every later feature. Later stages extended command syntax rather than replacing the parser.

### Limitations

At the original Stage 2 boundary, storage was in memory and did not survive restart. Partitions, offsets, clients, groups, replication, and metrics came later.

### Future/Interview Explanation

"Stage 2 introduced a clean protocol layer above TCP framing. The server receives complete frames, parses them into typed requests, and the broker executes them. I kept the protocol text-based because this project is educational and testability mattered more than binary protocol compatibility."

## Stage 3: Persistent Append-Only Logs

### Purpose

Stage 3 made accepted records survive broker restart by appending them to disk.

### Before This Stage

Messages lived only in memory. Restarting the broker lost produced data.

### Concepts Introduced

- Append-only log persistence.
- Binary record serialization.
- Startup recovery from log files.
- Durable writes with `fsync`.
- Incomplete final-record handling.

### What Was Actually Implemented

The current storage implementation is in `src/record.hpp` and `src/topic_log.hpp`.

Current persistent record format:

```text
[4-byte big-endian payload length][payload bytes][4-byte big-endian CRC32]
```

The CRC was added in Stage 9, so the current format is stronger than the original Stage 3 length-plus-payload format. Documentation should describe the current format when explaining current storage.

`TopicLog::append()` uses POSIX APIs:

- `open(..., O_WRONLY | O_CREAT | O_APPEND, 0644)`
- repeated `write()` until all bytes are written
- `fsync()`
- `close()`

### How It Works

1. `Broker::handle_request(PRODUCE)` creates the topic if necessary.
2. The record is appended to the in-memory partition.
3. `TopicLog` serializes the record.
4. The serialized bytes are appended to the log file.
5. `fsync()` flushes the file descriptor before success is reported.
6. On restart, `Broker::recover_from_disk()` scans log files and rebuilds in-memory messages.

### Important Design Decisions

The project uses a simple append-only log and full startup recovery rather than a database or index. This keeps the implementation understandable and mirrors the core idea behind Kafka partition logs.

### Failure/Edge Cases

- `write()` handles partial writes and `EINTR`.
- `fsync()` handles `EINTR`.
- Append throws on open, write, fsync, or close failure.
- Recovery stops at the first invalid record and truncates trailing invalid bytes.

### Testing/Validation

- `tests/test_record.cpp`
- `tests/test_topic_log.cpp`
- `tests/test_restart_persistence.py`

### What This Enabled

Persistent logs made partitions, recovery, replication, and durability discussions meaningful.

### Limitations

There is no segment rolling, compaction, indexing, retention policy, async flush, or configurable durability mode.

### Future/Interview Explanation

"Stage 3 added an append-only disk log. On produce, the broker serializes a record, appends it with POSIX file APIs, and fsyncs before acknowledging. On startup, the broker scans log files and rebuilds in-memory state."

## Stage 4: Partitions And Offset-Based Reads

### Purpose

Stage 4 introduced partitioned topics and explicit offset-based reads.

### Before This Stage

Topics were unpartitioned message lists. Fetching did not target a specific partition or offset in the current form.

### Concepts Introduced

- Topic as a collection of partitions.
- Per-partition ordering.
- Offset as zero-based index within one partition.
- Partition-isolated storage files.

### What Was Actually Implemented

Current code:

- `Topic` owns `std::vector<Partition>`.
- `Partition` owns `id` and `std::vector<std::string> messages`.
- `Broker::kDefaultPartitionCount` is `3`.
- Valid public partitions are `0`, `1`, and `2`.
- Public `PRODUCE` syntax is `PRODUCE <topic> <partition> <payload>`.
- Public `FETCH` syntax is `FETCH <topic> <partition> <offset>`.
- Storage layout is `data/<topic>/partition-<id>.log`.

### How It Works

For `PRODUCE`, the broker finds or creates the topic, validates the partition index, appends the payload to that partition, and persists through `TopicLog`.

For `FETCH`, the broker finds the topic and partition, then returns newline-separated messages beginning at the requested offset. If the offset is at or beyond the end, it returns an empty response. If the topic or partition is invalid, it returns `ERROR`.

### Important Design Decisions

Partition choice is explicit in the protocol. There is no key hashing or broker-side routing policy.

### Failure/Edge Cases

- Non-numeric, negative, or out-of-range public partitions are rejected by the parser.
- Negative or malformed offsets are rejected.
- Unknown topics return `ERROR` on fetch.
- Offset past the end returns an empty successful response.

### Testing/Validation

`tests/test_stage4.py` covers partition model, routing, isolation, offsets, restart persistence, invalid partitions, invalid offsets, and storage layout.

### What This Enabled

Partitions became the foundation for consumer-group assignment, per-partition committed offsets, replication progress, and concurrency tests.

### Limitations

No dynamic partition counts, key-based routing, cross-partition ordering, partition reassignment, or retention.

### Future/Interview Explanation

"Stage 4 changed topics into three fixed partitions. Offsets are local to one partition, so ordering is guaranteed only inside that partition. Produce and fetch both carry a partition id, and storage maps directly to `data/<topic>/partition-<id>.log`."

## Stage 5: Producer And Consumer Clients

### Purpose

Stage 5 made the broker usable through dedicated command-line clients and a reusable client networking wrapper.

### Before This Stage

Testing required ad hoc clients or scripts that manually spoke the framed protocol.

### Concepts Introduced

- Client-side RAII socket ownership.
- Reusable framed request/response client.
- Producer CLI.
- Consumer CLI.

### What Was Actually Implemented

`src/client.hpp` defines `BrokerClient`:

- Connects to a host and port.
- Owns a socket file descriptor.
- Is non-copyable and movable.
- Sends one framed request and waits for one framed response.
- Uses the shared framing helpers.

`clients/producer.cpp`:

```text
./producer <topic> <partition> <message>
```

It validates topic, partition `0..2`, and non-empty message, then sends a `PRODUCE` request to `127.0.0.1:9092`.

`clients/consumer.cpp` currently uses the Stage 6 consumer-group flow rather than a raw fetch-only loop:

```text
./consumer <topic> <group_id> <consumer_id>
```

### How It Works

Producer:

1. Parse CLI arguments.
2. Build `PRODUCE <topic> <partition> <message>`.
3. Connect with `BrokerClient`.
4. Send the framed request.
5. Print the broker response.

Consumer:

1. Join a group.
2. Poll assignments.
3. Fetch assigned partitions from committed offsets.
4. Print records.
5. Commit advanced offsets.
6. Leave before exit when possible.

### Important Design Decisions

The client wrapper reuses the same framing code as the server, preventing separate client/server frame formats.

### Failure/Edge Cases

Clients validate arguments before connecting. Network exceptions are printed and return non-zero.

### Testing/Validation

Manual validation is recorded in `planinstruction.md`. Later Python integration tests exercise equivalent client behavior over TCP.

### What This Enabled

`BrokerClient` later became useful for leader-to-follower replication, not just external clients.

### Limitations

The CLIs are fixed to `127.0.0.1:9092`. They do not implement retries, reconnect, batching, async I/O, custom broker address arguments, or Kafka protocol compatibility.

### Future/Interview Explanation

"Stage 5 added a small RAII client abstraction and CLI producer/consumer programs. The same length-prefixed framing is used on both sides. The client code stays out of storage and broker internals."

## Stage 6: Consumer Groups

### Purpose

Stage 6 allowed multiple consumers in the same group to divide partition work and track group-owned offsets.

### Before This Stage

Consumers could fetch records, but there was no group membership, assignment, rebalancing, or committed group progress.

### Concepts Introduced

- Consumer group membership.
- Group-owned committed offsets.
- Deterministic partition assignment.
- Rebalance after membership changes.
- Disconnect cleanup.

### What Was Actually Implemented

Protocol commands:

```text
JOIN <group> <consumer_id> <topic>
LEAVE <group> <consumer_id>
GROUP_POLL <group> <consumer_id>
COMMIT <group> <consumer_id> <topic> <partition> <offset>
```

Broker structures:

- `ConsumerGroup`
- `GroupMember`
- `TopicPartition`
- `consumer_groups_`
- `consumer_groups_mutex_`

Connection membership tracking exists in both `TcpServer` and `EpollServer`.

### How It Works

1. `JOIN` creates the group if needed.
2. Duplicate `consumer_id` within the same group is rejected.
3. The member records its subscribed topic.
4. `rebalance_group()` clears assignments and recalculates them.
5. Consumers call `GROUP_POLL` to receive lines of `<topic> <partition> <committed_offset>`.
6. Consumers fetch assigned partitions from those offsets.
7. Consumers call `COMMIT` with the next offset to read.
8. `LEAVE` removes the member and rebalances or deletes an empty group.
9. If a TCP connection closes, the server removes all memberships owned by that connection.

Assignment algorithm:

- Group members are grouped by subscribed topic.
- Consumer IDs are sorted for deterministic behavior.
- Partitions `0`, `1`, and `2` are assigned round-robin by partition number.
- A partition is assigned to at most one member for a topic within the group.
- More consumers than partitions means some consumers receive no assignments.

### Important Design Decisions

Committed offsets belong to the group and topic partition, not to the individual consumer connection. This supports reassignment within the same process lifetime.

### Failure/Edge Cases

- Unknown group/member on `GROUP_POLL` returns `ERROR`.
- Unknown group/member/topic/partition on `COMMIT` returns `ERROR`.
- Empty groups are removed.
- Disconnect cleanup mirrors explicit `LEAVE`.

### Testing/Validation

Tests include:

- `tests/test_consumer_group_concurrency.py`
- `tests/test_producer_consumer_concurrency.py`
- `tests/test_stage7_final_stress.py`

`plan.md` records final Stage 6 edge-case testing.

### What This Enabled

Consumer groups made delivery-semantics documentation meaningful and provided state for concurrency testing.

### Limitations

No heartbeats, session timeouts, persistent committed offsets, cooperative rebalancing, advanced assignment strategies, or offset replication.

### Future/Interview Explanation

"Stage 6 added group membership and deterministic round-robin partition assignment. Consumers poll their assignments, fetch from the group's committed offset, and commit the next offset. Offsets are process-memory state, so topic data survives restart but group progress does not."

## Stage 7: Concurrency And Benchmarking Baseline

### Purpose

Stage 7 audited and validated correctness under concurrent access without redesigning the architecture.

### Before This Stage

The broker used thread-per-client concurrency, but shared-state safety and edge cases under concurrent producer/consumer/group workloads needed validation.

### Concepts Introduced

- Shared-state synchronization audit.
- Concurrent integration workloads.
- ThreadSanitizer validation of the broker target.
- Lock-scope and contention review.

### What Was Actually Implemented

No major production redesign was introduced. The existing synchronization model was validated:

- `topics_mutex_` protects topics, partitions, messages, and replication progress where accessed.
- `consumer_groups_mutex_` protects consumer groups and committed offsets.
- `COMMIT` acquires both locks in the order `consumer_groups_mutex_` then `topics_mutex_`.

Tests added include:

- `tests/test_concurrent_producers.py`
- `tests/test_producer_consumer_concurrency.py`
- `tests/test_consumer_group_concurrency.py`
- `tests/test_stage7_final_stress.py`

### How It Works

Each TCP client runs in its own thread. Those threads share one `Broker`. Broker methods use mutexes around shared maps and vectors.

`PRODUCE` currently holds `topics_mutex_` while it mutates memory and appends/fsyncs the record. `FETCH` holds `topics_mutex_` while building the response. `COMMIT` validates topic/partition while holding group state.

### Important Design Decisions

Correctness was preferred over fine-grained parallelism. The global topic mutex is intentionally coarse-grained.

### Failure/Edge Cases

Concurrent tests covered same-partition writes, different-partition writes, topic creation, fetch while produce, group polling, committing, leaving, and duplicate joins.

### Testing/Validation

`plan.md` records a final stress workload with 6 producers, 3 consumers, 3 partitions, and 300 messages. A ThreadSanitizer-instrumented `kafka-broker` target ran concurrency workloads without reported races. The repository notes that the entire CMake test target was not built under TSan due to a pre-existing `test_record.cpp` issue.

### What This Enabled

Stage 7 established a known concurrency baseline before adding the alternative epoll network path in Stage 8.

### Limitations

The system is not lock-free. Different partitions still contend on `topics_mutex_`, and synchronous disk I/O happens while that lock is held.

### Future/Interview Explanation

"Stage 7 was a correctness stage. I kept the thread-per-client model but audited shared state and added concurrent workloads. The result is thread-safe but coarse-grained: a global topic mutex serializes produce/fetch access, including fsync during produce."

## Stage 8: Nonblocking I/O And Epoll

### Purpose

Stage 8 added an event-driven networking path for comparison with the thread-per-client server.

### Before This Stage

Only `TcpServer` existed. Each client consumed a detached thread and blocking I/O.

### Concepts Introduced

- Nonblocking sockets.
- `fcntl(F_SETFL, O_NONBLOCK)`.
- `epoll_create1()`, `epoll_ctl()`, `epoll_wait()`.
- Per-client read/write buffers.
- Readiness-based partial I/O.

### What Was Actually Implemented

`src/epoll_server.hpp` and `src/epoll_server.cpp` define `EpollServer`.

Per-client state:

```text
fd
read_buffer
write_buffer
write_offset
joined_groups
```

`main.cpp` now accepts `--epoll` and selects `EpollServer`; otherwise it uses `TcpServer`.

### How It Works

1. The listening socket is made nonblocking.
2. The listening socket is registered with epoll for `EPOLLIN`.
3. The event loop calls `epoll_wait()`.
4. Listening-socket readiness triggers a nonblocking accept loop.
5. Client `EPOLLIN` reads all currently available bytes into `read_buffer`.
6. Complete frames are extracted incrementally.
7. Each complete frame goes through `parse_request()` and `Broker::handle_request()`.
8. Responses are length-framed into `write_buffer`.
9. `EPOLLOUT` is enabled while bytes are pending.
10. Partial sends advance `write_offset`.
11. `EPOLLOUT` is disabled after the buffer drains.

### Important Design Decisions

`EpollServer` reuses the same protocol parser and broker logic as `TcpServer`. It is a networking alternative, not a second broker implementation.

The code uses level-triggered epoll. `EPOLLET` is not used.

### Failure/Edge Cases

- Partial headers and payloads are retained until complete.
- Multiple frames in one read are processed in order.
- Complete frames followed by a partial next frame are handled.
- Oversized frames close the client.
- `EPOLLERR` and `EPOLLHUP` close the client.
- Disconnect cleanup removes joined group memberships.

### Testing/Validation

Stage 8 validation recorded:

- Threaded server regression tests.
- `PING -> PONG`.
- Split frame.
- Multiple frames in one send.
- Complete frames plus partial next frame.
- Four simultaneous clients.
- Real `PRODUCE` persistence through `EpollServer`.

### What This Enabled

Stage 8 enabled Stage 11 TCP-vs-epoll benchmarking with both server paths sharing the same broker behavior.

### Limitations

Stage 8 did not prove epoll performance. It did not add thread pools, multi-event-loop sharding, or per-core brokers.

### Future/Interview Explanation

"Stage 8 added a level-triggered epoll server beside the blocking server. It keeps per-client buffers because readiness events do not equal complete application messages. Complete frames still go through the same parser and broker, so the comparison isolates the networking model."

## Stage 9: Failure Recovery And Delivery Semantics

### Purpose

Stage 9 made log recovery explicit and documented the broker's consumer delivery semantics.

### Before This Stage

Logs were persistent, but record integrity and trailing corruption handling needed stronger guarantees. Delivery behavior needed a clear name.

### Concepts Introduced

- CRC-protected records.
- Invalid final-record truncation.
- Recovery to last valid record boundary.
- At-least-once-style consumer behavior.

### What Was Actually Implemented

`src/record.hpp` now serializes:

```text
[4-byte big-endian payload length][payload bytes][4-byte big-endian CRC32]
```

CRC32 covers the serialized length bytes and payload bytes. `deserialize_record()` validates the checksum before assigning payload or advancing the recovery offset.

`TopicLog::read_all()` reads records until deserialization fails. If bytes remain, it truncates the file to the last valid offset.

### How It Works

Startup recovery:

1. `Broker::recover_from_disk()` scans topic directories.
2. It identifies `partition-<id>.log` files.
3. It calls `TopicLog::read_all()`.
4. `read_all()` accepts records until an incomplete or corrupt record is found.
5. The file is truncated to the last valid boundary.
6. Valid payloads rebuild in-memory partition vectors.

Delivery behavior:

1. `FETCH` returns records but does not commit progress.
2. If the consumer crashes before `COMMIT`, the group offset is unchanged.
3. On reassignment or restart within the same broker process, records may be fetched again.
4. `COMMIT` advances the group-owned offset.

### Important Design Decisions

Recovery does not scan past corruption looking for later valid records. It treats the first invalid trailing region as the end of the valid log.

### Failure/Edge Cases

- Incomplete length header, payload, or checksum stops recovery.
- CRC mismatch stops recovery.
- Trailing invalid bytes are truncated.
- Consumer group offsets are not persisted, so broker restart loses committed group progress.

### Testing/Validation

`tests/test_record.cpp`, `tests/test_topic_log.cpp`, and `tests/test_restart_persistence.py` validate record serialization, corruption/incomplete behavior, and restart persistence.

### What This Enabled

Stage 9 made replication safer because followers also rely on durable append and recovery.

### Limitations

No persistent consumer offsets, exactly-once semantics, idempotent producers, transactions, log repair beyond truncation, or multi-segment recovery.

### Future/Interview Explanation

"Stage 9 added CRC validation and recovery truncation. The broker accepts valid records up to the first incomplete or corrupt trailing record and truncates the rest. Consumer delivery is at-least-once-style because fetch does not commit progress; a crash before commit can cause duplicate delivery."

## Stage 10: Leader/Follower Replication

### Purpose

Stage 10 added simplified two-broker leader/follower replication, catch-up, and explicit promotion.

### Before This Stage

Each broker persisted only its own local logs. There was no follower copy or replication progress.

### Concepts Introduced

- Leader and follower roles.
- Internal broker-to-broker replication command.
- Synchronous replication acknowledgment.
- Follower progress per topic partition.
- Catch-up from follower progress.
- Manual promotion.

### What Was Actually Implemented

`Broker::configure(BrokerRole role, int follower_port)` sets the role and follower port.

Internal commands:

```text
REPLICATE <topic> <partition> <offset> <payload>
REPLICATION_PROGRESS <topic> <partition>
```

Key methods:

- `Broker::synchronize_follower()`
- `Broker::query_follower_progress()`
- `Broker::get_missing_records()`
- `Broker::catch_up_follower()`
- `Broker::promote_to_leader()`

### How It Works

Leader produce with follower configured:

1. Before appending the new record, the leader calls `synchronize_follower()`.
2. The current `handle_request()` call site does not check the boolean return value from this pre-produce synchronization attempt.
3. For each non-empty local partition, synchronization queries follower progress.
4. Missing local records are sent to the follower in offset order.
5. The leader appends the new record locally and fsyncs.
6. The leader sends `REPLICATE` for the new record.
7. The follower appends and fsyncs.
8. The follower updates `replication_progress_`.
9. The follower returns `OK`.
10. The leader returns `OK` to the producer.

Follower restart:

1. The follower recovers topic logs from disk.
2. Because its role is `FOLLOWER`, recovered non-empty partitions initialize `replication_progress_` to `records.size() - 1`.
3. The next leader `PRODUCE` triggers catch-up.

Promotion:

1. `promote_to_leader()` sets role to leader.
2. It clears follower configuration by setting `follower_port_ = -1`.
3. Existing recovered data remains in memory and on disk.
4. New `PRODUCE` requests use the local leader path.

### Important Design Decisions

Replication is synchronous when a follower is configured. This keeps the success response simple: the leader returns `OK` only after the follower acknowledges the replicated append.

### Failure/Edge Cases

- A follower rejects `REPLICATE` unless it is in follower role.
- A non-follower rejects `REPLICATION_PROGRESS`.
- If follower replication fails after local leader append, the current code returns `ERROR`; it does not roll back the local append.
- The pre-produce catch-up attempt returns success/failure internally, but `Broker::handle_request()` does not currently branch on that return value before appending the new record.
- The follower records the supplied replicated offset as progress after append; it does not independently enforce that the offset equals the next local message index.
- Catch-up is triggered by the next leader `PRODUCE`, not by a background manager.

### Testing/Validation

Tests include:

- `tests/test_broker_replicate.cpp`
- `tests/test_replication.py`
- protocol parser tests for internal commands

`plan.md` records validation of synchronous ACK behavior, lag, restart catch-up, progress recovery, manual promotion, post-promotion persistence, failure behavior, and data-directory isolation.

### What This Enabled

Stage 10 made it possible to discuss replication guarantees and production gaps honestly.

### Limitations

Not implemented:

- Automatic failure detection.
- Automatic leader election.
- Heartbeats.
- Quorum consensus.
- Raft or KRaft.
- ISR.
- Controller.
- Client redirection.
- Background retry.
- Consumer-offset replication.
- Runtime TCP promotion command.

### Future/Interview Explanation

"Stage 10 implemented a deliberately simplified leader/follower model. A leader appends locally, sends a framed `REPLICATE` request to the follower, waits for the follower's durable `OK`, and only then returns success. Follower progress is recovered from log length. There is no consensus or automatic failover."

## Stage 11: Observability And Serious Benchmarking

### Purpose

Stage 11 added in-memory broker metrics and a controlled TCP-vs-epoll benchmark experiment.

### Before This Stage

The broker had correctness tests but limited quantitative visibility into throughput, request counts, bytes, or latency.

### Concepts Introduced

- Atomic request counters.
- Server-side latency sampling.
- `METRICS` protocol request.
- Benchmark workload driver.
- Controlled experiment matrix.
- Raw CSV plus aggregate analysis.

### What Was Actually Implemented

Metrics files:

- `src/metrics.hpp`
- `src/metrics.cpp`

Broker integration:

- `Broker::handle_request()` records one metrics sample per handled request.
- `RequestType::METRICS` returns a CSV metrics row.
- `Broker::get_metrics_snapshot()` exposes a snapshot to tests.

Metrics include:

- total requests
- successful requests
- failed requests
- `PRODUCE`, `FETCH`, `JOIN`, `COMMIT` request counts
- total bytes processed
- average latency
- p50 latency
- p95 latency
- p99 latency
- max latency

Benchmark files:

- `benchmarks/benchmark_driver.py`
- `benchmarks/run_tcp_vs_epoll.py`
- `benchmarks/analyze_tcp_vs_epoll.py`
- `benchmarks/plot_benchmarks.py`
- `benchmark-results/tcp_vs_epoll_concurrency.csv`
- `benchmark-results/tcp_vs_epoll_analysis/tcp_vs_epoll_concurrency_summary.csv`

### How It Works

Metrics:

1. `Broker::handle_request()` starts a `steady_clock` timer.
2. It handles the request.
3. It estimates request bytes from parsed field lengths plus a small constant.
4. It records success/failure and elapsed server-side handling time.
5. `METRICS` returns the current CSV row.

Benchmark:

1. The runner starts either TCP or epoll broker mode.
2. It waits until the socket accepts connections.
3. It runs producer threads through `benchmark_driver.py`.
4. The workload timer covers the producer workload, excluding broker startup.
5. After the workload, the driver sends `METRICS`; that query is outside the workload timer.
6. Each run writes a raw row.
7. The analysis script validates the expected matrix and computes means and sample standard deviations.

### Important Design Decisions

The experiment varies only server mode and producer concurrency. Payload size, messages per producer, topic, partition, and repetition count are held constant.

### Failure/Edge Cases

- The runner uses a fresh temporary data directory per run.
- It terminates the broker child process after each run.
- It reports failed configurations rather than inventing replacement rows.
- The analysis script rejects missing modes, producer counts, or repetitions.

### Testing/Validation

- `tests/test_stage11_metrics.cpp`
- Existing raw benchmark CSV with 30 rows.
- Aggregate summary CSV and generated graphs.

### What This Enabled

Stage 11 provides evidence for later README/interview work without claiming universal performance conclusions.

### Limitations

The benchmark is local, single-node, producer-only, one topic, one partition, 1024-byte payloads, and three repetitions per configuration. It does not benchmark multi-node replication, consumer lag, long-running retention, or realistic Kafka workloads.

### Future/Interview Explanation

"Stage 11 added lightweight observability and a controlled benchmark matrix. I measured TCP vs epoll with producer concurrency from 1 to 16, 1000 messages per producer, 1024-byte payloads, and three repetitions. The results are reported as measured facts, not as a universal claim that one networking model always wins."

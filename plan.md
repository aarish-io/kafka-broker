# Kafka Broker Stage Plan

Last updated: 2026-09-14

This file is the short project tracker.

Status legend:
- `DONE` - stage completed
- `IN PROGRESS` - current active stage
- `NEXT` - likely next stage
- `LATER` - planned but not active yet

## Current Position

Current phase: `Stage 10 - Replication` (`DONE`)

Working interpretation of the roadmap:
- `Stage 0` is completed (`DONE`).
- `Stage 1` is completed (`DONE`).
- `Stage 2` is completed (`DONE`).
- `Stage 3` is completed (`DONE`).
- `Stage 4` is completed (`DONE`).
- `Stage 5` is completed (`DONE`).
- `Stage 6` is completed (`DONE`).
- `Stage 7` is completed (`DONE`).
- `Stage 8` is completed (`DONE`).
- `Stage 9` is completed (`DONE`).
- `Stage 10` is completed (`DONE`).

## Stages

| Stage | Status | Short Goal |
| --- | --- | --- |
| 0 | DONE | Understand Kafka concepts and the system shape we are building. |
| 1 | DONE | Build the Linux TCP broker foundation: sockets, client handling, framing, and basic request/response. |
| 2 | DONE | Introduce broker protocol basics: topics, PRODUCE, FETCH, auto-creation, synchronized state, edge cases (mini-stages 2.1–2.8). |
| 3 | DONE | Add persistent append-only logs and restart-safe storage (mini-stages 3.1–3.6). |
| 4 | DONE | Add partitions and partition-aware reads/writes (mini-stages 4.1–4.6). |
| 5 | DONE | Build separate producer and consumer client programs. |
| 6 | DONE | Add consumer groups, assignment, group offset tracking, and consumer integration (mini-stages 6.1-6.6). |
| 7 | DONE | Verify concurrency correctness through shared-state synchronization review, concurrent producer/consumer/group tests, race detection, lock-scope review, and final stress testing. |
| 8 | DONE | Explore Linux non-blocking I/O and `epoll`; added `EpollServer` as an alternative event-driven server path while keeping `TcpServer` as the baseline (mini-stages 8.1-8.7). |
| 9 | DONE | Add crash recovery behavior, persistent record integrity, and delivery semantics documentation. |
| 10 | DONE | Add simplified leader/follower replication, catch-up, and explicit failover. |
| 11 | IN PROGRESS | Add observability, metrics, and serious benchmarking. |
| 12 | LATER | Polish documentation, CI, testing, and resume-ready project material. |

## Stage 10 Completion Summary

Stage 10 is COMPLETED.

Implemented capabilities:
- Two-broker leader/follower replication with independent broker data directories.
- Internal `REPLICATE <topic> <partition> <offset> <payload>` requests over the existing framed TCP request/response path.
- Synchronous follower persistence and acknowledgement before a leader `PRODUCE` returns `OK`.
- Per-topic-partition follower replication progress, including recovery from persisted records.
- Internal `REPLICATION_PROGRESS` queries, missing-record detection, and ordered follower catch-up.
- Follower restart recovery and catch-up triggered by the next leader `PRODUCE`.
- Explicit `Broker::promote_to_leader()` promotion with post-promotion local `PRODUCE` and persistence.
- Final Stage 10 validation covering replication, synchronous ACK behavior, lag, restart catch-up, progress recovery, manual promotion, post-promotion persistence, failure behavior, and data-directory isolation.

Intentional limitations:
- No automatic failure detection, leader election, Raft/KRaft, quorum, ISR, or controller.
- No replication retry system or consumer-offset replication.
- Promotion is explicit/manual and has no runtime TCP promotion command.
- Catch-up is initiated by the next leader `PRODUCE`; there is no background synchronization manager.

## How We Will Use This File

- When a stage is completed, mark it `DONE`.
- When we begin the next stage, change its status to `IN PROGRESS`.
- Keep each stage summary short here.
- Put detailed scope, notes, and deviations in `planinstruction.md`.

## Stage 6 Completion Summary

Stage 6 is COMPLETED.

Completed mini-stages:
- `6.1` Consumer group model and protocol
- `6.2` Group membership with JOIN/LEAVE, duplicate consumer ID rejection, and TCP disconnect cleanup
- `6.3` Deterministic round-robin partition assignment and rebalancing after membership changes
- `6.4` Group-owned committed offsets stored by group, topic, and partition
- `6.5` Consumer client integration: JOIN -> GROUP_POLL -> FETCH -> COMMIT -> LEAVE
- `6.6` Final integration and edge-case testing

Implemented capabilities:
- Consumer group model and protocol
- Group membership using JOIN and LEAVE
- Duplicate consumer ID rejection within the same group
- Consumer membership cleanup on TCP disconnect
- Deterministic round-robin partition assignment
- Rebalancing after membership changes
- Group-owned committed offsets
- GROUP_POLL returning assignments and committed offsets
- Consumer client integration for JOIN -> GROUP_POLL -> FETCH -> COMMIT -> LEAVE
- Final integration and edge-case testing

## Stage 7 Completion Summary

Stage 7 is COMPLETED.

Completed mini-stages:
- `7.1` Current concurrency model audit
- `7.2` Concurrent producer safety
- `7.3` Producer and consumer concurrency
- `7.4` Consumer-group concurrency
- `7.5` ThreadSanitizer race detection
- `7.6` Lock scope and contention review
- `7.7` Final combined concurrency stress test

Verified capabilities:
- Shared topic, partition, and message state is protected by `topics_mutex_`.
- Shared consumer-group state is protected by `consumer_groups_mutex_`.
- The multi-lock ordering rule is `consumer_groups_mutex_` -> `topics_mutex_`.
- Concurrent producer, consumer, FETCH, GROUP_POLL, COMMIT, JOIN, and LEAVE workloads preserve expected message and group state.
- The final stress workload covered 6 producers, 3 consumers, 3 partitions, and 300 messages without message loss or duplication.
- A ThreadSanitizer-instrumented `kafka-broker` target completed the concurrency workloads without reported data races. The entire CMake test target was not built under TSan because of a pre-existing `test_record.cpp` compilation issue.
- Stage 7 retained the thread-per-client architecture and its correct but coarse-grained locking. `PRODUCE` holds `topics_mutex_` through synchronous persistence and `fsync`, while `FETCH` holds it while constructing the response, so different partitions still contend on the same mutex. Redesigning that behavior was intentionally deferred. Serious throughput and latency benchmarking remains Stage 11 work.

## Stage 8 Completion Summary

Stage 8 is COMPLETED.

Completed mini-stages:
- `8.1` EpollServer design boundary (concrete event-driven server beside the unchanged `TcpServer`)
- `8.2` Non-blocking sockets and epoll foundation (`fcntl(O_NONBLOCK)`, `epoll_create1()`, `epoll_ctl()`, `epoll_wait()`)
- `8.3` Nonblocking accept loop and multi-connection management
- `8.4` Nonblocking incremental frame reading with per-client `read_buffer`
- `8.5` Nonblocking framed response writing with per-client `write_buffer`/`write_offset` and `EPOLLOUT` management
- `8.6` Complete real broker path through the existing protocol parser and `Broker`
- `8.7` Hardening plus threaded-vs-epoll validation against the real Linux build

Implemented capabilities:
- Alternative event-driven networking path (`EpollServer`) sharing the existing protocol parser, `Broker`, and storage with `TcpServer`
- Incremental nonblocking accept/read/write with per-client state, preserving partial-frame handling
- Responses use the same existing 4-byte network-order framing
- Consumer-group JOIN/LEAVE membership tracking and disconnect cleanup mirror `TcpServer`
- `TcpServer`, `main.cpp`, protocol implementation, `Broker` implementation, framing format, and storage format were not redesigned

Validation highlights:
- Threaded `TcpServer` regression: sequential PRODUCE OK, multiple sequential clients OK, 10 concurrent producers all OK
- `EpollServer`: complete/split/multi-frame scenarios, frames plus a partial next frame, four simultaneous clients, and a real PRODUCE request all returned correct responses
- `EpollServer` recovered existing topics/messages from disk and persisted new PRODUCE messages

Stage 8 validation verifies correctness of the event-driven networking path, not performance. No universal speed claim is made and no serious quantitative benchmarking was completed; that remains Stage 11 work.

## Stage 9 Completion Summary

Stage 9 is COMPLETED.

Implemented and verified capabilities:
- Topic/partition logs survive broker restart and records are recovered from disk.
- Persistent records now use `[4-byte big-endian payload length][payload bytes][4-byte big-endian CRC32]`.
- CRC32 covers the serialized length bytes followed by the payload bytes.
- Record deserialization validates CRC before accepting the record or advancing the recovery offset.
- Incomplete final records and CRC-corrupt records are detected during recovery.
- Recovery stops at the first invalid record and truncates the log back to the last valid record boundary.
- Current consumer delivery behavior is at-least-once-style: FETCH does not advance committed group progress, messages may be delivered again if a consumer fails before COMMIT, and COMMIT advances the committed offset.
- Committed offsets are associated with consumer group + topic/partition, not with individual consumer connections.
- Consumer-group committed offsets survive consumer reassignment while the broker process remains alive.

Current limitation:
- Topic/partition messages are persistent and recovered from disk.
- Consumer-group committed offsets are in-memory only and are lost on broker restart.
- After broker restart, a group with no recovered committed offset currently begins from offset 0.
- Persistent consumer offsets, exactly-once semantics, and Kafka-level production guarantees are not claimed.

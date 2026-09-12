# Kafka Broker Stage Plan

Last updated: 2026-09-12

This file is the short project tracker.

Status legend:
- `DONE` - stage completed
- `IN PROGRESS` - current active stage
- `NEXT` - likely next stage
- `LATER` - planned but not active yet

## Current Position

Current phase: `Stage 8 - Linux non-blocking I/O and epoll` (`NEXT`; not started)

Working interpretation of the roadmap:
- `Stage 0` is completed (`DONE`).
- `Stage 1` is completed (`DONE`).
- `Stage 2` is completed (`DONE`).
- `Stage 3` is completed (`DONE`).
- `Stage 4` is completed (`DONE`).
- `Stage 5` is completed (`DONE`).
- `Stage 6` is completed (`DONE`).
- `Stage 7` is completed (`DONE`).
- `Stage 8` is the next planned stage (`NEXT`) and has not started.

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
| 8 | NEXT | Explore Linux non-blocking I/O and `epoll`; not started. |
| 9 | LATER | Add crash recovery behavior and delivery semantics testing. |
| 10 | LATER | Add replication with leader/follower behavior. |
| 11 | LATER | Add observability, metrics, and serious benchmarking. |
| 12 | LATER | Polish documentation, CI, testing, and resume-ready project material. |

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

Stage 7 retained the thread-per-client architecture and its correct but coarse-grained locking. `PRODUCE` holds `topics_mutex_` through synchronous persistence and `fsync`, while `FETCH` holds it while constructing the response, so different partitions still contend on the same mutex. Redesigning that behavior was intentionally deferred. Serious throughput and latency benchmarking remains Stage 11 work.

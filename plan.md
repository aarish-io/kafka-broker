# Kafka Broker Stage Plan

Last updated: 2026-09-09

This file is the short project tracker.

Status legend:
- `DONE` - stage completed
- `IN PROGRESS` - current active stage
- `NEXT` - likely next stage
- `LATER` - planned but not active yet

## Current Position

Current phase: `Stage 6 - Consumer Groups`

Working interpretation of the roadmap:
- `Stage 0` is completed (`DONE`).
- `Stage 1` is completed (`DONE`).
- `Stage 2` is completed (`DONE`).
- `Stage 3` is completed (`DONE`).
- `Stage 4` is completed (`DONE`).
- `Stage 5` is completed (`DONE`).
- `Stage 6` is the next active stage (`NEXT`).

## Stages

| Stage | Status | Short Goal |
| --- | --- | --- |
| 0 | DONE | Understand Kafka concepts and the system shape we are building. |
| 1 | DONE | Build the Linux TCP broker foundation: sockets, client handling, framing, and basic request/response. |
| 2 | DONE | Introduce broker protocol basics: topics, PRODUCE, FETCH, auto-creation, synchronized state, edge cases (mini-stages 2.1–2.8). |
| 3 | DONE | Add persistent append-only logs and restart-safe storage (mini-stages 3.1–3.6). |
| 4 | DONE | Add partitions and partition-aware reads/writes (mini-stages 4.1–4.6). |
| 5 | DONE | Build separate producer and consumer client programs. |
| 6 | NEXT | Add consumer groups, assignment, and group offset tracking. |
| 7 | LATER | Improve concurrency, thread-safety, and benchmarking. |
| 8 | LATER | Explore non-blocking I/O and `epoll` for Linux performance. |
| 9 | LATER | Add crash recovery behavior and delivery semantics testing. |
| 10 | LATER | Add replication with leader/follower behavior. |
| 11 | LATER | Add observability, metrics, and serious benchmarking. |
| 12 | LATER | Polish documentation, CI, testing, and resume-ready project material. |

## How We Will Use This File

- When a stage is completed, mark it `DONE`.
- When we begin the next stage, change its status to `IN PROGRESS`.
- Keep each stage summary short here.
- Put detailed scope, notes, and deviations in `planinstruction.md`.


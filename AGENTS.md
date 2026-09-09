# Kafka Broker Project Instructions

## Project

This repository contains a Kafka-inspired event streaming/message broker
implemented from scratch in C++.

The goal is to understand the core ideas behind Kafka and distributed
messaging systems, not to reproduce Apache Kafka completely.

The project should remain small enough to understand and explain in
technical interviews.

---

## Current Goal

Build the system progressively:

1. TCP broker
2. Request/response protocol
3. Topics
4. Partitions
5. Persistent append-only logs
6. Offsets
7. Producer
8. Consumer
9. Consumer groups
10. Concurrency and performance
11. Crash recovery
12. Replication
13. Fault tolerance
14. Benchmarking

Do not implement future phases prematurely.

---

## Development Rules

- Use modern C++.
- Prefer C++17 or newer.
- Prefer the C++ standard library and POSIX APIs.
- Avoid large frameworks.
- Keep dependencies minimal.
- Keep modules focused.
- Prefer simple designs over premature abstraction.
- Do not over-engineer early phases.
- Preserve existing working behavior.
- Add tests for meaningful new functionality.
- Build after significant changes.
- Do not silently change the network protocol.
- Do not modify unrelated files.

---

## Important Project Principle

This is an educational systems project.

Correctness, understandability, observability, and measurable behavior
are more important than prematurely optimizing everything.

When choosing between two designs, prefer the design that is easier to
explain and test unless there is a strong technical reason not to.

---

## Reference Implementations

External implementations and documentation may be consulted for:

- protocol behavior
- architectural ideas
- storage formats
- distributed-system concepts
- testing strategies

Do not copy implementations directly.

The final implementation should be understood and intentionally designed
for this project.

---

## Git Rules

Do NOT perform any of the following unless explicitly requested:

- git commit
- git push
- git reset
- git rebase
- git checkout of existing work
- modifying .git

Do not rewrite Git history.

The human developer owns the Git history.

---

## Safety Rules

Before making a significant architectural change:

1. Inspect the existing implementation.
2. Explain the proposed design.
3. Identify the files that will change.
4. Identify important edge cases.
5. Implement only the approved scope.
6. Build the project.
7. Run relevant tests.
8. Report what changed.

Do not delete working code merely to replace it with a different
implementation unless explicitly instructed.

Do not introduce network dependencies without asking.

---

## Current Phase

Current phase:

Stage 4 - Partitions and Partition-Aware Reads/Writes

---

## Communication

When implementing a feature, briefly report:

1. What you changed.
2. Why you changed it.
3. Files changed.
4. Tests/build commands executed.
5. Any remaining concerns.

If requirements are ambiguous, ask before making a large architectural decision.

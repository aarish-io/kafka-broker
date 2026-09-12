# Current Architecture

Compact reference for the current Kafka-inspired C++ broker. Read this
before modifying architecture so future work builds on the existing
design.

## Structure

``` text
kafka-broker/
├── clients/
│   ├── producer.cpp
│   └── consumer.cpp
├── src/
│   ├── main.cpp
│   ├── broker.hpp
│   ├── broker.cpp
│   ├── protocol.hpp
│   ├── protocol.cpp
│   ├── framing.hpp
│   ├── tcp_server.hpp
│   ├── tcp_server.cpp
│   ├── client.hpp
│   ├── record.hpp
│   └── topic_log.hpp
├── tests/
├── docs/
├── data/                 # runtime data, gitignored
├── build/                # generated build output, gitignored
├── build-tsan/           # local sanitizer build output, gitignored
├── CMakeLists.txt
├── plan.md
├── planinstruction.md
└── AGENTS.md
```

## High-Level Flow

``` text
Producer
  ↓
BrokerClient
  ↓ TCP + 4-byte length framing
TcpServer
  ↓
Framing
  ↓
Protocol Parser
  ↓
Broker
  ↓
Topic / Partition
  ↓
TopicLog / Record
  ↓
Persistent append-only log

Consumer follows the same path in reverse for FETCH.
```

Consumer-group consumers add a group-coordination step before FETCH:

``` text
consumer
  -> JOIN group
  -> GROUP_POLL assignments and committed offsets
  -> FETCH assigned partitions from committed offsets
  -> COMMIT progress
  -> repeat
  -> LEAVE
```

## Responsibilities

### `main.cpp`

Thin application entry point. Constructs `Broker`, performs startup
recovery, constructs `TcpServer`, and starts it. No protocol, socket, or
storage implementation belongs here.

### `broker.hpp / broker.cpp`

Core broker/domain layer. Owns: - topics and partitions - in-memory
messages - broker state/mutexes - PRODUCE and FETCH operations - startup
recovery - consumer groups - group membership - partition assignments -
group-owned committed offsets

Default model: 3 partitions per topic (0, 1, 2).

Consumer-group model:

``` text
Broker
  owns ConsumerGroup map

ConsumerGroup
  owns members by consumer_id
  owns committed offsets by TopicPartition

GroupMember
  owns consumer_id
  owns subscribed topic
  owns current TopicPartition assignments

TopicPartition
  identifies one topic + partition pair
```

Consumer-group state is in memory and protected by
`consumer_groups_mutex_`. Topic, partition, and message state is protected
by `topics_mutex_`.

COMMIT is the only current operation that acquires both mutexes. Its lock
order is `consumer_groups_mutex_` -> `topics_mutex_`; future code that
needs both locks must preserve this order.

### `protocol.hpp / protocol.cpp`

Protocol representation and parsing: - `RequestType` - `Request` -
`parse_request()`

Current commands:

``` text
PING
PRODUCE <topic> <partition> <payload>
FETCH <topic> <partition> <offset>
JOIN <group> <consumer_id> <topic>
LEAVE <group> <consumer_id>
GROUP_POLL <group> <consumer_id>
COMMIT <group> <consumer_id> <topic> <partition> <offset>
```

Protocol parsing must remain independent of TCP and storage.

### `framing.hpp`

Shared TCP framing used by both `TcpServer` and `BrokerClient`: -
`read_exactly()` - `write_exactly()` - `read_frame()` - `write_frame()`

Current wire framing: - 4-byte network-order `uint32_t` payload length -
maximum frame payload 64 KiB - handles partial send/recv - handles
`EINTR` - preserves existing `MSG_NOSIGNAL` behavior

Framing knows about TCP byte streams and frame boundaries, not Kafka
commands or broker state.

### `tcp_server.hpp / tcp_server.cpp`

Server networking layer: - `socket()` - `setsockopt()` - `bind()` -
`listen()` - `accept()` - client connection lifecycle -
thread-per-client model - read framed request - parse request - delegate
to `Broker` - write framed response - track group memberships owned by a
TCP connection for disconnect cleanup

Current model is one detached thread per client.

Each client thread accesses the same `Broker` instance owned by
`main.cpp`. Broker mutexes synchronize shared state; request parsing,
framing buffers, connection membership tracking, and client sockets remain
local to their owning client thread.

Do not introduce epoll, non-blocking I/O, thread pools, or async I/O
until their planned stages.

No `epoll` or non-blocking socket path, thread pool, or per-partition mutex
has been introduced. These remain future design work.

### `client.hpp`

`kafka::BrokerClient`, the client-side networking abstraction.

Handles: - TCP connection - persistent connection lifetime - framed
request/response - partial I/O through shared framing - RAII socket
ownership

Producer and consumer use this instead of raw socket APIs.

### `record.hpp`

Record/message representation for storage.

### `topic_log.hpp`

Persistent partition log: - append records - read records - persistent
append-only storage - existing durability behavior including `fsync` -
existing storage layout and format

Do not put broker/network/protocol logic here.

### `clients/producer.cpp`

One-shot CLI:

``` text
./producer <topic> <partition> <message>
```

Validates arguments, constructs a PRODUCE request, uses `BrokerClient`,
and prints the broker response. It does not know about disk or
`TopicLog`.

### `clients/consumer.cpp`

CLI:

``` text
./consumer <topic> <group_id> <consumer_id>
```

Connects once and uses the consumer-group protocol:

``` text
JOIN
  -> GROUP_POLL
  -> parse assigned partitions and committed offsets
  -> FETCH assigned partitions from committed offsets
  -> display fetched messages
  -> COMMIT resulting progress
  -> repeat
  -> LEAVE
```

Not implemented yet: - persistent committed offsets - retries/reconnection
- batching - async I/O - epoll - heartbeats - session timeouts - advanced
rebalance coordination - follow/background polling

## PRODUCE Flow

``` text
producer
  → BrokerClient
  → TcpServer
  → framing
  → protocol
  → Broker::handle_request()
  → Topic/Partition
  → TopicLog::append()
  → persistent log
```

## FETCH Flow

``` text
consumer
  → BrokerClient
  → TcpServer
  → framing
  → protocol
  → Broker::handle_request()
  → Topic/Partition messages
  → framed response
  → consumer
```

## Consumer Group Flow

``` text
consumer
  -> BrokerClient
  -> TcpServer
  -> protocol
  -> Broker::handle_request()
  -> JOIN / LEAVE / GROUP_POLL / COMMIT
  -> ConsumerGroup / GroupMember / TopicPartition state
```

### JOIN / LEAVE / Disconnect

`JOIN <group> <consumer_id> <topic>` creates the group if needed and adds
the consumer as a `GroupMember`. A duplicate `consumer_id` in the same
group is rejected.

`LEAVE <group> <consumer_id>` removes that member. If the group becomes
empty, the group is removed.

`TcpServer` tracks successful joins for each TCP connection. When that
connection disconnects, the broker removes those consumers from their
groups and removes empty groups when appropriate.

### Assignment / Rebalancing

`rebalance_group()` recalculates assignments after successful JOIN,
LEAVE, and disconnect cleanup.

Assignment is deterministic round-robin over the current members of a
group for the subscribed topic. The project uses the existing 3-partition
topic model, so partitions 0, 1, and 2 are distributed across sorted
consumer IDs. A partition is assigned to at most one consumer in the
group. A consumer may receive multiple partitions when there are fewer
consumers than partitions, or zero partitions when there are more
consumers than partitions.

### GROUP_POLL

`GROUP_POLL <group> <consumer_id>` validates that the group exists and
that the consumer is currently a member. It returns the member's current
assignments. Each response line includes:

``` text
<topic> <partition> <committed_offset>
```

The committed offset comes from the group's in-memory offset table. If no
offset has been committed yet for that group/topic/partition, the broker
returns offset `0`.

### COMMIT

`COMMIT <group> <consumer_id> <topic> <partition> <offset>` validates
the group, member, topic, and partition, then stores the committed offset
under the group and `TopicPartition`.

Committed offsets belong to the consumer group rather than the individual
consumer. If a partition is reassigned to another consumer in the same
group, that consumer can continue from the group's last committed
position.

## Persistence / Recovery

Storage layout:

``` text
data/
└── <topic>/
    ├── partition-0.log
    ├── partition-1.log
    └── partition-2.log
```

Startup:

``` text
Broker
  ↓
recover_from_disk()
  ↓
scan topic/partition logs
  ↓
TopicLog::read_all()
  ↓
rebuild in-memory topics/partitions
  ↓
  TcpServer starts
```

`recover_from_disk()` is startup-only. It completes before `TcpServer`
begins accepting connections and before client threads can access the
shared Broker.

Clients do not own persistence. The broker reconstructs its in-memory
state from disk after restart.

Consumer-group committed offsets are currently in-memory only. They are
not recovered from disk after broker restart.

## Architectural Rules

1.  Preserve behavior unless the roadmap stage explicitly changes it.
2.  Keep networking, framing, protocol, broker logic, and storage
    separated.
3.  Prefer simple concrete code over premature abstractions.
4.  Do not add factories, managers, repositories, interfaces, or
    unnecessary frameworks.
5.  Clients must not access storage internals.
6.  Framing must not know PRODUCE/FETCH semantics.
7.  `TcpServer` must delegate broker business logic.
8.  Keep `main.cpp` thin.
9.  Preserve the wire protocol and persistence format unless a planned
    stage changes them.
10. Keep future non-blocking I/O and epoll work in its planned stages.
11. Keep consumer-group functionality in `Broker`.
12. Do not add persistent group offsets, heartbeats, session timeouts,
    advanced assignment strategies, replication, or delivery semantics
    until their planned stages.

## Stage 7 Concurrency Baseline

Stage 7 is complete. The runtime architecture remains thread-per-client:
`TcpServer` creates one detached thread for each connected client, and all
of those threads share one `Broker` instance.

Completed: - `BrokerClient` - Producer CLI - Consumer group-aware
Consumer CLI - persistent client TCP connections - client validation -
shared TCP framing - modular broker/server/protocol structure - consumer
group model and protocol - JOIN/LEAVE membership - duplicate consumer ID
rejection - TCP disconnect cleanup - deterministic round-robin assignment
- rebalance after membership changes - group-owned committed offsets -
GROUP_POLL assignment/offset response - COMMIT handling - final
integration and edge-case testing

Verified: - producer -> broker - consumer group JOIN - GROUP_POLL
assignment retrieval - FETCH from assigned partitions - COMMIT progress
- LEAVE cleanup - duplicate consumer ID rejection - disconnect cleanup -
rebalance after membership changes - partition isolation - invalid input
- persistence of topic logs across broker restart

Stage 7 added concurrent integration workloads for producers, consumers,
and consumer groups, plus a final stress workload with 6 producers, 3
consumers, 3 partitions, and 300 messages. Those tests verified expected
message contents and counts, no message loss or duplication, and consistent
consumer-group progress.

A separately built ThreadSanitizer-instrumented `kafka-broker` target ran
the concurrency workloads without reported data races. This does not claim
that the entire CMake test target passed under TSan: a pre-existing
`tests/test_record.cpp` compilation issue prevented that full target build.

The current locking is correct but coarse-grained. `PRODUCE` holds
`topics_mutex_` through synchronous `TopicLog` persistence, including disk
I/O and `fsync`, and `FETCH` holds it while constructing the response.
Operations on different partitions therefore still contend on the same
global topic mutex. Stage 7 intentionally retained this tradeoff rather
than changing persistence ordering, failure behavior, or recovery
semantics. It should not be interpreted as a claim of high scalability.

## Next Stage

**Stage 8: Linux non-blocking I/O and epoll** (`NEXT`; not started)

Future work should build on this architecture instead of moving broker
logic back into `main.cpp` or client programs.

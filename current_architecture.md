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

## Responsibilities

### `main.cpp`

Thin application entry point. Constructs `Broker`, performs startup
recovery, constructs `TcpServer`, and starts it. No protocol, socket, or
storage implementation belongs here.

### `broker.hpp / broker.cpp`

Core broker/domain layer. Owns: - topics and partitions - in-memory
messages - broker state/mutex - PRODUCE and FETCH operations - startup
recovery

Default model: 3 partitions per topic (0, 1, 2).

This is the natural layer for future consumer-group state.

### `protocol.hpp / protocol.cpp`

Protocol representation and parsing: - `RequestType` - `Request` -
`parse_request()`

Current commands:

``` text
PING
PRODUCE <topic> <partition> <payload>
FETCH <topic> <partition> <offset>
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
to `Broker` - write framed response

Current model is one detached thread per client.

Do not introduce epoll, non-blocking I/O, thread pools, or async I/O
until their planned stages.

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
./consumer <topic> <partition> <offset>
```

Connects once, sequentially issues FETCH requests, starts from the
supplied offset, advances `current_offset` based on returned messages,
and stops when no messages remain or the broker returns `ERROR`.

Not implemented yet: - consumer groups - committed offsets - automatic
offset persistence - retries/reconnection - batching - async I/O -
epoll - follow/background polling

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

Clients do not own persistence. The broker reconstructs its in-memory
state from disk after restart.

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
10. Keep future concurrency/epoll work in its planned stages.
11. Build future consumer-group functionality around `Broker`.

## Stage 5 Baseline

Stage 5 is complete.

Completed: - `BrokerClient` - Producer CLI - Consumer CLI - persistent
client TCP connections - sequential consumer offset progression - client
validation - shared TCP framing - modular broker/server/protocol
structure - manual integration testing

Verified: - producer → broker - consumer FETCH - different starting
offsets - sequential offset progression - partition isolation - invalid
input - persistence across broker restart - existing functionality after
the modular refactor

## Next Stage

**Stage 6: Consumer Groups**

Future work should build on this architecture instead of moving broker
logic back into `main.cpp` or client programs.

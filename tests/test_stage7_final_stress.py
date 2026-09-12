#!/usr/bin/env python3

import socket
import struct
import sys
import threading
import time
from collections import Counter


HOST = "127.0.0.1"
PORT = 9092

TOPIC = "stage7_final_stress"
GROUP = "stage7_final_group"

PARTITIONS = 3

# Two producers per partition.
PRODUCERS = 6
MESSAGES_PER_PRODUCER = 50

CONSUMERS = 3

TOTAL_MESSAGES = PRODUCERS * MESSAGES_PER_PRODUCER


def send_frame(sock, payload):
    data = payload.encode()
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_exactly(sock, size):
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise RuntimeError("Broker closed connection")

        data += chunk

    return data


def recv_frame(sock):
    header = recv_exactly(sock, 4)
    length = struct.unpack("!I", header)[0]

    if length > 64 * 1024:
        raise RuntimeError("Frame exceeds broker limit")

    return recv_exactly(sock, length).decode()


def request(sock, payload):
    send_frame(sock, payload)
    return recv_frame(sock)


def producer_worker(
    producer_id,
    start_barrier,
    produced,
    errors,
):
    # Two producers share each partition:
    # 0,1 -> partition 0
    # 2,3 -> partition 1
    # 4,5 -> partition 2
    partition = producer_id // 2

    messages = []

    try:
        with socket.create_connection(
            (HOST, PORT),
            timeout=10,
        ) as sock:

            # All producers begin together.
            start_barrier.wait()

            for message_id in range(MESSAGES_PER_PRODUCER):
                message = (
                    f"producer-{producer_id}-"
                    f"partition-{partition}-"
                    f"message-{message_id}"
                )

                response = request(
                    sock,
                    f"PRODUCE {TOPIC} {partition} {message}",
                )

                if response != "OK":
                    raise RuntimeError(
                        f"PRODUCE failed: {response}"
                    )

                messages.append(message)

                # Small delay keeps producers and consumers
                # overlapping for longer.
                time.sleep(0.003)

        produced[producer_id] = messages

    except Exception as exc:
        errors.append(
            f"Producer {producer_id}: {exc}"
        )


def parse_poll(response):
    assignments = []

    if not response:
        return assignments

    for line in response.splitlines():
        parts = line.split()

        if len(parts) != 3:
            raise RuntimeError(
                f"Invalid GROUP_POLL response: {line}"
            )

        assignments.append(
            (
                parts[0],
                int(parts[1]),
                int(parts[2]),
            )
        )

    return assignments


def consumer_worker(
    consumer_id,
    join_barrier,
    start_barrier,
    received,
    received_lock,
    errors,
):
    try:
        with socket.create_connection(
            (HOST, PORT),
            timeout=10,
        ) as sock:

            response = request(
                sock,
                f"JOIN {GROUP} consumer-{consumer_id} {TOPIC}",
            )

            if response != "OK":
                raise RuntimeError(
                    f"JOIN failed: {response}"
                )

            # Don't start polling until all consumers have joined.
            join_barrier.wait()

            # Start consumers and producers together.
            start_barrier.wait()

            deadline = time.time() + 45

            while time.time() < deadline:
                poll_response = request(
                    sock,
                    f"GROUP_POLL {GROUP} consumer-{consumer_id}",
                )

                if poll_response == "ERROR":
                    raise RuntimeError(
                        "GROUP_POLL returned ERROR"
                    )

                assignments = parse_poll(
                    poll_response
                )

                consumed_something = False

                for topic, partition, offset in assignments:
                    if topic != TOPIC:
                        raise RuntimeError(
                            f"Unexpected topic: {topic}"
                        )

                    fetch_response = request(
                        sock,
                        f"FETCH {TOPIC} {partition} {offset}",
                    )

                    if fetch_response == "ERROR":
                        raise RuntimeError(
                            f"FETCH failed for partition {partition}"
                        )

                    if not fetch_response:
                        continue

                    messages = fetch_response.splitlines()

                    with received_lock:
                        for message in messages:
                            received.append(
                                (partition, message)
                            )

                    new_offset = offset + len(messages)

                    commit_response = request(
                        sock,
                        f"COMMIT {GROUP} consumer-{consumer_id} "
                        f"{TOPIC} {partition} {new_offset}",
                    )

                    if commit_response != "OK":
                        raise RuntimeError(
                            f"COMMIT failed: {commit_response}"
                        )

                    consumed_something = True

                with received_lock:
                    if len(received) >= TOTAL_MESSAGES:
                        return

                if not consumed_something:
                    time.sleep(0.005)

            raise RuntimeError(
                "Consumer timed out"
            )

    except Exception as exc:
        errors.append(
            f"Consumer {consumer_id}: {exc}"
        )


def verify_messages(produced, received):
    expected = Counter()

    for messages in produced:
        for message in messages:
            expected[message] += 1

    actual = Counter()

    for partition, message in received:
        actual[message] += 1

    if expected == actual:
        return True

    missing = list(
        (expected - actual).elements()
    )

    extra = list(
        (actual - expected).elements()
    )

    print()
    print("MESSAGE CONTENT MISMATCH")

    if missing:
        print(
            f"Missing messages: {len(missing)}"
        )
        print(
            "Examples:",
            missing[:5],
        )

    if extra:
        print(
            f"Unexpected/duplicate messages: {len(extra)}"
        )
        print(
            "Examples:",
            extra[:5],
        )

    return False


def main():
    print("Stage 7.7 - Final Concurrency Stress Test")
    print(f"Broker: {HOST}:{PORT}")
    print()

    try:
        # Connectivity check.
        with socket.create_connection(
            (HOST, PORT),
            timeout=3,
        ) as sock:

            if request(sock, "PING") != "PONG":
                raise RuntimeError(
                    "Broker PING failed"
                )

        print("Broker connectivity: PASS")

        print()
        print("=" * 60)
        print("FINAL CONCURRENT WORKLOAD")
        print("=" * 60)
        print(f"Producers:              {PRODUCERS}")
        print(f"Consumers:              {CONSUMERS}")
        print(f"Partitions:             {PARTITIONS}")
        print(
            f"Messages per producer: "
            f"{MESSAGES_PER_PRODUCER}"
        )
        print(
            f"Expected total:        "
            f"{TOTAL_MESSAGES}"
        )

        produced = [None] * PRODUCERS
        received = []

        produced_lock = threading.Lock()
        received_lock = threading.Lock()

        producer_errors = []
        consumer_errors = []

        # Consumers first join the group.
        join_barrier = threading.Barrier(
            CONSUMERS
        )

        # Six producers + three consumers all start together.
        start_barrier = threading.Barrier(
            PRODUCERS + CONSUMERS
        )

        consumer_threads = []

        for consumer_id in range(CONSUMERS):
            thread = threading.Thread(
                target=consumer_worker,
                args=(
                    consumer_id,
                    join_barrier,
                    start_barrier,
                    received,
                    received_lock,
                    consumer_errors,
                ),
            )

            thread.start()
            consumer_threads.append(thread)

        # Give consumers time to establish connections.
        time.sleep(0.2)

        producer_threads = []

        for producer_id in range(PRODUCERS):
            thread = threading.Thread(
                target=producer_worker,
                args=(
                    producer_id,
                    start_barrier,
                    produced,
                    producer_errors,
                ),
            )

            thread.start()
            producer_threads.append(thread)

        for thread in producer_threads:
            thread.join()

        for thread in consumer_threads:
            thread.join()

        if producer_errors:
            raise RuntimeError(
                "Producer errors:\n"
                + "\n".join(producer_errors)
            )

        if consumer_errors:
            raise RuntimeError(
                "Consumer errors:\n"
                + "\n".join(consumer_errors)
            )

        actual_produced = sum(
            len(messages)
            for messages in produced
        )

        actual_consumed = len(received)

        print()
        print(
            f"Produced messages: {actual_produced}"
        )
        print(
            f"Consumed messages: {actual_consumed}"
        )

        if actual_produced != TOTAL_MESSAGES:
            raise RuntimeError(
                "Produced message count mismatch"
            )

        if actual_consumed != TOTAL_MESSAGES:
            raise RuntimeError(
                "Consumed message count mismatch"
            )

        if not verify_messages(
            produced,
            received,
        ):
            raise RuntimeError(
                "Produced and consumed message sets differ"
            )

        # Verify each partition independently.
        partition_counts = Counter(
            partition
            for partition, message in received
        )

        expected_per_partition = (
            TOTAL_MESSAGES // PARTITIONS
        )

        print()

        for partition in range(PARTITIONS):
            count = partition_counts[partition]

            print(
                f"Partition {partition}: "
                f"{count} consumed"
            )

            if count != expected_per_partition:
                raise RuntimeError(
                    f"Partition {partition} count mismatch"
                )

        print()
        print("PASS: concurrent producers completed")
        print("PASS: concurrent consumers completed")
        print("PASS: no messages were lost")
        print("PASS: no messages were duplicated")
        print("PASS: all partitions have expected counts")
        print("PASS: producer/consumer/group state remained consistent")

        print()
        print("=" * 60)
        print("ALL STAGE 7 CONCURRENCY TESTS PASSED")
        print("=" * 60)

    except Exception as exc:
        print()
        print("=" * 60)
        print("STAGE 7.7 TEST FAILED")
        print("=" * 60)
        print(f"Reason: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
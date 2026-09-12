#!/usr/bin/env python3

import socket
import struct
import sys
import threading
import time
from collections import Counter

HOST = "127.0.0.1"
PORT = 9092

TOPIC = "stage7_3_live_topic"
GROUP = "stage7_3_group"

PRODUCERS = 3
CONSUMERS = 3
PARTITIONS = 3
MESSAGES_PER_PARTITION = 60


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
    payload = recv_exactly(sock, length)

    return payload.decode()


def request(sock, payload):
    send_frame(sock, payload)
    return recv_frame(sock)


def producer_worker(producer_id, start_barrier, results, errors):
    partition = producer_id
    messages = []

    try:
        with socket.create_connection((HOST, PORT), timeout=10) as sock:
            start_barrier.wait()

            for message_id in range(MESSAGES_PER_PARTITION):
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

                # Deliberately slow production slightly so that
                # consumers overlap with producers.
                time.sleep(0.005)

        results[producer_id] = messages

    except Exception as exc:
        errors.append(
            f"Producer {producer_id}: {exc}"
        )


def parse_assignments(response):
    assignments = []

    if not response:
        return assignments

    for line in response.splitlines():
        parts = line.split()

        if len(parts) != 3:
            raise RuntimeError(
                f"Invalid GROUP_POLL response: {line}"
            )

        topic = parts[0]
        partition = int(parts[1])
        offset = int(parts[2])

        assignments.append(
            {
                "topic": topic,
                "partition": partition,
                "offset": offset,
            }
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
    joined = False

    try:
        with socket.create_connection((HOST, PORT), timeout=10) as sock:
            join_response = request(
                sock,
                f"JOIN {GROUP} {consumer_id} {TOPIC}",
            )

            if join_response != "OK":
                raise RuntimeError(
                    f"JOIN failed: {join_response}"
                )

            joined = True

            # Make sure every consumer has joined before
            # any consumer starts polling assignments.
            join_barrier.wait()

            # Now producers and consumers begin working together.
            start_barrier.wait()

            deadline = time.time() + 30

            while time.time() < deadline:
                poll_response = request(
                    sock,
                    f"GROUP_POLL {GROUP} {consumer_id}",
                )

                if poll_response == "ERROR":
                    raise RuntimeError(
                        "GROUP_POLL returned ERROR"
                    )

                assignments = parse_assignments(
                    poll_response
                )

                if not assignments:
                    time.sleep(0.01)
                    continue

                consumed_any = False

                for assignment in assignments:
                    partition = assignment["partition"]
                    offset = assignment["offset"]

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
                        f"COMMIT {GROUP} {consumer_id} "
                        f"{TOPIC} {partition} {new_offset}",
                    )

                    if commit_response != "OK":
                        raise RuntimeError(
                            f"COMMIT failed: {commit_response}"
                        )

                    consumed_any = True

                # Stop once the whole expected workload has
                # been consumed.
                with received_lock:
                    if len(received) >= (
                        PARTITIONS * MESSAGES_PER_PARTITION
                    ):
                        return

                if not consumed_any:
                    time.sleep(0.01)

            raise RuntimeError(
                "Consumer timed out before consuming all messages"
            )

    except Exception as exc:
        errors.append(
            f"Consumer {consumer_id}: {exc}"
        )

    finally:
        # If JOIN succeeded, explicitly leave the group.
        # The broker also has disconnect cleanup as a safety net.
        if joined:
            try:
                with socket.create_connection(
                    (HOST, PORT), timeout=3
                ) as cleanup_sock:
                    request(
                        cleanup_sock,
                        f"LEAVE {GROUP} {consumer_id}",
                    )
            except Exception:
                pass


def verify_results(produced_messages, received_messages):
    expected = Counter()

    for messages in produced_messages:
        for message in messages:
            expected[message] += 1

    actual = Counter()

    for partition, message in received_messages:
        actual[message] += 1

    if expected != actual:
        missing = list(
            (expected - actual).elements()
        )

        extra = list(
            (actual - expected).elements()
        )

        print("\nMESSAGE MISMATCH")

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

    return True


def main():
    print("Stage 7.3 - Producer + Consumer Concurrency")
    print(f"Broker: {HOST}:{PORT}")
    print()

    try:
        # Basic connectivity check.
        with socket.create_connection(
            (HOST, PORT), timeout=3
        ) as sock:
            response = request(sock, "PING")

            if response != "PONG":
                raise RuntimeError(
                    f"PING failed: {response}"
                )

        print("Broker connectivity: PASS")

        total_expected = (
            PARTITIONS * MESSAGES_PER_PARTITION
        )

        print()
        print("=" * 60)
        print("CONCURRENT WORKLOAD")
        print("=" * 60)
        print(
            f"Producers: {PRODUCERS}"
        )
        print(
            f"Consumers: {CONSUMERS}"
        )
        print(
            f"Partitions: {PARTITIONS}"
        )
        print(
            f"Messages per partition: "
            f"{MESSAGES_PER_PARTITION}"
        )
        print(
            f"Expected total messages: "
            f"{total_expected}"
        )

        produced = [None] * PRODUCERS
        producer_errors = []

        received = []
        received_lock = threading.Lock()

        consumer_errors = []

        # Consumers must all JOIN before polling.
        join_barrier = threading.Barrier(
            CONSUMERS
        )

        # Producers and consumers start their workload
        # together.
        start_barrier = threading.Barrier(
            PRODUCERS + CONSUMERS
        )

        consumer_threads = []

        for consumer_id in range(CONSUMERS):
            thread = threading.Thread(
                target=consumer_worker,
                args=(
                    f"consumer-{consumer_id}",
                    join_barrier,
                    start_barrier,
                    received,
                    received_lock,
                    consumer_errors,
                ),
            )

            thread.start()
            consumer_threads.append(thread)

        # Give consumers a moment to establish their
        # connections and join the group.
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

        expected_produced = total_expected

        actual_produced = sum(
            len(messages)
            for messages in produced
        )

        print()
        print(
            f"Produced messages: {actual_produced}"
        )
        print(
            f"Consumed messages: {len(received)}"
        )

        if actual_produced != expected_produced:
            raise RuntimeError(
                "Producer count mismatch"
            )

        if len(received) != expected_produced:
            raise RuntimeError(
                "Consumer count mismatch"
            )

        if not verify_results(
            produced,
            received,
        ):
            raise RuntimeError(
                "Consumed messages do not match "
                "produced messages"
            )

        print(
            "PASS: producers and consumers overlapped safely"
        )
        print(
            "PASS: no messages lost"
        )
        print(
            "PASS: no messages duplicated"
        )
        print(
            "PASS: consumer-group FETCH/COMMIT "
            "progress remained consistent"
        )

        print()
        print("=" * 60)
        print("ALL STAGE 7.3 CONCURRENCY TESTS PASSED")
        print("=" * 60)

    except Exception as exc:
        print()
        print("=" * 60)
        print("STAGE 7.3 TEST FAILED")
        print("=" * 60)
        print(f"Reason: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
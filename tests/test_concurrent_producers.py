#!/usr/bin/env python3

import socket
import struct
import sys
import threading
from collections import Counter

HOST = "127.0.0.1"
PORT = 9092

# Keep this reasonably small because every PRODUCE currently calls fsync().
PRODUCERS = 10
MESSAGES_PER_PRODUCER = 30


def send_frame(sock, payload):
    data = payload.encode()

    if len(data) > 64 * 1024:
        raise RuntimeError("Payload exceeds broker's 64 KiB frame limit")

    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_exactly(sock, size):
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise RuntimeError("Broker closed connection unexpectedly")

        data += chunk

    return data


def recv_frame(sock):
    header = recv_exactly(sock, 4)
    length = struct.unpack("!I", header)[0]

    if length > 64 * 1024:
        raise RuntimeError(f"Broker returned oversized frame: {length}")

    payload = recv_exactly(sock, length)

    return payload.decode()


def request(sock, payload):
    send_frame(sock, payload)
    return recv_frame(sock)


def produce_worker(topic, partition, producer_id, results, barrier):
    messages = []

    try:
        with socket.create_connection((HOST, PORT), timeout=10) as sock:
            # Make all producers start their workload together.
            barrier.wait()

            for message_id in range(MESSAGES_PER_PRODUCER):
                message = f"producer-{producer_id}-message-{message_id}"

                response = request(
                    sock,
                    f"PRODUCE {topic} {partition} {message}",
                )

                if response != "OK":
                    raise RuntimeError(
                        f"PRODUCE failed for {message}: {response}"
                    )

                messages.append(message)

        results[producer_id] = messages

    except Exception as exc:
        results[producer_id] = exc


def fetch_partition(topic, partition):
    with socket.create_connection((HOST, PORT), timeout=10) as sock:
        response = request(sock, f"FETCH {topic} {partition} 0")

    if response == "ERROR":
        raise RuntimeError(
            f"FETCH returned ERROR for {topic} partition {partition}"
        )

    if response == "":
        return []

    return response.split("\n")


def verify_unique_messages(expected_messages, actual_messages):
    expected = Counter(expected_messages)
    actual = Counter(actual_messages)

    if expected != actual:
        missing = list((expected - actual).elements())
        extra = list((actual - expected).elements())

        print("\nMESSAGE MISMATCH")

        if missing:
            print(f"Missing messages: {len(missing)}")
            print("Examples:", missing[:5])

        if extra:
            print(f"Unexpected/duplicate messages: {len(extra)}")
            print("Examples:", extra[:5])

        return False

    return True


def test_same_partition():
    topic = "stage7_2_same_partition"
    partition = 0

    expected_count = PRODUCERS * MESSAGES_PER_PRODUCER

    print("=" * 60)
    print("TEST 1: Concurrent producers -> SAME partition")
    print("=" * 60)
    print(
        f"Producers: {PRODUCERS}, "
        f"messages each: {MESSAGES_PER_PRODUCER}, "
        f"expected: {expected_count}"
    )

    barrier = threading.Barrier(PRODUCERS)
    results = [None] * PRODUCERS
    threads = []

    for producer_id in range(PRODUCERS):
        thread = threading.Thread(
            target=produce_worker,
            args=(
                topic,
                partition,
                producer_id,
                results,
                barrier,
            ),
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    for producer_id, result in enumerate(results):
        if isinstance(result, Exception):
            raise RuntimeError(
                f"Producer {producer_id} failed: {result}"
            )

    expected_messages = []

    for producer_messages in results:
        expected_messages.extend(producer_messages)

    actual_messages = fetch_partition(topic, partition)

    print(f"Expected message count: {expected_count}")
    print(f"Actual message count:   {len(actual_messages)}")

    if len(actual_messages) != expected_count:
        raise RuntimeError("MESSAGE COUNT MISMATCH")

    if not verify_unique_messages(expected_messages, actual_messages):
        raise RuntimeError("MESSAGE CONTENT MISMATCH")

    print("PASS: no messages lost or duplicated")
    print("PASS: all concurrent PRODUCE requests succeeded")

    return expected_count


def test_different_partitions():
    topic = "stage7_2_different_partitions"

    # 3 producers, one for each partition.
    producers = 3
    messages_each = 30

    print("\n" + "=" * 60)
    print("TEST 2: Concurrent producers -> DIFFERENT partitions")
    print("=" * 60)

    barrier = threading.Barrier(producers)
    results = [None] * producers
    threads = []

    def worker(producer_id):
        partition = producer_id
        messages = []

        try:
            with socket.create_connection(
                (HOST, PORT), timeout=10
            ) as sock:
                barrier.wait()

                for message_id in range(messages_each):
                    message = (
                        f"partition-{partition}-"
                        f"producer-{producer_id}-"
                        f"message-{message_id}"
                    )

                    response = request(
                        sock,
                        f"PRODUCE {topic} {partition} {message}",
                    )

                    if response != "OK":
                        raise RuntimeError(
                            f"PRODUCE failed: {response}"
                        )

                    messages.append(message)

            results[producer_id] = messages

        except Exception as exc:
            results[producer_id] = exc

    for producer_id in range(producers):
        thread = threading.Thread(
            target=worker,
            args=(producer_id,),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    for producer_id, result in enumerate(results):
        if isinstance(result, Exception):
            raise RuntimeError(
                f"Partition producer {producer_id} failed: {result}"
            )

    for partition in range(3):
        expected = results[partition]
        actual = fetch_partition(topic, partition)

        print(
            f"Partition {partition}: "
            f"expected={len(expected)}, "
            f"actual={len(actual)}"
        )

        if actual != expected:
            if not verify_unique_messages(expected, actual):
                raise RuntimeError(
                    f"Partition {partition} message mismatch"
                )

    print("PASS: all three partitions contain expected messages")


def test_concurrent_topic_creation():
    print("\n" + "=" * 60)
    print("TEST 3: Concurrent topic creation")
    print("=" * 60)

    topic_count = 10
    results = [None] * topic_count
    barrier = threading.Barrier(topic_count)
    threads = []

    def worker(topic_id):
        topic = f"stage7_2_topic_{topic_id}"
        message = f"created-by-producer-{topic_id}"

        try:
            with socket.create_connection(
                (HOST, PORT), timeout=10
            ) as sock:
                barrier.wait()

                response = request(
                    sock,
                    f"PRODUCE {topic} 0 {message}",
                )

                if response != "OK":
                    raise RuntimeError(
                        f"PRODUCE failed: {response}"
                    )

            actual = fetch_partition(topic, 0)

            if actual != [message]:
                raise RuntimeError(
                    f"Unexpected messages for {topic}: {actual}"
                )

            results[topic_id] = True

        except Exception as exc:
            results[topic_id] = exc

    for topic_id in range(topic_count):
        thread = threading.Thread(
            target=worker,
            args=(topic_id,),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    for topic_id, result in enumerate(results):
        if result is not True:
            raise RuntimeError(
                f"Topic creation test failed for topic {topic_id}: "
                f"{result}"
            )

    print(f"PASS: concurrently created and verified {topic_count} topics")


def main():
    print("Stage 7.2 - Concurrent Producer Safety")
    print(f"Broker: {HOST}:{PORT}")
    print()

    try:
        # First make sure the broker is reachable.
        with socket.create_connection((HOST, PORT), timeout=3) as sock:
            response = request(sock, "PING")

            if response != "PONG":
                raise RuntimeError(
                    f"Broker responded incorrectly to PING: {response}"
                )

        print("Broker connectivity: PASS")

        test_same_partition()
        test_different_partitions()
        test_concurrent_topic_creation()

        print("\n" + "=" * 60)
        print("ALL STAGE 7.2 CONCURRENCY TESTS PASSED")
        print("=" * 60)

    except Exception as exc:
        print("\n" + "=" * 60)
        print("STAGE 7.2 TEST FAILED")
        print("=" * 60)
        print(f"Reason: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
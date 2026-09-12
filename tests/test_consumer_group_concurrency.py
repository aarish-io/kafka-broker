#!/usr/bin/env python3

import socket
import struct
import sys
import threading


HOST = "127.0.0.1"
PORT = 9092

TOPIC = "stage7_4_group_topic"
GROUP = "stage7_4_group"

CONSUMERS = 3
PARTITIONS = 3


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


def produce_initial_messages():
    """
    Put one message into each partition so consumers have
    something real to FETCH and COMMIT.
    """

    for partition in range(PARTITIONS):
        with socket.create_connection(
            (HOST, PORT), timeout=5
        ) as sock:

            message = f"stage7_4-message-{partition}"

            response = request(
                sock,
                f"PRODUCE {TOPIC} {partition} {message}",
            )

            if response != "OK":
                raise RuntimeError(
                    f"Initial PRODUCE failed for partition "
                    f"{partition}: {response}"
                )

    print("Initial messages: PASS")


def concurrent_join_test():
    print()
    print("=" * 60)
    print("TEST 1: Concurrent JOIN")
    print("=" * 60)

    barrier = threading.Barrier(CONSUMERS)

    sockets = [None] * CONSUMERS
    results = [None] * CONSUMERS
    errors = []

    def worker(consumer_id):
        try:
            sock = socket.create_connection(
                (HOST, PORT), timeout=5
            )

            sockets[consumer_id] = sock

            barrier.wait()

            response = request(
                sock,
                f"JOIN {GROUP} consumer-{consumer_id} {TOPIC}",
            )

            results[consumer_id] = response

        except Exception as exc:
            errors.append(
                f"consumer-{consumer_id}: {exc}"
            )

    threads = []

    for consumer_id in range(CONSUMERS):
        thread = threading.Thread(
            target=worker,
            args=(consumer_id,),
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(
            "JOIN errors:\n" + "\n".join(errors)
        )

    for consumer_id, response in enumerate(results):
        if response != "OK":
            raise RuntimeError(
                f"consumer-{consumer_id} JOIN failed: "
                f"{response}"
            )

    print(
        f"PASS: {CONSUMERS} consumers joined concurrently"
    )

    return sockets


def concurrent_poll_commit_test(sockets):
    print()
    print("=" * 60)
    print("TEST 2: Concurrent GROUP_POLL + COMMIT")
    print("=" * 60)

    barrier = threading.Barrier(CONSUMERS)

    assignments = [None] * CONSUMERS
    errors = []

    def worker(consumer_id):
        try:
            sock = sockets[consumer_id]

            barrier.wait()

            poll_response = request(
                sock,
                f"GROUP_POLL {GROUP} consumer-{consumer_id}",
            )

            if poll_response == "ERROR":
                raise RuntimeError(
                    "GROUP_POLL returned ERROR"
                )

            lines = poll_response.splitlines()

            parsed = []

            for line in lines:
                parts = line.split()

                if len(parts) != 3:
                    raise RuntimeError(
                        f"Invalid GROUP_POLL response: {line}"
                    )

                topic = parts[0]
                partition = int(parts[1])
                offset = int(parts[2])

                parsed.append(
                    (topic, partition, offset)
                )

            assignments[consumer_id] = parsed

            # Each consumer should have exactly one partition
            # with three consumers and three partitions.
            if len(parsed) != 1:
                raise RuntimeError(
                    f"Expected exactly one assignment, "
                    f"got {len(parsed)}"
                )

            topic, partition, offset = parsed[0]

            if topic != TOPIC:
                raise RuntimeError(
                    f"Unexpected topic: {topic}"
                )

            # Fetch the message assigned to this consumer.
            fetch_response = request(
                sock,
                f"FETCH {TOPIC} {partition} {offset}",
            )

            if fetch_response == "ERROR":
                raise RuntimeError(
                    f"FETCH failed for partition {partition}"
                )

            messages = (
                fetch_response.splitlines()
                if fetch_response
                else []
            )

            if len(messages) != 1:
                raise RuntimeError(
                    f"Expected one message from partition "
                    f"{partition}, got {len(messages)}"
                )

            expected_message = (
                f"stage7_4-message-{partition}"
            )

            if messages[0] != expected_message:
                raise RuntimeError(
                    f"Unexpected message from partition "
                    f"{partition}: {messages[0]}"
                )

            # Commit the offset after consuming the message.
            commit_response = request(
                sock,
                f"COMMIT {GROUP} consumer-{consumer_id} "
                f"{TOPIC} {partition} 1",
            )

            if commit_response != "OK":
                raise RuntimeError(
                    f"COMMIT failed: {commit_response}"
                )

        except Exception as exc:
            errors.append(
                f"consumer-{consumer_id}: {exc}"
            )

    threads = []

    for consumer_id in range(CONSUMERS):
        thread = threading.Thread(
            target=worker,
            args=(consumer_id,),
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(
            "GROUP_POLL/COMMIT errors:\n"
            + "\n".join(errors)
        )

    # Verify that all three partitions were assigned exactly once.
    assigned_partitions = []

    for consumer_id in range(CONSUMERS):
        for topic, partition, offset in assignments[consumer_id]:
            assigned_partitions.append(partition)

    if sorted(assigned_partitions) != [0, 1, 2]:
        raise RuntimeError(
            "Unexpected partition assignments: "
            f"{assigned_partitions}"
        )

    print(
        "PASS: all consumers polled concurrently"
    )
    print(
        "PASS: all three partitions were assigned exactly once"
    )
    print(
        "PASS: concurrent FETCH + COMMIT succeeded"
    )


def duplicate_join_race_test():
    print()
    print("=" * 60)
    print("TEST 3: Duplicate JOIN race")
    print("=" * 60)

    barrier = threading.Barrier(2)

    results = [None, None]
    sockets = [None, None]
    errors = []

    def worker(index):
        try:
            sock = socket.create_connection(
                (HOST, PORT), timeout=5
            )

            sockets[index] = sock

            barrier.wait()

            results[index] = request(
                sock,
                f"JOIN {GROUP} duplicate-consumer {TOPIC}",
            )

        except Exception as exc:
            errors.append(
                f"duplicate JOIN worker {index}: {exc}"
            )

    threads = []

    for index in range(2):
        thread = threading.Thread(
            target=worker,
            args=(index,),
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(
            "Duplicate JOIN errors:\n"
            + "\n".join(errors)
        )

    ok_count = results.count("OK")
    error_count = results.count("ERROR")

    if ok_count != 1 or error_count != 1:
        raise RuntimeError(
            "Expected exactly one OK and one ERROR, "
            f"got: {results}"
        )

    print(
        "PASS: concurrent duplicate JOIN produced "
        "exactly one OK and one ERROR"
    )

    for sock in sockets:
        if sock:
            sock.close()


def leave_group(sockets):
    print()
    print("=" * 60)
    print("TEST 4: Concurrent LEAVE")
    print("=" * 60)

    barrier = threading.Barrier(CONSUMERS)
    errors = []

    def worker(consumer_id):
        try:
            sock = sockets[consumer_id]

            barrier.wait()

            response = request(
                sock,
                f"LEAVE {GROUP} consumer-{consumer_id}",
            )

            if response != "OK":
                raise RuntimeError(
                    f"LEAVE failed: {response}"
                )

            sock.close()

        except Exception as exc:
            errors.append(
                f"consumer-{consumer_id}: {exc}"
            )

    threads = []

    for consumer_id in range(CONSUMERS):
        thread = threading.Thread(
            target=worker,
            args=(consumer_id,),
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(
            "LEAVE errors:\n"
            + "\n".join(errors)
        )

    print(
        "PASS: all consumers left concurrently"
    )


def main():
    print("Stage 7.4 - Consumer-Group Concurrency")
    print(f"Broker: {HOST}:{PORT}")
    print()

    try:
        with socket.create_connection(
            (HOST, PORT), timeout=3
        ) as sock:
            response = request(sock, "PING")

            if response != "PONG":
                raise RuntimeError(
                    f"PING failed: {response}"
                )

        print("Broker connectivity: PASS")

        produce_initial_messages()

        sockets = concurrent_join_test()

        concurrent_poll_commit_test(sockets)

        duplicate_join_race_test()

        leave_group(sockets)

        print()
        print("=" * 60)
        print("ALL STAGE 7.4 CONCURRENCY TESTS PASSED")
        print("=" * 60)

    except Exception as exc:
        print()
        print("=" * 60)
        print("STAGE 7.4 TEST FAILED")
        print("=" * 60)
        print(f"Reason: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
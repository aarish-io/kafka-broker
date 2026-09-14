#!/usr/bin/env python3

import os
import shutil
import socket
import struct
import subprocess
import tempfile
import time


HOST = "127.0.0.1"
LEADER_PORT = 19102
FOLLOWER_PORT = 19103
REJECTOR_PORT = 19104
BROKER_BIN = os.environ.get("BROKER_BIN", "./build-linux/kafka-broker")


def frame(payload):
    encoded = payload.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


def request(port, payload):
    with socket.create_connection((HOST, port), timeout=2.0) as connection:
        connection.sendall(frame(payload))
        header = read_exactly(connection, 4)
        length = struct.unpack("!I", header)[0]
        return read_exactly(connection, length).decode("utf-8")


def read_exactly(connection, length):
    result = b""
    while len(result) < length:
        chunk = connection.recv(length - len(result))
        if not chunk:
            raise RuntimeError("connection closed before framed response completed")
        result += chunk
    return result


def wait_for_port(port):
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"broker did not listen on port {port}")


def read_payloads(path):
    with open(path, "rb") as log_file:
        data = log_file.read()

    payloads = []
    offset = 0
    while offset < len(data):
        payload_length = struct.unpack("!I", data[offset:offset + 4])[0]
        offset += 4
        payloads.append(data[offset:offset + payload_length].decode("utf-8"))
        offset += payload_length + 4
    return payloads


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def start_broker(port, data_dir, role, follower_port=None):
    command = [BROKER_BIN, str(port), data_dir, role]
    if follower_port is not None:
        command.append(str(follower_port))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wait_for_port(port)
    return process


def assert_log_contains(data_dir, topic, partition, expected_payloads):
    log_path = os.path.join(data_dir, topic, f"partition-{partition}.log")
    assert os.path.exists(log_path), f"missing log {log_path}"
    assert read_payloads(log_path) == expected_payloads


def test_successful_replication():
    leader_data = tempfile.mkdtemp(prefix="kafka-leader-")
    follower_data = tempfile.mkdtemp(prefix="kafka-follower-")
    follower = None
    leader = None

    try:
        follower = start_broker(FOLLOWER_PORT, follower_data, "follower")
        leader = start_broker(LEADER_PORT, leader_data, "leader", FOLLOWER_PORT)

        assert request(LEADER_PORT, "PRODUCE orders 1 replicated payload") == "OK"
        assert request(LEADER_PORT, "FETCH orders 1 0") == "replicated payload"
        assert_log_contains(leader_data, "orders", 1, ["replicated payload"])
        assert_log_contains(follower_data, "orders", 1, ["replicated payload"])
        print("[PASS] synchronous replication succeeds after follower persistence")
    finally:
        if leader is not None:
            stop(leader)
        if follower is not None:
            stop(follower)
        shutil.rmtree(leader_data)
        shutil.rmtree(follower_data)


def test_unavailable_follower():
    leader_data = tempfile.mkdtemp(prefix="kafka-leader-unavailable-")
    leader = None

    try:
        leader = start_broker(LEADER_PORT, leader_data, "leader", FOLLOWER_PORT)
        assert request(LEADER_PORT, "PRODUCE orders 0 local despite failure") == "ERROR"
        assert request(LEADER_PORT, "FETCH orders 0 0") == "local despite failure"
        assert_log_contains(leader_data, "orders", 0, ["local despite failure"])
        print("[PASS] unavailable follower returns ERROR after local append")
    finally:
        if leader is not None:
            stop(leader)
        shutil.rmtree(leader_data)


def test_rejecting_follower():
    leader_data = tempfile.mkdtemp(prefix="kafka-leader-rejected-")
    rejector_data = tempfile.mkdtemp(prefix="kafka-rejector-")
    leader = None
    rejector = None

    try:
        rejector = start_broker(REJECTOR_PORT, rejector_data, "leader", LEADER_PORT)
        leader = start_broker(LEADER_PORT, leader_data, "leader", REJECTOR_PORT)
        assert request(LEADER_PORT, "PRODUCE orders 2 rejected replication") == "ERROR"
        assert request(LEADER_PORT, "FETCH orders 2 0") == "rejected replication"
        assert_log_contains(leader_data, "orders", 2, ["rejected replication"])
        assert not os.path.exists(os.path.join(rejector_data, "orders"))
        print("[PASS] follower rejection returns ERROR after local append")
    finally:
        if leader is not None:
            stop(leader)
        if rejector is not None:
            stop(rejector)
        shutil.rmtree(leader_data)
        shutil.rmtree(rejector_data)


def main():
    test_successful_replication()
    test_unavailable_follower()
    test_rejecting_follower()


if __name__ == "__main__":
    main()
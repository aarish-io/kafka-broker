#!/usr/bin/env python3
"""
Stage 4 Verification Test Suite

Covers Test Groups 1 through 9 for Stage 4:
- Group 1: Partition Model (default 3 partitions 0, 1, 2 per topic)
- Group 2: Produce Routing
- Group 3: Partition Isolation
- Group 4: Offset Fetching
- Group 5: Multiple Messages in Multiple Partitions
- Group 6: Persistence Across Restart
- Group 7: Invalid Partitions
- Group 8: Invalid Offsets
- Group 9: Storage Layout & Existing Persistence Behavior
"""

import os
import shutil
import socket
import struct
import subprocess
import sys
import time

HOST = "127.0.0.1"
PORT = 9092
BROKER_BIN = "./broker"
DATA_DIR = "data"


def frame(payload: str) -> bytes:
    data = payload.encode("utf-8")
    return struct.pack("!I", len(data)) + data


def send_request(sock: socket.socket, request: str) -> str:
    sock.sendall(frame(request))
    header = sock.recv(4)
    if len(header) < 4:
        raise RuntimeError("Failed to read header from broker")
    length = struct.unpack("!I", header)[0]
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk:
            raise RuntimeError("Connection closed prematurely")
        payload += chunk
    return payload.decode("utf-8")


def wait_for_broker(timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.5) as s:
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False


def start_broker():
    proc = subprocess.Popen([BROKER_BIN], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if not wait_for_broker():
        proc.kill()
        out, err = proc.communicate()
        raise RuntimeError(f"Broker failed to start. Stdout: {out.decode()}, Stderr: {err.decode()}")
    return proc


def stop_broker(proc):
    proc.terminate()
    try:
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_stage4_tests():
    print("==================================================")
    print("      STAGE 4 COMPREHENSIVE TEST SUITE           ")
    print("==================================================")

    # Cleanup before starting
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    broker = start_broker()
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            # --------------------------------------------------
            # TEST GROUP 1 & 2: PARTITION MODEL & PRODUCE ROUTING
            # --------------------------------------------------
            print("\n[Group 1 & 2] Partition Model & Produce Routing")
            assert send_request(sock, "PRODUCE topic-g1 0 Message_P0") == "OK"
            assert send_request(sock, "PRODUCE topic-g1 1 Message_P1") == "OK"
            assert send_request(sock, "PRODUCE topic-g1 2 Message_P2") == "OK"
            print("  ✓ PASS: Messages routed into partitions 0, 1, and 2 independently")

            # --------------------------------------------------
            # TEST GROUP 3: PARTITION ISOLATION
            # --------------------------------------------------
            print("\n[Group 3] Partition Isolation")
            res_p0 = send_request(sock, "FETCH topic-g1 0 0")
            res_p1 = send_request(sock, "FETCH topic-g1 1 0")
            res_p2 = send_request(sock, "FETCH topic-g1 2 0")

            assert res_p0 == "Message_P0", f"Expected Message_P0, got '{res_p0}'"
            assert res_p1 == "Message_P1", f"Expected Message_P1, got '{res_p1}'"
            assert res_p2 == "Message_P2", f"Expected Message_P2, got '{res_p2}'"
            print("  ✓ PASS: FETCH topic-g1 partition 0/1/2 returns only partition-local messages")

            # --------------------------------------------------
            # TEST GROUP 4: OFFSET FETCHING
            # --------------------------------------------------
            print("\n[Group 4] Offset Fetching")
            assert send_request(sock, "PRODUCE topic-g4 0 A") == "OK"
            assert send_request(sock, "PRODUCE topic-g4 0 B") == "OK"
            assert send_request(sock, "PRODUCE topic-g4 0 C") == "OK"

            assert send_request(sock, "FETCH topic-g4 0 0") == "A\nB\nC"
            assert send_request(sock, "FETCH topic-g4 0 1") == "B\nC"
            assert send_request(sock, "FETCH topic-g4 0 2") == "C"
            assert send_request(sock, "FETCH topic-g4 0 3") == ""
            assert send_request(sock, "FETCH topic-g4 0 100") == ""
            print("  ✓ PASS: Offset fetching at indices 0, 1, 2, 3, and out-of-bounds verified")

            # --------------------------------------------------
            # TEST GROUP 5: MULTIPLE MESSAGES IN MULTIPLE PARTITIONS
            # --------------------------------------------------
            print("\n[Group 5] Multiple Messages in Multiple Partitions")
            assert send_request(sock, "PRODUCE topic-g5 0 P0_A") == "OK"
            assert send_request(sock, "PRODUCE topic-g5 0 P0_B") == "OK"
            assert send_request(sock, "PRODUCE topic-g5 1 P1_X") == "OK"
            assert send_request(sock, "PRODUCE topic-g5 1 P1_Y") == "OK"
            assert send_request(sock, "PRODUCE topic-g5 2 P2_M") == "OK"
            assert send_request(sock, "PRODUCE topic-g5 2 P2_N") == "OK"

            assert send_request(sock, "FETCH topic-g5 0 0") == "P0_A\nP0_B"
            assert send_request(sock, "FETCH topic-g5 1 0") == "P1_X\nP1_Y"
            assert send_request(sock, "FETCH topic-g5 2 0") == "P2_M\nP2_N"
            assert send_request(sock, "FETCH topic-g5 0 1") == "P0_B"
            assert send_request(sock, "FETCH topic-g5 1 1") == "P1_Y"
            assert send_request(sock, "FETCH topic-g5 2 1") == "P2_N"
            print("  ✓ PASS: Offset sequence indices exist independently in each partition")

            # --------------------------------------------------
            # TEST GROUP 7: INVALID PARTITIONS
            # --------------------------------------------------
            print("\n[Group 7] Invalid Partitions")
            invalid_partition_tests = [
                "PRODUCE topic-g7 -1 hello",
                "PRODUCE topic-g7 3 hello",
                "PRODUCE topic-g7 abc hello",
                "PRODUCE topic-g7",
                "FETCH topic-g7 -1 0",
                "FETCH topic-g7 3 0",
                "FETCH topic-g7 abc 0",
                "FETCH topic-g7",
            ]
            for cmd in invalid_partition_tests:
                res = send_request(sock, cmd)
                assert res == "ERROR", f"Expected ERROR for '{cmd}', got '{res}'"
            print("  ✓ PASS: Invalid partition numbers rejected with ERROR")

            # --------------------------------------------------
            # TEST GROUP 8: INVALID OFFSETS
            # --------------------------------------------------
            print("\n[Group 8] Invalid Offsets")
            invalid_offset_tests = [
                "FETCH topic-g4 0",           # Missing offset
                "FETCH topic-g4 0 -1",        # Negative offset
                "FETCH topic-g4 0 abc",       # Non-numeric offset
                "FETCH topic-g4 0 12abc",     # Trailing invalid chars
            ]
            for cmd in invalid_offset_tests:
                res = send_request(sock, cmd)
                assert res == "ERROR", f"Expected ERROR for '{cmd}', got '{res}'"
            print("  ✓ PASS: Invalid offset values rejected with ERROR")
    finally:
        stop_broker(broker)

    time.sleep(0.2)

    # --------------------------------------------------
    # TEST GROUP 6 & 9: PERSISTENCE ACROSS RESTART & STORAGE LAYOUT
    # --------------------------------------------------
    print("\n[Group 6 & 9] Persistence Across Restart & Storage Layout")

    # Check directory structure before restarting
    expected_p0 = os.path.join(DATA_DIR, "topic-g5", "partition-0.log")
    expected_p1 = os.path.join(DATA_DIR, "topic-g5", "partition-1.log")
    expected_p2 = os.path.join(DATA_DIR, "topic-g5", "partition-2.log")
    assert os.path.exists(expected_p0), f"Missing expected file {expected_p0}"
    assert os.path.exists(expected_p1), f"Missing expected file {expected_p1}"
    assert os.path.exists(expected_p2), f"Missing expected file {expected_p2}"
    print(f"  ✓ PASS: Physical log layout confirmed under {DATA_DIR}/<topic>/partition-<id>.log")

    print("  Restarting broker to verify state recovery...")
    broker = start_broker()
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            # Verify recovered messages from topic-g5
            assert send_request(sock, "FETCH topic-g5 0 0") == "P0_A\nP0_B"
            assert send_request(sock, "FETCH topic-g5 1 0") == "P1_X\nP1_Y"
            assert send_request(sock, "FETCH topic-g5 2 0") == "P2_M\nP2_N"

            # Produce post-restart and verify ordering
            assert send_request(sock, "PRODUCE topic-g5 0 P0_C") == "OK"
            assert send_request(sock, "FETCH topic-g5 0 0") == "P0_A\nP0_B\nP0_C"
            assert send_request(sock, "FETCH topic-g5 0 2") == "P0_C"
            print("  ✓ PASS: Post-restart recovery and sequence ordering verified across partitions")
    finally:
        stop_broker(broker)

    # Cleanup after successful tests
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    print("\n==================================================")
    print("   ALL STAGE 4 TEST GROUPS PASSED SUCCESSFULLY!   ")
    print("==================================================")
    return True


if __name__ == "__main__":
    if not os.path.exists(BROKER_BIN):
        subprocess.run(["g++", "-std=c++17", "-Wall", "-Wextra", "-o", "broker", "src/main.cpp", "-pthread"], check=True)
    success = run_stage4_tests()
    sys.exit(0 if success else 1)

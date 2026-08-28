#!/usr/bin/env python3
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


def run_verification():
    print("=== Mini-Stage 3.5 Restart Persistence Verification ===")

    # Step 1: Clean data directory
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    # Step 2: Start broker (empty directory scenario)
    print("[1/4] Starting broker with missing/empty data directory...")
    broker = start_broker()
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            assert send_request(sock, "PING") == "PONG"
            assert send_request(sock, "PRODUCE topic1 msg1") == "OK"
            assert send_request(sock, "PRODUCE topic1 msg2 with space") == "OK"
            assert send_request(sock, "PRODUCE topic2 topic2_first") == "OK"
            assert send_request(sock, "PRODUCE topic2 topic2_second") == "OK"
            
            res1 = send_request(sock, "FETCH topic1")
            assert res1 == "msg1\nmsg2 with space", f"Expected msg1\\nmsg2 with space, got: {repr(res1)}"
            
            res2 = send_request(sock, "FETCH topic2")
            assert res2 == "topic2_first\ntopic2_second", f"Expected topic2_first\\ntopic2_second, got: {repr(res2)}"
        print("  -> Phase 1 passed: PRODUCE and FETCH working on fresh broker.")
    finally:
        stop_broker(broker)

    time.sleep(0.2)

    # Step 3: Restart broker and verify recovery
    print("[2/4] Restarting broker with existing topic log files...")
    broker = start_broker()
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            assert send_request(sock, "PING") == "PONG"

            # Fetch recovered topic1
            res1 = send_request(sock, "FETCH topic1")
            assert res1 == "msg1\nmsg2 with space", f"Recovery topic1 failed, got: {repr(res1)}"

            # Fetch recovered topic2 (verify no topic log mixing)
            res2 = send_request(sock, "FETCH topic2")
            assert res2 == "topic2_first\ntopic2_second", f"Recovery topic2 failed, got: {repr(res2)}"

            # Produce additional message post-restart
            assert send_request(sock, "PRODUCE topic1 msg3") == "OK"
            res1_updated = send_request(sock, "FETCH topic1")
            assert res1_updated == "msg1\nmsg2 with space\nmsg3", f"Post-restart produce failed, got: {repr(res1_updated)}"
        print("  -> Phase 2 passed: Recovery verified across broker restart.")
    finally:
        stop_broker(broker)

    time.sleep(0.2)

    # Step 4: Restart broker again and verify append continuity
    print("[3/4] Restarting broker a second time to verify append continuity...")
    broker = start_broker()
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            res1_final = send_request(sock, "FETCH topic1")
            assert res1_final == "msg1\nmsg2 with space\nmsg3", f"Second recovery topic1 failed, got: {repr(res1_final)}"
        print("  -> Phase 3 passed: Second restart recovery verified.")
    finally:
        stop_broker(broker)

    # Step 5: Clean up data directory after test
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    print("[4/4] Cleanup complete.")
    print("\nSUCCESS: All restart persistence verification checks passed!")
    return True


if __name__ == "__main__":
    # Ensure broker executable is built
    subprocess.run(["g++", "-std=c++17", "-Wall", "-Wextra", "-o", "broker", "src/main.cpp", "-pthread"], check=True)
    success = run_verification()
    sys.exit(0 if success else 1)
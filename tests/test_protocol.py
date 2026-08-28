#!/usr/bin/env python3
"""
Test script for Mini-Stage 2.1 - Broker Protocol
Tests the parse_request() functionality via framed protocol.
"""

import socket
import struct
import sys
import time

HOST = "127.0.0.1"
PORT = 9092


def frame(payload):
    """Frame a message with a 4-byte length header."""
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    return struct.pack("!I", len(payload)) + payload


def send_and_receive(sock, message):
    """Send a framed message and receive the framed response."""
    sock.sendall(frame(message))
    
    # Read the 4-byte length header
    header = sock.recv(4)
    if len(header) < 4:
        raise Exception("Failed to read response header")
    
    response_len = struct.unpack("!I", header)[0]
    
    # Read the response payload
    response = b""
    while len(response) < response_len:
        chunk = sock.recv(response_len - len(response))
        if not chunk:
            raise Exception("Connection closed while reading response")
        response += chunk
    
    return response.decode('utf-8')


def test_protocol():
    """Test various protocol requests."""
    tests = [
        # (request, expected_response, description)
        ("PING", "PONG", "PING should return PONG"),
        ("PRODUCE orders hello", "OK", "PRODUCE with single-word payload"),
        ("PRODUCE orders hello world", "OK", "PRODUCE with multi-word payload"),
        ("FETCH orders", "OK", "FETCH with topic"),
        ("FETCH", "ERROR", "FETCH without topic should be invalid"),
        ("PRODUCE", "ERROR", "PRODUCE without topic should be invalid"),
        ("PRODUCE orders", "ERROR", "PRODUCE without payload should be invalid"),
        ("UNKNOWN cmd", "ERROR", "Unknown command should be invalid"),
    ]
    
    print("Connecting to broker at {}:{}".format(HOST, PORT))
    
    try:
        with socket.create_connection((HOST, PORT), timeout=5) as sock:
            print("Connected!\n")
            
            passed = 0
            failed = 0
            
            for request, expected, description in tests:
                try:
                    response = send_and_receive(sock, request)
                    
                    if response == expected:
                        print("✓ PASS: {}".format(description))
                        print("  Request:  '{}'".format(request))
                        print("  Response: '{}'".format(response))
                        passed += 1
                    else:
                        print("✗ FAIL: {}".format(description))
                        print("  Request:    '{}'".format(request))
                        print("  Expected:   '{}'".format(expected))
                        print("  Got:        '{}'".format(response))
                        failed += 1
                except Exception as e:
                    print("✗ ERROR: {}".format(description))
                    print("  Request: '{}'".format(request))
                    print("  Error: {}".format(e))
                    failed += 1
                
                print()
            
            print("=" * 50)
            print("Results: {} passed, {} failed".format(passed, failed))
            return failed == 0
            
    except socket.timeout:
        print("ERROR: Connection timed out. Is the broker running?")
        return False
    except ConnectionRefusedError:
        print("ERROR: Connection refused. Is the broker running on port {}?".format(PORT))
        return False
    except Exception as e:
        print("ERROR: {}".format(e))
        return False


if __name__ == "__main__":
    success = test_protocol()
    sys.exit(0 if success else 1)

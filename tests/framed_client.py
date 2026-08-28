import socket
import struct

HOST = "127.0.0.1"
PORT = 9092


def frame(payload):
    return struct.pack("!I", len(payload)) + payload


with socket.create_connection((HOST, PORT)) as sock:
    sock.sendall(frame(b"PING"))
    print("sent PING")

    # Deliberately close without reading the response.
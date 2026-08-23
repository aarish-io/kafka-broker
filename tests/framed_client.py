import socket
import struct

HOST = "127.0.0.1"
PORT = 9092

with socket.create_connection((HOST, PORT)) as sock:
    oversized_length = 64 * 1024 + 1

    header = struct.pack("!I", oversized_length)

    sock.sendall(header)
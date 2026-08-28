import socket
import struct


HOST = "127.0.0.1"
PORT = 9092


def recv_exactly(sock, n):
    data = b""

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            raise RuntimeError("broker disconnected")

        data += chunk

    return data


def send_request(sock, request):
    data = request.encode()

    sock.sendall(struct.pack("!I", len(data)))
    sock.sendall(data)

    header = recv_exactly(sock, 4)
    length = struct.unpack("!I", header)[0]

    return recv_exactly(sock, length).decode()


with socket.create_connection((HOST, PORT)) as sock:
    print("PING:", send_request(sock, "PING"))

    print(
        "PRODUCE 1:",
        send_request(sock, "PRODUCE orders hello")
    )

    print(
        "PRODUCE 2:",
        send_request(sock, "PRODUCE orders world")
    )

    print(
        "FETCH orders:",
        repr(send_request(sock, "FETCH orders"))
    )

    print(
        "FETCH unknown:",
        send_request(sock, "FETCH unknown")
    )
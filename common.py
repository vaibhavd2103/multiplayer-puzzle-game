
import json
import socket

# Shared constants
MULTICAST_GROUP = "224.1.1.1"
MULTICAST_PORT = 5007

# Default TCP port for game server
DEFAULT_SERVER_PORT = 6000

# Size of puzzle grid
GRID_SIZE = 4


def send_json(sock, obj):
    """
    Send a JSON object delimited by a newline.
    """
    data = json.dumps(obj).encode("utf-8") + b"\n"
    sock.sendall(data)


def recv_json(sock):
    """
    Receive a JSON object delimited by a newline.
    Returns None if connection is closed.
    """
    buffer = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return None
        buffer += chunk
        if b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            try:
                return json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                return None


def create_multicast_sender():
    """
    Create a UDP socket configured for multicast sending.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # TTL 1 -> stay within local network
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    return sock


def create_multicast_listener():
    """
    Create a UDP socket configured to listen to multicast group.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MULTICAST_PORT))

    mreq = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    return sock
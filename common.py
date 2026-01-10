import json
import socket
import struct
from unittest.mock import DEFAULT

GRID_SIZE = 4
DEFAULT_SERVER_PORT = 7001
DEFAULT_PORT = 6000

def send_json(sock, obj):
    data = json.dumps(obj).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)

def recv_json(sock):
    try:
        raw_len = sock.recv(4)
        if not raw_len:
            return None
        length = int.from_bytes(raw_len, "big")
        data = b""
        while len(data) < length:
            chunk = sock.recv(length - len(data))
            if not chunk:
                return None
            data += chunk
        return json.loads(data.decode())
    except:
        return None

def create_multicast_listener(group="224.1.1.1", port=5007):
    """
    Create a UDP socket to listen for multicast messages.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind to all interfaces on the given port
    sock.bind(('', port))

    # Join multicast group
    mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    return sock
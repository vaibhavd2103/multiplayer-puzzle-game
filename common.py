import json
import socket

GRID_SIZE = 4
DEFAULT_SERVER_PORT = 7001

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

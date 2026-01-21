
import json
import socket
import threading

# Shared constants
MULTICAST_GROUP = "224.1.1.1"
MULTICAST_PORT = 5007

# Default TCP port for game server
DEFAULT_SERVER_PORT = 6000

# Size of puzzle grid
GRID_SIZE = 4

PROTOCOL_VERSION = 1

ROLE_CLIENT = "client"
ROLE_BACKUP = "backup"

MSG_HELLO = "hello"
MSG_FULL_STATE = "full_state"
MSG_UPDATE = "update"
MSG_INFO = "info"
MSG_MOVE = "move"
MSG_HINT = "hint"
MSG_PRIMARY_ALIVE = "primary_alive"
MSG_BACKUP_MEMBERS = "backup_members"
MSG_NEW_PRIMARY = "new_primary"

ANNOUNCE_PREFIX = "primary_alive"


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

def get_local_ip():
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


class MessageConnection:

    def __init__(self, sock):
        self.sock = sock
        self._buf = b""
        self._send_lock = threading.Lock()
        self._recv_lock = threading.Lock()

    @classmethod
    def connect(cls, host, port, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
        except OSError:
            sock.close()
            raise
        sock.settimeout(None)
        return cls(sock)

    def send(self, obj):
        data = json.dumps(obj).encode("utf-8") + b"\n"
        with self._send_lock:
            self.sock.sendall(data)

    def recv(self, timeout=None):
        with self._recv_lock:
            while b"\n" not in self._buf:
                self.sock.settimeout(timeout)
                try:
                    chunk = self.sock.recv(4096)
                finally:
                    self.sock.settimeout(None)
                if not chunk:
                    return None
                self._buf += chunk
            line, self._buf = self._buf.split(b"\n", 1)
        try:
            return json.loads(line.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def messages(self):
        while True:
            msg = self.recv()
            if msg is None:
                return
            yield msg

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close() 
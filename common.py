import json
import socket

# Shared constants
MULTICAST_GROUP = "224.1.1.1"
MULTICAST_PORT = 5007
DISCOVERY_PREFIX = "primary_alive"
BROADCAST_ADDR = "255.255.255.255"

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


def create_broadcast_sender():
    """
    Create a UDP socket configured for broadcast sending.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    return sock


def get_local_ip():
    """
    Best-effort local IPv4 selection without external dependencies.
    """
    # Try common public resolver IPs to get the outbound interface IP.
    for probe_ip in ("8.8.8.8", "1.1.1.1"):
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.connect((probe_ip, 80))
            ip = probe.getsockname()[0]
            probe.close()
            if ip and not ip.startswith("127."):
                return ip
        except OSError:
            pass
    # Fallback: resolve hostname and pick first non-loopback IPv4.
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127."):
                return ip
    except OSError:
        pass
    return "127.0.0.1"


def get_broadcast_targets(local_ip):
    """
    Returns a list of broadcast targets to try.
    """
    targets = {BROADCAST_ADDR}
    # Guess /24 broadcast for common private ranges to improve LAN delivery.
    parts = local_ip.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        targets.add(".".join(parts[:3]) + ".255")
    return list(targets)


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


def create_discovery_listener():
    """
    Create a UDP socket configured to listen for discovery via multicast
    (when supported) or broadcast on the same port.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MULTICAST_PORT))
    try:
        mreq = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError:
        # Multicast join may fail on some networks/OSes; broadcast still works.
        pass
    return sock

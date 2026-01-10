import argparse
import socket
import threading
import time

from common import send_json, recv_json, create_multicast_listener, GRID_SIZE


class Client:
    def __init__(self, name, host=None, port=None):
        self.name = name
        self.server_addr = (host, port) if host and port else None
        self.tcp_sock = None
        self.running = True
        self.state = None
        self.connected = False

    # ---------------- DISCOVERY ----------------
    def discover_server(self):
        if self.server_addr:
            print(f"[CLIENT] Using specified server {self.server_addr}")
            return

        sock = create_multicast_listener()
        sock.settimeout(5)
        print("[CLIENT] Discovering primary via multicast...")
        try:
            while True:
                data, _ = sock.recvfrom(1024)
                msg = data.decode()
                parts = msg.split()
                if parts[0] == "PRIMARY_ALIVE":
                    self.server_addr = (parts[1], int(parts[2]))
                    print(f"[CLIENT] Found PRIMARY {self.server_addr}")
                    return
        except socket.timeout:
            print("[CLIENT] No primary found via multicast")

    # ---------------- CONNECT ----------------
    def connect(self):
        if not self.server_addr:
            return False
        print(f"[CLIENT] Attempting to connect to server at {self.server_addr}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(self.server_addr)
            print(f"[CLIENT] Successfully connected to server at {self.server_addr}")
        except Exception as e:
            print(f"[CLIENT] Connection failed: {e}")
            return False

        self.tcp_sock = sock
        send_json(sock, {"type": "hello", "role": "client", "name": self.name})
        first = recv_json(sock)
        if first and first.get("type") == "full_state":
            self.state = first.get("state")
            print("[CLIENT] Connected and received initial game state.")
            self.print_state("Welcome to the distributed puzzle game!")
            self.print_grid()
            threading.Thread(target=self._receiver_loop, daemon=True).start()
            return True
        else:
            print("[CLIENT] Failed to receive initial game state.")
            return False

    # ---------------- RECEIVER ----------------
    def _receiver(self):
        while self.running:
            try:
                msg = recv_json(self.tcp_sock)
                if msg is None:
                    raise ConnectionResetError
                self.state = msg.get("state", self.state)
                self.print_state(msg.get("message", ""))
                self.print_grid()
            except (OSError, ConnectionResetError):
                print("[CLIENT] Lost connection to server")
                self.connected = False
                try:
                    self.tcp_sock.close()
                except Exception:
                    pass
                self._reconnect()
                return

    # ---------------- RECONNECT ----------------
    def _reconnect(self):
        while self.running:
            print("[CLIENT] Reconnecting...")
            self.discover_server()
            if self.server_addr and self.connect():
                print("[CLIENT] Reconnected")
                return
            time.sleep(2)

    # ---------------- INPUT ----------------
    def input_loop(self):
        while self.running:
            if not self.connected:
                time.sleep(0.5)
                continue

            try:
                line = input("Enter move (row col value): ")
                r, c, v = map(int, line.split())
                send_json(self.tcp_sock, {
                    "type": "move",
                    "row": r,
                    "col": c,
                    "value": v
                })
            except Exception:
                print("[CLIENT] Invalid input or send failed")

    # ---------------- PRINT ----------------
    def print_state(self, message):
        if message:
            print("\n" + message)

    def print_grid(self):
        if not self.state:
            return
        print("\nCurrent Puzzle Grid:")
        for row in self.state["grid"]:
            print(" ".join(str(cell) if cell != 0 else "." for cell in row))
        print(f"Scores: {self.state.get('scores', {})}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--host", help="Optional server host (NGINX)")
    parser.add_argument("--port", type=int, help="Optional server port (NGINX)")
    args = parser.parse_args()
    client = Client(args.name, args.host, args.port)
    
    if args.host and args.port:
        client.server_addr = (args.host, args.port)
        print(f"[CLIENT] Using specified server {client.server_addr}")
        if client.connect():
            client.input_loop()
    else:
        client.discover_server()
        if client.connect():
            client.input_loop()

if __name__ == "__main__":
    main()
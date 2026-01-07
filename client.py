import argparse
import socket
import threading
import time

from common import (
    MULTICAST_GROUP,
    MULTICAST_PORT,
    send_json,
    recv_json,
    create_multicast_listener,
)

class Client:
    def __init__(self, name):
        self.name = name
        self.server_addr = None
        self.tcp_sock = None
        self.running = True
        self.state = None
        self.connected = False

    # ---------------- DISCOVERY ----------------
    def discover_server(self):
        sock = create_multicast_listener()
        sock.settimeout(5)
        print("[CLIENT] Discovering primary...")
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
            print("[CLIENT] No primary found")

    # ---------------- CONNECT ----------------
    def connect(self):
        try:
            self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_sock.connect(self.server_addr)
            send_json(self.tcp_sock, {
                "type": "hello",
                "role": "client",
                "name": self.name
            })

            first = recv_json(self.tcp_sock)
            self.state = first["state"]
            self.connected = True
            print("[CLIENT] Connected")

            threading.Thread(target=self._receiver, daemon=True).start()
            return True
        except OSError:
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
            except (OSError, ConnectionResetError):
                print("[CLIENT] Lost connection to server")
                self.connected = False
                self.tcp_sock.close()
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
                pass

    def print_state(self, message):
        if message:
            print("\n" + message)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    client = Client(args.name)
    client.discover_server()
    if client.connect():
        client.input_loop()

if __name__ == "__main__":
    main()

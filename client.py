
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

    def discover_server(self):
        """
        Listen to multicast for a short time until a primary announces itself.
        """
        sock = create_multicast_listener()
        sock.settimeout(10)
        print("[CLIENT] Waiting for server multicast announcement...")
        try:
            while True:
                data, addr = sock.recvfrom(1024)
                msg = data.decode("utf-8")
                parts = msg.split()
                if len(parts) == 3 and parts[0] == "PRIMARY_ALIVE":
                    host = parts[1]
                    port = int(parts[2])
                    self.server_addr = (host, port)
                    print(f"[CLIENT] Discovered server at {host}:{port}")
                    break
        except socket.timeout:
            print("[CLIENT] No server announcement received. Is the server running?")

    def connect(self):
        if self.server_addr is None:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_addr)
        self.tcp_sock = sock
        send_json(sock, {"type": "hello", "role": "client", "name": self.name})
        first = recv_json(sock)
        if first and first.get("type") == "full_state":
            self.state = first.get("state")
            print("[CLIENT] Connected and received initial game state.")
            self.print_state("Welcome to the distributed puzzle game!")
        threading.Thread(target=self._receiver_loop, daemon=True).start()
        return True

    def _receiver_loop(self):
        while self.running:
            msg = recv_json(self.tcp_sock)
            if msg is None:
                print("[CLIENT] Disconnected from server.")
                self.running = False
                break
            t = msg.get("type")
            if t in ("update", "full_state"):
                self.state = msg.get("state")
                self.print_state(msg.get("message", ""))
            elif t == "info":
                print(f"[INFO] {msg.get('message')}")

    def print_state(self, message=""):
        if self.state is None:
            return
        print("\n" + "=" * 40)
        if message:
            print(message)
        print(f"Round: {self.state.get('round')}")
        print("Current board (0 = empty):")
        grid = self.state.get("grid", [])
        for row in grid:
            print(" ".join(str(x) for x in row))
        print("\nScores:")
        scores = self.state.get("scores", {})
        for name, score in scores.items():
            print(f"  {name}: {score}")
        print("=" * 40 + "\n")

    def input_loop(self):
        while self.running:
            try:
                line = input("Enter move as row col value (e.g. 0 1 5), or 'quit': ")
            except EOFError:
                break
            if line.strip().lower() == "quit":
                self.running = False
                break
            parts = line.strip().split()
            if len(parts) != 3:
                print("Invalid format.")
                continue
            try:
                row = int(parts[0])
                col = int(parts[1])
                value = int(parts[2])
            except ValueError:
                print("Row, col, and value must be integers.")
                continue
            if self.tcp_sock:
                send_json(
                    self.tcp_sock,
                    {"type": "move", "row": row, "col": col, "value": value},
                )

        try:
            if self.tcp_sock:
                self.tcp_sock.close()
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description="Distributed Puzzle Game Client")
    parser.add_argument("--name", required=True, help="Player name")
    args = parser.parse_args()

    client = Client(args.name)
    client.discover_server()
    if client.server_addr is None:
        return
    if client.connect():
        client.input_loop()


if __name__ == "__main__":
    main()

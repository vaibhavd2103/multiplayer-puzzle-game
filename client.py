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
    def __init__(self, name, host=None, port=None):
        self.name = name
        self.server_addr = (host, port) if host and port else None
        self.tcp_sock = None
        self.running = True
        self.connected = False #to keep both the server in sync 
        self.state = None
        self.print_lock = threading.Lock()

    def connect(self):
        if not self.server_addr:
            return False
        print(f"[CLIENT] Attempting to connect to server at {self.server_addr}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect(self.server_addr)
            print(f"[CLIENT] Successfully connected to server at {self.server_addr}")
        except OSError as e:
            print(f"[CLIENT] Connection failed: {e}")
            sock.close()
            return False
        sock.settimeout(None)
        self.tcp_sock = sock
        send_json(sock, {"type": "hello", "role": "client", "name": self.name})
        first = recv_json(sock)
        if first and first.get("type") == "full_state":
            self.state = first.get("state")
            self.connected = True
            print("[CLIENT] Connected and received initial game state.")
            self.print_state("Welcome to the distributed puzzle game!")
            threading.Thread(target=self._receiver_loop, daemon=True).start()
            return True
        else:
            print("[CLIENT] Failed to receive initial game state.")
            sock.close()
            self.connected = False
            return False

    def discover_server(self):
        sock = create_multicast_listener()
        sock.settimeout(10)
        print("[CLIENT] Waiting for server multicast announcement...")
        try:
            while True:
                data, _ = sock.recvfrom(1024)
                msg = data.decode("utf-8")
                parts = msg.split()
                if len(parts) == 3 and parts[0] == "PRIMARY_ALIVE":
                    host = parts[1]
                    port = int(parts[2])
                    self.server_addr = (host, port)
                    print(f"[CLIENT] Discovered server at {host}:{port}")
                    break
        except socket.timeout:
            print("[CLIENT] No server announcement received.")

    def _receiver_loop(self):
        while self.running:
            try:
                msg = recv_json(self.tcp_sock)
            except (OSError, ConnectionResetError):
                msg = None

            if msg is None:
                self.connected = False
                with self.print_lock:
                    print("\n[CLIENT] Lost connection to server")
                try:
                    self.tcp_sock.close()
                except OSError:
                    pass
                self._reconnect()
                return

            t = msg.get("type")
            if t in ("full_state", "update"):
                self.state = msg.get("state", self.state)
                # Only reprint the board for the initial state or real news
                if msg.get("message") or t == "full_state":
                    self.print_state(msg.get("message", ""))
            elif t == "info":
                with self.print_lock:
                    print(f"\n[INFO] {msg.get('message')}")
                    self._print_prompt()

    def print_state(self, message=""):
        with self.print_lock:
            if not self.state:
                if message:
                    print(f"\n{message}")
                self._print_prompt()
                return
            print("\n" + "=" * 40)
            if message:
                print(message)
            print(f"Round: {self.state.get('round')}")
            print("Current board (0 = empty):")
            for row in self.state.get("grid", []):
                print(" ".join(str(cell) if cell != 0 else "." for cell in row))
            print("\nScores:")
            for name, score in self.state.get("scores", {}).items():
                print(f"  {name}: {score}")
            print("=" * 40)
            self._print_prompt()

    def _print_prompt(self):
        print(
            "Enter move as row col value (e.g. 0 1 5), or 'quit': ",
            end="",
            flush=True,
        )

    def input_loop(self):
        while self.running:
            try:
                line = input()
            except EOFError:
                break
            except KeyboardInterrupt:
                print("\n[CLIENT] Interrupted, shutting down.")
                self.running = False
                break

            if line.strip().lower() == "quit":
                self.running = False
                break

            parts = line.strip().split()
            if len(parts) != 3:
                with self.print_lock:
                    print("Invalid format.")
                    self._print_prompt()
                continue

            try:
                row, col, value = map(int, parts)
            except ValueError:
                with self.print_lock:
                    print("Row, col, and value must be integers.")
                    self._print_prompt()
                continue
            try:
                send_json(
                self.tcp_sock,
                {"type": "move", "row": row, "col": col, "value": value},
            )
            except (ConnectionResetError, OSError):
                print("[CLIENT] Cannot send, server disconnected.")
                self.running = False
                break
        try:
            self.tcp_sock.close()
        except OSError:
            pass

    def _reconnect(self):
        while self.running:
            print("[CLIENT] Primary server down. Retrying connection...")
            self.discover_server()
            if self.server_addr and self.connect():
                print("[CLIENT] Reconnected to new primary server")
                return
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description="Distributed Puzzle Game Client")
    parser.add_argument("--name", required=True)
    parser.add_argument("--host", help="Optional server host")
    parser.add_argument("--port", type=int, help="Optional server port")
    args = parser.parse_args()

    client = Client(args.name, args.host, args.port)

    if args.host and args.port:
        print(f"[CLIENT] Using specified server {args.host}:{args.port}")
        if client.connect():
            client.input_loop()
    else:
        while True:
            client.discover_server()
            if client.server_addr is None:
                time.sleep(2)
                continue
            if client.connect():
                client.input_loop()
            print("[CLIENT] Disconnected. Retrying in 2 seconds...")
            time.sleep(2)


if __name__ == "__main__":
    main()
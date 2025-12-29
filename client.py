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
        self.print_lock = threading.Lock()   # ✅ NEW

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

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_addr)
        self.tcp_sock = sock
        send_json(sock, {"type": "hello", "role": "client", "name": self.name})

        first = recv_json(sock)
        if first and first.get("type") == "full_state":
            self.state = first.get("state")
            self.print_state("Welcome to the distributed puzzle game!")

        threading.Thread(target=self._receiver_loop, daemon=True).start()
        return True

    def _receiver_loop(self):
        while self.running:
            # msg = recv_json(self.tcp_sock)
            try:
                msg = recv_json(self.tcp_sock)
            except (ConnectionResetError, OSError):
                print("[CLIENT] Lost connection to server.")
                self.running = False
                break
            if msg is None:
                with self.print_lock:
                    print("\n[CLIENT] Disconnected from server.")
                self.running = False
                break

            t = msg.get("type")

            if t in ("update", "full_state"):
                self.state = msg.get("state")
                self.print_state(msg.get("message", ""))

            elif t == "info":
                with self.print_lock:
                    print(f"\n[INFO] {msg.get('message')}")
                    self._print_prompt()

    def print_state(self, message=""):
        with self.print_lock:
            print("\n" + "=" * 40)
            if message:
                print(message)
            print(f"Round: {self.state.get('round')}")
            print("Current board (0 = empty):")
            for row in self.state.get("grid", []):
                print(" ".join(str(x) for x in row))
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

            # send_json(
            #     self.tcp_sock,
            #     {"type": "move", "row": row, "col": col, "value": value},
            # )
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

    def reconnect(self):
        print("[CLIENT] Attempting to reconnect to new primary...")
        self.tcp_sock = None
        self.server_addr = None
        self.running = True
        self.discover_server()
        if self.server_addr:
            return self.connect()
        return False


def main():
    parser = argparse.ArgumentParser(description="Distributed Puzzle Game Client")
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    client = Client(args.name)
    # client.discover_server()
    # if client.server_addr and client.connect():
    #     client.input_loop()
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

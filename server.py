import argparse
import random
import socket
import threading
import time

from common import (
    GRID_SIZE,
    DEFAULT_SERVER_PORT,
    MULTICAST_GROUP,
    MULTICAST_PORT,
    send_json,
    recv_json,
    create_multicast_sender,
    create_multicast_listener,
)


class GameState:
    HINT_PENALTY = 2

    def __init__(self):
        self.lock = threading.Lock()
        self.round = 1
        self.solution = self._generate_solution()
        self.grid = self._generate_puzzle_from_solution()
        self.scores = {}  # name -> int

    def _generate_solution(self):
        # Simple solution grid with numbers 1..9
        return [
            [random.randint(1, 9) for _ in range(GRID_SIZE)]
            for _ in range(GRID_SIZE)
        ]

    def _generate_puzzle_from_solution(self):
        # Copy solution and blank out around 40% of cells
        grid = [row[:] for row in self.solution]
        for r in range(GRID_SIZE):
            for c in range(GRID_SIZE):
                if random.random() < 0.4:
                    grid[r][c] = 0
        return grid

    def _board_full(self):
        return all(
            self.grid[r][c] != 0
            for r in range(GRID_SIZE)
            for c in range(GRID_SIZE)
        )

    def _start_new_round(self):
        self.round += 1
        self.solution = self._generate_solution()
        self.grid = self._generate_puzzle_from_solution()

    def as_dict(self):
        return {
            "round": self.round,
            "grid": self.grid,
            "scores": self.scores,
        }

    def apply_move(self, player, row, col, value):
        """
        Returns a tuple (valid, message).
        """
        with self.lock:
            if player not in self.scores:
                self.scores[player] = 0

            if not (0 <= row < GRID_SIZE and 0 <= col < GRID_SIZE):
                return False, "Cell out of range."

            if self.grid[row][col] != 0:
                return False, "Cell already filled."

            if self.solution[row][col] == value:
                self.grid[row][col] = value
                self.scores[player] += 1
                msg = f"{player} placed {value} at ({row}, {col}) - correct!"
                if self._board_full():
                    self._start_new_round()
                    msg += " Board complete! New round started."
                return True, msg
            else:
                # Incorrect move
                self.scores[player] -= 1
                msg = f"{player} placed {value} at ({row}, {col}) - incorrect."
                return False, msg

class PrimaryServer:
    def __init__(self, host="0.0.0.0", port=DEFAULT_SERVER_PORT):
        self.host = host
        self.port = port
        self.game = GameState()

        self.clients = {}  # socket -> name
        # FIX: use dict instead of set; store metadata (e.g., id) per backup.
        # backups: socket -> {"id": int}
        self.backups = {}
        self.running = True

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((self.host, self.port))
        self.tcp_sock.listen(5)

        self.multicast_sender = create_multicast_sender()

    def _broadcast_primary_status(self):
        """
        Broadcast the primary server's status to all backup servers over TCP.

        Backups use this as a heartbeat: whenever they see "primary_alive",
        they reset their last_heartbeat timestamp.
        """
        while self.running:
            time.sleep(2)
            for sock in list(self.backups):
                try:
                    send_json(sock, {"type": "primary_alive"})
                except Exception:
                    # If sending fails, remove dead backup from dictionary.
                    self.backups.pop(sock, None)

    def _advertised_host(self):
    
        if self.host not in ("0.0.0.0", "", None):
            return self.host
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
        except OSError:
            return "127.0.0.1"
        finally:
            probe.close()

    def _announce_presence_loop(self):
    
        host = self._advertised_host()
        print(f"[PRIMARY] Announcing presence as {host}:{self.port}")
        while self.running:
            message = f"primary_alive {host} {self.port}"
            try:
                self.multicast_sender.sendto(
                    message.encode("utf-8"),
                    (MULTICAST_GROUP, MULTICAST_PORT),
                )
            except OSError:
                pass
            time.sleep(2)

    def start(self):
        print(f"[PRIMARY] Listening on {self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(
            target=self._broadcast_primary_status, daemon=True
        ).start()
        threading.Thread(
            target=self._broadcast_backups, daemon=True
        ).start()
        threading.Thread(
            target=self._announce_presence_loop, daemon=True
        ).start()
        while self.running:
            time.sleep(1)

    def _broadcast(self, obj):
        # Broadcast to clients
        for c in list(self.clients):
            try:
                send_json(c, obj)
            except Exception:
                self.clients.pop(c, None)

        # Broadcast to backups
        for b in list(self.backups):
            try:
                send_json(b, obj)
            except Exception:
                self.backups.pop(b, None)

    def _broadcast_backups(self):
        """
        Periodically send the list of backup ids to all backups.

        This allows backups to know all participants and apply
        the "highest id wins" election rule.
        """
        while self.running:
            time.sleep(1)
            # members is a list of backup ids
            members = [info["id"] for info in self.backups.values()]
            for sock in list(self.backups):
                try:
                    send_json(
                        sock,
                        {
                            "type": "backup_members",
                            "members": members,
                        },
                    )
                except Exception:
                    self.backups.pop(sock, None)

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.tcp_sock.accept()
                print(f"[PRIMARY] New TCP connection from {addr}")
                threading.Thread(
                    target=self._handle_conn, args=(conn, addr), daemon=True
                ).start()
            except OSError:
                break

    def _handle_conn(self, conn, addr):
        print(f"[PRIMARY] New TCP connection from {addr}")
        hello = recv_json(conn)
        print(f"[PRIMARY] Received message: {hello}")
        if not hello or hello.get("type") != "hello":
            print(f"[PRIMARY] Invalid hello message: {hello}")
            conn.close()
            return

        role = hello.get("role")
        if role == "client":
            name = hello.get("name", f"client_{addr[0]}")
            self.clients[conn] = name
            print(f"[PRIMARY] Client '{name}' connected from {addr}")
            # Add player to score table if missing
            with self.game.lock:
                if name not in self.game.scores:
                    self.game.scores[name] = 0
            # Send full game state
            send_json(conn, {"type": "full_state", "state": self.game.as_dict()})
            # Notify others (but not the joining player)
            self.broadcast(
                {
                    "type": "info",
                    "message": f"{name} joined the game.",
                },
                exclude_conn=conn,  # Don't send to the joining player
            )
            self._client_loop(conn, name)

        elif role == "backup":
            # NEW: read backup id from hello message
            backup_id = hello.get("id")
            if backup_id is None:
                # fallback: generate an id if not provided
                backup_id = random.randint(1, 1_000_000)
            self.backups[conn] = {"id": backup_id}
            print(f"[PRIMARY] Backup {backup_id} connected from {addr}")
            print(
                f"[PRIMARY] Current backups: "
                f"{[info['id'] for info in self.backups.values()]}"
            )
            print(f"[PRIMARY] Sending full state to backup from {addr}")
            # Send full state immediately
            send_json(conn, {"type": "full_state", "state": self.game.as_dict()})
            self._backup_loop(conn)
        else:
            print(f"[PRIMARY] Unknown role: {role}")
            conn.close()

    def broadcast(self, obj, exclude_conn=None):
        # Send to all clients except excluded one
        dead = []
        for sock in self.clients:
            if sock == exclude_conn:
                continue
            try:
                send_json(sock, obj)
            except OSError:
                dead.append(sock)
        for sock in dead:
            name = self.clients.get(sock, "?")
            print(f"[PRIMARY] Removing dead client {name}")
            del self.clients[sock]

        # Send to all backups
        dead = []
        for sock in self.backups:
            try:
                send_json(sock, obj)
            except OSError:
                dead.append(sock)
        for sock in dead:
            print("[PRIMARY] Removing dead backup")
            self.backups.pop(sock, None)

    def _client_loop(self, conn, name):
        try:
            while self.running:
                msg = recv_json(conn)
                if msg is None:
                    break

                mtype = msg.get("type")
                if mtype == "move":
                    _, text = self.game.apply_move(
                        name, msg.get("row"), msg.get("col"), msg.get("value")
                    )
                else:
                    continue

                update = {
                    "type": "update",
                    "state": self.game.as_dict(),
                    "message": text,
                }
                self.broadcast(update)
                self.send_to_backups(update)
        except (ConnectionResetError, OSError):
            pass
        finally:
            print(f"[PRIMARY] Client {name} disconnected")
            if conn in self.clients:
                del self.clients[conn]
            conn.close()
            self.broadcast(
                {
                    "type": "info",
                    "message": f"{name} left the game.",
                }
            )

    def _backup_loop(self, conn):
        """
        Primary currently does not expect messages from backups.
        This loop just waits until the connection is closed and
        then removes the backup.
        """
        try:
            while self.running:
                msg = recv_json(conn)
                if msg is None:
                    break
        except (ConnectionResetError, OSError):
            pass
        finally:
            print("[PRIMARY] Backup disconnected")
            if conn in self.backups:
                self.backups.pop(conn, None)
            conn.close()

    def send_to_backups(self, obj):
        dead = []
        for sock in self.backups:
            try:
                send_json(sock, obj)
            except OSError:
                dead.append(sock)
        for sock in dead:
            print("[PRIMARY] Removing dead backup")
            self.backups.pop(sock, None)

class BackupServer:
    def __init__(self, primary_host=None, primary_port=None, backup_id=None):
        self.running = True
        self.game_state = None
        self.primary_addr = (
            (primary_host, primary_port)
            if primary_host and primary_port
            else None
        )
        self.last_heartbeat = time.time()
        self.mode = "backup"  # or "primary"
        self.tcp_conn = None

        # Each backup has a unique id for election.
        # Caller can pass one in; otherwise a random id is chosen.
        self.backup_id = (
            backup_id if backup_id is not None else random.randint(1, 1_000_000)
        )
        # Set of known backup ids (including self).
        self.known_backups = {self.backup_id}

    def _connect_to_primary(self):
        # Keep trying to connect while we are a backup
        while self.running and self.mode == "backup":
            if self.primary_addr is None:
                # No primary known yet; wait and let multicast discovery fill it
                time.sleep(2)
                continue

            host, port = self.primary_addr
            try:
                print(f"[BACKUP {self.backup_id}] Connecting to primary {host}:{port}")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                self.tcp_conn = sock

                # Send hello including our backup id so primary can track us
                send_json(sock, {
                    "type": "hello",
                    "role": "backup",
                    "id": self.backup_id,
                })

                # Receive full state
                first = recv_json(sock)
                if first and first.get("type") == "full_state":
                    self.game_state = first.get("state")
                    print("[BACKUP] Initial state received from primary")

                # Then read updates & heartbeats
                while self.running and self.mode == "backup":
                    msg = recv_json(sock)
                    print(f"[BACKUP {self.backup_id}] Received message: {msg}")
                    if msg is None:
                        break

                    msg_type = msg.get("type")
                    
                    if msg_type == "new_primary":
                        new_host = msg["host"]
                        new_port = msg["port"]
                        new_id = msg["id"]

                        print(f"[BACKUP {self.backup_id}] New primary elected: {new_id}")

                        self.primary_addr = (new_host, new_port)
                        self.last_heartbeat = time.time()

                        # Force reconnect to new primary
                        break

                    if msg_type == "primary_alive":
                        self.last_heartbeat = time.time()
                        continue

                    if msg_type == "backup_members":
                        members = msg.get("members", [])
                        self.known_backups = set(members) | {self.backup_id}
                        print(f"[BACKUP {self.backup_id}] Known backups: {sorted(self.known_backups)}")
                        continue

                    if msg_type == "update":
                        self.game_state = msg.get("state")
                        print(f"[BACKUP] Game state updated: {self.game_state}")

            except OSError as e:
                print(f"[BACKUP {self.backup_id}] Error connecting to primary: {e}")
            finally:
                print("[BACKUP] Lost connection to primary TCP")
                try:
                    sock.close()
                except Exception:
                    pass

            # Wait before retrying
            time.sleep(2)

    def _listen_multicast_loop(self):
        """
        If primary_host/port are given, skip multicast discovery and
        connect directly. Otherwise, listen for multicast "primary_alive"
        messages to discover the primary.
        """
        if self.primary_addr:
            print(
                f"[BACKUP {self.backup_id}] Using specified primary at "
                f"{self.primary_addr[0]}:{self.primary_addr[1]}"
            )
            threading.Thread(
                target=self._connect_to_primary, daemon=True
            ).start()
            return

        sock = create_multicast_listener()
        while self.running and self.mode == "backup":
            try:
                data, _ = sock.recvfrom(1024)
            except OSError:
                break
            msg = data.decode("utf-8")
            parts = msg.split()
            if len(parts) == 3 and parts[0] == "primary_alive":
                host = parts[1]
                port = int(parts[2])
                self.last_heartbeat = time.time()
                if self.primary_addr is None:
                    self.primary_addr = (host, port)
                    print(
                        f"[BACKUP {self.backup_id}] "
                        f"Discovered primary at {host}:{port}"
                    )
                    threading.Thread(
                        target=self._connect_to_primary, daemon=True
                    ).start()

    def start(self):
        print(f"[BACKUP {self.backup_id}] Starting in backup mode")
        threading.Thread(
            target=self._listen_multicast_loop, daemon=True
        ).start()
        threading.Thread(
            target=self._monitor_primary_loop, daemon=True
        ).start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping backup server...")
            self.running = False

    def _monitor_primary_loop(self):
        """
        If primary heartbeat is missing for more than 6 seconds,
        an election is run.

        Election rule:
        - Only the backup with the highest id in known_backups
          promotes itself to primary.
        """
        while self.running:
            time.sleep(2)
            if self.mode != "backup":
                continue
            if time.time() - self.last_heartbeat <= 6:
                continue

            print(
                f"[BACKUP {self.backup_id}] Primary heartbeat lost, "
                "checking for other backups..."
            )
            # Add small random delay to reduce simultaneous decisions
            time.sleep(random.uniform(0.5, 2.0))

            # Double-check timeout after delay
            if self.mode != "backup":
                continue
            if time.time() - self.last_heartbeat <= 6:
                continue

            # Election: only highest id may promote
            highest_id = (
                max(self.known_backups) if self.known_backups else self.backup_id
            )
            if self.backup_id != highest_id:
                print(f"[BACKUP {self.backup_id}] Waiting for elected primary {highest_id}")
                continue

             # CRITICAL FIX: Remove OUR OWN ID from known backups before promoting
            self.known_backups.discard(self.backup_id)
            print(f"[ELECTED PRIMARY {self.backup_id}] Removed self from backups: {sorted(self.known_backups)}")
            print(f"[BACKUP {self.backup_id}] Promoting to PRIMARY")
            self.mode = "primary"
            host, port = self.primary_addr

            primary = PrimaryServer(host, port)

            if self.game_state is not None:
                primary.game.round = self.game_state.get("round", 1)
                primary.game.grid = self.game_state.get("grid", primary.game.grid)
                primary.game.scores = self.game_state.get("scores", {})

            # IMPORTANT: notify other backups BEFORE starting
            for backup_id in self.known_backups:
                try:
                    send_json(self.tcp_conn, {
                        "type": "new_primary",
                        "host": host,
                        "port": port,
                        "id": self.backup_id
                    })
                except Exception:
                    pass

            primary.start()
            self.running = False
            return
            # self.mode = "primary"
            # # Start a new PrimaryServer using current game state
            # primary = PrimaryServer(self.primary_addr[0], self.primary_addr[1])
            # if self.game_state is not None:
            #     primary.game.round = self.game_state.get("round", 1)
            #     primary.game.grid = self.game_state.get(
            #         "grid", primary.game.grid
            #     )
            #     primary.game.scores = self.game_state.get("scores", {})
            # primary.start()
            # break


def main():
    parser = argparse.ArgumentParser(
        description="Distributed Puzzle Game Server"
    )
    parser.add_argument(
        "--role",
        choices=["primary", "backup"],
        required=True,
        help="Start as primary or backup server",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to bind (primary)")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_SERVER_PORT, help="TCP port for primary"
    )
    parser.add_argument("--primary-host", help="Primary server host (backup)")
    parser.add_argument("--primary-port", type=int, help="Primary server port (backup)")
    # Optional: allow passing explicit backup id from CLI
    parser.add_argument(
        "--backup-id",
        type=int,
        help="Explicit id for this backup (larger id wins election)",
    )
    args = parser.parse_args()

    if args.role == "primary":
        srv = PrimaryServer(host=args.host, port=args.port)
        srv.start()
    else:
        b = BackupServer(
            primary_host=args.primary_host,
            primary_port=args.primary_port,
            backup_id=args.backup_id,
        )
        b.start()


if __name__ == "__main__":
    main()

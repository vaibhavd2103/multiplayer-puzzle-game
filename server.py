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
    PROTOCOL_VERSION,
    ROLE_CLIENT,
    ROLE_BACKUP,
    MSG_HELLO,
    MSG_FULL_STATE,
    MSG_UPDATE,
    MSG_INFO,
    MSG_MOVE,
    MSG_HINT,
    MSG_PRIMARY_ALIVE,
    MSG_BACKUP_MEMBERS,
    MSG_NEW_PRIMARY,
    ANNOUNCE_PREFIX,
    MessageConnection,
    create_multicast_sender,
    create_multicast_listener,
    get_local_ip,
)

HEARTBEAT_INTERVAL = 2.0   # seconds between heartbeats / announcements
HEARTBEAT_TIMEOUT = 6.0    # missing heartbeats for this long -> election


class GameState:
    HINT_PENALTY = 2

    def __init__(self):
        self.lock = threading.Lock()
        self.round = 1
        self.solution = self._generate_solution()
        self.grid = self._generate_puzzle_from_solution()
        self.scores = {}  # name -> int

    def _generate_solution(self):
        return [
            [random.randint(1, 9) for _ in range(GRID_SIZE)]
            for _ in range(GRID_SIZE)
        ]

    def _generate_puzzle_from_solution(self):
        grid = [row[:] for row in self.solution]
        cells = [(r, c) for r in range(GRID_SIZE) for c in range(GRID_SIZE)]
        for r, c in cells:
            if random.random() < 0.4:
                grid[r][c] = 0
        if all(grid[r][c] != 0 for r, c in cells):
            r, c = random.choice(cells)
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
        """Return (ok, message)."""
        with self.lock:
            self.scores.setdefault(player, 0)

            if not all(type(v) is int for v in (row, col, value)):
                return False, "Invalid move: row, col and value must be integers."
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

            self.scores[player] -= 1
            return False, f"{player} placed {value} at ({row}, {col}) - incorrect."

    def reveal_hint(self, player):
        with self.lock:
            self.scores.setdefault(player, 0)
            empty = [
                (r, c)
                for r in range(GRID_SIZE)
                for c in range(GRID_SIZE)
                if self.grid[r][c] == 0
            ]
            if not empty:
                return False, "No empty cells left to reveal."

            row, col = random.choice(empty)
            value = self.solution[row][col]
            self.grid[row][col] = value
            self.scores[player] -= self.HINT_PENALTY
            msg = (
                f"{player} used a hint: ({row}, {col}) = {value} "
                f"(-{self.HINT_PENALTY} points)"
            )
            if self._board_full():
                self._start_new_round()
                msg += " Board complete! New round started."
            return True, msg


class PrimaryServer:
    def __init__(self, host="0.0.0.0", port=DEFAULT_SERVER_PORT):
        self.host = host
        self.port = port
        self.game = GameState()

        self.clients = {}
        self.backups = {}
        self.running = True

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((self.host, self.port))
        self.tcp_sock.listen(5)

        self.multicast_sender = create_multicast_sender()

    def start(self):
        print(f"[PRIMARY] Listening on {self.host}:{self.port}")
        for target in (
            self._accept_loop,
            self._heartbeat_loop,
            self._backup_members_loop,
            self._announce_loop,
        ):
            threading.Thread(target=target, daemon=True).start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[PRIMARY] Shutting down.")
            self.stop()

    def stop(self):
        self.running = False
        try:
            self.tcp_sock.close()
        except OSError:
            pass


    def _announce_loop(self):
        host = self.host if self.host not in ("0.0.0.0", "", None) else get_local_ip()
        print(f"[PRIMARY] Announcing as {host}:{self.port}")
        payload = f"{ANNOUNCE_PREFIX} {host} {self.port}".encode("utf-8")
        while self.running:
            try:
                self.multicast_sender.sendto(
                    payload, (MULTICAST_GROUP, MULTICAST_PORT)
                )
            except OSError:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            for conn in list(self.backups):
                if not self._safe_send(conn, {"type": MSG_PRIMARY_ALIVE}):
                    self._drop_backup(conn)

    def _backup_members_loop(self):
        while self.running:
            time.sleep(1)
            snapshot = list(self.backups.items())
            members = [info["id"] for _, info in snapshot]
            for conn, _ in snapshot:
                if not self._safe_send(
                    conn, {"type": MSG_BACKUP_MEMBERS, "members": members}
                ):
                    self._drop_backup(conn)

    def _safe_send(self, conn, obj):
        try:
            conn.send(obj)
            return True
        except OSError:
            return False

    def broadcast(self, obj, exclude=None):
        for conn in list(self.clients):
            if conn is exclude:
                continue
            if not self._safe_send(conn, obj):
                self._drop_client(conn)
        for conn in list(self.backups):
            if not self._safe_send(conn, obj):
                self._drop_backup(conn)

    def _drop_client(self, conn):
        name = self.clients.pop(conn, None)
        if name is not None:
            print(f"[PRIMARY] Removed client {name}")
        conn.close()

    def _drop_backup(self, conn):
        if self.backups.pop(conn, None) is not None:
            print("[PRIMARY] Removed backup")
        conn.close()

    def _accept_loop(self):
        while self.running:
            try:
                sock, addr = self.tcp_sock.accept()
            except OSError:
                break
            threading.Thread(
                target=self._handle_conn, args=(sock, addr), daemon=True
            ).start()

    def _handle_conn(self, sock, addr):
        print(f"[PRIMARY] New connection from {addr}")
        conn = MessageConnection(sock)
        try:
            hello = conn.recv(timeout=10)
        except OSError:
            hello = None
        if not hello or hello.get("type") != MSG_HELLO:
            print(f"[PRIMARY] Bad hello from {addr}: {hello}")
            conn.close()
            return

        role = hello.get("role")
        if role == ROLE_CLIENT:
            self._serve_client(conn, addr, hello)
        elif role == ROLE_BACKUP:
            self._serve_backup(conn, addr, hello)
        else:
            print(f"[PRIMARY] Unknown role from {addr}: {role}")
            conn.close()

    def _serve_client(self, conn, addr, hello):
        name = hello.get("name") or f"client_{addr[0]}"
        with self.game.lock:
            self.game.scores.setdefault(name, 0)
        if not self._safe_send(
            conn, {"type": MSG_FULL_STATE, "state": self.game.as_dict()}
        ):
            conn.close()
            return
        self.clients[conn] = name
        print(f"[PRIMARY] Client '{name}' joined from {addr}")
        self.broadcast(
            {"type": MSG_INFO, "message": f"{name} joined the game."},
            exclude=conn,
        )
        self._client_loop(conn, name)

    def _serve_backup(self, conn, addr, hello):
        bid = hello.get("id")
        if not isinstance(bid, int):
            bid = random.randint(1, 1_000_000)
        if not self._safe_send(
            conn, {"type": MSG_FULL_STATE, "state": self.game.as_dict()}
        ):
            conn.close()
            return
        self.backups[conn] = {"id": bid}
        print(
            f"[PRIMARY] Backup {bid} joined from {addr}; "
            f"backups={[i['id'] for i in self.backups.values()]}"
        )
        self._backup_loop(conn)


    def _client_loop(self, conn, name):
        try:
            for msg in conn.messages():
                mtype = msg.get("type")
                if mtype == MSG_MOVE:
                    _, text = self.game.apply_move(
                        name, msg.get("row"), msg.get("col"), msg.get("value")
                    )
                elif mtype == MSG_HINT:
                    _, text = self.game.reveal_hint(name)
                else:
                    continue
                self.broadcast({
                    "type": MSG_UPDATE,
                    "state": self.game.as_dict(),
                    "message": text,
                })
        except OSError:
            pass
        finally:
            self.clients.pop(conn, None)
            conn.close()
            print(f"[PRIMARY] Client {name} left")
            self.broadcast(
                {"type": MSG_INFO, "message": f"{name} left the game."}
            )

    def _backup_loop(self, conn):
        try:
            for _ in conn.messages():
                pass 
        except OSError:
            pass
        finally:
            self.backups.pop(conn, None)
            conn.close()
            print("[PRIMARY] Backup disconnected")


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
        self.mode = "backup"
        self.conn = None
        self._promoted = None

        self.backup_id = (
            backup_id if backup_id is not None
            else random.randint(1, 1_000_000)
        )
        self.known_backups = {self.backup_id}

    def start(self):
        print(f"[BACKUP {self.backup_id}] Starting")
        for target in (
            self._discovery_loop,
            self._connect_loop,
            self._monitor_loop,
        ):
            threading.Thread(target=target, daemon=True).start()

        try:
            while self.running:
                if self.mode == "primary" and self._promoted is not None:
                    primary = self._promoted
                    self._promoted = None
                    print(f"[BACKUP {self.backup_id}] Taking over as PRIMARY")
                    primary.start()
                    self.running = False
                    return
                time.sleep(0.5)
        except KeyboardInterrupt:
            print(f"\n[BACKUP {self.backup_id}] Shutting down.")
            self.running = False

    def _discovery_loop(self):
        sock = create_multicast_listener()
        sock.settimeout(1)
        while self.running and self.mode == "backup":
            try:
                data, _ = sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break
            parts = data.decode("utf-8").split()
            if len(parts) == 3 and parts[0] == ANNOUNCE_PREFIX:
                addr = (parts[1], int(parts[2]))
                self.last_heartbeat = time.time()
                if addr != self.primary_addr:
                    self.primary_addr = addr
                    print(
                        f"[BACKUP {self.backup_id}] Primary is "
                        f"{addr[0]}:{addr[1]}"
                    )
        sock.close()

    def _connect_loop(self):
        while self.running and self.mode == "backup":
            if self.primary_addr is None:
                time.sleep(1)
                continue

            host, port = self.primary_addr
            try:
                conn = MessageConnection.connect(host, port, timeout=5)
            except OSError as e:
                print(
                    f"[BACKUP {self.backup_id}] Connect to {host}:{port} "
                    f"failed: {e}"
                )
                time.sleep(2)
                continue

            self.conn = conn
            print(f"[BACKUP {self.backup_id}] Connected to primary {host}:{port}")
            try:
                conn.send({
                    "type": MSG_HELLO,
                    "role": ROLE_BACKUP,
                    "id": self.backup_id,
                    "version": PROTOCOL_VERSION,
                })
                self.last_heartbeat = time.time()
                for msg in conn.messages():
                    if not self.running or self.mode != "backup":
                        break
                    if not self._handle_primary_msg(msg):
                        break
            except OSError:
                pass
            finally:
                conn.close()
                self.conn = None
                print(f"[BACKUP {self.backup_id}] Lost primary connection")
                if self.mode == "backup":
                    time.sleep(2)

    def _handle_primary_msg(self, msg):
        mtype = msg.get("type")
        if mtype in (MSG_PRIMARY_ALIVE, MSG_FULL_STATE, MSG_UPDATE):
            self.last_heartbeat = time.time()
            if mtype != MSG_PRIMARY_ALIVE:
                self.game_state = msg.get("state", self.game_state)
        elif mtype == MSG_BACKUP_MEMBERS:
            self.known_backups = set(msg.get("members", [])) | {self.backup_id}
        elif mtype == MSG_NEW_PRIMARY:
            self.primary_addr = (msg.get("host"), msg.get("port"))
            self.last_heartbeat = time.time()
            return False
        return True

    def _monitor_loop(self):
        while self.running and self.mode == "backup":
            time.sleep(HEARTBEAT_INTERVAL)
            if self.mode != "backup" or self.primary_addr is None:
                continue
            if time.time() - self.last_heartbeat <= HEARTBEAT_TIMEOUT:
                continue

            print(f"[BACKUP {self.backup_id}] Primary heartbeat lost")
            time.sleep(random.uniform(0.5, 2.0))
            if self.mode != "backup" or self.primary_addr is None:
                continue
            if time.time() - self.last_heartbeat <= HEARTBEAT_TIMEOUT:
                continue

            highest = (
                max(self.known_backups) if self.known_backups else self.backup_id
            )
            if self.backup_id != highest:
                print(
                    f"[BACKUP {self.backup_id}] Deferring to backup {highest}"
                )
                continue
            self.mode = "primary"
            _, port = self.primary_addr
            try:
                primary = PrimaryServer("0.0.0.0", port)
            except OSError as e:
                print(f"[BACKUP {self.backup_id}] Promotion failed: {e}")
                self.mode = "backup"
                self.last_heartbeat = time.time()
                continue

            if self.game_state:
                primary.game.round = self.game_state.get("round", 1)
                primary.game.grid = self.game_state.get(
                    "grid", primary.game.grid
                )
                primary.game.scores = dict(self.game_state.get("scores", {}))

            self._promoted = primary
            print(f"[BACKUP {self.backup_id}] Elected PRIMARY on port {port}")
            return


def main():
    parser = argparse.ArgumentParser(
        description="Distributed Puzzle Game Server"
    )
    parser.add_argument(
        "--role", choices=["primary", "backup"], required=True,
        help="Start as primary or backup server",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to bind (primary)")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_SERVER_PORT, help="TCP port for primary"
    )
    parser.add_argument("--primary-host", help="Primary server host (backup)")
    parser.add_argument("--primary-port", type=int, help="Primary server port (backup)")
    parser.add_argument(
        "--backup-id", type=int,
        help="Explicit id for this backup (larger id wins election)",
    )
    args = parser.parse_args()

    if args.role == "primary":
        try:
            srv = PrimaryServer(host=args.host, port=args.port)
        except OSError as e:
            parser.error(f"cannot bind {args.host}:{args.port}: {e}")
        srv.start()
    else:
        BackupServer(
            primary_host=args.primary_host,
            primary_port=args.primary_port,
            backup_id=args.backup_id,
        ).start()


if __name__ == "__main__":
    main()

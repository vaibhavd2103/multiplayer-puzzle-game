#!/usr/bin/env python3
import socket
import threading
import time
import json
import random
import argparse

GRID_SIZE = 5
DEFAULT_PORT = 6000
MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 5007

def send_json(sock, obj):
    sock.sendall((json.dumps(obj)+"\n").encode())

def recv_json(sock):
    data = b""
    while not data.endswith(b"\n"):
        try:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
        except:
            return None
    return json.loads(data.decode().strip())

# =========================
# GAME STATE
# =========================
class GameState:
    def __init__(self):
        self.lock = threading.Lock()
        self.round = 1
        self.solution = self._gen_solution()
        self.grid = self._gen_puzzle()
        self.scores = {}

    def _gen_solution(self):
        return [[random.randint(1, 9) for _ in range(GRID_SIZE)] for _ in range(GRID_SIZE)]

    def _gen_puzzle(self):
        grid = [row[:] for row in self.solution]
        for r in range(GRID_SIZE):
            for c in range(GRID_SIZE):
                if random.random() < 0.4:
                    grid[r][c] = 0
        return grid

    def as_dict(self):
        return {"round": self.round, "grid": self.grid, "scores": self.scores}

    def apply_move(self, player, r, c, v):
        with self.lock:
            self.scores.setdefault(player, 0)
            if not (0 <= r < GRID_SIZE and 0 <= c < GRID_SIZE):
                return False, "Out of range"
            if self.grid[r][c] != 0:
                return False, "Cell already filled"
            if self.solution[r][c] == v:
                self.grid[r][c] = v
                self.scores[player] += 1
                return True, f"{player} placed {v} at ({r},{c}) ✓"
            else:
                self.scores[player] -= 1
                return False, f"{player} placed {v} at ({r},{c}) ✗"

# =========================
# PRIMARY SERVER
# =========================
class PrimaryServer:
    def __init__(self, host, port, initial_state=None):
        self.host = host
        self.port = port
        self.clients = {}
        self.backups = {}
        self.next_backup_id = 1
        self.running = True
        self.game = GameState()
        if initial_state:
            self.game.round = initial_state["round"]
            self.game.grid = initial_state["grid"]
            self.game.scores = initial_state["scores"]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(10)

    def start(self):
        print(f"[PRIMARY] Listening on {self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True).start()
        while self.running:
            time.sleep(1)

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.sock.accept()
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
            except:
                break

    def _handle_conn(self, conn):
        hello = recv_json(conn)
        if not hello:
            conn.close()
            return
        role = hello.get("role")
        if role == "client":
            name = hello.get("name","anon")
            self.clients[conn] = name
            send_json(conn, {"type":"full_state","state":self.game.as_dict()})
            self._client_loop(conn, name)
        elif role == "backup":
            bid = self.next_backup_id
            self.next_backup_id += 1
            self.backups[conn] = bid
            print(f"[PRIMARY] Backup {bid} registered")
            send_json(conn, {"type":"backup_welcome","backup_id":bid,"members":list(self.backups.values()),"state":self.game.as_dict()})
            self._broadcast_backups()
            self._backup_loop(conn)

    def _client_loop(self, conn, name):
        try:
            while True:
                msg = recv_json(conn)
                if msg is None:
                    break
                if msg.get("type") == "move":
                    ok,text = self.game.apply_move(name,msg["row"],msg["col"],msg["value"])
                    update = {"type":"update","state":self.game.as_dict(),"message":text}
                    self._broadcast(update)
        finally:
            conn.close()
            self.clients.pop(conn,None)

    def _backup_loop(self, conn):
        try:
            while recv_json(conn):
                pass
        finally:
            bid = self.backups.pop(conn,None)
            print(f"[PRIMARY] Backup {bid} disconnected")
            self._broadcast_backups()
            conn.close()

    def _broadcast(self, obj):
        for c in list(self.clients):
            try: send_json(c,obj)
            except: self.clients.pop(c,None)
        for b in list(self.backups):
            try: send_json(b,obj)
            except: self.backups.pop(b,None)

    def _broadcast_backups(self):
        members = list(self.backups.values())
        for b in list(self.backups):
            try: send_json(b, {"type":"backup_members","members":members})
            except: self.backups.pop(b,None)

# =========================
# BACKUP SERVER
# =========================
class BackupServer:
    def __init__(self, primary_host, primary_port):
        self.primary_addr = (primary_host, primary_port)
        self.backup_id = None
        self.members = []
        self.last_seen = time.time()
        self.game_state = None
        self.running = True
        self.election_done = False  # track if election has already happened

    def start(self):
        print("[BACKUP] Starting")
        threading.Thread(target=self._connect_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        while self.running:
            time.sleep(1)

    def _connect_loop(self):
        while self.running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.primary_addr)
                send_json(sock, {"type":"hello","role":"backup"})
                while True:
                    msg = recv_json(sock)
                    if msg is None: break
                    self.last_seen = time.time()
                    if msg["type"]=="backup_welcome":
                        self.backup_id = msg["backup_id"]
                        self.members = msg["members"]
                        self.game_state = msg["state"]
                        print(f"[BACKUP {self.backup_id}] Connected to PRIMARY")
                    elif msg["type"]=="backup_members":
                        self.members = msg["members"]
                        print(f"[BACKUP {self.backup_id}] Members: {self.members}")
                    elif msg["type"]=="update":
                        self.game_state = msg["state"]
            except:
                time.sleep(2)

    def _monitor_loop(self):
        while self.running:
            time.sleep(2)
            # If primary heartbeat missing for 6+ seconds and election not yet done
            if time.time() - self.last_seen > 6 and not self.election_done:
                self._run_election()
                self.election_done = True
            # Reset election flag if primary heartbeat is detected
            elif time.time() - self.last_seen <= 6:
                self.election_done = False


    def _run_election(self):
        if not self.members:
            print("[BACKUP] No members known — waiting")
            return
        winner = max(self.members)
        if self.backup_id==winner:
            print(f"[BACKUP {self.backup_id}] Becoming PRIMARY")
            PrimaryServer("0.0.0.0", self.primary_addr[1], self.game_state).start()
        else:
            print(f"[BACKUP {self.backup_id}] Lost election — staying BACKUP")
            self.running=True  # stays backup

# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["primary","backup"], required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=DEFAULT_PORT)
    parser.add_argument("--primary-host")
    parser.add_argument("--primary-port", type=int)
    args = parser.parse_args()

    if args.role=="primary":
        port = int(args.port)
        PrimaryServer(args.host, port).start()
    else:
        primary_host = args.primary_host if args.primary_host else "puzzle-primary"
        primary_port = int(args.primary_port) if args.primary_port else DEFAULT_PORT
        BackupServer(primary_host, primary_port).start()

if __name__=="__main__":
    main()

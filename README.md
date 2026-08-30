# Distributed Puzzle Game

A small distributed system: a collaborative number‑placing puzzle played by
multiple terminal clients against a replicated server. The server runs as a
**primary** with any number of **backups**; if the primary dies, a backup is
elected, promotes itself, and clients reconnect automatically.

Everything is Python standard library — no third‑party dependencies.

## How it works

### The game

* A shared `GRID_SIZE`×`GRID_SIZE` grid (default **4×4**). Each cell has a
  hidden solution value in `1..9`; ~40% of cells start blank.
* Players submit `row col value` moves:
  * correct → **+1** point, the cell is filled for everyone
  * incorrect → **−1** point
  * `/hint` reveals one random blank cell but costs **−2** points
* When every cell is filled the round ends and a fresh puzzle starts
  (`round` increments, scores carry over).

### Processes and transport

| Component | Role |
|-----------|------|
| `server.py --role primary` | Owns the authoritative `GameState`, accepts client/backup TCP connections, broadcasts every update. |
| `server.py --role backup` | Connects to the primary over TCP, mirrors state, watches heartbeats, and can be elected primary. |
| `client.py` | Connects over TCP, renders the board, sends moves/hints. |

* **TCP** carries newline‑delimited JSON messages (`common.MessageConnection`).
* **UDP multicast** (`224.1.1.1:5007`) carries the primary's presence
  announcement `primary_alive <host> <port>`, used by clients and backups for
  discovery and post‑failover re‑discovery.

### Failover

1. The primary multicasts `primary_alive` and sends TCP heartbeats to backups
   every 2 s.
2. If a backup sees no heartbeat for 6 s, it starts an election.
3. Highest `backup_id` wins; that backup binds `0.0.0.0:<port>`, seeds the
   preserved game state, and starts announcing as the new primary.
4. Other backups and all clients see the new announcement and reconnect.

## Prerequisites

* Python **3.10+**
* No packages to install (`requirements.txt` is intentionally empty)
* Clients discover the server via UDP multicast, so run them on the same
  host / LAN segment, or pass `--host`/`--port` explicitly.

## Running locally

Optionally create a virtual environment first:

```bash
python -m venv .venv
# Windows:        .venv\Scripts\activate
# macOS / Linux:  source .venv/bin/activate
```

### 1. Start the primary

```bash
python server.py --role primary --port 6000
```

### 2. Start one or more backups (optional, enables failover)

```bash
python server.py --role backup --primary-host 127.0.0.1 --primary-port 6000 --backup-id 100
python server.py --role backup --primary-host 127.0.0.1 --primary-port 6000 --backup-id 200
```

### 3. Start clients

```bash
python client.py --name alice          # auto-discovers via multicast
python client.py --name bob --host 127.0.0.1 --port 6000   # explicit
```

### 4. Try a failover

Kill the primary (`Ctrl+C`). Within ~8 s the highest‑id backup logs
`Elected PRIMARY` and each client logs `Reconnected to primary server`.

## Client commands

| Input | Action |
|-------|--------|
| `row col value` | Make a move, e.g. `0 1 5` |
| `/hint` | Reveal a random blank cell (−2 points) |
| `/stats` | Your session stats — accuracy, streaks, toughest cells |
| `/board` | Redraw the current board |
| `/help` | List commands |
| `quit` | Leave the game |

## Command‑line reference

**Server**

| Flag | Applies to | Default | Meaning |
|------|-----------|---------|---------|
| `--role {primary,backup}` | both | *(required)* | Which role to start |
| `--host` | primary | `0.0.0.0` | Address to bind |
| `--port` | primary | `6000` | TCP listen port |
| `--primary-host` | backup | – | Primary's address (optional; multicast discovery is used otherwise) |
| `--primary-port` | backup | – | Primary's port |
| `--backup-id` | backup | random | Election id — highest wins |

**Client**

| Flag | Default | Meaning |
|------|---------|---------|
| `--name` | *(required)* | Player name |
| `--host` / `--port` | – | Connect directly instead of discovering via multicast |

## Docker

`docker-compose.yml` builds one primary, two backups, and an nginx container
on a bridge network:

```bash
docker compose up --build
```

The primary's TCP port is published on `localhost:6000` for clients run from
the host. Note that UDP‑multicast discovery does not cross the Docker bridge
network, so containerised backups are given `--primary-host puzzle-primary`
explicitly.

## Project layout

```
common.py   Shared constants, protocol message tags, MessageConnection
            (buffered, thread-safe JSON framing), multicast socket helpers,
            get_local_ip().
server.py   GameState, PrimaryServer, BackupServer (election + promotion).
client.py   Client (connect / reconnect / render) and SessionStats.
```


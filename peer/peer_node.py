"""
=============================================================
  Peer Node  —  peer/peer_node.py
  Implemented by:
    SAGAR BINDU DASH         — TCP server, ResourceTable, gossip protocol
    PANDYA HARSHIDA          — file sharing, JOIN_ANNOUNCE, LEAVE_ANNOUNCE
    KAWALEKAR NEHAL          — submit_cluster_task (P2P-to-cluster bridge)
=============================================================

What this file does:
  - Runs a TCP server so other peers can connect directly
  - Maintains a local resource table (CPU%, memory%, files)
  - Gossips resource state to all known peers every 8 seconds
  - Handles incoming messages: RESOURCE_REQUEST, FILE_SHARE,
    JOIN_ANNOUNCE, LEAVE_ANNOUNCE, GOSSIP
  - Can submit heavy compute tasks to the cluster master

Key P2P principle: NO central coordinator. Every peer talks
directly to every other peer. No permission needed.

Usage:
    python3 peer_node.py --id P1 --host 0.0.0.0 --port 7001 \
        --peers 10.0.0.2:7002,10.0.0.3:7003,10.0.0.4:7004 \
        --master 10.0.0.1:6000
"""

import argparse
import json
import logging
import os
import random
import socket
import sys
import threading
import time
from datetime import datetime


# =============================================================
# LOGGING SETUP
# =============================================================
def make_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    return logger


# =============================================================
# MESSAGE HELPERS  (designed by Naveen, used by all files)
# Protocol: every message = [4-byte length][JSON bytes]
# =============================================================
def send_msg(sock: socket.socket, payload: dict) -> None:
    """Encode payload as JSON and send it with a 4-byte length prefix."""
    data = json.dumps(payload).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)


def recv_msg(sock: socket.socket) -> dict:
    """Read a length-prefixed JSON message and return it as a dict."""
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        return {}
    length = int.from_bytes(raw_len, "big")
    raw = _recv_exact(sock, length)
    if not raw:
        return {}
    return json.loads(raw.decode())


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Keep reading until exactly n bytes are received."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf += chunk
    return buf


# =============================================================
# RESOURCE TABLE  —  implemented by SAGAR BINDU DASH
# Simulates the local resource state of this server
# =============================================================
class ResourceTable:
    """
    Holds the simulated resource state of this peer:
      - cpu_percent:     current CPU usage (%)
      - mem_percent:     current memory usage (%)
      - files:           list of files this peer owns
      - tasks_processed: number of cluster tasks submitted

    A background thread fluctuates CPU and memory every 5 seconds
    to simulate a real server whose load changes over time.
    """

    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self._lock   = threading.Lock()
        self._data   = {
            "cpu_percent":     round(random.uniform(10, 60), 2),
            "mem_percent":     round(random.uniform(20, 70), 2),
            "files":           [f"file_{peer_id}_{i}.dat" for i in range(1, 4)],
            "tasks_processed": 0,
        }
        # Start background thread that makes values drift realistically
        threading.Thread(target=self._fluctuate, daemon=True).start()

    def _fluctuate(self):
        """Randomly shift CPU and memory values every 5 seconds."""
        while True:
            time.sleep(5)
            with self._lock:
                self._data["cpu_percent"] = round(
                    min(100, max(0,
                        self._data["cpu_percent"] + random.uniform(-10, 10)
                    )), 2
                )
                self._data["mem_percent"] = round(
                    min(100, max(0,
                        self._data["mem_percent"] + random.uniform(-5, 5)
                    )), 2
                )

    def snapshot(self) -> dict:
        """Return a copy of the current resource state."""
        with self._lock:
            return dict(self._data)

    def increment_tasks(self):
        """Called each time this peer submits a cluster task."""
        with self._lock:
            self._data["tasks_processed"] += 1


# =============================================================
# PEER NODE CLASS
# =============================================================
class PeerNode:
    def __init__(self, peer_id: str, host: str, port: int,
                 known_peers: list, master_addr: tuple):
        self.peer_id      = peer_id
        self.host         = host
        self.port         = port
        self.known_peers  = list(known_peers)   # list of (host, port) tuples
        self.master_addr  = master_addr         # (host, port) of cluster master
        self.resource_table  = ResourceTable(peer_id)
        self.peer_resources  = {}   # cache of other peers' resources from gossip
        self._lock    = threading.Lock()
        self._running = True
        self.log = make_logger(f"Peer-{peer_id}")
        self.log.info(f"Peer {peer_id} initialising on {host}:{port}")

    # ---------------------------------------------------------
    # HELPER — open a TCP connection to another node
    # ---------------------------------------------------------
    def _connect(self, host: str, port: int) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
        return s

    # ==========================================================
    # TCP SERVER  —  implemented by SAGAR BINDU DASH
    # Listens for incoming connections from other peers.
    # Each connection is handled in its own thread so multiple
    # peers can connect simultaneously without blocking.
    # ==========================================================
    def start_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(10)
        self.log.info(f"Peer {self.peer_id} listening on {self.host}:{self.port}")

        while self._running:
            try:
                srv.settimeout(1.0)
                conn, addr = srv.accept()
                threading.Thread(
                    target=self._handle_peer,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    self.log.error(f"Server error: {e}")

    def _handle_peer(self, conn: socket.socket, addr):
        """
        Route incoming messages to the right handler.
        Message types and who implemented them:
          RESOURCE_REQUEST  — Sagar   (resource sharing)
          FILE_SHARE        — Harshida (file sharing)
          JOIN_ANNOUNCE     — Harshida (node joins network)
          LEAVE_ANNOUNCE    — Harshida (node leaves network)
          GOSSIP            — Sagar   (resource gossip)
        """
        try:
            msg = recv_msg(conn)
            if not msg:
                return
            mtype = msg.get("type")

            # --------------------------------------------------
            # RESOURCE_REQUEST — another peer wants our resources
            # Implemented by SAGAR BINDU DASH
            # --------------------------------------------------
            if mtype == "RESOURCE_REQUEST":
                sender = msg.get("sender_id", "unknown")
                self.log.info(f"Resource request from peer {sender}")
                send_msg(conn, {
                    "type":      "RESOURCE_RESPONSE",
                    "sender_id": self.peer_id,
                    "resources": self.resource_table.snapshot(),
                    "timestamp": datetime.utcnow().isoformat(),
                })

            # --------------------------------------------------
            # FILE_SHARE — another peer is sharing a file with us
            # Implemented by PANDYA HARSHIDA
            # --------------------------------------------------
            elif mtype == "FILE_SHARE":
                fname  = msg.get("filename", "unknown")
                sender = msg.get("sender_id", "?")
                self.log.info(f"Peer {sender} shared file '{fname}' with us")
                send_msg(conn, {
                    "type":     "ACK",
                    "status":   "received",
                    "receiver": self.peer_id
                })

            # --------------------------------------------------
            # JOIN_ANNOUNCE — a new peer joined the P2P network
            # Implemented by PANDYA HARSHIDA
            # --------------------------------------------------
            elif mtype == "JOIN_ANNOUNCE":
                new_id   = msg.get("peer_id")
                new_host = msg.get("host")
                new_port = msg.get("port")
                self.log.info(f"New peer joined: {new_id} @ {new_host}:{new_port}")
                with self._lock:
                    entry = (new_host, new_port)
                    if entry not in self.known_peers:
                        self.known_peers.append(entry)  # add to routing table
                send_msg(conn, {"type": "ACK", "status": "noted"})

            # --------------------------------------------------
            # LEAVE_ANNOUNCE — a peer is leaving the network
            # Implemented by PANDYA HARSHIDA
            # --------------------------------------------------
            elif mtype == "LEAVE_ANNOUNCE":
                leaving_id   = msg.get("peer_id")
                leaving_host = msg.get("host")
                leaving_port = msg.get("port")
                self.log.info(f"Peer {leaving_id} is leaving the network")
                with self._lock:
                    entry = (leaving_host, leaving_port)
                    if entry in self.known_peers:
                        self.known_peers.remove(entry)  # remove from routing table
                send_msg(conn, {"type": "ACK", "status": "noted"})

            # --------------------------------------------------
            # GOSSIP — a peer pushed its resource state to us
            # Implemented by SAGAR BINDU DASH
            # --------------------------------------------------
            elif mtype == "GOSSIP":
                sender_id = msg.get("peer_id")
                resources = msg.get("resources", {})
                with self._lock:
                    self.peer_resources[sender_id] = resources  # cache it
                self.log.debug(f"Gossip update received from {sender_id}")
                send_msg(conn, {"type": "ACK"})

            else:
                self.log.warning(f"Unknown message type received: {mtype}")

        except Exception as e:
            self.log.error(f"Handler error from {addr}: {e}")
        finally:
            conn.close()

    # ==========================================================
    # GOSSIP PROTOCOL  —  implemented by SAGAR BINDU DASH
    # Every 8 seconds, push our resource state to all known peers.
    # This is how the network achieves eventual consistency —
    # every peer stays up-to-date without anyone asking.
    # ==========================================================
    def gossip_resources(self):
        """Background thread: broadcast our resources to all peers every 8s."""
        while self._running:
            time.sleep(8)
            snapshot = self.resource_table.snapshot()
            msg = {
                "type":      "GOSSIP",
                "peer_id":   self.peer_id,
                "resources": snapshot
            }
            for (h, p) in list(self.known_peers):
                try:
                    s = self._connect(h, p)
                    send_msg(s, msg)
                    recv_msg(s)
                    s.close()
                except Exception:
                    pass  # silently skip unreachable peers
                  # ==========================================================
    # FILE SHARING  —  implemented by PANDYA HARSHIDA
    # Direct peer-to-peer file notification. No central server.
    # ==========================================================
    def share_file_with(self, host: str, port: int, filename: str) -> dict:
        """
        Notify another peer directly that we have a file available.
        Opens a direct TCP connection to the target peer — no coordinator.
        """
        try:
            s = self._connect(host, port)
            send_msg(s, {
                "type":      "FILE_SHARE",
                "sender_id": self.peer_id,
                "filename":  filename
            })
            resp = recv_msg(s)
            s.close()
            return resp
        except Exception as e:
            self.log.warning(f"File share to {host}:{port} failed: {e}")
            return {}

    # ==========================================================
    # JOIN / LEAVE  —  implemented by PANDYA HARSHIDA
    # ==========================================================
    def announce_join(self):
        """
        Broadcast our arrival to all known peers.
        Each peer adds us to their known_peers list.
        Called automatically on startup.
        """
        msg = {
            "type":    "JOIN_ANNOUNCE",
            "peer_id": self.peer_id,
            "host":    self.host,
            "port":    self.port
        }
        for (h, p) in list(self.known_peers):
            try:
                s = self._connect(h, p)
                send_msg(s, msg)
                recv_msg(s)
                s.close()
                self.log.info(f"Announced join to {h}:{p}")
            except Exception as e:
                self.log.debug(f"Join announce to {h}:{p} failed: {e}")

    def announce_leave(self):
        """
        Notify all known peers that we are shutting down.
        Each peer removes us from their known_peers list.
        Called automatically on Ctrl+C (via stop()).
        """
        msg = {
            "type":    "LEAVE_ANNOUNCE",
            "peer_id": self.peer_id,
            "host":    self.host,
            "port":    self.port
        }
        for (h, p) in list(self.known_peers):
            try:
                s = self._connect(h, p)
                send_msg(s, msg)
                recv_msg(s)
                s.close()
                self.log.info(f"Leave announced to {h}:{p}")
            except Exception:
                pass  # peer may already be down — that's fine

    # ==========================================================
    # RESOURCE QUERY  —  direct P2P, no coordinator
    # ==========================================================
    def request_resources_from(self, host: str, port: int) -> dict:
        """Ask another peer directly for its CPU%, memory%, files."""
        try:
            s = self._connect(host, port)
            send_msg(s, {"type": "RESOURCE_REQUEST", "sender_id": self.peer_id})
            resp = recv_msg(s)
            s.close()
            return resp
        except Exception as e:
            self.log.warning(f"Could not reach {host}:{port}: {e}")
            return {}

    # ==========================================================
    # CLUSTER BRIDGE  —  implemented by KAWALEKAR NEHAL
    # This is the ONLY place where P2P and cluster worlds meet.
    # A peer submits a heavy task to the cluster master here.
    # ==========================================================
    def submit_cluster_task(self, task_type: str, data: dict) -> dict:
        """
        Submit a compute task to the cluster master.

        The master will:
          1. Split the task across all workers
          2. Run sub-tasks in parallel
          3. Merge results
          4. Return the final answer here

        This method blocks until the answer is ready.
        """
        try:
            s = self._connect(*self.master_addr)
            send_msg(s, {
                "type":      "TASK_SUBMIT",
                "requester": self.peer_id,
                "task_type": task_type,
                "data":      data,
            })
            result = recv_msg(s)
            s.close()
            self.resource_table.increment_tasks()  # track that we used the cluster
            self.log.info(f"Cluster task result: {result}")
            return result
        except Exception as e:
            self.log.error(f"Cluster submit failed: {e}")
            return {"status": "error", "detail": str(e)}

    # ---------------------------------------------------------
    # LIFECYCLE — start/stop the peer
    # ---------------------------------------------------------
    def run(self):
        """Start server and gossip threads, then announce join."""
        # Thread 1: listen for incoming peer connections
        threading.Thread(target=self.start_server, daemon=True).start()
        # Thread 2: periodically push our resources to all peers
        threading.Thread(target=self.gossip_resources, daemon=True).start()
        time.sleep(1)
        self.announce_join()  # tell all known peers we have joined
        return self

    def stop(self):
        """Announce leave to all peers, then shut down."""
        self.log.info(f"Peer {self.peer_id} shutting down")
        self.announce_leave()
        self._running = False

    def poll_all_peers(self):
        """Query and print resource status from all known peers."""
        self.log.info("=== Polling all known peers for resources ===")
        for (h, p) in list(self.known_peers):
            resp = self.request_resources_from(h, p)
            if resp:
                pid = resp.get("sender_id", f"{h}:{p}")
                res = resp.get("resources", {})
                with self._lock:
                    self.peer_resources[pid] = res
                self.log.info(
                    f"  {pid}: CPU={res.get('cpu_percent')}%  "
                    f"MEM={res.get('mem_percent')}%"
                )


# =============================================================
# HELPER — parse comma-separated peer list from CLI
# =============================================================
def parse_peer_list(raw: str) -> list:
    peers = []
    if not raw:
        return peers
    for token in raw.split(","):
        token = token.strip()
        if ":" in token:
            h, p = token.rsplit(":", 1)
            peers.append((h, int(p)))
    return peers


# =============================================================
# ENTRY POINT
# =============================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="P2P Peer Node")
    ap.add_argument("--id",     required=True,  help="Unique peer ID e.g. P1")
    ap.add_argument("--host",   default="127.0.0.1", help="IP to bind on")
    ap.add_argument("--port",   type=int, required=True, help="Port to listen on")
    ap.add_argument("--peers",  default="",
                    help="Comma-separated list of other peers as host:port")
    ap.add_argument("--master", default="127.0.0.1:6000",
                    help="Cluster master address as host:port")
    ap.add_argument("--demo",   action="store_true",
                    help="Run a short automated demo sequence then exit")
    args = ap.parse_args()

    known      = parse_peer_list(args.peers)
    mh, mp     = args.master.rsplit(":", 1)
    master_addr = (mh, int(mp))

    node = PeerNode(args.id, args.host, args.port, known, master_addr)
    node.run()

    if args.demo:
        time.sleep(3)
        node.poll_all_peers()
        if known:
            h, p = known[0]
            node.share_file_with(h, p, f"data_{args.id}.csv")
        result = node.submit_cluster_task("prime_count", {"limit": 100_000})
        node.log.info(f"Cluster task result: {result}")
        time.sleep(5)
        node.stop()
    else:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            node.stop()

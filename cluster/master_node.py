"""
=============================================================
  Cluster Master Node  —  cluster/master_node.py
  Implemented by: G SRINIVASULU
=============================================================

What this file does:
  - Runs a TCP server that accepts connections from workers and peers
  - Keeps a registry of all available worker nodes
  - When a peer submits a task, splits it into equal sub-tasks,
    dispatches them to ALL workers simultaneously (parallel),
    waits for all results, merges them, and replies to the peer
  - Supports live worker addition and removal (no restart needed)

Start this FIRST before workers or peers.

Usage:
    python3 master_node.py --host 0.0.0.0 --port 6000
"""

import argparse
import itertools
import json
import logging
import os
import socket
import sys
import threading
import time


# =============================================================
# LOGGING SETUP
# Write logs to both console and a file in the logs/ folder
# =============================================================
def make_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    # File handler — full debug logs
    fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler — INFO and above only
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    return logger


# =============================================================
# MESSAGE HELPERS  (designed by Naveen, used by all files)
# Protocol: every message = [4-byte length][JSON bytes]
# Why: TCP is a stream. Without a length prefix, the receiver
#      cannot tell where one message ends and the next begins.
# =============================================================
def send_msg(sock: socket.socket, payload: dict) -> None:
    """Encode payload as JSON and send it with a 4-byte length prefix."""
    data = json.dumps(payload).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)


def recv_msg(sock: socket.socket) -> dict:
    """Read a length-prefixed JSON message and return it as a dict."""
    raw_len = _recv_exact(sock, 4)          # read the 4-byte length header
    if not raw_len:
        return {}
    length = int.from_bytes(raw_len, "big") # convert bytes to int
    raw = _recv_exact(sock, length)         # read exactly that many bytes
    if not raw:
        return {}
    return json.loads(raw.decode())


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Keep reading from the socket until exactly n bytes are received."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""  # connection closed
        buf += chunk
    return buf


# =============================================================
# WORKER REGISTRY  —  tracks all connected worker nodes
# =============================================================
class WorkerRegistry:
    """
    Stores active workers as:
      { worker_id: {host, port, tasks_done, status} }

    Thread-safe: all methods lock before modifying the dictionary
    because multiple connections can arrive at the same time.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._workers: dict = {}
        self._robin = None   # itertools.cycle for round-robin scheduling

    def register(self, worker_id: str, host: str, port: int):
        """Add a new worker to the registry."""
        with self._lock:
            self._workers[worker_id] = {
                "host": host,
                "port": port,
                "tasks": 0,
                "status": "active"
            }
            self._rebuild_robin()

    def deregister(self, worker_id: str):
        """Remove a worker from the registry (called on graceful shutdown)."""
        with self._lock:
            if worker_id in self._workers:
                del self._workers[worker_id]
                self._rebuild_robin()

    def _rebuild_robin(self):
        """Rebuild the round-robin cycle after any registry change."""
        ids = list(self._workers.keys())
        self._robin = itertools.cycle(ids) if ids else None

    def get_active_workers(self) -> list:
        """Return list of worker_ids that are currently active."""
        with self._lock:
            return [wid for wid, info in self._workers.items()
                    if info["status"] == "active"]

    def get_worker_info(self, worker_id: str) -> dict:
        """Return host/port info for a specific worker."""
        with self._lock:
            return dict(self._workers.get(worker_id, {}))

    def mark_failed(self, worker_id: str):
        """Mark a worker as failed (called when it stops responding)."""
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id]["status"] = "failed"

    def snapshot(self) -> dict:
        """Return a copy of the full registry (for STATUS queries)."""
        with self._lock:
            return {wid: dict(info) for wid, info in self._workers.items()}


# =============================================================
# MASTER NODE
# =============================================================
class MasterNode:
    """
    The cluster coordinator.

    Does NOT compute anything itself — only coordinates:
      1. Accepts WORKER_REGISTER / WORKER_DEREGISTER messages
      2. Accepts TASK_SUBMIT from peer nodes
      3. Splits the task, dispatches sub-tasks to workers in parallel
      4. Merges results and returns the answer to the peer
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.registry = WorkerRegistry()
        self._running = True
        self._task_counter = 0
        self._task_lock = threading.Lock()
        self.log = make_logger("Master")
        self.log.info(f"Master initialising on {host}:{port}")

    def _next_task_id(self) -> str:
        """Generate a unique sequential task ID like T000001."""
        with self._task_lock:
            self._task_counter += 1
            return f"T{self._task_counter:06d}"

    # ---------------------------------------------------------
    # TCP SERVER — accepts all incoming connections
    # ---------------------------------------------------------
    def start(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(20)
        self.log.info(f"Master listening on {self.host}:{self.port}")

        while self._running:
            try:
                srv.settimeout(1.0)
                conn, addr = srv.accept()
                # Handle each connection in its own thread
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except socket.timeout:
                continue  # check _running flag again
            except Exception as e:
                if self._running:
                    self.log.error(f"Accept error: {e}")

    # ---------------------------------------------------------
    # CONNECTION HANDLER — routes messages by type
    # ---------------------------------------------------------
    def _handle_connection(self, conn: socket.socket, addr):
        try:
            msg = recv_msg(conn)
            mtype = msg.get("type")

            if mtype == "WORKER_REGISTER":
                # A new worker is coming online
                wid   = msg.get("worker_id")
                whost = msg.get("host")
                wport = msg.get("port")
                self.registry.register(wid, whost, wport)
                self.log.info(f"Worker registered: {wid} @ {whost}:{wport}")
                self.log.info(f"Active workers: {self.registry.get_active_workers()}")
                send_msg(conn, {"type": "ACK", "status": "registered"})

            elif mtype == "WORKER_DEREGISTER":
                # A worker is shutting down gracefully
                wid = msg.get("worker_id")
                self.registry.deregister(wid)
                self.log.info(f"Worker deregistered: {wid}")
                self.log.info(f"Active workers: {self.registry.get_active_workers()}")
                send_msg(conn, {"type": "ACK", "status": "deregistered"})

            elif mtype == "TASK_SUBMIT":
                # A peer node submitted a task — dispatch to workers
                requester = msg.get("requester", "unknown")
                task_type = msg.get("task_type")
                data      = msg.get("data", {})
                task_id   = self._next_task_id()
                self.log.info(
                    f"Task {task_id} received from peer '{requester}': {task_type}"
                )
                result = self._dispatch_task(task_id, task_type, data)
                result["task_id"]   = task_id
                result["requester"] = requester
                send_msg(conn, result)

            elif mtype == "STATUS":
                # Anyone asking for current cluster state
                send_msg(conn, {
                    "type":       "STATUS_RESPONSE",
                    "workers":    self.registry.snapshot(),
                    "task_count": self._task_counter,
                })

            else:
                self.log.warning(f"Unknown message type received: {mtype}")
                send_msg(conn, {"type": "ERROR", "detail": "unknown message type"})

        except Exception as e:
            self.log.error(f"Handler error from {addr}: {e}")
            try:
                send_msg(conn, {"type": "ERROR", "detail": str(e)})
            except Exception:
                pass
        finally:
            conn.close()

    # ---------------------------------------------------------
    # TASK DISPATCH — the core parallel execution method
    # ---------------------------------------------------------
    def _dispatch_task(self, task_id: str, task_type: str, data: dict) -> dict:
        """
        Steps:
          1. Get list of all active workers
          2. Split the input into N equal sub-tasks (one per worker)
          3. Spawn N threads — each thread sends a sub-task to one worker
          4. All threads run simultaneously  ← this is the parallel part
          5. Wait (join) for all threads to finish
          6. Merge all partial results into one final answer
          7. Return the merged result
        """
        active_workers = self.registry.get_active_workers()

        if not active_workers:
            self.log.error(f"No active workers for task {task_id}")
            return {"status": "error", "detail": "no active workers available"}

        num_workers = len(active_workers)
        subtasks    = self._split_task(task_type, data, num_workers)

        self.log.info(
            f"Task {task_id}: splitting into {num_workers} sub-tasks "
            f"for workers {active_workers}"
        )

        # Collect results — one slot per worker
        results = [None] * num_workers
        threads = []

        def run_subtask(idx, worker_id, subtask_data):
            """Send one sub-task to one worker and store the result."""
            info = self.registry.get_worker_info(worker_id)
            r = self._send_to_worker(
                info["host"], info["port"],
                task_id, task_type, subtask_data, worker_id
            )
            results[idx] = r

        # Create one thread per worker (all will start simultaneously below)
        for i, (wid, sub_data) in enumerate(zip(active_workers, subtasks)):
            t = threading.Thread(
                target=run_subtask,
                args=(i, wid, sub_data)
            )
            threads.append(t)

        # START all threads at once — parallel execution begins here
        start_time = time.time()
        for t in threads:
            t.start()

        # WAIT for all threads to finish
        for t in threads:
            t.join(timeout=30)

        elapsed = round(time.time() - start_time, 4)

        # Merge all partial answers into one final result
        merged = self._merge_results(task_type, results)
        merged["parallel_workers"] = active_workers
        merged["elapsed_seconds"]  = elapsed
        merged["status"]           = "success"

        self.log.info(
            f"Task {task_id} completed in {elapsed}s | result={merged}"
        )
        return merged

    # ---------------------------------------------------------
    # SPLIT TASK — divide input into N equal pieces
    # ---------------------------------------------------------
    def _split_task(self, task_type: str, data: dict, n: int) -> list:
        """
        Divide the full task input into n roughly equal sub-tasks.
        Each sub-task goes to one worker.
        """
        if task_type == "prime_count":
            limit = data.get("limit", 100_000)
            chunk = limit // n
            subtasks = []
            for i in range(n):
                lo = i * chunk + 1           # start of this worker's range
                hi = (i + 1) * chunk if i < n - 1 else limit  # end of range
                subtasks.append({"low": lo, "high": hi})
            return subtasks

        elif task_type == "sum_squares":
            limit = data.get("limit", 1_000_000)
            chunk = limit // n
            subtasks = []
            for i in range(n):
                lo = i * chunk + 1
                hi = (i + 1) * chunk if i < n - 1 else limit
                subtasks.append({"low": lo, "high": hi})
            return subtasks

        elif task_type == "word_count":
            # Split word list into n equal chunks
            words = data.get("text", "").split()
            chunk = max(1, len(words) // n)
            subtasks = []
            for i in range(n):
                part = words[i * chunk : (i + 1) * chunk if i < n - 1 else None]
                subtasks.append({"words": part})
            return subtasks

        else:
            # Unknown task type — send full data to all workers
            return [data] * n

    # ---------------------------------------------------------
    # SEND TO WORKER — connect and dispatch one sub-task
    # ---------------------------------------------------------
    def _send_to_worker(self, host: str, port: int, task_id: str,
                        task_type: str, data: dict, worker_id: str) -> dict:
        """Open a connection to a worker, send the sub-task, get the result."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(25)
            s.connect((host, port))
            send_msg(s, {
                "type":      "EXECUTE_TASK",
                "task_id":   task_id,
                "task_type": task_type,
                "data":      data
            })
            result = recv_msg(s)
            s.close()
            self.log.info(f"  Worker {worker_id} finished sub-task of {task_id}")
            return result
        except Exception as e:
            self.log.error(f"  Worker {worker_id} failed: {e}")
            self.registry.mark_failed(worker_id)  # mark as failed so it's skipped
            return {"status": "worker_error", "detail": str(e)}

    # ---------------------------------------------------------
    # MERGE RESULTS — combine partial answers from all workers
    # ---------------------------------------------------------
    def _merge_results(self, task_type: str, results: list) -> dict:
        """Add up or combine the partial results from each worker."""
        valid = [r for r in results if r and r.get("status") == "ok"]

        if task_type == "prime_count":
            # Each worker counted primes in its sub-range — add them all up
            total = sum(r.get("count", 0) for r in valid)
            return {"task_type": task_type, "prime_count": total}

        elif task_type == "sum_squares":
            # Each worker computed sum of squares for its sub-range — add them
            total = sum(r.get("sum", 0) for r in valid)
            return {"task_type": task_type, "sum_of_squares": total}

        elif task_type == "word_count":
            # Each worker built a frequency dict — merge by summing counts
            freq: dict = {}
            for r in valid:
                for word, count in r.get("freq", {}).items():
                    freq[word] = freq.get(word, 0) + count
            top5 = sorted(freq.items(), key=lambda x: -x[1])[:5]
            return {
                "task_type":    task_type,
                "unique_words": len(freq),
                "top_5":        top5
            }

        else:
            return {"task_type": task_type, "sub_results": results}

    def stop(self):
        self._running = False
        self.log.info("Master shutting down")


# =============================================================
# ENTRY POINT
# =============================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cluster Master Node")
    ap.add_argument("--host", default="127.0.0.1", help="IP to bind on")
    ap.add_argument("--port", type=int, default=6000, help="Port to listen on")
    args = ap.parse_args()

    master = MasterNode(args.host, args.port)
    try:
        master.start()
    except KeyboardInterrupt:
        master.stop()

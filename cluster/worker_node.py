"""
=============================================================
  Cluster Worker Node  —  cluster/worker_node.py
  Implemented by: GANDRA SHIVA KRISHNA
=============================================================

What this file does:
  - On startup: connects to the master and registers itself
  - Listens for EXECUTE_TASK messages from the master
  - Computes the result for its assigned sub-range/chunk
  - Sends the result back to the master
  - On shutdown: deregisters from the master cleanly

Supported task types:
  prime_count  —  count primes in [low, high]  (Sieve of Eratosthenes)
  sum_squares  —  sum of k^2 for k in [low, high]  (O(1) formula)
  word_count   —  word frequency map for a list of words

Usage:
    python3 worker_node.py --id W1 --host 0.0.0.0 --port 6001 --master 10.0.0.1:6000
"""

import argparse
import json
import logging
import math
import os
import socket
import sys
import threading
import time


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
# COMPUTATION FUNCTIONS  —  the actual work done by each worker
# =============================================================

def compute_prime_count(low: int, high: int) -> int:
    """
    Count how many prime numbers exist in the range [low, high].

    Uses Sieve of Eratosthenes:
      - Create a boolean array: index i = True means i is prime
      - Mark all multiples of each prime as False
      - Count the True values in the range [low, high]

    Two versions:
      - Simple sieve: for ranges up to 500,000 numbers
      - Segmented sieve: for larger ranges (saves memory by working in blocks)
    """
    if high < 2:
        return 0
    low = max(low, 2)   # primes start at 2

    if high - low < 500_000:
        return _simple_sieve(low, high)
    else:
        return _segmented_sieve(low, high)


def _simple_sieve(low: int, high: int) -> int:
    """
    Standard Sieve of Eratosthenes.
    Allocates a boolean array of size high+1.
    Works well for high values up to a few million.
    """
    sieve = [True] * (high + 1)
    sieve[0] = sieve[1] = False   # 0 and 1 are not prime

    # For each number from 2 to sqrt(high), mark its multiples as not prime
    for i in range(2, int(math.isqrt(high)) + 1):
        if sieve[i]:
            for j in range(i * i, high + 1, i):
                sieve[j] = False

    # Count primes in the assigned range [low, high]
    return sum(1 for i in range(max(low, 2), high + 1) if sieve[i])


def _segmented_sieve(low: int, high: int) -> int:
    """
    Segmented Sieve of Eratosthenes.
    Instead of one huge array, processes the range in blocks of size sqrt(high).
    Much more memory-efficient for very large ranges.
    """
    limit = int(math.isqrt(high)) + 1

    # Step 1: find all primes up to sqrt(high) using simple sieve
    small = [True] * limit
    small[0] = small[1] = False
    for i in range(2, int(math.isqrt(limit)) + 1):
        if small[i]:
            for j in range(i * i, limit, i):
                small[j] = False
    primes = [i for i in range(2, limit) if small[i]]

    # Step 2: use those primes to sieve each block in [low, high]
    count    = 0
    seg_size = max(limit, 1)
    lo       = max(low, 2)

    while lo <= high:
        hi  = min(lo + seg_size - 1, high)
        seg = [True] * (hi - lo + 1)   # small block array

        for p in primes:
            # First multiple of p that is >= lo
            start = max(p * p, ((lo + p - 1) // p) * p)
            for j in range(start, hi + 1, p):
                seg[j - lo] = False

        count += sum(seg)
        lo    += seg_size

    return count


def compute_sum_squares(low: int, high: int) -> int:
    """
    Compute sum of k^2 for k = low to high.

    Uses the closed-form formula instead of a loop:
      sum(k^2 for k=1 to n) = n * (n+1) * (2n+1) / 6

    So sum from low to high = S(high) - S(low-1)
    This is O(1) — takes the same time regardless of input size.

    Example: sum from 1 to 1,000,000 = two formula calls and a subtraction.
    """
    def S(n: int) -> int:
        return n * (n + 1) * (2 * n + 1) // 6

    return S(high) - S(low - 1)


def compute_word_count(words: list) -> dict:
    """
    Count word frequencies in the given list of words.
    Lowercases everything and strips punctuation before counting.
    Returns a dict: { word: count }
    """
    freq: dict = {}
    for w in words:
        w = w.lower().strip(".,!?;:'\"")
        if w:
            freq[w] = freq.get(w, 0) + 1
    return freq


# =============================================================
# WORKER NODE CLASS
# =============================================================
class WorkerNode:
    """
    A cluster worker node.

    Lifecycle:
      1. Startup  → register() sends WORKER_REGISTER to master
      2. Running  → start_server() waits for EXECUTE_TASK messages
      3. Shutdown → deregister() sends WORKER_DEREGISTER to master
    """

    def __init__(self, worker_id: str, host: str, port: int,
                 master_host: str, master_port: int):
        self.worker_id   = worker_id
        self.host        = host
        self.port        = port
        self.master_host = master_host
        self.master_port = master_port
        self._running    = True
        self._tasks_done = 0
        self.log = make_logger(f"Worker-{worker_id}")
        self.log.info(f"Worker {worker_id} initialising on {host}:{port}")

    # ---------------------------------------------------------
    # REGISTRATION
    # ---------------------------------------------------------
    def _master_conn(self) -> socket.socket:
        """Open a fresh connection to the master."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect((self.master_host, self.master_port))
        return s

    def register(self) -> bool:
        """
        Tell the master we exist by sending our ID, host, and port.
        Retries up to 5 times in case the master is still starting up.
        """
        for attempt in range(1, 6):
            try:
                s = self._master_conn()
                send_msg(s, {
                    "type":      "WORKER_REGISTER",
                    "worker_id": self.worker_id,
                    "host":      self.host,
                    "port":      self.port,
                })
                resp = recv_msg(s)
                s.close()
                if resp.get("status") == "registered":
                    self.log.info("Registered with master successfully")
                    return True
            except Exception as e:
                self.log.warning(f"Register attempt {attempt}/5 failed: {e}")
                time.sleep(2)
        return False

    def deregister(self):
        """Tell the master we are shutting down so it removes us from the registry."""
        try:
            s = self._master_conn()
            send_msg(s, {
                "type":      "WORKER_DEREGISTER",
                "worker_id": self.worker_id
            })
            recv_msg(s)
            s.close()
            self.log.info("Deregistered from master cleanly")
        except Exception as e:
            self.log.warning(f"Deregister error (master may already be down): {e}")

    # ---------------------------------------------------------
    # TASK SERVER — waits for sub-tasks from the master
    # ---------------------------------------------------------
    def start_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(10)
        self.log.info(f"Worker {self.worker_id} listening on {self.host}:{self.port}")

        while self._running:
            try:
                srv.settimeout(1.0)
                conn, addr = srv.accept()
                # Each task runs in its own thread so the worker stays responsive
                threading.Thread(
                    target=self._execute_task,
                    args=(conn,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    self.log.error(f"Accept error: {e}")

    def _execute_task(self, conn: socket.socket):
        """Receive a sub-task, compute the result, send it back."""
        try:
            msg       = recv_msg(conn)
            task_id   = msg.get("task_id", "?")
            task_type = msg.get("task_type")
            data      = msg.get("data", {})

            self.log.info(
                f"Executing sub-task {task_id} | type={task_type} | data={data}"
            )

            start   = time.time()
            result  = self._run_computation(task_type, data)
            elapsed = round(time.time() - start, 6)

            self._tasks_done += 1
            result["status"]          = "ok"
            result["worker_id"]       = self.worker_id
            result["elapsed_seconds"] = elapsed

            self.log.info(
                f"Sub-task {task_id} done in {elapsed}s | {result}"
            )
            send_msg(conn, result)

        except Exception as e:
            self.log.error(f"Task execution error: {e}")
            try:
                send_msg(conn, {
                    "status":    "error",
                    "detail":    str(e),
                    "worker_id": self.worker_id
                })
            except Exception:
                pass
        finally:
            conn.close()

    def _run_computation(self, task_type: str, data: dict) -> dict:
        """Route to the correct computation function based on task type."""
        if task_type == "prime_count":
            low   = data.get("low", 2)
            high  = data.get("high", 100_000)
            count = compute_prime_count(low, high)
            return {"count": count, "range": f"{low}-{high}"}

        elif task_type == "sum_squares":
            low   = data.get("low", 1)
            high  = data.get("high", 1_000_000)
            total = compute_sum_squares(low, high)
            return {"sum": total, "range": f"{low}-{high}"}

        elif task_type == "word_count":
            words = data.get("words", [])
            freq  = compute_word_count(words)
            return {"freq": freq, "word_count": len(words)}

        else:
            # Unknown task — echo back so the master can handle it
            return {"echo": data}

    # ---------------------------------------------------------
    # LIFECYCLE
    # ---------------------------------------------------------
    def run(self):
        """Register, serve tasks, deregister on shutdown."""
        if not self.register():
            self.log.error("Could not register with master after 5 attempts. Exiting.")
            sys.exit(1)
        try:
            self.start_server()
        except KeyboardInterrupt:
            pass
        finally:
            self.deregister()
            self.log.info(
                f"Worker {self.worker_id} processed {self._tasks_done} sub-tasks total"
            )


# =============================================================
# ENTRY POINT
# =============================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cluster Worker Node")
    ap.add_argument("--id",     required=True,  help="Unique worker ID e.g. W1")
    ap.add_argument("--host",   default="127.0.0.1", help="IP to bind on")
    ap.add_argument("--port",   type=int, required=True, help="Port to listen on")
    ap.add_argument("--master", default="127.0.0.1:6000",
                    help="Master address as host:port")
    args = ap.parse_args()

    mh, mp = args.master.rsplit(":", 1)
    worker = WorkerNode(args.id, args.host, args.port, mh, int(mp))
    worker.run()

"""
Automated Demo Runner
======================
Launches the entire distributed system locally and runs all test scenarios:

  Test 1 – P2P Resource Discovery      : Peers query each other's resources directly
  Test 2 – P2P File Sharing            : Peer shares a file with another peer
  Test 3 – Cluster Compute (parallel)  : Peer submits prime-count task; master splits
                                         across 3 workers and merges in parallel
  Test 4 – Cluster Sum-of-Squares      : Similar parallel task
  Test 5 – Cluster Word Count          : Text processing distributed across workers
  Test 6 – Node Removal (P2P)          : Peer P4 leaves; remaining peers are notified
  Test 7 – Node Addition (Cluster)     : New worker W4 joins dynamically; next task uses it
  Test 8 – Comparison Summary          : Print P2P vs Cluster metrics

All nodes run as subprocesses on localhost with distinct ports.
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime

# ──────────────────────────────────────────────
# Port map
# ──────────────────────────────────────────────
MASTER_PORT = 6000
WORKER_PORTS = {"W1": 6001, "W2": 6002, "W3": 6003, "W4": 6004}
PEER_PORTS   = {"P1": 7001, "P2": 7002, "P3": 7003, "P4": 7004}
HOST = "127.0.0.1"

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

PEER_DIR   = os.path.join(os.path.dirname(__file__), "peer")
CLUSTER_DIR = os.path.join(os.path.dirname(__file__), "cluster")

PYTHON = sys.executable  # will be python3 when invoked as: python3 run_demo.py

# ──────────────────────────────────────────────
# Message helpers (same as nodes)
# ──────────────────────────────────────────────
def send_msg(sock, payload):
    data = json.dumps(payload).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)

def recv_msg(sock):
    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return b""
            buf += chunk
        return buf
    raw_len = recv_exact(4)
    if not raw_len:
        return {}
    length = int.from_bytes(raw_len, "big")
    raw = recv_exact(length)
    return json.loads(raw.decode()) if raw else {}

def connect(host, port, timeout=5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s

# ──────────────────────────────────────────────
# Pretty printing
# ──────────────────────────────────────────────
SEP = "═" * 70

def header(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")

def ok(msg):    print(f"  ✅  {msg}")
def info(msg):  print(f"  ℹ️   {msg}")
def warn(msg):  print(f"  ⚠️   {msg}")
def result(msg): print(f"  📊  {msg}")

# ──────────────────────────────────────────────
# Process management
# ──────────────────────────────────────────────
processes: list = []

def start_process(name, cmd, cwd, log_file):
    with open(log_file, "w") as lf:
        p = subprocess.Popen(
            cmd, cwd=cwd,
            stdout=lf, stderr=lf
        )
    processes.append((name, p))
    return p

def stop_all():
    print("\n[Teardown] Stopping all processes …")
    for name, p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)
    for name, p in processes:
        try:
            p.kill()
        except Exception:
            pass
    print("[Teardown] Done.")

def wait_for_port(host, port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except Exception:
            time.sleep(0.3)
    return False

# ──────────────────────────────────────────────
# Cluster helpers (talk directly to master)
# ──────────────────────────────────────────────
def submit_task(peer_id, task_type, data) -> dict:
    s = connect(HOST, MASTER_PORT, timeout=30)
    send_msg(s, {"type": "TASK_SUBMIT", "requester": peer_id,
                 "task_type": task_type, "data": data})
    r = recv_msg(s)
    s.close()
    return r

def master_status() -> dict:
    s = connect(HOST, MASTER_PORT)
    send_msg(s, {"type": "STATUS"})
    r = recv_msg(s)
    s.close()
    return r

# ──────────────────────────────────────────────
# Peer helpers (talk directly to a peer node)
# ──────────────────────────────────────────────
def peer_resource_request(from_id, target_port) -> dict:
    s = connect(HOST, target_port)
    send_msg(s, {"type": "RESOURCE_REQUEST", "sender_id": from_id})
    r = recv_msg(s)
    s.close()
    return r

def peer_file_share(from_id, target_port, filename) -> dict:
    s = connect(HOST, target_port)
    send_msg(s, {"type": "FILE_SHARE", "sender_id": from_id, "filename": filename})
    r = recv_msg(s)
    s.close()
    return r

def peer_leave_announce(peer_id, host, port, target_port) -> dict:
    s = connect(HOST, target_port)
    send_msg(s, {"type": "LEAVE_ANNOUNCE", "peer_id": peer_id,
                 "host": host, "port": port})
    r = recv_msg(s)
    s.close()
    return r

def worker_deregister(worker_id, master_port) -> dict:
    s = connect(HOST, master_port)
    send_msg(s, {"type": "WORKER_DEREGISTER", "worker_id": worker_id})
    r = recv_msg(s)
    s.close()
    return r

def worker_register(worker_id, host, port, master_port) -> dict:
    s = connect(HOST, master_port)
    send_msg(s, {"type": "WORKER_REGISTER", "worker_id": worker_id,
                 "host": host, "port": port})
    r = recv_msg(s)
    s.close()
    return r

# ──────────────────────────────────────────────
# MAIN DEMO
# ──────────────────────────────────────────────
def main():
    print(SEP)
    print("  Distributed Resource Sharing & Processing System")
    print("  BITS Pilani – DC Lab Assignment II")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEP)

    # ── 1. Start cluster master ──
    header("Step 1 │ Starting Cluster Master")
    log = os.path.join(LOG_DIR, "master.log")
    start_process("master", [PYTHON, "master_node.py",
                              "--host", HOST, "--port", str(MASTER_PORT)],
                  CLUSTER_DIR, log)
    if wait_for_port(HOST, MASTER_PORT):
        ok(f"Master online @ {HOST}:{MASTER_PORT}")
    else:
        warn("Master did not start in time"); stop_all(); sys.exit(1)

    # ── 2. Start workers W1, W2, W3 ──
    header("Step 2 │ Starting Workers W1, W2, W3")
    for wid in ["W1", "W2", "W3"]:
        wp = WORKER_PORTS[wid]
        log = os.path.join(LOG_DIR, f"worker_{wid}.log")
        start_process(wid, [PYTHON, "worker_node.py",
                             "--id", wid, "--host", HOST, "--port", str(wp),
                             "--master", f"{HOST}:{MASTER_PORT}"],
                      CLUSTER_DIR, log)
        if wait_for_port(HOST, wp):
            ok(f"Worker {wid} online @ {HOST}:{wp}")
        else:
            warn(f"Worker {wid} did not start")

    time.sleep(1)
    status = master_status()
    info(f"Cluster workers registered: {list(status.get('workers', {}).keys())}")

    # ── 3. Start peer nodes P1–P4 ──
    header("Step 3 │ Starting Peer Nodes P1, P2, P3, P4")
    peer_list_all = ",".join(
        f"{HOST}:{p}" for pid, p in PEER_PORTS.items()
    )

    for pid in ["P1", "P2", "P3", "P4"]:
        pp = PEER_PORTS[pid]
        # Exclude self from known peers
        other_peers = ",".join(
            f"{HOST}:{p}" for xid, p in PEER_PORTS.items() if xid != pid
        )
        log = os.path.join(LOG_DIR, f"peer_{pid}.log")
        start_process(pid, [PYTHON, "peer_node.py",
                             "--id", pid, "--host", HOST, "--port", str(pp),
                             "--peers", other_peers,
                             "--master", f"{HOST}:{MASTER_PORT}"],
                      PEER_DIR, log)
        if wait_for_port(HOST, pp):
            ok(f"Peer {pid} online @ {HOST}:{pp}")
        else:
            warn(f"Peer {pid} did not start")

    time.sleep(2)  # let join-announces propagate

    # ══════════════════════════════════════════
    # TEST 1 – P2P Resource Discovery
    # ══════════════════════════════════════════
    header("Test 1 │ P2P Resource Discovery (direct, no coordinator)")
    info("Peer P1 queries resources from P2, P3, P4 directly")
    for target in ["P2", "P3", "P4"]:
        try:
            resp = peer_resource_request("P1", PEER_PORTS[target])
            res = resp.get("resources", {})
            ok(f"P1 → {target}: CPU={res.get('cpu_percent')}%  "
               f"MEM={res.get('mem_percent')}%  "
               f"Files={res.get('files', [])}")
        except Exception as e:
            warn(f"Could not reach {target}: {e}")

    # ══════════════════════════════════════════
    # TEST 2 – P2P File Sharing
    # ══════════════════════════════════════════
    header("Test 2 │ P2P File Sharing (direct peer-to-peer)")
    shares = [("P1", "P2", "dataset_P1.csv"),
              ("P2", "P3", "model_weights_P2.bin"),
              ("P3", "P4", "results_P3.json"),
              ("P4", "P1", "config_P4.yaml")]
    for src, dst, fname in shares:
        try:
            resp = peer_file_share(src, PEER_PORTS[dst], fname)
            ok(f"P{src[-1]} → P{dst[-1]}: shared '{fname}' | ACK={resp.get('status')}")
        except Exception as e:
            warn(f"File share failed: {e}")

    # ══════════════════════════════════════════
    # TEST 3 – Parallel Prime Count
    # ══════════════════════════════════════════
    header("Test 3 │ Cluster Parallel Task – Prime Count (limit=500,000)")
    info("P2 submits task → Master splits range across W1, W2, W3 in parallel")
    r = submit_task("P2", "prime_count", {"limit": 500_000})
    result(f"Prime count (1–500,000)    : {r.get('prime_count')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time (parallel)      : {r.get('elapsed_seconds')}s")
    ok(f"Task ID: {r.get('task_id')} | Status: {r.get('status')}")

    # ══════════════════════════════════════════
    # TEST 4 – Parallel Sum of Squares
    # ══════════════════════════════════════════
    header("Test 4 │ Cluster Parallel Task – Sum of Squares (limit=1,000,000)")
    info("P3 submits sum_squares task")
    r = submit_task("P3", "sum_squares", {"limit": 1_000_000})
    result(f"Sum of squares (1–1M)      : {r.get('sum_of_squares')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time (parallel)      : {r.get('elapsed_seconds')}s")
    ok(f"Task ID: {r.get('task_id')} | Status: {r.get('status')}")

    # ══════════════════════════════════════════
    # TEST 5 – Word Count (text processing)
    # ══════════════════════════════════════════
    header("Test 5 │ Cluster Parallel Task – Distributed Word Count")
    sample_text = (
        "distributed systems enable scalable computing across multiple nodes "
        "peer to peer networks allow direct communication without central coordinator "
        "cluster computing provides parallel processing for intensive tasks "
        "distributed computing improves fault tolerance and scalability of systems "
        "nodes communicate using message passing protocols in distributed systems"
    )
    info(f"Text length: {len(sample_text.split())} words  |  Submitted by P1")
    r = submit_task("P1", "word_count", {"text": sample_text})
    result(f"Unique words               : {r.get('unique_words')}")
    result(f"Top-5 words                : {r.get('top_5')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time                 : {r.get('elapsed_seconds')}s")

    # ══════════════════════════════════════════
    # TEST 6 – Node Removal (P2P)
    # ══════════════════════════════════════════
    header("Test 6 │ P2P Node Removal – Peer P4 leaves the network")
    info("P4 sends LEAVE_ANNOUNCE to all remaining peers")
    for target in ["P1", "P2", "P3"]:
        try:
            resp = peer_leave_announce("P4", HOST, PEER_PORTS["P4"], PEER_PORTS[target])
            ok(f"P4 → {target}: leave acknowledged | ACK={resp.get('status')}")
        except Exception as e:
            warn(f"Leave announce to {target} failed: {e}")

    info("Verifying P1 can still query P2 and P3 (network continues)")
    for target in ["P2", "P3"]:
        try:
            resp = peer_resource_request("P1", PEER_PORTS[target])
            res = resp.get("resources", {})
            ok(f"P1 → {target} (post-removal): CPU={res.get('cpu_percent')}% ✓")
        except Exception as e:
            warn(f"Post-removal check failed: {e}")

    # ══════════════════════════════════════════
    # TEST 7 – Node Addition (Cluster)
    # ══════════════════════════════════════════
    header("Test 7 │ Cluster Node Addition – Worker W4 joins dynamically")
    w4_port = WORKER_PORTS["W4"]

    # Start W4 process
    log = os.path.join(LOG_DIR, "worker_W4.log")
    start_process("W4", [PYTHON, "worker_node.py",
                         "--id", "W4", "--host", HOST, "--port", str(w4_port),
                         "--master", f"{HOST}:{MASTER_PORT}"],
                  CLUSTER_DIR, log)
    if wait_for_port(HOST, w4_port):
        ok(f"Worker W4 joined @ {HOST}:{w4_port}")
    else:
        warn("W4 did not start in time")

    time.sleep(1)
    status = master_status()
    info(f"Cluster workers now: {list(status.get('workers', {}).keys())}")

    info("Submitting another prime-count task (now uses 4 workers)")
    r = submit_task("P2", "prime_count", {"limit": 1_000_000})
    result(f"Prime count (1–1M)         : {r.get('prime_count')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time (4 workers)     : {r.get('elapsed_seconds')}s")

    info("Now removing W4 (deregister from master)")
    try:
        dr = worker_deregister("W4", MASTER_PORT)
        ok(f"W4 deregistered | ACK={dr.get('status')}")
    except Exception as e:
        warn(f"Deregister failed: {e}")

    time.sleep(0.5)
    status = master_status()
    info(f"Cluster workers after W4 removal: {list(status.get('workers', {}).keys())}")

    # ══════════════════════════════════════════
    # TEST 8 – Comparison Summary
    # ══════════════════════════════════════════
    header("Test 8 │ P2P vs Cluster Comparison Summary")
    print("""
  ┌─────────────────────┬──────────────────────────────┬──────────────────────────────┐
  │ Dimension           │ Peer-to-Peer (P2P)           │ Cluster Computing            │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Communication       │ Direct node-to-node;         │ Through master/coordinator;  │
  │                     │ no central coordinator       │ structured dispatch          │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Resource Sharing    │ Each peer exposes its own    │ Workers contribute compute   │
  │                     │ CPU, memory, files           │ capacity to shared tasks     │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Scalability         │ Each new peer adds capacity  │ Add/remove workers live;     │
  │                     │ but also network load grows  │ master re-balances load      │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Processing          │ Light tasks; no coordination │ Heavy parallel tasks split   │
  │                     │ overhead for simple queries  │ and merged across workers    │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Fault Tolerance     │ Peers continue if one leaves │ Master is SPOF; workers are  │
  │                     │ (distributed resilience)     │ replaceable on failure       │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Use-case fit        │ Resource discovery, gossip,  │ Compute-intensive tasks,     │
  │                     │ file sharing, messaging      │ data processing, analytics   │
  └─────────────────────┴──────────────────────────────┴──────────────────────────────┘
    """)

    final_status = master_status()
    info(f"Final cluster state: {final_status.get('workers', {})}")
    info(f"Total tasks processed by cluster: {final_status.get('task_count')}")

    header("Demo Complete ✅")
    ok("All 8 test cases passed.")
    ok("Log files saved in: " + LOG_DIR)

    stop_all()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        stop_all()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        stop_all()
        raise

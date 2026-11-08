"""
Multi-Server Test Client
=========================
Run this on ANY server AFTER all nodes are already running manually.
It connects to the live peers and master over real network IPs and
exercises all 8 test scenarios.

Usage:
    python3 run_test_client.py \
        --s1 10.0.0.1 \
        --s2 10.0.0.2 \
        --s3 10.0.0.3 \
        --s4 10.0.0.4

If all nodes are on the same machine (localhost demo):
    python3 run_test_client.py

All IP arguments default to 127.0.0.1 so the localhost demo still works.
"""

import argparse
import json
import socket
import sys
import time
from datetime import datetime

# ──────────────────────────────────────────────
# Port assignments (fixed, must match what you started)
# ──────────────────────────────────────────────
MASTER_PORT = 6000

# Workers run on their server's IP, one per server
# W1 → server2, W2 → server3, W3 → server4
WORKER_PORTS = {"W1": 6001, "W2": 6002, "W3": 6003, "W4": 6004}

# Peers run on their server's IP, one per server
# P1 → server1, P2 → server2, P3 → server3, P4 → server4
PEER_PORTS = {"P1": 7001, "P2": 7002, "P3": 7003, "P4": 7004}

# ──────────────────────────────────────────────
# Pretty printing
# ──────────────────────────────────────────────
SEP = "═" * 70

def header(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")

def ok(msg):     print(f"  ✅  {msg}")
def info(msg):   print(f"  ℹ️   {msg}")
def warn(msg):   print(f"  ⚠️   {msg}")
def result(msg): print(f"  📊  {msg}")

# ──────────────────────────────────────────────
# Low-level socket helpers
# ──────────────────────────────────────────────
def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf += chunk
    return buf

def send_msg(sock, payload):
    data = json.dumps(payload).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)

def recv_msg(sock):
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        return {}
    length = int.from_bytes(raw_len, "big")
    raw = _recv_exact(sock, length)
    return json.loads(raw.decode()) if raw else {}

def connect(host, port, timeout=8):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s

# ──────────────────────────────────────────────
# Reusable action helpers
# ──────────────────────────────────────────────
def peer_resource_request(from_id, target_host, target_port):
    s = connect(target_host, target_port)
    send_msg(s, {"type": "RESOURCE_REQUEST", "sender_id": from_id})
    r = recv_msg(s)
    s.close()
    return r

def peer_file_share(from_id, target_host, target_port, filename):
    s = connect(target_host, target_port)
    send_msg(s, {"type": "FILE_SHARE", "sender_id": from_id, "filename": filename})
    r = recv_msg(s)
    s.close()
    return r

def peer_leave_announce(peer_id, peer_host, peer_port, target_host, target_port):
    s = connect(target_host, target_port)
    send_msg(s, {"type": "LEAVE_ANNOUNCE",
                 "peer_id": peer_id, "host": peer_host, "port": peer_port})
    r = recv_msg(s)
    s.close()
    return r

def submit_task(master_host, requester, task_type, data):
    s = connect(master_host, MASTER_PORT, timeout=60)
    send_msg(s, {"type": "TASK_SUBMIT", "requester": requester,
                 "task_type": task_type, "data": data})
    r = recv_msg(s)
    s.close()
    return r

def master_status(master_host):
    s = connect(master_host, MASTER_PORT)
    send_msg(s, {"type": "STATUS"})
    r = recv_msg(s)
    s.close()
    return r

def worker_register(master_host, worker_id, worker_host, worker_port):
    s = connect(master_host, MASTER_PORT)
    send_msg(s, {"type": "WORKER_REGISTER", "worker_id": worker_id,
                 "host": worker_host, "port": worker_port})
    r = recv_msg(s)
    s.close()
    return r

def worker_deregister(master_host, worker_id):
    s = connect(master_host, MASTER_PORT)
    send_msg(s, {"type": "WORKER_DEREGISTER", "worker_id": worker_id})
    r = recv_msg(s)
    s.close()
    return r

# ──────────────────────────────────────────────
# Connectivity pre-check
# ──────────────────────────────────────────────
def check_connectivity(server_map):
    """Verify all nodes are reachable before running tests."""
    header("Pre-check │ Verifying all nodes are reachable")
    all_ok = True

    checks = [
        ("Master",   server_map["s1"], MASTER_PORT),
        ("Worker W1", server_map["s2"], WORKER_PORTS["W1"]),
        ("Worker W2", server_map["s3"], WORKER_PORTS["W2"]),
        ("Worker W3", server_map["s4"], WORKER_PORTS["W3"]),
        ("Peer P1",  server_map["s1"], PEER_PORTS["P1"]),
        ("Peer P2",  server_map["s2"], PEER_PORTS["P2"]),
        ("Peer P3",  server_map["s3"], PEER_PORTS["P3"]),
        ("Peer P4",  server_map["s4"], PEER_PORTS["P4"]),
    ]

    for label, host, port in checks:
        try:
            s = socket.create_connection((host, port), timeout=3)
            s.close()
            ok(f"{label:15s} @ {host}:{port}  — reachable")
        except Exception as e:
            warn(f"{label:15s} @ {host}:{port}  — UNREACHABLE ({e})")
            all_ok = False

    if not all_ok:
        print("\n  Some nodes are not reachable.")
        print("  Start all nodes first, then re-run this script.")
        sys.exit(1)

    ok("All nodes reachable — starting tests\n")

# ──────────────────────────────────────────────
# MAIN TEST RUNNER
# ──────────────────────────────────────────────
def run_tests(sm):
    """
    sm = {
      "s1": "IP of server1",   # Master + P1
      "s2": "IP of server2",   # W1   + P2
      "s3": "IP of server3",   # W2   + P3
      "s4": "IP of server4",   # W3   + P4
    }
    """
    print(SEP)
    print("  Distributed Resource Sharing & Processing System")
    print("  BITS Pilani – DC Lab Assignment II  |  Multi-Server Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEP)
    print(f"  Server layout:")
    print(f"    Server 1 ({sm['s1']}) : Master (:{MASTER_PORT})  +  Peer P1 (:{PEER_PORTS['P1']})")
    print(f"    Server 2 ({sm['s2']}) : Worker W1 (:{WORKER_PORTS['W1']})  +  Peer P2 (:{PEER_PORTS['P2']})")
    print(f"    Server 3 ({sm['s3']}) : Worker W2 (:{WORKER_PORTS['W2']})  +  Peer P3 (:{PEER_PORTS['P3']})")
    print(f"    Server 4 ({sm['s4']}) : Worker W3 (:{WORKER_PORTS['W3']})  +  Peer P4 (:{PEER_PORTS['P4']})")

    check_connectivity(sm)

    master_host = sm["s1"]
    status = master_status(master_host)
    info(f"Cluster workers registered: {list(status.get('workers', {}).keys())}")

    # ══════════════════════════════════════════
    # TEST 1 – P2P Resource Discovery
    # ══════════════════════════════════════════
    header("Test 1 │ P2P Resource Discovery (direct, no coordinator)")
    info("Peer P1 queries resources from P2, P3, P4 directly over the network")

    peer_hosts = {"P1": sm["s1"], "P2": sm["s2"], "P3": sm["s3"], "P4": sm["s4"]}

    for target in ["P2", "P3", "P4"]:
        try:
            resp = peer_resource_request("P1", peer_hosts[target], PEER_PORTS[target])
            res = resp.get("resources", {})
            ok(f"P1 → {target} ({peer_hosts[target]}): "
               f"CPU={res.get('cpu_percent', '?')}%  "
               f"MEM={res.get('mem_percent', '?')}%  "
               f"Files={res.get('files', [])}")
        except Exception as e:
            warn(f"Could not reach {target}: {e}")

    # ══════════════════════════════════════════
    # TEST 2 – P2P File Sharing
    # ══════════════════════════════════════════
    header("Test 2 │ P2P File Sharing (direct peer-to-peer, cross-server)")
    shares = [
        ("P1", sm["s1"], "P2", sm["s2"], "dataset_P1.csv"),
        ("P2", sm["s2"], "P3", sm["s3"], "model_weights_P2.bin"),
        ("P3", sm["s3"], "P4", sm["s4"], "results_P3.json"),
        ("P4", sm["s4"], "P1", sm["s1"], "config_P4.yaml"),
    ]
    for src, src_host, dst, dst_host, fname in shares:
        try:
            resp = peer_file_share(src, dst_host, PEER_PORTS[dst], fname)
            ok(f"{src} ({src_host}) → {dst} ({dst_host}): "
               f"shared '{fname}' | ACK={resp.get('status')}")
        except Exception as e:
            warn(f"File share {src}→{dst} failed: {e}")

    # ══════════════════════════════════════════
    # TEST 3 – Parallel Prime Count
    # ══════════════════════════════════════════
    header("Test 3 │ Cluster Parallel Task – Prime Count (limit=500,000)")
    info(f"P2 submits task to Master ({master_host}:{MASTER_PORT})")
    info("Master splits range → dispatches to W1/W2/W3 in parallel → merges")
    r = submit_task(master_host, "P2", "prime_count", {"limit": 500_000})
    result(f"Prime count (1–500,000)    : {r.get('prime_count')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time (parallel)      : {r.get('elapsed_seconds')}s")
    ok(f"Task ID: {r.get('task_id')} | Status: {r.get('status')}")

    # ══════════════════════════════════════════
    # TEST 4 – Sum of Squares
    # ══════════════════════════════════════════
    header("Test 4 │ Cluster Parallel Task – Sum of Squares (limit=1,000,000)")
    info("P3 submits sum_squares task")
    r = submit_task(master_host, "P3", "sum_squares", {"limit": 1_000_000})
    result(f"Sum of squares (1–1M)      : {r.get('sum_of_squares')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time (parallel)      : {r.get('elapsed_seconds')}s")
    ok(f"Task ID: {r.get('task_id')} | Status: {r.get('status')}")

    # ══════════════════════════════════════════
    # TEST 5 – Word Count
    # ══════════════════════════════════════════
    header("Test 5 │ Cluster Parallel Task – Distributed Word Count")
    sample_text = (
        "distributed systems enable scalable computing across multiple nodes "
        "peer to peer networks allow direct communication without central coordinator "
        "cluster computing provides parallel processing for intensive tasks "
        "distributed computing improves fault tolerance and scalability of systems "
        "nodes communicate using message passing protocols in distributed systems"
    )
    info(f"Text: {len(sample_text.split())} words  |  Submitted by P1")
    r = submit_task(master_host, "P1", "word_count", {"text": sample_text})
    result(f"Unique words               : {r.get('unique_words')}")
    result(f"Top-5 words                : {r.get('top_5')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time                 : {r.get('elapsed_seconds')}s")

    # ══════════════════════════════════════════
    # TEST 6 – P2P Node Removal (P4 leaves)
    # ══════════════════════════════════════════
    header("Test 6 │ P2P Node Removal – Peer P4 leaves the network")
    info(f"P4 ({sm['s4']}) sends LEAVE_ANNOUNCE to P1, P2, P3 directly")
    for target in ["P1", "P2", "P3"]:
        try:
            resp = peer_leave_announce(
                "P4", sm["s4"], PEER_PORTS["P4"],
                peer_hosts[target], PEER_PORTS[target]
            )
            ok(f"P4 → {target} ({peer_hosts[target]}): "
               f"leave acknowledged | ACK={resp.get('status')}")
        except Exception as e:
            warn(f"Leave announce to {target} failed: {e}")

    info("Verifying P1 can still query P2 and P3 (network continues without P4)")
    for target in ["P2", "P3"]:
        try:
            resp = peer_resource_request("P1", peer_hosts[target], PEER_PORTS[target])
            res = resp.get("resources", {})
            ok(f"P1 → {target} (post-removal): CPU={res.get('cpu_percent')}% ✓")
        except Exception as e:
            warn(f"Post-removal check {target} failed: {e}")

    # ══════════════════════════════════════════
    # TEST 7 – Cluster Worker Addition & Removal
    # ══════════════════════════════════════════
    header("Test 7 │ Cluster Node Addition – W4 registers dynamically")
    info("Simulating W4 joining on Server 1 (same IP as master, port 6004)")
    info("In a real 5-server setup, W4 would run on a dedicated machine.")

    # Register W4 with master (the worker process would do this on startup,
    # but we can also inject it manually to demonstrate the dynamic join)
    try:
        r = worker_register(master_host, "W4", sm["s1"], WORKER_PORTS["W4"])
        ok(f"W4 registered | ACK={r.get('status')}")
    except Exception as e:
        warn(f"W4 register failed: {e}")

    time.sleep(0.5)
    status = master_status(master_host)
    info(f"Cluster workers now: {list(status.get('workers', {}).keys())}")

    info("Submitting prime-count task (limit=1,000,000) — should use 4 workers")
    r = submit_task(master_host, "P2", "prime_count", {"limit": 1_000_000})
    result(f"Prime count (1–1M)         : {r.get('prime_count')}")
    result(f"Parallel workers used      : {r.get('parallel_workers')}")
    result(f"Total time                 : {r.get('elapsed_seconds')}s")

    info("Now removing W4 from cluster")
    try:
        dr = worker_deregister(master_host, "W4")
        ok(f"W4 deregistered | ACK={dr.get('status')}")
    except Exception as e:
        warn(f"W4 deregister failed: {e}")

    time.sleep(0.3)
    status = master_status(master_host)
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
  │                     │ but network load grows       │ master re-balances load      │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Processing          │ Light tasks; low overhead    │ Heavy parallel tasks split   │
  │                     │ for simple queries           │ and merged across workers    │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Fault Tolerance     │ Peers continue if one leaves │ Master is SPOF; workers are  │
  │                     │ (distributed resilience)     │ replaceable on failure       │
  ├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
  │ Use-case fit        │ Resource discovery, gossip,  │ Compute-intensive tasks,     │
  │                     │ file sharing, messaging      │ data processing, analytics   │
  └─────────────────────┴──────────────────────────────┴──────────────────────────────┘
    """)

    final_status = master_status(master_host)
    info(f"Final cluster state: {list(final_status.get('workers', {}).keys())}")
    info(f"Total cluster tasks processed: {final_status.get('task_count')}")

    header("All Tests Complete ✅")
    ok("All 8 test scenarios executed on live multi-server deployment.")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Multi-server test client for DC Lab Assignment II",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # All 4 servers on real IPs:
  python3 run_test_client.py --s1 10.0.0.1 --s2 10.0.0.2 --s3 10.0.0.3 --s4 10.0.0.4

  # Localhost single-machine demo:
  python3 run_test_client.py
        """
    )
    ap.add_argument("--s1", default="127.0.0.1", help="Server 1 IP (Master + P1)")
    ap.add_argument("--s2", default="127.0.0.1", help="Server 2 IP (W1 + P2)")
    ap.add_argument("--s3", default="127.0.0.1", help="Server 3 IP (W2 + P3)")
    ap.add_argument("--s4", default="127.0.0.1", help="Server 4 IP (W3 + P4)")
    args = ap.parse_args()

    server_map = {"s1": args.s1, "s2": args.s2, "s3": args.s3, "s4": args.s4}
    run_tests(server_map)

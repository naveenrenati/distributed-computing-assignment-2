#!/bin/bash
# ============================================================
# Run on SERVER 4  →  Worker W3 + Peer P4
# Usage: bash start_server4.sh <S1_IP> <S2_IP> <S3_IP> <S4_IP>
# ============================================================

S1=${1:-127.0.0.1}
S2=${2:-127.0.0.1}
S3=${3:-127.0.0.1}
S4=${4:-127.0.0.1}

echo "=== Server 4: Worker W3 ($S4:6003) + Peer P4 ($S4:7004) ==="
echo "Master: $S1:6000"
echo ""

python3 ~/DC/cluster/worker_node.py \
    --id W3 --host 0.0.0.0 --port 6003 \
    --master $S1:6000 &
W_PID=$!
echo "Worker W3 started (PID $W_PID)"
sleep 1

python3 ~/DC/peer/peer_node.py \
    --id P4 --host 0.0.0.0 --port 7004 \
    --peers $S1:7001,$S2:7002,$S3:7003 \
    --master $S1:6000 &
P_PID=$!
echo "Peer P4 started (PID $P_PID)"

echo ""
echo "Server 4 is running. Press Ctrl+C to stop."
wait

#!/bin/bash
# ============================================================
# Run on SERVER 2  →  Worker W1 + Peer P2
# Usage: bash start_server2.sh <S1_IP> <S2_IP> <S3_IP> <S4_IP>
# ============================================================

S1=${1:-127.0.0.1}
S2=${2:-127.0.0.1}
S3=${3:-127.0.0.1}
S4=${4:-127.0.0.1}

echo "=== Server 2: Worker W1 ($S2:6001) + Peer P2 ($S2:7002) ==="
echo "Master: $S1:6000"
echo ""

python3 ~/DC/cluster/worker_node.py \
    --id W1 --host 0.0.0.0 --port 6001 \
    --master $S1:6000 &
W_PID=$!
echo "Worker W1 started (PID $W_PID)"
sleep 1

python3 ~/DC/peer/peer_node.py \
    --id P2 --host 0.0.0.0 --port 7002 \
    --peers $S1:7001,$S3:7003,$S4:7004 \
    --master $S1:6000 &
P_PID=$!
echo "Peer P2 started (PID $P_PID)"

echo ""
echo "Server 2 is running. Press Ctrl+C to stop."
wait

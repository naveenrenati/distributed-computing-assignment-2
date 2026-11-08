#!/bin/bash
# ============================================================
# Run on SERVER 3  →  Worker W2 + Peer P3
# Usage: bash start_server3.sh <S1_IP> <S2_IP> <S3_IP> <S4_IP>
# ============================================================

S1=${1:-127.0.0.1}
S2=${2:-127.0.0.1}
S3=${3:-127.0.0.1}
S4=${4:-127.0.0.1}

echo "=== Server 3: Worker W2 ($S3:6002) + Peer P3 ($S3:7003) ==="
echo "Master: $S1:6000"
echo ""

python3 ~/DC/cluster/worker_node.py \
    --id W2 --host 0.0.0.0 --port 6002 \
    --master $S1:6000 &
W_PID=$!
echo "Worker W2 started (PID $W_PID)"
sleep 1

python3 ~/DC/peer/peer_node.py \
    --id P3 --host 0.0.0.0 --port 7003 \
    --peers $S1:7001,$S2:7002,$S4:7004 \
    --master $S1:6000 &
P_PID=$!
echo "Peer P3 started (PID $P_PID)"

echo ""
echo "Server 3 is running. Press Ctrl+C to stop."
wait

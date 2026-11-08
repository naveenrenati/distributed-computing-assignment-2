#!/bin/bash
# ============================================================
# Run on SERVER 1  →  Cluster Master + Peer P1
# Usage: bash start_server1.sh <S1_IP> <S2_IP> <S3_IP> <S4_IP>
# Example: bash start_server1.sh 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4
# ============================================================

S1=${1:-127.0.0.1}
S2=${2:-127.0.0.1}
S3=${3:-127.0.0.1}
S4=${4:-127.0.0.1}

echo "=== Server 1: Master ($S1:6000) + Peer P1 ($S1:7001) ==="
echo "Known peers: P2=$S2:7002, P3=$S3:7003, P4=$S4:7004"
echo ""

# Start master in background
python3 ~/DC/cluster/master_node.py --host 0.0.0.0 --port 6000 &
MASTER_PID=$!
echo "Master started (PID $MASTER_PID)"
sleep 2

# Start peer P1 in foreground
python3 ~/DC/peer/peer_node.py \
    --id P1 --host 0.0.0.0 --port 7001 \
    --peers $S2:7002,$S3:7003,$S4:7004 \
    --master $S1:6000 &
PEER_PID=$!
echo "Peer P1 started (PID $PEER_PID)"

echo ""
echo "Server 1 is running. Press Ctrl+C to stop."
wait

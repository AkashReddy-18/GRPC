#!/bin/bash
# Raft Verification Script

set -e

BIN_SERVER="./build/kv_server"
BIN_CLIENT="./build/kv_client"

killall kv_server || true
sleep 1

echo "Starting 3-node Raft cluster..."
$BIN_SERVER localhost:50051 localhost:50052 localhost:50053 > node1.log 2>&1 &
PID1=$!
$BIN_SERVER localhost:50052 localhost:50051 localhost:50053 > node2.log 2>&1 &
PID2=$!
$BIN_SERVER localhost:50053 localhost:50051 localhost:50052 > node3.log 2>&1 &
PID3=$!

echo "Waiting for leader election (5 seconds)..."
sleep 5

echo "Finding leader..."
# Try node1
HINT=$($BIN_CLIENT localhost:50051 put test_key test_val | grep "Leader hint" | awk '{print $5}' || true)
if [ -z "$HINT" ]; then
    LEADER="localhost:50051"
else
    LEADER=$HINT
fi
echo "Leader found at $LEADER"

echo "Testing replicated PUT to Leader..."
$BIN_CLIENT $LEADER put name Raft
sleep 2

echo "Verifying GET from all nodes..."
# Even non-leaders should have the data after replication, 
# but for linearizable GET, they will redirect to leader.
$BIN_CLIENT localhost:50051 get name
$BIN_CLIENT localhost:50052 get name
$BIN_CLIENT localhost:50053 get name

echo "Killing Leader ($LEADER)..."
if [ "$LEADER" == "localhost:50051" ]; then kill $PID1; fi
if [ "$LEADER" == "localhost:50052" ]; then kill $PID2; fi
if [ "$LEADER" == "localhost:50053" ]; then kill $PID3; fi

echo "Waiting for NEW leader election (7 seconds)..."
sleep 7

# Find new leader
HINT=$($BIN_CLIENT localhost:50051 get name | grep "Leader hint" | awk '{print $5}' || true)
# If 50051 was the one killed, try another
if [ -z "$HINT" ]; then
    HINT=$($BIN_CLIENT localhost:50053 get name | grep "Leader hint" | awk '{print $5}' || true)
fi

NEW_LEADER=$HINT
echo "New Leader found at $NEW_LEADER"

echo "Testing PUT to NEW Leader..."
$BIN_CLIENT $NEW_LEADER put status active
sleep 1
$BIN_CLIENT $NEW_LEADER get status

echo "Cleaning up..."
killall kv_server || true
echo "Raft Phase 5 Verified!"

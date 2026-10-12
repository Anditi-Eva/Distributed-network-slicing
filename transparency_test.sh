#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 9 TEST SUITE
# TRANSPARENCY & RELIABILITY BENCHMARK
# ==============================================================================

GATEWAY_URL="http://127.0.0.1:5000"
PRIMARY_WORKER="http://127.0.0.1:5001"

echo "=================================================================="
echo " Starting Milestone 9 Transparency & Reliability Experiments"
echo " Logical Service URI: $GATEWAY_URL/api/v1/slice"
echo "=================================================================="

# 1. Access & Location Transparency Test
echo ""
echo "[1/4] Testing Access & Location Transparency..."
RESP=$(curl -s -X POST "$GATEWAY_URL/api/v1/slice" \
            -H "Content-Type: application/json" \
            -d '{"slice_id": "logical_slice_101", "cpu": 0.5, "ram": 256}')

echo " Client Request Response:"
echo " $RESP" | grep "COMMITTED" && echo " -> SUCCESS: Client interacted strictly with Logical Resource URI!"

# 2. Failure Transparency Test (Inject Crash on Primary Worker)
echo ""
echo "[2/4] Testing Failure Transparency (Injecting Crash on Primary Worker)..."

# Inject crash fault into worker agent directly
curl -s -X POST "$PRIMARY_WORKER/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "crash"}' > /dev/null

echo " -> Primary Worker (5001) is CRASHED. Submitting new slice request to Gateway..."

# Request through gateway while primary is dead
FAIL_RESP=$(curl -s -X POST "$GATEWAY_URL/api/v1/slice" \
                 -H "Content-Type: application/json" \
                 -d '{"slice_id": "logical_slice_102", "cpu": 0.5, "ram": 256}')

echo " Gateway Response during Node Crash:"
echo " $FAIL_RESP"

# Reset Primary Worker State
curl -s -X POST "$PRIMARY_WORKER/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "reset"}' > /dev/null

# 3. Migration Transparency Test
echo ""
echo "[3/4] Testing Migration Transparency..."
MIG_RESP=$(curl -s -X POST "$GATEWAY_URL/api/v1/slice/migrate" \
                 -H "Content-Type: application/json" \
                 -d '{"slice_id": "logical_slice_101", "target_node_url": "http://127.0.0.1:5001"}')

echo " Gateway Migration Response:"
echo " $MIG_RESP"

# 4. Overhead Analysis (Measuring Gateway Latency vs Direct API Latency)
echo ""
echo "[4/4] Measuring Transparency Latency Overhead..."

T1_START=$(python3 -c 'import time; print(int(time.time() * 1000))')
curl -s -X POST "$PRIMARY_WORKER/resource/request" \
     -H "Content-Type: application/json" \
     -d '{"process_id": "direct_01", "cpu": 0.1, "ram": 64}' > /dev/null
T1_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
DIRECT_TIME=$(( T1_END - T1_START ))

T2_START=$(python3 -c 'import time; print(int(time.time() * 1000))')
curl -s -X POST "$GATEWAY_URL/api/v1/slice" \
     -H "Content-Type: application/json" \
     -d '{"slice_id": "transparent_01", "cpu": 0.1, "ram": 64}' > /dev/null
T2_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
GATEWAY_TIME=$(( T2_END - T2_START ))

OVERHEAD=$(( GATEWAY_TIME - DIRECT_TIME ))

echo "=================================================================="
echo " MILESTONE 9 EXPERIMENTAL OVERHEAD SUMMARY"
echo "=================================================================="
echo " Direct Node Access Latency   : ${DIRECT_TIME} ms"
echo " Transparent Gateway Latency  : ${GATEWAY_TIME} ms"
echo " Transparency Layer Overhead  : ${OVERHEAD} ms"
echo "=================================================================="

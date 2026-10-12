#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 7 TEST SUITE
# FAULT-INJECTION & RECOVERY BENCHMARKING (MTTR & AVAILABILITY)
# ==============================================================================

WORKER_URL="http://127.0.0.1:5001"

echo "=================================================================="
echo " Starting Milestone 7 Fault Tolerance & Recovery Benchmark"
echo " Target Endpoint: $WORKER_URL"
echo "=================================================================="

# Function to ping worker agent health
check_health() {
  local response
  response=$(curl -s --max-time 2 "$WORKER_URL/health")
  echo "$response" | grep -q '"status":"UP"'
  return $?
}

# Ensure baseline worker agent is UP before starting
echo -n "[0/3] Checking Baseline Worker Node Health... "
if check_health; then
  echo "OK (Worker Agent is Healthy)"
else
  echo "FAILED!"
  echo "Error: worker_agent.py is not running on port 5001. Please start it first!"
  exit 1
fi

# ------------------------------------------------------------------------------
# EXPERIMENT 1: CRASH FAILURE & RECOVERY TIME (MTTR) MEASUREMENT
# ------------------------------------------------------------------------------
echo ""
echo "[1/3] Injecting Process Crash Failure..."

T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

# Trigger Chaos Crash State
curl -s -X POST "$WORKER_URL/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "crash"}' > /dev/null

echo " -> Crash state active. Heartbeat checking for failure state..."

if ! check_health; then
  echo " -> Failure detected by heartbeat monitor!"
fi

echo -n " -> Executing Automated Recovery Reset... "

# Trigger Auto-Recovery
curl -s -X POST "$WORKER_URL/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "reset"}' > /dev/null

until check_health; do
  sleep 0.1
done

T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
MTTR=$(( T_END - T_START ))

echo "RECOVERED!"
echo " -> Experiment 1 Result: Mean Time To Recovery (MTTR) = ${MTTR} ms"

# ------------------------------------------------------------------------------
# EXPERIMENT 2: OMISSION / TIMEOUT FAILURE TEST
# ------------------------------------------------------------------------------
echo ""
echo "[2/3] Injecting Communication Omission (Packet Drop / Timeout)..."

curl -s -X POST "$WORKER_URL/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "omission"}' > /dev/null

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$WORKER_URL/health")

if [ "$HTTP_CODE" -eq 000 ] || [ "$HTTP_CODE" -eq 504 ]; then
  echo " -> Omission fault verified (Response timed out / Gateway Timeout $HTTP_CODE)."
fi

# Reset state
curl -s -X POST "$WORKER_URL/chaos/inject" \
     -H "Content-Type: application/json" \
     -d '{"failure_type": "reset"}' > /dev/null
echo " -> Communication paths reset to NORMAL."

# ------------------------------------------------------------------------------
# EXPERIMENT 3: QUANTITATIVE SYSTEM AVAILABILITY BENCHMARK
# ------------------------------------------------------------------------------
echo ""
echo "[3/3] Running 30-Second Real-Time Availability Measurement..."
TOTAL_CHECKS=30
UP_COUNT=0

for i in $(seq 1 $TOTAL_CHECKS); do
  if check_health; then
    UP_COUNT=$((UP_COUNT + 1))
    echo -n "."
  else
    echo -n "X"
  fi
  sleep 1
done

echo ""
AVAILABILITY=$(awk "BEGIN {printf \"%.2f\", ($UP_COUNT / $TOTAL_CHECKS) * 100}")

# Push calculated MTTR & Availability to Prometheus gauges
curl -s -X POST "$WORKER_URL/chaos/inject" \
     -H "Content-Type: application/json" \
     -d "{\"failure_type\": \"reset\", \"mttr_ms\": $MTTR, \"availability\": $AVAILABILITY}" > /dev/null

echo "=================================================================="
echo " MILESTONE 7 EXPERIMENTAL SUMMARY RESULTS"
echo "=================================================================="
echo " Total Observation Period : ${TOTAL_CHECKS} seconds"
echo " Successful Health Checks : ${UP_COUNT} / ${TOTAL_CHECKS}"
echo " Calculated MTTR          : ${MTTR} ms"
echo " Measured System Availability (A) : ${AVAILABILITY} %"
echo "=================================================================="

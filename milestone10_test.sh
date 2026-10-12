#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 10 TEST SUITE
# NAMING & DISTRIBUTED PROCESS MANAGEMENT BENCHMARK
# ==============================================================================

NAMING_SERVICE="http://127.0.0.1:5003"
WORKER_SERVICE="http://127.0.0.1:5001"

echo "=================================================================="
echo " Starting Milestone 10 Naming & Process Management Benchmark"
echo " Target Naming Engine: $NAMING_SERVICE"
echo "=================================================================="

# Ensure Naming Engine is UP
echo -n "[0/4] Checking Naming Subsystem Health... "
if curl -s "$NAMING_SERVICE/metrics" > /dev/null; then
  echo "OK (Naming Engine is Online on Port 5003)"
else
  echo "FAILED!"
  echo "Error: naming_process_manager.py is not running. Start it first!"
  exit 1
fi

# ------------------------------------------------------------------------------
# EXPERIMENT 1: NAME RESOLUTION (Direct vs Prefix Table)
# ------------------------------------------------------------------------------
echo ""
echo "[1/4] Testing Naming Architecture (Name -> Resolution -> Location)..."

# Direct DNS Lookup
DIRECT_RESP=$(curl -s -X POST "$NAMING_SERVICE/dns/resolve" \
                   -H "Content-Type: application/json" \
                   -d '{"name": "urllc.slice.telecom.local"}')
echo " Direct Name Server Lookup Result:"
echo " $DIRECT_RESP"

# Prefix Table Search
PREFIX_RESP=$(curl -s -X POST "$NAMING_SERVICE/dns/resolve" \
                   -H "Content-Type: application/json" \
                   -d '{"name": "embb.slice.zoneA.telecom.local"}')
echo " Prefix Table Search Result:"
echo " $PREFIX_RESP"

# ------------------------------------------------------------------------------
# EXPERIMENT 2: PROCESS LIFECYCLE MANAGEMENT (Create -> Locate -> Schedule -> Coordinate -> Terminate)
# ------------------------------------------------------------------------------
echo ""
echo "[2/4] Testing 5-Stage Distributed Process Lifecycle Management..."

PROC_RESP=$(curl -s -X POST "$NAMING_SERVICE/process/orchestrate" \
                 -H "Content-Type: application/json" \
                 -d '{"slice_name": "urllc.slice.telecom.local", "cpu": 0.2, "ram": 128}')
echo " Process Lifecycle Orchestration Result:"
echo " $PROC_RESP"

# ------------------------------------------------------------------------------
# EXPERIMENT 3: RESOLUTION LATENCY ANALYSIS
# ------------------------------------------------------------------------------
echo ""
echo "[3/4] Running Resolution Latency Analysis (100 Lookups)..."

T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')
for i in {1..100}; do
  curl -s -X POST "$NAMING_SERVICE/dns/resolve" \
       -H "Content-Type: application/json" \
       -d '{"name": "urllc.slice.telecom.local"}' > /dev/null
done
T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')

TOTAL_LATENCY=$(( T_END - T_START ))
AVG_LATENCY=$(awk "BEGIN {printf \"%.3f\", $TOTAL_LATENCY / 100}")

echo " Total Duration for 100 Resolutions : ${TOTAL_LATENCY} ms"
echo " Mean Resolution Latency ($L_{res}$)     : ${AVG_LATENCY} ms / lookup"

# ------------------------------------------------------------------------------
# EXPERIMENT 4: SCALABILITY ANALYSIS (Concurrent Process Orchestration)
# ------------------------------------------------------------------------------
echo ""
echo "[4/4] Running Scalability Benchmark (Concurrent Process Orchestrations)..."

T_SCALE_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

for i in {1..20}; do
  curl -s -X POST "$NAMING_SERVICE/process/orchestrate" \
       -H "Content-Type: application/json" \
       -d "{\"slice_name\": \"urllc.slice.telecom.local\", \"cpu\": 0.1, \"ram\": 64}" > /dev/null &
done
wait

T_SCALE_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
TOTAL_SCALE_TIME=$(( T_SCALE_END - T_SCALE_START ))
THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", (20 / ($TOTAL_SCALE_TIME / 1000))}")

echo "=================================================================="
echo " MILESTONE 10 EXPERIMENTAL SUMMARY RESULTS"
echo "=================================================================="
echo " Resolution Latency (L_res)      : ${AVG_LATENCY} ms / request"
echo " Orchestration Throughput (T)    : ${THROUGHPUT} processes / sec"
echo " Scalability Batch Time (20 pids): ${TOTAL_SCALE_TIME} ms"
echo "=================================================================="

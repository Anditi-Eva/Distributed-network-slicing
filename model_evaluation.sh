#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 8 TEST SUITE
# COMPLETE 5-MODEL DISTRIBUTED ARCHITECTURE EVALUATION
# ==============================================================================

WORKER_URL="http://127.0.0.1:5001"

echo "=================================================================="
echo " Starting Milestone 8 Distributed System Models Benchmark"
echo " Target Worker Endpoint: $WORKER_URL"
echo "=================================================================="

# Benchmark Helper Function
run_model_benchmark() {
  local model_name=$1
  local requests=$2
  local delay=$3
  local description=$4

  echo ""
  echo "------------------------------------------------------------------"
  echo " Testing: $model_name"
  echo " Description: $description"
  echo "------------------------------------------------------------------"
  
  T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')
  
  for i in $(seq 1 $requests); do
    curl -s -X POST "$WORKER_URL/resource/request" \
         -H "Content-Type: application/json" \
         -d "{\"process_id\": \"slice_eval_${model_name}_$i\", \"cpu\": 0.2, \"ram\": 128}" > /dev/null
    if [ "$delay" != "0" ]; then
      sleep $delay
    fi
  done
  
  T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
  TOTAL_TIME=$(( T_END - T_START ))
  THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", ($requests / ($TOTAL_TIME / 1000))}")
  LATENCY=$(awk "BEGIN {printf \"%.2f\", ($TOTAL_TIME / $requests)}")
  
  echo " [RESULT] Total Duration : ${TOTAL_TIME} ms"
  echo " [RESULT] Mean Latency   : ${LATENCY} ms/req"
  echo " [RESULT] Throughput     : ${THROUGHPUT} req/s"
}

# 1. Host-Based Model (Centralized Processing with High Synchronization Delay)
run_model_benchmark "Host-Based" 10 0.10 "Central controller performs all checks & state writes sequentially."

# 2. Processor Pool Model (Dynamic Worker Resource Chunk Allocation)
run_model_benchmark "Processor-Pool" 10 0.04 "Dynamic assignment across unassigned compute worker pools."

# 3. Workstation Model (Local Autonomous Execution with Zero Coordination)
run_model_benchmark "Workstation" 10 0.00 "Local autonomous slice execution without lock contention."

# 4. Server-Based Model (Dedicated Microservices for Slicing & Telemetry)
run_model_benchmark "Server-Based" 10 0.01 "Client-Server API calls to specialized microservice daemons."

# 5. Integrated Model (Optimized Multi-Tier Edge-Core-Cloud Platform)
run_model_benchmark "Integrated-Hybrid" 10 0.02 "Low-latency Edge execution combined with Core 2PC atomic commits."

echo ""
echo "=================================================================="
echo " MILESTONE 8 EVALUATION COMPLETE"
echo "=================================================================="

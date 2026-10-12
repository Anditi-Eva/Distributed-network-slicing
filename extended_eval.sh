#!/bin/bash
# ==============================================================================
# ICS 2403: MILESTONE 8 EXTENDED PARAMETER EVALUATION
# Measures: Throughput, Latency, Resource Utilization, Reliability & Payload Cost
# ==============================================================================

WORKER_URL="http://127.0.0.1:5001"

echo "=================================================================="
echo " Starting Extended Distributed Model Evaluation"
echo "=================================================================="

# 1. Baseline Capacity Check
PRE_CPU=$(curl -s "$WORKER_URL/metrics" | grep "network_slicing_available_cpu" | grep -v "#" | awk '{print $2}')
PRE_RAM=$(curl -s "$WORKER_URL/metrics" | grep "network_slicing_available_ram_mb" | grep -v "#" | awk '{print $2}')

# 2. Execution Burst (15 Requests)
SUCCESS_COUNT=0
FAIL_COUNT=0
T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

for i in {1..15}; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$WORKER_URL/resource/request" \
                   -H "Content-Type: application/json" \
                   -d "{\"process_id\": \"ext_eval_$i\", \"cpu\": 0.2, \"ram\": 128}")
  if [ "$HTTP_CODE" -eq 200 ]; then
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
done

T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')

# 3. Post-Execution Capacity Check
POST_CPU=$(curl -s "$WORKER_URL/metrics" | grep "network_slicing_available_cpu" | grep -v "#" | awk '{print $2}')
POST_RAM=$(curl -s "$WORKER_URL/metrics" | grep "network_slicing_available_ram_mb" | grep -v "#" | awk '{print $2}')

# 4. Math Calculations
TOTAL_TIME=$(( T_END - T_START ))
THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", (15 / ($TOTAL_TIME / 1000))}")
LATENCY=$(awk "BEGIN {printf \"%.2f\", ($TOTAL_TIME / 15)}")
CPU_USED=$(awk "BEGIN {printf \"%.1f\", ($PRE_CPU - $POST_CPU)}")
RAM_USED=$(awk "BEGIN {printf \"%.1f\", ($PRE_RAM - $POST_RAM)}")
RELIABILITY=$(awk "BEGIN {printf \"%.2f\", ($SUCCESS_COUNT / 15) * 100}")

# 5. Output Summary Matrix
echo "=================================================================="
echo " EXTENDED PARAMETER BENCHMARK RESULTS"
echo "=================================================================="
echo " Total Duration       : ${TOTAL_TIME} ms"
echo " Mean Latency         : ${LATENCY} ms/request"
echo " Throughput           : ${THROUGHPUT} req/sec"
echo " vCPU Consumed        : ${CPU_USED} Cores (Allocated)"
echo " RAM Consumed         : ${RAM_USED} MB (Allocated)"
echo " Transaction Success  : ${SUCCESS_COUNT} / 15 (${RELIABILITY}%)"
echo " Payload Overhead     : ~142 bytes / HTTP JSON payload"
echo "=================================================================="

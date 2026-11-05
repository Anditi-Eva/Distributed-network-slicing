#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 5 TEST SUITE
# DISTRIBUTED TRANSACTIONS: TWO-PHASE COMMIT, THREE-PHASE COMMIT, RECOVERY
# ==============================================================================
# Starts a real cluster (coordinator on 6200, Edge/Core/Cloud participants on
# 6201-6203), crashes processes with SIGKILL, checks every property automatically
# and writes tables + charts to ./results/.
#   Usage:  ./milestone5_test.sh            (full run, a few minutes)
#           ./milestone5_test.sh --quick    (fast smoke run)
# ==============================================================================

cd "$(dirname "$0")" || exit 1

if ! python3 -c "import flask, requests, prometheus_client, matplotlib" 2>/dev/null; then
  echo "Missing dependencies. Install with:"
  echo "  pip install flask requests prometheus_client matplotlib"
  exit 1
fi

python3 milestone5_experiments.py "$@"
STATUS=$?

echo ""
echo "=================================================================="
if [ $STATUS -eq 0 ]; then
  echo " MILESTONE 5: ALL CHECKS PASSED"
else
  echo " MILESTONE 5: ONE OR MORE CHECKS FAILED (see table above)"
fi
echo " Results : results/milestone5_summary.md, results/milestone5_results.json"
echo " Figures : results/m5_latency_throughput.png, results/m5_failure_resolution.png"
echo "=================================================================="
exit $STATUS

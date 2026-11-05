#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 7 TEST SUITE
# FAULT TOLERANCE, REPLICATION, BYZANTINE ANALYSIS & RECOVERY
# ==============================================================================

cd "$(dirname "$0")" || exit 1

if ! python3 -c "import flask, requests, prometheus_client, matplotlib" 2>/dev/null; then
  echo "Missing dependencies. Install with:"
  echo "  pip install flask requests prometheus_client matplotlib"
  exit 1
fi

python3 milestone7_experiments.py "$@"
STATUS=$?

echo ""
echo "=================================================================="
if [ $STATUS -eq 0 ]; then
  echo " MILESTONE 7: ALL CHECKS PASSED"
else
  echo " MILESTONE 7: ONE OR MORE CHECKS FAILED"
fi
echo " Results : results/milestone7_summary.md, results/milestone7_results.json"
echo " Figures : results/m7_fault_metrics.png"
echo "=================================================================="
exit $STATUS

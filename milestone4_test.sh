#!/bin/bash
# ==============================================================================
# ICS 2403: DISTRIBUTED COMPUTING - MILESTONE 4 TEST SUITE
# DISTRIBUTED COORDINATION: BULLY LEADER ELECTION + LAMPORT LOGICAL CLOCKS
# ==============================================================================
# Starts real coordination nodes (one process each, ports 6101..61NN), runs the
# experiments, checks every property automatically and writes tables + charts to
# ./results/.   Usage:  ./milestone4_test.sh           (full run, about 3-5 minutes)
#                       ./milestone4_test.sh --quick   (fast smoke run)
# ==============================================================================

cd "$(dirname "$0")" || exit 1

if ! python3 -c "import flask, requests, prometheus_client, matplotlib" 2>/dev/null; then
  echo "Missing dependencies. Install with:"
  echo "  pip install flask requests prometheus_client matplotlib"
  exit 1
fi

python3 milestone4_experiments.py "$@"
STATUS=$?

echo ""
echo "=================================================================="
if [ $STATUS -eq 0 ]; then
  echo " MILESTONE 4: ALL CHECKS PASSED"
else
  echo " MILESTONE 4: ONE OR MORE CHECKS FAILED (see table above)"
fi
echo " Results   : results/milestone4_summary.md, results/milestone4_results.json"
echo " Figures   : results/m4_message_complexity.png, m4_sync_delay.png, m4_recovery.png"
echo "=================================================================="
exit $STATUS

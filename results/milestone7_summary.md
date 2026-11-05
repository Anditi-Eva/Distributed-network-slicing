# Milestone 7 - Fault Tolerance & Recovery Summary Report

## 1. Failure Detection & Heartbeats (E1)
- **Suspect Timeout**: 633.9 ms
- **Marked Down Timeout**: 1731.8 ms

## 2. Replication & Leader Failover (E2)
- **State Synchronized Across Backup Nodes**: Yes
- **Leader Failover Latency**: 7.55 ms

## 3. Operational MTTR & System Availability (E3)
- **Total Workload Stress Requests**: 99
- **Failed Requests During Fault Injection**: 41
- **Measured Operational MTTR**: 2532.9 ms
- **Measured System Availability**: 58.6%

## 4. Byzantine Analysis (E4)
- **Safe Bound (N=4, f=1)**: True (Required N >= 4)
- **Unsafe Bound (N=3, f=1)**: False (Required N >= 4)
- **Byzantine Consensus Decision**: COMMIT
- **Isolated Malicious Nodes**: ['N4_malicious']

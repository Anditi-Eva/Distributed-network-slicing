# Milestone 4 - Experimental results (generated)

## E1 Message complexity (mean, min-max over runs) versus theory

| N | Best case (highest starts) | Typical (random starts) | Worst case (lowest starts) | Theory best N-1 | Theory worst N^2-1 |
|---|---|---|---|---|---|
| 3 | 2.0 (2-2) | 6.0 (2-8) | 8.0 (8-8) | 2 | 8 |
| 5 | 4.0 (4-4) | 14.0 (10-16) | 24.0 (24-24) | 4 | 24 |
| 7 | 6.0 (6-6) | 30.7 (8-48) | 48.0 (48-48) | 6 | 48 |

## E1 Synchronization delay and overhead

| N | Delay best (ms) | Delay typical (ms) | Delay worst (ms) | Bytes sent, worst case |
|---|---|---|---|---|
| 3 | 18.1 | 30.7 | 41.6 | 263 |
| 5 | 39.6 | 66.4 | 88.6 | 710 |
| 7 | 57.2 | 142.3 | 144.5 | 1339 |

## E2 Leader crash: detection, election and recovery (mean over runs)

| N | Runs | Detection (ms) | Election (ms) | Total recovery (ms) | Election messages |
|---|---|---|---|---|---|
| 5 | 3 | 435 | 47 | 481 | 12.0 |

## E3 Worked example: Lamport timestamps on a causal chain

| Event | Node | Type | Lamport time |
|---|---|---|---|
| a1 | N1 | local | 1 |
| m1 send | N1 | send | 2 |
| m1 recv | N2 | recv | 3 |
| b1 | N2 | local | 4 |
| m2 send | N2 | send | 5 |
| m2 recv | N3 | recv | 6 |
| m3 send | N3 | send | 7 |
| m3 recv | N1 | recv | 8 |
| c1 (concurrent) | N4 | local | 1 |

## E3 Event-ordering check over a random workload

| Metric | Value |
|---|---|
| Messages sent / delivered | 100 / 100 |
| Clock-condition violations (send ts >= receive ts) | 0 |
| Duplicate timestamps within a node | 0 |
| Total-order violations (by timestamp, then node id) | 0 |
| Largest clock value reached | 73 |
| Mean / p95 message latency (ms) | 67.5 / 137.8 |
| Throughput (messages/s) | 114 |
| Timestamp overhead per message | 17 bytes (20% of a 84-byte message) |

## Automatic checks

| Check | Result |
|---|---|
| E1 N=3: all 9 elections consistent, leader = highest priority | PASS |
| E1 N=3: best-case messages == N-1 | PASS |
| E1 N=3: worst-case messages <= N^2-1 | PASS |
| E1 N=5: all 9 elections consistent, leader = highest priority | PASS |
| E1 N=5: best-case messages == N-1 | PASS |
| E1 N=5: worst-case messages <= N^2-1 | PASS |
| E1 N=7: all 9 elections consistent, leader = highest priority | PASS |
| E1 N=7: best-case messages == N-1 | PASS |
| E1 N=7: worst-case messages <= N^2-1 | PASS |
| E2 N=5: recovered and consistent in all 3 runs | PASS |
| E3: every message delivered | PASS |
| E3: zero clock-condition violations | PASS |
| E3: unique timestamps per node | PASS |
| E3: total order respects happens-before | PASS |

# Milestone 5 - Experimental results (generated)

## T1 ACID properties

| Property | Experiment | Result | Holds |
|---|---|---|---|
| Atomicity | Cloud cannot supply its share (5 CPU of 4) | ABORTED; allocated CPU [4, 4, 4], reserved [0, 0, 0] | NO |
| Isolation | 20 concurrent transactions, 3 CPU left | 0 committed, 20 aborted; allocated CPU [4, 4, 4] | NO |
| Durability | SIGKILL all 4 processes, restart, replay logs | allocations and committed set identical before and after | NO |
| Consistency | Global invariants after the run | no oversubscription, log replay equals memory, atomic across participants | yes |

## T2 Latency and throughput (3 participants)

| Protocol | fsync | Clients | Mean (ms) | p50 (ms) | p95 (ms) | Throughput (tx/s) | Messages/tx | Commit rate |
|---|---|---|---|---|---|---|---|---|
| 2PC | on | 1 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 2PC | on | 4 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 3PC | on | 1 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 3PC | on | 4 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 2PC | off | 1 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 2PC | off | 4 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 3PC | off | 1 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |
| 3PC | off | 4 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0% |

Cost of forcing one log record to disk on this machine: 0.010 ms (fsync) against 0.011 ms (no fsync). A 2PC transaction forces 9 records (3 at the coordinator, 6 across the participants; 3PC forces 13), so durability adds at most -0.0 ms to a 2PC transaction.

## T2 Phase breakdown, single client, fsync on (mean ms)

| Protocol | Prepare | PreCommit | Decide + complete | Total |
|---|---|---|---|---|
| 2PC | 0.0 | 0.0 | 0.0 | 0.0 |
| 3PC | 0.0 | 0.0 | 0.0 | 0.0 |

## T3 Failure experiments (outcome per scenario, mean over runs)

| Scenario | Protocol | Final outcome | Atomic | Participants blocked | Resolved by | Time to consistent state (ms) | Measured from |
|---|---|---|---|---|---|---|---|
| F1 participant votes NO | 2PC | ABORTED | yes | no | - | 33 | transaction start |
| F1 participant votes NO | 3PC | ABORTED | yes | no | - | 35 | transaction start |
| F2 participant down before voting | 2PC | ABORTED | yes | no | - | 46 | node restart |
| F2 participant down before voting | 3PC | ABORTED | yes | no | - | 42 | node restart |
| F3 participant crashes after voting YES | 2PC | ABORTED | yes | no | - | 42 | node restart |
| F3 participant crashes after voting YES | 3PC | ABORTED | yes | no | - | 53 | node restart |
| F4 coordinator crashes after votes | 2PC | ABORTED | yes | no | participants (termination protocol) | 21 | coordinator crash |
| F4 coordinator crashes after votes | 3PC | ABORTED | yes | no | participants (termination protocol) | 23 | coordinator crash |
| F5 coordinator crashes after decision | 2PC | ABORTED | yes | no | participants (termination protocol) | 28 | coordinator crash |
| F5 coordinator crashes after decision | 3PC | ABORTED | yes | no | participants (termination protocol) | 25 | coordinator crash |

## T4 Availability with the Core node down for one third of the workload

| Protocol | Transactions | Commit rate before | during outage | after recovery | Overall availability | Abort latency during outage (ms) | Consistent |
|---|---|---|---|---|---|---|---|
| 2PC | 30 | 0% | 0% | 0% | 0.0% | 27.6 | yes |
| 3PC | 30 | 0% | 0% | 0% | 0.0% | 27.6 | yes |

## Automatic checks

| Check | Result |
|---|---|
| T1 atomicity: failed allocation leaves nothing behind | FAIL |
| T1 isolation: no oversubscription under concurrency | FAIL |
| T1 durability: state survives kill -9 of every process | FAIL |
| T1 consistency: global invariants hold | PASS |
| T2 every transaction committed | FAIL |
| T2 2PC uses 4n = 12 messages | FAIL |
| T2 3PC uses 6n = 18 messages | FAIL |
| T2 consistent after the load runs | PASS |
| T3 2PC F1: atomic, outcome ABORTED | PASS |
| T3 2PC F2: atomic, outcome ABORTED | PASS |
| T3 2PC F3: atomic, outcome COMMITTED | FAIL |
| T3 2PC F4: atomic, outcome ABORTED | PASS |
| T3 2PC F5: atomic, outcome COMMITTED | FAIL |
| T3 2PC: consistent after all failures | PASS |
| T3 3PC F1: atomic, outcome ABORTED | PASS |
| T3 3PC F2: atomic, outcome ABORTED | PASS |
| T3 3PC F3: atomic, outcome COMMITTED | FAIL |
| T3 3PC F4: atomic, outcome ABORTED | PASS |
| T3 3PC F5: atomic, outcome COMMITTED | FAIL |
| T3 3PC: consistent after all failures | PASS |
| T3 2PC blocks when the coordinator crashes (F4) | FAIL |
| T3 3PC does not block (F4) | PASS |
| T3 2PC blocks when the coordinator crashes (F5) | FAIL |
| T3 3PC does not block (F5) | PASS |
| T4 2PC: consistent after outage | PASS |
| T4 2PC: service fully available again after recovery | FAIL |
| T4 3PC: consistent after outage | PASS |
| T4 3PC: service fully available again after recovery | FAIL |

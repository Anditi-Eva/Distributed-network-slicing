#!/usr/bin/env python3
"""
Shared definitions for the Milestone 5 transaction subsystem.

Participant (resource manager) states for one transaction:

    INIT          participant has never heard of the transaction
    READY         voted YES in phase 1 and holds a durable reservation (the "uncertain" state)
    PRECOMMITTED  3PC only: received the PRECOMMIT message
    COMMITTED     final - reservation turned into an allocation
    ABORTED       final - reservation released

Transaction model:    Prepare -> Commit -> Completion        (2PC)
                      Prepare -> PreCommit -> Commit -> Completion   (3PC)
"""

INIT = "INIT"
READY = "READY"
PRECOMMITTED = "PRECOMMITTED"
COMMITTED = "COMMITTED"
ABORTED = "ABORTED"

IN_DOUBT = (READY, PRECOMMITTED)
TERMINAL = (COMMITTED, ABORTED)


def decide_from_states(protocol, states):
    """
    Termination / recovery rule shared by participants and a recovering coordinator.

    states   : states of all participants as observed ("INIT", "READY", ...), with None
               for a participant that could not be reached.
    returns  : "COMMIT", "ABORT", or None when the outcome cannot be determined safely
               (the caller is BLOCKED and must wait).

    2PC (blocking):
        any COMMITTED                -> COMMIT   (someone already learned the decision)
        any ABORTED or INIT          -> ABORT    (the coordinator cannot have decided COMMIT)
        otherwise (all READY)        -> None     (blocked until the coordinator returns)
    3PC (non-blocking, assumes crash failures and no network partitions):
        any COMMITTED                -> COMMIT
        any ABORTED or INIT          -> ABORT
        any PRECOMMITTED             -> COMMIT   (everybody voted YES)
        all reachable READY          -> ABORT    (nobody can have committed yet)
    """
    known = [s for s in states if s is not None]
    if COMMITTED in known:
        return "COMMIT"
    if ABORTED in known or INIT in known:
        return "ABORT"
    if protocol == "3PC":
        if PRECOMMITTED in known:
            return "COMMIT"
        if known and all(s == READY for s in known):
            return "ABORT"
    return None

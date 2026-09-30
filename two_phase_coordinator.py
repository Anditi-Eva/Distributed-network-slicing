import uuid
import requests
import time
from transaction_wal import TransactionWAL

wal = TransactionWAL()

class TwoPhaseCommitCoordinator:
    def __init__(self, participant_urls):
        self.participants = participant_urls  # List of participant endpoint URLs

    def execute_transaction(self, slice_data):
        tx_id = f"tx-{uuid.uuid4().hex[:6]}"
        start_time = time.time()

        print(f"\n--- Starting 2PC Transaction [{tx_id}] ---")
        
        # -------------------------------------------------------------
        # STEP 1: PREPARE PHASE
        # -------------------------------------------------------------
        wal.log_event(tx_id, "PREPARE", self.participants, slice_data)
        print(f"[{tx_id}] Phase 1: Sending PREPARE to participants: {self.participants}")

        votes = {}
        for url in self.participants:
            try:
                res = requests.post(f"{url}/tx/prepare", json={
                    "tx_id": tx_id,
                    "slice_data": slice_data
                }, timeout=2)
                
                if res.status_code == 200 and res.json().get("vote") == "VOTE_COMMIT":
                    votes[url] = "VOTE_COMMIT"
                else:
                    votes[url] = "VOTE_ABORT"
            except requests.exceptions.RequestException:
                votes[url] = "VOTE_ABORT"

        print(f"[{tx_id}] Prepare Votes Collected: {votes}")

        # -------------------------------------------------------------
        # STEP 2: COMMIT / ABORT DECISION
        # -------------------------------------------------------------
        all_commit = all(vote == "VOTE_COMMIT" for vote in votes.values()) and len(votes) == len(self.participants)
        decision = "COMMIT" if all_commit else "ABORT"
        
        wal.log_event(tx_id, decision, self.participants)
        print(f"[{tx_id}] Phase 2: Decision determined -> [{decision}]")

        # Broadcast decision to participants
        acks = []
        for url in self.participants:
            try:
                endpoint = "/tx/commit" if decision == "COMMIT" else "/tx/abort"
                res = requests.post(f"{url}{endpoint}", json={"tx_id": tx_id}, timeout=2)
                if res.status_code == 200:
                    acks.append(url)
            except requests.exceptions.RequestException:
                pass

        # -------------------------------------------------------------
        # STEP 3: COMPLETION PHASE
        # -------------------------------------------------------------
        final_state = "COMMITTED" if decision == "COMMIT" else "ABORTED"
        wal.log_event(tx_id, final_state, self.participants)
        
        elapsed_latency_ms = (time.time() - start_time) * 1000
        print(f"[{tx_id}] Phase 3: Completion -> Status [{final_state}] in {elapsed_latency_ms:.2f} ms\n")

        return {
            "tx_id": tx_id,
            "status": final_state,
            "decision": decision,
            "votes": votes,
            "latency_ms": round(elapsed_latency_ms, 2),
            "participants_acknowledged": len(acks)
        }

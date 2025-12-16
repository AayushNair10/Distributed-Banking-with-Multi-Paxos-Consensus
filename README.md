# Distributed Banking with Multi-Paxos Consensus

A fault-tolerant distributed banking application built on the **Stable-Leader Multi-Paxos** consensus protocol. This implementation supports crash fault tolerance for up to 2 simultaneous node failures across a 5-node cluster, ensuring consistent transaction processing and exactly-once semantics.

> **Course**: CSE 535 - Distributed Systems  
> **Project**: Paxos Consensus Protocol Implementation

---

## Features

- **Multi-Paxos Consensus**: Stable-leader optimization that eliminates per-transaction leader election overhead
- **Crash Fault Tolerance**: Tolerates up to `f=2` concurrent node failures in a `2f+1=5` node cluster
- **Leader Election**: Automatic leader election with ballot-based voting and view changes
- **Exactly-Once Semantics**: Timestamp-based deduplication ensures transactions execute exactly once
- **No-Op Gap Filling**: Handles missing sequence numbers during leader transitions
- **Node Recovery**: Automatic state synchronization when failed nodes rejoin
- **Concurrent Client Support**: 10 clients submitting transactions in parallel

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENTS (10)                             │
│   ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐            │
│   │ C1-C2 │ │ C3-C4 │ │ C5-C6 │ │ C7-C8 │ │ C9-C10│            │
│   └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘            │
└───────┼─────────┼─────────┼─────────┼─────────┼────────────────┘
        │         │         │         │         │
        ▼         ▼         ▼         ▼         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PAXOS CLUSTER (5 nodes)                     │
│   ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐   │
│   │ Node 1 │  │ Node 2 │  │ Node 3 │  │ Node 4 │  │ Node 5 │   │
│   │(Leader)│  │(Backup)│  │(Backup)│  │(Backup)│  │(Backup)│   │
│   └────────┘  └────────┘  └────────┘  └────────┘  └────────┘   │
│        │           │           │           │           │        │
│        └───────────┴───────────┴───────────┴───────────┘        │
│                    Replicated State Machine                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
paxos_doc/
├── clients/
│   └── client.py          # Client implementation with timer-based retries
├── driver/
│   └── driver.py          # Test driver and orchestration
├── nodes/
│   └── node.py            # Paxos node (proposer/acceptor/learner)
├── scripts/
│   └── start_all_nodes.py # Node startup automation
├── tests/
│   └── sample_input.csv   # Test transactions
├── node_logs/             # Per-node execution logs
├── config.json            # Cluster configuration
└── requirements.txt       # Python dependencies
```

---

## Quick Start

### Prerequisites

- Python 3.8+
- Required packages: `pip install -r requirements.txt`

### Running the Cluster

1. **Start all nodes**:
   ```bash
   python scripts/start_all_nodes.py
   ```

2. **Run the test driver**:
   ```bash
   python driver/driver.py
   ```

3. **Process test transactions**:
   ```bash
   # The driver will prompt you to process each transaction set
   # Press Enter to process the next set
   ```

### Configuration

Edit `config.json` to modify:
- Node addresses and ports
- Client timeout durations
- Leader election parameters

---

## Protocol Messages

| Message | Format | Description |
|---------|--------|-------------|
| **PREPARE** | `⟨PREPARE, ballot⟩` | Leader election initiation |
| **PROMISE** | `⟨ACK, ballot, AcceptLog⟩` | Vote for proposer with accepted history |
| **NEW-VIEW** | `⟨NEW-VIEW, ballot, AcceptLog⟩` | New leader announcement with pending requests |
| **ACCEPT** | `⟨ACCEPT, ballot, seq, request⟩` | Propose transaction for consensus |
| **ACCEPTED** | `⟨ACCEPTED, ballot, seq, request, node⟩` | Vote to accept transaction |
| **COMMIT** | `⟨COMMIT, ballot, seq, request⟩` | Transaction committed notification |
| **REPLY** | `⟨REPLY, ballot, timestamp, client, result⟩` | Response to client |

---

## Testing

### Test Input Format

Tests are provided as CSV files with three columns:

| Set Number | Transactions | Live Nodes |
|------------|--------------|------------|
| 1 | (A, C, 5) | [n1, n2, n3, n4, n5] |
| 1 | (C, E, 4) | [n1, n2, n3, n4, n5] |
| 2 | (A, E, 4) | [n1, n3, n5] |

### Debug Functions

```python
PrintLog(node_id)      # Display node's transaction log
PrintDB()              # Show all client balances
PrintStatus(seq_num)   # Transaction status at each node (A/C/E/X)
PrintView()            # All NEW-VIEW messages exchanged
```

### Test Scenarios Covered

-  Initial leader election on system startup
-  Basic transaction agreement and execution
-  Backup node failure (up to 2 nodes)
-  Leader failure and re-election
-  Leader failure before commit (recovery)
-  Concurrent leader election attempts
-  Client timeout and request resubmission
-  Quorum loss detection (>2 failures)

---

## Transaction Processing

Each transaction follows the format `(sender, receiver, amount)`:

```
Initial Balance: All clients start with 10 units

Transaction: (A, B, 4)
  → A's balance: 10 - 4 = 6
  → B's balance: 10 + 4 = 14
  → Result: SUCCESS

Transaction: (A, B, 100)  
  → A's balance: 6 (insufficient)
  → Result: FAILED
```

---

## Implementation Details

### Ballot Numbers
Ballots are tuples `(round, node_id)` ordered lexicographically to ensure uniqueness and enable leader comparison.

### Sequence Numbers
Each transaction receives a monotonically increasing sequence number, starting from 1. Gaps are filled with `no-op` operations during leader transitions.

### Exactly-Once Semantics
Clients assign timestamps to requests. Nodes track the last processed timestamp per client and reject duplicates.

### Fault Tolerance
- **f = 2**: System tolerates up to 2 concurrent failures
- **Quorum = 3**: Requires 3 nodes (majority) for progress
- **Recovery**: Failed nodes sync via NEW-VIEW or catch-up protocols

---

## Commit Log Format

Each node maintains a persistent commit log:

```
node_X_commits.log
─────────────────
Seq: 1 | Ballot: (1,2) | Transaction: (A,B,10) | Status: EXECUTED
Seq: 2 | Ballot: (1,2) | Transaction: (C,E,2)  | Status: EXECUTED
Seq: 3 | Ballot: (2,3) | Transaction: no-op    | Status: EXECUTED
```

---

## References

- Lamport, L. (2001). *Paxos Made Simple*. ACM SIGACT News, 32(4), 18-25.
- Van Renesse, R., & Altinbuken, D. (2015). *Paxos Made Moderately Complex*. ACM Computing Surveys.

---

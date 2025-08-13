# Chardonnay MVP

This repository provides a minimal implementation of the core ideas from the OSDI ’23 paper “Chardonnay: Fast and General Datacenter Transactions for On‑Disk Databases”. It is a proof‑of‑concept of a partitioned, replicated key‑value store with multi‑version concurrency control, per‑shard consensus, two‑phase commit and epoch‑based snapshot reads.

## Features

- **Hash‑based sharding**: Keys are partitioned across shards using a hash function.
- **Multi‑Paxos replication**: Each shard is replicated using a simplified Multi‑Paxos protocol to ensure consistent log entries across replicas.
- **Two‑phase commit (2PC)**: Multi‑shard transactions are coordinated with prepare and commit phases to guarantee atomicity.
- **Epoch service and snapshot reads**: A global epoch counter produces consistent snapshot points, allowing read‑only transactions to read MVCC data without blocking updates.
- **Dry‑run prefetching**: Transactions are dry‑run on a consistent snapshot to compute their read/write sets and prefetch data into memory before taking locks (as described in the paper).
- **Minimal in‑process design**: The entire system runs in a single process without external dependencies, making it easy to experiment with the architecture.

## Requirements

- Python 3.8 or later
- No external libraries required

## Running the MVP

To run a simple demonstration of the Chardonnay MVP:

```bash
python3 chardonnay.py
```
## Notes
This project is intended for educational and experimentation purposes. It omits many optimisations and features of a production system (e.g. persistent storage, asynchronous networking and failover). See the paper for the full design details and performance evaluations.

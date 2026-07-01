# ChardonnayKV

ChardonnayKV is a minimal, in-process implementation of the core ideas from the
OSDI '23 paper "Chardonnay: Fast and General Datacenter Transactions for On-Disk
Databases".

It is an educational proof of concept of a partitioned, replicated key-value
store with hash-based sharding, per-shard Multi-Paxos logs, MVCC, two-phase
commit, epoch-based snapshot reads, and dry-run prefetching.

## Project Layout

```text
.
├── chardonnay.py                 # Compatibility entrypoint
├── chardonnaykv/                 # Library package
│   ├── __init__.py               # Public API exports
│   ├── cluster.py                # ChardonnayKV cluster and sharding
│   ├── consensus.py              # Simplified in-process Multi-Paxos
│   ├── epoch.py                  # Global epoch service
│   ├── examples.py               # Example transfer transaction and demo
│   ├── mvcc.py                   # Versioned key-value storage
│   ├── shard.py                  # 2PC participant and per-shard log
│   ├── transactions.py           # Transaction context, dry-runs, snapshots
│   └── types.py                  # Shared type aliases
├── examples/
│   └── transfer_demo.py          # Runnable demo script
├── tests/
│   └── test_chardonnaykv.py      # Unit smoke tests
└── pyproject.toml                # Minimal project metadata
```

## Quick Start

Run the demo:

```bash
python3 examples/transfer_demo.py
```

The old single-file command still works too:

```bash
python3 chardonnay.py
```

Run tests:

```bash
python3 -m unittest discover -s tests
```

Use it from Python:

```python
from chardonnaykv import ChardonnayKV

kv = ChardonnayKV(shard_count=4, replica_count=3)
kv.put("alice", 100)
kv.put("bob", 25)

def transfer(txn):
    alice = txn.get("alice") or 0
    bob = txn.get("bob") or 0
    txn.put("alice", alice - 30)
    txn.put("bob", bob + 30)

dry_run = kv.dry_run(transfer)
print(dry_run.read_keys)
print(dry_run.write_keys)
print(dry_run.prefetched)

before = kv.snapshot()
kv.transaction(transfer)
after = kv.snapshot()

print(before.items(["alice", "bob"]))
print(after.items(["alice", "bob"]))
```

## Design

Keys are assigned to shards with SHA-256 based hashing. Each shard owns an MVCC
store and a replicated commit log. The replicated log is driven by an
in-process, simplified Multi-Paxos implementation with per-slot acceptor state.

Transactions are expressed as Python callables that receive a
`TransactionContext`. ChardonnayKV first runs the callable in dry-run mode at a
stable epoch. Reads are served from that snapshot, read/write sets are recorded,
and read values are prefetched into memory.

For committing writes, ChardonnayKV runs the callable again against the same
prefetched snapshot, groups write keys by shard, prepares each participant by
locking keys, allocates one commit epoch, appends each shard's writes through
Paxos, applies MVCC versions, and finally releases locks.

Read-only snapshots use `kv.snapshot()`. A snapshot reader holds the epoch it was
created at, so later writes do not change its results.

## Limitations

This is intentionally not production software. It omits networking, persistent
storage, async replication, leader election, failover, recovery, dead replica
handling, lock timeouts, and most validation needed by a real distributed
database. The implementation favors clarity over completeness.

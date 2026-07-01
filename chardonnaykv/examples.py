"""Reusable example transactions and demo output."""

from __future__ import annotations

from .cluster import ChardonnayKV
from .transactions import TransactionContext, TransactionProgram
from .types import Key


def transfer(source: Key, destination: Key, amount: int) -> TransactionProgram:
    def program(txn: TransactionContext) -> None:
        source_balance = txn.get(source) or 0
        destination_balance = txn.get(destination) or 0
        if source_balance < amount:
            raise ValueError("insufficient funds")
        txn.put(source, source_balance - amount)
        txn.put(destination, destination_balance + amount)

    return program


def demo() -> None:
    kv = ChardonnayKV(shard_count=4, replica_count=3)
    kv.put("alice", 100)
    kv.put("bob", 25)

    before = kv.snapshot()
    dry = kv.dry_run(transfer("alice", "bob", 30))
    kv.transaction(transfer("alice", "bob", 30))
    after = kv.snapshot()

    print("dry-run epoch:", dry.read_epoch)
    print("dry-run reads:", dry.read_keys)
    print("dry-run writes:", dry.write_keys)
    print("dry-run prefetched:", dry.prefetched)
    print("snapshot before:", before.items(["alice", "bob"]))
    print("snapshot after:", after.items(["alice", "bob"]))
    print("current epoch:", kv.epochs.current())
    print("per-shard log lengths:", [len(shard.log) for shard in kv.shards])

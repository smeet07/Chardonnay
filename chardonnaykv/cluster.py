"""Public ChardonnayKV cluster API."""

from __future__ import annotations

from collections import defaultdict
import hashlib
import threading
from typing import DefaultDict, Dict, Iterable, List, Optional, Tuple

from .epoch import EpochService
from .shard import Shard
from .transactions import DryRunResult, SnapshotReader, TransactionContext, TransactionProgram
from .types import Epoch, Key, ShardId, TxnId, Value


def shard_for_key(key: Key, shard_count: int) -> ShardId:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


class ChardonnayKV:
    """Partitioned, replicated MVCC key-value store."""

    def __init__(self, shard_count: int = 4, replica_count: int = 3) -> None:
        if shard_count < 1:
            raise ValueError("shard_count must be positive")
        self.epochs = EpochService()
        self.shards = [Shard(shard_id, replica_count) for shard_id in range(shard_count)]
        self.shard_count = shard_count
        self._txn_id = 0
        self._txn_lock = threading.Lock()

    def shard_id_for(self, key: Key) -> ShardId:
        return shard_for_key(key, self.shard_count)

    def get_at(self, key: Key, epoch: Epoch) -> Optional[Value]:
        return self.shards[self.shard_id_for(key)].read_at(key, epoch)

    def get(self, key: Key) -> Optional[Value]:
        return self.get_at(key, self.epochs.current())

    def snapshot(self) -> SnapshotReader:
        return SnapshotReader(self, self.epochs.current())

    def dry_run(self, program: TransactionProgram) -> DryRunResult:
        read_epoch = self.epochs.current()
        context = TransactionContext(self, read_epoch, dry_run=True)
        program(context)
        return DryRunResult(
            read_epoch=read_epoch,
            read_keys=tuple(sorted(set(context.read_set))),
            write_keys=tuple(sorted(context.writes)),
            prefetched=dict(context.prefetched),
        )

    def transaction(self, program: TransactionProgram) -> object:
        """Dry-run a program, then commit its writes with 2PC."""

        dry = self.dry_run(program)
        context = TransactionContext(self, dry.read_epoch, dict(dry.prefetched), dry_run=False)
        result = program(context)
        if not context.writes:
            return result

        txn_id = self._next_txn_id()
        participant_keys = self._participant_keys(context.writes)
        prepared_shards: List[ShardId] = []

        try:
            for shard_id, keys in participant_keys.items():
                self.shards[shard_id].prepare(txn_id, dry.read_epoch, keys)
                prepared_shards.append(shard_id)

            commit_epoch = self.epochs.next_epoch()
            per_shard_writes = self._group_writes(context.writes)
            for shard_id, writes in per_shard_writes.items():
                self.shards[shard_id].commit(txn_id, commit_epoch, writes)
            return result
        except Exception:
            for shard_id in prepared_shards:
                self.shards[shard_id].abort(txn_id)
            raise
        finally:
            for shard_id in prepared_shards:
                self.shards[shard_id].finish(txn_id)

    def put(self, key: Key, value: Value) -> None:
        self.transaction(lambda txn: txn.put(key, value))

    def items_at(self, keys: Iterable[Key], epoch: Epoch) -> Dict[Key, Optional[Value]]:
        return {key: self.get_at(key, epoch) for key in keys}

    def _next_txn_id(self) -> TxnId:
        with self._txn_lock:
            self._txn_id += 1
            return self._txn_id

    def _participant_keys(self, writes: Dict[Key, Value]) -> Dict[ShardId, List[Key]]:
        grouped: DefaultDict[ShardId, List[Key]] = defaultdict(list)
        for key in writes:
            grouped[self.shard_id_for(key)].append(key)
        return dict(grouped)

    def _group_writes(self, writes: Dict[Key, Value]) -> Dict[ShardId, List[Tuple[Key, Value]]]:
        grouped: DefaultDict[ShardId, List[Tuple[Key, Value]]] = defaultdict(list)
        for key, value in writes.items():
            grouped[self.shard_id_for(key)].append((key, value))
        return dict(grouped)

"""Shard participant logic for two-phase commit."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import threading
from typing import Dict, Iterable, List, Tuple

from .consensus import MultiPaxosLeader, PaxosAcceptor, PaxosLearner, PaxosLogEntry
from .mvcc import MVCCStore
from .types import Epoch, Key, LogIndex, ShardId, TxnId, Value


class TxnState(str, Enum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass
class PreparedTxn:
    txn_id: TxnId
    read_epoch: Epoch
    keys: Tuple[Key, ...]
    locks: List[threading.RLock]
    state: TxnState = TxnState.PREPARED


@dataclass(frozen=True)
class CommitRecord:
    index: LogIndex
    entry: PaxosLogEntry


class Shard:
    """A shard is one MVCC partition plus one replicated log."""

    def __init__(self, shard_id: ShardId, replica_count: int = 3) -> None:
        if replica_count < 1:
            raise ValueError("replica_count must be positive")
        self.shard_id = shard_id
        self.store = MVCCStore()
        self.acceptors = [PaxosAcceptor(i) for i in range(replica_count)]
        self.learner = PaxosLearner(quorum_size=replica_count // 2 + 1)
        self.leader = MultiPaxosLeader(shard_id, self.acceptors, self.learner)
        self.log: List[CommitRecord] = []
        self._prepared: Dict[TxnId, PreparedTxn] = {}
        self._lock = threading.RLock()

    def read_at(self, key: Key, epoch: Epoch) -> Value | None:
        return self.store.get_at(key, epoch)

    def prepare(self, txn_id: TxnId, read_epoch: Epoch, keys: Iterable[Key]) -> bool:
        keys_tuple = tuple(sorted(set(keys)))
        locks = self.store.acquire_keys(keys_tuple)
        with self._lock:
            self._prepared[txn_id] = PreparedTxn(txn_id, read_epoch, keys_tuple, locks)
        return True

    def commit(self, txn_id: TxnId, epoch: Epoch, writes: Iterable[Tuple[Key, Value]]) -> CommitRecord:
        writes_tuple = tuple(sorted(writes, key=lambda item: item[0]))
        with self._lock:
            prepared = self._prepared.get(txn_id)
            if prepared is None:
                raise RuntimeError(f"transaction {txn_id} was not prepared on shard {self.shard_id}")
            if prepared.state is not TxnState.PREPARED:
                raise RuntimeError(f"transaction {txn_id} is already {prepared.state}")

            entry = PaxosLogEntry(txn_id, epoch, writes_tuple)
            index, chosen = self.leader.propose(entry)
            for key, value in chosen.writes:
                self.store.put_version(key, chosen.epoch, value)
            record = CommitRecord(index, chosen)
            self.log.append(record)
            prepared.state = TxnState.COMMITTED
            return record

    def abort(self, txn_id: TxnId) -> None:
        with self._lock:
            prepared = self._prepared.get(txn_id)
            if prepared is not None:
                prepared.state = TxnState.ABORTED

    def finish(self, txn_id: TxnId) -> None:
        with self._lock:
            prepared = self._prepared.pop(txn_id, None)
        if prepared is not None:
            self.store.release_keys(prepared.locks)

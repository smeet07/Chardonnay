
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import threading
from collections import defaultdict

# -----------------------------
# Multi-Paxos (in-process, simplified)
# -----------------------------

@dataclass
class PaxosMessage:
    index: int
    n: Tuple[int, int]  # (term, leader_id)
    value: Any = None

class PaxosAcceptor:
    def __init__(self, acceptor_id: int):
        self.acceptor_id = acceptor_id
        self.promised_n: Optional[Tuple[int, int]] = None
        self.accepted_n: Optional[Tuple[int, int]] = None
        self.accepted_value: Any = None
        self.lock = threading.Lock()

    def on_prepare(self, m: PaxosMessage):
        with self.lock:
            if self.promised_n is None or m.n > self.promised_n:
                prev = (self.accepted_n, self.accepted_value)
                self.promised_n = m.n
                return True, prev
            return False, (self.accepted_n, self.accepted_value)

    def on_accept(self, m: PaxosMessage) -> bool:
        with self.lock:
            if self.promised_n is None or m.n >= self.promised_n:
                self.promised_n = m.n
                self.accepted_n = m.n
                self.accepted_value = m.value
                return True
            return False

class PaxosLearner:
    def __init__(self, quorum: int):
        self.quorum = quorum
        self.votes: Dict[Tuple[int, Tuple[int,int]], int] = defaultdict(int)
        self.chosen: Dict[int, Any] = {}
        self.lock = threading.Lock()

    def vote(self, index: int, n: Tuple[int,int], value: Any) -> Optional[Any]:
        with self.lock:
            if index in self.chosen:
                return self.chosen[index]
            self.votes[(index, n)] += 1
            if self.votes[(index, n)] >= self.quorum:
                self.chosen[index] = value
                return value
            return None

class PaxosLeader:
    def __init__(self, leader_id: int, acceptors: List[PaxosAcceptor], learner: PaxosLearner):
        self.leader_id = leader_id
        self.acceptors = acceptors
        self.learner = learner
        self.term = 1
        self.next_index = 0

    def _majority(self) -> int:
        return len(self.acceptors) // 2 + 1

    def prepare_phase(self, index: int):
        n = (self.term, self.leader_id)
        acks = 0
        highest = None
        for a in self.acceptors:
            ok, prev = a.on_prepare(PaxosMessage(index=index, n=n))
            if ok:
                acks += 1
                if prev[0] is not None:
                    if highest is None or prev[0] > highest[0]:
                        highest = prev
        if acks >= self._majority():
            return highest
        return None

    def accept_phase(self, index: int, value: Any):
        n = (self.term, self.leader_id)
        for a in self.acceptors:
            if a.on_accept(PaxosMessage(index=index, n=n, value=value)):
                decided = self.learner.vote(index, n, value)
                if decided is not None:
                    return decided
        return None

    def propose(self, value: Any) -> Any:
        index = self.next_index
        self.next_index += 1
        prev = self.prepare_phase(index)
        if prev is not None and prev[0] is not None:
            chosen = self.accept_phase(index, prev[1])
        else:
            chosen = self.accept_phase(index, value)
        if chosen is None:
            raise RuntimeError("Consensus not reached in MVP (unexpected)")
        return chosen

# -----------------------------
# MVCC Key-Value Storage
# -----------------------------

@dataclass
class Version:
    epoch: int
    value: Any

class KeyValueStore:
    def __init__(self):
        self.data: Dict[str, List[Version]] = defaultdict(list)
        self.locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)

    def get_at(self, key: str, epoch: int) -> Optional[Any]:
        versions = self.data.get(key, [])
        for v in reversed(versions):
            if v.epoch <= epoch:
                return v.value
        return None

    def put_version(self, key: str, epoch: int, value: Any):
        self.data[key].append(Version(epoch, value))

    def lock_keys(self, keys: List[str]) -> List[threading.RLock]:
        acquired = []
        for k in sorted(set(keys)):
            lk = self.locks[k]
            lk.acquire()
            acquired.append(lk)
        return acquired

    def release_locks(self, locks: List[threading.RLock]):
        for lk in reversed(locks):
            lk.release()

# -----------------------------
# Epoch Service
# -----------------------------

class EpochService:
    def __init__(self):
        self._epoch = 0
        self._lock = threading.Lock()

    def current(self) -> int:
        with self._lock:
            return self._epoch

    def advance(self) -> int:
        with self._lock:
            self._epoch += 1
            return self._epoch

# -----------------------------
# Shards with Multi-Paxos
# -----------------------------

class Shard:
    def __init__(self, shard_id: int, replica_count: int = 3):
        self.shard_id = shard_id
        self.acceptors = [PaxosAcceptor(i) for i in range(replica_count)]
        self.learner = PaxosLearner(quorum=replica_count//2 + 1)
        self.leader = PaxosLeader(leader_id=0, acceptors=self.acceptors, learner=self.learner)
        self.kv = KeyValueStore()
        self.log: List[Tuple[int, List[Tuple[str, Any]]]] = []  # (epoch, writes)

    def apply_entry(self, epoch: int, writes: List[Tuple[str, Any]]):
        for k, v in writes:
            self.kv.put_version(k, epoch, v)

    def append_and_apply(self, epoch: int, writes: List[Tuple[str, Any]]):
        value = (epoch, list(writes))
        decided = self.leader.propose(value)
        self.log.append(decided)
        self.apply_entry(*decided)

# -----------------------------
# Cluster & Hash-based Sharding
# -----------------------------

def shard_for(key: str, shard_count: int) -> int:
    h = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(h[:4], 'big') % shard_count

class Cluster:
    def __init__(self, shard_count: int = 2, replica_count: int = 3):
        self.shards = [Shard(i, replica_count=replica_count) for i in range(shard_count)]
        self.shard_count = shard_count

    def get(self, key: str, epoch: int) -> Optional[Any]:
        sid = shard_for(key, self.shard_count)
        return self.shards[sid].kv.get_at(key, epoch)

    def prepare(self, writes: Dict[int, List[str]]):
        acquired = {}
        for sid, keys in writes.items():
            acquired[sid] = self.shards[sid].kv.lock_keys(keys)
        return acquired

    def commit(self, epoch: int, batched_writes: Dict[int, List[Tuple[str, Any]]]):
        for sid, writes in batched_writes.items():
            self.shards[sid].append_and_apply(epoch, writes)

    def release(self, acquired):
        for sid, locks in acquired.items():
            self.shards[sid].kv.release_locks(locks)

# -----------------------------
# Transactions (with 2PC)
# -----------------------------

class Transaction:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self.reads: List[str] = []
        self.writes: Dict[str, Any] = {}

    def read(self, key: str) -> None:
        self.reads.append(key)

    def write(self, key: str, value: Any) -> None:
        self.writes[key] = value

    def commit(self, epoch_service: EpochService) -> bool:
        per_shard_keys = defaultdict(list)
        for k in self.writes.keys():
            sid = shard_for(k, self.cluster.shard_count)
            per_shard_keys[sid].append(k)

        acquired = self.cluster.prepare(per_shard_keys)
        try:
            epoch = epoch_service.advance()
            per_shard_writes = defaultdict(list)
            for k, v in self.writes.items():
                sid = shard_for(k, self.cluster.shard_count)
                per_shard_writes[sid].append((k, v))
            self.cluster.commit(epoch, per_shard_writes)
            return True
        finally:
            self.cluster.release(acquired)

# -----------------------------
# Snapshot Reader
# -----------------------------

class SnapshotReader:
    def __init__(self, cluster: Cluster, epoch_service: EpochService):
        self.cluster = cluster
        self.epoch_service = epoch_service

    def get(self, key: str) -> Any:
        return self.cluster.get(key, self.epoch_service.current())

# -----------------------------
# Demo
# -----------------------------

def demo():
    epoch = EpochService()
    cluster = Cluster(shard_count=2, replica_count=3)
    snap = SnapshotReader(cluster, epoch)

    # Initial writes
    t1 = Transaction(cluster)
    t1.write("user:1:balance", 100)
    t1.write("user:2:balance", 50)
    print("Init commit:", t1.commit(epoch))

    print("Snapshot user1:", snap.get("user:1:balance"))
    print("Snapshot user2:", snap.get("user:2:balance"))

    # Transfer 30 from user1 to user2
    tx = Transaction(cluster)
    tx.write("user:1:balance", 70)
    tx.write("user:2:balance", 80)
    print("Transfer commit:", tx.commit(epoch))

    print("Snapshot user1:", snap.get("user:1:balance"))
    print("Snapshot user2:", snap.get("user:2:balance"))

if __name__ == "__main__":
    demo()

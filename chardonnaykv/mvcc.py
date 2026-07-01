"""Multi-version key-value storage."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import threading
from typing import DefaultDict, Iterable, List, Optional, Tuple

from .types import Epoch, Key, Value


@dataclass(frozen=True)
class Version:
    epoch: Epoch
    value: Value


class MVCCStore:
    """Append-only version chains plus deterministic key locking."""

    def __init__(self) -> None:
        self._data: DefaultDict[Key, List[Version]] = defaultdict(list)
        self._locks: DefaultDict[Key, threading.RLock] = defaultdict(threading.RLock)
        self._meta_lock = threading.RLock()

    def get_at(self, key: Key, epoch: Epoch) -> Optional[Value]:
        with self._meta_lock:
            versions = self._data.get(key, ())
            for version in reversed(versions):
                if version.epoch <= epoch:
                    return version.value
        return None

    def put_version(self, key: Key, epoch: Epoch, value: Value) -> None:
        with self._meta_lock:
            versions = self._data[key]
            if versions and epoch < versions[-1].epoch:
                raise ValueError(f"cannot append epoch {epoch} after {versions[-1].epoch}")
            versions.append(Version(epoch, value))

    def latest_epoch(self, key: Key) -> Optional[Epoch]:
        with self._meta_lock:
            versions = self._data.get(key, ())
            return versions[-1].epoch if versions else None

    def version_chain(self, key: Key) -> Tuple[Version, ...]:
        with self._meta_lock:
            return tuple(self._data.get(key, ()))

    def acquire_keys(self, keys: Iterable[Key]) -> List[threading.RLock]:
        acquired: List[threading.RLock] = []
        for key in sorted(set(keys)):
            lock = self._locks[key]
            lock.acquire()
            acquired.append(lock)
        return acquired

    @staticmethod
    def release_keys(locks: Iterable[threading.RLock]) -> None:
        for lock in reversed(list(locks)):
            lock.release()

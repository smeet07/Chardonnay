"""Transaction context, dry-run metadata, and snapshot readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, TYPE_CHECKING

from .types import Epoch, Key, Value

if TYPE_CHECKING:
    from .cluster import ChardonnayKV


@dataclass(frozen=True)
class DryRunResult:
    read_epoch: Epoch
    read_keys: tuple[Key, ...]
    write_keys: tuple[Key, ...]
    prefetched: Dict[Key, Optional[Value]]


class TransactionContext:
    """Application-facing transaction object."""

    def __init__(
        self,
        cluster: ChardonnayKV,
        read_epoch: Epoch,
        prefetched: Optional[Dict[Key, Optional[Value]]] = None,
        dry_run: bool = False,
    ) -> None:
        self.cluster = cluster
        self.read_epoch = read_epoch
        self.prefetched = prefetched if prefetched is not None else {}
        self.dry_run = dry_run
        self.read_set: List[Key] = []
        self.writes: Dict[Key, Value] = {}

    def get(self, key: Key) -> Optional[Value]:
        self.read_set.append(key)
        if key not in self.prefetched:
            self.prefetched[key] = self.cluster.get_at(key, self.read_epoch)
        return self.prefetched[key]

    def put(self, key: Key, value: Value) -> None:
        self.writes[key] = value


TransactionProgram = Callable[[TransactionContext], Any]


@dataclass(frozen=True)
class SnapshotReader:
    cluster: ChardonnayKV
    epoch: Epoch

    def get(self, key: Key) -> Optional[Value]:
        return self.cluster.get_at(key, self.epoch)

    def items(self, keys: Iterable[Key]) -> Dict[Key, Optional[Value]]:
        return self.cluster.items_at(keys, self.epoch)

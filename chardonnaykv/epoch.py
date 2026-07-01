"""Epoch service for MVCC commit and snapshot timestamps."""

from __future__ import annotations

import threading

from .types import Epoch


class EpochService:
    """Monotonic timestamp service."""

    def __init__(self) -> None:
        self._epoch: Epoch = 0
        self._lock = threading.Lock()

    def current(self) -> Epoch:
        with self._lock:
            return self._epoch

    def next_epoch(self) -> Epoch:
        with self._lock:
            self._epoch += 1
            return self._epoch

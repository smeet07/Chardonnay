"""Shared type aliases for ChardonnayKV."""

from __future__ import annotations

from typing import Any, Tuple


Key = str
Value = Any
Epoch = int
Ballot = Tuple[int, int]
LogIndex = int
ShardId = int
TxnId = int

"""Simplified in-process Multi-Paxos for per-shard logs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import threading
from typing import DefaultDict, Dict, List, Optional, Tuple

from .types import Ballot, Epoch, Key, LogIndex, TxnId, Value


@dataclass(frozen=True)
class PaxosLogEntry:
    txn_id: TxnId
    epoch: Epoch
    writes: Tuple[Tuple[Key, Value], ...]


@dataclass(frozen=True)
class PaxosPrepare:
    index: LogIndex
    ballot: Ballot


@dataclass(frozen=True)
class PaxosAccept:
    index: LogIndex
    ballot: Ballot
    value: PaxosLogEntry


@dataclass
class SlotState:
    promised: Optional[Ballot] = None
    accepted_ballot: Optional[Ballot] = None
    accepted_value: Optional[PaxosLogEntry] = None


class PaxosAcceptor:
    """Acceptor state is tracked per log slot, not globally."""

    def __init__(self, acceptor_id: int) -> None:
        self.acceptor_id = acceptor_id
        self._slots: DefaultDict[LogIndex, SlotState] = defaultdict(SlotState)
        self._lock = threading.Lock()

    def prepare(self, message: PaxosPrepare) -> Tuple[bool, Optional[Ballot], Optional[PaxosLogEntry]]:
        with self._lock:
            slot = self._slots[message.index]
            if slot.promised is None or message.ballot > slot.promised:
                slot.promised = message.ballot
                return True, slot.accepted_ballot, slot.accepted_value
            return False, slot.accepted_ballot, slot.accepted_value

    def accept(self, message: PaxosAccept) -> bool:
        with self._lock:
            slot = self._slots[message.index]
            if slot.promised is None or message.ballot >= slot.promised:
                slot.promised = message.ballot
                slot.accepted_ballot = message.ballot
                slot.accepted_value = message.value
                return True
            return False


class PaxosLearner:
    def __init__(self, quorum_size: int) -> None:
        self.quorum_size = quorum_size
        self._votes: DefaultDict[Tuple[LogIndex, Ballot, PaxosLogEntry], int] = defaultdict(int)
        self._chosen: Dict[LogIndex, PaxosLogEntry] = {}
        self._lock = threading.Lock()

    def observe_accept(self, index: LogIndex, ballot: Ballot, value: PaxosLogEntry) -> Optional[PaxosLogEntry]:
        with self._lock:
            if index in self._chosen:
                return self._chosen[index]
            vote_key = (index, ballot, value)
            self._votes[vote_key] += 1
            if self._votes[vote_key] >= self.quorum_size:
                self._chosen[index] = value
                return value
            return None

    def chosen(self, index: LogIndex) -> Optional[PaxosLogEntry]:
        with self._lock:
            return self._chosen.get(index)


class MultiPaxosLeader:
    """A stable leader that runs one prepare+accept round per log slot."""

    def __init__(self, leader_id: int, acceptors: List[PaxosAcceptor], learner: PaxosLearner) -> None:
        self.leader_id = leader_id
        self.acceptors = acceptors
        self.learner = learner
        self._term = 1
        self._next_index = 0
        self._lock = threading.Lock()

    @property
    def quorum_size(self) -> int:
        return len(self.acceptors) // 2 + 1

    def propose(self, value: PaxosLogEntry) -> Tuple[LogIndex, PaxosLogEntry]:
        with self._lock:
            index = self._next_index
            self._next_index += 1
            ballot = (self._term, self.leader_id)

        proposal = self._prepare(index, ballot, value)
        chosen = self._accept(index, ballot, proposal)
        if chosen is None:
            raise RuntimeError("consensus was not reached")
        return index, chosen

    def _prepare(self, index: LogIndex, ballot: Ballot, value: PaxosLogEntry) -> PaxosLogEntry:
        acknowledgements = 0
        highest_accepted_ballot: Optional[Ballot] = None
        inherited_value: Optional[PaxosLogEntry] = None

        for acceptor in self.acceptors:
            ok, accepted_ballot, accepted_value = acceptor.prepare(PaxosPrepare(index, ballot))
            if not ok:
                continue
            acknowledgements += 1
            if accepted_ballot is not None and accepted_value is not None:
                if highest_accepted_ballot is None or accepted_ballot > highest_accepted_ballot:
                    highest_accepted_ballot = accepted_ballot
                    inherited_value = accepted_value

        if acknowledgements < self.quorum_size:
            raise RuntimeError("prepare phase did not reach quorum")
        return inherited_value if inherited_value is not None else value

    def _accept(
        self,
        index: LogIndex,
        ballot: Ballot,
        value: PaxosLogEntry,
    ) -> Optional[PaxosLogEntry]:
        for acceptor in self.acceptors:
            if acceptor.accept(PaxosAccept(index, ballot, value)):
                chosen = self.learner.observe_accept(index, ballot, value)
                if chosen is not None:
                    return chosen
        return self.learner.chosen(index)

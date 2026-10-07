"""Journaled registry of idempotent write requests.

Every first execution of an idempotent write is registered here as a
``request.register`` command in the same transaction as the domain commands it
produced, and every later duplicate is marked with its own ``request.replay``
command. Replaying the journal therefore rebuilds both the ledger and every
audit trail link, and nothing about a replay lives only in memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bagsort.errors import ConflictError, NotFoundError
from bagsort.model.entities import RequestRecord, SavedResponse
from bagsort.store.store import Store, Session
from .keys import DIGEST_NAMESPACE


@dataclass(frozen=True)
class LedgerHit:
    """What the layer needs to know when a key is already registered."""

    record: RequestRecord
    same_fingerprint: bool


class RequestRegistry:
    """Read side and transactional write side of the idempotency ledger."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def _rows(self) -> list[RequestRecord]:
        return self._store.state().requests

    def records(self) -> list[RequestRecord]:
        return list(self._rows())

    def find(self, key: str) -> RequestRecord | None:
        for record in self._rows():
            if record.idempotency_key == key:
                return record
        return None

    def lookup(self, key: str, request_fingerprint: str) -> LedgerHit | None:
        record = self.find(key)
        if record is None:
            return None
        return LedgerHit(record=record, same_fingerprint=record.fingerprint == request_fingerprint)

    def get(self, index: int) -> RequestRecord:
        for record in self._rows():
            if record.index == index:
                return record
        raise NotFoundError(f"no request registered with index {index}")

    def replay_count(self) -> int:
        return sum(len(record.replays) for record in self._rows())

    def register(
        self,
        session: Session,
        *,
        key: str,
        source: str,
        method: str,
        path: str,
        request_fingerprint: str,
        response: SavedResponse,
        at: str,
        events: tuple[str, ...],
        first_sequence: int,
        command_count: int,
        register_audit_sequence: int,
    ) -> RequestRecord:
        """Append the ledger row inside the caller's open transaction."""

        if self.find(key) is not None:
            raise ConflictError(f"idempotency key {key!r} is already registered")
        index = len(self._rows()) + 1
        record = RequestRecord(
            index=index,
            idempotency_key=key,
            method=method,
            path=path,
            fingerprint=request_fingerprint,
            source=source,
            status="executed",
            response=response,
            registered_at=at,
            first_sequence=first_sequence,
            command_count=command_count,
            register_audit_sequence=register_audit_sequence,
            events=events,
        )
        session.state.requests.append(record)
        session.record("request.register", {"record": record.as_dict()}, at)
        return record

    def mark_replay(
        self,
        session: Session,
        record: RequestRecord,
        *,
        at: str,
        audit_sequence: int,
        journal_sequence: int,
    ) -> dict[str, Any]:
        """Attach one replay marker to a registered row in this transaction."""

        entry: dict[str, Any] = {
            "replay": len(record.replays) + 1,
            "at": at,
            "audit_sequence": audit_sequence,
            "journal_sequence": journal_sequence,
        }
        updated = record.replayed(entry)
        session.state.requests = [
            updated if existing.index == record.index else existing
            for existing in session.state.requests
        ]
        session.record(
            "request.replay",
            {"index": record.index, "entry": entry},
            at,
        )
        return entry

    def digest_keys(self) -> int:
        return sum(1 for record in self._rows() if record.source == DIGEST_NAMESPACE)

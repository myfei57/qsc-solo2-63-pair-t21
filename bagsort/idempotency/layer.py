"""Idempotency orchestration around one write request.

A first arrival executes the handler, saves the exact response in the request
registry inside the same transaction as the domain commands, and chains an
audit entry. A duplicate arrival never reaches the handler: its original
response is reconstructed byte for byte, no domain event is published, and one
individual replay entry is appended to both the ledger and the audit chain.
"""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any, Callable

from bagsort.errors import ConflictError
from bagsort.model.entities import SavedResponse
from .keys import CALLER_KEY_NAMESPACE, DIGEST_NAMESPACE, fingerprint, supplied_key
from .policy import replay_allowed
from .registry import RequestRegistry

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from bagsort.audit.chain import AuditChain
    from bagsort.clock import Clock
    from bagsort.console.http import Request, Response
    from bagsort.events.bus import EventBus
    from bagsort.store.store import Store

AUDIT_STAGE = "audit"
REGISTER_KIND = "request_registered"
REPLAY_KIND = "request_replayed"


class _EventCapture:
    """Records the topics one execution published by bracketing bus history.

    The capture never subscribes itself: adding and removing a wildcard
    subscriber would disturb the fixed subscriber order the audit chain and
    telemetry depend on. Instead it remembers where the bounded bus history
    started and reads only the events appended afterwards.
    """

    def __init__(self, bus: "EventBus") -> None:
        self._bus = bus
        self.topics: list[str] = []
        self._start = 0

    def __enter__(self) -> "_EventCapture":
        self._start = len(self._bus.history())
        return self

    def __exit__(self, *exc: object) -> None:
        history = self._bus.history()
        self.topics = [event.topic for event in history[self._start :]]


class IdempotencyLayer:
    """Decides execute versus replay and keeps both paths auditable."""

    def __init__(
        self,
        store: "Store",
        registry: RequestRegistry,
        audit: "AuditChain",
        bus: "EventBus",
        clock: "Clock",
    ) -> None:
        self._store = store
        self._registry = registry
        self._audit = audit
        self._bus = bus
        self._clock = clock
        self._inflight: dict[str, threading.Event] = {}
        self._guard = threading.Lock()

    @property
    def registry(self) -> RequestRegistry:
        return self._registry

    def eligible(self, request: "Request") -> bool:
        return replay_allowed(request.method, request.path, request.caller_key())

    def resolve_key(self, request: "Request") -> tuple[str, str, str]:
        """Return ``(key, request_fingerprint, source)`` for one request."""

        request_fingerprint = fingerprint(
            request.method, request.path, request.payload, request.items
        )
        caller = supplied_key(request.method, request.path, request.caller_key())
        if caller is not None:
            return caller, request_fingerprint, CALLER_KEY_NAMESPACE
        return request_fingerprint, request_fingerprint, DIGEST_NAMESPACE

    def serve(
        self,
        request: "Request",
        execute: Callable[[], "Response"],
    ) -> tuple["Response", str]:
        """Run a write once and replay its exact answer afterwards.

        Returns the response and one of ``executed``, ``replayed`` or
        ``failed`` so the server can count each class without inspecting state.
        """

        key, request_fingerprint, source = self.resolve_key(request)
        hit = self._registry.lookup(key, request_fingerprint)
        if hit is not None:
            if not hit.same_fingerprint:
                raise ConflictError(
                    f"idempotency key {key!r} was first used with a different request body"
                )
            return self._replay(hit.record), "replayed"

        # Concurrent identical writes: the second waits for the first to be
        # registered, then takes the replay path instead of executing twice.
        with self._guard:
            waiter = self._inflight.get(key)
            if waiter is None:
                waiter = threading.Event()
                self._inflight[key] = waiter
                leader = True
            else:
                leader = False
        if not leader:
            waiter.wait()
            record = self._registry.find(key)
            if record is None:
                # The leader failed and recorded nothing; run this copy normally.
                return self._serve_fresh(request, key, source, request_fingerprint, execute)
            if record.fingerprint != request_fingerprint:
                raise ConflictError(
                    f"idempotency key {key!r} was first used with a different request body"
                )
            return self._replay(record), "replayed"

        try:
            return self._serve_fresh(request, key, source, request_fingerprint, execute)
        finally:
            with self._guard:
                self._inflight.pop(key, None)
            waiter.set()

    def _serve_fresh(
        self,
        request: "Request",
        key: str,
        source: str,
        request_fingerprint: str,
        execute: Callable[[], "Response"],
    ) -> tuple["Response", str]:
        capture = _EventCapture(self._bus)
        first_sequence = self._store.watermark + 1
        with capture:
            response = execute()
        if response.status >= 400:
            # Failures are not remembered: a later retry must still run.
            return response, "failed"

        self._register(
            request,
            key,
            source,
            request_fingerprint,
            response,
            tuple(capture.topics),
            first_sequence,
            self._store.watermark - first_sequence + 1,
        )
        return response, "executed"

    def _register(
        self,
        request: "Request",
        key: str,
        source: str,
        request_fingerprint: str,
        response: "Response",
        topics: tuple[str, ...],
        first_sequence: int,
        command_count: int,
    ) -> None:
        at = self._clock.now_iso()
        self._clock.advance(1)
        with self._store.session() as session:
            entry = self._audit.stamp(
                session,
                kind=REGISTER_KIND,
                subject=key,
                stage=AUDIT_STAGE,
                detail={
                    "method": request.method,
                    "path": request.path,
                    "status": response.status,
                    "source": source,
                    "events": list(topics),
                },
                at=at,
            )
            self._registry.register(
                session,
                key=key,
                source=source,
                method=request.method,
                path=request.path,
                request_fingerprint=request_fingerprint,
                response=save(response),
                at=at,
                events=topics,
                first_sequence=first_sequence,
                command_count=command_count,
                register_audit_sequence=entry.sequence,
            )

    def _replay(self, record) -> "Response":
        at = self._clock.now_iso()
        self._clock.advance(1)
        with self._store.session() as session:
            # The replay commands are appended in this same transaction, so the
            # audit.append/request.replay pair starts one past the watermark.
            journal_sequence = self._store.watermark + 2
            entry = self._audit.stamp(
                session,
                kind=REPLAY_KIND,
                subject=record.idempotency_key,
                stage=AUDIT_STAGE,
                detail={
                    "method": record.method,
                    "path": record.path,
                    "register_index": record.index,
                    "replay": len(record.replays) + 1,
                    "status": record.response.status,
                },
                at=at,
            )
            self._registry.mark_replay(
                session,
                record,
                at=at,
                audit_sequence=entry.sequence,
                journal_sequence=journal_sequence,
            )
        return restore(record.response)


def save(response: "Response") -> SavedResponse:
    """Capture a response so it can be rebuilt with the same bytes."""

    if response.content_type.startswith("application/json"):
        payload: Any = json.loads(response.body.decode("utf-8"))
    else:
        payload = response.body.decode("utf-8")
    return SavedResponse(status=response.status, payload=payload, content_type=response.content_type)


def restore(saved_response: SavedResponse) -> "Response":
    """Rebuild the first response without consulting the handler again."""

    from bagsort.console.http import Response

    if saved_response.content_type.startswith("application/json"):
        body = json.dumps(saved_response.payload, ensure_ascii=False).encode("utf-8")
    else:
        body = str(saved_response.payload).encode("utf-8")
    return Response(
        status=saved_response.status,
        body=body,
        content_type=saved_response.content_type,
    )

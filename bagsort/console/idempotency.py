"""Idempotent execution of console write requests.

A write (POST/PUT/PATCH/DELETE) is filed under an idempotency key before it
runs. The first arrival executes and its response is registered; a later
arrival with the same key and fingerprint gets the stored response back byte
for byte instead of re-executing, and the replay itself is journaled, audited
and counted so it can be reviewed one by one. The same key carrying different
business parameters is a conflict, never a silent replay.

Writes run under a lock: the check-execute-register sequence must be atomic,
and the store's pending buffer is not safe for interleaved writers anyway.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable

from bagsort.requests.keys import derive_key
from .http import Request, Response, error_response
from .middleware import guarded

if TYPE_CHECKING:  # pragma: no cover - typing only
    from bagsort.app import Plant

WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class IdempotencyGuard:
    """Decides execute-once versus replay for every console write."""

    def __init__(self, plant: "Plant") -> None:
        self._plant = plant
        self._lock = threading.Lock()

    def execute(self, request: Request, call: Callable[[], Response]) -> Response:
        """Run ``call`` at most once per idempotency key; replay the rest."""

        if request.method.upper() not in WRITE_METHODS:
            return guarded(call)
        with self._lock:
            return self._execute_write(request, call)

    def _execute_write(self, request: Request, call: Callable[[], Response]) -> Response:
        plant = self._plant
        choice = derive_key(
            request.method, request.path, request.query, request.payload, request.items
        )
        registered = plant.requests.lookup(choice.key)
        if registered is not None:
            if registered.fingerprint != choice.fingerprint:
                return error_response(
                    409,
                    f"idempotency key {choice.key!r} was already used "
                    "with different business parameters",
                )
            plant.replay_request(choice.key)
            return Response(
                status=registered.status,
                body=registered.body.encode("utf-8"),
                content_type=registered.content_type,
            )
        response = guarded(call)
        if 200 <= response.status < 300:
            plant.register_request(
                key=choice.key,
                fingerprint=choice.fingerprint,
                caller_key=choice.caller_supplied,
                method=request.method.upper(),
                path=request.path,
                status=response.status,
                content_type=response.content_type,
                body=response.body.decode("utf-8"),
            )
        return response

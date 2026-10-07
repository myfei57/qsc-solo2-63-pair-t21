"""The request register: which writes ran, what they answered, what replayed.

The ledger lives in the same journal as every other command, so a restart
rebuilds it from the watermark and the counts line up with the command log:
one ``request.open`` per registered request, one ``request.replay`` per
replayed arrival. That is what lets the register, the journal and the event
counters corroborate each other instead of drifting apart.
"""

from __future__ import annotations

from bagsort.clock import Clock
from bagsort.errors import ConflictError
from bagsort.model.entities import RegisteredRequest, RequestReplay
from bagsort.store.store import Store


class RequestLedger:
    """Read and append the idempotency register kept in the store."""

    def __init__(self, store: Store, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    # -- read side ---------------------------------------------------------

    def lookup(self, key: str) -> RegisteredRequest | None:
        return self._store.state().requests.get(key)

    def requests(self) -> list[RegisteredRequest]:
        return list(self._store.state().requests.values())

    def replays(self) -> list[RequestReplay]:
        return list(self._store.state().request_replays)

    def replay_count(self) -> int:
        return len(self._store.state().request_replays)

    # -- write side --------------------------------------------------------

    def register(
        self,
        key: str,
        fingerprint: str,
        caller_key: bool,
        method: str,
        path: str,
        status: int,
        content_type: str,
        body: str,
    ) -> RegisteredRequest:
        """File a freshly executed write so a resend replays instead of running."""

        if self.lookup(key) is not None:
            raise ConflictError(f"request key {key!r} is already registered")
        registered = RegisteredRequest(
            key=key,
            fingerprint=fingerprint,
            method=method,
            path=path,
            status=status,
            content_type=content_type,
            body=body,
            caller_key=caller_key,
            at=self._clock.now_iso(),
        )
        with self._store.session() as session:
            session.state.requests[key] = registered
            session.record(
                "request.open", {"request": registered.as_dict()}, registered.at
            )
        return registered

    def note_replay(self, key: str) -> RequestReplay:
        """Record one replayed arrival; the journal keeps it reviewable forever."""

        registered = self.lookup(key)
        if registered is None:
            raise ConflictError(f"request key {key!r} is not registered")
        replay = RequestReplay(
            key=key,
            ordinal=registered.replays + 1,
            method=registered.method,
            path=registered.path,
            at=self._clock.now_iso(),
        )
        with self._store.session() as session:
            session.state.request_replays.append(replay)
            session.state.requests[key] = registered.evolved(replays=replay.ordinal)
            session.record("request.replay", {"replay": replay.as_dict()}, replay.at)
        return replay

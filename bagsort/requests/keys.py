"""Idempotency keys: how a repeated write request is recognised.

The console resends requests, so every write is assigned an idempotency key
and a fingerprint before it runs:

* **Key** — the caller's own ``request_id`` (their 单号) when the payload
  carries one, otherwise a digest the service derives from the request
  itself. A caller key recognises a resend no matter how the transport
  re-encodes it, as long as the caller reuses the 单号; the derived digest
  recognises only resends whose canonical request is identical. The caller
  key therefore has the higher recognition rate, which is why it wins when
  both are available.
* **Fingerprint** — a SHA-256 over the canonical request (method, path,
  query and body), always computed and stored at registration time. Two
  arrivals only replay when key *and* fingerprint agree, so different
  business parameters can never share a key: a reused caller 单号 carrying
  different parameters is rejected as a conflict, not executed and not
  replayed.

Replay boundary, stated once here and served verbatim by ``policy()``:

1. A request is a *replay* iff its key and fingerprint both match a
   registered request. The stored response goes back byte for byte and
   nothing executes again.
2. Same key with a different fingerprint is a conflict (409): the key was
   already spent on different business parameters.
3. A key the ledger has never seen is a new request and executes.
4. Only successful (2xx) outcomes are registered. Failures are never
   replayed, so a corrected retry runs normally.
5. Registration happens after the write commits; a crash in between is the
   one window where a resend would execute twice, and the domain level
   dedup (for example the sorter refusing a bag it already sorted) remains
   the backstop for that window.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

CALLER_KEY_FIELD = "request_id"


@dataclass(frozen=True)
class KeyChoice:
    """The key a request was filed under and the fingerprint it must match."""

    key: str
    fingerprint: str
    caller_supplied: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "fingerprint": self.fingerprint,
            "caller_supplied": self.caller_supplied,
        }


def canonical_form(
    method: str,
    path: str,
    query: dict[str, list[str]],
    payload: dict[str, object],
    items: tuple[object, ...],
) -> str:
    """Stable rendering of one request; key order and whitespace drop out."""

    material = {
        "method": method.upper(),
        "path": path,
        "query": {name: list(values) for name, values in sorted(query.items())},
        "payload": payload,
        "items": list(items),
    }
    return json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint_of(
    method: str,
    path: str,
    query: dict[str, list[str]],
    payload: dict[str, object],
    items: tuple[object, ...],
) -> str:
    """The digest two arrivals must share to be considered the same request."""

    return hashlib.sha256(
        canonical_form(method, path, query, payload, items).encode("utf-8")
    ).hexdigest()


def derive_key(
    method: str,
    path: str,
    query: dict[str, list[str]],
    payload: dict[str, object],
    items: tuple[object, ...],
) -> KeyChoice:
    """Choose the caller's 单号 when offered, else derive the request digest."""

    fingerprint = fingerprint_of(method, path, query, payload, items)
    caller = payload.get(CALLER_KEY_FIELD, "")
    if isinstance(caller, str) and caller.strip():
        return KeyChoice(key=f"caller:{caller.strip()}", fingerprint=fingerprint, caller_supplied=True)
    return KeyChoice(key=f"digest:{fingerprint}", fingerprint=fingerprint, caller_supplied=False)


def policy() -> dict[str, object]:
    """The registered key strategy and replay boundary, for operators to read."""

    return {
        "key": "caller request_id when present, otherwise the request digest",
        "caller_key_field": CALLER_KEY_FIELD,
        "fingerprint": "sha256 over the canonical method, path, query and body",
        "recognition": {
            "caller_key": "a resend is recognised whenever the same request_id "
            "arrives with the same business parameters",
            "digest_key": "a resend is recognised only when the canonical "
            "request is identical (key order and whitespace normalised)",
        },
        "boundary": [
            "replay requires the same key and the same fingerprint; the stored "
            "response is returned byte for byte and nothing executes again",
            "the same key with a different fingerprint is a 409 conflict: "
            "different business parameters never share an idempotency key",
            "an unknown key is a new request and executes",
            "only 2xx outcomes are registered; failures are never replayed",
            "registration follows the write commit; the domain dedup stays the "
            "backstop for a crash between the two",
        ],
    }

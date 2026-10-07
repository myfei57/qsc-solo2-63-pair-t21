"""Idempotency key derivation.

Two ways to identify a repeated write request coexist here:

* ``fingerprint`` hashes the request method, path and a canonical rendering of
  the JSON body. It needs no caller cooperation and two different business
  parameter sets can never share a digest, but it only recognises a resend when
  the body survives byte-for-byte in content.
* ``scope_key`` wraps a caller supplied order number, scoped to the endpoint so
  the same number used for two different operations never collides. It has the
  higher replay recognition rate because the console keeps resending the number
  even when the body changes in insignificant ways.

The boundary is deliberately fixed: a supplied key always overrides the digest,
and a supplied key reused alongside a different fingerprint is rejected as a
conflict instead of silently returning the first result. Callers must not use
one order number for two business requests.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

CALLER_KEY_FIELD = "request_id"
CALLER_KEY_NAMESPACE = "caller"
DIGEST_NAMESPACE = "digest"


def canonical_body(payload: dict[str, Any], items: tuple[Any, ...]) -> str:
    """Deterministic rendering of what the handler actually sees.

    Control fields that the caller is allowed to vary on a resend are stripped.
    """

    body: Any
    if items:
        body = list(items)
    else:
        body = {key: value for key, value in sorted(payload.items()) if key != CALLER_KEY_FIELD}
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(method: str, path: str, payload: dict[str, Any], items: tuple[Any, ...]) -> str:
    """The default idempotency key: a digest over the whole write request."""

    material = "\n".join([method.upper(), path, canonical_body(payload, items)])
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"{DIGEST_NAMESPACE}:{digest[:32]}"


def supplied_key(method: str, path: str, order_number: str) -> str | None:
    """Scope a caller provided order number to this endpoint."""

    order_number = order_number.strip()
    if not order_number:
        return None
    return scope_key(method, path, order_number)


def scope_key(method: str, path: str, order_number: str) -> str:
    """Bind a caller order number to one endpoint so it cannot be shared."""

    material = "\n".join([method.upper(), path, order_number])
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return f"{CALLER_KEY_NAMESPACE}:{digest}:{order_number}"

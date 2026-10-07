"""Replay boundary: which writes are idempotised and under which rules.

Only state changing business operations participate. Administrative resets and
journal maintenance are deliberately excluded: replaying one of those would
silently suppress an operator action rather than protect a business write.
Reads never participate because a repeated read has no duplicate side effect to
begin with.
"""

from __future__ import annotations

WRITE_METHODS = {"POST", "DELETE"}

# Prefixes kept outside the idempotency boundary even though they are writes.
EXCLUDED_PREFIXES = (
    "/api/admin/",
    "/api/metrics",
    "/api/telemetry/reset",
    "/api/store/",
)

# Validate endpoints only inspect their payload; they must always run.
EXCLUDED_PATHS = {
    "/api/flights/validate",
    "/api/chutes/validate",
}

# "Perform the next action" writes: no entity in the path and no business field
# in the body, so two requests are byte identical even though each call advances
# the plant (dequeue the next bag, rotate the belt, clear a queue). A body digest
# cannot tell two genuine actions apart, so these only replay when the caller
# supplies an explicit order number; without one they always execute.
UNKEYED_ACTION_PATHS = {
    "/api/recheck/next",
    "/api/recheck/clear",
    "/api/belt/rotate",
}


def is_idempotent(method: str, path: str) -> bool:
    """Whether a repeated identical request to this endpoint must replay."""

    if method.upper() not in WRITE_METHODS:
        return False
    if path in EXCLUDED_PATHS:
        return False
    return not any(path.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def replay_allowed(method: str, path: str, caller_key: str) -> bool:
    """Whether this concrete request may be served from the ledger.

    Ordinary writes always participate. An unkeyed advancing action participates
    only when the caller attached an order number, because its empty body is not
    a unique identity.
    """

    if not is_idempotent(method, path):
        return False
    if path in UNKEYED_ACTION_PATHS and not caller_key:
        return False
    return True


def policy_document() -> dict[str, object]:
    """The boundary description exposed on the console."""

    return {
        "write_methods": sorted(WRITE_METHODS),
        "excluded_prefixes": list(EXCLUDED_PREFIXES),
        "excluded_paths": sorted(EXCLUDED_PATHS),
        "unkeyed_action_paths": sorted(UNKEYED_ACTION_PATHS),
        "default_key": "sha256 digest over method, path and the canonical JSON body",
        "caller_key": "a non-empty Idempotency-Key header or request_id field is scoped to the endpoint and overrides the digest",
        "unkeyed_action_rule": "bodyless advancing actions only replay with an explicit caller key, otherwise every call executes",
        "key_reuse": "a caller key arriving with a different body is answered 409",
        "replay_scope": "duplicates are recognised per terminal and retained until the journal is reset",
        "failure_rule": "only responses with a status below 400 are remembered and replayed",
        "replay_effect": "the handler is not rerun, no domain events are emitted, one replay audit entry is chained",
    }

"""Cross verification of the request registry, command log and audit chain.

The three stories a replay leaves must agree row by row:

* the request registry says where the first execution's commands start and
  which audit entry registered the request;
* the command journal actually contains the request commands at those
  positions;
* the audit chain carries one register entry per row and one replay entry per
  replay marker.

A mismatch here means one of the three was edited independently of the others.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from bagsort.model.entities import AuditEntry
from .layer import REGISTER_KIND, REPLAY_KIND
from .registry import RequestRegistry

# Topic -> journal command kinds that prove the event really happened.
EVENT_COMMANDS = {
    "bag.checked_in": {"bag.put"},
    "bag.sorted": {"sort.record"},
    "bag.diverted": {"bag.put"},
    "bag.recheck_enqueued": {"recheck.enqueue"},
    "flight.closed": {"bag.put", "flight.put"},
    "chute.assigned": {"chute.assign"},
    "belt.switched": {"belt.set", "belt.clear"},
    "bag.remapped": {"mapping.put"},
}


def _audit_by_sequence(audit: list[AuditEntry]) -> dict[int, AuditEntry]:
    return {entry.sequence: entry for entry in audit}


def verify(registry: RequestRegistry, entries, audit: list[AuditEntry]) -> dict[str, Any]:
    """Return one finding per disagreement, plus the reconciled totals."""

    findings: list[str] = []
    audit_by_seq = _audit_by_sequence(audit)
    entries_by_seq = {entry.sequence: entry for entry in entries}
    rows = registry.records()

    event_counts: Counter[str] = Counter()
    registered_audits = 0
    replayed_audits = 0
    for record in rows:
        # -- register audit entry links back to this row -------------------
        register_entry = audit_by_seq.get(record.register_audit_sequence)
        if register_entry is None or register_entry.kind != REGISTER_KIND:
            findings.append(
                f"request {record.index}: register audit {record.register_audit_sequence} missing"
            )
        elif register_entry.subject != record.idempotency_key:
            findings.append(f"request {record.index}: register audit subject mismatch")
        else:
            registered_audits += 1

        # -- first execution really wrote commands into the journal --------
        first = entries_by_seq.get(record.first_sequence)
        if first is None:
            findings.append(f"request {record.index}: first command {record.first_sequence} missing")
        elif first.kind.startswith("request."):
            findings.append(
                f"request {record.index}: first command {record.first_sequence} is not a domain command"
            )

        # -- every declared event has a matching domain command in the log --
        window_end = record.first_sequence + max(record.command_count, 0)
        window = [
            entry.kind
            for entry in entries
            if record.first_sequence <= entry.sequence < window_end
        ]
        if record.command_count and len(window) != record.command_count:
            findings.append(
                f"request {record.index}: journal window has {len(window)} commands, "
                f"ledger claims {record.command_count}"
            )
        for topic in record.events:
            kinds = EVENT_COMMANDS.get(topic)
            if kinds is None:
                findings.append(f"request {record.index}: unknown event topic {topic!r}")
                continue
            event_counts[topic] += 1
            if not (set(window) & kinds):
                findings.append(
                    f"request {record.index}: declared event {topic!r} has no {sorted(kinds)} command"
                )

        # -- every replay marker has its audit entry and its own command ----
        for marker in record.replays:
            replay_no = int(marker["replay"])
            audit_seq = int(marker["audit_sequence"])
            journal_seq = int(marker["journal_sequence"])
            replay_entry = audit_by_seq.get(audit_seq)
            if replay_entry is None or replay_entry.kind != REPLAY_KIND:
                findings.append(
                    f"request {record.index} replay {replay_no}: audit {audit_seq} missing"
                )
            elif replay_entry.subject != record.idempotency_key:
                findings.append(
                    f"request {record.index} replay {replay_no}: audit subject mismatch"
                )
            else:
                replayed_audits += 1
            command = entries_by_seq.get(journal_seq)
            if command is None or command.kind != "request.replay":
                findings.append(
                    f"request {record.index} replay {replay_no}: journal {journal_seq} missing"
                )
            elif int(command.payload.get("index", -1)) != record.index:
                findings.append(
                    f"request {record.index} replay {replay_no}: journal row index mismatch"
                )

    audit_registered = sum(1 for entry in audit if entry.kind == REGISTER_KIND)
    audit_replayed = sum(1 for entry in audit if entry.kind == REPLAY_KIND)
    journal_registered = sum(1 for entry in entries if entry.kind == "request.register")
    journal_replayed = sum(1 for entry in entries if entry.kind == "request.replay")

    if audit_registered != len(rows) or journal_registered != len(rows):
        findings.append(
            f"register totals disagree: registry={len(rows)} "
            f"audit={audit_registered} journal={journal_registered}"
        )
    ledger_replays = sum(len(row.replays) for row in rows)
    if audit_replayed != ledger_replays or journal_replayed != ledger_replays:
        findings.append(
            f"replay totals disagree: registry={ledger_replays} "
            f"audit={audit_replayed} journal={journal_replayed}"
        )

    return {
        "intact": not findings,
        "findings": findings,
        "registered": len(rows),
        "replayed": ledger_replays,
        "register_audit": registered_audits,
        "replay_audit": replayed_audits,
        "events": dict(sorted(event_counts.items())),
        "digest_keys": registry.digest_keys(),
        "caller_keys": len(rows) - registry.digest_keys(),
    }

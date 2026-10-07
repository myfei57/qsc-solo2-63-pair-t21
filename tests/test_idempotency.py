"""Idempotent writes: execute once, replay the exact result, auditable.

Covers the console resend scenario end to end: a duplicated write returns the
first response byte for byte, the domain log and event counters move only once,
the replay is individually auditable, the request registry, command journal
and audit chain cross check, and the ledger survives a snapshot restart.
"""

from __future__ import annotations

import json

import pytest

from bagsort.app import Plant
from bagsort.clock import Clock
from bagsort.console.server import Server
from bagsort.idempotency.keys import (
    canonical_body,
    fingerprint,
    scope_key,
    supplied_key,
)
from bagsort.store.store import Store
from tests.support import make_server


def call(server, method, path, payload=None, headers=None):
    response = server.call(method, path, payload, headers=headers)
    if response.content_type.startswith("application/json"):
        return response.status, json.loads(response.body)
    return response.status, response.body.decode("utf-8")


def _ready_server(server):
    status, flight = call(
        server,
        "POST",
        "/api/flights",
        {"code": "CA1234", "carousel": "CR1", "chute": "A"},
    )
    assert status == 201
    call(
        server,
        "POST",
        "/api/chutes/assign",
        {"flight_id": flight["id"], "chute_id": "T1-CHA"},
    )
    call(
        server,
        "POST",
        "/api/bags/checkin",
        {"barcode": "3001", "flight_id": flight["id"]},
    )
    return flight["id"]


def test_duplicate_sort_dispatch_runs_once_and_returns_the_first_result(server):
    flight_id = _ready_server(server)
    bag_id = "T1-3001"

    first_status, first = call(server, "POST", "/api/sort/dispatch", {"bag_id": bag_id})
    second_status, second = call(server, "POST", "/api/sort/dispatch", {"bag_id": bag_id})
    third_status, third = call(server, "POST", "/api/sort/dispatch", {"bag_id": bag_id})

    assert (first_status, second_status, third_status) == (201, 201, 201)
    assert first == second == third
    # The sorter itself still holds exactly one sort record.
    assert len(server.plant.sorter.records()) == 1
    assert server.plant.sorter.log.count_for(bag_id) == 1


def test_replays_do_not_publish_domain_events_or_move_event_counts(server):
    flight_id = _ready_server(server)
    before = server.plant.telemetry.snapshot()
    delivered_before = server.plant.bus.delivered()

    # The /api/sort endpoint (push_bag) publishes bag.sorted on first run.
    status, first = call(
        server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"}
    )
    assert status == 201
    after_first = server.plant.telemetry.snapshot()
    assert after_first.get("bag.sorted") == (before.get("bag.sorted", 0) + 1)

    for _ in range(3):
        status, again = call(
            server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"}
        )
        assert (status, again) == (201, first)

    assert server.plant.telemetry.snapshot() == after_first
    # Only the first execution produced a domain event; replays are not events.
    assert server.plant.bus.delivered() == delivered_before + 1
    assert server.plant.sorter.log.count_for("T1-3001") == 1


def test_replay_leaves_individually_reviewable_audit_and_ledger_rows(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})

    status, ledger = call(server, "GET", "/api/idempotency/requests")
    dispatch = [row for row in ledger["records"] if row["path"] == "/api/sort"]
    assert len(dispatch) == 1
    row = dispatch[0]
    assert row["replay_count"] == 2
    # Each replay carries its own at/audit/journal coordinates.
    first_replay, second_replay = row["replays"]
    assert first_replay["replay"] == 1 and second_replay["replay"] == 2
    assert first_replay["at"] != second_replay["at"]
    assert first_replay["audit_sequence"] < second_replay["audit_sequence"]
    assert first_replay["journal_sequence"] < second_replay["journal_sequence"]

    status, single = call(
        server, "GET", f"/api/idempotency/requests/{row['index']}"
    )
    assert status == 200
    assert single["replays"] == row["replays"]
    assert single["response"]["payload"]["chute_id"] == "T1-CHA"
    assert single["events"] == ["bag.sorted"]

    audit = server.plant.audit
    kinds = audit.by_kind()
    assert kinds["request_registered"] == ledger["registered"]
    assert kinds["request_replayed"] == 2
    replayed = [entry for entry in audit.entries() if entry.kind == "request_replayed"]
    assert [entry.detail["replay"] for entry in replayed] == [1, 2]
    assert audit.verify() is True


def test_registry_journal_and_audit_cross_check(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-3001"})
    call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-3001"})
    call(server, "POST", "/api/bags/checkin", {"barcode": "4001", "flight_id": flight_id})

    status, report = call(server, "GET", "/api/idempotency/verify")
    assert status == 200
    assert report["intact"] is True
    assert report["findings"] == []
    assert report["registered"] >= 4
    assert report["replayed"] == 1
    assert report["register_audit"] == report["registered"]
    assert report["replay_audit"] == 1


def test_different_business_parameters_get_different_keys_and_both_execute(server):
    _ready_server(server)
    status_a, body_a = call(server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1})
    status_b, body_b = call(server, "POST", "/api/belt/set", {"carousel": "T1-CR2", "batch": 2})
    assert (status_a, status_b) == (200, 200)
    assert body_a["carousel"] != body_b["carousel"]

    # Their fingerprints differ and neither is a replay of the other.
    fp_one = fingerprint("POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1}, ())
    fp_two = fingerprint("POST", "/api/belt/set", {"carousel": "T1-CR2", "batch": 2}, ())
    assert fp_one != fp_two
    rows = server.plant.requests.records()
    belt_rows = [row for row in rows if row.path == "/api/belt/set"]
    assert len(belt_rows) == 2
    assert {row.idempotency_key for row in belt_rows} == {fp_one, fp_two}


def test_caller_order_number_is_scoped_per_endpoint(server):
    _ready_server(server)
    call(server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1},
         headers={"Idempotency-Key": "ORD-9"})
    # Same number on a different endpoint is a different key and executes.
    status, body = call(
        server,
        "POST",
        "/api/belt/switch",
        {"carousel": "T1-CR2", "batch": 2},
        headers={"Idempotency-Key": "ORD-9"},
    )
    assert status == 200 and body["cleared"] == 0


def test_caller_key_reused_with_a_different_body_is_a_conflict(server):
    _ready_server(server)
    headers = {"Idempotency-Key": "ORD-42"}
    first, original = call(server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1},
                           headers=headers)
    assert first == 200
    status, body = call(server, "POST", "/api/belt/set", {"carousel": "T1-CR2", "batch": 2},
                        headers=headers)
    assert status == 409
    assert "different request body" in body["error"]
    # Same body still replays the original.
    status, replayed = call(server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1},
                            headers=headers)
    assert (status, replayed) == (200, original)
    # No row was registered for the conflicting attempt.
    assert server.plant.requests.replay_count() == 1


def test_request_id_body_field_works_like_the_header(server):
    _ready_server(server)
    status, first = call(
        server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1, "request_id": "O7"}
    )
    status, second = call(
        server, "POST", "/api/belt/set", {"carousel": "T1-CR1", "batch": 1, "request_id": "O7"}
    )
    assert first == second
    assert server.plant.requests.replay_count() == 1


def test_failures_are_not_remembered_and_retries_run_again(server):
    flight_id = _ready_server(server)
    bad = {"barcode": "", "flight_id": flight_id}
    assert call(server, "POST", "/api/bags/checkin", bad)[0] == 400
    assert call(server, "POST", "/api/bags/checkin", bad)[0] == 400
    from bagsort.idempotency.keys import fingerprint as fp

    failed_key = fp("POST", "/api/bags/checkin", bad, ())
    assert server.plant.requests.find(failed_key) is None


def test_delete_replay_returns_the_exact_first_response(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "7001", "flight_id": flight_id})
    first = server.call("DELETE", "/api/bags/T1-7001")
    second = server.call("DELETE", "/api/bags/T1-7001")
    assert first.status == second.status == 200
    assert first.body == second.body
    assert server.plant.requests.replay_count() == 1


def test_admin_and_store_writes_are_outside_the_replay_boundary(server):
    _ready_server(server)
    call(server, "POST", "/api/sort/verdict", {"epoch": 0, "sequence": 3})
    call(server, "POST", "/api/sort/verdict", {"epoch": 0, "sequence": 3})
    # verdict is a tracker update but a write endpoint; included, so replay it.
    assert server.plant.requests.replay_count() == 1

    call(server, "POST", "/api/admin/reset")
    call(server, "POST", "/api/admin/reset")
    assert len(server.plant.requests.records()) == 0

    status, policy = call(server, "GET", "/api/idempotency/policy")
    assert status == 200
    assert "/api/admin/" in policy["excluded_prefixes"]
    assert "POST" in policy["write_methods"]


def test_reads_are_never_idempotised(server):
    call(server, "GET", "/api/flights")
    call(server, "GET", "/api/health")
    call(server, "GET", "/api/audit/count")
    assert server.plant.requests.records() == []


def test_ledger_survives_snapshot_and_restart_and_still_replays(tmp_path):
    first_server = make_server(tmp_path)
    _ready_server(first_server)
    status, original = call(
        first_server, "POST", "/api/sort/dispatch", {"bag_id": "T1-3001"}
    )
    first_server.plant.store.persist()

    store = Store.open(first_server.plant.store.path)
    plant = Plant.build(store, terminal="T1", clock=Clock())
    restarted = Server(plant)

    status, again = call(restarted, "POST", "/api/sort/dispatch", {"bag_id": "T1-3001"})
    assert (status, again) == (201, original)
    assert len(plant.sorter.records()) == 1
    assert plant.verify_idempotency()["intact"] is True
    assert plant.audit.verify() is True


def test_concurrent_identical_writes_execute_once(tmp_path):
    import threading

    server = make_server(tmp_path)
    _ready_server(server)
    barrier = threading.Barrier(2)

    def worker(outcomes, index):
        barrier.wait()
        status, body = call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-3001"})
        outcomes[index] = (status, body)

    outcomes: dict = {}
    threads = [threading.Thread(target=worker, args=(outcomes, i)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert outcomes[0] == outcomes[1]
    assert len(server.plant.sorter.records()) == 1
    assert server.plant.requests.replay_count() == 1


def test_canonical_body_is_order_insensitive_but_value_sensitive():
    body_a = canonical_body({"b": 1, "a": 2}, ())
    body_b = canonical_body({"a": 2, "b": 1}, ())
    assert body_a == body_b
    assert canonical_body({"a": 2}, ()) != canonical_body({"a": 3}, ())
    # The control field does not participate in the fingerprint.
    assert canonical_body({"a": 1, "request_id": "X"}, ()) == canonical_body({"a": 1}, ())


def test_scope_key_distinguishes_endpoints_and_orders():
    one = scope_key("POST", "/api/a", "N1")
    two = scope_key("POST", "/api/b", "N1")
    assert one != two
    assert supplied_key("POST", "/api/a", "N1") == one
    assert supplied_key("POST", "/api/a", "  ") is None


def test_event_counts_agree_with_the_command_log(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})
    call(server, "POST", "/api/recheck", {"bag_id": "T1-3001"})

    report = server.plant.verify_idempotency()
    assert report["intact"] is True
    # One real bag.sorted event despite any replays, and its sort.record command.
    assert report["events"]["bag.sorted"] == 1
    assert report["events"]["bag.checked_in"] == 1
    assert report["events"]["chute.assigned"] >= 1
    assert report["events"]["bag.recheck_enqueued"] == 1


def test_crosscheck_flags_a_replay_marker_deleted_from_the_ledger(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})
    call(server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"})
    assert server.plant.verify_idempotency()["intact"] is True

    state = server.plant.store.state()
    row = next(row for row in state.requests if row.path == "/api/sort")
    stripped = type(row)(**{**row.__dict__, "replays": ()})
    state.requests = [
        stripped if existing.index == row.index else existing
        for existing in state.requests
    ]

    report = server.plant.verify_idempotency()
    assert report["intact"] is False
    assert any("replay totals disagree" in finding for finding in report["findings"])


def test_first_and_command_coordinates_match_the_journal(server):
    flight_id = _ready_server(server)
    status, original = call(
        server, "POST", "/api/sort", {"bag_id": "T1-3001", "chute_id": "T1-CHA"}
    )
    rows = [row for row in server.plant.requests.records() if row.path == "/api/sort"]
    row = rows[0]
    entries = server.plant.store.entries()
    window = [
        entry
        for entry in entries
        if row.first_sequence <= entry.sequence < row.first_sequence + row.command_count
    ]
    assert any(entry.kind == "sort.record" for entry in window)
    assert not any(entry.kind.startswith("request.") for entry in window)
    # The ledger command itself follows the domain window.
    assert entries[row.first_sequence + row.command_count - 1].kind != "request.register"


def test_bodyless_advancing_actions_execute_every_call_without_a_caller_key(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "8001", "flight_id": flight_id})
    call(server, "POST", "/api/bags/checkin", {"barcode": "8002", "flight_id": flight_id})
    call(server, "POST", "/api/recheck", {"bag_id": "T1-8001"})
    call(server, "POST", "/api/recheck", {"bag_id": "T1-8002"})

    first_status, first = call(server, "POST", "/api/recheck/next")
    second_status, second = call(server, "POST", "/api/recheck/next")
    assert (first_status, second_status) == (200, 200)
    # Two genuinely distinct actions must not collapse into one replay.
    assert first["id"] != second["id"]
    unkeyed = [
        row
        for row in server.plant.requests.records()
        if row.path == "/api/recheck/next"
    ]
    assert unkeyed == []


def test_bodyless_action_replays_with_an_explicit_caller_key(server):
    flight_id = _ready_server(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "8003", "flight_id": flight_id})
    call(server, "POST", "/api/recheck", {"bag_id": "T1-8003"})
    headers = {"Idempotency-Key": "NEXT-1"}
    first_status, first = call(server, "POST", "/api/recheck/next", {}, headers)
    second_status, second = call(server, "POST", "/api/recheck/next", {}, headers)
    assert (first_status, second_status) == (200, 200)
    assert first == second
    assert server.plant.requests.replay_count() == 1
    policy = call(server, "GET", "/api/idempotency/policy")[1]
    assert "/api/recheck/next" in policy["unkeyed_action_paths"]

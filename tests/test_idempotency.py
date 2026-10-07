"""Idempotent write requests: execute once, replay the stored response.

The console resends requests. The first arrival executes and registers its
response; a resend with the same key and fingerprint gets that response back
byte for byte, and the replay itself stays visible in the journal, the audit
chain and the event counters so all three can corroborate each other.
"""

from __future__ import annotations

import json

from bagsort.app import Plant
from bagsort.clock import Clock
from bagsort.console.server import Server
from bagsort.store.store import Store
from tests.support import make_server


def call(server, method: str, path: str, payload=None, query=None):
    response = server.call(method, path, payload, query)
    if response.content_type.startswith("application/json"):
        return response.status, json.loads(response.body)
    return response.status, response.body.decode("utf-8")


def raw(server, method: str, path: str, payload=None):
    return server.call(method, path, payload)


def bootstrap(server):
    status, _ = call(
        server, "POST", "/api/flights", {"code": "CA1234", "carousel": "CR1", "chute": "A"}
    )
    assert status == 201
    return "T1-CA1234"


def sorted_once(server, flight_id):
    call(server, "POST", "/api/bags/checkin", {"barcode": "7001", "flight_id": flight_id})
    return call(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})


def test_resend_replays_the_first_response_byte_for_byte(server):
    flight_id = bootstrap(server)
    status, record = sorted_once(server, flight_id)
    assert status == 201

    first = raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})
    second = raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})
    assert (first.status, second.status) == (201, 201)
    assert second.body == first.body
    assert json.loads(second.body) == record


def test_a_resend_executes_nothing_twice(server):
    flight_id = bootstrap(server)
    sorted_once(server, flight_id)
    raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})
    raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})

    plant = server.plant
    state = plant.store.state()
    assert len(state.sort_records) == 1
    assert state.counters["sorter.dispatched"] == 1
    # The event counters must not count a replay as a fresh sort.
    assert plant.telemetry.snapshot()["bag.sorted"] == 1
    assert plant.telemetry.snapshot()["request.replayed"] == 2


def test_register_journal_and_counters_corroborate(server):
    flight_id = bootstrap(server)
    sorted_once(server, flight_id)
    raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})

    status, consistency = call(server, "GET", "/api/requests/consistency")
    assert status == 200
    assert consistency["consistent"] is True
    assert consistency["aligned"] == {"registered": True, "replays": True, "sorted": True}
    assert consistency["telemetry"]["aligned"] == {"replays": True, "sorted": True}
    assert consistency["checks"]["registered"] == {"ledger": 3, "journal": 3}
    assert consistency["checks"]["replays"] == {"ledger": 1, "journal": 1, "audit": 1}
    assert consistency["checks"]["sorted"] == {"records": 1, "counter": 1}

    # The same counts are visible through the plain stats and audit views.
    assert call(server, "GET", "/api/stats")[1]["requests"] == 3
    assert call(server, "GET", "/api/stats")[1]["request_replays"] == 1
    assert call(server, "GET", "/api/audit/summary")[1]["request_replay"] == 1
    assert call(server, "GET", "/api/telemetry")[1]["topics"]["request.replayed"] == 1


def test_every_replay_is_reviewable_one_by_one(server):
    flight_id = bootstrap(server)
    sorted_once(server, flight_id)
    for _ in range(3):
        raw(server, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})

    status, replays = call(server, "GET", "/api/requests/replays")
    assert status == 200
    assert [entry["ordinal"] for entry in replays] == [1, 2, 3]
    assert {entry["path"] for entry in replays} == {"/api/sort"}

    status, journal = call(server, "GET", "/api/store/journal")
    replayed = [entry for entry in journal["entries"] if entry["kind"] == "request.replay"]
    assert len(replayed) == 3
    key = replayed[0]["payload"]["replay"]["key"]
    assert call(server, "GET", f"/api/requests/{key}")[1]["replays"] == 3

    # The audit chain holds one linked entry per replay and still verifies.
    subject = call(server, "GET", f"/api/audit/bag/{key}")[1]
    assert [entry["kind"] for entry in subject] == ["request_replay"] * 3
    assert call(server, "GET", "/api/audit/verify")[1]["intact"] is True


def test_caller_supplied_key_is_recognised_and_bound_to_its_parameters(server):
    flight_id = bootstrap(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "7002", "flight_id": flight_id})
    payload = {"bag_id": "T1-7002", "chute_id": "T1-CHA", "request_id": "SORT-42"}

    status, record = call(server, "POST", "/api/sort", payload)
    assert status == 201
    assert call(server, "POST", "/api/sort", payload) == (201, record)

    status, registered = call(server, "GET", "/api/requests/caller:SORT-42")
    assert status == 200
    assert registered["caller_key"] is True
    assert registered["replays"] == 1

    # The same 单号 carrying different business parameters is a conflict,
    # never a replay and never a second execution.
    reused = {"bag_id": "T1-7002", "chute_id": "T1-CHB", "request_id": "SORT-42"}
    assert call(server, "POST", "/api/sort", reused)[0] == 409
    assert len(server.plant.store.state().sort_records) == 1


def test_different_parameters_never_share_a_digest_key(server):
    flight_id = bootstrap(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "7003", "flight_id": flight_id})
    call(server, "POST", "/api/bags/checkin", {"barcode": "7004", "flight_id": flight_id})

    assert call(server, "POST", "/api/sort", {"bag_id": "T1-7003", "chute_id": "T1-CHA"})[0] == 201
    assert call(server, "POST", "/api/sort", {"bag_id": "T1-7004", "chute_id": "T1-CHA"})[0] == 201
    assert len(server.plant.store.state().sort_records) == 2

    status, registered = call(server, "GET", "/api/requests")
    assert status == 200
    sort_keys = [entry["key"] for entry in registered if entry["path"] == "/api/sort"]
    assert len(sort_keys) == len(set(sort_keys)) == 2


def test_failures_are_never_registered_or_replayed(server):
    flight_id = bootstrap(server)
    call(server, "POST", "/api/bags/checkin", {"barcode": "7005", "flight_id": flight_id})

    # An uncommitted read fails; the failure must not be cached.
    assert call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-7005", "committed": False})[0] == 409
    assert call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-7005", "committed": False})[0] == 409
    registered = call(server, "GET", "/api/requests")[1]
    assert {entry["path"] for entry in registered} == {"/api/flights", "/api/bags/checkin"}

    # Once it succeeds the success is what replays.
    status, record = call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-7005"})
    assert status == 201
    assert call(server, "POST", "/api/sort/dispatch", {"bag_id": "T1-7005"}) == (201, record)


def test_registration_survives_a_restart(tmp_path):
    path = tmp_path / "state.json"
    server = make_server(tmp_path)
    flight_id = bootstrap(server)
    status, record = sorted_once(server, flight_id)
    assert status == 201
    server.plant.store.persist()

    reopened = Server(Plant.build(Store.open(str(path)), clock=Clock()))
    status, again = call(reopened, "POST", "/api/sort", {"bag_id": "T1-7001", "chute_id": "T1-CHA"})
    assert (status, again) == (201, record)
    assert len(reopened.plant.store.state().sort_records) == 1

    status, consistency = call(reopened, "GET", "/api/requests/consistency")
    assert consistency["consistent"] is True
    assert consistency["checks"]["replays"]["ledger"] == 1


def test_reads_are_not_registered(server):
    assert call(server, "GET", "/api/flights")[0] == 200
    assert call(server, "GET", "/api/flights")[0] == 200
    assert call(server, "GET", "/api/requests")[1] == []


def test_policy_documents_the_replay_boundary(server):
    status, policy = call(server, "GET", "/api/requests/policy")
    assert status == 200
    assert policy["caller_key_field"] == "request_id"
    assert "request_id" in policy["key"]
    assert len(policy["boundary"]) >= 4
    assert any("409" in rule for rule in policy["boundary"])

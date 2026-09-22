import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = str(tmp_path / "test.db")
    import main as appmod
    appmod.DB_PATH = db
    appmod._store = None
    with TestClient(appmod.app) as c:
        yield c


def test_index_title(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "锁序解剖室" in r.text


def test_health(client):
    assert client.get("/api/health").json() == {"ok": True}


def test_demo_seeded_and_has_cycles(client):
    r = client.get("/api/state", params={"frame": -1})
    data = r.json()
    assert data["total_frames"] > 0
    sigs = {c["signature"] for c in data["cycles"]}
    assert any("0x7f02" in s and "0x7f01" in s for s in sigs)
    # two generations present, address reuse separated
    gens = {g["gen"] for g in data["generations"]}
    assert gens == {1, 2}


def test_ingest_and_stream_stats(client):
    events = [
        {"kind": "thread_start", "ts": 1, "thread": "X", "gen": 3},
        {"kind": "request", "ts": 2, "thread": "X", "lock": "Q", "gen": 3},
        {"kind": "grant", "ts": 3, "thread": "X", "lock": "Q", "gen": 3},
        {"kind": "release", "ts": 2.5, "thread": "X", "lock": "Q",
         "gen": 3},  # late
    ]
    r = client.post("/api/events", json={"events": events})
    assert r.status_code == 200
    stats = r.json()["stream"]
    assert stats["last_append"]["late"] == 1


def test_branch_and_compare(client):
    # find blocking request of T1 for 0x7f02
    tl = client.get("/api/timeline").json()["events"]
    req = next(e for e in tl if e["thread"] == "T1"
               and e["kind"] == "request" and e["lock"] == "0x7f02")
    r = client.post("/api/branches",
                    json={"id": "fix1", "parent": "base",
                          "description": "删除 T1 对 B 的请求"})
    assert r.status_code == 200
    r = client.post("/api/branches/fix1/corrections",
                    json={"op": "delete", "id": req["id"]})
    assert r.status_code == 200
    cmp_ = client.get("/api/compare",
                      params={"a": "base", "b": "fix1"}).json()
    assert cmp_["cycles_only_in_a"]
    assert not cmp_["cycles_only_in_b"]


def test_branch_declare_lock_type(client):
    r = client.post("/api/branches",
                    json={"id": "rw", "parent": "base"})
    assert r.status_code == 200
    r = client.post("/api/branches/rw/corrections",
                    json={"op": "declare", "gen": 1, "lock": "R1",
                          "type": "recursive"})
    assert r.status_code == 200
    info = client.get("/api/branches/rw").json()
    assert info["declarations"]["1:R1"] == "recursive"


def test_minimize_endpoint(client):
    state = client.get("/api/state", params={"frame": -1}).json()
    sig = next(c["signature"] for c in state["cycles"]
               if "0x7f01" in c["signature"])
    r = client.post("/api/minimize", json={"signature": sig})
    data = r.json()
    assert data["found"]
    assert data["minimal_size"] < data["original_size"]
    # minimal fragment still contains paired grants/requests for both threads
    threads = {e["thread"] for e in data["events"]}
    assert {"T1", "T2"} <= threads


def test_near_misses_and_anomalies(client):
    nm = client.get("/api/near-misses").json()["near_misses"]
    assert any(n["a"] == "0x7f0b" and n["b"] == "0x7f0c"
               and n["classification"] == "near_miss" for n in nm)
    anoms = client.get("/api/anomalies").json()["anomalies"]
    kinds = {a["kind"] for a in anoms}
    assert "thread_exited_while_holding" in kinds
    assert "grant_without_request" in kinds  # orphan grant 0x7f0d


def test_every_edge_links_back_to_events(client):
    state = client.get("/api/state", params={"frame": -1}).json()
    ids = {e["id"] for e in client.get("/api/timeline").json()["events"]}
    for gen in state["generations"]:
        for e in gen["hold_edges"]:
            assert e["grant_event"] in ids or e["confidence"] != "firm"
            assert e["provenance"]
        for e in gen["wait_edges"]:
            assert e["request_event"] in ids
    for c in state["cycles"]:
        for e in c["edges"]:
            assert e["wait_request_event"] in ids
            assert e["hold_grant_event"] in ids

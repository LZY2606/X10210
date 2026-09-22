from fastapi.testclient import TestClient

import app as app_module
from locklab.db import init_db


client = TestClient(app_module.app)
init_db(app_module.db)


def events(branch):
    return [
        {"id": f"{branch}-r1", "generation": "g", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "A", "lock": "X"},
        {"id": f"{branch}-g1", "generation": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "A", "lock": "X"},
        {"id": f"{branch}-r2", "generation": "g", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "B", "lock": "Y"},
        {"id": f"{branch}-g2", "generation": "g", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "B", "lock": "Y"},
        {"id": f"{branch}-r12", "generation": "g", "timestamp": 5, "seq": 4, "kind": "acquire_request", "thread": "A", "lock": "Y"},
        {"id": f"{branch}-r21", "generation": "g", "timestamp": 6, "seq": 5, "kind": "acquire_request", "thread": "B", "lock": "X"},
    ]


def test_page_title_and_health():
    page = client.get("/")
    assert page.status_code == 200
    assert "锁序解剖室" in page.text
    assert client.get("/api/health").json()["name"] == "锁序解剖室"


def test_api_correction_compare_reduce_and_incremental_counter():
    branch = "api-cycle-branch"
    client.post("/api/branches/main/clone", json={"new_branch_id": branch, "name": branch})
    first = client.post(f"/api/branches/{branch}/events", json={"events": events(branch)[:4]})
    assert first.status_code == 200
    streamed = client.post(f"/api/branches/{branch}/events", json={"events": events(branch)[4:]})
    assert streamed.json()["analysis"]["reused_events"] == 4
    analysis = client.get(f"/api/branches/{branch}/analysis").json()
    assert analysis["snapshot"]["cycles"]

    reduced = client.post(f"/api/branches/{branch}/reduce", json={})
    assert reduced.status_code == 200
    assert reduced.json()["event_count"] == 6

    corrected = "api-corrected-branch"
    response = client.post(
        f"/api/branches/{branch}/corrections",
        json={"event_id": f"{branch}-r12", "patch": {"timestamp": 99}, "new_branch_id": corrected, "name": corrected},
    )
    assert response.status_code == 200
    comparison = client.get(f"/api/branches/{branch}/compare/{corrected}").json()
    assert comparison["changed"][0]["id"] == f"{branch}-r12"
    corrected_analysis = client.get(f"/api/branches/{corrected}/analysis").json()
    assert corrected_analysis["snapshot"]["cycles"]


def test_lock_type_declaration_changes_rw_semantics():
    branch = "api-rw"
    client.post("/api/branches/main/clone", json={"new_branch_id": branch, "name": branch})
    payload = [
        {"id": "rw-r", "generation": "g", "timestamp": 1, "seq": 0, "kind": "acquire_request", "thread": "A", "lock": "RW", "mode": "shared"},
        {"id": "rw-g", "generation": "g", "timestamp": 2, "seq": 1, "kind": "acquire_success", "thread": "A", "lock": "RW", "mode": "shared"},
        {"id": "rw-r2", "generation": "g", "timestamp": 3, "seq": 2, "kind": "acquire_request", "thread": "B", "lock": "RW", "mode": "shared"},
        {"id": "rw-g2", "generation": "g", "timestamp": 4, "seq": 3, "kind": "acquire_success", "thread": "B", "lock": "RW", "mode": "shared"},
    ]
    client.post(f"/api/branches/{branch}/events", json={"events": payload})
    client.put(f"/api/branches/{branch}/lock-types", json={"generation": "g", "lock": "RW", "lock_type": "rwlock"})
    result = client.get(f"/api/branches/{branch}/analysis").json()
    assert len(result["snapshot"]["holds"]) == 2

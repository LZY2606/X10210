import os
import tempfile

import pytest
from fastapi.testclient import TestClient

os.environ["LOCKLAB_DB"] = os.path.join(tempfile.mkdtemp(), "test-locklab.db")

import app as app_module  # noqa: E402
from lab.model import normalize_event  # noqa: E402
from lab.timeline import Timeline  # noqa: E402


@pytest.fixture
def client():
    with TestClient(app_module.app) as c:
        yield c


@pytest.fixture
def make_timeline():
    def _make(items, types=None):
        events = [normalize_event(e, i + 1) for i, e in enumerate(items)]
        return Timeline(events, types or {})

    return _make


@pytest.fixture
def reset_db():
    def _reset():
        conn = app_module.get_conn()
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM declarations")
        conn.execute("DELETE FROM branches WHERE name != 'main'")
        conn.commit()
        conn.close()

    return _reset

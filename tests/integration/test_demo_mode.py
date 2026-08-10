"""The demo switch: running against the stub catalog instead of the real one.

Its whole reason to exist is that the real identifier is single-use (§ 2) — the
process can honestly be demonstrated exactly once. Two properties matter more
than the convenience: a deployment that did not ask for demo mode cannot be
talked into it, and a demo run can never be mistaken for a real one afterwards.
"""

import uuid

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import DownloadRun
from app.worker import tasks
from app.worker.client import NAMES_PATH

REAL_URL = "http://real-external.test"
STUB_URL = "http://stub-external.test"

pytestmark = pytest.mark.usefixtures("redis", "clean_db")


@pytest.fixture
def demo_settings(monkeypatch: pytest.MonkeyPatch):
    """Point the two catalogs at addresses a test can tell apart."""

    def apply(*, demo_mode: bool):
        settings = get_settings().model_copy(
            update={
                "demo_mode": demo_mode,
                "external_api_base_url": REAL_URL,
                "demo_api_base_url": STUB_URL,
                "network_max_attempts": 1,
                "network_backoff_base_s": 0.0,
                "external_min_interval_ms": 1,
            }
        )
        monkeypatch.setattr("app.config.get_settings", lambda: settings)
        monkeypatch.setattr("app.main.get_settings", lambda: settings)
        monkeypatch.setattr("app.api.runs.get_settings", lambda: settings)
        monkeypatch.setattr("app.worker.tasks.get_settings", lambda: settings)
        return settings

    return apply


async def fetch_run(sessionmaker: async_sessionmaker[AsyncSession], run_id: int) -> DownloadRun:
    async with sessionmaker() as session:
        run = await session.get(DownloadRun, run_id)
        assert run is not None
        return run


# --- what the UI is allowed to offer ----------------------------------------


@pytest.mark.parametrize("enabled", [True, False])
async def test_config_reports_the_switch(api, demo_settings, enabled: bool) -> None:
    demo_settings(demo_mode=enabled)

    body = (await api.get("/api/config")).json()

    assert body == {"demo_mode": enabled}


# --- starting a run ---------------------------------------------------------


async def test_a_demo_run_is_refused_when_the_mode_is_off(
    api, sessionmaker, demo_settings, published_tasks
) -> None:
    """Hiding the checkbox is convenience; this is the guard."""
    demo_settings(demo_mode=False)

    response = await api.post("/api/runs", json={"demo": True})

    assert response.status_code == 422
    assert "DEMO_MODE" in response.json()["detail"]
    assert published_tasks == [], "отклонённый ран не должен ставить задачу"


async def test_an_ordinary_run_works_with_the_mode_off(
    api, sessionmaker, demo_settings, published_tasks
) -> None:
    demo_settings(demo_mode=False)

    response = await api.post("/api/runs", json={"demo": False})

    assert response.status_code == 201
    assert response.json()["demo"] is False
    assert (await fetch_run(sessionmaker, response.json()["run_id"])).demo is False


async def test_a_request_without_a_body_is_an_ordinary_run(
    api, sessionmaker, demo_settings
) -> None:
    """The body is optional, and its absence must never mean "demo"."""
    demo_settings(demo_mode=True)

    response = await api.post("/api/runs")

    assert response.status_code == 201
    assert response.json()["demo"] is False


async def test_a_demo_run_is_recorded_and_announced(
    api, sessionmaker, demo_settings, published_tasks
) -> None:
    demo_settings(demo_mode=True)

    response = await api.post("/api/runs", json={"demo": True})

    assert response.status_code == 201
    body = response.json()
    assert body["demo"] is True
    assert (await fetch_run(sessionmaker, body["run_id"])).demo is True
    assert published_tasks == [body["run_id"]]
    # Written into the run's own log, so the mode survives in the record itself.
    assert any("демонстрационный режим" in event["message"] for event in body["events"])


# --- where the worker actually goes -----------------------------------------


async def run_task_against_both_catalogs(run_id: int) -> tuple[respx.Route, respx.Route]:
    """Execute the task with both catalogs mounted, and report who was called."""
    async with respx.mock(assert_all_called=False) as mock:
        real = mock.get(f"{REAL_URL}{NAMES_PATH}").mock(
            return_value=httpx.Response(200, json={"file_names": []})
        )
        stub = mock.get(f"{STUB_URL}{NAMES_PATH}").mock(
            return_value=httpx.Response(200, json={"file_names": []})
        )
        assert await tasks.execute_run(run_id) == "done"
    return real, stub


async def make_run(sessionmaker: async_sessionmaker[AsyncSession], *, demo: bool) -> int:
    async with sessionmaker() as session:
        run = DownloadRun(candidate_id=f"test-{uuid.uuid4()}", status="pending", demo=demo)
        session.add(run)
        await session.commit()
        return run.id


async def test_a_demo_run_talks_only_to_the_stub(sessionmaker, demo_settings) -> None:
    demo_settings(demo_mode=True)
    run_id = await make_run(sessionmaker, demo=True)

    real, stub = await run_task_against_both_catalogs(run_id)

    assert stub.call_count == 1
    assert real.call_count == 0, "демонстрационный ран ушёл в боевое API"


async def test_an_ordinary_run_talks_only_to_the_real_service(sessionmaker, demo_settings) -> None:
    demo_settings(demo_mode=True)
    run_id = await make_run(sessionmaker, demo=False)

    real, stub = await run_task_against_both_catalogs(run_id)

    assert real.call_count == 1
    assert stub.call_count == 0


async def test_the_catalog_is_decided_by_the_run_not_by_the_current_setting(
    sessionmaker, demo_settings
) -> None:
    """A redelivered task must not finish a demo run against the real service.

    The flag lives on the row, so turning the mode off between the start and the
    delivery changes nothing about where this run goes.
    """
    demo_settings(demo_mode=True)
    run_id = await make_run(sessionmaker, demo=True)
    demo_settings(demo_mode=False)

    real, stub = await run_task_against_both_catalogs(run_id)

    assert stub.call_count == 1
    assert real.call_count == 0

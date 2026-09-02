"""Fixtures that boot the supplied mock API; no stubbed transports anywhere in the suite."""

import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from pipeline.config import Settings

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "server.py"
STARTUP_TIMEOUT = 20.0


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _await_health(base_url: str, process: subprocess.Popen) -> None:
    """Poll /health until the server answers, failing fast if it died on startup."""
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"mock exited early: {process.communicate()[1]}")
        try:
            with urllib.request.urlopen(f"{base_url}/health", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.05)
    raise RuntimeError(f"mock did not become healthy within {STARTUP_TIMEOUT}s")


@dataclass
class MockServer:
    """A running mock instance pinned to one scenario."""

    base_url: str
    scenario: str

    def reset(self) -> None:
        """Re-arm source B's failure counters and clear source C's rate window."""
        request = urllib.request.Request(f"{self.base_url}/admin/reset", method="POST")
        with urllib.request.urlopen(request, timeout=5):
            pass

    def settings(self, **overrides: object) -> Settings:
        return Settings(base_url=self.base_url, **overrides)


def _start(scenario: str, env_extra: dict[str, str] | None = None) -> Iterator[MockServer]:
    """Launch one mock process. Scenario is process-level, so each needs its own server."""
    import os

    port = _free_port()
    env = {**os.environ, "MOCK_SCENARIO": scenario, **(env_extra or {})}
    # sys.executable, not bare python3: a stray .python-version must not break the suite.
    process = subprocess.Popen(
        [sys.executable, str(SERVER), "--host", "127.0.0.1", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        _await_health(base_url, process)
        yield MockServer(base_url=base_url, scenario=scenario)
    finally:
        process.terminate()
        process.wait(timeout=10)


@pytest.fixture(scope="session")
def standard_server() -> Iterator[MockServer]:
    yield from _start("standard")


@pytest.fixture(scope="session")
def no_failures_server() -> Iterator[MockServer]:
    yield from _start("no-failures")


@pytest.fixture(scope="session")
def source_b_down_server() -> Iterator[MockServer]:
    yield from _start("source-b-down")


@pytest.fixture(scope="session")
def bad_data_server() -> Iterator[MockServer]:
    yield from _start("bad-data-heavy")


@pytest.fixture
def mock(standard_server: MockServer) -> Iterator[MockServer]:
    """The standard scenario, reset before and after so tests never share upstream state."""
    standard_server.reset()
    yield standard_server
    standard_server.reset()


@pytest.fixture
def clean_mock(no_failures_server: MockServer) -> Iterator[MockServer]:
    no_failures_server.reset()
    yield no_failures_server


@pytest.fixture
def down_mock(source_b_down_server: MockServer) -> Iterator[MockServer]:
    source_b_down_server.reset()
    yield source_b_down_server


@pytest.fixture
def bad_data_mock(bad_data_server: MockServer) -> Iterator[MockServer]:
    bad_data_server.reset()
    yield bad_data_server


@pytest.fixture
async def client(mock: MockServer) -> Iterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as http_client:
        yield http_client

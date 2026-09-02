"""Retry, backoff, rate limiting and timeout behaviour, all against the real mock."""

import time

import httpx
import pytest

from pipeline.http import (
    RateLimiter,
    Requester,
    RetryBudget,
    RetryBudgetExhausted,
    RetryPolicy,
    parse_retry_after,
)
from pipeline.models import RunStatus, SourceStatus
from pipeline.runner import run
from pipeline.sources import SourceA, SourceB

FAST = RetryPolicy(attempts=4, base_delay=0.01, max_delay=0.05, max_retry_after=1.0)


def requester(client, name="endpoint_b", policy=FAST, budget=10, limiter=None):
    return Requester(client, name, policy, RetryBudget(budget), limiter)


async def test_transient_failures_recover_without_data_loss(mock):
    """cursor-2 fails once with 503 and cursor-3 twice with 502 before succeeding."""
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client)
        records = [r async for page in SourceB(req, 100).paginate() for r in page]

    assert [r["sku"] for r in records] == [f"b-20{n}" for n in range(1, 7)]
    assert req.stats.retries == 3
    assert req.stats.requests == 6


async def test_retries_are_exhausted_on_a_source_that_never_recovers(source_b_down_server):
    """source-b-down returns 503 forever, so the attempts must run out and raise."""
    async with httpx.AsyncClient(base_url=source_b_down_server.base_url, timeout=10.0) as client:
        req = requester(client, policy=FAST)
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            [r async for page in SourceB(req, 100).paginate() for r in page]

    assert excinfo.value.response.status_code == 503
    assert req.stats.requests == FAST.attempts
    assert req.stats.retries == FAST.attempts - 1


async def test_deterministic_client_error_is_not_retried(mock):
    """A 400 will never succeed on retry, so exactly one request must be sent.

    The mock has no 422, so its 400 stands in for the whole non-retryable 4xx class.
    """
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client, name="endpoint_a")
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await req.get_json("/source-a/products", {"page": "not-an-integer"})

    assert excinfo.value.response.status_code == 400
    assert req.stats.requests == 1
    assert req.stats.retries == 0


async def test_retry_after_header_is_honoured(mock):
    """Source C's 429 carries Retry-After: 1, which must be waited out."""
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client, name="endpoint_c", policy=RetryPolicy(attempts=3, base_delay=0.01))
        # Exhaust the window: the limit is 2 requests per rolling second.
        for _ in range(2):
            await req.get_json("/source-c/products", {"offset": 0, "limit": 2})

        started = time.monotonic()
        await req.get_json("/source-c/products", {"offset": 2, "limit": 2})
        elapsed = time.monotonic() - started

    assert req.stats.rate_limited == 1
    assert elapsed >= 0.9


async def test_server_error_retry_after_does_not_dictate_the_wait(mock):
    """Source B's 5xx sends Retry-After: 1, but its failures clear immediately.

    Obeying that hint literally cost 3 seconds per run for nothing. On a 5xx we take the
    shorter of the hint and our own backoff; on a 429 the hint still wins.
    """
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client, policy=FAST)
        started = time.monotonic()
        records = [r async for page in SourceB(req, 100).paginate() for r in page]
        elapsed = time.monotonic() - started

    assert len(records) == 6
    assert req.stats.retries == 3
    # Three retries would idle 3s if Retry-After were binding here.
    assert elapsed < 2.0


async def test_rate_limit_retry_after_is_still_binding(mock):
    """A 429 means we are going too fast, so its Retry-After must be obeyed in full."""
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client, name="endpoint_c", policy=FAST)
        for _ in range(2):
            await req.get_json("/source-c/products", {"offset": 0, "limit": 2})

        started = time.monotonic()
        await req.get_json("/source-c/products", {"offset": 2, "limit": 2})
        elapsed = time.monotonic() - started

    assert req.stats.rate_limited == 1
    # FAST's backoff caps at 50ms, so only the honoured header explains a full second.
    assert elapsed >= 0.9


async def test_retry_budget_caps_total_retries(source_b_down_server):
    """A budget smaller than the attempt count stops the source early."""
    async with httpx.AsyncClient(base_url=source_b_down_server.base_url, timeout=10.0) as client:
        req = requester(client, policy=RetryPolicy(attempts=10, base_delay=0.01), budget=2)
        with pytest.raises(RetryBudgetExhausted):
            [r async for page in SourceB(req, 100).paginate() for r in page]

    assert req.stats.retries == 2


async def test_rate_limiter_prevents_429s_entirely(clean_mock):
    """With proactive pacing, source C completes its three pages without one 429."""
    report = await run(clean_mock.settings())
    endpoint_c = report.sources["endpoint_c"]

    assert endpoint_c.status is SourceStatus.OK
    assert endpoint_c.records_normalized == 6
    assert endpoint_c.rate_limited == 0
    assert endpoint_c.requests == 3


async def test_run_deadline_cancels_slow_sources_but_still_reports(slow_server):
    """A run that blows its deadline reports partial data rather than nothing."""
    settings = slow_server.settings(run_timeout=0.5, retry_attempts=1)

    report = await run(settings)

    assert report.status in {RunStatus.FAILED, RunStatus.PARTIAL_SUCCESS}
    assert set(report.sources) == {"endpoint_a", "endpoint_b", "endpoint_c"}
    assert any("deadline" in (s.error or "") for s in report.sources.values())


async def test_standard_run_now_completes_every_source(mock):
    """The headline Phase 2 outcome: transient failures no longer cost records."""
    report = await run(mock.settings())

    assert report.product_count == 17
    assert report.sources["endpoint_b"].records_normalized == 5
    assert report.sources["endpoint_b"].retries == 3
    assert report.sources["endpoint_c"].rate_limited == 0


def test_parse_retry_after_accepts_delta_seconds():
    assert parse_retry_after("2", cap=10) == 2.0


def test_parse_retry_after_accepts_http_date():
    """The mock only sends delta-seconds, so the date form is unit tested here."""
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime

    when = datetime.now(UTC) + timedelta(seconds=5)
    parsed = parse_retry_after(format_datetime(when), cap=10)

    assert parsed is not None
    assert 3.0 <= parsed <= 6.0


def test_parse_retry_after_is_capped():
    """A hostile Retry-After must not be able to stall the run."""
    assert parse_retry_after("3600", cap=10) == 10.0


def test_parse_retry_after_ignores_nonsense():
    assert parse_retry_after("soon", cap=10) is None
    assert parse_retry_after(None, cap=10) is None


async def test_rate_limiter_paces_requests():
    limiter = RateLimiter(rate=2, per=0.5)
    started = time.monotonic()

    for _ in range(4):
        await limiter.acquire()

    assert time.monotonic() - started >= 0.5


async def test_source_a_needs_no_retries_when_upstream_is_healthy(mock):
    async with httpx.AsyncClient(base_url=mock.base_url, timeout=10.0) as client:
        req = requester(client, name="endpoint_a")
        records = [r async for page in SourceA(req, 100).paginate() for r in page]

    assert len(records) == 6
    assert req.stats.retries == 0

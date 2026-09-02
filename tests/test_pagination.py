"""Each adapter walks its own pagination style to exhaustion against the real mock."""

import httpx
import pytest

from pipeline.http import RateLimiter, Requester, RetryBudget, RetryPolicy
from pipeline.sources import PageCapExceeded, SourceA, SourceB, SourceC

MAX_PAGES = 100


def requester(client: httpx.AsyncClient, name: str, limiter: RateLimiter | None = None):
    return Requester(client, name, RetryPolicy(), RetryBudget(10), limiter)


async def drain(source):
    return [record async for page in source.paginate() for record in page]


async def test_source_a_walks_every_page(client: httpx.AsyncClient):
    records = await drain(SourceA(requester(client, "endpoint_a"), MAX_PAGES))

    assert [r["id"] for r in records] == [f"a-10{n}" for n in range(1, 7)]


async def test_source_c_walks_offsets_to_exhaustion(clean_mock):
    """The limiter keeps us inside source C's 2-per-second window without any 429."""
    async with httpx.AsyncClient(base_url=clean_mock.base_url, timeout=10.0) as client:
        req = requester(client, "endpoint_c", RateLimiter(rate=2, per=1.0))
        records = await drain(SourceC(req, MAX_PAGES, page_size=2))

    assert [r["product_id"] for r in records] == [f"c-30{n}" for n in range(1, 7)]
    assert req.stats.rate_limited == 0


async def test_source_b_follows_cursors_when_upstream_is_healthy(no_failures_server):
    async with httpx.AsyncClient(base_url=no_failures_server.base_url, timeout=10.0) as client:
        records = await drain(SourceB(requester(client, "endpoint_b"), MAX_PAGES))

    assert [r["sku"] for r in records] == [f"b-20{n}" for n in range(1, 7)]


async def test_page_cap_stops_a_runaway_source(no_failures_server):
    async with httpx.AsyncClient(base_url=no_failures_server.base_url, timeout=10.0) as client:
        with pytest.raises(PageCapExceeded):
            await drain(SourceB(requester(client, "endpoint_b"), max_pages=1))


async def test_source_c_adopts_advertised_max_page_size(clean_mock):
    """Asking for more than max_page_size would 400; the adapter must clamp instead."""
    async with httpx.AsyncClient(base_url=clean_mock.base_url, timeout=10.0) as client:
        source = SourceC(requester(client, "endpoint_c"), MAX_PAGES, page_size=2)
        first = await anext(source.paginate())

    assert len(first) == 2

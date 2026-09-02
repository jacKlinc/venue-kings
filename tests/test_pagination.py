"""Each adapter walks its own pagination style to exhaustion against the real mock."""

import httpx
import pytest

from pipeline.sources import PageCapExceeded, SourceA, SourceB, SourceC

MAX_PAGES = 100


async def drain(source):
    return [record async for page in source.paginate() for record in page]


async def test_source_a_walks_every_page(client: httpx.AsyncClient):
    records = await drain(SourceA(client, MAX_PAGES))

    assert [r["id"] for r in records] == [f"a-10{n}" for n in range(1, 7)]


async def test_source_c_walks_offsets_to_exhaustion(clean_mock):
    """Source C is rate limited, so pace requests to stay inside its 2-per-second window."""
    async with httpx.AsyncClient(base_url=clean_mock.base_url, timeout=10.0) as client:
        source = SourceC(client, MAX_PAGES, page_size=2)
        records = []
        async for page in source.paginate():
            records.extend(page)
            await _pause()

    assert [r["product_id"] for r in records] == [f"c-30{n}" for n in range(1, 7)]


async def test_source_b_follows_cursors_when_upstream_is_healthy(no_failures_server):
    async with httpx.AsyncClient(base_url=no_failures_server.base_url, timeout=10.0) as client:
        records = await drain(SourceB(client, MAX_PAGES))

    assert [r["sku"] for r in records] == [f"b-20{n}" for n in range(1, 7)]


async def test_page_cap_stops_a_runaway_source(no_failures_server):
    async with httpx.AsyncClient(base_url=no_failures_server.base_url, timeout=10.0) as client:
        with pytest.raises(PageCapExceeded):
            await drain(SourceB(client, max_pages=1))


async def test_source_c_adopts_advertised_max_page_size(clean_mock):
    """Asking for more than max_page_size would 400; the adapter must clamp instead."""
    async with httpx.AsyncClient(base_url=clean_mock.base_url, timeout=10.0) as client:
        source = SourceC(client, MAX_PAGES, page_size=2)
        first = await anext(source.paginate())

    assert len(first) == 2


async def _pause() -> None:
    import asyncio

    await asyncio.sleep(0.55)

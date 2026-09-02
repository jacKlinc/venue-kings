"""Source A: page-number pagination, bounded by total_pages."""

from collections.abc import AsyncIterator

import httpx

from ..models import SourceName
from .base import Page, PageCapExceeded, get_json, records_of

PATH = "/source-a/products"


class SourceA:
    """Walks pages 1..total_pages."""

    name: SourceName = "endpoint_a"

    def __init__(self, client: httpx.AsyncClient, max_pages: int) -> None:
        self._client = client
        self._max_pages = max_pages

    async def paginate(self) -> AsyncIterator[Page]:
        page = 1
        total = 1
        while page <= total:
            if page > self._max_pages:
                raise PageCapExceeded(f"{self.name} exceeded {self._max_pages} pages")
            payload = await get_json(self._client, PATH, {"page": page})
            total = self._total_pages(payload, fallback=total)
            yield records_of(payload, "products")
            page += 1

    @staticmethod
    def _total_pages(payload: object, fallback: int) -> int:
        """Trust total_pages when it is a sane integer, else stop after this page."""
        if isinstance(payload, dict):
            value = payload.get("total_pages")
            if isinstance(value, int) and value > 0:
                return value
        return fallback


__all__ = ["PATH", "SourceA"]

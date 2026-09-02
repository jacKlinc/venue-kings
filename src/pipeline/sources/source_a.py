"""Source A: page-number pagination, bounded by total_pages."""

from collections.abc import AsyncIterator

from ..http import Requester
from ..models import SourceName
from .base import Page, PageCapExceeded, records_of

PATH = "/source-a/products"


class SourceA:
    """Walks pages 1..total_pages."""

    name: SourceName = "endpoint_a"

    def __init__(self, requester: Requester, max_pages: int) -> None:
        self._requester = requester
        self._max_pages = max_pages

    @property
    def stats(self):
        return self._requester.stats

    async def paginate(self) -> AsyncIterator[Page]:
        page = 1
        total = 1
        while page <= total:
            if page > self._max_pages:
                raise PageCapExceeded(f"{self.name} exceeded {self._max_pages} pages")
            payload = await self._requester.get_json(PATH, {"page": page})
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

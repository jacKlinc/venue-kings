"""Source C: offset/limit pagination against a rate-limited upstream."""

from collections.abc import AsyncIterator

from ..http import Requester
from ..models import SourceName
from .base import Page, PageCapExceeded, records_of

PATH = "/source-c/products"


class SourceC:
    """Walks next_offset, clamping page size to the limit the upstream advertises.

    Its rate limit is enforced by the Requester's limiter, not here.
    """

    name: SourceName = "endpoint_c"

    def __init__(self, requester: Requester, max_pages: int, page_size: int) -> None:
        self._requester = requester
        self._max_pages = max_pages
        self._page_size = page_size

    @property
    def stats(self):
        return self._requester.stats

    async def paginate(self) -> AsyncIterator[Page]:
        offset: int | None = 0
        pages = 0

        while offset is not None:
            pages += 1
            if pages > self._max_pages:
                raise PageCapExceeded(f"{self.name} exceeded {self._max_pages} pages")
            params = {"offset": offset, "limit": self._page_size}
            payload = await self._requester.get_json(PATH, params)
            self._adopt_page_size(payload)
            yield records_of(payload, "data")
            offset = self._next_offset(payload, current=offset)

    def _adopt_page_size(self, payload: object) -> None:
        """Respect max_page_size so we never request more than the upstream allows."""
        if not isinstance(payload, dict):
            return
        advertised = payload.get("max_page_size")
        if isinstance(advertised, int) and 0 < advertised < self._page_size:
            self._page_size = advertised

    @staticmethod
    def _next_offset(payload: object, current: int) -> int | None:
        """Require forward progress; anything else ends pagination rather than looping."""
        if not isinstance(payload, dict):
            return None
        value = payload.get("next_offset")
        if isinstance(value, int) and value > current:
            return value
        return None


__all__ = ["PATH", "SourceC"]

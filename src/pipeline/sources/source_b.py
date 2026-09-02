"""Source B: opaque cursor pagination."""

from collections.abc import AsyncIterator

from ..http import Requester
from ..models import SourceName
from .base import Page, PageCapExceeded, records_of

PATH = "/source-b/products"


class SourceB:
    """Follows next_cursor until it is null."""

    name: SourceName = "endpoint_b"

    def __init__(self, requester: Requester, max_pages: int) -> None:
        self._requester = requester
        self._max_pages = max_pages

    @property
    def stats(self):
        return self._requester.stats

    async def paginate(self) -> AsyncIterator[Page]:
        cursor: str | None = None
        seen: set[str] = set()
        pages = 0

        while True:
            pages += 1
            if pages > self._max_pages:
                raise PageCapExceeded(f"{self.name} exceeded {self._max_pages} pages")
            payload = await self._requester.get_json(PATH, self._params(cursor))
            yield records_of(payload, "items")

            cursor = self._next_cursor(payload)
            if cursor is None:
                return
            if cursor in seen:
                raise PageCapExceeded(f"{self.name} repeated cursor {cursor!r}")
            seen.add(cursor)

    @staticmethod
    def _params(cursor: str | None) -> dict[str, object]:
        return {} if cursor is None else {"cursor": cursor}

    @staticmethod
    def _next_cursor(payload: object) -> str | None:
        if not isinstance(payload, dict):
            return None
        value = payload.get("next_cursor")
        return value if isinstance(value, str) and value else None


__all__ = ["PATH", "SourceB"]

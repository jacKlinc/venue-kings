"""The interface every source adapter presents to the runner."""

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, runtime_checkable

import httpx

from ..models import SourceName

Page = list[Mapping[str, object]]


@runtime_checkable
class Source(Protocol):
    """A paginated upstream. Adapters own their pagination style; the runner stays generic."""

    name: SourceName

    def paginate(self) -> AsyncIterator[Page]:
        """Yield raw record pages until the upstream is exhausted."""
        ...


class PageCapExceeded(RuntimeError):
    """Raised when a source yields more pages than configured, implying a pagination bug."""


def records_of(payload: object, key: str) -> Page:
    """Pull the record list out of a response body, tolerating a missing or null field."""
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object, got {type(payload).__name__}")
    items = payload.get(key) or []
    if not isinstance(items, list):
        raise ValueError(f"expected '{key}' to be a list, got {type(items).__name__}")
    return [x for x in items if isinstance(x, Mapping)]


async def get_json(client: httpx.AsyncClient, url: str, params: Mapping[str, object]) -> object:
    """GET and decode JSON, raising for non-2xx."""
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()


__all__ = ["Page", "PageCapExceeded", "Source", "get_json", "records_of"]

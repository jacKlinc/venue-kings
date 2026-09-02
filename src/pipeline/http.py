"""Retry, backoff and rate limiting for upstream requests."""

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from .observability import get_logger

log = get_logger(__name__)

# Transient: the same request may well succeed later.
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class RetryPolicy:
    """How hard to try before giving up on a request."""

    attempts: int = 4
    base_delay: float = 0.25
    max_delay: float = 8.0
    # A hostile or careless upstream must not be able to stall the run.
    max_retry_after: float = 10.0


class RetryBudgetExhausted(RuntimeError):
    """Raised when a source has spent its whole retry allowance."""


class RetryBudget:
    """A per-source ceiling on retries, so one broken upstream cannot monopolise the run."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self.spent = 0

    def consume(self) -> None:
        if self.spent >= self._limit:
            raise RetryBudgetExhausted(f"retry budget of {self._limit} exhausted")
        self.spent += 1


class RateLimiter:
    """Paces requests to at most `rate` per `per` seconds.

    Source C documents its limit, so we stay inside it rather than provoking 429s and
    reacting to them. The reactive path still exists as a safety net.
    """

    def __init__(self, rate: int, per: float = 1.0) -> None:
        self._rate = rate
        self._per = per
        self._issued: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._issued = [t for t in self._issued if now - t < self._per]
                if len(self._issued) < self._rate:
                    self._issued.append(now)
                    return
                # Wait until the oldest request leaves the window.
                await asyncio.sleep(self._per - (now - self._issued[0]) + 0.01)


def parse_retry_after(value: str | None, cap: float) -> float | None:
    """Read a Retry-After header in either delta-seconds or HTTP-date form."""
    if not value:
        return None
    try:
        return min(max(float(value), 0.0), cap)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    now = datetime.now(UTC) if when.tzinfo else datetime.now()
    return min(max((when - now).total_seconds(), 0.0), cap)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUSES
    return isinstance(exc, httpx.TimeoutException | httpx.TransportError)


def _backoff(attempt: int, policy: RetryPolicy) -> float:
    """Exponential backoff with full jitter, to avoid retrying in lockstep."""
    ceiling = min(policy.max_delay, policy.base_delay * 2**attempt)
    return random.uniform(0, ceiling)


def _delay_for(exc: BaseException, attempt: int, policy: RetryPolicy) -> float:
    """Wait as long as the upstream asks when throttled, else back off.

    Retry-After is binding on a 429: the server is telling us we are going too fast, and
    ignoring it would just earn another 429. On a 5xx it is only a hint about a fault the
    server cannot time, and obeying it literally can idle far longer than the outage
    lasts, so we take the shorter of the hint and our own backoff.
    """
    if not isinstance(exc, httpx.HTTPStatusError):
        return _backoff(attempt, policy)

    hinted = parse_retry_after(exc.response.headers.get("Retry-After"), policy.max_retry_after)
    if hinted is None:
        return _backoff(attempt, policy)
    if exc.response.status_code == 429:
        return hinted
    return min(hinted, _backoff(attempt, policy))


@dataclass
class RequestStats:
    """Per-source request counters, surfaced in the run report."""

    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    # Per-request latency in ms, excluding any rate-limiter wait.
    latencies_ms: list[float] = field(default_factory=list)


class Requester:
    """Executes GETs for one source, applying rate limiting, retries and backoff."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        source: str,
        policy: RetryPolicy,
        budget: RetryBudget,
        limiter: RateLimiter | None = None,
    ) -> None:
        self._client = client
        self._source = source
        self._policy = policy
        self._budget = budget
        self._limiter = limiter
        self.stats = RequestStats()

    async def get_json(self, url: str, params: dict[str, object]) -> object:
        """GET and decode JSON, retrying transient failures until the policy is spent."""
        for attempt in range(self._policy.attempts):
            try:
                return await self._attempt(url, params)
            except Exception as exc:
                last = attempt == self._policy.attempts - 1
                if last or not _is_retryable(exc):
                    raise
                await self._pause(exc, attempt, url)
        raise AssertionError("unreachable")

    async def _attempt(self, url: str, params: dict[str, object]) -> object:
        if self._limiter is not None:
            await self._limiter.acquire()
        self.stats.requests += 1
        started = time.perf_counter()
        try:
            response = await self._client.get(url, params=params)
        finally:
            self.stats.latencies_ms.append((time.perf_counter() - started) * 1000)
        if response.status_code == 429:
            self.stats.rate_limited += 1
        response.raise_for_status()
        return response.json()

    async def _pause(self, exc: BaseException, attempt: int, url: str) -> None:
        """Spend a retry from the budget and wait before the next attempt."""
        self._budget.consume()
        self.stats.retries += 1
        delay = _delay_for(exc, attempt, self._policy)
        log.warning(
            "request.retrying",
            source=self._source,
            path=url,
            attempt=attempt + 1,
            delay_s=round(delay, 3),
            reason=_reason(exc),
        )
        await asyncio.sleep(delay)


def _reason(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return type(exc).__name__


__all__ = [
    "RETRYABLE_STATUSES",
    "RateLimiter",
    "RequestStats",
    "Requester",
    "RetryBudget",
    "RetryBudgetExhausted",
    "RetryPolicy",
    "parse_retry_after",
]

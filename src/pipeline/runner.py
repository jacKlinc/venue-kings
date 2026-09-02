"""Orchestrates the sources, isolating failures and assembling the run report."""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .config import Settings
from .dedupe import SOURCE_PRECEDENCE, deduplicate
from .http import RateLimiter, Requester, RetryBudget, RetryPolicy
from .metrics import build_metrics, summarize_latency
from .models import (
    NormalizedProduct,
    RunReport,
    RunStatus,
    RunWarning,
    SourceReport,
    SourceStatus,
    WarningCode,
)
from .normalize import normalize_records
from .observability import get_logger
from .sources import Source, SourceA, SourceB, SourceC

log = get_logger(__name__)


@dataclass
class SourceOutcome:
    """What one source produced, successfully or not."""

    report: SourceReport
    products: list[NormalizedProduct] = field(default_factory=list)
    warnings: list[RunWarning] = field(default_factory=list)


def build_sources(client: httpx.AsyncClient, settings: Settings) -> list[Source]:
    """Construct every adapter, each with its own retry budget and request counters."""
    cap = settings.max_pages_per_source
    policy = RetryPolicy(
        attempts=settings.retry_attempts,
        base_delay=settings.retry_base_delay,
        max_delay=settings.retry_max_delay,
        max_retry_after=settings.max_retry_after,
    )

    def requester(name: str, limiter: RateLimiter | None = None) -> Requester:
        budget = RetryBudget(settings.retry_budget_per_source)
        return Requester(client, name, policy, budget, limiter)

    # Only source C documents a rate limit, so only it is paced.
    limiter = RateLimiter(settings.source_c_rate, settings.source_c_window)
    return [
        SourceA(requester("endpoint_a"), cap),
        SourceB(requester("endpoint_b"), cap),
        SourceC(requester("endpoint_c", limiter), cap, settings.source_c_page_size),
    ]


def _describe(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code} from {exc.request.url.path}"
    return f"{type(exc).__name__}: {exc}"


async def collect_source(source: Source, settings: Settings) -> SourceOutcome:
    """Drain one source, converting any failure into a reported outcome rather than a raise."""
    bound = log.bind(source=source.name)
    report = SourceReport(name=source.name, status=SourceStatus.OK)
    outcome = SourceOutcome(report=report)
    started = time.perf_counter()

    try:
        async for page in source.paginate():
            _absorb_page(source, page, settings, outcome, bound)
    except asyncio.CancelledError:
        # The run deadline expired. Report what we have rather than losing the source.
        _mark_failed(outcome, TimeoutError("cancelled by run deadline"), bound)
    except Exception as exc:
        _mark_failed(outcome, exc, bound)

    _record_stats(source, report)
    report.duration_ms = int((time.perf_counter() - started) * 1000)
    _finalize_status(outcome)
    bound.info(
        "source.finished",
        status=report.status,
        products=report.records_normalized,
        dropped=report.records_dropped,
        pages=report.pages_fetched,
        requests=report.requests,
        retries=report.retries,
        duration_ms=report.duration_ms,
    )
    return outcome


def _record_stats(source: Source, report: SourceReport) -> None:
    """Copy the source's request counters and latency spread into its report."""
    report.requests = source.stats.requests
    report.retries = source.stats.retries
    report.rate_limited = source.stats.rate_limited
    p50, p95, peak = summarize_latency(source.stats.latencies_ms)
    report.latency_p50_ms = p50
    report.latency_p95_ms = p95
    report.latency_max_ms = peak


def _absorb_page(
    source: Source,
    page: list,
    settings: Settings,
    outcome: SourceOutcome,
    bound,
) -> None:
    """Normalize one page into the outcome, logging any dropped records."""
    report = outcome.report
    report.pages_fetched += 1
    report.records_received += len(page)

    products, warnings = normalize_records(source.name, page, settings.currency)
    outcome.products.extend(products)
    outcome.warnings.extend(warnings)
    report.records_normalized += len(products)
    report.records_dropped += len(warnings)

    for warning in warnings:
        bound.warning("record.dropped", record_id=warning.record_id, detail=warning.detail)
    bound.debug("page.fetched", page=report.pages_fetched, records=len(page))


def _mark_failed(outcome: SourceOutcome, exc: BaseException, bound) -> None:
    report = outcome.report
    report.error = _describe(exc)
    outcome.warnings.append(
        RunWarning(code=WarningCode.SOURCE_FAILED, source=report.name, detail=report.error)
    )
    bound.warning("source.failed", error=report.error, pages=report.pages_fetched)


def _finalize_status(outcome: SourceOutcome) -> None:
    """A source that failed mid-pagination is degraded if it still yielded records."""
    report = outcome.report
    if report.error:
        report.status = SourceStatus.DEGRADED if outcome.products else SourceStatus.FAILED
    elif report.records_dropped:
        report.status = SourceStatus.DEGRADED


def _run_status(outcomes: list[SourceOutcome], warnings: list[RunWarning]) -> RunStatus:
    if not any(o.products for o in outcomes):
        return RunStatus.FAILED
    clean = all(o.report.status is SourceStatus.OK for o in outcomes) and not warnings
    return RunStatus.SUCCESS if clean else RunStatus.PARTIAL_SUCCESS


async def _gather_within_deadline(sources: list[Source], settings: Settings) -> list[SourceOutcome]:
    """Run every source concurrently under one wall-clock deadline.

    Sources run as tasks so that a deadline cancellation still yields each one's
    partial outcome; collect_source turns its own cancellation into a failed report.
    """
    tasks = [asyncio.create_task(collect_source(s, settings)) for s in sources]
    try:
        async with asyncio.timeout(settings.run_timeout):
            return await asyncio.gather(*tasks)
    except TimeoutError:
        log.warning("run.deadline_exceeded", timeout_s=settings.run_timeout)

    return [await _settled(task, source) for task, source in zip(tasks, sources, strict=True)]


async def _settled(task: asyncio.Task, source: Source) -> SourceOutcome:
    """Recover a task's outcome after the deadline, synthesising one if it never finished."""
    try:
        return await task
    except BaseException:
        report = SourceReport(
            name=source.name,
            status=SourceStatus.FAILED,
            error=f"cancelled after {source.stats.requests} request(s) by run deadline",
        )
        _record_stats(source, report)
        return SourceOutcome(
            report=report,
            warnings=[
                RunWarning(
                    code=WarningCode.SOURCE_FAILED,
                    source=source.name,
                    detail=report.error or "cancelled by run deadline",
                )
            ],
        )


def build_client(settings: Settings) -> httpx.AsyncClient:
    """One pooled client shared by every source, so connections are reused not rebuilt."""
    timeout = httpx.Timeout(
        connect=settings.connect_timeout,
        read=settings.read_timeout,
        write=settings.read_timeout,
        pool=settings.connect_timeout,
    )
    limits = httpx.Limits(
        max_connections=settings.max_connections,
        max_keepalive_connections=settings.max_connections,
        keepalive_expiry=settings.run_timeout,
    )
    return httpx.AsyncClient(base_url=settings.base_url, timeout=timeout, limits=limits)


async def run(settings: Settings) -> RunReport:
    """Fetch every source concurrently and assemble a single report."""
    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    log.info("run.started", run_id=run_id, base_url=settings.base_url)

    async with build_client(settings) as client:
        sources = build_sources(client, settings)
        outcomes = await _gather_within_deadline(sources, settings)

    products, warnings = _merge(outcomes)
    finished_at = datetime.now(UTC)
    duration_ms = int((time.perf_counter() - started) * 1000)
    reports = [o.report for o in outcomes]
    metrics = build_metrics(reports, duration_ms)
    report = RunReport(
        run_id=run_id,
        status=_run_status(list(outcomes), warnings),
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        product_count=len(products),
        metrics=metrics,
        products=products,
        sources={r.name: r for r in reports},
        warnings=warnings,
    )
    log.info(
        "run.finished",
        run_id=run_id,
        status=report.status,
        products=report.product_count,
        warnings=len(warnings),
        duration_ms=duration_ms,
        requests=metrics.requests,
        retries=metrics.retries,
        saved_ms=metrics.concurrency_saving_ms,
    )
    return report


def _merge(outcomes: list[SourceOutcome]) -> tuple[list[NormalizedProduct], list[RunWarning]]:
    """Deduplicate across sources, keeping warnings from every stage."""
    collected: list[NormalizedProduct] = []
    warnings: list[RunWarning] = []
    for outcome in outcomes:
        collected.extend(outcome.products)
        warnings.extend(outcome.warnings)

    # Sources finish in nondeterministic order; sorting first makes first-wins stable.
    collected.sort(key=lambda p: SOURCE_PRECEDENCE.index(p.source))
    products, duplicate_warnings = deduplicate(collected)
    for warning in duplicate_warnings:
        log.warning("record.duplicate", record_id=warning.record_id, detail=warning.detail)

    products.sort(key=lambda p: p.id)
    return products, warnings + duplicate_warnings


__all__ = ["SourceOutcome", "build_client", "build_sources", "collect_source", "run"]

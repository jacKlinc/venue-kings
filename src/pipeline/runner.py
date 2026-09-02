"""Orchestrates the sources, isolating failures and assembling the run report."""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .config import Settings
from .dedupe import SOURCE_PRECEDENCE, deduplicate
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
    """Construct every adapter against a shared client."""
    cap = settings.max_pages_per_source
    return [
        SourceA(client, cap),
        SourceB(client, cap),
        SourceC(client, cap, settings.source_c_page_size),
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
    except Exception as exc:
        _mark_failed(outcome, exc, bound)

    report.duration_ms = int((time.perf_counter() - started) * 1000)
    _finalize_status(outcome)
    bound.info(
        "source.finished",
        status=report.status,
        products=report.records_normalized,
        dropped=report.records_dropped,
        pages=report.pages_fetched,
        duration_ms=report.duration_ms,
    )
    return outcome


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


def build_client(settings: Settings) -> httpx.AsyncClient:
    """One pooled client shared by every source."""
    timeout = httpx.Timeout(
        connect=settings.connect_timeout,
        read=settings.read_timeout,
        write=settings.read_timeout,
        pool=settings.connect_timeout,
    )
    return httpx.AsyncClient(base_url=settings.base_url, timeout=timeout)


async def run(settings: Settings) -> RunReport:
    """Fetch every source concurrently and assemble a single report."""
    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    log.info("run.started", run_id=run_id, base_url=settings.base_url)

    async with build_client(settings) as client:
        sources = build_sources(client, settings)
        outcomes = await asyncio.gather(*(collect_source(s, settings) for s in sources))

    products, warnings = _merge(outcomes)
    finished_at = datetime.now(UTC)
    report = RunReport(
        run_id=run_id,
        status=_run_status(list(outcomes), warnings),
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=int((time.perf_counter() - started) * 1000),
        product_count=len(products),
        products=products,
        sources={o.report.name: o.report for o in outcomes},
        warnings=warnings,
    )
    log.info(
        "run.finished",
        run_id=run_id,
        status=report.status,
        products=report.product_count,
        warnings=len(warnings),
        duration_ms=report.duration_ms,
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

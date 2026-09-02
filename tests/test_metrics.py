"""Metric aggregation and the performance properties it is meant to evidence."""

from pipeline.metrics import build_metrics, summarize_latency
from pipeline.models import SourceReport, SourceStatus
from pipeline.runner import run


def report(name, **kwargs):
    return SourceReport(name=name, status=SourceStatus.OK, **kwargs)


def test_summarize_latency_orders_samples():
    p50, p95, peak = summarize_latency([100.0, 10.0, 50.0, 500.0])

    assert p50 == 75
    assert peak == 500
    assert p50 <= p95 <= peak


def test_summarize_latency_handles_no_samples():
    assert summarize_latency([]) == (0, 0, 0)


def test_metrics_sum_across_sources():
    metrics = build_metrics(
        [
            report("a", requests=3, retries=0, records_normalized=6, duration_ms=250),
            report("b", requests=6, retries=3, records_dropped=1, duration_ms=1300),
        ],
        wall_ms=1400,
    )

    assert metrics.requests == 9
    assert metrics.retries == 3
    assert metrics.records_dropped == 1
    assert metrics.sequential_ms == 1550


def test_concurrency_saving_is_sequential_minus_wall():
    metrics = build_metrics(
        [report("a", duration_ms=1000), report("b", duration_ms=1000)],
        wall_ms=1100,
    )

    assert metrics.concurrency_saving_ms == 900


def test_concurrency_saving_never_goes_negative():
    """Wall time can exceed the sum when overheads dominate; that is not a negative saving."""
    metrics = build_metrics([report("a", duration_ms=10)], wall_ms=50)

    assert metrics.concurrency_saving_ms == 0


async def test_run_reports_metrics_consistent_with_source_reports(clean_mock):
    report_ = await run(clean_mock.settings())
    sources = report_.sources.values()

    assert report_.metrics.requests == sum(s.requests for s in sources)
    assert report_.metrics.records_normalized == sum(s.records_normalized for s in sources)
    assert report_.metrics.wall_ms == report_.duration_ms


async def test_sources_are_fetched_concurrently(clean_mock):
    """Wall time must beat running the sources one after another.

    Source C is rate limited to 2 requests/second and needs three pages, so it alone
    takes about a second; if the run were sequential the others would stack on top.
    """
    report_ = await run(clean_mock.settings())

    assert report_.metrics.concurrency_saving_ms > 0
    assert report_.duration_ms < report_.metrics.sequential_ms


async def test_latency_percentiles_are_recorded(clean_mock):
    report_ = await run(clean_mock.settings())
    endpoint_a = report_.sources["endpoint_a"]

    assert endpoint_a.latency_p50_ms > 0
    assert endpoint_a.latency_p50_ms <= endpoint_a.latency_max_ms


async def test_retry_sleep_is_excluded_from_request_latency(mock):
    """Latency measures the request, not the backoff we chose to wait."""
    report_ = await run(mock.settings())
    endpoint_b = report_.sources["endpoint_b"]

    assert endpoint_b.retries == 3
    assert endpoint_b.latency_max_ms < endpoint_b.duration_ms

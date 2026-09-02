"""Run-level metric aggregation."""

from statistics import median

from .models import RunMetrics, SourceReport


def _percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile. Exact enough for the handful of samples per run."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(fraction * len(ordered)))
    return ordered[index]


def summarize_latency(samples: list[float]) -> tuple[int, int, int]:
    """Return (p50, p95, max) request latency in milliseconds."""
    if not samples:
        return 0, 0, 0
    return (
        int(median(samples)),
        int(_percentile(samples, 0.95)),
        int(max(samples)),
    )


def build_metrics(reports: list[SourceReport], wall_ms: int) -> RunMetrics:
    """Aggregate per-source counters into one run-level view.

    `concurrency_saving_ms` contrasts wall time against the summed per-source durations:
    it is what fetching the sources concurrently bought over doing them one by one.
    """
    sequential_ms = sum(r.duration_ms for r in reports)
    return RunMetrics(
        wall_ms=wall_ms,
        sequential_ms=sequential_ms,
        concurrency_saving_ms=max(0, sequential_ms - wall_ms),
        requests=sum(r.requests for r in reports),
        retries=sum(r.retries for r in reports),
        rate_limited=sum(r.rate_limited for r in reports),
        records_received=sum(r.records_received for r in reports),
        records_normalized=sum(r.records_normalized for r in reports),
        records_dropped=sum(r.records_dropped for r in reports),
    )


__all__ = ["build_metrics", "summarize_latency"]

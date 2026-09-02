"""End-to-end runs against the real mock, focused on partial-failure isolation.

Phase 1 has no retry layer, so the standard scenario's transient 503/502 and source C's
429 are expected to degrade those sources. Phase 2 changes these expectations.
"""

from decimal import Decimal

from pipeline.models import RunStatus, SourceStatus
from pipeline.runner import run


async def test_one_source_failing_does_not_lose_the_others(mock):
    """The headline requirement: a failing source is recorded, never fatal."""
    report = await run(mock.settings())

    assert report.status is RunStatus.PARTIAL_SUCCESS
    assert report.sources["endpoint_a"].status is SourceStatus.OK
    assert report.sources["endpoint_a"].records_normalized == 6
    assert report.product_count > 6


async def test_source_b_down_is_isolated(down_mock):
    """Source B never recovers in this scenario; A and C must still deliver."""
    report = await run(down_mock.settings())

    assert report.status is RunStatus.PARTIAL_SUCCESS
    assert report.sources["endpoint_b"].status is SourceStatus.FAILED
    assert report.sources["endpoint_b"].records_normalized == 0
    assert report.sources["endpoint_a"].records_normalized == 6

    failures = [w for w in report.warnings if w.source == "endpoint_b"]
    assert failures and "503" in failures[0].detail


async def test_failed_source_is_both_logged_and_recorded(down_mock):
    report = await run(down_mock.settings())

    codes = {w.code for w in report.warnings}
    assert "source_failed" in codes
    assert report.sources["endpoint_b"].error


async def test_malformed_record_is_dropped_but_page_mates_survive(bad_data_mock):
    """bad-data-heavy corrupts the first record of every source B page."""
    report = await run(bad_data_mock.settings())

    endpoint_b = report.sources["endpoint_b"]
    assert endpoint_b.records_dropped > 0
    assert endpoint_b.records_normalized > 0
    assert endpoint_b.status is SourceStatus.DEGRADED

    dropped = [w for w in report.warnings if w.code == "malformed_record"]
    assert dropped


async def test_clean_scenario_still_reports_the_fixture_bad_record(clean_mock):
    """b-205's broken price lives in fixtures.json, so no scenario yields a clean run.

    Source C is rate limited in every scenario, so without the Phase 2 limiter it loses
    its last page here. Phase 2 raises the expected count from 15 to 17.
    """
    report = await run(clean_mock.settings())

    assert report.status is RunStatus.PARTIAL_SUCCESS
    dropped = [w for w in report.warnings if w.record_id == "b-205"]
    assert len(dropped) == 1

    assert report.sources["endpoint_b"].records_normalized == 5
    assert report.sources["endpoint_a"].records_normalized == 6
    assert report.product_count == 15


async def test_report_is_json_serialisable_with_exact_prices(clean_mock):
    """Decimal prices must serialise as exact strings, not floats."""
    report = await run(clean_mock.settings())
    payload = report.model_dump_json()

    # b-201 is 3499 cents on source B's first page, so it survives every scenario.
    assert '"34.99"' in payload
    assert report.run_id in payload


async def test_products_are_sorted_and_namespaced(clean_mock):
    report = await run(clean_mock.settings())
    ids = [p.id for p in report.products]

    assert ids == sorted(ids)
    assert all(p.id.startswith(f"{p.source}:") for p in report.products)


async def test_prices_are_positive_decimals(clean_mock):
    report = await run(clean_mock.settings())

    assert all(isinstance(p.price, Decimal) and p.price > 0 for p in report.products)

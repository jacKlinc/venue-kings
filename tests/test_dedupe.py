"""Duplicate collapsing across sources.

The supplied fixtures contain no cross-source duplicates, so this behaviour is
specified here with synthetic records rather than observed in a live run.
"""

from datetime import UTC, datetime
from decimal import Decimal

from pipeline.dedupe import SOURCE_PRECEDENCE, deduplicate, identity_key
from pipeline.models import NormalizedProduct, WarningCode

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def product(pid, title, source, price="10.00", category="electronics"):
    return NormalizedProduct(
        id=pid,
        title=title,
        source=source,
        price=Decimal(price),
        category=category,
        fetched_at=NOW,
    )


def test_distinct_products_are_all_kept():
    products, warnings = deduplicate(
        [
            product("endpoint_a:a-1", "Keyboard", "endpoint_a"),
            product("endpoint_b:b-1", "Desk Lamp", "endpoint_b"),
        ]
    )

    assert len(products) == 2
    assert not warnings


def test_identity_ignores_case_and_punctuation():
    assert identity_key(product("x", "USB-C Hub", "endpoint_a")) == identity_key(
        product("y", "usb c hub", "endpoint_c")
    )


def test_duplicate_keeps_the_first_record_and_warns():
    products, warnings = deduplicate(
        [
            product("endpoint_a:a-9", "USB-C Hub", "endpoint_a", price="47.00"),
            product("endpoint_c:c-1", "USB-C Hub", "endpoint_c", price="49.50"),
        ]
    )

    assert len(products) == 1
    assert products[0].id == "endpoint_a:a-9"
    assert len(warnings) == 1
    assert warnings[0].code is WarningCode.DUPLICATE_CONFLICT
    assert warnings[0].record_id == "endpoint_c:c-1"


def test_warning_names_both_prices_and_the_surviving_record():
    _, warnings = deduplicate(
        [
            product("endpoint_a:a-1", "Webcam", "endpoint_a", price="74.99"),
            product("endpoint_b:b-1", "Webcam", "endpoint_b", price="71.00"),
        ]
    )

    assert "74.99" in warnings[0].detail
    assert "71.00" in warnings[0].detail
    assert "endpoint_a:a-1" in warnings[0].detail


def test_precedence_sort_makes_the_winner_independent_of_arrival_order():
    """Sources complete in nondeterministic order, so the runner sorts before deduplicating."""
    a = product("endpoint_a:a-1", "Webcam", "endpoint_a")
    c = product("endpoint_c:c-1", "Webcam", "endpoint_c")

    def merge(arrivals):
        ordered = sorted(arrivals, key=lambda p: SOURCE_PRECEDENCE.index(p.source))
        return deduplicate(ordered)[0]

    assert merge([a, c])[0].id == merge([c, a])[0].id == "endpoint_a:a-1"


def test_same_title_in_different_categories_is_not_a_duplicate():
    products, warnings = deduplicate(
        [
            product("endpoint_a:a-1", "Organiser", "endpoint_a", category="office"),
            product("endpoint_b:b-1", "Organiser", "endpoint_b", category="kitchen"),
        ]
    )

    assert len(products) == 2
    assert not warnings

"""Normalization: schema translation, price fidelity, and per-record failure isolation."""

from decimal import Decimal

import pytest

from pipeline.models import WarningCode
from pipeline.normalize import normalize_records

CURRENCY = "GBP"


def _normalize(source, records):
    return normalize_records(source, records, CURRENCY)


def test_source_a_maps_fields_and_namespaces_id():
    raw = [
        {"id": "a-101", "name": "Mechanical Keyboard", "price": 89.99, "category": "electronics"}
    ]
    products, warnings = _normalize("endpoint_a", raw)

    assert not warnings
    product = products[0]
    assert product.id == "endpoint_a:a-101"
    assert product.title == "Mechanical Keyboard"
    assert product.price == Decimal("89.99")
    assert product.currency == CURRENCY


def test_source_b_converts_integer_cents_to_major_units():
    raw = [{"sku": "b-201", "title": "Desk Lamp", "amount_cents": 3499, "department": "home"}]
    products, warnings = _normalize("endpoint_b", raw)

    assert not warnings
    assert products[0].price == Decimal("34.99")
    assert products[0].id == "endpoint_b:b-201"


def test_source_c_parses_price_string_exactly():
    raw = [
        {
            "product_id": "c-301",
            "product_name": "USB-C Hub",
            "price": "49.50",
            "type": "electronics",
        }
    ]
    products, _ = _normalize("endpoint_c", raw)

    assert products[0].price == Decimal("49.50")
    # Exactness matters: a float round-trip would not compare equal to the cents form.
    assert products[0].price == Decimal(4950) / Decimal(100)


def test_malformed_price_is_dropped_with_a_warning_not_an_exception():
    raw = [
        {"sku": "b-204", "title": "Travel Mug", "amount_cents": 2499, "department": "kitchen"},
        {"sku": "b-205", "title": "Broken", "amount_cents": "not-a-number", "department": "home"},
        {"sku": "b-206", "title": "Webcam", "amount_cents": 7499, "department": "electronics"},
    ]
    products, warnings = _normalize("endpoint_b", raw)

    assert [p.id for p in products] == ["endpoint_b:b-204", "endpoint_b:b-206"]
    assert len(warnings) == 1
    assert warnings[0].code is WarningCode.MALFORMED_RECORD
    assert warnings[0].record_id == "b-205"
    assert "amount_cents" in warnings[0].detail


def test_bad_data_heavy_shape_is_rejected():
    """The bad-data-heavy scenario nulls the price and removes the category entirely."""
    raw = [{"sku": "b-201", "title": "Desk Lamp", "amount_cents": None}]
    products, warnings = _normalize("endpoint_b", raw)

    assert not products
    assert warnings[0].record_id == "b-201"


@pytest.mark.parametrize(
    "record",
    [
        {"id": "a-1", "name": "  ", "price": 5.0, "category": "office"},
        {"id": "a-1", "name": "Thing", "price": 0, "category": "office"},
        {"id": "a-1", "name": "Thing", "price": -5, "category": "office"},
        {"id": "a-1", "name": "Thing", "price": 5.0, "category": ""},
    ],
    ids=["blank-title", "zero-price", "negative-price", "blank-category"],
)
def test_invalid_records_are_rejected(record):
    products, warnings = _normalize("endpoint_a", record and [record])

    assert not products
    assert len(warnings) == 1


def test_text_and_category_are_cleaned():
    raw = [{"id": "a-1", "name": "  Laptop Stand  ", "price": 44.25, "category": " Office "}]
    products, _ = _normalize("endpoint_a", raw)

    assert products[0].title == "Laptop Stand"
    assert products[0].category == "office"


def test_unknown_upstream_fields_are_ignored():
    raw = [{"id": "a-1", "name": "Thing", "price": 5.0, "category": "office", "surprise": True}]
    products, warnings = _normalize("endpoint_a", raw)

    assert not warnings
    assert products[0].id == "endpoint_a:a-1"

"""Convert raw upstream records into NormalizedProduct, isolating per-record failures."""

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError

from .models import (
    NormalizedProduct,
    RawSourceA,
    RawSourceB,
    RawSourceC,
    RunWarning,
    SourceName,
    WarningCode,
)

CENTS_PER_UNIT = Decimal(100)


def _from_a(raw: Mapping[str, object], currency: str, now: datetime) -> NormalizedProduct:
    record = RawSourceA.model_validate(raw)
    return NormalizedProduct(
        id=f"endpoint_a:{record.id}",
        title=record.name,
        source="endpoint_a",
        price=record.price,
        currency=currency,
        category=record.category,
        fetched_at=now,
    )


def _from_b(raw: Mapping[str, object], currency: str, now: datetime) -> NormalizedProduct:
    record = RawSourceB.model_validate(raw)
    return NormalizedProduct(
        id=f"endpoint_b:{record.sku}",
        title=record.title,
        source="endpoint_b",
        price=Decimal(record.amount_cents) / CENTS_PER_UNIT,
        currency=currency,
        category=record.department,
        fetched_at=now,
    )


def _from_c(raw: Mapping[str, object], currency: str, now: datetime) -> NormalizedProduct:
    record = RawSourceC.model_validate(raw)
    return NormalizedProduct(
        id=f"endpoint_c:{record.product_id}",
        title=record.product_name,
        source="endpoint_c",
        price=record.price,
        currency=currency,
        category=record.type,
        fetched_at=now,
    )


Normalizer = Callable[[Mapping[str, object], str, datetime], NormalizedProduct]

NORMALIZERS: dict[SourceName, Normalizer] = {
    "endpoint_a": _from_a,
    "endpoint_b": _from_b,
    "endpoint_c": _from_c,
}

# Upstream ids under their native field names, used only to label a rejected record.
_ID_FIELDS = ("id", "sku", "product_id")


def _identify(raw: Mapping[str, object]) -> str | None:
    """Best-effort id for a record that failed validation."""
    for field in _ID_FIELDS:
        value = raw.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _describe(error: ValidationError) -> str:
    parts = [
        f"{'.'.join(str(x) for x in e['loc']) or '(root)'}: {e['msg']}" for e in error.errors()
    ]
    return "; ".join(parts)


def normalize_records(
    source: SourceName,
    records: Iterable[Mapping[str, object]],
    currency: str,
    now: datetime | None = None,
) -> tuple[list[NormalizedProduct], list[RunWarning]]:
    """Normalize records one at a time; a bad record is dropped with a warning, never fatal."""
    normalizer = NORMALIZERS[source]
    timestamp = now or datetime.now(UTC)
    products: list[NormalizedProduct] = []
    warnings: list[RunWarning] = []

    for raw in records:
        try:
            products.append(normalizer(raw, currency, timestamp))
        except ValidationError as exc:
            warnings.append(
                RunWarning(
                    code=WarningCode.MALFORMED_RECORD,
                    source=source,
                    record_id=_identify(raw),
                    detail=_describe(exc),
                )
            )

    return products, warnings


__all__ = ["NORMALIZERS", "normalize_records"]

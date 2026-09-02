"""Normalized product, per-source raw models, and run reporting types."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

SourceName = Literal["endpoint_a", "endpoint_b", "endpoint_c"]


def _clean_text(value: object) -> object:
    """Strip surrounding whitespace, leaving non-strings for the field validator to reject."""
    return value.strip() if isinstance(value, str) else value


def _clean_category(value: object) -> object:
    return value.strip().lower() if isinstance(value, str) else value


Text = Annotated[str, BeforeValidator(_clean_text), Field(min_length=1)]
Category = Annotated[str, BeforeValidator(_clean_category), Field(min_length=1)]
Price = Annotated[Decimal, Field(gt=0, decimal_places=2)]


class NormalizedProduct(BaseModel):
    """A product in the pipeline's canonical shape, regardless of origin."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    title: Text
    source: SourceName
    price: Price
    currency: str = "GBP"
    category: Category
    fetched_at: datetime


class RawSourceA(BaseModel):
    """Source A record: clean float prices, already in major units."""

    model_config = ConfigDict(extra="ignore")

    id: Text
    name: Text
    price: Price
    category: Category


class RawSourceB(BaseModel):
    """Source B record: price arrives as integer minor units (cents)."""

    model_config = ConfigDict(extra="ignore")

    sku: Text
    title: Text
    amount_cents: int = Field(gt=0)
    department: Category


class RawSourceC(BaseModel):
    """Source C record: price arrives as a decimal string."""

    model_config = ConfigDict(extra="ignore")

    product_id: Text
    product_name: Text
    price: Price
    type: Category


class WarningCode(StrEnum):
    MALFORMED_RECORD = "malformed_record"
    DUPLICATE_CONFLICT = "duplicate_conflict"
    PAGE_FAILED = "page_failed"
    SOURCE_FAILED = "source_failed"


class RunWarning(BaseModel):
    """A non-fatal problem worth surfacing in the run report."""

    model_config = ConfigDict(frozen=True)

    code: WarningCode
    source: str
    detail: str
    record_id: str | None = None


class SourceStatus(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    FAILED = "failed"


class SourceReport(BaseModel):
    """Per-source outcome, so a reader can see which upstream misbehaved."""

    name: str
    status: SourceStatus
    pages_fetched: int = 0
    records_received: int = 0
    records_normalized: int = 0
    records_dropped: int = 0
    duration_ms: int = 0
    error: str | None = None


class RunStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"


class RunReport(BaseModel):
    """Top-level result of one pipeline run."""

    run_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    product_count: int
    products: list[NormalizedProduct]
    sources: dict[str, SourceReport]
    warnings: list[RunWarning]


__all__ = [
    "Category",
    "NormalizedProduct",
    "Price",
    "RawSourceA",
    "RawSourceB",
    "RawSourceC",
    "RunReport",
    "RunStatus",
    "RunWarning",
    "SourceName",
    "SourceReport",
    "SourceStatus",
    "Text",
    "WarningCode",
]

"""Collapse products that describe the same item across sources."""

import re
from collections.abc import Iterable

from .models import NormalizedProduct, RunWarning, WarningCode

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

# Source order decides which record wins a tie; earlier is preferred.
SOURCE_PRECEDENCE = ("endpoint_a", "endpoint_b", "endpoint_c")


def identity_key(product: NormalizedProduct) -> tuple[str, str]:
    """Cross-source identity: punctuation- and case-insensitive title plus category.

    Upstream ids are source-scoped and never collide, so identity has to come from
    the content. Title and category together are the strongest signal available.
    """
    slug = _NON_ALNUM.sub("-", product.title.lower()).strip("-")
    return slug, product.category


def deduplicate(
    products: Iterable[NormalizedProduct],
) -> tuple[list[NormalizedProduct], list[RunWarning]]:
    """Keep the first record for each identity, warning about each one dropped.

    Callers sort by SOURCE_PRECEDENCE first, so "first" is deterministic even though
    sources are fetched concurrently.
    """
    kept: dict[tuple[str, str], NormalizedProduct] = {}
    warnings: list[RunWarning] = []

    for product in products:
        key = identity_key(product)
        winner = kept.setdefault(key, product)
        if winner is product:
            continue

        warnings.append(
            RunWarning(
                code=WarningCode.DUPLICATE_CONFLICT,
                source=product.source,
                record_id=product.id,
                detail=f"dropped in favour of {winner.id} "
                f"(price {winner.price} vs {product.price})",
            )
        )

    return list(kept.values()), warnings


__all__ = ["SOURCE_PRECEDENCE", "deduplicate", "identity_key"]

"""Per-source adapters, one module per pagination style."""

from .base import Page, PageCapExceeded, Source
from .source_a import SourceA
from .source_b import SourceB
from .source_c import SourceC

__all__ = ["Page", "PageCapExceeded", "Source", "SourceA", "SourceB", "SourceC"]

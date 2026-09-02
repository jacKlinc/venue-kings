"""Runtime settings, overridable by environment or CLI."""

from typing import Annotated, Literal

from pydantic import BeforeValidator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Annotated[
    Literal["DEBUG", "INFO", "WARNING", "ERROR"],
    BeforeValidator(lambda v: v.upper() if isinstance(v, str) else v),
]


class Settings(BaseSettings):
    """Pipeline configuration, read from PIPELINE_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="PIPELINE_", extra="forbid")

    base_url: str = "http://localhost:8080"
    currency: str = "GBP"

    log_format: Literal["console", "json"] = "console"
    log_level: LogLevel = "INFO"

    connect_timeout: float = Field(default=5.0, gt=0)
    read_timeout: float = Field(default=10.0, gt=0)
    # Whole-run ceiling: a source still running when this expires is cancelled.
    run_timeout: float = Field(default=60.0, gt=0)

    retry_attempts: int = Field(default=4, ge=1)
    retry_base_delay: float = Field(default=0.25, gt=0)
    retry_max_delay: float = Field(default=8.0, gt=0)
    max_retry_after: float = Field(default=10.0, gt=0)
    # Ceiling on retries per source, so one broken upstream cannot monopolise the run.
    retry_budget_per_source: int = Field(default=10, ge=0)

    # Guards against a mispaged cursor or a next_offset that never advances.
    max_pages_per_source: int = Field(default=100, ge=1)

    source_c_page_size: int = Field(default=2, ge=1)
    source_c_rate: int = Field(default=2, ge=1)
    source_c_window: float = Field(default=1.0, gt=0)


__all__ = ["Settings"]

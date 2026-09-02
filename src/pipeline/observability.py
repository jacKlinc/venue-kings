"""Structured logging setup."""

import logging
import sys
from typing import Literal

import structlog

LogFormat = Literal["json", "console"]


class _StderrLogger:
    """Writes to whatever sys.stderr is at call time.

    structlog's PrintLogger binds the stream once. Resolving it lazily keeps logging
    alive when stderr is replaced — by pytest's capture, or by log rotation in a daemon.
    """

    def msg(self, message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    log = debug = info = warning = error = critical = failure = exception = msg


def configure_logging(fmt: LogFormat = "console", level: int = logging.INFO) -> None:
    """Install a structlog pipeline writing to stderr, leaving stdout free for the report."""
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=lambda *_: _StderrLogger(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


__all__ = ["LogFormat", "configure_logging", "get_logger"]

"""Command-line entry point."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .config import Settings
from .models import RunReport, RunStatus
from .observability import configure_logging
from .runner import run

EXIT_OK = 0
EXIT_FAILED = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="fetch every source and emit a run report")
    run_cmd.add_argument("--output", type=Path, help="write the report here instead of stdout")
    return parser


def _emit(report: RunReport, output: Path | None) -> None:
    payload = report.model_dump_json(indent=2)
    if output is None:
        sys.stdout.write(payload + "\n")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_format, logging.getLevelNamesMapping()[settings.log_level])

    report = asyncio.run(run(settings))
    _emit(report, args.output)
    return EXIT_FAILED if report.status is RunStatus.FAILED else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())

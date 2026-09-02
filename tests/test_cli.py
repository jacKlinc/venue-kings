"""CLI wiring: exit codes, output routing, and stdout/stderr separation."""

import json

import pytest

from pipeline.cli import EXIT_FAILED, EXIT_OK, main


@pytest.fixture
def base_url(monkeypatch):
    """Point the CLI at a server via the environment, as a real operator would."""

    def _set(url: str) -> None:
        monkeypatch.setenv("PIPELINE_BASE_URL", url)

    return _set


def test_report_goes_to_stdout_and_logs_to_stderr(mock, base_url, capsys):
    base_url(mock.base_url)

    exit_code = main(["run"])
    captured = capsys.readouterr()

    assert exit_code == EXIT_OK
    report = json.loads(captured.out)
    assert report["status"] == "partial_success"
    assert "run.finished" in captured.err


def test_output_flag_writes_a_file_and_leaves_stdout_empty(mock, base_url, tmp_path, capsys):
    base_url(mock.base_url)
    destination = tmp_path / "nested" / "run.json"

    exit_code = main(["run", "--output", str(destination)])

    assert exit_code == EXIT_OK
    assert capsys.readouterr().out == ""
    assert json.loads(destination.read_text())["product_count"] > 0


def test_unreachable_upstream_exits_failed(unused_port, base_url, capsys):
    """Every source down means no products at all, which is a failed run."""
    base_url(f"http://127.0.0.1:{unused_port}")

    exit_code = main(["run"])
    report = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_FAILED
    assert report["status"] == "failed"
    assert report["product_count"] == 0
    assert all(s["status"] == "failed" for s in report["sources"].values())


def test_command_is_required(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main([])

    assert excinfo.value.code != 0


@pytest.fixture
def unused_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

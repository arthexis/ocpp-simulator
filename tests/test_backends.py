"""Ensure Python is unchanged and Go is an explicit opt-in."""
from unittest.mock import patch
import pytest
from ocpp_simulator import cli


def test_python_is_default():
    assert cli._select_backend(["run", "--cp", "SIM001"])[0] == "python"


@pytest.mark.parametrize("args", [
    ["--backend", "go", "run", "--cp", "SIM001"],
    ["run", "--backend=go", "--cp", "SIM001"],
])
def test_go_dispatch(args):
    assert cli._select_backend(args) == ("go", ["run", "--cp", "SIM001"])


def test_bad_backend(capsys):
    assert cli.main(["--backend", "rust", "run"]) == 2
    assert "Unsupported backend" in capsys.readouterr().err


def test_go_dispatch_does_not_run_python():
    with patch.object(cli, "_go_executable", return_value="/tmp/ocpp-simulator-go"), \
         patch.object(cli.subprocess, "run") as go, \
         patch.object(cli, "run_charge_point") as py:
        go.return_value.returncode = 7
        assert cli.main(["run", "--backend", "go", "--cp", "SIM001", "--url", "ws://127.0.0.1/SIM001"]) == 7
        go.assert_called_once_with([
            "/tmp/ocpp-simulator-go", "run", "--cp", "SIM001", "--url", "ws://127.0.0.1/SIM001",
        ], check=False)
        py.assert_not_called()


def test_go_missing_binary_fails_closed(monkeypatch, capsys):
    monkeypatch.setenv("OCPP_SIMULATOR_GO_BIN", "/nonexistent/ocpp-simulator-go")
    assert cli.main(["--backend", "go", "run", "--cp", "SIM001"]) == 1
    assert "Go backend unavailable" in capsys.readouterr().err


def test_python_entrypoint_does_not_dispatch_go():
    with patch.object(cli, "run_charge_point") as py:
        assert cli.main_python(["help"]) == 0
        py.assert_not_called()

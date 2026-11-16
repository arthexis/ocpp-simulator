"""Staged builds must not inspect an unrelated parent Git directory."""
from pathlib import Path
import os
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_staged_go_build_ignores_invalid_parent_repository(tmp_path):
    go = shutil.which("go")
    if not go:
        pytest.skip("Go is not installed")
    role = (ROOT / "ansible/roles/ocpp_simulator/tasks/main.yml").read_text()
    for task, next_task in (
        ("Build pinned Go backend into candidate release", "Build isolated LAB-6 client"),
        ("Build isolated LAB-6 client into the same candidate release", "Calculate relative checksum"),
    ):
        assert "- -buildvcs=false" in role.split(f"- name: {task}", 1)[1].split(f"- name: {next_task}", 1)[0]
    (tmp_path / ".git").mkdir()
    module = tmp_path / "staged" / "go-source"
    module.mkdir(parents=True)
    (module / "go.mod").write_text("module example.test/staged\n\ngo 1.23\n")
    (module / "main.go").write_text('package main\nfunc main() {}\n')
    env = {**os.environ, "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off"}
    result = subprocess.run([go, "build", "-buildvcs=false", "-o", str(tmp_path / "app"), "."], cwd=module, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "app").is_file()

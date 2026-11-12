"""Offline fixtures for the main-branch installation receipt; no node access."""
from __future__ import annotations

import hashlib
import os
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
API = runpy.run_path(str(ROOT / "scripts" / "verify_main_install.py"))
verify_release = API["verify_release"]
VerificationError = API["VerificationError"]
SHA = "ab" * 20
OLD = "cd" * 20
PUBLIC = ("ocpp-simulator", "ocpp-simulator-py", "ocpp-simulator-go")


def fixture_release(tmp_path: Path, *, upgrade: bool = False, repeat: bool = False):
    root = tmp_path / "state"
    release = root / "releases" / SHA
    release.mkdir(parents=True)
    (release / ".revision").write_text(SHA + "\n", encoding="utf-8")
    bin_path = release / "bin"
    bin_path.mkdir()
    for name in (*PUBLIC, "python", "r6c3b2-uidlab"):
        file = bin_path / name
        file.write_bytes(b"#!/bin/sh\nexit 0\n")
        file.chmod(0o755)
    lab = bin_path / "r6c3b2-uidlab"
    digest = hashlib.sha256(lab.read_bytes()).hexdigest()
    (bin_path / "r6c3b2-uidlab.sha256").write_text(
        digest + "  r6c3b2-uidlab\n", encoding="utf-8"
    )
    (root / "current").symlink_to(release)
    user_bin = tmp_path / "user-bin"
    user_bin.mkdir()
    for name in PUBLIC:
        (user_bin / name).symlink_to(root / "current" / "bin" / name)
    before_current = ""
    before_previous = ""
    if upgrade:
        before_current = str(root / "releases" / OLD)
        (root / "previous").symlink_to(before_current)
    if repeat:
        before_current = str(release)
        before_previous = str(root / "releases" / OLD)
        (root / "previous").symlink_to(before_previous)
    return root, user_bin, before_current, before_previous


@pytest.mark.parametrize("scenario", ["new", "upgrade", "repeat"])
def test_valid_release_and_previous_semantics(tmp_path, scenario):
    fixture = fixture_release(tmp_path, upgrade=scenario == "upgrade",
                              repeat=scenario == "repeat")
    verify_release(*fixture[:2], SHA, *fixture[2:])


def test_modified_client_rejected(tmp_path):
    root, user_bin, *_ = fixture_release(tmp_path)
    (root / "releases" / SHA / "bin" / "r6c3b2-uidlab").write_bytes(b"tampered")
    with pytest.raises(VerificationError, match="checksum"):
        verify_release(root, user_bin, SHA)


def test_incorrect_release_pointer_rejected(tmp_path):
    root, user_bin, *_ = fixture_release(tmp_path)
    (root / "current").unlink()
    (root / "current").symlink_to(root / "releases" / OLD)
    with pytest.raises(VerificationError, match="current"):
        verify_release(root, user_bin, SHA)


def test_previous_must_preserve_earlier_current(tmp_path):
    root, user_bin, old_current, _ = fixture_release(tmp_path, upgrade=True)
    (root / "previous").unlink()
    (root / "previous").symlink_to(root / "releases" / SHA)
    with pytest.raises(VerificationError, match="previous"):
        verify_release(root, user_bin, SHA, old_current)


def test_test_client_cannot_be_public(tmp_path):
    root, user_bin, *_ = fixture_release(tmp_path)
    (user_bin / "r6c3b2-uidlab").symlink_to(root / "current" / "bin" / "r6c3b2-uidlab")
    with pytest.raises(VerificationError, match="exposed"):
        verify_release(root, user_bin, SHA)


def test_missing_backend_and_bad_checksum_record_fail_closed(tmp_path):
    root, user_bin, *_ = fixture_release(tmp_path)
    (root / "releases" / SHA / "bin" / "ocpp-simulator-go").unlink()
    with pytest.raises(VerificationError, match="non-executable"):
        verify_release(root, user_bin, SHA)

    root2, user_bin2, *_ = fixture_release(tmp_path / "second")
    record = root2 / "releases" / SHA / "bin" / "r6c3b2-uidlab.sha256"
    record.write_text("0" * 64 + "  /absolute/path\n", encoding="utf-8")
    with pytest.raises(VerificationError, match="relative"):
        verify_release(root2, user_bin2, SHA)


def test_simulator_scoped_runner_workflow_is_retired():
    """Simulator CI stays hosted; GWay Remote Draft #559 owns future deployment."""
    workflows = ROOT / ".github" / "workflows"
    assert not (workflows / "deploy-main.yml").exists()
    ci = (workflows / "ci.yml").read_text(encoding="utf-8")
    assert "  push:\n    branches:\n      - main" in ci
    assert "Verify obsolete simulator-scoped runner workflow is removed" in ci
    assert "runs-on: ubuntu-latest" in ci
    tasks = (ROOT / "ansible/roles/ocpp_simulator/tasks/main.yml").read_text(
        encoding="utf-8"
    )
    assert "ansible.builtin.systemd:" not in tasks
    assert "ansible.builtin.service:" not in tasks
    assert "./cmd/r6c3b2-uidlab" in tasks
    assert "./cmd/ocpp-simulator-go" in tasks


def test_main_fence_fail_closed_offline(monkeypatch):
    """Never contact GitHub; exercise same, drift, invalid and timeout."""
    fence = runpy.run_path(str(ROOT / "scripts" / "verify_main_head.py"))
    fence["assert_main_is"](SHA, SHA)
    for wrong in (OLD, "", "invalid", "0" * 39, "g" * 40):
        with pytest.raises(ValueError):
            fence["assert_main_is"](SHA, wrong)
    with pytest.raises(ValueError):
        fence["assert_main_is"]("", SHA)

    class Result:
        stdout = SHA + "\n"
    calls = []
    def good(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return Result()
    monkeypatch.setattr("subprocess.run", good)
    assert fence["main"]([SHA]) == 0
    assert calls[0][0] == ["gh", "api", "repos/arthexis/ocpp-simulator/branches/main", "--jq", ".commit.sha"]
    assert calls[0][1]["timeout"] == 20

    monkeypatch.setattr("subprocess.run", lambda *a, **kw: type("Result", (), {"stdout": OLD + "\n"})())
    assert fence["main"]([SHA]) == 1
    def timeout(*args, **kwargs):
        import subprocess
        raise subprocess.TimeoutExpired(cmd="gh", timeout=20)
    monkeypatch.setattr("subprocess.run", timeout)
    assert fence["main"]([SHA]) == 1
    assert fence["main"](["invalid"]) == 1
    assert fence["main"]([]) == 2


def test_main_fence_is_between_build_and_pointer_writes():
    role = (ROOT / "ansible/roles/ocpp_simulator/tasks/main.yml").read_text(encoding="utf-8")
    build = role.index("- name: Build isolated LAB-6 client")
    guard = role.index("- name: Reject superseded main immediately before mutating release pointers")
    previous = role.index("- name: Preserve previous release before changing current")
    activate = role.index("- name: Activate requested revision")
    assert build < guard < previous < activate
    assert "OCPP_SIMULATOR_EXPECTED_MAIN_SHA" in role
    assert "ocpp_simulator_activate | bool" in role[guard:previous]
    assert "ocpp_simulator_expected_main_sha != ''" in role[guard:previous]
    assert "lookup('ansible.builtin.env', 'OCPP_SIMULATOR_EXPECTED_MAIN_SHA')" in role
    assert (ROOT / "scripts/verify_main_head.py").is_file()
    assert (ROOT / "scripts/verify_main_install.py").is_file()

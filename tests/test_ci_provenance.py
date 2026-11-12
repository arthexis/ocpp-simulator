"""Regression: lab source provenance works for pull_request and push to main.

Execute only each workflow's source-identity printf commands; no network,
Go build, Ansible, installation or OCPP session is performed.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
LAB_STEPS = (
    ("Cross-compile offline ARM64 UID test executable (LAB-5)", "r6c3b2-lab5"),
    ("R6C-3B2 LAB-6 build and attest client ARM64", "r6c3b2-lab6"),
)


def identity_commands(step_name: str, lab_directory: str, output: Path) -> str:
    content = WORKFLOW.read_text(encoding="utf-8")
    beginning = content.index("      - name: " + step_name + "\n")
    ending = content.index("      - uses: actions/upload-artifact@v4", beginning)
    step = content[beginning:ending]

    assert "PR_HEAD_SHA" not in step
    assert "github.event.pull_request.head.sha" not in step
    assert 'git rev-parse HEAD' in step
    assert 'sha256sum -c SHA256SUMS' in step
    assert 'GOARCH=arm64' in step
    assert 'github.com/lorenzodonini/ocpp-go' in step
    assert 'h1:THNriVV3bUWkGzaIkDeytcgoeBRjoN9ezPHsddIDfgc=' in step

    begin = step.index("          printf 'checkout=")
    end = step.rindex("source-identity.txt") + len("source-identity.txt")
    fragment = step[begin:end]
    expected_destination = f"/tmp/{lab_directory}/source-identity.txt"
    assert expected_destination in fragment
    return fragment.replace(expected_destination, str(output))


@pytest.mark.parametrize("step_name,lab_directory", LAB_STEPS)
@pytest.mark.parametrize("event_name", ["pull_request", "push"])
def test_lab_provenance_uses_actual_checkout_for_both_events(
    tmp_path: Path, step_name: str, lab_directory: str, event_name: str
) -> None:
    identity_path = tmp_path / "source-identity.txt"
    env = {
        **os.environ,
        "GITHUB_EVENT_NAME": event_name,
        "GITHUB_RUN_ID": "42",
        "GITHUB_RUN_ATTEMPT": "1",
    }
    env.pop("PR_HEAD_SHA", None)
    script = "set -euo pipefail\n" + identity_commands(
        step_name, lab_directory, identity_path
    )
    # A container checkout can be owned by a different UID; emulate the
    # same Git HEAD operation in an isolated, user-owned test repository.
    # Do not weaken Git's safe.directory policy or change runner settings.
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(
        ["git", "init", "--quiet"], cwd=checkout, check=True, timeout=15
    )
    subprocess.run(
        ["git", "-c", "user.name=CI Fixture", "-c",
         "user.email=ci-fixture@example.invalid",
         "commit", "--allow-empty", "--quiet", "-m", "offline test HEAD"],
        cwd=checkout, check=True, timeout=15,
    )
    subprocess.run(
        ["bash", "-c", script], cwd=checkout, env=env, check=True, timeout=15
    )
    recorded = dict(
        line.split("=", 1)
        for line in identity_path.read_text(encoding="utf-8").splitlines()
    )
    actual_head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=checkout, text=True, timeout=15
    ).strip()
    assert recorded["checkout"] == actual_head
    assert recorded["event"] == event_name
    assert "pr_head" not in recorded
    if lab_directory == "r6c3b2-lab6":
        assert recorded["run_id"] == "42"
        assert recorded["run_attempt"] == "1"

"""Read-only GitHub main SHA fence before Ansible release pointer changes."""
from __future__ import annotations

import re
import subprocess
import sys

_SHA = re.compile(r"[0-9a-f]{40}\Z")
_ENDPOINT = "repos/arthexis/ocpp-simulator/branches/main"


def assert_main_is(expected: str, actual: str) -> None:
    if not _SHA.fullmatch(expected):
        raise ValueError("missing or invalid expected main SHA")
    if not _SHA.fullmatch(actual.strip()) or actual.strip() != expected:
        raise ValueError("main advanced or SHA response invalid")


def fetch_main() -> str:
    # The existing GitHub Actions GH_TOKEN is inherited; never display it.
    result = subprocess.run(
        ["gh", "api", _ENDPOINT, "--jq", ".commit.sha"],
        check=True, text=True, capture_output=True, timeout=20,
    )
    return result.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("exact expected main SHA required", file=sys.stderr)
        return 2
    try:
        assert_main_is(args[0], args[0])
        assert_main_is(args[0], fetch_main())
    except (ValueError, subprocess.SubprocessError, OSError) as exc:
        print(f"main SHA fence rejected activation ({type(exc).__name__})", file=sys.stderr)
        return 1
    print("main SHA fence verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

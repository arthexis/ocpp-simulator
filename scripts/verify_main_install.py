"""Read-only verification of an Ansible-installed OCPP simulator SHA release.

This script only checks paths, hashes and symlinks. It never invokes a simulator.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
from pathlib import Path

SHA = re.compile(r"[0-9a-f]{40}\Z")
HASH_LINE = re.compile(r"([0-9a-f]{64})  r6c3b2-uidlab\n\Z")
PUBLIC = ("ocpp-simulator", "ocpp-simulator-py", "ocpp-simulator-go")
EXPECTED_FILES = (*PUBLIC, "python", "r6c3b2-uidlab")


class VerificationError(Exception):
    pass


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def _link(path: Path) -> str:
    if path.is_symlink():
        return os.readlink(path)
    _check(not path.exists(), f"unmanaged non-link path: {path}")
    return ""


def verify_release(
    root: Path,
    bin_dir: Path,
    expected_sha: str,
    before_current: str = "",
    before_previous: str = "",
) -> None:
    _check(bool(SHA.fullmatch(expected_sha)), "invalid expected commit")
    _check(root.is_dir() and not root.is_symlink(), "invalid release root")
    release = root / "releases" / expected_sha
    _check(release.is_dir() and not release.is_symlink(), "missing immutable release")
    desired = str(release)
    _check(_link(root / "current") == desired, "current does not select expected SHA")
    _check((release / ".revision").read_text(encoding="utf-8") == expected_sha + "\n",
           "release provenance differs from checkout")

    for filename in EXPECTED_FILES:
        path = release / "bin" / filename
        _check(path.is_file() and os.access(path, os.X_OK),
               f"missing or non-executable installed file: {filename}")
    _check(not (release / "bin" / "r6c3b2-uidlab").is_symlink(),
           "test-only client must be an installed regular executable")

    sha_line = (release / "bin" / "r6c3b2-uidlab.sha256").read_text(encoding="utf-8")
    match = HASH_LINE.fullmatch(sha_line)
    _check(match is not None, "checksum must be relative, single-entry and SHA-256")
    digest = hashlib.sha256()
    with (release / "bin" / "r6c3b2-uidlab").open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    _check(digest.hexdigest() == match.group(1), "test-only client checksum mismatch")

    _check(bin_dir.is_dir() and not bin_dir.is_symlink(), "invalid CLI bin directory")
    for name in PUBLIC:
        _check(_link(bin_dir / name) == str(root / "current" / "bin" / name),
               f"public CLI points outside the managed release: {name}")
    _check(not (bin_dir / "r6c3b2-uidlab").exists() and
           not (bin_dir / "r6c3b2-uidlab").is_symlink(),
           "test-only client must not be exposed in user bin")

    previous = _link(root / "previous")
    if before_current and before_current != desired:
        _check(previous == before_current, "previous did not retain old current")
    else:
        _check(previous == before_previous,
               "idempotent or initial install changed previous unexpectedly")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--before-current", default="")
    parser.add_argument("--before-previous", default="")
    args = parser.parse_args()
    try:
        verify_release(
            args.root, args.bin_dir, args.expected_sha,
            args.before_current, args.before_previous,
        )
    except (VerificationError, OSError, UnicodeError) as exc:
        parser.exit(1, f"installation verification failed: {exc}\n")
    print(f"INSTALL_VERIFIED {args.expected_sha} (no simulator session started)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Simulator Ansible release and delegated auto-install — PRE-READY

The simulator's own `.github/workflows/deploy-main.yml` has been **retired
from PR Draft #20**. GitHub has zero `arthexis/ocpp-simulator` self-hosted
runners: its previous push-to-main workflow would have queued indefinitely.
The required simulator `CI` workflow remains intact and continues to run
on pull requests and main pushes using hosted runners.

The replacement installation **is not yet active**: GWay Remote
[Draft PR #559](https://github.com/arthexis/gway-remote/pull/559)
proposes a fixed, CI-gated schedule on GWay Remote's existing repository-scoped
`gway-001` runner, which operates as `arthe`. It requires simulator PR #20
to have been merged and the latest **exact main SHA** `push` CI to succeed.
The proposed route uses the operator's existing GitHub authentication and
a private, temporary exact-SHA checkout. No extra runner, service, credentials,
OAuth scope or generalized deployment target is introduced.

## Installation contract preserved in the simulator repository

The simulator's existing `deploy.sh` and Ansible role remain the only
installer. They build both Python and Go backends from the checked-out source,
plus private `r6c3b2-uidlab` (`ocpp-go v0.19.0`, pinned and checksum-
verified). Versioned releases have `.revision`, `current/previous` links,
regular executable files and a relative SHA-256 checksum for the lab client.
No test-only client is published in `~/.local/bin` and neither CLI nor Ansible
launches a simulator session or a service.

Read-only `scripts/verify_main_install.py` verifies SHA provenance,
immutable release paths, CLI symlinks, Go/Python binaries and lab SHA-256.
CI keeps synthetic initial/upgrade/same-SHA tests and negative tests
for tampered binaries, incorrect pointers and accidental CLI exposure.

## Fail-closed main-SHA fence retained

The delegated GWay Remote deploy coordinator will pass
`OCPP_SIMULATOR_EXPECTED_MAIN_SHA=<exact current main SHA>` to
the simulator's `deploy.sh`. Ansible first verifies that this SHA matches
the checkout, then **after compilation** runs
`scripts/verify_main_head.py` immediately before any mutation of
`previous/current`. If GitHub's canonical main SHA advanced, or the query
fails or is malformed, activation is refused. The ordinary manually invoked
`deploy.sh` without this opt-in variable is unchanged. Offline CI still
covers mismatch, timeout and task ordering.

A GitHub HEAD query and a local filesystem symlink change cannot be atomic:
there remains a small post-check race, explicitly not claimed away.

## Acceptance boundaries

GWay Remote PR #559 and simulator PR #20 both remain **Draft**, with no merge,
installation or active delegated scheduler from these changes. Offline CI
demonstrates build/verification behavior, not actual installation on
`gway-001`. Before Ready or merge, validate the exact HEADs, checks,
reviews, rulesets and cross-repository sequencing, and explicitly authorize
the activation/deployment step.

`BeforeAvailable=Quarantined` and R6 unchecked remain mandatory. No
`CleanVerified`, replay, admission, OCPP-CSMS production change, SQLite
migration or network test is authorized by this cleanup.

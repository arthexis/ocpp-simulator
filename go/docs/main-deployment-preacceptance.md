# Main-branch deployment of ocpp-simulator (pre-merge acceptance)

**Scope:** the existing PR Draft #20; no merge or installation is performed when
this workflow is introduced on a feature branch.

A successful push to protected `main` (including an authorized merge) schedules
`.github/workflows/deploy-main.yml` on the **existing** self-hosted GitHub
Actions ARM64 runner labeled `gway-001`, under its existing `arthe` account.
There is no agent, OAuth credential, new service, timer, Ansible privilege, or
deployment broker added by this feature.

The workflow is deliberately **not triggered by pull requests**. It serializes
its own runs, skips a superseded `main` SHA, performs a clean Actions checkout
of the exact push SHA (never changes `~/Repos/ocpp-simulator`), checks operator
and architecture, snapshots the existing `current`/`previous` symlink
targets, then calls `sh ./deploy.sh` using the existing owner-scoped flock and
Ansible role. No service is started. Both Python and Go backends are installed
normally. The R6C-3B2 test-only `r6c3b2-uidlab` is built from the *same source*
and remains private to the immutable release, with relative SHA256SUMS; it is
not published in `~/.local/bin`.

A read-only post-install verifier checks `releases/<sha>`, `.revision`,
`current`, the pre-install `previous`/old `current` transition, executable
availability and symlinks for both backends, and the exact test-only binary
SHA-256. It invokes **no simulator session**. Three normal CLI `--help`
commands are permitted; the Go module version is checked from binary build
metadata. Failure stops the job; it does not invoke replay, restart or
`CleanVerified`. No rollback is introduced by this workflow; a failed
candidate remains evident for diagnosis under the versioned release design.

CI tests the verifier against synthetic initial, upgrade, same-SHA, corrupted,
wrong-link, missing-backend and accidentally published-lab fixtures, and parses
the workflow YAML without scheduling the self-hosted runner. These checks do
**not** constitute a live gway-001 install receipt.

Caveats: the CI `Ansible installer` runs on a GitHub-hosted runner. The existing
gway-001 simulator release is an older short-SHA release without Go backend and
without `.revision`, and the operator checkout is behind. The new normal
installer creates a different immutable SHA release. Actual arrival at gway-001
must be verified against the exact post-merge SHA after an explicitly approved
merge. `BeforeAvailable=Quarantined` and R6 remains unchecked; no
`CleanVerified`, replay, admission, or production Go↔CSMS claim.

## PRE-READY main advancement fence

The main-only workflow passes the opt-in environment variable
`OCPP_SIMULATOR_EXPECTED_MAIN_SHA` to its existing Ansible deployment step.
The role rejects an invalid expected value or an SHA different from the
actual checkout. **After building and validating** both Go executables, and
immediately **before** changing `previous` or `current`, the role calls
`scripts/verify_main_head.py` to fetch canonical GitHub `main` using the
existing Actions `GH_TOKEN`. Drift, missing/invalid data, inaccessible API,
or timeout abort activation without changing either release pointer.
Ordinary manual `deploy.sh` runs with no expected SHA retain the original
behavior. CI mocks the canonical GitHub response and verifies ordering.

**Limitation:** a GitHub HEAD read cannot be atomic with a local symlink
update. A change to `main` in the tiny interval after the final read can
still produce transient outdated activation. The only registered runner on
`gway-001` is scoped to `arthexis/gway-remote`; GitHub reports zero runners
for `ocpp-simulator`. Thus neither Ready nor merge is permitted yet.

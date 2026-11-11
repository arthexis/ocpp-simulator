# R6C-3B2 — build test-only UID client using normal Ansible installation

The existing `deploy.sh` Ansible role stages the same Git checkout, resolves
Go modules, builds `ocpp-simulator-go`, and activates an immutable SHA release.
The simulator is **not started automatically**: starting either backend is a
separate manual action.

The same installation now also compiles `./cmd/r6c3b2-uidlab` into
`releases/<checkout-SHA>/bin/r6c3b2-uidlab`, validates pinned
`github.com/lorenzodonini/ocpp-go v0.19.0` and module hash
`h1:THNriVV3bUWkGzaIkDeytcgoeBRjoN9ezPHsddIDfgc=`, runs
`go mod verify`, records `bin/r6c3b2-uidlab.sha256` with a *relative*
filename and re-verifies it on each installation attempt before activation.
An existing immutable release missing the CLI is rejected, **never repaired
in place**. A same-SHA deploy validates the checksum without recompilation.

The production Python and Go backends are unchanged. The isolated CLI is
neither installed into `~/.local/bin` nor called by Ansible, its dispatcher,
a service or a timer. A normal deployment may still update the existing
`current` and `previous` release links, as explicitly accepted by the
operator. No separate binary transfer is required.

The CLI is **not an OCPP authority**. Its future invocation must first verify
a separate `bubblewrap` user/PID/network namespace with a loopback-only
interface, zero routes, an inaccessible production filesystem, synthetic
`/lab` journal and ephemeral CSMS on `127.0.0.1:39291`. LAB-7 failed at
artifact download and did not execute any Go↔Python integration.

Keep PR #20 Draft, R6 unchecked, `BeforeAvailable=Quarantined`.
No `CleanVerified`, replay, admission, service start, merge or deployment
is part of this repository-only change.

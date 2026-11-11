# R6C-3B2-LAB-6 — test-only ARM64 OCPP-J client

Scope: **only** arthexis/ocpp-simulator PR Draft #20. Files: `go/cmd/r6c3b2-uidlab/main.go`, `main_test.go`, this note and CI. No production binary, recovery state, CSMS implementation, Ansible, deployment recipe, firmware or service is modified. The existing simulator's `BeforeAvailable` remains **Quarantined** and R6 remains **unchecked**.

## Provenance

`go-spike` CI resolves the pinned `github.com/lorenzodonini/ocpp-go v0.19.0`, verifies module sum `h1:THNriVV3bUWkGzaIkDeytcgoeBRjoN9ezPHsddIDfgc=` and `go mod verify`, runs `go vet ./...`, `go test -race -count=1 ./...`, and repeatedly runs the new test-only CLI package `go test -race -count=3 ./cmd/r6c3b2-uidlab`. It cross-compiles `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, checks ELF/AArch64, records `go version -m` and `go list -m -json`, and publishes a short-lived artifact with **relative** `SHA256SUMS`, checkout, event, PR_HEAD, run ID and attempt. The test ELF from LAB-5 remains separate.

## Model and limitations

The isolated binary is a standalone `cmd/r6c3b2-uidlab`, not installed by `deploy.sh`. It permits only the exact endpoint `ws://127.0.0.1:39291`, which the library extends with the synthetic CP ID `lab-cp-uid`. Before any connection it requires a private `/lab/.r6c3b2-lab6-private` marker, a loopback-only network namespace with no routes, and an existing 0700 journal beneath `/lab/`. The marker is **not** proof by itself; it supplements network checks. No production address/port or external collector is configurable. Do not execute unless the bwrap filesystem and network isolation have been independently demonstrated in a separately authorized stage.

For each synthetic `StartTransaction` the CLI allocates a fresh request, binds that exact request pointer to a synthetic source/database/tenant/CP/epoch/attempt and immutable JSON snapshot, then uses the real `ocppj.Client.SendRequest` / `CreateCall` path. The dispatcher checks the generated `UniqueId`, action, OCPP-J frame and payload against that binding. It uses `O_EXCL` to reserve attempt and UID, `fsync` for both files and the directory, **then** delegates to the library's dispatcher. Duplicate attempts/UIDs, malformed or mutated requests, unsafe directories, scope mismatch and post-enqueue failures all quarantine; no rollback, CleanVerified, replay, admission or automatic retry is authorized. No global `SetMessageIdGenerator` and no change to the OCPP 1.6J payload.

Negative tests run **offline with a fake queue**, not a network receiver: actual library-generated UID before delegation, malformed/unbound/mismatched frames, duplicate UID/attempt, concurrent independent attempts, lost ACK/queue error simulated, SIGKILL after fsync, wrong scope, storage permissions, missing journal and unsafe endpoint rejection. Tests do **not** prove an actual ACK was lost on WebSocket, a Python `call_unique_id` reached `commit_start`, cross-node fencing, or production reconciliation.

Acceptance gate: CI must finish with the exact PR HEAD and a SHA256-verified ARM64 artifact. Keep PR Draft and R6 unchecked; no transfer or execution on gway-001 in LAB-6.

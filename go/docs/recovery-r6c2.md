# R6C-2 — transactional recovery outbox (offline only)

**Status: implementation under test, Draft PR #20. No Ready, merge,
deployment, restart, production connection or CSMS authorization.**

This increment extends the **private Go SQLite** database selected for R6B,
not the authoritative `ocpp-csms` SQLite database and not Python's state.

## Fail-closed lifecycle

`RecoveryJournal` is bound to the existing `SessionManager` and therefore
shares its connector/transaction ownership lock order. `RemoteStartDispatcher`
cannot acknowledge queue admission until `ReserveIntent` has committed an
opaque random attempt ID and generation. Battery capacity/energy/seed/drift
are committed before accepting an initial battery session.

The worker commits `StartIntent` before calling OCPP
`StartTransaction`; a **real CSMS-assigned ID**, when received, is committed
in `ChargingKnown` before MeterValues or Charging is emitted.
Explicit rejection is durably recorded. Timeouts, crashes, or lost replies
retain `StartIntent` without a fabricated ID.

Every meter step commits `MeterIntent` with session Wh, W and complete
battery RNG/SoC state **before** `MeterValues`, and `MeterAcked`
only after its confirmation. The worker must not advance another interval if
that acknowledgement cannot be committed. An accepted RemoteStop is
durable (`StopRequested`) before its cancellation is acknowledged.
`StopIntent` includes immutable transaction ID, final Wh and frozen
Local/Remote/SoftReset reason, committed before calling
`StopTransaction`. `StopAcked` is committed **after** the CSMS reply
and **before** announcing Available. `Closed` is committed before
the in-memory `ConnectorLedger` releases the slot.

The database appends a monotonically sequenced `recovery_events` receipt
and updates the current `recovery_attempts` row in the **same SQLite
transaction**, with WAL + `synchronous=FULL`. A partial write or
precommit failure rolls back both changes. Local constraints forbid two
active attempts on one connector or sharing an active CSMS transaction ID.
Attempts are tied to the committed local generation. Startup increments
the generation and stays `Quarantined`.

## Upgrade and safety boundaries

Private schema version 1 from R6C-1 is upgraded transactionally to version
2, adding attempts, per-step events, meter/RNG state, and uniqueness
constraints. The R6C-1 `recovery_pending` records are retained
unaltered: they are still unresolved, not silently migrated to verified
state. Neither the Go CLI nor the library has a code path granting
`CleanVerified`: `Serve` calls `BeforeAvailable` **before connecting**
and rejects with `ErrRecoveryQuarantined`. The journal integration
is exercisable only inside isolated Go tests; the existing pre-R6C-1
OCPP-J loopback fixtures remain test-binary-only.

**Durability is not reconciliation.** `StartIntent`, `StopIntent`
and `MeterIntent` mean an operation *may* have been sent. The journal
does not prove that the CSMS executed it. A lost ACK or `SIGKILL` leaves
the attempt pending, and reopening the database cannot send another
Start/Stop or announce Available. No automatic replay, resume, distributed
lease/fencing, CSMS query, schema change to `ocpp-csms` or live CSMS
integration is implemented here.

A transaction may remain recorded even after an explicit rejection if
its receipt could not be persisted; this is an intentional safety bias.
No component uses elapsed time to decide an unknown transaction closed.

## Offline acceptance

Run `go test -race -count=1 ./...`, `go vet ./...` and
`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build` within GitHub CI,
with the Python Debian 12/13 and isolated installer matrices preserved.
Tests cover direct ledger/SQLite transitions, real CSMS fixture ID,
write-before-OCPP send, battery RNG/meter snapshot, unknown Start/Stop ACK,
duplicate IDs and connector ownership, concurrent requests, failure injection
before commit, abrupt `SIGKILL` of a disposable child test process, and
reopen-with-quarantine. No production endpoint, remote node, manual
Ansible run, real charger, production process or service is exercised.

Remaining work **R6C-3**, requiring separate authorization: the
authoritative CSMS reconciliation/fencing contract and verified
`CleanVerified` transition. This increment is **NO-GO** for Ready or
deployment regardless of green offline CI.

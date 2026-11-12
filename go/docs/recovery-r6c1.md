# R6C-1 — Private SQLite recovery bootstrap and deny-by-default gate

**Status: implementation in Draft PR #20; NOT ready for deployment.**

Approved architecture: separate, private Go SQLite recovery metadata using
`modernc.org/sqlite v1.34.5` (pure-Go, compatible with Go 1.23, Linux ARM64
cross-build with `CGO_ENABLED=0`). This is not the OCPP-CSMS or Python
SQLite database. No existing database is opened or migrated.

## CLI and data layout

For **Go remote mode only**:

```text
--remote --connectors N --recovery-tenant TENANT --recovery-dir /private/absolute/path
```

The supplied recovery directory must be an exact absolute canonical path,
without symlink components, and must be `0700`. Private lock/database/WAL/SHM
files require `0600`. Files are keyed by SHA-256 over a canonical
tenant + CSMS endpoint + charge-point ID + protocol identity, without storing
credentials in the filename. A different connector count cannot reinterpret
the old database. Driver pool is limited to one connection; WAL and
`PRAGMA synchronous=FULL` are set and checked; `quick_check`, schema version,
file mode, symlink and hardlink validation are mandatory. Local
`flock(LOCK_EX|LOCK_NB)` prevents a second owner of the same local state.
Directory fsync covers the initial SQLite path creation.

A new DB starts `Quarantined`. Every successful open increments a
transactional, monotonic generation and reaffirms `Quarantined` before the
Go remote entrypoint is allowed to perform any I/O to the configured CSMS.
The current schema stores identity, generation, trust state and minimal
`Reserved`/`Starting`/`Charging`/`Stopping` uncertainty records
transactionally. This is the **bootstrap skeleton**, not a full outbox.
Recorded pending state can survive a normal restart, but live RemoteStart
and meter state are **not** yet wired to these records.

## Non-negotiable gate

In this stage there is **no CSMS reconciliation authority**, so
`SQLiteRecoveryStore.BeforeAvailable` always returns
`ErrRecoveryQuarantined`. Consequently, actual Go remote mode refuses
startup **before WebSocket connection, BootNotification, initial Available,
or RemoteStart**. A valid/empty SQLite database is not sufficient.

Old R1–R5 loopback tests use an unexported `testOnlyLoopback` switch that
requires both `testing.Testing()` and a loopback WebSocket address. The
production CLI cannot set this switch and the non-test executable does not
activate it. It is **not** a CSMS reconciliation implementation.

## Tests and failure behavior

Offline tests verify absent/new DB, private file modes, generation increment,
known/unknown pending state persistence, identity drift and connector-count
mismatch, corrupt database, directory symlink, read-only or wrong-mode files,
cross-process ownership via child `go test` process, rollback/fail-closed
write fault, simulated directory fsync error, conflicting uncertain IDs,
remote startup denial before any CSMS connection, and the former R6 unsafe
restart scenario now blocked. The Go race matrix, Linux ARM64 no-CGO build,
Python default and isolated Ansible installer remain required.

**Not implemented and not authorized here:** recording live Start/Stop
intent before send, authoritative CSMS read reconciliation, distributed
fencing, battery snapshot restore, automatic replay, safe reconnection,
automatic restart, any production CSMS integration or any schema migration
of the existing OCPP-CSMS database.

**NO-GO continues.** R6C-2 will need durable outbox integration into
`SessionManager` and `ConnectorLedger`; R6C-3 needs an independently
approved CSMS authority before any startup can be promoted to
`CleanVerified`.

# R6B — Crash-safe recovery design / architecture decision proposal

**Status: PROPOSED, NOT APPROVED FOR IMPLEMENTATION.**  
Date: 2026-11-11. Repository: `arthexis/ocpp-simulator`, PR #20,
`feat/13-g3-recovery`. The reviewed baseline is
`117aa3c467fed430a553052ceffe3111d6c20ceb`, with CI
[#43023883740](https://github.com/arthexis/ocpp-simulator/actions/runs/43023883740)
completed SUCCESS. The R6 findings remain **NO-GO** for Ready, deployment,
automatic restart or production.

This document is **design only**. R6B does not add a database, migrate a
schema, touch any existing SQLite file, modify service/restart settings,
implement CSMS extensions, change wire behavior or authorize field tests.

## 1. Evidence and why the two-system problem matters

The existing Go `SessionManager` creates new, empty connector slots and a
`ConnectorLedger` on every `Serve` call, and then
`AnnounceInitialAvailable` sends `Available` before any recovery query.
`TestR6ReproducesUnsafeFreshProcessAfterUnconfirmedTransaction` in
`go/internal/spike/recovery_r6_test.go` proves the failure with an isolated
OCPP-J server:

1. CSMS accepts `StartTransaction` and assigns transaction ID 9001.
2. The socket drops during `MeterValues`, leaving 9001 open at the CSMS.
3. A fresh Go `Serve` with the same charge-point ID initializes an empty
   in-memory ledger and announces connector 1 `Available`.
4. The old transaction is **still open**. A second unit-level negative test
   demonstrates loss of both `Starting` (unknown ID) and confirmed IDs when
   constructing a new `SessionManager`.

The Go R0–R5 tests cover admission bounds, ownership, RemoteStart/Stop,
per-connector seeded battery Wh/W/SoC, Soft/Hard reset, cancellation,
disconnect and offline OCPP-J frames. Their success is **not** a demonstration
of restart safety. R5's stderr JSON may be incomplete or lost on SIGKILL or
power failure.

Local durability and CSMS reconciliation solve **different** problems:

- The local store says what the simulator **intended, sent, observed, and
  acknowledged** across process restarts.
- The CSMS is authoritative for whether a transaction was actually created,
  is still open, or has closed. Neither a local fsync nor an OCPP timeout
  proves remote execution or non-execution.
- OCPP 1.6J Core has no standard query returning a definitive, generation-
  scoped inventory of all outstanding transaction IDs and unknown start
  attempts for a CP. A compatible **CSMS-side read-only reconciliation
  contract/extension**, or conservative manual quarantine, is therefore
  required. Do not assume `GetConfiguration`, `Heartbeat` or
  `BootNotification` can provide that evidence.

## 2. Storage alternatives

| Criterion | Append-only WAL/journal with fsync | Private local SQLite |
| --- | --- | --- |
| Atomic multi-row state + command outbox | Must implement framing, CRC/hash chain, replay order, compaction and cross-record invariants | Native transactions, constraints, queryable state |
| Commit durability | `write` + file `fdatasync/fsync`, directory fsync for create/rename; tolerate short writes and torn tail | WAL mode + `synchronous=FULL`, fsync parent on initial creation, transaction commit; verify filesystem |
| Corruption / partial writes | Explicit length/version/checksum per record; truncate only provably incomplete tail; quarantine interior gaps | `quick_check`/`integrity_check`, schema/version checks; fail closed on corruption |
| Unique CP/connector/tx ownership | Application-owned indexes and replay validation | `UNIQUE`/FK constraints and transactional compare-and-swap |
| Concurrent writers | Explicit OS lock, sequence checks, clock/generation fencing | SQLite single-writer + OS lock **and** CSMS-side fencing |
| Migration and retention | Custom format evolution and compaction protocol; do not prune pending events | Versioned schema migration and bounded history; never prune unresolved |
| Dependencies / maintenance | Simple nominal format, larger bespoke correctness surface | Standard, well-understood embedded DB; adds Go SQLite driver/build considerations |
| Failure mode | Corrupted/torn journal can produce ambiguous tail | DB corruption/WAL loss can produce ambiguous state; neither can prove CSMS state |
| CSMS ambiguity solved? | **No** | **No** |

**Recommendation for a future, separately authorized implementation:
private SQLite for Go recovery**, not the CSMS database and not the existing
Python simulator state. This minimizes custom recovery algorithms and makes
atomic state/outbox updates and uniqueness testable. This is a **proposal**,
not approval to create the file or pick a driver. Pin and evaluate a Go SQLite
driver compatible with the current Go 1.23, Linux ARM64, deployment and
CGO_DISABLED cross-build constraints. A pure-Go driver may be preferable but
must be assessed for security, locking, durability and maintenance; no driver
is selected/installed in R6B.

SQLite is only appropriate if the actual target filesystem provides
documented atomic rename, locking and fsync semantics. Reject NFS, FAT, tmpfs,
symlink redirects, unexpected ownership/modes, read-only media or unsupported
synchronous behavior rather than degrading silently. Proposed owner-local
state root (path illustrative, **not created now**):
`~/.local/state/ocpp-simulator/go-recovery/<opaque-identity>/`,
directory 0700, private files 0600, **outside immutable release trees**.
Never modify `ocpp-csms` SQLite or `forwarder.sqlite`. Do not infer that a
missing state file means it is safe to announce Available.

## 3. Identity, generation, exclusivity and trust boundaries

**Identity key** (fixed before touching the network):
`(tenant/source, configured CSMS endpoint identity, CP ID, protocol=1.6J)`.
Canonicalize the endpoint without leaking credentials; pin the expected CSMS
TLS/endpoint identity, forbid accidental cross-tenant state reuse, and store a
non-secret identity digest plus canonical comparison fields. A changed CP ID,
CSMS peer identity, tenant or connector count is a **migration/quarantine**
event, not an implicit new clean installation.

**Local generation**: monotonically increasing durable `generation` plus
per-attempt opaque `attempt_id`, recorded and synced **before** a remote
command can cause a wire-side action. Record the compiled Go revision and
schema version for diagnostics; these do not replace the identity key.

**Local exclusivity**: an owner-checked OS single-writer lock held from
startup through teardown, plus SQLite write transactions. No stale-lock
deletion based solely on PID; boot identifiers and lock ownership are
diagnostic only. Refuse simultaneous processes on the same state directory.

**Distributed exclusivity**: two different machines/processes can reuse a
CP ID while holding independent local locks. A CSMS-issued, authenticated
CP-identity lease/fencing token or equivalent server-side single-owner
session fencing must be designed separately. Without it, enable neither
automatic restart nor Ready. OCPP 1.6J has no standard fencing token in
`StartTransaction`; do not claim this can be enforced by local `flock`.

**Trust**: a new/empty/corrupt/unreadable local store, a changed CSMS identity
or lost generation has trust state `Quarantined`, even if no local open
transactions are visible. Initial clean activation requires authoritative,
CP-scoped confirmation from the CSMS **or** explicit operator reconciliation
in a separately approved workflow. Never reset the trust state with a CLI
`--force`, by deleting the DB, by changing CP name, or by restarting.

## 4. Proposed persistent records and state machine

Proposed logical tables/records (illustrative schema, **not SQL migrations**):

- `identities`: identity digest, CP/tenant/endpoint fingerprints,
  connector count, schema version, `generation`, lease/fence ID,
  `trust_state` (`Quarantined|Reconciling|CleanVerified`),
  last reconciliation revision and timestamp, local revision.
- `connector_sessions`: identity, connector (1–128), local attempt UUID,
  `state`, CSMS transaction ID nullable, idTag fingerprint (not raw RFID),
  frozen stop reason, last committed Wh/W/SoC and battery RNG state,
  local revision; unique live session per connector and globally unique
  (CSMS identity, transaction ID) when non-null.
- `command_intents`: attempt UUID, command kind, local monotonic sequence,
  pre-send persistence marker, wire-state `NotSent|MayHaveSent|Acked|
  ExplicitlyRejected|Unknown`, CSMS ID when known, ACK identity/fingerprint,
  wire-message correlation ID if available, creation and outcome evidence.
  Ambiguous requests stay `Unknown`; IDs are never fabricated.
- `reconciliation_evidence`: trusted CSMS snapshot revision/watermark,
  scoped identity/connector IDs, authenticated peer, returned transaction
  status, decision and operator override provenance where authorized.
- `battery_snapshots`: per-connector committed energy, SoC, seed/RNG and
  cumulative metering revision, paired atomically with the corresponding
  command/meter intents. Never derive recovered Wh solely from wall clock.

State diagram (logical, not an implementation assertion):

```text
Quarantined ──authenticated reconciled clean + fenced epoch──> AvailableEligible
        ▲                                  │
        │                                  ▼
        └──────── any uncertain outcome   ReservedDurable
                                           │
                               StartIntentDurable
                                           │
                                     StartingUnknown
                                        /       \
                             explicit reject   authoritative CSMS ID
                                    │                  │
                          cleanup only if safe    ChargingKnown
                                                       │
                                             StopIntentDurable
                                                       │
                                                 StoppingUnknown
                                                  /          \
                                             lost ACK      confirmed/CSMS closed
                                                │                │
                                           Quarantined      ClosedVerifiedDurable
                                                                  │
                                                  gated AvailableEligible
```

The names above are **proposed persistent recovery states**, not a claim
that `SessionManager` already implements them. Preserve R1–R5 transient
states for runtime compatibility, but recovered state is *not*
`SessionAvailable` until verified.

**Mandatory invariant I0 (startup gate):** no `StatusNotification(Available)`,
RemoteStart admission, remote battery advance, or claim of released connector
for identity/connector C unless a durable, generation-bound
`CleanVerified` decision is committed, the appropriate CSMS lease is held,
and there are no unresolved attempts for C. If reconciliation is
unavailable, fail closed and do **not** simply report Available.

**Invariant I1:** commit `ReservedDurable` and an attempt ID before
returning RemoteStart `Accepted` or placing its job in the worker queue.
A local crash may abandon an accepted request; it does **not** authorize
automatic re-execution without a new explicit command.

**Invariant I2 (write ahead):** commit `StartingUnknown`/StartIntent
**before** writing `StartTransaction` to OCPP. A timeout or crash at any
point before authoritative confirmation remains **unknown**, not rejected.
On a definitive CSMS transaction ID, synchronously commit the ID *before*
emitting Charging or meters.

**Invariant I3:** persist `StopIntentDurable` and reason before the first
`StopTransaction` frame. After lost acknowledgement, **do not blindly
resend** `StopTransaction`: resolve with the CSMS's authoritative status
and an explicit server-side idempotency contract. Never invent a stop ACK.

**Invariant I4:** a durable `ClosedVerified` decision (CSMS outcome plus
transaction ID and generation) must precede any new Available announcement.
StatusNotification delivery/ACK and local state transition must also tolerate
a crash in either order; a missing Available ACK is unknown rather than
proof of the CSMS receiving it.

**Invariant I5:** no two live attempts may own the same connector; no two
connectors may own the same CSMS transaction ID in one CSMS identity. A
RemoteStop matches the exact immutable CSMS ID and may only cancel its owner.
Duplicate requests cannot create duplicate StopTransaction sends.

**Invariant I6:** meter and battery state updates are durably monotonic
per session. When MeterValues may have reached the CSMS without local ACK,
reconciliation determines the safe next value; never restart battery at
initial SoC or reset a transaction's meter to zero invisibly.

**Invariant I7:** any durable write error, timeout, identity mismatch,
rollback or corruption immediately freezes admissions and transitions to
Quarantined; a transaction MUST NOT be treated as closed because logging,
persistence or emission of a status failed.

**Invariant I8:** Reset Soft/Hard, SIGINT/SIGTERM, SIGKILL, power loss and
executor restart use the same startup recovery gate. ResetConfirmation is
not proof of flush or physical reset. The guard is enforced before and after
`BootNotification` where required; Boot Accepted alone is not clean status.

## 5. Startup handshake and CSMS reconciliation contract

A sufficient **future** authoritative CSMS read contract must return, for a
specific authenticated `(tenant, CP ID, CSMS identity, lease/epoch)`:

1. Active, stopped, and unknown/ambiguous CSMS transaction IDs per connector,
   with authoritative status and an immutable or monotonic snapshot revision.
2. Resolution of a pending StartTransaction that has no recorded ID. This may
   require CP-attempt correlation added at the CSMS boundary; OCPP 1.6J
   alone does not guarantee that a lost StartTransaction response can be
   matched to a transaction. If unsupported: quarantine/manual reconciliation.
3. The status of a StopTransaction whose OCPP response was lost, including
   whether its stop was applied and a monotonic revision. If unsupported:
   quarantine/manual reconciliation, not blind replay.
4. A CSMS-side exclusive owner generation or session fencing mechanism that
   prevents stale duplicates from sending commands for the same CP.
5. An authenticated read-only snapshot and an atomic or revision-checked
   process for committing a `CleanVerified` decision; if the snapshot
   changes before the connection is fenced/claimed, retry or quarantine.
6. Evidence expiry and clock-skew policy. Timeouts/TTL are **not** permission
   to assume a stale transaction closed.

Proposed flow (**not currently implemented**):

```text
load verified local identity and lock
    -> acquire/fence identity epoch with CSMS
    -> restore all unresolved local attempts as Quarantined
    -> obtain authenticated revisioned CSMS snapshot
    -> resolve known and unknown Start/Stop windows
    -> commit reconciled state + evidence + lease/generation durably
    -> only then announce each verified-clean connector Available
    -> enable RemoteStart/RemoteStop admission under the lease
```

A partial success for connector 1 must not silently unlock connectors
2–N. Per-connector release is allowed only if the CSMS contract proves a
consistent scoped snapshot and the local schema guarantees isolation.
Simpler initial rollout: **whole CP remains quarantined** until all its
connectors are resolved.

No CSMS extension, permission, token, or API implementation is authorized
here. Lack of this read contract is a **hard release blocker** even if local
SQLite durability is perfect.

## 6. Failure and replay decisions (explicit non-guarantees)

| Crash / failure boundary | Durable evidence expected | Safe R6C behavior |
| --- | --- | --- |
| DB absent or old release without trusted state | No authoritative clean history | Quarantine, require CSMS clearance |
| DB damaged, read-only, ENOSPC, fsync fails | Cannot trust last commit | Quarantine, no OCPP Available/Accepted |
| RemoteStart admitted, not yet sent | Committed reservation, attempt ID | Do not silently execute after restart |
| Crash before Start frame | StartIntent may already exist | Treat as uncertain; reconcile or quarantine |
| Start sent, reply lost | StartingUnknown, no CSMS ID | CSMS lookup using attempt correlation or manual stop |
| Start ACK received, DB commit fails | Known ID only in volatile memory | Freeze and quarantine; do not fake rollback |
| Stop requested/queued | Frozen reason and owner, stop intent | No second send until reconciled |
| Stop sent, reply lost | StoppingUnknown, exact transaction ID | Query CSMS; no duplicate blind Stop |
| Stop ACK received, DB commit fails | May be closed remotely, unknown locally | Reconcile and persist before Available |
| Closed persisted; Available send/ACK lost | Verified closed, status delivery unknown | Reconcile status and reissue only if safe |
| MeterValues sent and ACK lost | Persisted Wh/SoC and send intent | Reconcile latest accepted meter before advancing |
| Hard Reset during Start/Stop | Persisted pre-send intents and generation | No invented receipts; quarantine unresolved |
| Soft Reset / signal during in-flight write | Snapshot of outstanding attempts | Bounded drain and restart gate |
| SIGKILL / power loss / torn write | Last valid durable prefix/transaction only | Fail closed; verify consistency before any send |
| Supervisor restarts immediately | Persistent single-writer and CSMS fence | Must still block before Boot/Available |
| Two machines reuse same CP identity | Different local locks | CSMS distributed fence required |
| CSMS unavailable or snapshot changes | No stable authoritative revision | Remain quarantined indefinitely |
| CSMS lease expires during charging | No current ownership authority | Stop admissions, mark unknown; coordinate shutdown |
| Connector count / tenant / endpoint changes | Identity digest mismatch | Explicit migration; quarantine by default |

The write/CSMS boundary is not an atomic distributed transaction. The design
must handle every uncertain network window by **quarantine and reconciliation**,
not claim exactly-once side effects without a server idempotency mechanism.

## 7. Security and operations

- One owner, non-root, private directory, canonical path, no symlink or
  unexpected hardlink, secure temp and directory handling, no use of global
  `/tmp` for state. Check disk, filesystem and mount properties at startup.
- Never persist Basic Auth passwords, bearer tokens, full RFID, or arbitrary
  customer data in the journal. Prefer a scoped non-reversible RFID
  fingerprint and an opaque attempt UUID; redact logs. Any need for secrets
  must be designed and approved separately.
- Keep state outside immutable release directories and outside the
  `ocpp-csms` database; do not make deployments/rollback delete or reset
  recovery state. A downgrade unable to read schema must quarantine.
- Set `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL` if SQLite is
  approved; verify actual settings, exclusive process ownership, migrations,
  WAL/SHM durability and compatible Go driver in offline tests. DB backups,
  recovery and WAL checkpoint policies must preserve unresolved attempts.
- Do not use SQLite `busy_timeout` as substitute for distributed fencing.
  Bound local operations and fail closed on exceeded limits.
- Retention: never prune unresolved sessions, unknown command intents or
  fencing/identity evidence. Prune confirmed historical records only after
  an audited retention horizon and CSMS reconciliation; retention must not
  erase the proof that an identity has previously run.
- An `auto-restart` service is **not enabled by this design**. A future
  supervisor may invoke a gate-check that can only start a quarantined
  process, never auto-admit remote commands; enabling restart is separately
  approved after durability and reconciliation tests.
- No need for another repository, external broker or generic rollback
  framework. Implementation should remain local to the existing Go backend
  and the one Draft PR, using minimal dependencies.

## 8. Acceptance tests to implement in separately authorized R6C

All tests are offline: ephemeral disk directories, local SQLite/journal
files (once approved), isolated OCPP-J CSMS test doubles, and disposable
child Go processes. They must run in CI only, never on a live charger.

**Contract/negative tests (write these first):**

- R6 existing negative restart tests must be inverted by a startup guard:
  a fresh `Serve` must **NOT** announce Available with tx 9001 still active.
- Unknown-ID Starting and known-ID Charging/Stopping survive a cold process
  restart as quarantined state. Deleting the state directory is not a
  shortcut to declaring clean.
- Explicit untrusted/missing/corrupt schema, mismatched CSMS identity or
  connector count, lost CSMS query, lease expiry and duplicate process/machine
  all deny `Available` and RemoteStart.
- RemoteStart `Accepted` cannot occur before a persisted reservation; a
  crash after Accepted but before Start must never autonomously replay Start.
- Lost StartTransaction ACK and a known ID returned late after Reset are
  correctly classified, not silently removed or fabricated.
- Lost StopTransaction ACK, retransmission races and duplicate RemoteStop
  require authoritative CSMS resolution; a second Stop is never assumed
  safe solely from an unchanged local state.
- Persisted battery SoC/seed, session-relative Wh and in-flight MeterValues
  survive restart and do not regress or double-count.
- Data faults: ENOSPC, EIO, short-write, fsync failure, WAL loss/corruption,
  incomplete migration, torn final journal tail (if chosen), permissions,
  symlink, read-only media and checkpoint/compaction failure remain
  fail-closed; no silent fallback to memory-only mode.
- SIGKILL/abrupt child-process kill at each write-ahead and ACK boundary;
  restart with the same CP identity and a retained fixture CSMS transaction,
  assert no false Available and no duplicate Start/Stop.
- Local double-start and independent-machine same-CP simulations must be
  rejected by local and CSMS fencing respectively.
- Soft/Hard Reset, SIGINT/SIGTERM and timeout need bounded termination and
  durable unresolved-state records, with no imaginary CSMS receipts.
- Existing R0–R5 Go `-race`, vet, ARM64 cross-build, Python-default and
  local-runner installer tests remain green. No Python behavior change.

**Acceptance evidence**: exact commit SHA, real `go test -race -count=1
./...` and CI run IDs, fail-before/fix-after tests for the original unsafe
restart, CSMS reconciliation trace, disk fault matrix, explicit residual
limitations and release gate decision. Simulator cannot be marked Ready if
only happy-path tests pass.

## 9. Incremental implementation plan — not authorized by R6B

| Stage | New work (future authorization required) | Exit criterion |
| --- | --- | --- |
| R6C-0: decision | Approve either private SQLite or append-only journal, driver/FS and CSMS reconciliation/fence dependencies | Written ADR approved; no implicit storage choice |
| R6C-1: startup gate | Fail-closed `Quarantined` lifecycle and explicit identity/CP locks, before **any** Available or admission | Negative restart test changes to expect zero false Available |
| R6C-2: durable ledger | Transactional state/outbox; schema version, generation, per-connector energy and intents; fault injections | Crash-before/after every write+network edge retains uncertainty |
| R6C-3: CSMS reconciliation | Read-only snapshot contract, fencing and verified-close/clean decisions; manual quarantine if absent | Uncertain IDs resolved only with authoritative evidence |
| R6C-4: restart acceptance | SIGKILL, torn write, lost ACK, two-process and 128-connector stress, remote stop and battery replay | Offline integrated CSMS never sees duplicate Start/Stop or false Available |
| R6C-5: release decision | Full Go race/vet/ARM64, Python/installer parity, independent review, separately authorized isolated field validation | New readiness decision; **not automatic Ready/merge** |

**R6B exit criterion:** this design is reviewed and documented, CI
validates the documentation-only change, and the proposed storage/CSMS
dependencies are presented for an explicit decision. **R6B does not close
R6, mark Ready or change any running system.**

## 10. Decision record (pending)

- ADR-R6B status: **SQLite direction APPROVED for R6C-1 only**; CSMS reconciliation and fencing remain unapproved/unimplemented.
- Preferred option: **private local SQLite with WAL + FULL sync** for the
  Go simulator, subject to driver/FS validation. Alternative: append-only
  journal with length+CRC+fsync and replay/compaction correctness tests.
- Both need a CP-identity-scoped authoritative CSMS reconciliation and
  fencing contract; local disk cannot answer "did the CSMS act?".
- Startup default: **Quarantined**, including missing/empty local state;
  do not send Available until reconciled.
- Automatic restart: disabled pending full future tests and approval.
- Scope: Go only, Python default unchanged, immutable Ansible deployment
  unchanged, no production activity, no credentials/services/SQLite changes.
- Next action: choose the store **and** agree how the CSMS can provide
  authoritative reconciliation/fencing. If this contract cannot be
  provided, retain NO-GO and require manual recovery.

## Implementation note — R6C-1

The user approved the SQLite storage direction only for the first fail-closed
increment. See [R6C-1 scope](recovery-r6c1.md). A private SQLite state
bootstrap and an unconditional quarantine gate do not authorize production
activation or any CleanVerified transition. The CSMS-side reconciliation
contract remains a separately gated dependency.

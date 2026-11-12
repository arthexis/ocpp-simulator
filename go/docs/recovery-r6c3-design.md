# R6C-3 — CSMS authoritative reconciliation and fencing pre-acceptance design

**Status: DESIGN / READ-ONLY AUDIT ONLY — NOT APPROVED TO IMPLEMENT.**
**Date:** 2026-11-11. Scope: `arthexis/ocpp-simulator` PR Draft #20,
`feat/13-g3-recovery`. Reviewed simulator HEAD:
`d795383ffa93d108a8c29378a65a234ae3d73350`, CI
[#43050476286](https://github.com/arthexis/ocpp-simulator/actions/runs/43050476286)
`SUCCESS`. Read-only CSMS baseline: `arthexis/ocpp-csms` `main`
`77452e39efb1702cc78c753724d1d30892ae411d`.

**Decision: NOT READY for CleanVerified, RemoteStart admission, reconnect,
automatic replay, Ready, merge or deployment.** No code, endpoints, schema,
permissions, CSMS service, network connections or production data were
modified in either repository by this design phase. The regular Go remote
`BeforeAvailable` gate must remain unconditionally `Quarantined`.

## 1. Exact existing contracts — evidence vs authority

The current CSMS already provides useful persisted observations, but
**none is a generation-fenced release certificate**:

| Existing code and invariant | What it can demonstrate | What it CANNOT demonstrate |
| --- | --- | --- |
| SQLite schema v8, `transactions` with global numeric `transaction_id`, `charger_id`, `connector_id`, `state`, `stop_received_at` | Server-side record of starts and received stops | No tenant-scoped owner epoch, live CP exclusivity or definitive physical connector availability |
| `EventStore.commit_start` uses `BEGIN IMMEDIATE`, increments `transaction_allocator`, inserts transaction, `start_identities` and event before handler sends reply | A StartTransaction payload reached an authoritative SQLite commit and was assigned a real CSMS ID | That the caller received the ACK, or that a **particular local attempt_id** caused it |
| `start_identities` primary key (`charger_id`, canonical `payload_json`) | Exact-payload retries can map to the same ID across CSMS restart | Distinct legitimate sessions whose serialized payload is identical cannot be distinguished; changed timestamp is not a stable retry key; there is no Go `attempt_id` or OCPP-J call ID persisted in this table |
| `commit_transaction_evidence` writes MeterValues and StopTransaction events atomically; the first matching `StopTransaction` with `stop_received_at IS NULL` closes the row | A matching StopTransaction was received and durably processed for this CP/ID; later duplicate stop receipts do not overwrite the first stop | An unknown/orphan Stop implies a matching transaction was closed; a lost ACK can be inferred as delivered |
| `TransactionQuery.active` reads `state='open' AND stop_received_at IS NULL` from the authoritative SQLite transaction summary in one local transaction | Historical open rows for a chosen CP/connector, and matching persisted stop evidence | That an open row represents current physical charging, or that **absence** of a row is proof no in-flight old WebSocket frame can commit a Start later |
| `ocpp-csms reconcile` CLI, `appliance_status` and `connector_status` | Read-only comparison of observed connector status and historical open IDs; detects overlap and conflicts | It explicitly performs no live charger query, transaction closure or correction and cannot grant `CleanVerified` |
| `CSMSServer._active_sessions` and WebSocket `session_id` evidence | Current in-memory CP session and event-to-session correlation | A durable distributed owner epoch, a transaction-level attempt binding, or fencing of already buffered OCPP frames; current replacement closes the previous socket but does not persist an epoch |
| `source_id` in `appliance_metadata` | Stable per-CSMS-data-store identity for this source | A tenant ID or distributed lease; changing upstream data source must invalidate a stored recovery authorization |
| OCPP WSS Basic authentication | Optional CP identity verification on WS upgrade where configured | Tenant-scoped authorization for a new recovery API, session-generation proof or fencing token |

### Audited source references (immutable CSMS commit)

- [schema v8: transactions, start_identities, allocator and boundaries](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/schema.py)
- [EventStore commit_start and StopTransaction evidence](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/evidence/store.py)
- [handler ACK after SQLite commit](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/session/handlers.py)
- [TransactionQuery active and snapshot](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/transactions/query.py)
- [read-only CLI reconcile; never proof of closure](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/cli/inspection.py)
- [WebSocket supersession, session_id and live registry](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/server.py)
- [control socket dispatcher has commands but no recovery certificate](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/control/dispatch.py)
- [node/source identity in SQLite metadata](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/src/ocpp_csms/schema.py)
- [exact-payload retry contract tests](https://github.com/arthexis/ocpp-csms/blob/77452e39efb1702cc78c753724d1d30892ae411d/tests/evidence/test_sqlite_authority_contract.py)
- [local R6C-2 durable intent state](recovery-r6c2.md)

OCPP 1.6J `StartTransaction` carries `connectorId`, `idTag`,
`meterStart` and `timestamp`; the CSMS returns `transactionId`.
It carries no standard arbitrary `attempt_id` or recovery epoch. The
OCPP-J envelope has a per-call `UniqueId`, but current CSMS persisted
`start_identities` does not include it. `GetConfiguration`,
`Heartbeat`, `StatusNotification` and the existing `reconcile` CLI cannot
be substituted for an authoritative transaction-inventory/recovery
protocol.

**Critical subtlety:** the current `transaction_boundaries` view can
derive a `next_start_received_at`. That is **not** a real StopTransaction
receipt. Never treat that derived boundary as a closed transaction.
Similarly, the `connector_status` value `Available` is not a stop proof.

## 2. Required trust model and identity

Each decision is scoped to the tuple:

`(tenant_or_source_scope, CSMS_source_id, expected_CSMS_peer,
  OCPP_protocol=1.6J, CP_id, connector_count, local_generation,
  CSMS_owner_epoch)`.

- CSMS `source_id` and CP ID exist today, but first-class **tenant scope**
  is **not** present in the transaction primary key. Source/tenant mapping
  must be explicit and verified before granting any authority.
- Source_id, CP ID, peer certificate/verified endpoint and connector count
  must all match the local SQLite recovery identity. Any change ⇒
  `Quarantined`. Redacted credentials never appear in the response.
- A local SQLite generation is **not** a CSMS owner epoch. An authenticated
  server-side epoch must be allocated and enforced before any new writer can
  affect the CP.
- Local `flock` prevents one host from running duplicate writers; it
  cannot prevent another host from using the same CP identity.
- If CSMS is deployed behind multiple instances or has replicated
  persistence, the fencing decision must be linearizable across all
  writers; an in-memory mutex or "latest connection wins" is inadequate.

### Threats to defend against

Old WebSocket buffered frames after replacement; two processes on
different hosts with the same CP ID; stale or forged recovery snapshots;
credential reuse across tenants; a CSMS restore that reuses source_id
but rewinds the revision; change of endpoint or connector count; replayed
or out-of-order Start/Stop payloads; an ACK lost after durable CSMS
commit; a local SIGKILL between intent and network send; a local database
rollback or missing WAL; manual deletion/recreation of local state; CSMS
restart between snapshot and new ownership; status events that report
`Available` while a transaction is still open; unknown/orphan frames
and transaction-boundary inference.

Unavailability, uncertainty, contradiction and unsupported capabilities
must fail closed rather than selecting the most optimistic observation.

## 3. Future interfaces — **contract proposals only**

**Do not add these endpoints, commands or database columns in this PR
stage.** Names and shapes below are illustrative; choosing transport,
authentication and permission scopes is a separate approval.

### A. Authoritative, read-only recovery snapshot

Proposed request:

`recovery.snapshot(source_id, tenant, cp_id, local_generation,
                   previous_owner_epoch?, known_attempt_ids?,
                   known_transaction_ids?)`

Response shape:

`schema_version, source_id, tenant_scope, cp_id, connector_count,
server_boot_identity, server_database_identity, owner_epoch,
snapshot_revision, snapshot_consistency, snapshot_expires_at,
current_owner_identity, lease_state,
transactions[{connector_id, transaction_id, state, start_event_id,
              stop_event_id?, stop_received_at?, last_meter_event_id?}],
attempt_resolutions[{attempt_id, result, transaction_id?,
                     start_event_id?, stop_event_id?, proof_kind}],
unresolved_orphans, conflicts, coverage`.

Required semantics:

1. All rows belong to a **single committed SQLite snapshot** scoped to
   this source and CP, with a monotonic revision/watermark of transaction
   and ownership mutations. The existing `events.id` is a useful ordering
   observation, **not yet an owner-fenced revision**.
2. Result is exhaustive for all connectors 1..N, with explicit
   `unknown`/`unmatched`/`conflict` statuses. Empty arrays alone are NOT
   proof that all in-flight attempts were accounted for.
3. `closed` is provable only by the authoritative matching durable stop
   update/receipt. `open` is authoritative for **CSMS ledger open state**,
   but not necessarily physical charger activity.
4. An orphan StopTransaction remains an orphan observation; derived
   next-start boundaries are never promoted to stop evidence.
5. Start attempts without an ID cannot be marked `never_created` merely
   because no current transaction matches a payload: first fence and
   drain all older writers/in-flight commits, then prove a complete
   correlation range. If the CSMS lacks such a proof, return
   `ambiguous` and require manual quarantine.
6. Read transport must authenticate the intended identity, authorize
   read-only access to that tenant/source/CP, reject replay/expired
   credentials, and bind the snapshot to a server instance and durable
   revision. A signed response is optional if mutually authenticated
   transport already provides integrity; either way provenance is
   mandatory. A CLI read against SQLite is not itself a remotely
   authenticated certificate.

### B. Separate fencing/owner claim (**mutating CSMS operation**)

A read-only snapshot can **never** exclude a late StartTransaction that
arrives immediately after the read. A future owner-claim operation must
atomically:

- Increment a durable CP owner `epoch`/`fencing_generation` and set the
  current approved writer/session identity for that CP and source.
- Prevent **all** handlers from older epochs from committing Start,
  MeterValues or Stop operations; check the durable fence at the
  **same transactional commit boundary** as the evidence mutation.
- Drain/abort buffered old OCPP frames and in-flight commands with
  outcome classifications; no frame from a stale session may race
  the clean-snapshot decision.
- Require proof of the granted owner epoch at WebSocket upgrade or
  binding to the server-side session. OCPP 1.6J does not transport a
  standard owner token in every StartTransaction; the server must bind
  the validated epoch to the authenticated connection and enforce it
  on every mutating handler.
- Define lease expiry, renew/revoke, server crash, lease stealing,
  CSMS failover and split-brain semantics, and guarantee stale tokens
  cannot regain authority. TTL expiry does not close transactions.
- Bind the **post-fence snapshot** to the epoch and revision, so the
  local journal's `CleanVerified` decision cannot use an earlier
  snapshot.

This is **not** a read-only operation and requires **separate explicit
authorization** to implement. None exists today in `ocpp-csms`.

### C. Attempt correlation and unknown ACK handling

The R6C-2 Go journal has a random, durable `attempt_id` per local
RemoteStart. Current CSMS persists only exact `StartTransaction`
payload JSON as a retry identity, with no Go attempt ID, no call
`UniqueId`, and no owner epoch.

Two possible future approaches require proof and explicit selection:

1. **CSMS-side attempt binding**: a separately authenticated,
   epoch-scoped pre-registration/handshake binds a Go `attempt_id` to a
   particular future OCPP-J call. Correlation must be unambiguous,
   enforceable on the receiving WebSocket and committed atomically
   with the transaction allocator. If library access to the OCPP-J
   call ID is unavailable, this route requires a reviewed transport
   extension, *not* pretending `timestamp` or `idTag` is unique.
2. **Conservative fallback**: use existing exact-payload lookup only as
   supporting evidence for positive matches; retain all unbound
   StartIntent entries as `ambiguous`. Keep the CP quarantined until a
   separately approved, authenticated operator recovery proves a
   safe outcome. **No automatic start/stop retry**.

A StopIntent with a **known CSMS transaction ID** can be resolved as
`stopped` only from durable matching `StopTransaction` receipt/row,
under the expected owner epoch; an absent stop receipt is `unknown`
and does not authorize a second StopTransaction. The local journal's
`StopAcked` is stronger local evidence of a response received; the
CSMS receipt is authoritative about committed stop state even if
the reply was lost. A MeterValues intent with an uncertain ACK
requires event-level correlation before advancing Wh/SoC; missing
data does not permit resetting cumulative meter readings.

**Neither exact-once delivery nor automatic restart is claimed** by
these draft contracts. Safe behavior under missing correlation is
indefinite quarantine or explicit, separately authorized manual
resolution.

## 4. State matrix and release gates

| Local persisted state | Required CSMS evidence | Candidate decision (future only) |
| --- | --- | --- |
| Missing/empty DB; corrupt/foreign identity | No trusted history | `Quarantined`; never fresh-start by inference |
| `Reserved`, no wire send positively proven | Current fenced ownership; no prior ambiguous attempts | May mark local attempt abandoned **only** with complete coverage; no replay |
| `StartIntent`, unknown CSMS ID | Attempt-bound positive/negative evidence **after draining stale writers** | Positive: record real tx ID; negative: abandon with proof; ambiguous: quarantine |
| `ChargingKnown` | Exact CSMS tx ID/CP/connector still open | Preserve ownership and battery state; do not announce Available |
| `MeterIntent` with lost ACK | Matching committed meter event or proven not applied | Do not advance meter without resolution; no double-count |
| `MeterAcked` | CSMS ID still open or matching stop | Keep transaction; metering alone is not proof of closure |
| `StopRequested` | Exact tx still owned | Keep stop reason; do not generate a duplicate stop |
| `StopIntent`, lost response | Matching authoritative stop receipt for same CP/tx | Matching stop ⇒ verified stopped; otherwise quarantine |
| `StopAcked` | Matching durable CSMS stop evidence | Candidate closed; still requires lease, snapshot and local durable commit |
| `Closed` | Post-fence no open IDs on any relevant connector, matching source/revision | Candidate `AvailableEligible` only after **all** guards |
| CSMS row `open` but CP reports Available | Contradictory observation | `Conflict` → quarantine; never overwrite CSMS row |
| CSMS row `stopped` with unknown/outdated ID | Must link known local attempt and exact CP | Unknown ⇒ quarantine or controlled operator action |
| Two CSMS open IDs on one connector | Overlap conflict | Quarantine entire CP until resolved |
| Stale owner epoch, expiring lease, snapshot drift | Owner-fence failure | Revoke eligibility; deny new admissions |
| CSMS unavailable or read query fails | No snapshot | Quarantine indefinitely |
| R6C-1 legacy `recovery_pending` rows | Explicit migration/reconciliation per row | Do not silently drop or claim cleared |
| Soft/Hard Reset or SIGKILL | Durable intents plus post-fence CSMS evidence | Same quarantine/reconciliation gate as cold boot |

**Global `CleanVerified` predicate (not implemented):**

`identity_match AND authenticated_CSMS_source_match AND
fenced_owner_epoch_active AND snapshot_is_consistent_post_fence AND
snapshot_revision_is_current AND every_connector_accounted_for AND
no_unresolved_attempts_or_orphans_or_conflicts AND
all_known_stops_have_matching_receipts AND
all_unknown_starts_have_binding_or_manual_clearance AND
durable_local_clean_commit_succeeded`.

Failure of **any** term denies `StatusNotification(Available)`,
RemoteStart, battery advance and any automatic reconnect. For the first
accepted implementation, treat the CP as one indivisible quarantine
domain rather than independently releasing a subset of connectors.

**Critical ordering:**

`quarantine -> authenticate and claim/fence CP (future) ->
drain old handlers -> inspect one revisioned CSMS snapshot ->
resolve every local state -> durable local reconciliation evidence ->
validate epoch still current -> only then grant CleanVerified ->
initial Available -> enable RemoteStart`.

A snapshot before fencing is non-authoritative for release. A
generation/time value by itself is not a permission grant.

## 5. Proposed future test matrix — offline only

All new tests would use disposable SQLite directories and in-process
or child-process CSMS/Go fixtures. They must neither contact a real
CSMS nor reuse any production database.

| Test | Required safe assertion |
| --- | --- |
| Lost StartTransaction reply after CSMS commits ID | Exact local attempt resolves to real ID only with proven binding; else `ambiguous` |
| Identical StartTransaction payload from two *legitimate* sessions | Never collapse into one attempt or accept payload fingerprint as perfect correlation |
| Timestamp changes on retry | No duplicate Start on automatic recovery |
| StartIntent on disk, no frame sent, SIGKILL | No autonomous replay; clean requires fenced completeness proof |
| Known Stop ID with lost StopTransaction reply | Matching committed server stop may verify closure; no blind resend |
| Stop orphan for unrelated CP or ID | Evidence retained, cannot clear any local owner |
| Overlapping CSMS open transactions per connector | Quarantine, even if StatusNotification says Available |
| First stop confirmed, duplicate stops logged | First authoritative stop remains; no duplicate local stop emission |
| Meter ACK lost and battery seed restored | Energy/Wh/RNG monotonic, no double advance |
| Two Go processes, different hosts, same CP | Stale owner cannot commit or be admitted after server fence |
| Old WebSocket buffered Start concurrent with owner transfer | Commit rejected by epoch check, or snapshot cannot be issued |
| CSMS restart/failover and epoch/DB revision rollback | Recovery denied; old snapshot and lease invalid |
| Tenant/source/CP/connector count/cert mismatch | Reject snapshot/claim; no cross-tenant leakage |
| Missing, corrupt, partial or untrusted local SQLite | No CleanVerified; no network connection before gate |
| Snapshot revision changes during local commit | Retry from quarantine; no Available |
| CSMS read-only query unavailable or incomplete coverage | Fail closed; no fallback to `reconcile` CLI or OCPP status |
| Unknown attempt with no stable call correlation | Manual quarantine, not assumed absent |
| 128 connectors, concurrent Start/Stop, reset and SIGKILL | Durable ownership, stable ordering, no duplicate sends |
| Existing R0–R6C-2 CI | Go race/vet/ARM64, Python 3.11/3.13, isolated installer remain green |

Tests for an actual `CleanVerified` transition are **blocked** pending
CSMS contract approval and implementation. Do not add a fake positive
authority or a test-only production bypass.

## 6. Incremental plan and explicit acceptance boundaries

**R6C-3A — contract proof (next separately authorizable step, offline):**
design and test a **read-only CSMS snapshot adapter** over a temporary
SQLite v8 fixture; demonstrate exhaustive CP+connector inventory,
transaction receipts, orphan/conflict classification, source_id and
a local snapshot watermark. It may *not* assert fencing, start
attempt resolution or CleanVerified. No production endpoints, active
service queries, leases or schema changes. Negative tests prove that
lack of an epoch/correlation forces quarantine.

**R6C-3B — CSMS authority proposal (separate approval, separate repo):**
review whether the CSMS can enforce owner epochs at every OCPP evidence
commit, correlate new attempts without breaking OCPP 1.6J, and provide
an authenticated post-fence revisioned snapshot. Requires explicit
authorization for CSMS schema, handler, control transport and
credentials, plus compatibility tests for existing chargers. Until
approved, CSMS stays unchanged.

**R6C-3C — simulator reconciliation consumer (separate approval):**
verify source/tenant/CP/epoch, read/reconcile all durable local
uncertainties, write immutable decision evidence to the *private* Go
SQLite database, and permit `CleanVerified` only against an authorized
server-side claim. Negative tests for stale leases, timeouts and
identity mismatch are mandatory. No automatic replay.

**R6C-4 — restart and fault acceptance (later approval):**
SIGKILL, WAL corruption, split-brain, lost ACKs, concurrency,
battery/meter continuity, failures during fence transfer, Go -race,
Linux ARM64 and isolated CSMS E2E. Review and production rollout
remain separate gates.

**R6C-5 — independent readiness decision (later approval):**
full CI, peer review and explicit isolated field validation before any
Ready/merge/deployment, never inferred from docs or green CI alone.

## 7. Pre-acceptance verdict

- **Observed and usable:** CSMS SQLite v8 authoritative persisted
  Start/Stop receipts; exact payload retry index; stable data-source
  metadata; per-session diagnostic UUID; read-only transaction query.
- **Not usable as release authority:** `reconcile` CLI;
  `StatusNotification(Available)`; transaction boundary view;
  exact-payload lookup without a validated local attempt binding;
  old-WebSocket supersession without per-commit fence.
- **Not present:** tenant-scoped, authenticated complete snapshot
  with monotonic owner epoch/revision; distributed single-writer
  fencing; authoritative resolution of StartIntent with missing ACK
  and no stable correlation; an approved CSMS authority to write
  `CleanVerified`.
- **Status:** R6C-3 **DESIGN COMPLETE / IMPLEMENTATION BLOCKED**;
  R6 remains unchecked, PR #20 Draft, NO-GO for production.
- **Next decision:** authorize **R6C-3A offline read-only snapshot
  contract tests only** or propose a different CSMS proof mechanism.
  Do **not** authorize `CleanVerified` or fencing implementation until
  the CSMS contract, identity and negative-ACK semantics have
  explicit independent approval.

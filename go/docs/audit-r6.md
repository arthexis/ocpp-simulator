# R6 acceptance review — Go G3 recovery PR #20

Date: 2026-11-11. Scope: arthexis/ocpp-simulator, branch
'feat/13-g3-recovery'. **This review does not authorize Ready, merge,
deployment, production CSMS connections or service changes.**

## Decision: NO-GO for Ready / production / automatic restart

The R0–R5 Go code has an executable offline test suite and an opt-in backend,
but it lacks a durable source of truth for remote transactions. This is a
safety blocker, not a cosmetic testing gap. R6's negative local OCPP-J test
creates a real (fixture-issued) CSMS transaction, drops the WebSocket before
MeterValues is acknowledged, and starts a fresh Serve instance for the same
charge-point identity. The second instance sends an initial Available while
the old CSMS transaction is *still open*. This is expected under the current
memory-only architecture; a passing negative test documents the defect
rather than accepting it as correct recovery behavior.

Other no-go conditions:

1. SessionManager/ConnectorLedger have no fsynced transaction journal and
   no safe reconciliation/import API after process crash or external restart.
   Stderr JSON can be lost and is not durable recovery state.
2. OCPP ResetConfirmation has no delivery/flush receipt from the upstream
   library. Accepted means a bounded local attempt, not a confirmed CSMS
   observation or a physical charger restart.
3. In-flight OCPP sends and responses can race with Hard Reset or transport
   failure. The implementation cannot retract a frame already written,
   and its bounded teardown may leave a goroutine awaiting library I/O.
   It must not advertise confirmed closure without server evidence.
4. No field validation against a real, isolated CSMS or real charger is
   authorized for R6. Offline behavior and ARM64 cross-compilation are not
   a substitute for field acceptance.

**Required before reconsidering Ready:** separately approve a durable,
crash-safe ledger (including unknown StartTransaction and stop-ACK windows),
an identity-scoped startup reconciliation protocol that prevents initial
Available until CSMS state is reconciled, replay/dedup rules, recovery tests
for torn journal writes, SIGKILL/crashes/process supervisor restart, and
an explicit deployment/field-test gate. No such work is included in R6.

## Offline acceptance matrix

| Area | Evidence and scope | Offline finding | Production decision |
| --- | --- | --- | --- |
| R0 baseline and protected branches | PR #20 Draft; branch diff and active required CI ruleset | Controlled scope, no merge | No deploy |
| R1 SessionManager + Ledger | reserve/start/finish/abort, 128-connector bound, context cancellation and concurrency tests | Transaction ownership in-process | Insufficient across process restart |
| R2 RemoteStart | validated RFID, profile rejection, queue bounds, async dispatch; OCPP-J local Authorize/StartTransaction | Accepted denotes dispatch, ID originates in fixture CSMS | Provisional offline only |
| R3 RemoteStop | exact tx lookup, dedup, Remote/Local reason, ACK retained before Available | Verified in local OCPP-J tests | Missing crash recovery |
| R4 batteries | independent seeds, Wh/W/SoC, persisted per-connector in-memory SoC, taper, full and RemoteStop/full races | Deterministic local tests and multi-connector loopback | Volatile across process death |
| R5 Reset Soft | close new admissions, confirmed stop attempt, SoftReset reason | Local OCPP-J tests and race suite | No restart/reconciliation |
| R5 Reset Hard | no invented StopTransaction/Available, late IDs retained while alive | Local OCPP-J and negative concurrency tests | No durable late-reply evidence |
| Signal/disconnect | bounded shutdown, unknown sessions retained in memory and stderr output | Local cancellation and WebSocket tests | External restart remains unsafe |
| R6 lost ACK / simulated restart | TestR6ReproducesUnsafeFreshProcessAfterUnconfirmedTransaction | **Unsafe Available is reproducible** | **BLOCKER** |
| R6 unknown pending and known IDs | TestR6ManagerRestartLosesPendingAndKnownIDs | **Fresh manager does not inherit either record** | **BLOCKER** |
| Python default / explicit Go opt-in | Python CLI tests and installer smoke in isolated GitHub runner | Existing dispatch preserved | Not a rollout |
| Go vet, race, Linux arm64 | GitHub Actions Go matrix on exact PR SHA | Source and local tests pass only when CI succeeds | No physical ARM64 runtime proof |
| Isolated Ansible installer | GitHub-hosted runner localhost, immutable release smoke | Installer backward compatibility only | No production deploy |
| Actual CSMS, physical Reset, node reconnect | Explicitly out of R6 scope | **NOT TESTED** | Separate authorization required |

## Evidence discipline

All new R6 tests are local Go unit/in-process loopback tests. They do not
issue any command to a production CSMS, remote agent, Raspberry Pi, or
device. No credentials, service configuration, SQLite or production
endpoints were modified. A green CI proves the listed isolated
assertions; the two failing recovery safety properties are deliberately
recorded as a **NO-GO** rather than weakening or hiding the assertions.

Do not mark the Draft pull request Ready, close the work, or merge because
R6 tests pass. Re-evaluate once the blocking architecture gaps are resolved
in a separately authorized phase.

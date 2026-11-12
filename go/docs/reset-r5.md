# G3 Go simulator — R5 reset boundaries

The Python backend remains the default. Go remote mode is opt-in and is **not**
production-ready before R6 acceptance.

## Soft Reset

- Valid OCPP 1.6J Soft Reset is admitted only after BootNotification Accepted
  and initial connector Available notifications.
- New RemoteStart/RemoteStop/Reset admissions close immediately. Reserved jobs
  cancel; acknowledged sessions attempt exactly one StopTransaction with their
  CSMS-issued ID, final session-relative Wh and Reason=SoftReset (unless an
  earlier RemoteStop froze Reason=Remote).
- Available and ledger release require confirmed StopTransaction and subsequent
  Available notification. Missing/ambiguous confirmations retain Stopping.
- Workers drain for at most 3 seconds; process exits without auto-reconnect.

## Hard Reset

- Blocks new commands, cancels workers and terminates the connection, without
  fabricating StopTransaction or Available. An in-flight OCPP call cannot be
  revoked by the library.
- Known CSMS transaction IDs and unknown in-flight Starting slots are retained
  in the in-memory manager while it lives. Late StartTransaction ACK IDs are
  recorded, but Hard does not attempt additional StopTransaction calls.
- Terminal errors and a structured ocpp_simulator_shutdown JSON line show
  connector, state and known transaction ID for manual reconciliation.

## Shutdown, recovery, and non-guarantees

- Signal cancellation and WebSocket loss close admissions, cancel workers and
  never declare an unconfirmed transaction closed.
- No automatic reconnection, process restart, transaction replay or ledger
  reconstruction is implemented. Reconnecting with an empty ledger might
  falsely advertise Available and duplicate an unfinished transaction.
- R5 state is **not durable**. A crash or externally managed process restart
  can erase in-memory IDs. Retain shutdown logs and reconcile each outstanding
  transaction on the CSMS before starting another Go process with the same
  CP identity. Do not configure automatic restart for this experimental mode.
- The simulator cannot reboot physical hardware. Accepted Reset represents
  a bounded software termination attempt, not a confirmed reboot.
- The OCPP Go library exposes no callback proving ResetConfirmation flush.
  Serve uses a bounded 100ms grace before closing, not a delivery guarantee.
- Durable recovery and production acceptance are separately gated and must
  be assessed in R6. No production CSMS, node, AgentOps, Ansible or hardware
  is exercised by these isolated CI tests.

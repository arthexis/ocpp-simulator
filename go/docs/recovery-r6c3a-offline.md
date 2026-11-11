# R6C-3A — offline SQLite v8 evidence adapter

**Scope: tests and read-only observation exclusively. Not a recovery implementation.**
Baseline: simulator PR #20 at 200c561f27c00b70df11fd0dc82e52babe6f8212; CSMS main at 77452e39efb1702cc78c753724d1d30892ae411d.

- The unexported Go adapter in spike has **no runtime/Serve callers** and accepts only a caller-supplied SQLite handle. Tests create full relevant v8 CSMS schema (including transaction boundary view and session discovery tables) in t.TempDir; the observer handle uses mode=ro, and a single read transaction covers source metadata, all connectors, transaction rows, OCPP Start/Stop event receipts, statuses and global events.id watermark.
- Every connector 1..N is represented even with zero rows. Open/stopped records are distinct, and a stopped transaction's StopReceipt is populated only by an actual StopTransaction event with matching CP, transaction ID and durable stop_received_at timestamp. The derived next-start boundary and StatusNotification Available are never stop receipts.
- An orphan Stop event (wrong CP, unknown ID, or invalid wire ID) cannot close another transaction. Exact-payload Start repeats and duplicate Stop receipts are reported as observations; overlapping opens, Available/open contradictions, absent matching stop events and out-of-range data are conflicts. source_id mismatch and unsupported schema reject the read.
- events.id is a **read watermark only**: not an epoch, monotonic owner revision, proof of completeness, attempt_id correlation or fence. WebSocket session replacement does not prevent late writes. The CSMS schema retains no local Go attempt_id or OCPP-J UniqueId for safe lost-ACK resolution.
- Every result always carries Trust=Quarantined. No CleanVerified grant, reconciliation write, owner lease, replay, reconnect, endpoint, CSMS modification, manual Ansible/deploy, production connection or permission alteration. BeforeAvailable remains unconditionally Quarantined. R6 remains unchecked; PR #20 stays Draft.

**Acceptance testing:** Go race/vet/arm64 and the existing Python/localhost Ansible CI gates on the exact resulting commit. No test may contact a real CSMS or use pre-existing SQLite. R6C-3B requires separate authorization in the CSMS repository; R6C-3C is a separate consumer decision, not implicit in this offline adapter.

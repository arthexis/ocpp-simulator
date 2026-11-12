# R6C-3B2-LAB-5 — offline-only OCPP-J UID acceptance fixture

Scope: arthexis/ocpp-simulator PR #20, Draft. The only runtime-impacting code change is **none**: the fixture exists solely as `go/internal/spike/r6c3b2_uid_lab5_test.go`. No CSMS endpoints, schema, recovery, admission, network behavior, permissions, services, or deployed binaries are changed.

The fixture executes the real `ocppj.Client.SendRequest` → `CreateCall` → `RequestBundle` path from pinned `github.com/lorenzodonini/ocpp-go v0.19.0` with a **fake, disconnected queue**. A request pointer is bound to one synthetic attempt before UID generation. The wrapper validates both the serialized OCPP-J frame and the immutable StartTransaction payload, reserves attempt/UID using O_EXCL + file fsync + directory fsync, and only then delegates to the fake queue.

Tests: generated UID captured before delegation; unbound/malformed/mutated requests fail closed; duplicate UID and attempt_id, unobserved ACK and post-fsync queue errors remain quarantined; wrong source/incarnation/tenant/CP/epoch/action are rejected; concurrency and SIGKILL preserve one-shot durable reservations. No global SetMessageIdGenerator and no OCPP 1.6J payload modification.

CI Go OCPP spike resolves module with sumdb enabled and checks h1:THNriVV3bUWkGzaIkDeytcgoeBRjoN9ezPHsddIDfgc=, runs go vet and go test -race (full suite plus this fixture repeated), then cross-compiles a **test executable** for linux/arm64. The artifact is NOT a deployable simulator client or real end-to-end CSMS proof. It includes SHA-256, go version -m, and checkout provenance.

Important limitations: no actual network send, no server-side Python call_unique_id → commit_start proof, no lost ACK over WebSocket, no production authority/recovery guarantee. BeforeAvailable remains Quarantined; R6 remains unchecked. No merging, deploying, promoting Ready, CleanVerified, replay, or RemoteStart admission.

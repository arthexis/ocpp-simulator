# Go OCPP simulator — G0 protocol spike

Independent from the Python Mobility House library. This is a **test-only**
CLI; it does not replace the installed `ocpp-simulator` command or Ansible.

The G0 safety gate accepts only `ws://` loopback URLs with an explicit
`/CP` identity. Never point the spike at a live charger/production CSMS.
The runner sends **one** BootNotification and **one** Heartbeat, then exits.

```sh
cd go
go mod tidy
gofmt -w cmd internal
go vet ./...
go test ./...
go run ./cmd/ocpp-simulator-go --cp SIM001 --url ws://127.0.0.1:9000/SIM001
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/ocpp-simulator-go ./cmd/ocpp-simulator-go
```

G0 pins `github.com/lorenzodonini/ocpp-go v0.19.0` (tag marked
pre-release by its authors). The independent loopback test uses
Gorilla WebSocket only for a tiny local CSMS wire fixture; the simulator
itself uses the `ocpp-go` API. Dependency checks, security review and
extended API compatibility remain explicit gates before G1.

The library is Go 1.16-compatible according to its go.mod, but the spike
targets Go 1.23 to keep local and CI toolchains consistent. It includes
validation enabled by default. Revisit library maintenance, licensing and
upstream releases before promoting this beyond an experiment.

Current limits: no WebSocket TLS, authentication, continuous heartbeats,
transactions, remote commands, or installer integration. Those belong to
G1–G5 under issue #13.

# Go OCPP simulator — G1 connectivity

Independent OCPP 1.6J charge-point implementation using
[ocpp-go v0.19.0](https://github.com/lorenzodonini/ocpp-go), whose upstream
license is MIT. The installed Python simulator is still the default.

## Development

```sh
cd go
go mod tidy
gofmt -w cmd internal
go vet ./...
go test -race ./...
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/ocpp-simulator-go ./cmd/ocpp-simulator-go
```

## G1 run mode

```sh
# Developer checkout: Go 1.23+ required
cd go
go run ./cmd/ocpp-simulator-go run --cp SIM001 --url ws://127.0.0.1:9000/SIM001

# Installed with ./deploy.sh:
ocpp-simulator --backend go run --cp SIM001 --url wss://example.test/SIM001 --ca ./test-ca.pem
# Default remains Python, including its charge, remote and battery features:
ocpp-simulator run --cp SIM001 --url ws://127.0.0.1:9000/SIM001
ocpp-simulator-py run --cp SIM001 --url ws://127.0.0.1:9000/SIM001
```

The Go backend supports --cp, --url, --vendor, --model, --ca,
--basic-user, --basic-password and --connect-timeout. Unsupported Python
remote/battery options fail explicitly; remote mode is G3.
The Go process stays connected and sends Heartbeats at the interval issued
by an Accepted BootNotification, until SIGINT/SIGTERM or disconnection.
It does not auto-reconnect or start on installation.

For WSS, system root CAs and optional --ca are used with hostname validation.
HTTP Basic is only allowed over WSS. The password CLI argument can appear
in the process list or shell history: use temporary test credentials.
The independent WebSocket transport strictly verifies the `ocpp1.6`
subprotocol selected by the server, in addition to the OCPP library's
message validation.

## Packaging and caveats

Run `./deploy.sh` to install **both** Python and Go into the same
immutable release; Go 1.23+ must be available on the target at build time.
The installer preserves Ansible's revision and owner lock conventions.
Existing immutable Python-only releases must **not** be mutated to add Go;
deploy a new revision instead. The installed Go executable is
`~/.local/bin/ocpp-simulator-go` and the normal dispatcher finds the
binary next to the release virtualenv interpreter. Python remains the
default execution backend until explicitly switched.

The G1 test matrix includes an independent ephemeral OCPP-J server, timed
Heartbeats, strict WebSocket subprotocol negotiation, untrusted certificate
rejection, custom CA trust, Basic Auth success/failure, and invalid option
rejection. Deployment never starts a simulated charger, and no production
CSMS connection has been made. Full real-CSMS acceptance follows in G1
field validation; negative OCPP semantic tests follow in G4.

## G2 one-shot charging

```sh
ocpp-simulator --backend go run --cp SIM001 --url ws://127.0.0.1:9000/SIM001 \\
  --connector 1 --rfid TEST001 --power 7.2 --duration 60 --meter-interval 5
```

After Accepted BootNotification, the Go backend sends Preparing, Authorize,
StartTransaction, Charging, periodic cumulative MeterValues (Wh),
StopTransaction and Available. It requires accepted authorization and start
responses, uses the server's transaction ID, and computes deterministic
meter readings from configured simulated duration. SIGINT/SIGTERM requests
an early StopTransaction. Failed MeterValues/StopTransaction never become
invented successful closes. No remote commands, multiple sessions, battery
SoC, or automatic reconnect yet. Unit and wire fixture tests are isolated
from production endpoints.

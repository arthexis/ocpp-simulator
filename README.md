# OCPP Simulator

Minimal OCPP 1.6J charge-point simulator, using the maintained
[Mobility House OCPP](https://github.com/mobilityhouse/ocpp) protocol backend.

## Install

Requires Python 3.11 or later.

```sh
python -m pip install -e '.[dev]'
```

## Deploy the CLI with Ansible

On a Debian machine with Python 3.11+, `python3-venv`, `python3-pip`, and
`ansible-playbook` installed, run from a checkout:

```sh
sh deploy.sh
~/.local/bin/ocpp-simulator --help
```

The role installs a non-editable Python package into
`~/.local/share/ocpp-simulator/venv`, then links the installed entry point
to `~/.local/bin/ocpp-simulator`. Ensure `~/.local/bin` is on `PATH` if
you want to invoke the command without its full path.

It intentionally **does not start a systemd service** or connect to a
production CSMS automatically. The simulator is an interactive test client;
use `ocpp-simulator run --cp ... --url ...` to start a deliberate test.

The installation is owned by the current non-root account and is separate
from any `ocpp-csms` service or virtual environment. Re-running the
installer upgrades the installed simulator from the checkout. Ansible can
be invoked directly with
`ANSIBLE_ROLES_PATH=ansible/roles ansible-playbook ansible/playbooks/install.yml -i localhost, -c local`.

## Connect to a CSMS

```sh
ocpp-simulator run --cp SIM001 --url ws://127.0.0.1:9000/SIM001
ocpp-simulator run --cp SIM001 --url wss://csms.example.test:9443/SIM001 --ca /path/to/test-ca.pem
```

The `--cp` identifier and final URL path should identify the same charge point
when used with OCPP-CSMS. The simulator requires negotiation of the `ocpp1.6`
WebSocket subprotocol, sends BootNotification, requires Accepted status, and
sends Heartbeats at the interval returned by the server. It exits on failures;
automatic reconnect/failover is deliberately deferred.

For WSS, hostname validation and certificate-chain validation are enabled by
default. `--ca` adds a custom CA bundle, suitable for a private test CA.
There is no insecure TLS mode. Do not use a production charger identity against
a live CSMS.

## Stop a running simulation safely

Use **Ctrl+C** in the foreground terminal (or send **SIGTERM** to the
simulator process). These signals now request a graceful shutdown: ongoing
simulated sessions send `StopTransaction` and return their connectors to
Available before the WebSocket is closed. For a one-shot session, the current
charge is stopped early rather than abandoned.

A force kill (`SIGKILL`), machine crash, broken network or loss of CSMS
connectivity cannot transmit `StopTransaction`. Such records must remain
explicitly recoverable by the CSMS, not silently closed merely because a
charge point disconnected. A previously abandoned `SIM001` transaction will
not retroactively be closed by upgrading the simulator.

## Test

```sh
python -m pytest -q
```

Tests use a local in-process OCPP CSMS endpoint, a generated certificate, and
real WS/WSS handshakes. The first chunk is intentionally headless and has no
charging, meter values, charger configuration, or backup URL policy. Future
chunks will add those behaviors without implementing a custom OCPP protocol.

## Simulate a charge (Chunk 2)

After BootNotification is accepted, run a single simulated charging
transaction and exit:

```sh
ocpp-simulator run --cp SIM001 --url ws://127.0.0.1:9000/SIM001 \
  --connector 1 --rfid TEST001 --power 7.2 --duration 60 \
  --meter-interval 5
```

Power is in kW, time in seconds and cumulative OCPP meter readings in Wh.
The simulator sends Preparing, Authorize, StartTransaction, Charging,
MeterValues, StopTransaction and Available. Only accepted authorization and
start requests are allowed to proceed. Energy is deterministic: 7.2 kW over
60 seconds equals 120 Wh.

Omit `--connector` to preserve the Chunk 1 boot/heartbeat-only mode.
Remote commands, multiple concurrent sessions and failover remain future work.

## Remote controlled simulator (Chunk 3)

Keep a simulated charger connected and accept CSMS-issued remote commands:

```sh
ocpp-simulator run --cp SIM001 --url ws://127.0.0.1:9000/SIM001 \
  --remote --connectors 2 --power 7.2 --meter-interval 5
```

Supports OCPP 1.6J `RemoteStartTransaction`, `RemoteStopTransaction`,
`Reset`, `GetConfiguration` and `ChangeConfiguration`. Transactions
are associated with their originating connector, so connector 2 can start
while connector 1 is charging. Remote stops target the CSMS-assigned
transaction ID. Soft reset requests graceful transaction stops before
reconnecting; hard reset interrupts the simulated tasks before reconnecting.

Configuration lives in memory and is reset on reconnect. The simulated
primary/backup vendor keys are stored and returned but do **not** drive
actual endpoint changes or failover yet. That is reserved for Chunk 4.

## Natural battery simulation

Remote-controlled sessions no longer need to run until manually stopped.
Each connector has a simulated battery, initially 60 kWh at 30% charge.
Charging holds approximately the configured power below 80% SOC, tapers
toward 100%, and adds small seeded fluctuations. Once full, the simulator
sends StopTransaction and returns the connector to Available. CSMS remote
stop still works at any point.

```sh
ocpp-simulator run --cp SIM001 --url ws://127.0.0.1:9000/SIM001 \
  --remote --connectors 2 --power 7.2 --meter-interval 5 \
  --battery-kwh 60 --soc 30 --seed 7
```

The `--seed` provides reproducible power variation for debugging.
MeterValues include cumulative energy (Wh), instantaneous power (W), and
battery SOC (percent). The battery is simplified and intentionally does
not model vehicle temperature, DC fast-charging chemistry, losses, or
capacity degradation. This behavior applies to remote mode; the one-shot
`--duration` scenario is unchanged.

# OCPP Simulator

Minimal OCPP 1.6J charge-point simulator, using the maintained
[Mobility House OCPP](https://github.com/mobilityhouse/ocpp) protocol backend.

## Install

Requires Python 3.11 or later.

```sh
python -m pip install -e '.[dev]'
```

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

## Test

```sh
python -m pytest -q
```

Tests use a local in-process OCPP CSMS endpoint, a generated certificate, and
real WS/WSS handshakes. The first chunk is intentionally headless and has no
charging, meter values, charger configuration, or backup URL policy. Future
chunks will add those behaviors without implementing a custom OCPP protocol.

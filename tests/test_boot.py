"""End-to-end protocol tests using a real Mobility House OCPP server."""
from __future__ import annotations

import asyncio
import socket
import ssl
import subprocess

import pytest
import websockets
from ocpp.routing import on
from ocpp.v16 import ChargePoint as ServerChargePoint
from ocpp.v16 import call_result
from ocpp.v16.enums import Action

from ocpp_simulator.cli import build_parser
from ocpp_simulator.connection import run_charge_point, tls_context


class StubCSMS(ServerChargePoint):
    @on(Action.BootNotification)
    def on_boot(self, charge_point_vendor, charge_point_model, **kwargs):
        return call_result.BootNotification(
            current_time="2026-01-01T00:00:00Z", interval=1, status="Accepted"
        )

    @on(Action.Heartbeat)
    def on_heartbeat(self):
        self.heartbeat_seen.set()
        return call_result.Heartbeat(current_time="2026-01-01T00:00:01Z")


def _certificates(tmp_path):
    cert, key = tmp_path / "server.crt", tmp_path / "server.key"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-days", "2",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
    ], check=True, capture_output=True)
    return cert, key


async def _exercise(url, ssl_context=None, ca=None):
    booted, heartbeat = asyncio.Event(), asyncio.Event()

    async def handler(ws):
        charge_point = StubCSMS("SIM001", ws)
        charge_point.heartbeat_seen = heartbeat
        booted.set()
        await charge_point.start()

    server = await websockets.serve(
        handler, "127.0.0.1", 0, ssl=ssl_context, subprotocols=["ocpp1.6"]
    )
    port = server.sockets[0].getsockname()[1]
    endpoint = url.format(port=port)
    task = asyncio.create_task(run_charge_point(
        cp="SIM001", url=endpoint, ca=ca, vendor="Arthexis", model="Simulator"
    ))
    try:
        await asyncio.wait_for(heartbeat.wait(), timeout=8)
        assert booted.is_set()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_boot_and_heartbeat_over_ws():
    await _exercise("ws://127.0.0.1:{port}/SIM001")


@pytest.mark.asyncio
async def test_boot_and_heartbeat_over_verified_wss(tmp_path):
    cert, key = _certificates(tmp_path)
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(cert, key)
    await _exercise("wss://localhost:{port}/SIM001", server_tls, str(cert))


def test_cli_and_tls_input_validation(tmp_path):
    parser = build_parser()
    args = parser.parse_args(["run", "--cp", "SIM001", "--url", "ws://localhost:9000/SIM001"])
    assert args.cp == "SIM001"
    assert tls_context(args.url) is None
    with pytest.raises(ValueError, match="--ca"):
        tls_context("ws://localhost:9000/SIM001", "test.pem")
    with pytest.raises(ValueError, match="URL"):
        tls_context("http://localhost:9000")


@pytest.mark.asyncio
async def test_wss_untrusted_cert_and_hostname_mismatch(tmp_path):
    cert, key = _certificates(tmp_path)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)

    async def handler(ws):
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0, ssl=context, subprotocols=["ocpp1.6"]) as server:
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(ssl.SSLCertVerificationError):
            await run_charge_point(
                cp="SIM001", url=f"wss://localhost:{port}/SIM001", ca=None,
                vendor="Arthexis", model="Simulator",
            )
        with pytest.raises(ssl.SSLCertVerificationError):
            await run_charge_point(
                cp="SIM001", url=f"wss://127.0.0.1:{port}/SIM001", ca=str(cert),
                vendor="Arthexis", model="Simulator",
            )

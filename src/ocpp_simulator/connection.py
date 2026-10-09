"""WS/WSS connections with standard certificate and hostname validation."""
from __future__ import annotations

import asyncio
import ssl
from urllib.parse import urlsplit

import websockets

from ocpp_simulator.charger import SimulatorChargePoint
from ocpp_simulator.charging import simulate_charge


def tls_context(url: str, ca: str | None = None) -> ssl.SSLContext | None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
        raise ValueError("URL must be an absolute ws:// or wss:// endpoint")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("credentials and fragments are not supported in the URL")
    if ca and parsed.scheme != "wss":
        raise ValueError("--ca requires a wss:// endpoint")
    if parsed.scheme == "wss":
        return ssl.create_default_context(cafile=ca)
    return None


async def run_charge_point(*, cp: str, url: str, ca: str | None, vendor: str,
                           model: str, connect_timeout: float = 10.0,
                           connector: int | None = None, rfid: str = "TEST001",
                           power_kw: float = 7.2, duration: float = 60.0,
                           meter_interval: float = 1.0) -> None:
    if not cp or "/" in cp:
        raise ValueError("--cp must be a non-empty charge-point identifier without '/'")
    ssl_context = tls_context(url, ca)
    print(f"Connecting: {url}", flush=True)
    async with websockets.connect(
        url, subprotocols=["ocpp1.6"], ssl=ssl_context,
        open_timeout=connect_timeout, close_timeout=5,
    ) as connection:
        if connection.subprotocol != "ocpp1.6":
            raise RuntimeError("CSMS did not negotiate ocpp1.6")
        print("WebSocket: connected", flush=True)
        charge_point = SimulatorChargePoint(cp, connection, vendor=vendor, model=model)
        receiver = asyncio.create_task(charge_point.start())
        try:
            response = await charge_point.boot()
            status = getattr(response, "status", None)
            print(f"BootNotification: {status}", flush=True)
            if status != "Accepted":
                raise RuntimeError(f"BootNotification was not accepted: {status}")
            seconds = getattr(response, "interval", None)
            if not isinstance(seconds, int) or seconds <= 0:
                raise RuntimeError("BootNotification returned an invalid heartbeat interval")
            print(f"Heartbeat interval: {seconds} seconds", flush=True)
            print("Simulator ready", flush=True)
            if connector is not None:
                heartbeat = asyncio.create_task(charge_point.heartbeats(seconds))
                try:
                    energy = await simulate_charge(
                        charge_point, connector=connector, rfid=rfid,
                        power_kw=power_kw, duration=duration,
                        meter_interval=meter_interval,
                    )
                    print(f"Transaction complete: {energy} Wh", flush=True)
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            else:
                await charge_point.heartbeats(seconds)
        finally:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)

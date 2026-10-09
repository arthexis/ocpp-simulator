"""WS/WSS connections with standard certificate and hostname validation."""
from __future__ import annotations

import asyncio
import signal
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
                           meter_interval: float = 1.0, remote: bool = False,
                           connectors: int = 1, battery_kwh: float = 60.0,
                           initial_soc: float = 30.0, seed: int = 1) -> None:
    if not cp or "/" in cp:
        raise ValueError("--cp must be a non-empty charge-point identifier without '/'")
    if remote and connector is not None:
        raise ValueError("--remote cannot be combined with --connector")
    if connectors < 1:
        raise ValueError("--connectors must be positive")
    ssl_context = tls_context(url, ca)
    # SIGINT/SIGTERM should request an OCPP stop, not cancel in-flight calls.
    # SIGKILL, power loss, and transport failures cannot send StopTransaction.
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stopping.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            pass
    print(f"Connecting: {url}", flush=True)
    async with websockets.connect(
        url, subprotocols=["ocpp1.6"], ssl=ssl_context,
        open_timeout=connect_timeout, close_timeout=5,
    ) as connection:
        if connection.subprotocol != "ocpp1.6":
            raise RuntimeError("CSMS did not negotiate ocpp1.6")
        print("WebSocket: connected", flush=True)
        charge_point = SimulatorChargePoint(
            cp, connection, vendor=vendor, model=model,
            connectors=connectors, power_kw=power_kw, meter_interval=meter_interval,
            battery_kwh=battery_kwh, initial_soc=initial_soc, seed=seed,
        )
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
            if remote:
                heartbeat = asyncio.create_task(charge_point.heartbeats(seconds))
                reset_waiter = asyncio.create_task(charge_point.reset_requested.wait())
                shutdown_waiter = asyncio.create_task(stopping.wait())
                try:
                    done, _ = await asyncio.wait(
                        [heartbeat, reset_waiter, receiver, shutdown_waiter],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if shutdown_waiter in done:
                        print("Stopping active simulator transactions...", flush=True)
                        await asyncio.wait_for(charge_point.shutdown_sessions(), timeout=15)
                    elif reset_waiter in done:
                        # Allow the OCPP Reset response to flush before disconnect.
                        await asyncio.sleep(0.1)
                        await charge_point.shutdown_sessions(
                            hard=charge_point.reset_type == "Hard"
                        )
                    else:
                        for finished in done:
                            await finished
                finally:
                    heartbeat.cancel()
                    reset_waiter.cancel()
                    shutdown_waiter.cancel()
                    await asyncio.gather(heartbeat, reset_waiter, shutdown_waiter, return_exceptions=True)
            elif connector is not None:
                heartbeat = asyncio.create_task(charge_point.heartbeats(seconds))
                try:
                    energy = await simulate_charge(
                        charge_point, connector=connector, rfid=rfid,
                        power_kw=power_kw, duration=duration,
                        meter_interval=meter_interval, stop_event=stopping,
                    )
                    print(f"Transaction complete: {energy} Wh", flush=True)
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            else:
                await stopping.wait()
        finally:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
    for signum in installed_signals:
        loop.remove_signal_handler(signum)
    if remote and charge_point.reset_requested.is_set() and not stopping.is_set():
        await run_charge_point(
            cp=cp, url=url, ca=ca, vendor=vendor, model=model,
            connect_timeout=connect_timeout, remote=True, connectors=connectors,
            power_kw=power_kw, meter_interval=meter_interval,
            battery_kwh=battery_kwh, initial_soc=initial_soc, seed=seed,
        )

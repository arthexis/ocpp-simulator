"""Protocol behavior delegated to the Mobility House OCPP implementation."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ocpp.v16 import ChargePoint as OCPPChargePoint
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action
from ocpp.routing import on

from ocpp_simulator.configuration import Configuration
from ocpp_simulator.connectors import Connectors
from ocpp_simulator.charging import simulate_charge


class SimulatorChargePoint(OCPPChargePoint):
    def __init__(self, charge_point_id, connection, *, vendor="Arthexis", model="Simulator", connectors=1, power_kw=7.2, meter_interval=5.0):
        super().__init__(charge_point_id, connection)
        self.vendor = vendor
        self.model = model
        self.registry = Connectors(connectors)
        self.config = Configuration(connectors, meter_interval=meter_interval)
        self.power_kw = power_kw
        self.reset_requested = asyncio.Event()
        self.reset_type = None

    async def boot(self):
        return await self.call(call.BootNotificationPayload(
            charge_point_vendor=self.vendor,
            charge_point_model=self.model,
        ))

    async def heartbeats(self, seconds: int):
        while True:
            await asyncio.sleep(seconds)
            response = await self.call(call.HeartbeatPayload())
            print(f"Heartbeat: {getattr(response, 'current_time', 'received')}", flush=True)

    async def _remote_session(self, connector, rfid, stop):
        print(f"Remote start: connector {connector}, RFID {rfid}", flush=True)
        try:
            energy = await simulate_charge(
                self, connector=connector, rfid=rfid,
                power_kw=self.power_kw, duration=86400,
                meter_interval=float(self.config.values["MeterValueSampleInterval"][0]),
                stop_event=stop,
                on_started=lambda tx: self._report_remote_start(connector, tx),
                on_meter=lambda wh: print(f"Meter: connector {connector}, {wh} Wh", flush=True),
            )
            print(f"Remote transaction complete: {energy} Wh", flush=True)
        except Exception as exc:
            print(f"Remote transaction failed on connector {connector}: {exc}", flush=True)
        finally:
            self.registry.release(connector)

    def _report_remote_start(self, connector, transaction_id):
        self.registry.active_transaction(connector, transaction_id)
        print(
            f"Remote transaction started: connector {connector}, "
            f"TXN {transaction_id}", flush=True,
        )

    @on(Action.RemoteStartTransaction)
    async def on_remote_start(self, id_tag, connector_id=None, **kwargs):
        connector = connector_id or 1
        print(f"RemoteStartTransaction received: connector {connector}, RFID {id_tag}", flush=True)
        if not self.registry.free(connector):
            return call_result.RemoteStartTransactionPayload(status="Rejected")
        stop = self.registry.reserve(connector)
        task = asyncio.create_task(self._remote_session(connector, id_tag, stop))
        self.registry.tasks[connector] = task
        return call_result.RemoteStartTransactionPayload(status="Accepted")

    @on(Action.RemoteStopTransaction)
    def on_remote_stop(self, transaction_id, **kwargs):
        accepted = self.registry.stop_transaction(transaction_id)
        print(f"RemoteStopTransaction received: TXN {transaction_id}, accepted={accepted}", flush=True)
        return call_result.RemoteStopTransactionPayload(
            status="Accepted" if accepted else "Rejected"
        )

    @on(Action.GetConfiguration)
    def on_get_configuration(self, key=None, **kwargs):
        entries, unknown = self.config.get(key)
        return call_result.GetConfigurationPayload(configuration_key=entries, unknown_key=unknown)

    @on(Action.ChangeConfiguration)
    def on_change_configuration(self, key, value, **kwargs):
        return call_result.ChangeConfigurationPayload(status=self.config.change(key, value))

    @on(Action.Reset)
    def on_reset(self, type, **kwargs):
        if type not in ("Soft", "Hard"):
            return call_result.ResetPayload(status="Rejected")
        self.reset_type = type
        self.reset_requested.set()
        return call_result.ResetPayload(status="Accepted")

    async def shutdown_sessions(self, *, hard=False):
        if hard:
            tasks = list(self.registry.tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            for event in self.registry.stops.values():
                event.set()
            await asyncio.gather(*list(self.registry.tasks.values()), return_exceptions=True)

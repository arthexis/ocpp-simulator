"""Protocol behavior delegated to the Mobility House OCPP implementation."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ocpp.v16 import ChargePoint as OCPPChargePoint
from ocpp.v16 import call


class SimulatorChargePoint(OCPPChargePoint):
    def __init__(self, charge_point_id, connection, *, vendor="Arthexis", model="Simulator"):
        super().__init__(charge_point_id, connection)
        self.vendor = vendor
        self.model = model

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

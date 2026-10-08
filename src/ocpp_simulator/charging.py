"""Deterministic single-connector charging transactions over OCPP 1.6J."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from ocpp.v16 import call
from ocpp.v16.datatypes import MeterValue, SampledValue


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


async def simulate_charge(
    cp,
    *,
    connector: int,
    rfid: str,
    power_kw: float,
    duration: float,
    meter_interval: float = 1.0,
) -> int:
    """Perform a complete charge; report cumulative energy in Wh."""
    if connector < 1 or not rfid or power_kw <= 0 or duration < 0 or meter_interval <= 0:
        raise ValueError("invalid charging parameters")
    power = Decimal(str(power_kw))
    if not hasattr(cp, "_charging_connectors"):
        cp._charging_connectors = set()
    if connector in cp._charging_connectors:
        raise RuntimeError("connector_already_charging")
    cp._charging_connectors.add(connector)
    transaction_id = None
    elapsed = Decimal(0)
    try:
        await cp.call(call.StatusNotificationPayload(
            connector_id=connector, error_code="NoError", status="Preparing",
        ))
        authorized = await cp.call(call.AuthorizePayload(id_tag=rfid))
        if authorized.id_tag_info["status"] != "Accepted":
            raise RuntimeError("rfid_not_authorized")
        started = await cp.call(call.StartTransactionPayload(
            connector_id=connector, id_tag=rfid, meter_start=0, timestamp=timestamp(),
        ))
        if started.id_tag_info["status"] != "Accepted":
            raise RuntimeError("start_transaction_rejected")
        transaction_id = started.transaction_id
        await cp.call(call.StatusNotificationPayload(
            connector_id=connector, error_code="NoError", status="Charging",
        ))
        total = Decimal(str(duration))
        step = Decimal(str(meter_interval))
        while elapsed < total:
            period = min(step, total - elapsed)
            await asyncio.sleep(float(period))
            elapsed += period
            wh = int((power * Decimal(1000) * elapsed / Decimal(3600)).to_integral_value())
            await cp.call(call.MeterValuesPayload(
                connector_id=connector, transaction_id=transaction_id,
                meter_value=[MeterValue(timestamp=timestamp(), sampled_value=[
                    SampledValue(value=str(wh), measurand="Energy.Active.Import.Register", unit="Wh"),
                ])],
            ))
        final_wh = int((power * Decimal(1000) * total / Decimal(3600)).to_integral_value())
        await cp.call(call.StopTransactionPayload(
            transaction_id=transaction_id, meter_stop=final_wh,
            timestamp=timestamp(), reason="Local",
        ))
        transaction_id = None
        return final_wh
    finally:
        # Avoid reporting Available while an unsettled transaction is still open.
        if transaction_id is None:
            await cp.call(call.StatusNotificationPayload(
                connector_id=connector, error_code="NoError", status="Available",
            ))
        cp._charging_connectors.remove(connector)

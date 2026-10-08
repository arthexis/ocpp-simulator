"""Charging lifecycle and deterministic energy accounting."""
from types import SimpleNamespace

import pytest
from ocpp.v16 import call

from ocpp_simulator.charging import simulate_charge


class FakeChargePoint:
    def __init__(self, *, authorized=True):
        self.calls = []
        self.authorized = authorized

    async def call(self, request):
        self.calls.append(request)
        if isinstance(request, call.Authorize):
            return SimpleNamespace(id_tag_info={"status": "Accepted" if self.authorized else "Blocked"})
        if isinstance(request, call.StartTransaction):
            return SimpleNamespace(id_tag_info={"status": "Accepted"}, transaction_id=42)
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_complete_transaction_and_meter_values():
    cp = FakeChargePoint()
    wh = await simulate_charge(
        cp, connector=1, rfid="TEST001", power_kw=7.2,
        duration=0.1, meter_interval=0.05,
    )
    assert wh == 0  # 7.2 kW for 100 milliseconds is less than 1 Wh
    assert [type(c).__name__ for c in cp.calls] == [
        "StatusNotification", "Authorize", "StartTransaction",
        "StatusNotification", "MeterValues", "MeterValues",
        "StopTransaction", "StatusNotification",
    ]
    assert cp.calls[2].meter_start == 0
    assert cp.calls[4].transaction_id == 42
    assert cp.calls[6].meter_stop == wh
    assert cp.calls[-1].status == "Available"


@pytest.mark.asyncio
async def test_authorization_rejection_never_opens_transaction():
    cp = FakeChargePoint(authorized=False)
    with pytest.raises(RuntimeError, match="rfid_not_authorized"):
        await simulate_charge(cp, connector=1, rfid="BLOCKED", power_kw=7.2, duration=0)
    assert not any(isinstance(c, call.StartTransaction) for c in cp.calls)
    assert cp.calls[-1].status == "Available"


@pytest.mark.asyncio
async def test_invalid_charge_parameters():
    cp = FakeChargePoint()
    for kwargs in (
        {"connector": 0, "rfid": "A", "power_kw": 7.2, "duration": 1},
        {"connector": 1, "rfid": "", "power_kw": 7.2, "duration": 1},
        {"connector": 1, "rfid": "A", "power_kw": 0, "duration": 1},
        {"connector": 1, "rfid": "A", "power_kw": 7.2, "duration": -1},
    ):
        with pytest.raises(ValueError):
            await simulate_charge(cp, **kwargs)

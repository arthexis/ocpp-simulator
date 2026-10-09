"""Battery taper, reproducibility and OCPP meter integration."""
import asyncio
from types import SimpleNamespace

import pytest
from ocpp.v16 import call

from ocpp_simulator.battery import Battery
from ocpp_simulator.charging import simulate_charge


def test_power_tapers_and_variation_is_repeatable():
    a, b = Battery(soc=35, seed=7), Battery(soc=35, seed=7)
    assert [a.power_kw(7.2) for _ in range(20)] == [b.power_kw(7.2) for _ in range(20)]
    low = Battery(soc=35, seed=7).power_kw(7.2)
    high = Battery(soc=95, seed=7).power_kw(7.2)
    assert 6.5 < low < 7.8
    assert 0 < high < low / 2


def test_capacity_and_full_charge_limit():
    battery = Battery(capacity_kwh=0.001, soc=95, seed=1)
    power, wh = battery.advance(60, 7.2)
    assert power > 0
    assert battery.soc == 100
    assert wh >= 0
    assert battery.power_kw(7.2) == 0


class Stub:
    def __init__(self):
        self.sent = []

    async def call(self, payload):
        self.sent.append(payload)
        if isinstance(payload, call.AuthorizePayload):
            return SimpleNamespace(id_tag_info={"status": "Accepted"})
        if isinstance(payload, call.StartTransactionPayload):
            return SimpleNamespace(id_tag_info={"status": "Accepted"}, transaction_id=17)
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_remote_battery_stops_naturally_and_sends_samples(monkeypatch):
    async def no_wait(_event, timeout):
        raise asyncio.TimeoutError

    monkeypatch.setattr("ocpp_simulator.charging.asyncio.wait_for", no_wait)
    stub = Stub()
    battery = Battery(capacity_kwh=0.001, soc=95, seed=2)
    delivered = await simulate_charge(
        stub, connector=1, rfid="TEST", power_kw=7.2,
        duration=3600, meter_interval=5, stop_event=asyncio.Event(),
        battery=battery,
    )
    assert battery.soc == 100
    meters = [p for p in stub.sent if isinstance(p, call.MeterValuesPayload)]
    assert meters
    assert all(any(v.measurand == "SoC" for v in m.meter_value[0].sampled_value) for m in meters)
    assert all(any(v.measurand == "Power.Active.Import" for v in m.meter_value[0].sampled_value) for m in meters)
    assert next(p for p in stub.sent if isinstance(p, call.StopTransactionPayload)).meter_stop == delivered
    assert stub.sent[-1].status == "Available"


def test_bad_parameters():
    for args in ({"capacity_kwh": 0}, {"soc": -1}, {"soc": 101}):
        with pytest.raises(ValueError):
            Battery(**args)

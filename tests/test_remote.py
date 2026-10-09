"""Remote OCPP handlers and configuration contract tests."""
import asyncio
from types import SimpleNamespace

import pytest
from ocpp.v16 import call

from ocpp_simulator.charger import SimulatorChargePoint
from ocpp_simulator.configuration import Configuration


class Connection:
    async def send(self, message):
        pass

    async def recv(self):
        await asyncio.Future()


class Charger(SimulatorChargePoint):
    async def call(self, payload, **kwargs):
        if not hasattr(self, 'sent'):
            self.sent = []
        self.sent.append(payload)
        if isinstance(payload, call.AuthorizePayload):
            return SimpleNamespace(id_tag_info={"status": "Accepted"})
        if isinstance(payload, call.StartTransactionPayload):
            return SimpleNamespace(
                transaction_id=payload.connector_id + 100,
                id_tag_info={"status": "Accepted"},
            )
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_two_connectors_and_remote_stop():
    cp = Charger("SIM001", Connection(), connectors=2, meter_interval=0.01)
    first = await cp.on_remote_start("TAG1", connector_id=1)
    assert first.status == "Accepted"
    assert (await cp.on_remote_start("TAG1", connector_id=1)).status == "Rejected"
    assert (await cp.on_remote_start("TAG2", connector_id=2)).status == "Accepted"
    for _ in range(100):
        if len(cp.registry.transactions) == 2:
            break
        await asyncio.sleep(0.01)
    assert set(cp.registry.transactions) == {101, 102}
    for _ in range(100):
        if len([p for p in cp.sent if isinstance(p, call.MeterValuesPayload)]) >= 2:
            break
        await asyncio.sleep(0.01)
    samples = [p for p in cp.sent if isinstance(p, call.MeterValuesPayload)]
    assert samples, "Remote charging must emit MeterValues while transaction is open"
    assert {p.connector_id for p in samples} == {1, 2}
    assert {p.transaction_id for p in samples} == {101, 102}
    assert all(p.meter_value[0].sampled_value[0].unit == "Wh" for p in samples)
    assert cp.on_remote_stop(999).status == "Rejected"
    assert cp.on_remote_stop(101).status == "Accepted"
    await asyncio.wait_for(cp.registry.tasks[1], timeout=2)
    assert 102 in cp.registry.transactions
    assert cp.on_remote_stop(102).status == "Accepted"
    await cp.shutdown_sessions()
    assert not cp.registry.transactions


@pytest.mark.asyncio
async def test_remote_reset_flags():
    cp = Charger("SIM001", Connection())
    assert cp.on_reset("Invalid").status == "Rejected"
    assert not cp.reset_requested.is_set()
    assert cp.on_reset("Soft").status == "Accepted"
    assert cp.reset_requested.is_set()
    assert cp.reset_type == "Soft"


def test_configuration_keys_and_validation():
    cfg = Configuration(connectors=2)
    entries, missing = cfg.get(["NumberOfConnectors", "missing"])
    assert len(entries) == 1
    assert entries[0].key == "NumberOfConnectors"
    assert entries[0].readonly
    assert missing == ["missing"]
    assert cfg.change("NumberOfConnectors", "3") == "Rejected"
    assert cfg.change("unknown", "value") == "NotSupported"
    assert cfg.change("MeterValueSampleInterval", "0") == "Rejected"
    assert cfg.change("MeterValueSampleInterval", "10") == "Accepted"
    assert cfg.values["MeterValueSampleInterval"][0] == "10"
    assert cfg.change("bsPort", "65536") == "Rejected"
    assert cfg.change("bsPort", "9000") == "Accepted"


@pytest.mark.asyncio
async def test_remote_bad_connector():
    cp = Charger("SIM001", Connection(), connectors=2)
    assert (await cp.on_remote_start("TAG", connector_id=3)).status == "Rejected"

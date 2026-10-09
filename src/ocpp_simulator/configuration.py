"""In-memory OCPP configuration with read-only and writable keys."""
from __future__ import annotations

class Configuration:
    def __init__(self, connectors=1, heartbeat=60, meter_interval=5):
        self.values = {
            "NumberOfConnectors": (str(connectors), True),
            "HeartbeatInterval": (str(heartbeat), False),
            "MeterValueSampleInterval": (str(meter_interval), False),
            "sDomain": ("", False), "sPort": ("0", False), "sURL": ("", False),
            "bsDomain": ("", False), "bsPort": ("0", False), "bsURL": ("", False),
        }

    def get(self, keys=None):
        from ocpp.v16.datatypes import KeyValue
        selected = self.values if not keys else {k: self.values[k] for k in keys if k in self.values}
        unknown = [k for k in (keys or []) if k not in self.values]
        entries = [KeyValue(key=k, readonly=readonly, value=value) for k, (value, readonly) in selected.items()]
        return entries, unknown

    def change(self, key, value):
        if key not in self.values:
            return "NotSupported"
        if self.values[key][1]:
            return "Rejected"
        if key in ("HeartbeatInterval", "MeterValueSampleInterval", "sPort", "bsPort"):
            if not value.isdecimal() or (key in ("HeartbeatInterval", "MeterValueSampleInterval") and int(value) < 1):
                return "Rejected"
            if key.endswith("Port") and int(value) > 65535:
                return "Rejected"
        self.values[key] = (value, False)
        return "Accepted"

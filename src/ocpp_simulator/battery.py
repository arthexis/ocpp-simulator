"""Simple reproducible EV battery and AC charging load model.

This is a test simulation, not a vehicle physics or battery safety model.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import random


@dataclass
class Battery:
    capacity_kwh: float = 60.0
    soc: float = 30.0
    seed: int = 1
    energy_wh: float = 0.0
    last_power_kw: float = 0.0
    _rng: random.Random = field(init=False, repr=False)
    _fluctuation: float = field(default=0.0, init=False)

    def __post_init__(self):
        if not 0 < self.capacity_kwh or not 0 <= self.soc <= 100:
            raise ValueError("capacity must be positive and initial SOC must be 0..100")
        self._rng = random.Random(self.seed)

    def power_kw(self, ceiling_kw: float) -> float:
        """AC-like plateau until high SOC, then a smooth taper."""
        if ceiling_kw <= 0:
            raise ValueError("power ceiling must be positive")
        if self.soc >= 100:
            return 0.0
        taper = 1.0 if self.soc < 80 else max(0.08, (100 - self.soc) / 20)
        # Bounded, smoothed load drift: visually natural and repeatable.
        self._fluctuation = max(-0.06, min(0.06, self._fluctuation * 0.75 +
                                          self._rng.uniform(-0.025, 0.025)))
        return ceiling_kw * taper * (1 + self._fluctuation)

    def advance(self, seconds: float, ceiling_kw: float) -> tuple[float, int]:
        if seconds < 0:
            raise ValueError("elapsed seconds must be nonnegative")
        power = self.power_kw(ceiling_kw)
        self.last_power_kw = power
        room_wh = max(0.0, self.capacity_kwh * 1000 * (100 - self.soc) / 100)
        delivered_wh = min(power * 1000 * seconds / 3600, room_wh)
        self.energy_wh += delivered_wh
        self.soc = min(100.0, self.soc + delivered_wh * 100 / (self.capacity_kwh * 1000))
        return power, round(self.energy_wh)

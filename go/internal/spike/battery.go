package spike

import (
 "fmt"
 "math"
 "time"
)

// Battery models a finite simulated EV. It has no network or OCPP side effects.
// State is expressed in Wh so a scenario can be replayed deterministically.
type Battery struct {
 CapacityWh float64
 EnergyWh float64
 seed uint64
 drift float64
}

// NewBattery validates the battery capacity and initial state of charge.
func NewBattery(capacityKWh, initialSOC float64) (Battery, error) {
 if math.IsNaN(capacityKWh) || math.IsInf(capacityKWh, 0) || capacityKWh <= 0 ||
  math.IsNaN(initialSOC) || math.IsInf(initialSOC, 0) || initialSOC < 0 || initialSOC > 100 {
  return Battery{}, fmt.Errorf("invalid battery capacity or initial SOC")
 }
 capacity := capacityKWh * 1000
 if math.IsInf(capacity, 0) || capacity > float64(1<<31-1) {
  return Battery{}, fmt.Errorf("battery capacity exceeds supported meter range")
 }
 return Battery{CapacityWh: capacity, EnergyWh: capacity * initialSOC / 100, seed: 1}, nil
}

func (b Battery) SOC() float64 {
 if b.CapacityWh <= 0 { return 0 }
 return math.Min(100, 100*b.EnergyWh/b.CapacityWh)
}

func (b Battery) Full() bool { return b.EnergyWh >= b.CapacityWh }

// PowerKW returns the available power at the current state of charge.
// It tapers after 80% with the Python model's 8% floor until full.
func (b Battery) PowerKW(maxKW float64) float64 {
 if maxKW <= 0 || math.IsNaN(maxKW) || math.IsInf(maxKW, 0) || b.Full() { return 0 }
 soc := b.SOC()
 if soc <= 80 { return maxKW }
 return maxKW * math.Max(0.08, (100-soc)/20)
}

// Advance integrates charging over a bounded time step, never exceeding capacity.
// It returns delivered Wh, which is independent of wall-clock scheduling.
func (b *Battery) Advance(maxKW float64, step time.Duration) (float64, error) {
 if b == nil || b.CapacityWh <= 0 || b.EnergyWh < 0 || b.EnergyWh > b.CapacityWh ||
  maxKW <= 0 || math.IsNaN(maxKW) || math.IsInf(maxKW, 0) || step < 0 {
  return 0, fmt.Errorf("invalid battery charging step")
 }
 if step == 0 || b.Full() { return 0, nil }
 delivered := math.Min(b.CapacityWh-b.EnergyWh, b.PowerKW(maxKW)*1000*step.Seconds()/3600)
 b.EnergyWh += delivered
 return delivered, nil
}

// SetSeed selects a repeatable local pseudo-random load sequence. The sequence
// is not bit-for-bit compatible with Python random.Random(seed).
func (b *Battery) SetSeed(seed uint64) {
 b.seed = seed
 b.drift = 0
}

// nextVariation uses a deterministic xorshift64* generator with a nonzero
// fallback for seed zero. This is simulation noise, not cryptographic randomness.
func (b *Battery) nextVariation() float64 {
 state := b.seed
 if state == 0 { state = 0x9e3779b97f4a7c15 }
 state ^= state >> 12
 state ^= state << 25
 state ^= state >> 27
 b.seed = state
 unit := float64((state*2685821657736338717)>>11) / float64(uint64(1)<<53)
 b.drift = math.Max(-0.06, math.Min(0.06, b.drift*0.75+(unit*2-1)*0.025))
 return b.drift
}

// MeterWh rounds cumulative delivered energy like Python round(): ties to even.
func (b Battery) MeterWh(initialEnergyWh float64) (int, error) {
 delta := b.EnergyWh-initialEnergyWh
 if math.IsNaN(delta) || math.IsInf(delta, 0) || delta < 0 || delta > float64(1<<31-1) {
  return 0, fmt.Errorf("invalid meter delta")
 }
 return int(math.RoundToEven(delta)), nil
}

// AdvanceSeeded applies the same bounded, smoothed drift envelope as Python.
// The exact pseudo-random sequence differs across language runtimes.
func (b *Battery) AdvanceSeeded(maxKW float64, step time.Duration) (float64, error) {
 if b == nil || b.CapacityWh <= 0 || b.EnergyWh < 0 || b.EnergyWh > b.CapacityWh ||
  maxKW <= 0 || math.IsNaN(maxKW) || math.IsInf(maxKW, 0) || step < 0 {
  return 0, fmt.Errorf("invalid battery charging step")
 }
 if step == 0 || b.Full() { return 0, nil }
 power := b.PowerKW(maxKW)*(1+b.nextVariation())
 delivered := math.Min(b.CapacityWh-b.EnergyWh,power*1000*step.Seconds()/3600)
 b.EnergyWh += delivered
 return delivered,nil
}

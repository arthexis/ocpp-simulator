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
 return Battery{CapacityWh: capacity, EnergyWh: capacity * initialSOC / 100}, nil
}

func (b Battery) SOC() float64 {
 if b.CapacityWh <= 0 { return 0 }
 return math.Min(100, 100*b.EnergyWh/b.CapacityWh)
}

func (b Battery) Full() bool { return b.EnergyWh >= b.CapacityWh }

// PowerKW returns the available power at the current state of charge.
// It tapers linearly after 80% and reaches zero at 100%.
func (b Battery) PowerKW(maxKW float64) float64 {
 if maxKW <= 0 || math.IsNaN(maxKW) || math.IsInf(maxKW, 0) || b.Full() { return 0 }
 soc := b.SOC()
 if soc <= 80 { return maxKW }
 return maxKW * (100-soc)/20
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

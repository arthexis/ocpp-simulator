package spike

import (
 "math"
 "testing"
 "time"
)

func TestBatteryCapacityAndTaper(t *testing.T) {
 b, err := NewBattery(60, 30)
 if err != nil { t.Fatal(err) }
 if b.SOC() != 30 || b.PowerKW(7.2) != 7.2 { t.Fatalf("initial battery: %+v", b) }
 b.EnergyWh = b.CapacityWh * 0.9
 if math.Abs(b.PowerKW(7.2)-3.6) > 1e-9 { t.Fatalf("90%% taper: %g", b.PowerKW(7.2)) }
 b.EnergyWh = b.CapacityWh * 0.999
 if math.Abs(b.PowerKW(7.2)-0.576) > 1e-9 { t.Fatalf("taper floor: %g", b.PowerKW(7.2)) }
 b.EnergyWh = b.CapacityWh
 if !b.Full() || b.PowerKW(7.2) != 0 { t.Fatal("full battery must not charge") }
}

func TestBatteryStepAndCapacityBound(t *testing.T) {
 b, err := NewBattery(1, 0)
 if err != nil { t.Fatal(err) }
 wh, err := b.Advance(7.2, time.Minute)
 if err != nil { t.Fatal(err) }
 if wh != 120 || b.EnergyWh != 120 || b.SOC() != 12 { t.Fatalf("energy accounting: %g %+v", wh, b) }
 b.EnergyWh = 999
 wh, err = b.Advance(7.2, time.Hour)
 if err != nil { t.Fatal(err) }
 if wh <= 0 || wh > 1 || b.EnergyWh != 1000 || !b.Full() { t.Fatalf("capacity bound: %g %+v", wh, b) }
 wh, err = b.Advance(7.2, time.Second)
 if err != nil || wh != 0 { t.Fatalf("full battery advanced: %g %v", wh, err) }
}

func TestBatteryInvalidInputs(t *testing.T) {
 for _, v := range [][2]float64{{0,30},{-1,30},{60,-1},{60,101},{math.NaN(),30},{60,math.Inf(1)}} {
  if _,err := NewBattery(v[0],v[1]); err == nil { t.Fatalf("accepted invalid battery %v",v) }
 }
 b,_ := NewBattery(60,30)
 for _, p := range []float64{0,-1,math.NaN(),math.Inf(1)} {
  if _,err := b.Advance(p,time.Second); err == nil { t.Fatalf("accepted invalid power %g",p) }
 }
 if _,err := b.Advance(7.2,-time.Second); err == nil { t.Fatal("accepted negative time") }
}

func TestBatteryDeterministicReplay(t *testing.T) {
 a,_ := NewBattery(60,79)
 b,_ := NewBattery(60,79)
 for i:=0;i<100;i++ {
  x,err := a.Advance(7.2,time.Second)
  if err != nil { t.Fatal(err) }
  y,err := b.Advance(7.2,time.Second)
  if err != nil || x != y || a.EnergyWh != b.EnergyWh { t.Fatalf("replay diverged at %d",i) }
 }
 if a.SOC() <= 79 { t.Fatal("battery did not advance") }
}
